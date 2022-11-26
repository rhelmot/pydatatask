from typing import Union, Callable, Dict, Any, TYPE_CHECKING
from itertools import islice, cycle
from collections import Counter
import shutil
import string
from pathlib import Path
import logging
import yaml
import os
import hashlib
import io

import dreg_client
import dxf
import minio
import pymongo

from .pod_manager import PodManager

if TYPE_CHECKING:
    from .task import ExecutorTask

l = logging.getLogger(__name__)

__all__ = (
    'Repository',
    'BlobRepository',
    'MetadataRepository',
    'FileRepositoryBase',
    'FileRepository',
    'DirectoryRepository',
    'S3BucketRepository',
    'S3BucketInfo',
    'MongoMetadataRepository',
    'InProcessMetadataRepository',
    'DockerRepository',
    'LiveKubeRepository',
    'ExecutorLiveRepo',
    'AggregateOrRepository',
    'AggregateAndRepository',
    'AggregateRepositoryInfo',
    'BlockingRepository',
    'YamlMetadataRepository',
    'YamlMetadataFileRepository',
    'YamlMetadataS3Repository',
    'RelatedItemRepository',
)

def roundrobin(*iterables):
    "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
    # Recipe credited to George Sakkis
    num_active = len(iterables)
    nexts = cycle(iter(it).__next__ for it in iterables)
    while num_active:
        try:
            for next in nexts:
                yield next()
        except StopIteration:
            # Remove the iterator we just exhausted from the cycle.
            num_active -= 1
            nexts = cycle(islice(nexts, num_active))

def job_getter(f):
    f.is_job_getter = True
    return f

class Repository:
    """
    A repository is a key-value store where the keys are names of jobs. Since the values have unspecified semantics, the
    only operations you can do on a generic repository are query for keys.
    """

    CHARSET = string.ascii_letters + string.digits + '_-.'
    CHARSET_START_END = string.ascii_letters + string.digits
    @classmethod
    def is_valid_job_id(cls, ident):
        return 0 < len(ident) < 64 and \
               all(c in cls.CHARSET for c in ident) and \
               ident[0] in cls.CHARSET_START_END and \
               ident[-1] in cls.CHARSET_START_END

    def filter_jobs(self, iterator):
        for ident in iterator:
            if self.is_valid_job_id(ident):
                yield ident
            else:
                l.warning("Skipping %s %s - not a valid job id", self, ident)

    def __contains__(self, item):
        return any(item == x for x in self)

    def __iter__(self):
        return self.filter_jobs(self._unfiltered_iter())

    def _unfiltered_iter(self):
        raise NotImplementedError

    @job_getter
    def info(self, ident):
        """
        Returns an arbitrary piece of data related to ident. Notably, this is used during templating.
        This should do something meaningful even if the repository does not contain ident.
        """
        raise NotImplementedError

    def __delitem__(self, key):
        raise NotImplementedError

    def info_all(self) -> Dict[str, Any]:
        return {ident: self.info(ident) for ident in self}

    def map(self, func: Callable, allow_deletes=False) -> 'MapRepository':
        return MapRepository(self, func, allow_deletes=allow_deletes)

class MapRepository(Repository):
    def __init__(self, child: Repository, func: Callable, allow_deletes=False):
        self.child = child
        self.func = func
        self.allow_deletes = allow_deletes

    def __contains__(self, item):
        return item in self.child

    def __delitem__(self, key):
        if self.allow_deletes:
            del self.child[key]

    def _unfiltered_iter(self):
        return self.child._unfiltered_iter()

    def info(self, ident):
        return self.func(self.child.info(ident))

    def info_all(self) -> Dict[str, Any]:
        result = self.child.info_all()
        for k, v in result.items():
            result[k] = self.func(v)
        return result

class MetadataRepository(Repository):
    @job_getter
    def info(self, job):
        raise NotImplementedError

    @job_getter
    def dump(self, job, data):
        raise NotImplementedError

class BlobRepository(Repository):
    @job_getter
    def open(self, ident, mode='r'):
        raise NotImplementedError

class FileRepositoryBase(Repository):
    def __init__(self, basedir, extension='', case_insensitive=False):
        self.basedir = Path(basedir)
        self.extension = extension
        self.case_insensitive = case_insensitive

        self.ensure_exists()

    def __contains__(self, item):
        return (self.basedir / (item + self.extension)).exists()

    def __repr__(self):
        return f'<{type(self).__name__} {self.basedir / ("*" + self.extension)}>'

    def _unfiltered_iter(self):
        return (
            path.name[:-len(self.extension) if self.extension else None]
            for path in self.basedir.iterdir()
            if (
                path.name.lower().endswith(self.extension.lower())
                if self.case_insensitive
                else path.name.endswith(self.extension)
            )
        )

    def ensure_exists(self):
        self.basedir.mkdir(exist_ok=True, parents=True)
        if not os.access(self.basedir, os.W_OK):
            raise PermissionError(f"Cannot write to {self.basedir}")

    @job_getter
    def fullpath(self, ident):
        return self.basedir / (ident + self.extension)

    @job_getter
    def info(self, job):
        return str(self.fullpath(job))

class FileRepository(FileRepositoryBase, BlobRepository):
    """
    A file repository is a directory where each key is a filename, optionally suffixed with an extension before hitting
    the filesystem.
    """

    @job_getter
    def open(self, ident, mode='r'):
        return open(self.fullpath(ident), mode)

    def __delitem__(self, key):
        self.fullpath(key).unlink(missing_ok=True)

class DirectoryRepository(FileRepositoryBase):
    """
    A directory repository is like a file repository but its members are directories
    """
    def __init__(self, *args, discard_empty=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.discard_empty = discard_empty

    @job_getter
    def mkdir(self, ident):
        self.fullpath(ident).mkdir(exist_ok=True)

    def __delitem__(self, key):
        if key in self:
            shutil.rmtree(self.fullpath(key))

    def __contains__(self, item):
        result = super().__contains__(item)
        if not self.discard_empty:
            return result
        if not result:
            return False
        return bool(list(self.fullpath(item).iterdir()))

    def _unfiltered_iter(self):
        for item in super()._unfiltered_iter():
            if self.discard_empty:
                if bool(list(self.fullpath(item).iterdir())):
                    yield item
            else:
                yield item

class S3BucketBinaryWriter(io.BytesIO):
    def __init__(self, repo: 'S3BucketRepository', job: str):
        self.repo = repo
        self.job = job
        super().__init__()

    def close(self):
        self.seek(0, io.SEEK_END)
        size = self.tell()
        self.seek(0, io.SEEK_SET)
        self.repo.client.put_object(
            self.repo.bucket,
            self.repo.object_name(self.job),
            self,
            size,
            content_type=self.repo.mimetype,
        )

class S3BucketInfo:
    def __init__(self, endpoint: str, uri: str):
        self.endpoint = endpoint
        self.uri = uri

    def __str__(self):
        return self.uri

class S3BucketRepository(BlobRepository):
    def __init__(
            self,
            client: minio.Minio,
            bucket: str,
            prefix: str='',
            extension: str='',
            mimetype: str='application/octet-stream',
    ):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.extension = extension
        self.mimetype = mimetype

        self.ensure_exists()

    def __contains__(self, item):
        try:
            self.client.stat_object(self.bucket, self.object_name(item))
        except minio.error.S3Error:
            return False
        else:
            return True

    def _unfiltered_iter(self):
        for obj in self.client.list_objects(self.bucket, self.prefix):
            if obj.object_name.endswith(self.extension):
                yield obj.object_name[len(self.prefix):-len(self.extension) if self.extension else None]

    def ensure_exists(self):
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    @job_getter
    def object_name(self, ident):
        return f'{self.prefix}{ident}{self.extension}'

    @job_getter
    def open(self, ident, mode='r') -> Union[S3BucketBinaryWriter, io.TextIOWrapper]:
        if mode == 'wb':
            return S3BucketBinaryWriter(self, ident)
        elif mode == 'w':
            return io.TextIOWrapper(S3BucketBinaryWriter(self, ident))
        elif mode == 'rb':
            return self.client.get_object(self.bucket, self.object_name(ident))
        elif mode == 'r':
            return io.TextIOWrapper(self.client.get_object(self.bucket, self.object_name(ident)))
        else:
            raise ValueError(mode)

    @job_getter
    def info(self, ident):
        return S3BucketInfo(self.client._base_url._url.geturl(), f's3://{self.bucket}/{self.object_name(ident)}')

    def __delitem__(self, key):
        self.client.remove_object(self.bucket, self.object_name(key))

class MongoMetadataRepository(MetadataRepository):
    def __init__(self, collection: pymongo.collection.Collection):
        self.collection = collection

    def __contains__(self, item):
        return self.collection.count_documents({'_id': item}) != 0

    def __delitem__(self, key):
        self.collection.delete_one({'_id': key})

    def _unfiltered_iter(self):
        yield from (x['_id'] for x in self.collection.find({}, projection=[]))

    @job_getter
    def info(self, job):
        result = self.collection.find_one({'_id': job})
        if result is None:
            result = {}
        return result

    def info_all(self) -> Dict[str, Any]:
        return {entry['_id']: entry for entry in self.collection.find({})}

    @job_getter
    def dump(self, job, data):
        self.collection.replace_one({'_id': job}, data, upsert=True)

class DockerRepository(Repository):
    """
    A docker repository is, well, an actual docker repository hosted in some registry somewhere. Keys translate to tags
    on this repository.
    """
    def __init__(self, registry: dreg_client.Registry, domain: str, repository: str):
        self.registry = registry
        self.domain = domain
        self.repository = repository

    def _unfiltered_iter(self):
        try:
            return self.registry.repository(self.repository).tags()
        except Exception as e:
            if '404' in str(e):
                return []
            else:
                raise

    def __repr__(self):
        return f'<DockerRepository {self.domain}/{self.repository}>'

    @job_getter
    def info(self, job):
        return {
            'withdomain': f'{self.domain}/{self.repository}:{job}',
            'withoutdomain': f'{self.repository}:{job}',
        }

    def _dxf_auth(self, dxf_obj, response):
        # what a fucking hack
        username, password = self.registry._client._session.auth
        dxf_obj.authenticate(username, password, response)

    def __delitem__(self, key):
        if key not in self:
            return

        random_data = os.urandom(16)
        random_digest = 'sha256:' + hashlib.sha256(random_data).hexdigest()

        d = dxf.DXF(
            host=self.domain,
            repo=self.repository,
            auth=self._dxf_auth,
        )
        d.push_blob(data=random_data, digest=random_digest)
        d.set_alias(key, random_digest)
        d.del_alias(key)

class LiveKubeRepository(Repository):
    """
    A repository where keys translate to `job` labels on running kube pods.
    """
    def __init__(self, podman: PodManager, task: str):
        self.task = task
        self.podman = podman

    def _unfiltered_iter(self):
        return (pod.metadata.labels['job'] for pod in self.pods())

    def __contains__(self, item):
        return bool(self.podman.query(task=self.task, job=item))

    def __repr__(self):
        return f'<LiveKubeRepository task={self.task}>'

    @job_getter
    def info(self, job):
        # Cannot template with live kube info. Implement this if you have something in mind.
        return None

    def pods(self):
        return self.podman.query(task=self.task)

    def __delitem__(self, key):
        pods = self.podman.query(job=key, task=self.task)
        for pod in pods:  # there... really should be only one
            self.podman.delete(pod)

class AggregateAndRepository(Repository):
    """
    A repository which is said to contain a key if all its children also contain that key
    """
    def __init__(self, **children: Repository):
        assert children
        self.children = children

    def _unfiltered_iter(self):
        counting = Counter()
        for item in roundrobin(*(child._unfiltered_iter() for child in self.children.values())):
            counting[item] += 1
            if counting[item] == len(self.children):
                yield item

    def __contains__(self, item):
        return all(item in child for child in self.children.values())

    @job_getter
    def info(self, job):
        return AggregateRepositoryInfo(self, job)

    def __delitem__(self, key):
        for child in self.children.values():
            del child[key]

class AggregateOrRepository(Repository):
    """
    A repository which is said to contain a key if any of its children also contain that key
    """
    def __init__(self, **children: Repository):
        assert children
        self.children = children

    def _unfiltered_iter(self):
        seen = set()
        for child in self.children.values():
            for item in child._unfiltered_iter():
                if item in seen:
                    continue
                seen.add(item)
                yield item

    def __contains__(self, item):
        return any(item in child for child in self.children.values())

    @job_getter
    def info(self, job):
        return AggregateRepositoryInfo(self, job)

    def __delitem__(self, key):
        for child in self.children.values():
            del child[key]

class AggregateRepositoryInfo:
    def __init__(self, repo: Union[AggregateAndRepository, AggregateOrRepository], job):
        self.repo = repo
        self.job = job

    def __getattr__(self, item):
        return self.repo.children[item].info(self.job)

class BlockingRepository(Repository):
    """
    A class that is said to contain a key if `source` contains it and `unless` does not contain it
    """
    def __init__(self, source: Repository, unless: Repository, enumerate_unless=True):
        self.source = source
        self.unless = unless
        self.enumerate_unless = enumerate_unless

    def _unfiltered_iter(self):
        if self.enumerate_unless:
            blocked = set(self.unless._unfiltered_iter())
        else:
            blocked = None
        for item in self.source._unfiltered_iter():
            if self.enumerate_unless and item in blocked:
                continue
            if not self.enumerate_unless and item in self.unless:
                continue
            yield item

    def __contains__(self, item):
        return item in self.source and not item in self.unless

    @job_getter
    def info(self, job):
        return self.source.info(job)

    def __delitem__(self, key):
        del self.source[key]

class YamlMetadataRepository(BlobRepository, MetadataRepository):
    """
    A metadata repository. When info is accessed, it will **load the target file into memory**, parse it as yaml, and
    return the resulting object.
    """
    @job_getter
    def info(self, job):
        with self.open(job, 'rb') as fp:
            return yaml.safe_load(fp)

    @job_getter
    def dump(self, job, data):
        with self.open(job, 'w') as fp:
            yaml.safe_dump(data, fp)

class YamlMetadataFileRepository(YamlMetadataRepository, FileRepository):
    def __init__(self, filename, extension='.yaml', case_insensitive=False):
        super().__init__(filename, extension=extension, case_insensitive=case_insensitive)

class YamlMetadataS3Repository(YamlMetadataRepository, S3BucketRepository):
    def __init__(self, client, bucket, prefix, extension='.yaml', mimetype="text/yaml"):
        super().__init__(client, bucket, prefix, extension=extension, mimetype=mimetype)

    @job_getter
    def info(self, job):
        try:
            return super().info(job)
        except minio.S3Error as e:
            if e.code == 'NoSuchKey':
                return {}
            else:
                raise

class RelatedItemRepository(Repository):
    """
    A repository which returns items from another repository based on following a related-item lookup.
    """

    def __init__(
            self,
            base_repository: Repository,
            translator_repository: Repository,
            allow_deletes=False,
            prefetch_lookup=None,
    ):
        self.base_repository = base_repository
        self.translator_repository = translator_repository
        self.allow_deletes = allow_deletes
        self.prefetch_lookup_setting = prefetch_lookup
        self.prefetch_lookup = None

        if prefetch_lookup is True:
            self.prefetch_lookup = self.translator_repository.info_all()

    def _lookup(self, item):
        if self.prefetch_lookup is None and self.prefetch_lookup_setting is None:
            self.prefetch_lookup = self.translator_repository.info_all()
        if self.prefetch_lookup is not None:
            return self.prefetch_lookup.get(item)
        else:
            return self.translator_repository.info(item)

    def __contains__(self, item):
        basename = self._lookup(item)
        if basename is None:
            return False
        return basename in self.base_repository

    def __delitem__(self, key):
        if not self.allow_deletes:
            return

        basename = self._lookup(key)
        if basename is None:
            return

        del self.base_repository[basename]

    @job_getter
    def info(self, ident):
        basename = self._lookup(ident)
        if basename is None:
            raise LookupError(ident)

        return self.base_repository.info(basename)

    def __getattr__(self, item):
        v = getattr(self.base_repository, item)
        if not getattr(v, 'is_job_getter', False):
            return v

        def inner(job, *args, **kwargs):
            basename = self._lookup(job)
            if basename is None:
                raise LookupError(job)
            return v(basename, *args, **kwargs)

        return inner

    def _unfiltered_iter(self):
        for item in self.translator_repository:
            basename = self._lookup(item)
            if basename is not None: # and basename in self.base_repository:
                yield item

class ExecutorLiveRepo(Repository):
    def __init__(self, task: 'ExecutorTask'):
        self.task = task

    def _unfiltered_iter(self):
        return self.task.rev_jobs

    def __contains__(self, item):
        return item in self.task.rev_jobs

    def __delitem__(self, key):
        self.task.cancel(key)

    def info(self, ident):
        return None

class InProcessMetadataRepository(MetadataRepository):
    def __init__(self, data: Dict[str, Any]):
        self.data = data

    def info(self, job):
        return self.data.get(job)

    def dump(self, job, data):
        self.data[job] = data

    def __contains__(self, item):
        return item in self.data

    def __delitem__(self, key):
        del self.data[key]

    def _unfiltered_iter(self):
        return self.data
