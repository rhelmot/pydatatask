from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncGenerator,
    AsyncIterable,
    Callable,
    Coroutine,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Union,
)
from collections import Counter
from pathlib import Path
import base64
import codecs
import hashlib
import inspect
import io
import logging
import os
import string

from types_aiobotocore_s3.client import S3Client
import aiofiles.os
import aioshutil
import botocore.exceptions
import docker_registry_client_async
import dxf
import motor.motor_asyncio
import yaml

if TYPE_CHECKING:
    from .task import ExecutorTask, KubeTask

l = logging.getLogger(__name__)

__all__ = (
    "Repository",
    "BlobRepository",
    "MetadataRepository",
    "FileRepositoryBase",
    "FileRepository",
    "DirectoryRepository",
    "S3BucketRepository",
    "S3BucketInfo",
    "MongoMetadataRepository",
    "InProcessMetadataRepository",
    "InProcessBlobStream",
    "InProcessBlobRepository",
    "DockerRepository",
    "LiveKubeRepository",
    "ExecutorLiveRepo",
    "AggregateOrRepository",
    "AggregateAndRepository",
    "BlockingRepository",
    "YamlMetadataRepository",
    "YamlMetadataFileRepository",
    "YamlMetadataS3Repository",
    "RelatedItemRepository",
    "AReadStream",
    "AWriteStream",
    "AReadText",
    "AWriteText",
)


class AReadStream(Protocol, AsyncContextManager):
    async def read(self, n: Optional[int] = None) -> bytes:
        ...

    async def close(self) -> None:
        ...


class AWriteStream(Protocol, AsyncContextManager):
    async def write(self, data: bytes):
        ...

    async def close(self) -> None:
        ...


class AReadText:
    def __init__(
        self,
        base: AReadStream,
        encoding: str = "utf-8",
        errors="strict",
        chunksize=4096,
    ):
        self.base = base
        self.decoder = codecs.lookup(encoding).incrementaldecoder(errors)
        self.buffer = ""
        self.chunksize = chunksize

    async def read(self, n: Optional[int] = None) -> str:
        while n is None or len(self.buffer) < n:
            data = await self.base.read(self.chunksize)
            self.buffer += self.decoder.decode(data, final=not bool(data))
            if not data:
                break

        if n is not None:
            result, self.buffer = self.buffer[:n], self.buffer[:n]
        else:
            result, self.buffer = self.buffer, ""
        return result

    async def close(self):
        await self.base.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class AWriteText:
    def __init__(self, base: AWriteStream, encoding="utf-8", errors="strict"):
        self.base = base
        self.encoding = encoding
        self.errors = errors

    async def write(self, data: str):
        await self.base.write(data.encode(self.encoding, self.errors))

    async def close(self):
        await self.base.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


async def roundrobin(iterables: List):
    "roundrobin('ABC', 'D', 'EF') --> A D E B F C"
    i = 0
    while iterables:
        i %= len(iterables)
        try:
            yield await iterables[i].__anext__()
        except StopAsyncIteration:
            iterables.pop(i)
        else:
            i += 1


def job_getter(f):
    if not inspect.iscoroutinefunction(f):
        raise TypeError("only async functions can be job_getters")
    f.is_job_getter = True
    return f


class Repository:
    """
    A repository is a key-value store where the keys are names of jobs. Since the values have unspecified semantics, the
    only operations you can do on a generic repository are query for keys.
    """

    CHARSET = string.ascii_letters + string.digits + "_-."
    CHARSET_START_END = string.ascii_letters + string.digits

    @classmethod
    def is_valid_job_id(cls, ident):
        return (
            0 < len(ident) < 64
            and all(c in cls.CHARSET for c in ident)
            and ident[0] in cls.CHARSET_START_END
            and ident[-1] in cls.CHARSET_START_END
        )

    async def filter_jobs(self, iterator: AsyncIterable[str]):
        async for ident in iterator:
            if self.is_valid_job_id(ident):
                yield ident
            else:
                l.warning("Skipping %s %s - not a valid job id", self, repr(ident))

    async def contains(self, item):
        async for x in self:
            if x == item:
                return True
        return False

    def __aiter__(self):
        return self.filter_jobs(self._unfiltered_iter())

    async def _unfiltered_iter(self) -> AsyncGenerator[str, None]:
        raise NotImplementedError
        # noinspection PyUnreachableCode
        yield None

    @job_getter
    async def info(self, ident):
        """
        Returns an arbitrary piece of data related to ident. Notably, this is used during templating.
        This should do something meaningful even if the repository does not contain ident.
        """
        raise NotImplementedError

    async def delete(self, key):
        raise NotImplementedError

    async def info_all(self) -> Dict[str, Any]:
        return {ident: await self.info(ident) async for ident in self}

    async def validate(self):
        """
        Raise an exception if for any reason the repository is misconfigured.
        """
        pass

    def map(
        self, func: Callable, filter: Optional[Callable[[Any], Coroutine[None, None, Any]]] = None, allow_deletes=False
    ) -> "MapRepository":
        return MapRepository(self, func, filter, allow_deletes=allow_deletes)


class MapRepository(Repository):
    def __init__(
        self,
        child: Repository,
        func: Callable[[Any], Coroutine[None, None, Any]],
        filter: Optional[Callable[[str], Coroutine[None, None, bool]]] = None,
        allow_deletes=False,
    ):
        self.child = child
        self.func = func
        self.filter = filter
        self.allow_deletes = allow_deletes

    async def contains(self, item):
        if self.filter is None or await self.filter(item):
            return await self.child.contains(item)
        return False

    async def delete(self, key):
        if self.allow_deletes:
            await self.child.delete(key)

    async def _unfiltered_iter(self):
        async for item in self.child._unfiltered_iter():
            if self.filter is None or await self.filter(item):
                yield item

    async def info(self, ident):
        return await self.func(await self.child.info(ident))

    async def info_all(self) -> Dict[str, Any]:
        result = await self.child.info_all()
        to_remove = []
        for k, v in result.items():
            if self.filter is None or await self.filter(k):
                result[k] = await self.func(v)
            else:
                to_remove.append(k)
        for k in to_remove:
            result.pop(k)
        return result


class MetadataRepository(Repository):
    @job_getter
    async def info(self, job):
        raise NotImplementedError

    @job_getter
    async def dump(self, job, data):
        raise NotImplementedError


class BlobRepository(Repository):
    @job_getter
    async def open(
        self, ident, mode: Literal["r", "rb", "w", "wb"] = "r"
    ) -> Union[AReadStream, AWriteStream, AReadText, AWriteText]:
        raise NotImplementedError


class FileRepositoryBase(Repository):
    def __init__(self, basedir, extension="", case_insensitive=False):
        self.basedir = Path(basedir)
        self.extension = extension
        self.case_insensitive = case_insensitive

    async def contains(self, item):
        return await aiofiles.os.path.exists(self.basedir / (item + self.extension))

    def __repr__(self):
        return f'<{type(self).__name__} {self.basedir / ("*" + self.extension)}>'

    async def _unfiltered_iter(self):
        for name in await aiofiles.os.listdir(self.basedir):
            if self.case_insensitive:
                cond = name.lower().endswith(self.extension.lower())
            else:
                cond = name.endswith(self.extension)
            if cond:
                yield name[: -len(self.extension) if self.extension else None]

    async def validate(self):
        self.basedir.mkdir(exist_ok=True, parents=True)
        if not os.access(self.basedir, os.W_OK):
            raise PermissionError(f"Cannot write to {self.basedir}")

    def fullpath(self, ident):
        return self.basedir / (ident + self.extension)

    @job_getter
    async def info(self, job):
        return str(self.fullpath(job))


class FileRepository(FileRepositoryBase, BlobRepository):
    """
    A file repository is a directory where each key is a filename, optionally suffixed with an extension before hitting
    the filesystem.
    """

    @job_getter
    async def open(self, ident, mode="r"):
        return aiofiles.open(self.fullpath(ident), mode)

    async def delete(self, key):
        try:
            await aiofiles.os.unlink(self.fullpath(key))
        except FileNotFoundError:
            pass


class DirectoryRepository(FileRepositoryBase):
    """
    A directory repository is like a file repository but its members are directories
    """

    def __init__(self, *args, discard_empty=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.discard_empty = discard_empty

    @job_getter
    async def mkdir(self, ident):
        try:
            await aiofiles.os.mkdir(self.fullpath(ident))
        except FileExistsError:
            pass

    async def delete(self, key):
        if await self.contains(key):
            await aioshutil.rmtree(self.fullpath(key))

    async def contains(self, item):
        result = await super().contains(item)
        if not self.discard_empty:
            return result
        if not result:
            return False
        return bool(list(await aiofiles.os.listdir(self.fullpath(item))))

    async def _unfiltered_iter(self):
        async for item in super()._unfiltered_iter():
            if self.discard_empty:
                if bool(list(await aiofiles.os.listdir(self.fullpath(item)))):
                    yield item
            else:
                yield item


class S3BucketBinaryWriter:
    def __init__(self, repo: "S3BucketRepository", job: str):
        self.repo = repo
        self.job = job
        self.buffer = io.BytesIO()
        super().__init__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        self.buffer.seek(0, io.SEEK_END)
        size = self.buffer.tell()
        self.buffer.seek(0, io.SEEK_SET)
        await self.repo.client.put_object(
            Bucket=self.repo.bucket,
            Key=self.repo.object_name(self.job),
            Body=self.buffer,
            ContentLength=size,
            ContentType=self.repo.mimetype,
        )

    async def write(self, data: bytes):
        self.buffer.write(data)


class S3BucketReader:
    def __init__(self, body):
        self.body = body

    async def close(self):
        self.body.close()

    async def read(self, n=None):
        return await self.body.read()

    async def __aenter__(self):
        await self.body.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.body.__aexit__(exc_type, exc_val, exc_tb)


class S3BucketInfo:
    def __init__(self, endpoint: str, uri: str):
        self.endpoint = endpoint
        self.uri = uri

    def __str__(self):
        return self.uri


class S3BucketRepository(BlobRepository):
    def __init__(
        self,
        client: Callable[[], S3Client],
        bucket: str,
        prefix: str = "",
        extension: str = "",
        mimetype: str = "application/octet-stream",
        incluster_endpoint: Optional[str] = None,
    ):
        self._client = client
        self.bucket = bucket
        self.prefix = prefix
        self.extension = extension
        self.mimetype = mimetype
        self.incluster_endpoint = incluster_endpoint

    @property
    def client(self):
        return self._client()

    def __repr__(self):
        return f"<{type(self).__name__} {self.bucket}/{self.prefix}*{self.extension}>"

    async def contains(self, item):
        try:
            await self.client.head_object(Bucket=self.bucket, Key=self.object_name(item))
        except botocore.exceptions.ClientError:
            return False
        else:
            return True

    async def _unfiltered_iter(self):
        paginator = self.client.get_paginator("list_objects")
        async for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(self.extension):
                    yield obj["Key"][len(self.prefix) : -len(self.extension) if self.extension else None]

    async def validate(self):
        try:
            await self.client.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            if "404" in str(e):
                await self.client.create_bucket(Bucket=self.bucket)
            else:
                raise

    def object_name(self, ident):
        return f"{self.prefix}{ident}{self.extension}"

    @job_getter
    async def open(self, ident, mode="r"):
        if mode == "wb":
            return S3BucketBinaryWriter(self, ident)
        elif mode == "w":
            return AWriteText(S3BucketBinaryWriter(self, ident))
        elif mode == "rb":
            return S3BucketReader(
                (await self.client.get_object(Bucket=self.bucket, Key=self.object_name(ident)))["Body"]
            )
        elif mode == "r":
            return AReadText(
                S3BucketReader((await self.client.get_object(Bucket=self.bucket, Key=self.object_name(ident)))["Body"])
            )
        else:
            raise ValueError(mode)

    @job_getter
    async def info(self, ident):
        return S3BucketInfo(
            self.incluster_endpoint or self.client._endpoint.host,
            f"s3://{self.bucket}/{self.object_name(ident)}",
        )

    async def delete(self, key):
        await self.client.delete_object(Bucket=self.bucket, Key=self.object_name(key))


class MongoMetadataRepository(MetadataRepository):
    def __init__(
        self,
        collection: Callable[[], motor.motor_asyncio.AsyncIOMotorCollection],
        subcollection: Optional[str],
    ):
        self._collection = collection
        self._subcollection = subcollection

    def __repr__(self):
        return f"<{type(self).__name__} {self._subcollection}>"

    @property
    def collection(self):
        result = self._collection()
        if self._subcollection is not None:
            result = result[self._subcollection]
        return result

    async def contains(self, item):
        return await self.collection.count_documents({"_id": item}) != 0

    async def delete(self, key):
        await self.collection.delete_one({"_id": key})

    async def _unfiltered_iter(self):
        async for x in self.collection.find({}, projection=[]):
            yield x["_id"]

    @job_getter
    async def info(self, job):
        result = await self.collection.find_one({"_id": job})
        if result is None:
            result = {}
        return result

    async def info_all(self) -> Dict[str, Any]:
        return {entry["_id"]: entry async for entry in self.collection.find({})}

    @job_getter
    async def dump(self, job, data):
        await self.collection.replace_one({"_id": job}, data, upsert=True)


class DockerRepository(Repository):
    """
    A docker repository is, well, an actual docker repository hosted in some registry somewhere. Keys translate to tags
    on this repository.
    """

    def __init__(
        self,
        registry: Callable[[], docker_registry_client_async.DockerRegistryClientAsync],
        domain: str,
        repository: str,
    ):
        self._registry = registry
        self.domain = domain
        self.repository = repository

    @property
    def registry(self):
        return self._registry()

    async def _unfiltered_iter(self):
        try:
            image = docker_registry_client_async.ImageName(self.repository, endpoint=self.domain)
            tags = (await self.registry.get_tags(image)).tags["tags"]
            if tags is None:
                return
            for tag in tags:
                yield tag
        except Exception as e:
            if "404" in str(e):
                return
            else:
                raise

    def __repr__(self):
        return f"<DockerRepository {self.domain}/{self.repository}:*>"

    @job_getter
    async def info(self, job):
        return {
            "withdomain": f"{self.domain}/{self.repository}:{job}",
            "withoutdomain": f"{self.repository}:{job}",
        }

    def _dxf_auth(self, dxf_obj, response):
        # what a fucking hack
        for pattern, credentials in self.registry.credentials.items():
            if pattern.fullmatch(self.domain):
                result = credentials
                break
        else:
            raise PermissionError("Missing credentials for %s" % self.domain)
        if self.registry.ssl:
            username, password = base64.b64decode(result).decode().split(":")
            dxf_obj.authenticate(username, password, response)
        else:
            dxf_obj._headers = {"Authorization": "Basic " + result}

    async def delete(self, key):
        if not await self.contains(key):
            return

        self._delete_inner(key)  # blocking! epic fail

    def _delete_inner(self, key):
        random_data = os.urandom(16)
        random_digest = "sha256:" + hashlib.sha256(random_data).hexdigest()

        d = dxf.DXF(
            host=self.domain,
            repo=self.repository,
            auth=self._dxf_auth,
            insecure=not self.registry.ssl,
        )
        d.push_blob(data=random_data, digest=random_digest)
        d.set_alias(key, random_digest)
        d.del_alias(key)


class LiveKubeRepository(Repository):
    """
    A repository where keys translate to `job` labels on running kube pods.
    """

    def __init__(self, task: "KubeTask"):
        self.task = task

    async def _unfiltered_iter(self):
        for pod in await self.pods():
            yield pod.metadata.labels["job"]

    async def contains(self, item):
        return bool(await self.task.podman.query(task=self.task.name, job=item))

    def __repr__(self):
        return f"<LiveKubeRepository task={self.task.name}>"

    @job_getter
    async def info(self, job):
        # Cannot template with live kube info. Implement this if you have something in mind.
        return None

    async def pods(self):
        return await self.task.podman.query(task=self.task.name)

    async def delete(self, key):
        pods = await self.task.podman.query(job=key, task=self.task.name)
        for pod in pods:  # there... really should be only one
            await self.task.podman.delete(pod)
        # while await self.task.podman.query(job=key, task=self.task.name):
        #    await asyncio.sleep(0.2)


class AggregateAndRepository(Repository):
    """
    A repository which is said to contain a key if all its children also contain that key
    """

    def __init__(self, **children: Repository):
        assert children
        self.children = children

    async def _unfiltered_iter(self):
        counting = Counter()
        async for item in roundrobin([child._unfiltered_iter() for child in self.children.values()]):
            counting[item] += 1
            if counting[item] == len(self.children):
                yield item

    async def contains(self, item):
        for child in self.children.values():
            if not await child.contains(item):
                return False
        return True

    @job_getter
    async def info(self, job):
        return {name: await child.info(job) for name, child in self.children.items()}

    async def delete(self, key):
        for child in self.children.values():
            await child.delete(key)


class AggregateOrRepository(Repository):
    """
    A repository which is said to contain a key if any of its children also contain that key
    """

    def __init__(self, **children: Repository):
        assert children
        self.children = children

    async def _unfiltered_iter(self):
        seen = set()
        for child in self.children.values():
            async for item in child._unfiltered_iter():
                if item in seen:
                    continue
                seen.add(item)
                yield item

    async def contains(self, item):
        for child in self.children.values():
            if await child.contains(item):
                return True
        return False

    @job_getter
    async def info(self, job):
        return {name: await child.info(job) for name, child in self.children.items()}

    async def delete(self, key):
        for child in self.children.values():
            await child.delete(key)


class BlockingRepository(Repository):
    """
    A class that is said to contain a key if `source` contains it and `unless` does not contain it
    """

    def __init__(self, source: Repository, unless: Repository, enumerate_unless=True):
        self.source = source
        self.unless = unless
        self.enumerate_unless = enumerate_unless

    async def _unfiltered_iter(self):
        if self.enumerate_unless:
            blocked = set()
            async for x in self.unless._unfiltered_iter():
                blocked.add(x)
        else:
            blocked = None
        async for item in self.source._unfiltered_iter():
            if self.enumerate_unless and item in blocked:
                continue
            if not self.enumerate_unless and self.unless.contains(item):
                continue
            yield item

    async def contains(self, item):
        return await self.source.contains(item) and not await self.unless.contains(item)

    @job_getter
    async def info(self, job):
        return await self.source.info(job)

    async def delete(self, key):
        await self.source.delete(key)


class YamlMetadataRepository(BlobRepository, MetadataRepository):
    """
    A metadata repository. When info is accessed, it will **load the target file into memory**, parse it as yaml, and
    return the resulting object.
    """

    @job_getter
    async def info(self, job):
        async with await self.open(job, "rb") as fp:
            s = await fp.read()
        return yaml.safe_load(s)

    @job_getter
    async def dump(self, job, data):
        s = yaml.safe_dump(data, None)
        async with await self.open(job, "w") as fp:
            await fp.write(s)


class YamlMetadataFileRepository(YamlMetadataRepository, FileRepository):
    def __init__(self, filename, extension=".yaml", case_insensitive=False):
        super().__init__(filename, extension=extension, case_insensitive=case_insensitive)


class YamlMetadataS3Repository(YamlMetadataRepository, S3BucketRepository):
    def __init__(self, client, bucket, prefix, extension=".yaml", mimetype="text/yaml"):
        super().__init__(client, bucket, prefix, extension=extension, mimetype=mimetype)

    @job_getter
    async def info(self, job):
        try:
            return await super().info(job)
        except botocore.exceptions.ClientError as e:
            if "NoSuchKey" in str(e):
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
        prefetch_lookup=True,
    ):
        self.base_repository = base_repository
        self.translator_repository = translator_repository
        self.allow_deletes = allow_deletes
        self.prefetch_lookup_setting = prefetch_lookup
        self.prefetch_lookup = None

    def __repr__(self):
        return f"<{type(self).__name__} {self.base_repository} by {self.translator_repository}>"

    async def _lookup(self, item):
        if self.prefetch_lookup is None and self.prefetch_lookup_setting:
            self.prefetch_lookup = await self.translator_repository.info_all()
        if self.prefetch_lookup:
            return self.prefetch_lookup.get(item)
        else:
            return await self.translator_repository.info(item)

    async def contains(self, item):
        basename = await self._lookup(item)
        if basename is None:
            return False
        return await self.base_repository.contains(basename)

    async def delete(self, key):
        if not self.allow_deletes:
            return

        basename = await self._lookup(key)
        if basename is None:
            return

        await self.base_repository.delete(basename)

    @job_getter
    async def info(self, ident):
        basename = await self._lookup(ident)
        if basename is None:
            raise LookupError(ident)

        return await self.base_repository.info(basename)

    def __getattr__(self, item):
        v = getattr(self.base_repository, item)
        if not getattr(v, "is_job_getter", False):
            return v

        async def inner(job, *args, **kwargs):
            basename = await self._lookup(job)
            if basename is None:
                raise LookupError(job)
            return await v(basename, *args, **kwargs)

        return inner

    async def _unfiltered_iter(self):
        base_contents = {x async for x in self.base_repository}
        async for item in self.translator_repository:
            basename = await self._lookup(item)
            if basename is not None and basename in base_contents:
                yield item


class ExecutorLiveRepo(Repository):
    def __init__(self, task: "ExecutorTask"):
        self.task = task

    def __repr__(self):
        return f"<{type(self).__name__} task={self.task.name}>"

    async def _unfiltered_iter(self):
        for job in self.task.rev_jobs:
            yield job

    async def contains(self, item):
        return item in self.task.rev_jobs

    async def delete(self, key):
        await self.task.cancel(key)

    async def info(self, ident):
        return None


class InProcessMetadataRepository(MetadataRepository):
    def __init__(self, data: Optional[Dict[str, Any]] = None):
        self.data: Dict[str, Any] = data if data is not None else {}

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @job_getter
    async def info(self, job):
        return self.data.get(job)

    @job_getter
    async def dump(self, job, data):
        self.data[job] = data

    async def contains(self, item):
        return item in self.data

    async def delete(self, key):
        del self.data[key]

    async def _unfiltered_iter(self):
        for key in self.data:
            yield key


class InProcessBlobStream:
    def __init__(self, repo: "InProcessBlobRepository", job: str):
        self.repo = repo
        self.job = job
        self.data = io.BytesIO(repo.data.get(job, b""))

    async def read(self, n: Optional[int] = None) -> bytes:
        return self.data.read(n)

    async def write(self, data: bytes):
        self.data.write(data)

    async def close(self):
        self.repo.data[self.job] = self.data.getvalue()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class InProcessBlobRepository(BlobRepository):
    def __init__(self, data: Optional[Dict[str, bytes]] = None):
        self.data = data if data is not None else {}

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @job_getter
    async def info(self, job):
        # not... sure what to put here
        return None

    @job_getter
    async def open(self, ident, mode="r"):
        stream = InProcessBlobStream(self, ident)
        if mode == "r":
            return AReadText(stream)
        elif mode == "w":
            return AWriteText(stream)
        else:
            return stream

    async def _unfiltered_iter(self):
        for item in self.data:
            yield item

    async def contains(self, item):
        return item in self.data

    async def delete(self, key):
        del self.data[key]
