from typing import Union
import string
from pathlib import Path
import logging
import yaml

import docker_registry_client

l = logging.getLogger(__name__)


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

    def info(self, ident):
        """
        Returns an arbitrary piece of data related to ident. Notably, this is used during templating.
        This should do something meaningful even if the repository does not contain ident.
        """
        raise NotImplementedError

class FileRepository(Repository):
    """
    A file repository is a directory where each key is a filename, optionally suffixed with an extension before hitting
    the filesystem.
    """
    def __init__(self, basedir, extension='', case_insensitive=False):
        self.basedir = Path(basedir)
        self.basedir.mkdir(exist_ok=True, parents=True)
        self.extension = extension
        self.case_insensitive = case_insensitive

    def __contains__(self, item):
        return (self.basedir / (item + self.extension)).exists()

    def __repr__(self):
        return f'<FileRepository {self.basedir / ("*" + self.extension)}>'

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

    def fullpath(self, ident):
        return self.basedir / (ident + self.extension)

    def open(self, ident, mode='r'):
        return open(self.fullpath(ident), mode)

    def mkdir(self, ident):
        self.fullpath(ident).mkdir(exist_ok=True)

    def info(self, job):
        return str(self.fullpath(job))

class DockerRepository(Repository):
    """
    A docker repository is, well, an actual docker repository hosted in some registry somewhere. Keys translate to tags
    on this repository.
    """
    def __init__(self, client: docker_registry_client.DockerRegistryClient, domain: str, repository: str):
        self.client = client
        self.domain = domain
        self.repository = repository

    def _unfiltered_iter(self):
        try:
            return self.client.repository(self.repository).tags()
        except Exception as e:
            if '404' in str(e):
                return []
            else:
                raise

    def __repr__(self):
        return f'<DockerRepository {self.domain}/{self.repository}>'

    def info(self, job):
        return {
            'withdomain': f'{self.domain}/{self.repository}:{job}',
            'withoutdomain': f'{self.repository}:{job}',
        }

class LiveKubeRepository(Repository):
    """
    A repository where keys translate to `job` labels on running kube pods. Pretty heavily tied to the global PodManager
    instance.
    """
    def __init__(self, podman, task):
        self.task = task
        self.podman = podman

    def _unfiltered_iter(self):
        return (pod.metadata.labels['job'] for pod in self.pods())

    def __contains__(self, item):
        return bool(self.podman.query(task=self.task, job=item))

    def __repr__(self):
        return f'<LiveKubeRepository task={self.task}>'

    def info(self, job):
        # Cannot template with live kube info. Implement this if you have something in mind.
        return None

    def pods(self):
        return self.podman.query(task=self.task)

class AggregateAndRepository(Repository):
    """
    A repository which is said to contain a key if all its children also contain that key
    """
    def __init__(self, **children: Repository):
        assert children
        self.children = children

    def _unfiltered_iter(self):
        result = None
        for child in self.children.values():
            if result is None:
                result = set(child._unfiltered_iter())
            else:
                result &= set(child._unfiltered_iter())
        return result

    def __contains__(self, item):
        return all(item in child for child in self.children.values())

    def info(self, job):
        return AggregateRepositoryInfo(self, job)

class AggregateOrRepository(Repository):
    """
    A repository which is said to contain a key if all its children also contain that key
    """
    def __init__(self, **children: Repository):
        assert children
        self.children = children

    def _unfiltered_iter(self):
        result = None
        for child in self.children.values():
            if result is None:
                result = set(child._unfiltered_iter())
            else:
                result |= set(child._unfiltered_iter())
        return result

    def __contains__(self, item):
        return any(item in child for child in self.children.values())

    def info(self, job):
        return AggregateRepositoryInfo(self, job)

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
    def __init__(self, source: Repository, unless: Repository):
        self.source = source
        self.unless = unless

    def _unfiltered_iter(self):
        return set(self.source._unfiltered_iter()) - set(self.unless._unfiltered_iter())

    def __contains__(self, item):
        return item in self.source and not item in self.unless

    def info(self, job):
        return self.source.info(job)

class YamlMetadataRepository(FileRepository):
    """
    A metadata repository. When info is accessed, it will **load the target file into memory**, parse it as yaml, and
    return the resulting object.
    """
    def __init__(self, filename, extension='yaml', case_insensitive=False):
        super().__init__(filename, extension=extension, case_insensitive=case_insensitive)

    def info(self, job):
        with self.open(job, 'r') as fp:
            return yaml.safe_load(fp)
