"""
Repositories are arbitrary key-value stores. They are the data part of pydatatask.
You can store your data in any way you desire and as long as you can write a Repository class to describe it, it can be
used to drive a pipeline.

The notion of the "value" part of the key-value store abstraction is defined very, very loosely. The repository base
class doesn't have an interface to get or store values, only to query for and delete keys. Instead, you have to know
which repository subclass you're working with, and use its interfaces. For example, `MetadataRepository` assumes that
its values are structured objects and loads them fully into memory, and `BlobRepository` provides a streaming interface
to a flat address space.
"""
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    AsyncIterable,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    List,
    Literal,
    Optional,
    overload,
)
from abc import ABC, abstractmethod
from collections import Counter
from pathlib import Path
import inspect
import io
import logging
import os
import string

import aiofiles.os
import aiohttp.client_exceptions
import aioshutil
import yaml

if TYPE_CHECKING:
    from ..task import ExecutorTask, KubeTask

l = logging.getLogger(__name__)

from ..utils import AReadStream, AReadText, AWriteStream, AWriteText, roundrobin


# Helper Functions
def job_getter(f):
    """
    Use this function to annotate non-abstract methods which take a job identifier as their first parameter. This is
    used by RelatedItemRepository to automatically translate job identifiers to related ones.
    """
    if not inspect.iscoroutinefunction(f):
        raise TypeError("only async functions can be job_getters")
    f.is_job_getter = True
    return f


# Base Classes
class Repository(ABC):
    """
    A repository is a key-value store where the keys are names of jobs. Since the values have unspecified semantics, the
    only operations you can do on a generic repository are query for keys.

    A repository can be async-iterated to get a listing of its members.
    """

    CHARSET = CHARSET_START_END = string.ascii_letters + string.digits

    @classmethod
    def is_valid_job_id(cls, job: str):
        """
        Determine whether the given job identifier is valid, i.e. that it contains only valid characters
        (numbers and letters by default).
        """
        return (
            0 < len(job) < 64
            and all(c in cls.CHARSET for c in job)
            and job[0] in cls.CHARSET_START_END
            and job[-1] in cls.CHARSET_START_END
        )

    async def filter_jobs(self, iterator: AsyncIterable[str]) -> AsyncIterable[str]:
        """
        Apply `is_valid_job_id` as a filter to an async iterator.
        """
        async for job in iterator:
            if self.is_valid_job_id(job):
                yield job
            else:
                l.warning("Skipping %s %s - not a valid job id", self, repr(job))

    async def contains(self, item):
        """
        Determine whether the given job identifier is present in this repository.

        The default implementation is quite inefficient; please override this if possible.
        """
        async for x in self:
            if x == item:
                return True
        return False

    def __aiter__(self):
        return self.filter_jobs(self.unfiltered_iter())

    @abstractmethod
    async def unfiltered_iter(self) -> AsyncGenerator[str, None]:
        """
        The core method of Repository. Implement this to produce an iterable of every string which could potentially
        be a job identifier present in this repository. When the repository is iterated directly, this will be filtered
        by `filter_jobs`.
        """
        raise NotImplementedError
        # noinspection PyUnreachableCode
        yield None  # pylint: disable=unreachable

    @abstractmethod
    async def info(self, job) -> Any:
        """
        Returns an arbitrary piece of data related to job. Notably, this is used during templating.
        This should do something meaningful even if the repository does not contain the requested job.
        """
        raise NotImplementedError

    @abstractmethod
    async def delete(self, job):
        """
        Delete the given job from the repository. This should succeed even if the job is not present in this repository.
        """
        raise NotImplementedError

    async def info_all(self) -> Dict[str, Any]:
        """
        Produce a mapping from every job present in the repository to its corresponding info. The default implementation
        is somewhat inefficient; please override it if there is a more effective way to load all info.
        """
        return {job: await self.info(job) async for job in self}

    async def validate(self):
        """
        Override this method to raise an exception if for any reason the repository is misconfigured. This will be
        automatically called by the pipeline on opening.
        """

    def map(
        self, func: Callable, filt: Optional[Callable[[str], Awaitable[bool]]] = None, allow_deletes=False
    ) -> "MapRepository":
        """
        Generate a :class:`MapRepository` based on this repository and the given parameters.
        """
        return MapRepository(self, func, filt, allow_deletes=allow_deletes)


class MapRepository(Repository):
    """
    A MapRepository is a repository which uses arbitrary functions to map and filter results from a base repository.
    """

    def __init__(
        self,
        base: Repository,
        func: Callable[[Any], Coroutine[None, None, Any]],
        filt: Optional[Callable[[str], Awaitable[bool]]] = None,
        allow_deletes=False,
    ):
        """
        :param func: The function to use to translate the base repository's `info` results into the mapped `info`
                     results.
        :param filt: Optional: An async function to use to determine whether a given key should be considered part of
                     the mapped repository.
        :param allow_deletes: Whether the delete operation will do anything on the mapped repository.
        """
        self.base = base
        self.func = func
        self.filter = filt
        self.allow_deletes = allow_deletes

    async def contains(self, item):
        if self.filter is None or await self.filter(item):
            return await self.base.contains(item)
        return False

    async def delete(self, job):
        if self.allow_deletes:
            await self.base.delete(job)

    async def unfiltered_iter(self):
        async for item in self.base.unfiltered_iter():
            if self.filter is None or await self.filter(item):
                yield item

    async def info(self, job):
        return await self.func(await self.base.info(job))

    async def info_all(self) -> Dict[str, Any]:
        result = await self.base.info_all()
        to_remove = []
        for k, v in result.items():
            if self.filter is None or await self.filter(k):
                result[k] = await self.func(v)
            else:
                to_remove.append(k)
        for k in to_remove:
            result.pop(k)
        return result


class MetadataRepository(Repository, ABC):
    """
    A metadata repository has values which are small, structured data, and loads them entirely into memory, returning
    the structured data from the `info` method.
    """

    @abstractmethod
    async def info(self, job):
        """
        Retrieve the data with key ``job`` from the repository.
        """
        raise NotImplementedError

    @abstractmethod
    async def dump(self, job, data):
        """
        Insert ``data`` into the repository with key ``job``.
        """
        raise NotImplementedError


class BlobRepository(Repository, ABC):
    """
    A blob repository has values which are flat data blobs that can be streamed for reading or writing.
    """

    @overload
    async def open(self, job: str, mode: Literal["r"]) -> AReadText:
        ...

    @overload
    async def open(self, job: str, mode: Literal["rb"]) -> AReadStream:
        ...

    @overload
    async def open(self, job: str, mode: Literal["w"]) -> AWriteText:
        ...

    @overload
    async def open(self, job: str, mode: Literal["wb"]) -> AWriteStream:
        ...

    @abstractmethod
    async def open(self, job, mode="r"):
        """
        Open the given job's value as a stream for reading or writing, in text or binary mode.
        """
        raise NotImplementedError


class FileRepositoryBase(Repository, ABC):
    """
    A file repository is a local directory where each job identifier is a filename, optionally suffixed with an
    extension before hitting the filesystem. This is an abstract base class for other file repositories which have more
    to say about what is found at these filepaths.
    """

    def __init__(self, basedir, extension="", case_insensitive=False):
        self.basedir = Path(basedir)
        self.extension = extension
        self.case_insensitive = case_insensitive

    async def contains(self, item):
        return await aiofiles.os.path.exists(self.basedir / (item + self.extension))

    def __repr__(self):
        return f'<{type(self).__name__} {self.basedir / ("*" + self.extension)}>'

    async def unfiltered_iter(self):
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

    def fullpath(self, job) -> Path:
        """
        Construct the full local path of the file corresponding to ``job``.
        """
        return self.basedir / (job + self.extension)

    @job_getter
    async def info(self, job):
        """
        The templating info provided by a file repository is the full path to the corresponding file as a string.
        """
        return str(self.fullpath(job))


class FileRepository(FileRepositoryBase, BlobRepository):  # BlobFileRepository?
    """
    A file repository whose members are files, treated as streamable blobs.
    """

    @job_getter
    async def open(self, job, mode="r"):
        if not self.is_valid_job_id(job):
            raise KeyError(job)
        return aiofiles.open(self.fullpath(job), mode) # type: ignore

    async def delete(self, job):
        try:
            await aiofiles.os.unlink(self.fullpath(job))
        except FileNotFoundError:
            pass


class DirectoryRepository(FileRepositoryBase):
    """
    A file repository whose members are directories.
    """

    def __init__(self, *args, discard_empty=False, **kwargs):
        """
        :param discard_empty: Whether only directories containing at least one member should be considered as "present"
                              in the repository.
        """
        super().__init__(*args, **kwargs)
        self.discard_empty = discard_empty

    @job_getter
    async def mkdir(self, job):
        """
        Create an empty directory corresponding to ``job``. Do nothing if the directory already exists.
        """
        try:
            await aiofiles.os.mkdir(self.fullpath(job))
        except FileExistsError:
            pass

    async def delete(self, job):
        if await self.contains(job):
            await aioshutil.rmtree(self.fullpath(job))

    async def contains(self, item):
        result = await super().contains(item)
        if not self.discard_empty:
            return result
        if not result:
            return False
        return bool(list(await aiofiles.os.listdir(self.fullpath(item))))

    async def unfiltered_iter(self):
        async for item in super().unfiltered_iter():
            if self.discard_empty:
                if bool(list(await aiofiles.os.listdir(self.fullpath(item)))):
                    yield item
            else:
                yield item


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
        """
        :param base_repository: The repository from which to return results based on translated keys. The resulting
                                repository will duck-type as the same type as the base.
        :param translator_repository: A repository whose info() will be used to translate keys:
                                      ``info(job) == translated_job``.
        :param allow_deletes: Whether the delete operation on this repository does anything. If enabled, it will delete
                              only from the base repository.
        :param prefetch_lookup: Whether to cache the entirety of the translator repository in memory to improve
                                performance.
        """
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

    async def delete(self, job):
        if not self.allow_deletes:
            return

        basename = await self._lookup(job)
        if basename is None:
            return

        await self.base_repository.delete(basename)

    @job_getter
    async def info(self, job):
        basename = await self._lookup(job)
        if basename is None:
            raise LookupError(job)

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

    async def unfiltered_iter(self):
        base_contents = {x async for x in self.base_repository}
        async for item in self.translator_repository:
            basename = await self._lookup(item)
            if basename is not None and basename in base_contents:
                yield item


class AggregateAndRepository(Repository):
    """
    A repository which is said to contain a job if all its children also contain that job
    """

    def __init__(self, **children: Repository):
        assert children
        self.children = children

    async def unfiltered_iter(self):
        counting = Counter()
        async for item in roundrobin([child.unfiltered_iter() for child in self.children.values()]):
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
        """
        The info provided by an aggregate And repository is a dict mapping each child's name to that child's info.
        """
        return {name: await child.info(job) for name, child in self.children.items()}

    async def delete(self, job):
        """
        Deleting a job from an aggregate And repository deletes the job from all of its children.
        """
        for child in self.children.values():
            await child.delete(job)


class AggregateOrRepository(Repository):
    """
    A repository which is said to contain a job if any of its children also contain that job
    """

    def __init__(self, **children: Repository):
        assert children
        self.children = children

    async def unfiltered_iter(self):
        seen = set()
        for child in self.children.values():
            async for item in child.unfiltered_iter():
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
        """
        The info provided by an aggregate Or repository is a dict mapping each child's name to that child's info.
        """
        return {name: await child.info(job) for name, child in self.children.items()}

    async def delete(self, job):
        """
        Deleting a job from an aggregate Or repository deletes the job from all of its children.
        """
        for child in self.children.values():
            await child.delete(job)


class BlockingRepository(Repository):
    """
    A class that is said to contain a job if ``source`` contains it and ``unless`` does not contain it
    """

    def __init__(self, source: Repository, unless: Repository, enumerate_unless=True):
        self.source = source
        self.unless = unless
        self.enumerate_unless = enumerate_unless

    async def unfiltered_iter(self):
        if self.enumerate_unless:
            blocked = set()
            async for x in self.unless.unfiltered_iter():
                blocked.add(x)
        else:
            blocked = None
        async for item in self.source.unfiltered_iter():
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

    async def delete(self, job):
        await self.source.delete(job)


class YamlMetadataRepository(BlobRepository, MetadataRepository, ABC):
    """
    A metadata repository based on a blob repository. When info is accessed, it will **load the target file into
    memory**, parse it as yaml, and return the resulting object.

    This is a base class, and must be overridden to implement the blob loading portion.
    """

    @job_getter
    async def info(self, job):
        async with await self.open(job, "rb") as fp:
            s = await fp.read()
        return yaml.safe_load(s)

    @job_getter
    async def dump(self, job, data):
        if not self.is_valid_job_id(job):
            raise KeyError(job)
        s = yaml.safe_dump(data, None)
        async with await self.open(job, "w") as fp:
            await fp.write(s)


class YamlMetadataFileRepository(YamlMetadataRepository, FileRepository):
    """
    A metadata repository based on a file blob repository.
    """

    def __init__(self, basedir, extension=".yaml", case_insensitive=False):
        super().__init__(basedir, extension=extension, case_insensitive=case_insensitive)


class ExecutorLiveRepo(Repository):
    """
    A repository where keys translate to running jobs in an ExecutorTask. This repository is constructed automatically
    and is linked as the ``live`` repository. Do not construct this class manually.
    """

    def __init__(self, task: "ExecutorTask"):
        self.task = task

    def __repr__(self):
        return f"<{type(self).__name__} task={self.task.name}>"

    async def unfiltered_iter(self):
        for job in self.task.rev_jobs:
            yield job

    async def contains(self, item):
        return item in self.task.rev_jobs

    async def delete(self, job):
        """
        Deleting a job from the repository will cancel the corresponding task.
        """
        await self.task.cancel(job)

    async def info(self, job):
        """
        There is no templating info for an `ExecutorLiveRepo`.
        """
        return None


class InProcessMetadataRepository(MetadataRepository):
    """
    An incredibly simple metadata repository which stores all its values in a dict, and will let them vanish when the
    process terminates.
    """

    def __init__(self, data: Optional[Dict[str, Any]] = None):
        self.data: Dict[str, Any] = data if data is not None else {}

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @job_getter
    async def info(self, job):
        return self.data.get(job)

    @job_getter
    async def dump(self, job, data):
        if not self.is_valid_job_id(job):
            raise KeyError(job)
        self.data[job] = data

    async def contains(self, item):
        return item in self.data

    async def delete(self, job):
        del self.data[job]

    async def unfiltered_iter(self):
        for job in self.data:
            yield job


class InProcessBlobStream:
    """
    A stream returned from an `BlobRepository.open` call from `InProcessBlobRepository`. Do not construct this manually.
    """

    def __init__(self, repo: "InProcessBlobRepository", job: str):  # pylint: disable=missing-function-docstring
        self.repo = repo
        self.job = job
        self.data = io.BytesIO(repo.data.get(job, b""))

    async def read(self, n: Optional[int] = None) -> bytes:
        """
        Read up to ``n`` bytes from the stream.
        """
        return self.data.read(n)

    async def write(self, data: bytes):
        """
        Write ``data`` to the stream.
        """
        self.data.write(data)

    async def close(self):
        """
        Close and release the stream, syncing the data back to the repository.
        """
        self.repo.data[self.job] = self.data.getvalue()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class InProcessBlobRepository(BlobRepository):
    """
    An incredibly simple blob repository which stores all its values in a dict, and will let them vanish when the
    process terminates.
    """

    def __init__(self, data: Optional[Dict[str, bytes]] = None):
        self.data = data if data is not None else {}

    def __repr__(self):
        return f"<{type(self).__name__}>"

    @job_getter
    async def info(self, job):
        """
        There is no templating info for an `InProcessBlobRepository`.
        """
        return None

    @job_getter
    async def open(self, job, mode="r"):
        if not self.is_valid_job_id(job):
            raise KeyError(job)
        stream = InProcessBlobStream(self, job)
        if mode == "r":
            return AReadText(stream)
        elif mode == "w":
            return AWriteText(stream)
        else:
            return stream

    async def unfiltered_iter(self):
        for item in self.data:
            yield item

    async def contains(self, item):
        return item in self.data

    async def delete(self, job):
        del self.data[job]
