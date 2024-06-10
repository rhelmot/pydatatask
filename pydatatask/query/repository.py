"""High-level interfaces for accessing query results in the form of repositories."""

from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
from pathlib import Path

from pydatatask.repository import FilesystemRepository, MetadataRepository, Repository
from pydatatask.repository.filesystem import FilesystemType
from pydatatask.utils import AReadStreamBase, AWriteStreamBase

from .parser import QueryValueType
from .query import Query


class QueryRepository(Repository):
    """A QueryRepository is a repository which uses a query expression to generate the keys and values available."""

    def __init__(
        self,
        query: str,
        getters: Optional[Dict[str, QueryValueType]] = None,
        repos: Optional[Dict[str, Repository]] = None,
        jq: Optional[Dict[str, str]] = None,
    ):
        super().__init__()
        self.query = Query(QueryValueType.Repository, query, {}, getters or {}, repos or {}, jq or {})
        self._cached: Optional[Repository] = None

    def footprint(self):
        for repo in self.query.repos.values():
            yield from repo.footprint()

    def cache_flush(self):
        self._cached = None

    async def _resolve(self) -> Repository:
        if self._cached is None:
            repo = await self.query.execute({})
            assert repo.type == QueryValueType.Repository
            assert repo.repo_value is not None
            self._cached = repo.repo_value
        return self._cached

    async def contains(self, key: str, /):
        return await (await self._resolve()).contains(key)

    async def unfiltered_iter(self):
        async for key in await self._resolve():
            yield key

    async def delete(self, key: str, /):
        return await (await self._resolve()).delete(key)

    def __getstate__(self):
        return (self.query,)


class QueryMetadataRepository(QueryRepository, MetadataRepository):
    """A QueryMetadataRepository is just a QueryRepository but additionally a MetadataRepository."""

    async def _resolve(self) -> MetadataRepository:
        result = await super()._resolve()
        assert isinstance(result, MetadataRepository)
        return result

    async def info(self, key: str, /):
        return await (await self._resolve()).info(key)

    async def info_all(self):
        return await (await self._resolve()).info_all()

    async def dump(self, key: str, data: Any, /):
        return await (await self._resolve()).dump(key, data)


class QueryFilesystemRepository(QueryRepository, FilesystemRepository):
    """A QueryMetadataRepository is just a QueryRepository but additionally a FilesystemRepository."""

    async def _resolve(self) -> FilesystemRepository:
        result = await super()._resolve()
        assert isinstance(result, FilesystemRepository)
        return result

    async def dump(self, job: str):
        thing = (await self._resolve()).dump(job)
        try:
            while True:
                entry = yield
                await thing.asend(entry)
        except StopAsyncIteration:
            pass

    async def get_mode(self, job: str, path: str) -> Optional[int]:
        return await (await self._resolve()).get_mode(job, path)

    async def get_regular_meta(self, job: str, path: str) -> Tuple[int, Optional[str]]:
        return await (await self._resolve()).get_regular_meta(job, path)

    async def get_type(self, job: str, path: str) -> Optional[FilesystemType]:
        return await (await self._resolve()).get_type(job, path)

    async def open(self, job: str, path: Union[str, Path]) -> AsyncContextManager[AReadStreamBase]:
        return await (await self._resolve()).open(job, path)

    async def readlink(self, job: str, path: str) -> str:
        return await (await self._resolve()).readlink(job, path)

    async def walk(self, job: str) -> AsyncIterator[Tuple[str, List[str], List[str], List[str]]]:
        async for a, b, c, d in (await self._resolve()).walk(job):
            yield a, b, c, d

    async def dump_tarball(self, job: str, stream: AReadStreamBase) -> None:
        return await (await self._resolve()).dump_tarball(job, stream)

    async def get_tarball(self, job: str, dest: AWriteStreamBase) -> None:
        return await (await self._resolve()).get_tarball(job, dest)
