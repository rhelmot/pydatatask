"""High-level interfaces for accessing query results in the form of repositories."""

from typing import Any, Dict, Optional

from pydatatask.repository import MetadataRepository, Repository

from .parser import QueryValueType
from .query import Query


class QueryRepository(Repository):
    """A QueryRepository is a repository which uses a query expression to generate the keys and values available."""

    def __init__(
        self,
        query: str,
        getters: Optional[Dict[str, QueryValueType]] = None,
        repos: Optional[Dict[str, Repository]] = None,
    ):
        super().__init__()
        self.query = Query(QueryValueType.Repository, query, {}, getters or {}, repos or {})
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
