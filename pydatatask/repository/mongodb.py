from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, overload

import motor.core
import motor.motor_asyncio

from .base import MetadataRepository, job_getter


class MongoMetadataRepository(MetadataRepository):
    """
    A metadata repository using a mongodb collection as the backing store.
    """

    def __init__(
        self,
        collection: Callable[[], motor.core.AgnosticCollection],
        subcollection: Optional[str],
    ):
        """
        :param collection: A callable returning a motor async collection.
        :param subcollection: Optional: the name of a subcollection within the collection in which to store data.
        """
        self._collection = collection
        self._subcollection = subcollection

    def __repr__(self):
        return f"<{type(self).__name__} {self._subcollection}>"

    @property
    def collection(self) -> motor.core.AgnosticCollection:
        """
        The motor async collection data will be stored in. If this is provided by an unopened session, raise an error.
        """
        result = self._collection()
        if self._subcollection is not None:
            result = result[self._subcollection]
        return result

    async def contains(self, item):
        return await self.collection.count_documents({"_id": item}) != 0

    async def delete(self, job):
        await self.collection.delete_one({"_id": job})

    async def unfiltered_iter(self):
        async for x in self.collection.find({}, projection=[]):
            yield x["_id"]

    @job_getter
    async def info(self, job):
        """
        The info of a mongo metadata repository is the literal value stored in the repository with identifier ``job``.
        """
        result = await self.collection.find_one({"_id": job})
        if result is None:
            result = {}
        return result

    async def info_all(self) -> Dict[str, Any]:
        return {entry["_id"]: entry async for entry in self.collection.find({})}

    @job_getter
    async def dump(self, job, data):
        if not self.is_valid_job_id(job):
            raise KeyError(job)
        await self.collection.replace_one({"_id": job}, data, upsert=True)
