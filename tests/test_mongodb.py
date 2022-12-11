from typing import Optional
import asyncio
import os
import random
import shutil
import string
import unittest

import motor.motor_asyncio

import pydatatask


def rid(n=6):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))


class TestMongoDB(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

        self.docker_name = None
        self.docker_path = shutil.which("docker")
        self.mongo_url = os.getenv("PYDATATASK_TEST_MONGODB_URL")
        self.test_id = rid()
        self.client: Optional[motor.motor_asyncio.AsyncIOMotorClient] = None
        self.database = "test-pydatatask-" + self.test_id

    async def asyncSetUp(self):
        if self.mongo_url is None:
            if self.docker_path is None:
                raise unittest.SkipTest("No mongodb endpoint configured and docker is not installed")
            port = random.randrange(0x4000, 0x8000)
            self.mongo_url = f"mongodb://root:root@localhost:{port}"
            p = await asyncio.create_subprocess_exec(
                self.docker_path,
                "run",
                "--rm",
                "--name",
                self.database,
                "-d",
                "-p",
                f"{port}:27017",
                "-e",
                "MONGO_INITDB_ROOT_USERNAME=root",
                "-e",
                "MONGO_INITDB_ROOT_PASSWORD=root",
                "mongo:latest",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await p.communicate()
            if await p.wait() != 0:
                raise unittest.SkipTest("No minio endpoint configured and docker failed to launch mongo:latest")
            self.docker_name = self.database
            await asyncio.sleep(1)
        self.client = motor.motor_asyncio.AsyncIOMotorClient(self.mongo_url)

    async def test_mongo(self):
        repo = pydatatask.MongoMetadataRepository(lambda: self.client[self.database], "test")
        assert repr(repo)
        await repo.dump("foo", {"weh": 1})
        assert len([x async for x in repo]) == 1
        assert (await repo.info("foo"))["weh"] == 1
        assert (await self.client[self.database].test.find_one({"_id": "foo"}))["weh"] == 1
        assert await repo.contains("foo")
        assert not await repo.contains("bar")
        all_things = await repo.info_all()
        assert len(all_things) == 1
        assert "foo" in all_things
        assert all_things["foo"]["weh"] == 1

        await repo.delete("foo")
        assert await repo.info("foo") == {}
        assert len([x async for x in repo]) == 0

    async def asyncTearDown(self):
        if self.client is not None:
            await self.client.drop_database(self.database)

        if self.docker_name is not None:
            p = await asyncio.create_subprocess_exec(
                self.docker_path,
                "kill",
                self.docker_name,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await p.communicate()


if __name__ == "__main__":
    unittest.main()
