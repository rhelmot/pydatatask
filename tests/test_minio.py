import asyncio
import unittest
import shutil
import os
import random
import string
import aiobotocore.session

import pydatatask


def rid(n=6):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))


class TestMinio(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

        self.docker_name = None
        self.docker_path = shutil.which("docker")
        self.minio_endpoint = os.getenv("PYDATATASK_TEST_MINIO_ENDPOINT")
        self.minio_username = os.getenv("PYDATATASK_TEST_MINIO_USERNAME", "minioadmin")
        self.minio_password = os.getenv("PYDATATASK_TEST_MINIO_PASSWORD", "minioadmin")
        self.minio_secure = os.getenv("PYDATATASK_MINIO_SECURE", "0").lower() not in (
            "0",
            "",
            "false",
        )
        self.test_id = rid()
        self.client = None
        self.bucket = "test-pydatatask-" + self.test_id

    async def asyncSetUp(self):
        if self.minio_endpoint is None:
            if self.docker_path is None:
                raise unittest.SkipTest("No minio endpoint configured and docker is not installed")
            port = random.randrange(0x4000, 0x8000)
            p = await asyncio.create_subprocess_exec(
                self.docker_path,
                "run",
                "--rm",
                "--name",
                self.bucket,
                "-d",
                "-p",
                f"{port}:9000",
                "minio/minio:latest",
                "server",
                "/data",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await p.communicate()
            if await p.wait() != 0:
                raise unittest.SkipTest("No minio endpoint configured and docker failed to launch minio/minio:latest")
            self.minio_endpoint = f"localhost:{port}"
            self.docker_name = self.bucket
            await asyncio.sleep(1)
        minio_session = aiobotocore.session.get_session()
        self.client = await minio_session.create_client(
            "s3",
            endpoint_url="http://" + self.minio_endpoint,
            aws_access_key_id=self.minio_username,
            aws_secret_access_key=self.minio_password,
        ).__aenter__()

    async def test_minio(self):
        repo = pydatatask.S3BucketRepository(lambda: self.client, self.bucket, prefix="weh/", extension=".weh")
        await repo.validate()
        async with await repo.open("foo", "w") as fp:
            await fp.write("hello world")
        async with (await self.client.get_object(Bucket=self.bucket, Key="weh/foo.weh"))["Body"] as fp:
            assert await fp.read() == b"hello world"
        async with await repo.open("foo", "rb") as fp:
            assert await fp.read() == b"hello world"
        async for ident in repo:
            assert ident == "foo"
            break
        else:
            assert False, "there should be one key"

    async def asyncTearDown(self):
        if self.client is not None:
            await self.client.delete_objects(
                Bucket=self.bucket,
                Delete={
                    "Objects": [
                        {"Key": obj["Key"]}
                        for obj in (await self.client.list_objects(Bucket=self.bucket)).get("Contents", [])
                    ]
                },
            )
            await self.client.delete_bucket(Bucket=self.bucket)
            await self.client.close()

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
