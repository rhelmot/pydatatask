import asyncio
from typing import Optional
import unittest
import shutil
import os
import random
import string
import miniopy_async.deleteobjects

import pydatatask

def rid(n=6):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(n))

class TestMinio(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

        self.docker_name = None
        self.docker_path = shutil.which('docker')
        self.minio_endpoint = os.getenv("PYDATATASK_TEST_MINIO_ENDPOINT")
        self.minio_username = os.getenv("PYDATATASK_TEST_MINIO_USERNAME", "minioadmin")
        self.minio_password = os.getenv("PYDATATASK_TEST_MINIO_PASSWORD", "minioadmin")
        self.minio_secure = os.getenv("PYDATATASK_MINIO_SECURE", "0").lower() not in ('0', '', 'false')
        self.test_id = rid()
        self.client: Optional[miniopy_async.Minio] = None
        self.bucket = 'test-pydatatask-' + self.test_id

    async def asyncSetUp(self):
        if self.minio_endpoint is None:
            if self.docker_path is None:
                raise unittest.SkipTest("No minio endpoint configured and docker is not installed")
            port = random.randrange(0x4000, 0x8000)
            p = await asyncio.create_subprocess_exec(self.docker_path, 'run', '--rm', '--name', self.bucket, '-d', '-p', f'{port}:9000', 'minio/minio:latest', 'server', '/data')
            await p.communicate()
            if await p.wait() != 0:
                raise unittest.SkipTest("No minio endpoint configured and docker failed to launch minio/minio:latest")
            self.minio_endpoint = f'localhost:{port}'
            self.docker_name = self.bucket
            await asyncio.sleep(1)
        self.client = miniopy_async.Minio(self.minio_endpoint, self.minio_username, self.minio_password, secure=self.minio_secure)
        await self.client.make_bucket(self.bucket)

    async def test_minio(self):
        repo = pydatatask.S3BucketRepository(self.client, self.bucket, prefix='weh/', extension='.weh')
        async with await repo.open("foo", 'w') as fp:
            await fp.write('hello world')
        fp = await self.client.get_object(self.bucket, "weh/foo.weh")
        assert await fp.read() == b'hello world'
        fp.close()
        async with await repo.open("foo", 'rb') as fp:
            assert await fp.read() == b'hello world'

    async def asyncTearDown(self):
        if self.client is not None:
            await self.client.remove_objects(self.bucket, [miniopy_async.deleteobjects.DeleteObject(obj.object_name) for obj in await self.client.list_objects(self.bucket, recursive=True)])
            await self.client.remove_bucket(self.bucket)

        if self.docker_name is not None:
            p = await asyncio.create_subprocess_exec(self.docker_path, 'kill', self.docker_name)
            await p.communicate()

if __name__ == '__main__':
    unittest.main()
