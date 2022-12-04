import unittest
import tempfile
import aiofiles.os
import aioshutil

import pydatatask

class TestFilesystem(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

        self.dir = None

    async def asyncSetUp(self):
        self.dir = tempfile.mkdtemp()

    async def test_filesystem(self):
        repo = pydatatask.FileRepository(self.dir, extension='.txt')

        async with await repo.open("foo", 'w') as fp:
            await fp.write('hello world')
        async with aiofiles.open(self.dir + '/foo.txt', 'rb') as fp:
            assert await fp.read() == b'hello world'
        async with await repo.open("foo", 'rb') as fp:
            assert await fp.read() == b'hello world'

    async def asyncTearDown(self):
        if self.dir is not None:
            await aioshutil.rmtree(self.dir)

if __name__ == '__main__':
    unittest.main()
