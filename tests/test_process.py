import unittest
import random
import string
import base64

import aioshutil

import pydatatask


def rid(n=6):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))

class TestLocalProcess(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

        self.app = f'test-{rid()}'
        self.dir = f'/tmp/pydatatask-{self.app}'
        self.n = 50

    async def test_local_process(self):
        session = pydatatask.Session()

        @session.resource
        async def localhost():
            yield pydatatask.LocalLinuxManager(self.app, local_path=self.dir)

        quota = pydatatask.ResourceManager(pydatatask.Resources.parse(1, 1))

        repo_stdin = pydatatask.InProcessBlobRepository({str(i): str(i).encode() for i in range(self.n)})
        repo_stdout = pydatatask.InProcessBlobRepository()
        repo_stderr = pydatatask.InProcessBlobRepository()
        repo_done = pydatatask.InProcessMetadataRepository()
        repo_pids = pydatatask.InProcessMetadataRepository()

        template = """\
#!/usr/bin/env python3
import sys
import os
import base64

print("hello world!", file=sys.stderr)
print("The message of day {{job}} is " + base64.b64encode(sys.stdin.buffer.read()).decode() + ". That's pretty great!")
print("goodbye world!", file=sys.stderr)
        """

        task = pydatatask.ProcessTask("task", localhost, quota, pydatatask.Resources.parse('100m', '100m'), repo_pids, template, {}, repo_done, repo_stdin, repo_stdout, repo_stderr)

        pipeline = pydatatask.Pipeline([task], session)

        async with pipeline:
            await pydatatask.run(pipeline, False, False, 120)

        assert len(repo_stdin.data) == self.n
        assert len(repo_stdout.data) == self.n
        assert len(repo_stderr.data) == self.n
        assert len(repo_done.data) == self.n
        assert len(repo_pids.data) == 0

        for i in range(self.n):
            assert repo_stderr.data[str(i)] == f"hello world!\ngoodbye world!\n".encode()
            assert repo_stdout.data[str(i)] == f"The message of day {i} is {base64.b64encode(repo_stdin.data[str(i)]).decode()}. That's pretty great!\n".encode()
            assert repo_done.data[str(i)]['return_code'] == 0

    async def asyncTearDown(self):
        await aioshutil.rmtree(self.dir, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
