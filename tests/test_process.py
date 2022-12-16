from pathlib import Path
import asyncio
import base64
import random
import shutil
import string
import unittest

import aioshutil
import asyncssh

import pydatatask


def rid(n=6):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))


class TestLocalProcess(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

        self.app = f"test-{rid()}"
        self.dir = Path(f"/tmp/pydatatask-{self.app}")
        self.n = 50

    async def test_local_process(self):
        session = pydatatask.Session()

        @session.resource
        async def localhost():
            yield pydatatask.LocalLinuxManager(self.app, local_path=self.dir)

        quota = pydatatask.ResourceManager(pydatatask.Resources.parse(1, 1))

        repo_input = pydatatask.FileRepository(self.dir / "input")
        await repo_input.validate()
        for i in range(self.n):
            async with await repo_input.open(str(i), "w") as fp:
                await fp.write(str(i))
        repo_stdout = pydatatask.InProcessBlobRepository()
        repo_done = pydatatask.InProcessMetadataRepository()
        repo_pids = pydatatask.InProcessMetadataRepository()

        template = """\
#!/bin/sh
echo weh | cat {{input}} -
echo bye >&2
        """

        task = pydatatask.ProcessTask(
            "task",
            localhost,
            quota,
            pydatatask.Resources.parse("100m", "100m"),
            repo_pids,
            template,
            {},
            repo_done,
            None,
            repo_stdout,
            pydatatask.STDOUT,
        )
        task.link("input", repo_input, is_input=True)

        pipeline = pydatatask.Pipeline([task], session)

        async with pipeline:
            await pydatatask.run(pipeline, False, False, 120)

        assert len(repo_pids.data) == 0
        assert len(repo_stdout.data) == self.n
        assert len(repo_done.data) == self.n

        for i in range(self.n):
            assert repo_done.data[str(i)]["return_code"] == 0
            assert repo_stdout.data[str(i)] == f"{i}weh\nbye\n".encode()

    async def asyncTearDown(self):
        await aioshutil.rmtree(self.dir, ignore_errors=True)


class TestSSHProcess(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

        self.docker_name = None
        self.docker_path = shutil.which("docker")
        self.test_id = rid()
        self.port = None
        self.n = 50

    async def asyncSetUp(self):
        if self.docker_path is None:
            raise unittest.SkipTest("No mongodb endpoint configured and docker is not installed")
        self.port = random.randrange(0x4000, 0x8000)
        name = f"pydatatask-test-{self.test_id}"
        p = await asyncio.create_subprocess_exec(
            self.docker_path,
            "run",
            "--rm",
            "--name",
            name,
            "-d",
            "-p",
            f"{self.port}:2222",
            "-e",
            "USER_NAME=weh",
            "-e",
            "USER_PASSWORD=weh",
            "-e",
            "PASSWORD_ACCESS=true",
            "linuxserver/openssh-server:latest",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await p.communicate()
        if await p.wait() != 0:
            raise unittest.SkipTest(
                "No minio endpoint configured and docker failed to launch linuxserver/openssh-server:latest"
            )
        self.docker_name = name
        await asyncio.sleep(2)

    async def test_ssh_process(self):
        session = pydatatask.Session()

        @session.resource
        async def ssh():
            async with asyncssh.connect(
                "localhost", port=self.port, username="weh", password="weh", known_hosts=None
            ) as s:
                yield s

        @session.resource
        async def procman():
            yield pydatatask.SSHLinuxManager(self.test_id, ssh)

        quota = pydatatask.ResourceManager(pydatatask.Resources.parse(1, 1))

        repo_stdin = pydatatask.InProcessBlobRepository({str(i): str(i).encode() for i in range(self.n)})
        repo_stdout = pydatatask.InProcessBlobRepository()
        repo_stderr = pydatatask.InProcessBlobRepository()
        repo_done = pydatatask.InProcessMetadataRepository()
        repo_pids = pydatatask.InProcessMetadataRepository()

        template = """\
#!/bin/sh
echo 'hello world!' >&2
echo "The message of day {{job}} is $(base64). That's pretty great!"
echo 'goodbye world!' >&2
        """

        task = pydatatask.ProcessTask(
            "task",
            procman,
            quota,
            pydatatask.Resources.parse("100m", "100m"),
            repo_pids,
            template,
            {},
            repo_done,
            repo_stdin,
            repo_stdout,
            repo_stderr,
        )

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
            assert (
                repo_stdout.data[str(i)]
                == f"The message of day {i} is {base64.b64encode(repo_stdin.data[str(i)]).decode()}. That's pretty great!\n".encode()
            )
            assert repo_done.data[str(i)]["return_code"] == 0

    async def asyncTearDown(self):
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
