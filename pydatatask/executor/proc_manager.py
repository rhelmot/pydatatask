"""A process manager can be used with a ProcessTask to specify where and how the processes should run.

All a process manager needs to specify is how to launch a process and manipulate the filesystem, and the ProcessTask
will set up an appropriate environment for running the task and retrieve the results using this interface.
"""

from typing import (
    TYPE_CHECKING,
    AsyncContextManager,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
    overload,
)
from abc import abstractmethod
from pathlib import Path
import asyncio
import os
import shlex
import signal
import sys

from aiohttp import web
import aiofiles
import aioshutil
import asyncssh
import psutil
import yaml

from pydatatask import agent
from pydatatask.executor import Executor
from pydatatask.executor.container_manager import DockerContainerManager
from pydatatask.host import LOCAL_HOST, Host
from pydatatask.session import Ephemeral

from ..utils import _StderrIsStdout

if TYPE_CHECKING:
    from ..utils import AReadStreamManager, AReadText, AWriteStreamManager, AWriteText

__all__ = ("AbstractProcessManager", "LocalLinuxManager", "SSHLinuxManager")


class AbstractProcessManager(Executor):
    """The base class for process managers.

    Processes are managed through arbitrary "process identifier" handle strings.
    """

    def __init__(self, tmp_path: Union[str, Path]):
        self.tmp_path = Path(tmp_path)

    def to_process_manager(self) -> "AbstractProcessManager":
        return self

    @abstractmethod
    async def get_live_pids(self, hint: Set[str]) -> Set[str]:
        """Get a set of the live process identifiers.

        This may include processes which are not part of the current app/task, but MUST include all the processes in
        ``hint`` which are still alive.
        """
        raise NotImplementedError

    @abstractmethod
    async def spawn(
        self,
        args: List[str],
        environ: Dict[str, str],
        cwd: Union[str, Path],
        return_code: Union[str, Path],
        stdin: Optional[Union[str, Path]],
        stdout: Optional[Union[str, Path]],
        stderr: Optional[Union[str, Path, "_StderrIsStdout"]],
    ) -> str:
        """Launch a process on the target system. This function MUST NOT wait until the process has terminated
        before returning.

        :param args: The command line of the process to launch. ``args[0]`` is the executable to run.
        :param environ: A set of environment variables to add to the target process' environment.
        :param cwd: The directory to launch the process in.
        :param return_code: A filepath in which to store the process exit code, as an ascii integer.
        :param stdin: A filepath from which to read the process' stdin, or None for a null file.
        :param stdout: A filepath to which to write the process' stdout, or None for a null file.
        :param stderr: A filepath to which to write the process' stderr, None for a null file, or the constant
            `pydatatask.task.STDOUT` to interleave it into the stdout stream.
        :return: The process identifier of the process spawned.
        """
        raise NotImplementedError

    @abstractmethod
    async def kill(self, pid: str):
        """Terminate the process with the given identifier.

        This should do nothing if the process is not currently running. This should not prevent e.g. the ``return_code``
        file from being populated.
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def basedir(self) -> Path:
        """A path on the target system to a directory which can be freely manipulated by the app."""
        raise NotImplementedError

    @overload
    @abstractmethod
    async def open(self, path: Union[Path, str], mode: Literal["r"]) -> "AReadText":
        ...

    @overload
    @abstractmethod
    async def open(self, path: Union[Path, str], mode: Literal["w"]) -> "AWriteText":
        ...

    @overload
    @abstractmethod
    async def open(self, path: Union[Path, str], mode: Literal["rb"]) -> "AReadStreamManager":
        ...

    @overload
    @abstractmethod
    async def open(self, path: Union[Path, str], mode: Literal["wb"]) -> "AWriteStreamManager":
        ...

    @abstractmethod
    async def open(
        self, path: Union[Path, str], mode: Literal["r", "rb", "w", "wb"]
    ) -> Union["AReadText", "AWriteText", "AReadStreamManager", "AWriteStreamManager"]:
        """Open the file on the target system for reading or writing according to the given mode."""
        raise NotImplementedError

    @abstractmethod
    async def mkdir(self, path: Path):  # exist_ok=True, parents=True
        """Create a directory with the given path on the target system.

        This should have the semantics of ``mkdir -p``, i.e. create any necessary parent directories and also succeed if
        the directory already exists.
        """
        raise NotImplementedError

    @abstractmethod
    async def rmtree(self, path: Path):
        """Remove a directory and all its children with the given path on the target system."""
        raise NotImplementedError


class LocalLinuxManager(AbstractProcessManager):
    """A process manager to run tasks on the local linux machine.

    By default, it will create a directory ``/tmp/pydatatask`` in which to store data.
    """

    def to_container_manager(self):
        return DockerContainerManager(self.app)

    def __init__(self, app: str, local_path: Union[Path, str] = "/tmp/pydatatask"):
        super().__init__(Path(local_path) / app)
        self.app = app

    @property
    def host(self) -> Host:
        return LOCAL_HOST

    @property
    def basedir(self) -> Path:
        return self.tmp_path

    async def get_live_pids(self, hint: Set[str]) -> Set[str]:
        return {str(x) for x in psutil.pids()}

    async def spawn(self, args, environ, cwd, return_code, stdin, stdout, stderr):
        await self.mkdir(self.tmp_path)
        if stdin is None:
            stdin = "/dev/null"
        else:
            stdin = shlex.quote(stdin)
        if stdout is None:
            stdout = "/dev/null"
        else:
            stdout = shlex.quote(stdout)
        if stderr is None:
            stderr = "/dev/null"
        elif isinstance(stderr, _StderrIsStdout):
            stderr = "&1"
        else:
            stderr = shlex.quote(stderr)
        if "/" in args[0]:
            os.chmod(args[0], 0o755)
        return_code = shlex.quote(return_code)
        p = await asyncio.create_subprocess_shell(
            f"""
            {shlex.join(args)} <{stdin} >{stdout} 2>{stderr} &
            PID=$!
            echo $PID
            wait $PID >/dev/null 2>/dev/null
            echo $? >{return_code} 2>/dev/null
            """,
            stdout=asyncio.subprocess.PIPE,
            close_fds=True,
            cwd=cwd,
            env=environ,
        )
        assert p.stdout is not None
        pid = await p.stdout.readline()
        return pid.decode().strip()

    async def kill(self, pid: str):
        os.kill(int(pid), signal.SIGKILL)

    async def mkdir(self, path: Path):
        path.mkdir(parents=True, exist_ok=True)

    async def rmtree(self, path: Path):
        await aioshutil.rmtree(path)

    async def open(self, path, mode):
        return aiofiles.open(path, mode)

    async def _agent_live(self) -> Tuple[Optional[str], Optional[str]]:
        """Determine whether the agent is alive.

        If so, return (pid, version_string). Otherwise, return (None, None)
        """
        agent_path = self.tmp_path / "agent"
        try:
            async with await self.open(agent_path, "r") as fp:
                bdata = await fp.read()
            data = yaml.safe_load(bdata)
            pid = data["pid"]
            live_version = data["version"]
            if pid in await self.get_live_pids(set()):
                return pid, live_version
        except FileNotFoundError:
            pass

        return None, None

    async def teardown_agent(self):
        pid, _ = await self._agent_live()
        if pid is not None:
            await self.kill(pid)

    async def launch_agent(self, pipeline):
        if pipeline.source_file is None:
            raise ValueError("Cannot start a pipeline without a source_file")
            # if you really need this look into forking instead of subprocessing
        pid, version = await self._agent_live()
        if pid is not None:
            if version == pipeline.agent_version:
                return
            await self.kill(pid)

        # lmao, hack
        Path("/tmp/pydatatask").mkdir(exist_ok=True)
        with open("/tmp/pydatatask/agent-stdout", "wb") as fp:
            p = await asyncio.create_subprocess_exec(
                sys.executable,
                Path(__file__).parent.parent / "cli" / "main.py",
                "agent-http",
                "--port",
                str(pipeline.agent_port),
                "--secret",
                pipeline.agent_secret,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=fp,
                stderr=asyncio.subprocess.STDOUT,
                close_fds=True,
                env={"PIPELINE_YAML": pipeline.source_file},
            )
        agent_path = self.tmp_path / "agent"
        await self.mkdir(self.tmp_path)
        data = yaml.safe_dump({"pid": str(p.pid), "version": pipeline.agent_version})
        async with await self.open(agent_path, "w") as fp:
            await fp.write(data)


class SSHLinuxFile:
    """A file returned by `SSHLinuxManager.open`.

    Probably don't instantiate this directly.
    """

    def __init__(self, path: Union[Path, str], mode: Literal["r", "w", "rb", "wb"], ssh: asyncssh.SSHClientConnection):
        self.path = Path(path)
        self.mode = mode
        self.ssh = ssh
        self.sftp_mgr: Optional[AsyncContextManager] = None
        self.sftp: Optional[asyncssh.SFTPClient] = None
        self.fp_mgr: Optional[AsyncContextManager] = None
        self.fp: Optional[asyncssh.SFTPClientFile] = None

    async def __aenter__(self) -> asyncssh.SFTPClientFile:
        self.sftp_mgr = self.ssh.start_sftp_client()
        self.sftp = await self.sftp_mgr.__aenter__()
        self.fp_mgr = self.sftp.open(self.path, self.mode)
        self.fp = await self.fp_mgr.__aenter__()
        return self.fp

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        assert self.fp_mgr is not None
        assert self.sftp_mgr is not None
        await self.fp_mgr.__aexit__(exc_type, exc_val, exc_tb)
        await self.sftp_mgr.__aexit__(exc_type, exc_val, exc_tb)


class SSHLinuxManager(AbstractProcessManager):
    """A process manager that runs its processes on a remote linux machine, accessed over SSH. The SSH connection is
    parameterized by an :external:class:`asyncssh.connection.SSHClientConnection` instance.

    Sample usage:

    .. code:: python

        session = pydatatask.Session()

        @session.ephemeral
        async def ssh():
            async with asyncssh.connect(
                "localhost", port=self.port, username="weh", password="weh", known_hosts=None
            ) as s:
                yield s

        @session.ephemeral
        async def procman():
            yield pydatatask.SSHLinuxManager(self.test_id, ssh)

        task = pydatatask.ProcessTask("mytask", procman, ...)
    """

    def __init__(
        self,
        app: str,
        ssh: Ephemeral[asyncssh.SSHClientConnection],
        host: Host,
        remote_path: Union[Path, str] = "/tmp/pydatatask",
    ):
        super().__init__(Path(remote_path) / app)
        self._ssh = ssh
        self._host = host

    @property
    def host(self):
        return self._host

    @property
    def ssh(self) -> asyncssh.SSHClientConnection:
        """The `asyncssh.SSHClientConnection` instance associated.

        Will fail if the connection is provided by an unopened Session.
        """
        return self._ssh()

    @property
    def basedir(self) -> Path:
        return self.tmp_path

    async def open(self, path, mode):
        return SSHLinuxFile(path, mode, self.ssh)

    async def rmtree(self, path: Path):
        async with self.ssh.start_sftp_client() as sftp:
            await sftp.rmtree(path, ignore_errors=True)

    async def mkdir(self, path: Path):
        async with self.ssh.start_sftp_client() as sftp:
            await sftp.makedirs(path, exist_ok=True)

    async def kill(self, pid: str):
        await self.ssh.run(f"kill -9 {pid}")

    async def get_live_pids(self, hint):
        p = await self.ssh.run("ls /proc")
        assert p.stdout is not None
        return {x for x in p.stdout.split() if x.isdigit()}

    async def spawn(self, args, environ, cwd, return_code, stdin, stdout, stderr):
        await self.mkdir(self.tmp_path)
        if stdin is None:
            stdin = "/dev/null"
        else:
            stdin = shlex.quote(stdin)
        if stdout is None:
            stdout = "/dev/null"
        else:
            stdout = shlex.quote(stdout)
        if stderr is None:
            stderr = "/dev/null"
        elif isinstance(stderr, _StderrIsStdout):
            stderr = "&1"
        else:
            stderr = shlex.quote(stderr)
        if "/" in args[0]:
            async with self.ssh.start_sftp_client() as sftp:
                await sftp.chmod(args[0], 0o755)
        p = await self.ssh.create_process(
            f"""
            cd {shlex.quote(str(cwd))}
            {shlex.join(args)} <{stdin} >{stdout} 2>{stderr} &
            PID=$!
            echo $PID
            wait $PID >/dev/null 2>/dev/null
            echo $? >{shlex.quote(str(return_code))} 2>/dev/null
            """,
            env=environ,
        )
        pid = await p.stdout.readline()
        return pid.strip()


localhost_manager = LocalLinuxManager("default")


class InProcessLocalLinuxManager(LocalLinuxManager):
    """A process manager for the local linux system which can launch an http agent inside the current process."""

    def __init__(self, app: str, local_path: Union[Path, str] = "/tmp/pydatatask"):
        super().__init__(app, local_path)

        self.runner: Optional[web.AppRunner] = None

    async def teardown_agent(self):
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None

    async def launch_agent(self, pipeline):
        if self.runner is not None:
            return
        app = agent.build_agent_app(pipeline)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", pipeline.agent_port)
        await site.start()
