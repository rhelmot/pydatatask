from typing import List, Set, Dict, Optional, Union, Literal, TYPE_CHECKING, Callable, AsyncContextManager
from pathlib import Path
import psutil
import asyncio
import shlex
import os
import signal

import aiofiles
import aioshutil
import asyncssh

from .task import STDOUT, StderrIsStdout

if TYPE_CHECKING:
    from .repository import AReadText, AWriteText, AReadStream, AWriteStream

__all__ = ('AbstractLinuxManager', 'LocalLinuxManager', 'SSHLinuxManager')

class AbstractLinuxManager:
    async def get_live_pids(self, hint: Set[str]) -> Set[str]:
        raise NotImplementedError

    async def spawn(self, args: List[str], environ: Dict[str, str], cwd: str, return_code: str, stdin: Optional[str], stdout: Optional[str], stderr: Optional[Union[str, StderrIsStdout]]) -> str:
        raise NotImplementedError

    async def kill(self, pid: str):
        raise NotImplementedError

    @property
    def basedir(self) -> Path:
        raise NotImplementedError

    async def open(self, path: Path, mode: Literal['r', 'rb', 'w', 'wb']) -> Union['AReadText', 'AWriteText', 'AReadStream', 'AWriteStream']:
        raise NotImplementedError

    async def mkdir(self, path: Path):  # exist_ok=True, parents=True
        raise NotImplementedError

    async def rmtree(self, path: Path):
        raise NotImplementedError

class LocalLinuxManager(AbstractLinuxManager):
    def __init__(self, app: str, local_path: Union[Path, str]='/tmp/pydatatask'):
        self.local_path = Path(local_path) / app

    @property
    def basedir(self) -> Path:
        return self.local_path

    async def get_live_pids(self, hint: Set[str]) -> Set[str]:
        return {str(x) for x in psutil.pids()}

    async def spawn(self, args, environ, cwd, return_code, stdin, stdout, stderr):
        if stdin is None:
            stdin = '/dev/null'
        else:
            stdin = shlex.quote(stdin)
        if stdout is None:
            stdout = '/dev/null'
        else:
            stdout = shlex.quote(stdout)
        if stderr is None:
            stderr = '/dev/null'
        elif stderr is STDOUT:
            stderr = '&1'
        else:
            stderr = shlex.quote(stderr)
        try:
            os.chmod(args[0], 0o755)
        except:
            pass
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
        pid = await p.stdout.readline()
        return pid.strip()

    async def kill(self, pid: str):
        os.kill(int(pid), signal.SIGKILL)

    async def mkdir(self, path: Path):
        path.mkdir(parents=True, exist_ok=True)

    async def rmtree(self, path: Path):
        await aioshutil.rmtree(path)

    async def open(self, path, mode):
        return aiofiles.open(path, mode)

class SSHLinuxFile:
    def __init__(self, path: Union[Path, str], mode: Literal['r', 'w', 'rb', 'wb'], ssh: asyncssh.SSHClientConnection):
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
        await self.fp_mgr.__aexit__(exc_type, exc_val, exc_tb)
        await self.sftp_mgr.__aexit__(exc_type, exc_val, exc_tb)

class SSHLinuxManager(AbstractLinuxManager):
    def __init__(self, app: str, ssh: Callable[[], asyncssh.SSHClientConnection], remote_path: Union[Path, str]='/tmp/pydatatask'):
        self.remote_path = Path(remote_path) / app
        self._ssh = ssh

    @property
    def ssh(self) -> asyncssh.SSHClientConnection:
        return self._ssh()

    @property
    def basedir(self) -> Path:
        return self.remote_path

    async def open(self, path, mode) -> SSHLinuxFile:
        return SSHLinuxFile(path, mode, self.ssh)

    async def rmtree(self, path: Path):
        async with self.ssh.start_sftp_client() as sftp:
            await sftp.rmtree(path, ignore_errors=True)

    async def mkdir(self, path: Path):
        async with self.ssh.start_sftp_client() as sftp:
            await sftp.makedirs(path, exist_ok=True)

    async def kill(self, pid: str):
        await self.ssh.run(f'kill -9 {pid}')

    async def get_live_pids(self, hint):
        p = await self.ssh.run('ls /proc')
        return {x for x in p.stdout.split() if x.isdigit()}

    async def spawn(self, args, environ, cwd, return_code, stdin, stdout, stderr):
        if stdin is None:
            stdin = '/dev/null'
        else:
            stdin = shlex.quote(stdin)
        if stdout is None:
            stdout = '/dev/null'
        else:
            stdout = shlex.quote(stdout)
        if stderr is None:
            stderr = '/dev/null'
        elif stderr is STDOUT:
            stderr = '&1'
        else:
            stderr = shlex.quote(stderr)
        try:
            async with self.ssh.start_sftp_client() as sftp:
                await sftp.chmod(args[0], 0o755)
        except:
            pass
        p = await self.ssh.create_process(
            f"""
            cd {shlex.quote(cwd)}
            {shlex.join(args)} <{stdin} >{stdout} 2>{stderr} &
            PID=$!
            echo $PID
            wait $PID >/dev/null 2>/dev/null
            echo $? >{shlex.quote(return_code)} 2>/dev/null
            """,
            env=environ,
        )
        pid = await p.stdout.readline()
        return pid.strip()
