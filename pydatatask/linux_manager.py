from typing import List, Set, Dict, Optional, Union, Literal, TYPE_CHECKING
from pathlib import Path
import psutil
import asyncio
import shlex
import os
import signal

import aiofiles
import aioshutil

from .task import STDOUT, StderrIsStdout

if TYPE_CHECKING:
    from .repository import AReadText, AWriteText, AReadStream, AWriteStream

__all__ = ('AbstractLinuxManager', 'LocalLinuxManager')

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
        if stdout is None:
            stdout = '/dev/null'
        if stderr is None:
            stderr = '/dev/null'
        elif stderr is STDOUT:
            stderr = '&1'
        try:
            os.chmod(args[0], 0o755)
        except:
            pass
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
