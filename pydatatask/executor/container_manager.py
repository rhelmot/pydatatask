"""This module houses the container manager executors."""

from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    DefaultDict,
    Dict,
    List,
    Optional,
    Tuple,
    cast,
)
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import chain
import asyncio
import os

from aiodocker import DockerError
import aiodocker.containers
import aiofiles.ospath
import dateutil.parser

from pydatatask.executor import Executor
from pydatatask.host import LOCAL_HOST, Host
from pydatatask.quota import Quota
from pydatatask.session import Ephemeral

if TYPE_CHECKING:
    from pydatatask.executor import pod_manager


def docker_connect(url: Optional[str] = None):
    """Connect to a docker daemon.

    If url is provided, connect to the socket there. If not, connect to the default system daemon.
    """

    async def docker_connect_inner():
        async with aiodocker.Docker(url) as docker:
            yield docker

    return docker_connect_inner


class AbstractContainerManager(ABC, Executor):
    """The base class for container managers.

    Members of this class should be able to manage containers, including being able to track their lifecycles.
    """

    def __init__(self, quota: Quota, *, image_prefix: str = ""):
        super().__init__(quota)
        self._image_prefix = image_prefix

    @abstractmethod
    async def launch(
        self,
        task: str,
        job: str,
        replica: int,
        image: str,
        entrypoint: List[str],
        cmd: str,
        environ: Dict[str, str],
        quota: Quota,
        mounts: List[Tuple[str, str]],
        privileged: bool,
        tty: bool,
        host_mounts: Optional[Dict[str, str]] = None,
    ):
        """Launch a container with the given parameters.

        Mounts should be from localhost.
        """
        raise NotImplementedError

    @abstractmethod
    async def live(self, task: str, job: Optional[str] = None) -> Dict[Tuple[str, int], datetime]:
        """Determine which containers from the given task (and optionally, the given job) are still live.

        Should return a dict mapping job id to job start time.
        """
        raise NotImplementedError

    @abstractmethod
    async def kill(self, task: str, job: str, replica: int):
        """Kill the container associated with the given task and job and replica.

        This does not need to be done gracefully by any stretch. It should wipe any resources associated with the job,
        so it does not show up as a finished task next time `update` is called.
        """
        raise NotImplementedError

    @abstractmethod
    async def update(
        self, task: str, timeout: Optional[timedelta] = None
    ) -> Tuple[Dict[Tuple[str, int], datetime], Dict[str, Dict[int, Tuple[Optional[bytes], Dict[str, Any]]]]]:
        """Perform routine maintenence on the running set of jobs for the given task.

        Should return a tuple of a the set of live replicas and a dict mapping finished job names to a tuple of the
        output logs from the job and a dict with any metadata left over from any replicas of the job.

        If any job has been alive for longer than timeout, kill it and return it as part of the finished jobs, not the
        live jobs.
        """
        raise NotImplementedError

    def to_container_manager(self) -> "AbstractContainerManager":
        return self


class DockerContainerManager(AbstractContainerManager):
    """A container manager for a docker installation.

    By default it will try to use the local docker unix socket. If you provide a socket URL to a dockerd hosted
    somewhere other than localhost, don't forget to specify the host parameter.
    """

    def __init__(
        self,
        quota: Quota,
        *,
        app: str = "pydatatask",
        docker: Ephemeral[aiodocker.Docker],
        host: Host = LOCAL_HOST,
        image_prefix: str = "",
    ):
        super().__init__(quota, image_prefix=image_prefix)
        self._docker = docker
        self.app = app
        self._host = host
        self._net = None

    @property
    def docker(self) -> aiodocker.Docker:
        """The aiodocker client instance associated with this executor."""
        return self._docker()

    @property
    def host(self):
        return self._host

    def _name_to_id(self, task: str, name: str) -> Optional[Tuple[str, int]]:
        name = name.strip("/")
        prefix = f"{self.app}___{task}___"
        if name.startswith(prefix):
            ident, replica = name[len(prefix) :].split("___")
            return ident, int(replica)
        return None

    def _id_to_name(self, task: str, ident: str, replica: int) -> str:
        return f"{self.app}___{task}___{ident}___{replica}"

    async def launch(
        self,
        task: str,
        job: str,
        replica: int,
        image: str,
        entrypoint: List[str],
        cmd: str,
        environ: Dict[str, str],
        quota: Quota,
        mounts: List[Tuple[str, str]],
        privileged: bool,
        tty: bool,
        host_mounts: Optional[Dict[str, str]] = None,
    ):
        if self._net is None:
            if await aiofiles.ospath.exists("/.dockerenv"):
                try:
                    hostname = cast(str, os.getenv("HOSTNAME"))
                    bytes.fromhex(hostname)
                except:  # pylint: disable=broad-except,bare-except
                    pass
                else:
                    self_container = await self.docker.containers.get(hostname)
                    config = await self_container.show()
                    self._net = config["HostConfig"]["NetworkMode"]

        config = {
            "Image": self._image_prefix + image,
            "AttachStdout": False,
            "AttachStderr": False,
            "AttachStdin": False,
            "OpenStdin": False,
            "Tty": tty,
            "Entrypoint": entrypoint,
            "Cmd": cmd,
            "Env": [f"{key}={val}" for key, val in environ.items()],
            "HostConfig": {
                "Binds": [f"{a}:{b}" for a, b in mounts] + [f"{a}:{b}" for a, b in (host_mounts or {}).items()],
                "Privileged": privileged,
            },
        }
        if self._net is not None:
            config["HostConfig"]["NetworkMode"] = self._net

        await self.docker.containers.run(config, name=self._id_to_name(task, job, replica))

    async def live(self, task: str, job: Optional[str] = None) -> Dict[Tuple[str, int], datetime]:
        containers = await self.docker.containers.list(all=1)
        infos = await asyncio.gather(*(c.show() for c in containers), return_exceptions=True)

        # avoid docker errors, as they usually come from race-conditions like the container
        # being deleted between the list and the show
        docker_exceptions = []
        other_exceptions = []
        non_exception_infos = []
        for info in infos:
            if isinstance(info, DockerError):
                docker_exceptions.append(info)
            elif isinstance(info, BaseException):
                other_exceptions.append(info)
            else:
                non_exception_infos.append(info)

        # ignore docker exceptions, raise other exceptions, and continue with non-exceptions
        if other_exceptions:
            raise other_exceptions[0]

        live = [
            (info, self._name_to_id(task, info["Name"]))
            for info in non_exception_infos
            # if not info["State"]["Status"] in ('exited',)
        ]
        return {
            name: dateutil.parser.isoparse(info["State"]["StartedAt"])
            for info, name in live
            if name is not None and (job is None or name == job)
        }

    async def kill(self, task: str, job: str, replica: int):
        cont = await self.docker.containers.get(self._id_to_name(task, job, replica))
        try:
            await cont.kill()
        except aiodocker.exceptions.DockerError:
            pass
        await cont.delete()

    async def update(self, task: str, timeout: Optional[timedelta] = None):
        containers = await self.docker.containers.list(all=1)
        infos = await asyncio.gather(*(c.show() for c in containers), return_exceptions=True)
        infos_and_names = [
            (self._name_to_id(task, info["Name"]), info) for info in infos if not isinstance(info, BaseException)
        ]
        dead = [
            (info, container, name)
            for (name, info), container in zip(infos_and_names, containers)
            if info["State"]["Status"] in ("exited",) and name is not None
        ]
        now = datetime.now(tz=timezone.utc)
        timed_out = [
            (info, container, name)
            for (name, info), container in zip(infos_and_names, containers)
            if info["State"]["Status"] not in ("exited",)
            and name is not None
            and timeout
            and now - dateutil.parser.isoparse(info["State"]["StartedAt"]) > timeout
        ]
        live_replicas = {
            name: dateutil.parser.isoparse(info["State"]["StartedAt"])
            for (name, info), _ in zip(infos_and_names, containers)
            if info["State"]["Status"] not in ("exited",)
            and name is not None
            and not (timeout and now - dateutil.parser.isoparse(info["State"]["StartedAt"]) > timeout)
        }
        live_jobs = {job for job, _ in live_replicas}
        await asyncio.gather(*(cont.kill() for _, cont, _ in timed_out))
        results = await asyncio.gather(
            *(self._cleanup(container, info) for info, container, _ in dead),
            *(self._cleanup(container, info, True) for info, container, _ in timed_out),
        )
        final: DefaultDict[str, Dict[int, Tuple[Optional[bytes], Dict[str, Any]]]] = defaultdict(dict)
        for (_, _, (job, replica)), result in zip(chain(dead, timed_out), results):
            if job not in live_jobs:
                final[job][replica] = result

        return live_replicas, dict(final)

    async def _cleanup(
        self, container: aiodocker.containers.DockerContainer, info: Dict[str, Any], timed_out: bool = False
    ) -> Tuple[bytes, Dict[str, Any]]:
        log = "".join(
            line for line in await cast(Awaitable[List[str]], container.log(stdout=True, stderr=True))
        ).encode()
        try:
            await container.delete()
        except Exception:  # pylint: disable=broad-exception-caught
            pass
        info["timed_out"] = timed_out
        now = datetime.now(tz=timezone.utc)
        meta = {
            "success": not timed_out and info["State"]["ExitCode"] == 0,
            "start_time": dateutil.parser.isoparse(info["State"]["StartedAt"]),
            "end_time": now if timed_out else dateutil.parser.isoparse(info["State"]["FinishedAt"]),
            "timeout": timed_out,
            "exit_code": -1 if timed_out else info["State"]["ExitCode"],
            "image": info["Config"]["Image"],
        }
        return (log, meta)


class KubeContainerManager(AbstractContainerManager):
    """An executor that runs containers on a kubernetes cluster."""

    def __init__(self, quota, *, cluster: "pod_manager.PodManager", image_prefix: str = ""):
        super().__init__(quota, image_prefix=image_prefix)
        self.cluster = cluster

    @property
    def host(self):
        return self.cluster.host

    async def launch(
        self,
        task: str,
        job: str,
        replica: int,
        image: str,
        entrypoint: List[str],
        cmd: str,
        environ: Dict[str, str],
        quota: Quota,
        mounts: List[Tuple[str, str]],
        privileged: bool,
        tty: bool,
        host_mounts: Optional[Dict[str, str]] = None,
    ):
        if mounts or host_mounts:
            raise ValueError("Cannot do mounts from a container on a kube cluster")
        if tty:
            raise ValueError("Cannot do tty from a container on a kube cluster")
        await self.cluster.launch(
            task,
            job,
            replica,
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "spec": {
                    "containers": [
                        {
                            "name": "main",
                            "image": self._image_prefix + image,
                            "imagePullPolicy": "always",
                            "entrypoint": entrypoint,
                            "command": [cmd],
                            "env": [{"name": name, "value": value} for name, value in environ.items()],
                            "resources": {
                                "requests": {
                                    "cpu": str(quota.cpu),
                                    "memory": str(quota.mem),
                                },
                                "limits": {
                                    "cpu": str(quota.cpu),
                                    "memory": str(quota.mem),
                                },
                            },
                            "securityContext": {
                                "privileged": privileged,
                            },
                        }
                    ],
                },
            },
        )

    async def live(self, task: str, job: Optional[str] = None) -> Dict[Tuple[str, int], datetime]:
        pods = await self.cluster.query(job, task)
        return {pod.metadata.labels["job"]: pod.metadata.creation_timestamp for pod in pods}

    async def kill(self, task: str, job: str, replica: int):
        pods = await self.cluster.query(job, task, replica)
        for pod in pods:
            await self.cluster.delete(pod)

    async def update(self, task: str, timeout: Optional[timedelta] = None):
        pods = await self.cluster.query(job=None, task=task)
        finished_pods = [pod for pod in pods if pod.status.phase in ("Succeeded", "Failed")]
        now = datetime.now(tz=timezone.utc)
        timeout_pods = [
            pod
            for pod in pods
            if pod.status.phase not in ("Succeeded", "Failed")
            and timeout
            and now - pod.metadata.creation_timestamp > timeout
        ]
        live_replicas = {
            (pod.metadata.labels["job"], int(pod.metadata.labels["replica"])): pod.metadata.creation_timestamp
            for pod in pods
            if pod.status.phase not in ("Succeeded", "Failed")
            and not (timeout and now - pod.metadata.creation_timestamp > timeout)
        }
        live_jobs = {job for job, _ in live_replicas}
        results = await asyncio.gather(
            *chain((self._cleanup(pod) for pod in finished_pods), (self._cleanup(pod, True) for pod in timeout_pods))
        )
        final: DefaultDict[str, Dict[int, Tuple[Optional[bytes], Dict[str, Any]]]] = defaultdict(dict)
        for pod, result in zip(pods, results):
            if pod.metadata.labels["job"] not in live_jobs:
                final[pod.metadata.labels["job"]][int(pod.metadata.labels["replica"])] = result
        return live_replicas, dict(final)

    async def _cleanup(self, pod, timeout: bool = False) -> Tuple[bytes, Dict[str, Any]]:
        log = await self.cluster.logs(pod)
        await self.cluster.delete(pod)
        return (
            log,
            {
                "reason": pod.status.phase if not timeout else "Timeout",
                "timeout": timeout,
                "start_time": pod.metadata.creation_timestamp,
                "end_time": datetime.now(tz=timezone.utc),
                "image": pod.status.container_statuses[0].image,
                "node": pod.spec.node_name,
                "success": pod.status.phase == "Succeeded" and not timeout,
            },
        )
