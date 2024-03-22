"""This module houses the container manager executors."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from itertools import chain
import asyncio

from aiodocker import DockerError
from kubernetes_asyncio.client import V1Pod
import aiodocker.containers
import dateutil.parser

from pydatatask.executor import Executor
from pydatatask.host import LOCAL_HOST, Host
from pydatatask.quota import Quota

if TYPE_CHECKING:
    from pydatatask.executor import pod_manager


class AbstractContainerManager(ABC, Executor):
    """The base class for container managers.

    Members of this class should be able to manage containers, including being able to track their lifecycles.
    """

    @abstractmethod
    async def launch(
        self,
        task: str,
        job: str,
        image: str,
        entrypoint: List[str],
        cmd: str,
        environ: Dict[str, str],
        quota: Quota,
        mounts: List[Tuple[str, str]],
        privileged: bool,
        tty: bool,
    ):
        """Launch a container with the given parameters.

        Mounts should be from localhost.
        """
        raise NotImplementedError

    @abstractmethod
    async def live(self, task: str, job: Optional[str] = None) -> Dict[str, datetime]:
        """Determine which containers from the given task (and optionally, the given job) are still live.

        Should return a dict mapping job id to job start time.
        """
        raise NotImplementedError

    @abstractmethod
    async def kill(self, task: str, job: str):
        """Kill the container associated with the given task and job.

        This does not need to be done gracefully by any stretch. It should wipe any resources associated with the job,
        so it does not show up as a finished task next time `update` is called.
        """
        raise NotImplementedError

    @abstractmethod
    async def update(
        self, task: str, timeout: Optional[timedelta] = None
    ) -> Tuple[bool, Dict[str, Tuple[bytes, Dict[str, Any]]]]:
        """Perform routine maintenence on the running set of jobs for the given task.

        Should return a tuple of a bool indicating whether any jobs were running before this function was called, and a
        dict mapping finished job names to a tuple of the output logs from the job and a dict with any metadata left
        over from the job.

        If any job has been alive for longer than timeout, kill it and return it as part of the finished jobs.
        """
        raise NotImplementedError

    def to_container_manager(self) -> "AbstractContainerManager":
        return self


class DockerContainerManager(AbstractContainerManager):
    """A container manager for a docker installation.

    By default it will try to use the local docker unix socket. If you provide a socket URL to a dockerd hosted
    somewhere other than localhost, don't forget to specify the host parameter.
    """

    def __init__(self, app, url: Optional[str] = None, host: Host = LOCAL_HOST):
        self._url = url
        self._docker: Optional[aiodocker.Docker] = None
        self.app = app
        self._host = host

    @property
    def docker(self) -> aiodocker.Docker:
        """The aiodocker client instance associated with this executor."""
        if self._docker is None:
            self._docker = aiodocker.Docker(self._url)
        return self._docker

    @property
    def host(self):
        return self._host

    def _name_to_id(self, task: str, name: str) -> Optional[str]:
        name = name.strip("/")
        prefix = f"{self.app}___{task}___"
        if name.startswith(prefix):
            return name[len(prefix) :]
        return None

    def _id_to_name(self, task: str, ident: str) -> str:
        return f"{self.app}___{task}___{ident}"

    async def launch(
        self,
        task: str,
        job: str,
        image: str,
        entrypoint: List[str],
        cmd: str,
        environ: Dict[str, str],
        quota: Quota,
        mounts: List[Tuple[str, str]],
        privileged: bool,
        tty: bool,
    ):
        await self.docker.containers.run(
            {
                "Image": image,
                "AttachStdout": False,
                "AttachStderr": False,
                "AttachStdin": False,
                "OpenStdin": False,
                "Tty": tty,
                "Entrypoint": entrypoint,
                "Cmd": cmd,
                "Env": [f"{key}={val}" for key, val in environ.items()],
                "HostConfig": {
                    "Binds": [f"{a}:{b}" for a, b in mounts],
                    "Privileged": privileged,
                },
            },
            name=self._id_to_name(task, job),
        )

    async def live(self, task: str, job: Optional[str] = None) -> Dict[str, datetime]:
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

    async def kill(self, task: str, job: str):
        cont = await self.docker.containers.get(self._id_to_name(task, job))
        try:
            await cont.kill()
        except aiodocker.exceptions.DockerError:
            pass
        await cont.delete()

    async def update(
        self, task: str, timeout: Optional[timedelta] = None
    ) -> Tuple[bool, Dict[str, Tuple[bytes, Dict[str, Any]]]]:
        containers = await self.docker.containers.list(all=1)
        infos = await asyncio.gather(*(c.show() for c in containers))
        infos_and_names = [(self._name_to_id(task, info["Name"]), info) for info in infos]
        activity = any(name is not None for name, _ in infos_and_names)
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
        await asyncio.gather(*(cont.kill() for _, cont, _ in timed_out))
        results = await asyncio.gather(
            *(self._cleanup(container, info) for info, container, _ in dead),
            *(self._cleanup(container, info, True) for info, container, _ in timed_out),
        )
        return activity, {name: result for (_, _, name), result in zip(chain(dead, timed_out), results)}

    async def _cleanup(
        self, container: aiodocker.containers.DockerContainer, info: Dict[str, Any], timed_out: bool = False
    ) -> Tuple[bytes, Dict[str, Any]]:
        log = "".join(line for line in await container.log(stdout=True, stderr=True)).encode()
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

    def __init__(self, cluster: "pod_manager.PodManager"):
        self.cluster = cluster

    @property
    def host(self):
        return self.cluster.host

    async def launch(
        self,
        task: str,
        job: str,
        image: str,
        entrypoint: List[str],
        cmd: str,
        environ: Dict[str, str],
        quota: Quota,
        mounts: List[Tuple[str, str]],
        privileged: bool,
        tty: bool,
    ):
        if mounts:
            raise ValueError("Cannot do mounts from a container on a kube cluster")
        if tty:
            raise ValueError("Cannot do tty from a container on a kube cluster")
        await self.cluster.launch(
            job,
            task,
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "spec": {
                    "containers": [
                        {
                            "name": "main",
                            "image": image,
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

    async def live(self, task: str, job: Optional[str] = None) -> Dict[str, datetime]:
        pods = await self.cluster.query(job, task)
        return {pod.metadata.labels["job"]: pod.metadata.creation_timestamp for pod in pods}

    async def kill(self, task: str, job: str):
        pods = await self.cluster.query(job, task)
        for pod in pods:
            await self.cluster.delete(pod)

    async def update(
        self, task: str, timeout: Optional[timedelta] = None
    ) -> Tuple[bool, Dict[str, Tuple[bytes, Dict[str, Any]]]]:
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
        results = await asyncio.gather(
            *chain((self._cleanup(pod) for pod in finished_pods), (self._cleanup(pod, True) for pod in timeout_pods))
        )
        return bool(pods), {pod.metadata.labels["job"]: result for pod, result in zip(pods, results)}

    async def _cleanup(self, pod: V1Pod, timeout: bool = False) -> Tuple[bytes, Dict[str, Any]]:
        log = await self.cluster.logs(pod)
        await self.cluster.delete(pod)
        return (
            log.encode(),
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


localhost_docker_manager = DockerContainerManager("pydatatask")
