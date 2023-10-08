"""This module houses the container manager executors."""
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
from abc import ABC, abstractmethod
from datetime import datetime, timezone
import asyncio

from kubernetes_asyncio.client import V1Pod
import aiodocker.containers

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

        This does not need to be done gracefully by any stretch.
        """
        raise NotImplementedError

    @abstractmethod
    async def update(self, task: str) -> Tuple[bool, Dict[str, Tuple[bytes, Dict[str, Any]]]]:
        """Perform routine maintenence on the running set of jobs for the given task.

        Should return a tuple of a bool indicating whether any jobs were running before this function was called, and a
        dict mapping finished job names to a tuple of the output logs from the job and a dict with any metadata left
        over from the job.
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
        prefix = f"{self.app}_{task}_"
        if name.startswith(prefix):
            return name[len(prefix) :]
        return None

    def _id_to_name(self, task: str, ident: str) -> str:
        return f"{self.app}_{task}_{ident}"

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
    ):
        await self.docker.containers.run(
            {
                "Image": image,
                "AttachStdout": False,
                "AttachStderr": False,
                "AttachStdin": False,
                "OpenStdin": False,
                "Tty": False,
                "Entrypoint": entrypoint,
                "Cmd": cmd,
                "Env": [f"{key}={val}" for key, val in environ.items()],
                "HostConfig": {
                    "Binds": [f"{a}:{b}" for a, b in mounts],
                },
            },
            name=self._id_to_name(task, job),
        )

    async def live(self, task: str, job: Optional[str] = None) -> Dict[str, datetime]:
        containers = await self.docker.containers.list(all=1)
        infos = await asyncio.gather(*(c.show() for c in containers))
        live = [
            (info, self._name_to_id(task, info["Name"]))
            for info in infos
            if not info["State"]["Dead"] and not info["State"]["OOMKilled"]
        ]
        return {
            name: datetime.fromisoformat(info["State"]["StartedAt"])
            for info, name in live
            if name is not None and (job is None or name == job)
        }

    async def kill(self, task: str, job: str):
        cont = await self.docker.containers.get(self._id_to_name(task, job))
        await cont.kill()
        await cont.delete()

    async def update(self, task: str) -> Tuple[bool, Dict[str, Tuple[bytes, Dict[str, Any]]]]:
        containers = await self.docker.containers.list(all=1)
        infos = await asyncio.gather(*(c.show() for c in containers))
        infos_and_names = [(self._name_to_id(task, info["Name"]), info) for info in infos]
        activity = any(name is not None for name, _ in infos_and_names)
        dead = [
            (info, container, name)
            for (name, info), container in zip(infos_and_names, containers)
            if not info["State"]["Dead"] and not info["State"]["OOMKilled"] and name is not None
        ]
        results = await asyncio.gather(*(self._cleanup(container, info) for info, container, _ in dead))
        return activity, {name: result for (_, _, name), result in zip(dead, results)}

    async def _cleanup(
        self, container: aiodocker.containers.DockerContainer, info: Dict[str, Any]
    ) -> Tuple[bytes, Dict[str, Any]]:
        log = "".join(line for line in await container.log(stdout=True, stderr=True)).encode()
        await container.delete()
        return (log, info)


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
    ):
        if mounts:
            raise ValueError("Cannot do mounts from a container on a kube cluster")
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

    async def update(self, task: str) -> Tuple[bool, Dict[str, Tuple[bytes, Dict[str, Any]]]]:
        pods = await self.cluster.query(job=None, task=task)
        finished_pods = [pod for pod in pods if pod.status.phase in ("Succeeded", "Failed")]
        results = await asyncio.gather(*(self._cleanup(pod) for pod in finished_pods))
        return bool(pods), {pod.metadata.labels["job"]: result for pod, result in zip(pods, results)}

    async def _cleanup(self, pod: V1Pod) -> Tuple[bytes, Dict[str, Any]]:
        log = await self.cluster.logs(pod)
        await self.cluster.delete(pod)
        return (
            log.encode(),
            {
                "reason": pod.status.phsae,
                "start_time": pod.metadata.creation_timestamp,
                "end_time": datetime.now(tz=timezone.utc),
                "image": pod.status.container_statuses[0].image,
                "node": pod.spec.node_name,
            },
        )


localhost_docker_manager = DockerContainerManager("pydatatask")
