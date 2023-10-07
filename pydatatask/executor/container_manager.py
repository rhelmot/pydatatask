from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
from abc import ABC, abstractmethod
from datetime import datetime, timezone
import asyncio

from kubernetes_asyncio.client import V1Pod
import aiodocker.containers

from pydatatask.executor import Executor
from pydatatask.host import LOCAL_HOST, Host
from pydatatask.resource_manager import Resources

if TYPE_CHECKING:
    from pydatatask.executor.pod_manager import PodManager


class ContainerManagerAbstract(ABC, Executor):
    @abstractmethod
    async def launch(
        self,
        task: str,
        job: str,
        image: str,
        entrypoint: List[str],
        cmd: str,
        environ: Dict[str, str],
        resources: Resources,
        mounts: List[Tuple[str, str]],
    ):
        raise NotImplementedError

    @abstractmethod
    async def live(self, task: str, job: Optional[str] = None) -> Dict[str, datetime]:
        raise NotImplementedError

    @abstractmethod
    async def kill(self, task: str, job: str):
        raise NotImplementedError

    @abstractmethod
    async def update(self, task: str) -> Tuple[bool, Dict[str, Tuple[bytes, Dict[str, Any]]]]:
        raise NotImplementedError

    def to_container_manager(self) -> "ContainerManagerAbstract":
        return self


class DockerContainerManager(ContainerManagerAbstract):
    def __init__(self, app: str = "pydatatask", url: Optional[str] = None, host: Host = LOCAL_HOST):
        self.docker = aiodocker.Docker(url)
        self.app = app
        self._host = host

    @property
    def host(self):
        return self._host

    def name_to_id(self, task: str, name: str) -> Optional[str]:
        name = name.strip("/")
        prefix = f"{self.app}_{task}_"
        if name.startswith(prefix):
            return name[len(prefix) :]
        return None

    def id_to_name(self, task: str, ident: str) -> str:
        return f"{self.app}_{task}_{ident}"

    async def launch(
        self,
        task: str,
        job: str,
        image: str,
        entrypoint: List[str],
        cmd: str,
        environ: Dict[str, str],
        resources: Resources,
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
            name=self.id_to_name(task, job),
        )

    async def live(self, task: str, job: Optional[str] = None) -> Dict[str, datetime]:
        containers = await self.docker.containers.list(all=1)
        infos = await asyncio.gather(*(c.show() for c in containers))
        live = [
            (info, self.name_to_id(task, info["Name"]))
            for info in infos
            if not info["Status"]["Dead"] and not info["Status"]["OOMKilled"]
        ]
        return {
            name: datetime.fromisoformat(info["State"]["StartedAt"])
            for info, name in live
            if name is not None and (job is None or name == job)
        }

    async def kill(self, task: str, job: str):
        cont = await self.docker.containers.get(self.id_to_name(task, job))
        await cont.kill()
        await cont.delete()

    async def update(self, task: str) -> Tuple[bool, Dict[str, Tuple[bytes, Dict[str, Any]]]]:
        containers = await self.docker.containers.list(all=1)
        infos = await asyncio.gather(*(c.show() for c in containers))
        infos_and_names = [(self.name_to_id(task, info["Name"]), info) for info in infos]
        activity = any(name is not None for name, _ in infos_and_names)
        dead = [
            (info, container, name)
            for (name, info), container in zip(infos_and_names, containers)
            if not info["Status"]["Dead"] and not info["Status"]["OOMKilled"] and name is not None
        ]
        results = await asyncio.gather(*(self.cleanup(container, info) for info, container, _ in dead))
        return activity, {name: result for (_, _, name), result in zip(dead, results)}

    async def cleanup(
        self, container: aiodocker.containers.DockerContainer, info: Dict[str, Any]
    ) -> Tuple[bytes, Dict[str, Any]]:
        log = "".join(line for line in await container.log(stdout=True, stderr=True)).encode()
        await container.delete()
        return (log, info)


class KubeContainerManager(ContainerManagerAbstract):
    def __init__(self, cluster: "PodManager"):
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
        resources: Resources,
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
                                    "cpu": str(resources.cpu),
                                    "memory": str(resources.mem),
                                },
                                "limits": {
                                    "cpu": str(resources.cpu),
                                    "memory": str(resources.mem),
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
        results = await asyncio.gather(*(self.cleanup(pod) for pod in finished_pods))
        return bool(pods), {pod.metadata.labels["job"]: result for pod, result in zip(pods, results)}

    async def cleanup(self, pod: V1Pod) -> Tuple[bytes, Dict[str, Any]]:
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


localhost_docker_manager = DockerContainerManager()
