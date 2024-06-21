"""In order for a `KubeTask` or a subclass to connect, authenticate, and manage pods in a kubernetes cluster, it
needs several resource references.

the `PodManager` simplifies tracking the lifetimes of these resources.
"""

from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Callable,
    DefaultDict,
    Dict,
    List,
    Optional,
    Tuple,
)
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import asyncio
import logging
from typing_extensions import Self

from kubernetes_asyncio.client import ApiClient, ApiException, CoreV1Api
from kubernetes_asyncio.config import (
    ConfigException,
    load_incluster_config,
    load_kube_config,
)
from kubernetes_asyncio.config.kube_config import Configuration
from kubernetes_asyncio.stream import WsApiClient

from pydatatask.executor import Executor
from pydatatask.executor.container_manager import KubeContainerManager
from pydatatask.host import Host
from pydatatask.quota import Quota
from pydatatask.session import Ephemeral

l = logging.getLogger(__name__)

__all__ = ("PodManager", "KubeConnection", "kube_connect")


class KubeConnection:
    """A connection to a kubernetes cluster.

    Used as an argument to PodManager in order to separate the async bits from the sync bits. If you're loading
    configuration from standard paths, then rather than instantiating one directly, you should use kube_connect.
    """

    def __init__(self, config: Configuration):
        self.api: ApiClient = ApiClient(config)
        self.api_ws: WsApiClient = WsApiClient(config)
        self.v1 = CoreV1Api(self.api)
        self.v1_ws = CoreV1Api(self.api_ws)

    async def close(self):
        """Clean up the connection."""
        await self.api.close()
        await self.api_ws.close()


def kube_connect(
    config_file: Optional[str] = None, context: Optional[str] = None
) -> Callable[[], AsyncIterator[KubeConnection]]:
    """Load kuberenetes configuration from standard paths and generate a KubeConnection based on it.

    This should be used like so:

    .. code:: python

        session = Session()
        kube_connection = session.ephemeral(kube_connect(...))
        pod_manager = PodManager(..., connection=kube_connection)
    """

    async def inner():
        config = type.__call__(Configuration)
        try:
            load_incluster_config(config)
        except ConfigException:
            loader = await load_kube_config(config_file, context)
            await loader.load_and_set(config)
        connection = KubeConnection(config)
        yield connection
        await connection.close()

    return inner


@dataclass
class VolumeSpec:
    pvc: Optional[str] = None
    host_path: Optional[str] = None
    null: bool = False

    @classmethod
    def parse(cls, data: str) -> Self:
        if '/' in data:
            return cls(host_path=data)
        return cls(pvc=data)

    def to_kube(self, name: str) -> Dict[str, Any]:
        if self.pvc is not None:
            return {"name": name, "persistentVolumeClaim": {"claimName": self.pvc}}
        if self.host_path is not None:
            return {"name": name, "hostPath": {"path": self.host_path}}
        assert self.null
        raise Exception("VolumeSpec is null")


class PodManager(Executor):
    """A pod manager allows multiple tasks to share a connection to a kubernetes cluster and manage pods on it."""

    def to_pod_manager(self) -> "PodManager":
        return self

    def to_container_manager(self):
        return KubeContainerManager(self.quota, cluster=self)

    def __init__(
        self,
        quota: Quota,
        host: Host,
        app: str,
        namespace: str,
        connection: Ephemeral[KubeConnection],
        volumes: Optional[Dict[str, VolumeSpec]] = None,
    ):
        """
        :param app: The app name string with which to label all created pods.
        :param namespace: The namespace in which to create and query pods.
        :param config: Optional: A callable returning a kubernetes configuration object. If not provided, will attempt
                                 to use the "default" configuration, i.e. what is available after calling
                                 ``await kubernetes_asyncio.config.load_kube_config()``.
        """
        super().__init__(quota)
        self._host = host
        self.app = app
        self.namespace = namespace
        self._connection = connection
        self.volumes = volumes or {}

    @property
    def host(self):
        return self._host

    @property
    def connection(self) -> KubeConnection:
        """The ephemeral connection.

        This function will fail is the connection is provided by an unopened session.
        """
        return self._connection()

    @property
    def api(self) -> Any:
        """The current API client."""
        return self.connection.api

    @property
    def api_ws(self) -> Any:
        """The current websocket-aware API client."""
        return self.connection.api_ws

    @property
    def v1(self) -> Any:
        """A CoreV1Api instance associated with the current API client."""
        return self.connection.v1

    @property
    def v1_ws(self) -> Any:
        """A CoreV1Api instance associated with the current websocket-aware API client."""
        return self.connection.v1_ws

    def _id_to_name(self, task: str, job: str, replica: int) -> str:
        return f"{self.app}-{task}-{job}-{replica}"

    def _name_to_id(self, name: str, task: str) -> Tuple[str, int]:
        prefix = f"{self.app}-{task}-"
        if name.startswith(prefix):
            job, replica = name[len(prefix) :].split("-")
            return job, int(replica)
        raise Exception("Not a pod for this task")

    async def launch(self, task: str, job: str, replica: int, manifest):
        """Create a pod with the given manifest, named and labeled for this podman's app and the given job and
        task."""
        assert manifest["kind"] == "Pod"
        task = task.replace("_", "-")

        manifest["metadata"] = manifest.get("metadata", {})
        manifest["metadata"].update(
            {
                "name": self._id_to_name(task, job, replica),
                "labels": {
                    "app": self.app,
                    "task": task,
                    "job": job,
                    "replica": str(replica),
                },
            }
        )

        await self.v1.create_namespaced_pod(self.namespace, manifest)

    async def kill(self, task: str, job: str, replica: int):
        """Killllllllllllll."""
        await self.v1.delete_namespaced_pod(self._id_to_name(task, job, replica), self.namespace)

    async def update(
        self, task: str, timeout: Optional[timedelta] = None
    ) -> Tuple[Dict[Tuple[str, int], datetime], Dict[str, Dict[int, Tuple[Optional[bytes], Dict[str, Any]]]]]:
        """Do maintainence."""
        pods = await self.query(task=task)
        podmap = {pod.metadata.name: pod for pod in pods}
        dead = {name for name, pod in podmap.items() if pod.status.phase in ("Succeeded", "Failed")}
        live = set(podmap) - dead
        now = datetime.now(tz=timezone.utc)
        timed = {
            name for name in live if timeout is not None and podmap[name].metadata.creation_timesamp + timeout > now
        }
        live -= timed
        dead |= timed
        live_jobs = {self._name_to_id(name, task)[0] for name in live}

        def gen_done(pod):
            return {
                "reason": "Timeout" if pod.metadata.name in timed else pod.status.phase,
                "start_time": pod.metadata.creation_timestamp,
                "end_time": datetime.now(tz=timezone.utc),
                "image": pod.status.container_statuses[0].image,
                "node": pod.spec.node_name,
                "timeout": pod.metadata.name in timed,
                "success": pod.status.phase == "Succeeded",
            }

        async def io_guy(name) -> Optional[bytes]:
            try:
                return await self.logs(podmap[name])
            except (TimeoutError, ApiException):
                return None

        logs = await asyncio.gather(
            *(io_guy(name) for name in dead if self._name_to_id(name, task)[0] not in live_jobs)
        )
        await asyncio.gather(
            *(self.v1.delete_namespaced_pod(name, self.namespace) for name in dead), return_exceptions=True
        )

        live_result = {
            (str(podmap[name].metadata.labels["job"]), int(podmap[name].metadata.labels["replica"])): podmap[
                name
            ].metadata.creation_timestamp
            for name in live
        }
        reap_result: DefaultDict[str, Dict[int, Tuple[Optional[bytes], Dict[str, Any]]]] = defaultdict(dict)
        for name, log in zip(dead, logs):
            job, replica = self._name_to_id(name, task)
            if job not in live_jobs:
                reap_result[job][replica] = (log, gen_done(podmap[name]))

        return live_result, dict(reap_result)

    async def query(self, job=None, task=None, replica=None) -> List[Any]:
        """Return a list of pods labeled for this podman's app and (optional) the given job and task."""
        selectors = ["app=" + self.app]
        if job is not None:
            selectors.append("job=" + job)
        if task is not None:
            selectors.append("task=" + task.replace("_", "-"))
        if replica is not None:
            selectors.append("replica=" + str(replica))
        selector = ",".join(selectors)
        return (await self.v1.list_namespaced_pod(self.namespace, label_selector=selector)).items

    async def delete(self, pod: Any):
        """Destroy the given pod."""
        await self.v1.delete_namespaced_pod(pod.metadata.name, self.namespace)

    async def logs(self, pod: Any, timeout=10) -> bytes:
        """Retrieve the logs for the given pod."""
        return (
            await self.v1.read_namespaced_pod_log(pod.metadata.name, self.namespace, _request_timeout=timeout)
        ).encode()
