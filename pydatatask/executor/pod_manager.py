"""In order for a `KubeTask` or a subclass to connect, authenticate, and manage pods in a kubernetes cluster, it
needs several resource references.

the `PodManager` simplifies tracking the lifetimes of these resources.
"""

from typing import AsyncIterator, Callable, List, Optional
import logging

from kubernetes_asyncio.client import ApiClient, CoreV1Api, V1Pod
from kubernetes_asyncio.config import load_kube_config
from kubernetes_asyncio.config.kube_config import Configuration
from kubernetes_asyncio.stream import WsApiClient

from pydatatask.executor import Executor
from pydatatask.executor.container_manager import KubeContainerManager
from pydatatask.host import Host
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
        await load_kube_config(config_file, context, config)
        connection = KubeConnection(config)
        yield connection
        await connection.close()

    return inner


class PodManager(Executor):
    """A pod manager allows multiple tasks to share a connection to a kubernetes cluster and manage pods on it."""

    def to_pod_manager(self) -> "PodManager":
        return self

    def to_container_manager(self):
        return KubeContainerManager(self)

    def __init__(
        self,
        host: Host,
        app: str,
        namespace: str,
        connection: Ephemeral[KubeConnection],
    ):
        """
        :param app: The app name string with which to label all created pods.
        :param namespace: The namespace in which to create and query pods.
        :param config: Optional: A callable returning a kubernetes configuration object. If not provided, will attempt
                                 to use the "default" configuration, i.e. what is available after calling
                                 ``await kubernetes_asyncio.config.load_kube_config()``.
        """
        self._host = host
        self.app = app
        self.namespace = namespace
        self._connection = connection

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
    def api(self) -> ApiClient:
        """The current API client."""
        return self.connection.api

    @property
    def api_ws(self) -> WsApiClient:
        """The current websocket-aware API client."""
        return self.connection.api_ws

    @property
    def v1(self) -> CoreV1Api:
        """A CoreV1Api instance associated with the current API client."""
        return self.connection.v1

    @property
    def v1_ws(self) -> CoreV1Api:
        """A CoreV1Api instance associated with the current websocket-aware API client."""
        return self.connection.v1_ws

    async def launch(self, job, task, manifest):
        """Create a pod with the given manifest, named and labeled for this podman's app and the given job and
        task."""
        assert manifest["kind"] == "Pod"
        task = task.replace("_", "-")

        manifest["metadata"] = manifest.get("metadata", {})
        manifest["metadata"].update(
            {
                "name": "%s-%s-%s" % (self.app, job, task),
                "labels": {
                    "app": self.app,
                    "task": task,
                    "job": job,
                },
            }
        )

        l.info("Creating task %s for job %s", task, job)
        await self.v1.create_namespaced_pod(self.namespace, manifest)

    async def query(self, job=None, task=None) -> List[V1Pod]:
        """Return a list of pods labeled for this podman's app and (optional) the given job and task."""
        selectors = ["app=" + self.app]
        if job is not None:
            selectors.append("job=" + job)
        if task is not None:
            selectors.append("task=" + task.replace("_", "-"))
        selector = ",".join(selectors)
        return (await self.v1.list_namespaced_pod(self.namespace, label_selector=selector)).items

    async def delete(self, pod: V1Pod):
        """Destroy the given pod."""
        await self.v1.delete_namespaced_pod(pod.metadata.name, self.namespace)

    async def logs(self, pod: V1Pod, timeout=10) -> str:
        """Retrieve the logs for the given pod."""
        return await self.v1.read_namespaced_pod_log(pod.metadata.name, self.namespace, _request_timeout=timeout)
