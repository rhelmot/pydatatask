"""
In order for a `KubeTask` or a subclass to connect, authenticate, and manage pods in a kubernetes cluster, it needs
several resource references. the `PodManager` simplifies tracking the lifetimes of these resources.
"""

from typing import Callable, List, Optional
import logging

from kubernetes_asyncio.client import ApiClient, CoreV1Api, V1Pod
from kubernetes_asyncio.config.kube_config import Configuration
from kubernetes_asyncio.stream import WsApiClient

l = logging.getLogger(__name__)

__all__ = ("PodManager",)


class PodManager:
    """
    A pod manager allows multiple tasks to share a connection to a kubernetes cluster and manage pods on it.
    """

    def __init__(
        self,
        app: str,
        namespace: str,
        config: Optional[Callable[[], Configuration]] = None,
    ):
        """
        :param app: The app name string with which to label all created pods.
        :param namespace: The namespace in which to create and query pods.
        :param config: Optional: A callable returning a kubernetes configuration object. If not provided, will attempt
                                 to use the "default" configuration, i.e. what is available after calling
                                 ``await kubernetes_asyncio.config.load_kube_config()``.
        """
        self.app = app
        self.namespace = namespace
        self._config = config

        self._api: Optional[ApiClient] = None
        self._api_ws: Optional[WsApiClient] = None
        self._v1 = None
        self._v1_ws = None

    @property
    def api(self):
        """
        The current API client.
        """
        if self._api is None:
            if self._config is None:
                self._api = ApiClient()
            else:
                self._api = ApiClient(self._config())
        return self._api

    @property
    def api_ws(self) -> WsApiClient:
        """
        The current websocket-aware API client.
        """
        if self._api_ws is None:
            if self._config is None:
                self._api_ws = WsApiClient()
            else:
                self._api_ws = WsApiClient(self._config())
        return self._api_ws

    @property
    def v1(self) -> CoreV1Api:
        """
        A CoreV1Api instance associated with the current API client.
        """
        if self._v1 is None:
            self._v1 = CoreV1Api(self.api)
        return self._v1

    @property
    def v1_ws(self) -> CoreV1Api:
        """
        A CoreV1Api instance associated with the current websocket-aware API client.
        """
        if self._v1_ws is None:
            self._v1_ws = CoreV1Api(self.api_ws)
        return self._v1_ws

    async def close(self):
        """
        Close the network connections associated with this podman.
        """
        if self._api is not None:
            await self._api.close()
            self._api = None
        if self._api_ws is not None:
            await self._api_ws.close()
            self._api_ws = None

    async def launch(self, job, task, manifest):
        """
        Create a pod with the given manifest, named and labeled for this podman's app and the given job and task.
        """
        assert manifest["kind"] == "Pod"

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
        """
        Return a list of pods labeled for this podman's app and (optional) the given job and task.
        """
        selectors = ["app=" + self.app]
        if job is not None:
            selectors.append("job=" + job)
        if task is not None:
            selectors.append("task=" + task)
        selector = ",".join(selectors)
        return (await self.v1.list_namespaced_pod(self.namespace, label_selector=selector)).items

    async def delete(self, pod: V1Pod):
        """
        Destroy the given pod.
        """
        await self.v1.delete_namespaced_pod(pod.metadata.name, self.namespace)

    async def logs(self, pod: V1Pod, timeout=10) -> str:
        """
        Retrieve the logs for the given pod.
        """
        return await self.v1.read_namespaced_pod_log(pod.metadata.name, self.namespace, _request_timeout=timeout)
