from typing import Optional, Set, Union
import asyncio
import logging

from kubernetes.utils import parse_quantity
from kubernetes_asyncio.client import ApiClient, ApiException, CoreV1Api
from kubernetes_asyncio.stream import WsApiClient

l = logging.getLogger(__name__)

__all__ = ("PodManager",)


class PodManager:
    def __init__(
        self,
        app: str,
        namespace: str,
        cpu_quota: Union[str, int] = "1",
        mem_quota: Union[str, int] = "1Gi",
    ):
        self.app = app
        self.namespace = namespace
        self.cpu_quota = parse_quantity(cpu_quota)
        self.mem_quota = parse_quantity(mem_quota)
        self._api: Optional[ApiClient] = None
        self._api_ws: Optional[WsApiClient] = None

        self._cpu_usage = None
        self._mem_usage = None

        self.warned: Set[str] = set()
        self._v1 = None
        self._v1_ws = None

        self.lock = asyncio.Lock()

    @property
    def api(self):
        if self._api is None:
            self._api = ApiClient()
        return self._api

    @property
    def api_ws(self):
        if self._api_ws is None:
            self._api_ws = WsApiClient()
        return self._api_ws

    @property
    def v1(self):
        if self._v1 is None:
            self._v1 = CoreV1Api(self.api)
        return self._v1

    @property
    def v1_ws(self):
        if self._v1_ws is None:
            self._v1_ws = CoreV1Api(self.api_ws)
        return self._v1_ws

    async def close(self):
        if self._api is not None:
            await self._api.close()
            self._api = None
        if self._api_ws is not None:
            await self._api_ws.close()
            self._api_ws = None

    async def cpu_usage(self):
        await self._load_usage()
        return self._cpu_usage

    async def mem_usage(self):
        await self._load_usage()
        return self._mem_usage

    async def _load_usage(self):
        async with self.lock:
            if self._cpu_usage is not None:
                return
            cpu_usage = mem_usage = 0

            try:
                for pod in (await self.v1.list_namespaced_pod(self.namespace, label_selector="app=" + self.app)).items:
                    for container in pod.spec.containers:
                        cpu_usage += parse_quantity(container.resources.requests["cpu"])
                        mem_usage += parse_quantity(container.resources.requests["memory"])
            except ApiException as e:
                if e.reason != "Forbidden":
                    raise
            finally:
                self._cpu_usage = cpu_usage
                self._mem_usage = mem_usage

    async def launch(self, job, task, manifest):
        assert manifest["kind"] == "Pod"
        spec = manifest["spec"]
        spec["restartPolicy"] = "Never"

        cpu_request = 0
        mem_request = 0
        for container in spec["containers"]:
            cpu_request += parse_quantity(container["resources"]["requests"]["cpu"])
            mem_request += parse_quantity(container["resources"]["requests"]["memory"])

        await self._load_usage()
        async with self.lock:
            if cpu_request + self._cpu_usage > self.cpu_quota:
                if task not in self.warned:
                    self.warned.add(task)
                    l.info("Cannot launch %s - cpu limit", task)
                return False
            if mem_request + self._mem_usage > self.mem_quota:
                if task not in self.warned:
                    self.warned.add(task)
                    l.info("Cannot launch %s - memory limit", task)
                return False
            self._cpu_usage += cpu_request
            self._mem_usage += mem_request

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
        return True

    async def query(self, job=None, task=None):
        selectors = ["app=" + self.app]
        if job is not None:
            selectors.append("job=" + job)
        if task is not None:
            selectors.append("task=" + task)
        selector = ",".join(selectors)
        return (await self.v1.list_namespaced_pod(self.namespace, label_selector=selector)).items

    async def delete(self, pod):
        await self.v1.delete_namespaced_pod(pod.metadata.name, self.namespace)
        await self._load_usage()  # load into self
        async with self.lock:
            for container in pod.spec.containers:
                self._cpu_usage -= parse_quantity(container.resources.requests["cpu"])
                self._mem_usage -= parse_quantity(container.resources.requests["memory"])

    async def logs(self, pod, timeout=10):
        return await self.v1.read_namespaced_pod_log(pod.metadata.name, self.namespace, _request_timeout=timeout)
