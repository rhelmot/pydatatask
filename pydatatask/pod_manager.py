from typing import Optional, Union
import logging
import asyncio

from kubernetes_asyncio.client import ApiClient, CoreV1Api, ApiException
from kubernetes.utils import parse_quantity

l = logging.getLogger(__name__)

__all__ = ('PodManager',)

class PodManager:
    def __init__(
            self,
            app: str,
            namespace: str,
            api_client: Optional[ApiClient],
            cpu_quota: Union[str, int],
            mem_quota: Union[str, int]
    ):
        self.app = app
        self.namespace = namespace
        self.cpu_quota = parse_quantity(cpu_quota)
        self.mem_quota = parse_quantity(mem_quota)
        self.api = api_client

        self._cpu_usage = None
        self._mem_usage = None

        self.warned = set()

    @property
    def cpu_usage(self):
        if self._cpu_usage is None:
            asyncio.get_event_loop().run_until_complete(self._load_usage())
        return self._cpu_usage

    @cpu_usage.setter
    def cpu_usage(self, v):
        self._cpu_usage = v

    @property
    def mem_usage(self):
        if self._mem_usage is None:
            asyncio.get_event_loop().run_until_complete(self._load_usage())
        return self._mem_usage

    @mem_usage.setter
    def mem_usage(self, v):
        self._mem_usage = v

    async def _load_usage(self):
        cpu_usage = mem_usage = 0

        try:
            for pod in await self.v1.list_namespaced_pod(self.namespace, label_selector='app=' + self.app).items:
                for container in pod.spec.containers:
                    self._cpu_usage += parse_quantity(container.resources.requests['cpu'])
                    self._mem_usage += parse_quantity(container.resources.requests['memory'])
        except ApiException as e:
            if e.reason != "Forbidden":
                raise
        finally:
            self.cpu_usage = cpu_usage
            self.mem_usage = mem_usage

    @property
    def v1(self):
        return CoreV1Api(self.api)

    async def launch(self, job, task, manifest):
        assert manifest['kind'] == 'Pod'
        spec = manifest['spec']
        spec['restartPolicy'] = 'Never'

        cpu_request = 0
        mem_request = 0
        for container in spec['containers']:
            cpu_request += parse_quantity(container['resources']['requests']['cpu'])
            mem_request += parse_quantity(container['resources']['requests']['memory'])

        if cpu_request + self.cpu_usage > self.cpu_quota:
            if task not in self.warned:
                self.warned.add(task)
                l.info("Cannot launch %s - cpu limit", task)
            return False
        if mem_request + self.mem_usage > self.mem_quota:
            if task not in self.warned:
                self.warned.add(task)
                l.info("Cannot launch %s - memory limit", task)
            return False

        manifest['metadata'] = manifest.get('metadata', {})
        manifest['metadata'].update({
            'name': '%s-%s-%s' % (self.app, job, task),
            'labels': {
                'app': self.app,
                'task': task,
                'job': job,
            }
        })

        l.info("Creating task %s for job %s", task, job)
        await self.v1.create_namespaced_pod(self.namespace, manifest)
        self.cpu_usage += cpu_request
        self.mem_usage += mem_request
        return True

    async def query(self, job=None, task=None):
        selectors = ['app=' + self.app]
        if job is not None:
            selectors.append('job=' + job)
        if task is not None:
            selectors.append('task=' + task)
        selector = ','.join(selectors)
        return await self.v1.list_namespaced_pod(self.namespace, label_selector=selector).items

    async def delete(self, pod):
        await self.v1.delete_namespaced_pod(pod.metadata.name, self.namespace)
        for container in pod.spec.containers:
            self.cpu_usage -= parse_quantity(container.resources.requests['cpu'])
            self.mem_usage -= parse_quantity(container.resources.requests['memory'])

    async def logs(self, pod, timeout=10):
        return await self.v1.read_namespaced_pod_log(pod.metadata.name, self.namespace, _request_timeout=timeout)
