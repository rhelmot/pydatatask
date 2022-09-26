from typing import Optional, Union
import logging

from kubernetes.client import ApiClient, CoreV1Api
from kubernetes.utils import parse_quantity

l = logging.getLogger(__name__)

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

        self.cpu_usage = 0
        self.mem_usage = 0

        for pod in self.v1.list_namespaced_pod(self.namespace, label_selector='app=' + self.app).items:
            for container in pod.spec.containers:
                self.cpu_usage += parse_quantity(container.resources.requests['cpu'])
                self.mem_usage += parse_quantity(container.resources.requests['memory'])

    @property
    def v1(self):
        return CoreV1Api(self.api)

    def launch(self, job, task, manifest):
        assert manifest['kind'] == 'Pod'
        spec = manifest['spec']
        spec['restartPolicy'] = 'Never'

        cpu_request = 0
        mem_request = 0
        for container in spec['containers']:
            cpu_request += parse_quantity(container['resources']['requests']['cpu'])
            mem_request += parse_quantity(container['resources']['requests']['memory'])

        if cpu_request + self.cpu_usage > self.cpu_quota:
            l.info("Cannot launch %s-%s-%s - cpu limit" % (self.app, job, task))
            return False
        if mem_request + self.mem_usage > self.mem_quota:
            l.info("Cannot launch %s-%s-%s - memory limit" % (self.app, job, task))
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
        self.v1.create_namespaced_pod(self.namespace, manifest)
        self.cpu_usage += cpu_request
        self.mem_usage += mem_request
        return True

    def query(self, job=None, task=None):
        selectors = ['app=' + self.app]
        if job is not None:
            selectors.append('job=' + job)
        if task is not None:
            selectors.append('task=' + task)
        selector = ','.join(selectors)
        return self.v1.list_namespaced_pod(self.namespace, label_selector=selector).items

    def delete(self, pod):
        self.v1.delete_namespaced_pod(pod.metadata.name, self.namespace)
        for container in pod.spec.containers:
            self.cpu_usage -= parse_quantity(container.resources.requests['cpu'])
            self.mem_usage -= parse_quantity(container.resources.requests['memory'])

    def logs(self, pod):
        return self.v1.read_namespaced_pod_log(pod.metadata.name, self.namespace)
