"""
This module contains repositories for viewing the current state of a kubernetes cluster as a data store.
"""

from typing import TYPE_CHECKING, List

from kubernetes_asyncio.client import V1Pod

from .base import Repository, job_getter

if TYPE_CHECKING:
    from ..task import KubeTask


class LiveKubeRepository(Repository):
    """
    A repository where keys translate to ``job`` labels on running kube pods. This repository is constructed
    automatically by a `KubeTask` or subclass and is linked as the ``live`` repository. Do not construct this class
    manually.
    """

    def __init__(self, task: "KubeTask"):
        self.task = task

    async def unfiltered_iter(self):
        for pod in await self.pods():
            yield pod.metadata.labels["job"]

    async def contains(self, item):
        return bool(await self.task.podman.query(task=self.task.name, job=item))

    def __repr__(self):
        return f"<LiveKubeRepository task={self.task.name}>"

    @job_getter
    async def info(self, job):
        """
        Cannot template with live kube info. Implement this if you have something in mind.
        """
        return None

    async def pods(self) -> List[V1Pod]:
        """
        A list of live pod objects corresponding to this repository.
        """
        return await self.task.podman.query(task=self.task.name)

    async def delete(self, job):
        """
        Deleting a job from this repository will delete the pod.
        """
        pods = await self.task.podman.query(job=job, task=self.task.name)
        for pod in pods:  # there... really should be only one
            await self.task.delete(pod)
        # while await self.task.podman.query(job=job, task=self.task.name):
        #    await asyncio.sleep(0.2)
