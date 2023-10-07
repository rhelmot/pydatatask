"""
An executor is an environment in which a task can run.

Each executor should be able to host a variety of tasks.
"""

from typing import TYPE_CHECKING

from pydatatask.host import Host

if TYPE_CHECKING:
    from .proc_manager import AbstractProcessManager
    from .pod_manager import PodManager
    from .container_manager import ContainerManagerAbstract


class Executor:
    def to_process_manager(self) -> "AbstractProcessManager":
        raise NotImplementedError(f"{type(self)} cannot host a bare process")

    def to_pod_manager(self) -> "PodManager":
        raise NotImplementedError(f"{type(self)} cannot host a kubernetes pod")

    def to_container_manager(self) -> "ContainerManagerAbstract":
        raise NotImplementedError(f"{type(self)} cannot host a linux container")

    @property
    def host(self) -> Host:
        raise NotImplementedError
