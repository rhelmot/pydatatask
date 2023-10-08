"""An executor is an environment in which a task can run.

Each executor should be able to host a variety of tasks.
"""

__all__ = (
    "Executor",
    "PodManager",
    "AbstractProcessManager",
    "LocalLinuxManager",
    "localhost_manager",
    "AbstractContainerManager",
    "DockerContainerManager",
    "KubeContainerManager",
    "localhost_docker_manager",
    "SSHLinuxFile",
    "SSHLinuxManager",
)

from .base import Executor
from .pod_manager import PodManager
from .proc_manager import AbstractProcessManager, LocalLinuxManager, localhost_manager, SSHLinuxFile, SSHLinuxManager
from .container_manager import (
    AbstractContainerManager,
    DockerContainerManager,
    KubeContainerManager,
    localhost_docker_manager,
)
