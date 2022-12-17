"""
pydatatask defines the notion of resources, or numerical quantities of CPU and memory which can be allocated to a given
job. This is mediated through a :class:`ResourceManager`, an object which can atomically track increments and decrements
from a quota, and reject a request if it would break the quota.

Typical usage is to construct a :class:`ResourceManager` and pass it to a task constructor:

.. code:: python

    quota = pydatatask.ResourceManager(pydatatask.Resources.parse(cpu='1000m', mem='1Gi'))
    task = pydatatask.ProcessTask("my_task", localhost, quota, ...)
"""
from typing import Awaitable, Callable, List, Optional, Union
from asyncio import Lock
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto

from kubernetes.utils import parse_quantity

__all__ = ("ResourceType", "Resources", "ResourceManager", "parse_quantity")


class ResourceType(Enum):
    """
    An enum class indicating a type of resource. Presently can be CPU or MEM.
    """

    CPU = auto()
    MEM = auto()


@dataclass
class Resources:
    """
    A dataclass containing a quantity of resources.

    Resources can be summed:

    .. code:: python

        r = pydatatask.Resources.parse(1, 1)
        r += pydatatask.Resources.parse(2, 3)
        r -= pydatatask.Resources.parse(1, 1)
        assert r == pydatatask.Resources.parse(2, 3)
    """

    cpu: Decimal = field(default=Decimal(0))
    mem: Decimal = field(default=Decimal(0))

    @staticmethod
    def parse(cpu: Union[str, float, int, Decimal], mem: Union[str, float, int, Decimal]) -> "Resources":
        """
        Construct a :class:`Resources` instance by parsing the given quantities of CPU and memory.
        """
        return Resources(cpu=parse_quantity(cpu), mem=parse_quantity(mem))

    def __add__(self, other: "Resources"):
        return Resources(cpu=self.cpu + other.cpu, mem=self.mem + other.mem)

    def __mul__(self, other: int):
        return Resources(cpu=self.cpu * other, mem=self.mem * other)

    def __sub__(self, other: "Resources"):
        return self + other * -1

    def excess(self, limit: "Resources") -> Optional[ResourceType]:
        """
        Determine if these resources are over a given limit.

        :return: The ResourceType of the first resource that is over-limit, or None if self is under-limit.
        """
        if self.cpu > limit.cpu:
            return ResourceType.CPU
        elif self.mem > limit.mem:
            return ResourceType.MEM
        else:
            return None


class ResourceManager:
    """
    The ResourceManager tracks quotas of resources. Direct use of this class beyond construction will only be necessary
    if you are writing a custom Task class.
    """

    def __init__(self, quota: Resources):
        """
        :param quota: The resource limit that should be applied to the sum of all tasks using this manager.
        """
        self.quota = quota
        self._lock = Lock()
        self._cached: Optional[Resources] = None
        self._registered_getters: List[Callable[[], Awaitable[Resources]]] = []

    def register(self, func: Callable[[], Awaitable[Resources]]):
        """
        Register an async callback which will be used to load the current resource utilization on process start.
        The initial reported usage will be the sum of the result of all the registered callbacks.

        If you are writing a Task class, you should call this in your constructor to save a reference to a method on
        your class which retrieves the current resource utilization by that task.
        """
        self._registered_getters.append(func)

    async def _getter(self):
        result = Resources()
        for getter in self._registered_getters:
            result += await getter()
        return result

    async def reserve(self, request: Resources) -> Optional[ResourceType]:
        """
        Atomically reserve the given amount of resources and return None, or do nothing and return the limiting resource
        type if any resource would be over-quota.
        """
        async with self._lock:
            if self._cached is None:
                self._cached = await self._getter()

            target = self._cached + request
            excess = target.excess(self.quota)
            if excess is None:
                self._cached = target
                return None
            else:
                return excess

    async def relinquish(self, request: Resources):
        """
        Atomically mark the given amount of resources as available.
        """
        async with self._lock:
            if self._cached is None:
                self._cached = await self._getter()

            self._cached -= request
