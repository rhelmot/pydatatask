"""Pydatatask defines the notion of resources, or numerical quantities of CPU and memory which can be allocated to a
given job. This is mediated through a :class:`QuotaManager`, an object which can atomically track increments and
decrements from a quota, and reject a request if it would break the quota.

Typical usage is to construct a :class:`QuotaManager` and pass it to a task constructor:

.. code:: python

    quota = pydatatask.QuotaManager(pydatatask.Quota.parse(cpu='1000m', mem='1Gi'))
    task = pydatatask.ProcessTask("my_task", localhost, quota, ...)
"""

from typing import Awaitable, Callable, List, Optional, Union
from asyncio import Lock
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto

from kubernetes.utils import parse_quantity
import psutil

__all__ = ("QuotaType", "Quota", "QuotaManager", "parse_quantity", "localhost_quota_manager")


class QuotaType(Enum):
    """An enum class indicating a type of resource.

    Presently can be CPU, MEM, or RATE.
    """

    CPU = auto()
    MEM = auto()
    RATE = auto()


@dataclass
class Quota:
    """A dataclass containing a quantity of resources.

    Quotas can be summed:

    .. code:: python

        r = pydatatask.Quota.parse(1, 1, 100)
        r += pydatatask.Quota.parse(2, 3, 0)
        r -= pydatatask.Quota.parse(1, 1, 0)
        assert r == pydatatask.Quota.parse(2, 3, 100)
    """

    cpu: Decimal = field(default=Decimal(0))
    mem: Decimal = field(default=Decimal(0))
    launches: int = 1

    @staticmethod
    def parse(
        cpu: Union[str, float, int, Decimal],
        mem: Union[str, float, int, Decimal],
        launches: Union[str, float, int, Decimal, None] = None,
    ) -> "Quota":
        """Construct a :class:`Quota` instance by parsing the given quantities of CPU, memory, and launches."""
        if launches is None:
            launches = 999999999
        return Quota(cpu=parse_quantity(cpu), mem=parse_quantity(mem), launches=int(launches))

    def __add__(self, other: "Quota"):
        return Quota(cpu=self.cpu + other.cpu, mem=self.mem + other.mem, launches=self.launches + other.launches)

    def __mul__(self, other: int):
        return Quota(cpu=self.cpu * other, mem=self.mem * other, launches=self.launches * other)

    def __sub__(self, other: "Quota"):
        return self + other * -1

    def excess(self, limit: "Quota") -> Optional[QuotaType]:
        """Determine if these resources are over a given limit.

        :return: The QuotaType of the first resource that is over-limit, or None if self is under-limit.
        """
        if self.cpu > limit.cpu:
            return QuotaType.CPU
        elif self.mem > limit.mem:
            return QuotaType.MEM
        elif self.launches > limit.launches:
            return QuotaType.RATE
        else:
            return None


class QuotaManager:
    """The QuotaManager tracks quotas of resources.

    Direct use of this class beyond construction will only be necessary if you are writing a custom Task class.
    """

    def __init__(self, quota: Quota):
        """
        :param quota: The resource limit that should be applied to the sum of all tasks using this manager.
        """
        self.quota = quota
        self.__lock: Optional[Lock] = None
        self._cached: Optional[Quota] = None
        self._registered_getters: List[Callable[[], Awaitable[Quota]]] = []

    @property
    def _lock(self):
        if self.__lock is None:
            self.__lock = Lock()
        return self.__lock

    def register(self, func: Callable[[], Awaitable[Quota]]):
        """Register an async callback which will be used to load the current resource utilization on process start.
        The initial reported usage will be the sum of the result of all the registered callbacks.

        If you are writing a Task class, you should call this in your constructor to save a reference to a method on
        your class which retrieves the current resource utilization by that task.
        """
        self._registered_getters.append(func)

    async def _getter(self):
        result = Quota()
        for getter in self._registered_getters:
            result += await getter()
        return result

    async def flush(self):
        """Forget the cached amount of resources currently being used.

        Next call to reserve or relinquish will calculate usage anew.
        """
        async with self._lock:
            self._cached = None

    async def reserve(self, request: Quota) -> Optional[QuotaType]:
        """Atomically reserve the given amount of resources and return None, or do nothing and return the limiting
        resource type if any resource would be over-quota."""
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

    async def relinquish(self, request: Quota):
        """Atomically mark the given amount of resources as available."""
        async with self._lock:
            if self._cached is None:
                self._cached = await self._getter()

            self._cached -= request


localhost_quota_manager = QuotaManager(
    Quota.parse(cpu=psutil.cpu_count(), mem=psutil.virtual_memory().total, launches=1000)
)
