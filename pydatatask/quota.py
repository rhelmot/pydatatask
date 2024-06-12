"""Pydatatask defines the notion of resources, or numerical quantities of CPU and memory which can be allocated to a
given job. This is mediated through a :class:`QuotaManager`, an object which can atomically track increments and
decrements from a quota, and reject a request if it would break the quota.

Typical usage is to construct a :class:`QuotaManager` and pass it to a task constructor:

.. code:: python

    quota = pydatatask.QuotaManager(pydatatask.Quota.parse(cpu='1000m', mem='1Gi'))
    task = pydatatask.ProcessTask("my_task", localhost, quota, ...)

TODO REWRITE THIS GODDAMN SHIT
"""

from typing import Awaitable, Callable, List, Optional, Union
from asyncio import Lock
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum, auto

from kubernetes.utils import parse_quantity
from typing_extensions import Self
import psutil

__all__ = ("QuotaType", "Quota", "parse_quantity")


class QuotaType(Enum):
    """An enum class indicating a type of resource.

    Presently can be CPU, MEM, or RATE.
    """

    CPU = auto()
    MEM = auto()
    RATE = auto()


@dataclass(eq=False)
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

    @classmethod
    def parse(
        cls,
        cpu: Union[str, float, int, Decimal],
        mem: Union[str, float, int, Decimal],
        launches: Union[str, float, int, Decimal, None] = None,
    ) -> Self:
        """Construct a :class:`Quota` instance by parsing the given quantities of CPU, memory, and launches."""
        if launches is None:
            launches = 999999999
        return cls(cpu=parse_quantity(cpu), mem=parse_quantity(mem), launches=int(launches))

    def __add__(self, other: Self):
        return type(self)(cpu=self.cpu + other.cpu, mem=self.mem + other.mem, launches=self.launches + other.launches)

    def __mul__(self, other: int):
        return type(self)(cpu=self.cpu * other, mem=self.mem * other, launches=self.launches * other)

    def __sub__(self, other: Self):
        return self + other * -1

    def excess(self, limit: Self) -> Optional[QuotaType]:
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


LOCALHOST_QUOTA = Quota.parse(cpu=psutil.cpu_count(), mem=psutil.virtual_memory().total, launches=1000)
