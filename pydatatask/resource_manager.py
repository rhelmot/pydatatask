from typing import Callable, Awaitable, Optional, Union, List
from dataclasses import dataclass, field
from enum import Enum, auto
from asyncio import Lock
from decimal import Decimal

from kubernetes.utils import parse_quantity

__all__ = ('ResourceType', 'Resources', 'ResourceManager', 'parse_quantity')

class ResourceType(Enum):
    CPU = auto()
    MEM = auto()

@dataclass
class Resources:
    cpu: Decimal = field(default=Decimal(0))
    mem: Decimal = field(default=Decimal(0))

    @staticmethod
    def parse(cpu: Union[str, float, int, Decimal], mem: Union[str, float, int, Decimal]) -> 'Resources':
        return Resources(cpu=parse_quantity(cpu), mem=parse_quantity(mem))

    def __add__(self, other: 'Resources'):
        return Resources(cpu=self.cpu + other.cpu, mem=self.mem + other.mem)

    def __mul__(self, other: int):
        return Resources(cpu=self.cpu * other, mem=self.mem * other)

    def __sub__(self, other: 'Resources'):
        return self + other * -1

    def excess(self, limit: 'Resources') -> Optional[ResourceType]:
        if self.cpu > limit.cpu:
            return ResourceType.CPU
        elif self.mem > limit.mem:
            return ResourceType.MEM
        else:
            return None

class ResourceManager:
    def __init__(self, quota: Resources):
        self.quota = quota
        self.lock = Lock()
        self._cached: Optional[Resources] = None
        self._registered_getters: List[Callable[[], Awaitable[Resources]]] = []

    def register(self, func: Callable[[], Awaitable[Resources]]):
        self._registered_getters.append(func)

    async def getter(self):
        result = Resources()
        for getter in self._registered_getters:
            result += await getter()
        return result

    async def reserve(self, request: Resources) -> Optional[ResourceType]:
        async with self.lock:
            if self._cached is None:
                self._cached = await self.getter()

            target = self._cached + request
            excess = target.excess(self.quota)
            if excess is None:
                self._cached = target
                return None
            else:
                return excess

    async def relinquish(self, request: Resources):
        async with self.lock:
            if self._cached is None:
                self._cached = await self.getter()

            self._cached -= request
