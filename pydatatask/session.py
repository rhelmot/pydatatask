from typing import Any, Callable, Coroutine, Union

__all__ = ("Session",)


class Session:
    def __init__(self):
        self._resource_defs = {}
        self.resources = {}

    def resource(self, manager: Union[str, Callable[[], Coroutine]]) -> Callable[[], Any]:
        if isinstance(manager, str):
            assert manager in self._resource_defs

            def inner():
                if manager not in self.resources:
                    raise Exception("Session is not open")
                return self.resources[manager]

            return inner
        else:
            self._resource_defs[manager.__name__] = manager()
            return self.resource(manager.__name__)

    async def __aenter__(self):
        await self.open()

    async def open(self):
        for name, manager in self._resource_defs.items():
            self.resources[name] = await manager.__anext__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        for name, manager in self._resource_defs.items():
            try:
                await manager.__anext__()
            except StopAsyncIteration:
                pass
            else:
                print("Warning: resource has more than one yield")
            self.resources.pop(name)
