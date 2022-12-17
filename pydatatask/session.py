"""
A session is a tool for managing multiple live resources. Async resource manager routines can be registered, and in
their place will be left a callable which will return the live resource while the session is opened.

For example:

.. code:: python

    session = pydatatask.Session()

    @session.resource
    async def my_file_dst():
        with open('/tmp/my_file_dst', 'wb') as fp:
            yield fp

    @session.resource
    async def my_file_src():
        with open('/tmp/my_file_src', 'rb') as fp:
            yield fp

    async with session:
        my_file_dst().write(my_file_src().read())

The advantage over using normal contextlib context managers is that this produces a function reference which can be
passed into other locations in sync contexts with the promise that it will not be called until the session is opened.

If a session is passed to a pipeline, it will be opened and closed when the pipeline is opened and closed.

Sessions cannot be opened more than once. But this doesn't have to be the way! If you have a use case, complain in a
GitHub issue, and I'll see what can be done.
"""
from typing import AsyncGenerator, Callable, TypeVar

__all__ = ("Session",)

T = TypeVar("T")


class Session:
    """
    The session class. See module docs for usage information.
    """

    def __init__(self):
        self._resource_defs = {}
        self.resources = {}

    def resource(self, manager: Callable[[], AsyncGenerator[T, None]]) -> Callable[[], T]:
        """
        Decorator for resource managers. Should be called with an async function that will yield exactly one object, the
        live constructed resource, and then tear that resource down on completion.
        """
        self._resource_defs[manager.__name__] = manager()

        def inner():
            if manager.__name__ not in self.resources:
                raise Exception("Session is not open")
            return self.resources[manager.__name__]

        return inner

    async def __aenter__(self):
        await self.open()

    async def open(self):
        """
        Open the session, initializing all the resources.

        This is automatically called when entering an ``async with session:`` block.
        """
        for name, manager in self._resource_defs.items():
            self.resources[name] = await manager.__anext__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """
        Close the session, tearing down all the resources.

        This is automatically called when exiting an ``async with session:`` block.
        """
        for name, manager in self._resource_defs.items():
            try:
                await manager.__anext__()
            except StopAsyncIteration:
                pass
            else:
                print("Warning: resource has more than one yield")
            self.resources.pop(name)
