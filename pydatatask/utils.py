"""Various utility classes and functions that are used throughout the codebase but don't belong anywhere in
particular."""

from typing import (
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Generic,
    List,
    Optional,
    Protocol,
    TypeVar,
    Union,
)
import codecs

_T = TypeVar("_T")


class AReadStream(Protocol):
    """A protocol for reading data from an asynchronous stream."""

    async def read(self, n: int = -1, /) -> bytes:
        """Read and return up to ``n`` bytes, or if unspecified, the rest of the stream."""

    async def close(self) -> None:
        """Close and release the stream."""


class AWriteStream(Protocol):
    """A protocol for writing data to an asynchronous stream."""

    async def write(self, data: Union[bytes, bytearray, memoryview], /) -> int:
        """Write ``data`` to the stream."""

    async def close(self) -> None:
        """Close and release the stream."""


class AReadStreamManager(AReadStream, AsyncContextManager, Protocol):
    """A protocol for reading data from an asynchronous stream with context management."""


class AWriteStreamManager(AWriteStream, AsyncContextManager, Protocol):
    """A protocol for writing data to an asynchronous stream with context management."""


async def async_copyfile(copyfrom: AReadStream, copyto: AWriteStream, blocksize=1024 * 1024):
    """Stream data from ``copyfrom`` to ``copyto``."""
    while True:
        data = await copyfrom.read(blocksize)
        if not data:
            break
        await copyto.write(data)


class AReadText:
    """An async version of :external:class:`io.TextIOWrapper` which can only handle reading."""

    def __init__(
        self,
        base: AReadStream,
        encoding: str = "utf-8",
        errors="strict",
        chunksize=4096,
    ):
        self.base = base
        self.decoder = codecs.lookup(encoding).incrementaldecoder(errors)
        self.buffer = ""
        self.chunksize = chunksize

    async def read(self, n: Optional[int] = None) -> str:
        """Read up to ``n`` chars from the string, or the rest of the stream if not provided."""
        while n is None or len(self.buffer) < n:
            data = await self.base.read(self.chunksize)
            self.buffer += self.decoder.decode(data, final=not bool(data))
            if not data:
                break

        if n is not None:
            result, self.buffer = self.buffer[:n], self.buffer[:n]
        else:
            result, self.buffer = self.buffer, ""
        return result

    async def close(self) -> None:
        """Close and release the stream."""
        await self.base.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class AWriteText:
    """An async version of ``io.TextIOWrapper`` which can only handle writing."""

    def __init__(self, base: AWriteStream, encoding="utf-8", errors="strict"):
        self.base = base
        self.encoding = encoding
        self.errors = errors

    async def write(self, data: str):
        """Write ``data`` to the stream."""
        await self.base.write(data.encode(self.encoding, self.errors))

    async def close(self) -> None:
        """Close and release the stream."""
        await self.base.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


async def async_copyfile_str(copyfrom: AReadText, copyto: AWriteText, blocksize=1024 * 1024):
    """Stream text from ``copyfrom`` to ``copyto``."""
    while True:
        data = await copyfrom.read(blocksize)
        if not data:
            break
        await copyto.write(data)


async def roundrobin(iterables: List):
    """An async version of the itertools roundrobin recipe.

    roundrobin('ABC', 'D', 'EF') --> A D E B F C
    """
    i = 0
    while iterables:
        i %= len(iterables)
        try:
            yield await iterables[i].__anext__()
        except StopAsyncIteration:
            iterables.pop(i)
        else:
            i += 1


class asyncasynccontextmanager(Generic[_T]):
    """Like asynccontextmanager but needs to be awaited first."""

    def __init__(self, f: Callable[..., AsyncIterator[_T]]):
        self.f = f
        self.live: Optional[AsyncIterator[_T]] = None

    async def __call__(self, *args, **kwargs) -> "asyncasynccontextmanager[_T]":
        self.live = self.f(*args, **kwargs)
        return self

    async def __aenter__(self) -> _T:
        assert self.live is not None
        return await self.live.__anext__()

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        pass
