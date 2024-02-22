"""This module houses the filesystem repositories, or repositories whose data is a whole filesystem."""

from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
from dataclasses import dataclass
from enum import Enum, auto
from hashlib import sha256
from itertools import filterfalse, tee
from pathlib import Path
import abc
import io
import logging
import os
import stat
import tarfile

from aiofiles.threadpool import open as aopen
import aiofiles.os
import aioshutil

from pydatatask.repository import FileRepositoryBase
from pydatatask.repository.base import (
    BlobRepository,
    FileRepository,
    MetadataRepository,
    Repository,
    job_getter,
)
from pydatatask.utils import (
    AReadStreamBase,
    AWriteStreamBase,
    AWriteStreamWrapper,
    ReadSyncStreamAsyncWrapper,
    async_copyfile,
    asyncasynccontextmanager,
)

l = logging.getLogger(__name__)

__all__ = (
    "FilesystemType",
    "FilesystemEntry",
    "FilesystemRepository",
    "DirectoryRepository",
    "ContentAddressedBlobRepository",
    "TarfileFilesystemRepository",
)


class FilesystemType(Enum):
    """The type of a filesystem member."""

    FILE = auto()
    DIRECTORY = auto()
    SYMLINK = auto()

    @classmethod
    def from_name(cls, name: str) -> "FilesystemType":
        """Convert a filesystem member name string into an enum value."""
        return vars(cls)[name]


@dataclass
class FilesystemEntry:
    """An entry representing the full stored data of a single member of the filesystem."""

    name: str
    type: FilesystemType
    mode: int
    data: Optional[AReadStreamBase] = None
    link_target: Optional[str] = None
    content_hash: Optional[str] = None
    content_size: Optional[int] = None


class FilesystemRepository(Repository, abc.ABC):
    """The base class for repositories whose members are whole directory structures.

    The filesystem schema is somewhat simplified, only allowing files, directories, and symlinks.
    """

    @abc.abstractmethod
    def walk(self, job: str) -> AsyncIterator[Tuple[str, List[str], List[str], List[str]]]:
        """The os.walk interface but for the repository, but returns three lists - dirs, regular files, symlinks"""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_type(self, job: str, path: str) -> Optional[FilesystemType]:
        """Given a path, get the type of the file at that path."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_mode(self, job: str, path: str) -> Optional[int]:
        """Given a path, get the type of the file at that path."""
        raise NotImplementedError

    @abc.abstractmethod
    async def readlink(self, job: str, path: str) -> str:
        """Given a path to a symlink, return the path it points to."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_regular_meta(self, job: str, path: str) -> Tuple[int, Optional[str]]:
        """Given a path to a regular file, return the filesize and content hash (if available)."""
        raise NotImplementedError

    @abc.abstractmethod
    def dump(self, job: str) -> AsyncGenerator[None, FilesystemEntry]:
        """A generator interface for adding an entire filesystem as a value.

        See the `dump_tarball` implementation for an example of how to use this.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def open(self, job: str, path: Union[str, Path]) -> AsyncContextManager[AReadStreamBase]:
        """Open a file from the filesystem repository for reading.

        The file is opened with mode ``rb``.
        """
        raise NotImplementedError

    def construct_backup_repo(self, path: Path, force_compress: Optional[bool] = None) -> "FilesystemRepository":
        """Construct a repository appropriate for backing up this repository to the given path."""
        if (self.compress_backup or force_compress is True) and force_compress is not False:
            return TarfileFilesystemRepository(FileRepository(path, extension=".tar.gz"))
        else:
            return DirectoryRepository(path)

    async def dump_tarball(self, job: str, stream: AReadStreamBase) -> None:
        """Add an entire filesystem from a tarball."""

        cursor = self.dump(job)
        await cursor.__anext__()
        tar_stream = io.BytesIO(await stream.read())
        with tarfile.open(mode="r|*", fileobj=tar_stream) as tar:
            for entry in tar:
                if entry.issym():
                    await cursor.asend(
                        FilesystemEntry(
                            entry.name,
                            FilesystemType.SYMLINK,
                            entry.mode,
                            link_target=entry.linkpath,
                        )
                    )
                elif entry.isdir():
                    await cursor.asend(FilesystemEntry(entry.name, FilesystemType.DIRECTORY, entry.mode))
                elif entry.isfile():
                    await cursor.asend(
                        FilesystemEntry(
                            entry.name,
                            FilesystemType.FILE,
                            entry.mode,
                            data=ReadSyncStreamAsyncWrapper(tar.extractfile(entry)),
                        )
                    )
                else:
                    l.warning("Could not dump archive member %s: bad type %s", entry.name, entry.type)
        await cursor.aclose()

    async def iter_members(self, job: str) -> AsyncIterator[FilesystemEntry]:
        """Read out an entire filesystem iteratively, as a series of FilesystemEntry objects."""
        async for rtop, dirs, regulars, symlinks in self.walk(job):
            rtopp = Path(rtop)
            for name in dirs:
                fullname = rtopp / name
                perms = await self.get_mode(job, str(fullname))
                assert perms is not None
                yield FilesystemEntry(str(fullname), FilesystemType.DIRECTORY, perms)
            for name in symlinks:
                fullname = rtopp / name
                target = await self.readlink(job, str(fullname))
                perms = await self.get_mode(job, str(fullname))
                assert perms is not None
                yield FilesystemEntry(str(fullname), FilesystemType.SYMLINK, perms, link_target=target)
            for name in regulars:
                fullname = rtopp / name
                size, contenthash = await self.get_regular_meta(job, str(fullname))
                perms = await self.get_mode(job, str(fullname))
                assert perms is not None
                async with await self.open(job, fullname) as fp:
                    yield FilesystemEntry(
                        str(fullname), FilesystemType.FILE, perms, data=fp, content_size=size, content_hash=contenthash
                    )

    async def get_tarball(self, job: str, dest: AWriteStreamBase) -> None:
        """Stream a tarball for the given job to the provided asynchronous stream."""
        output_io = io.BytesIO()
        with tarfile.open(mode="w|gz", fileobj=output_io) as tar:
            async for member in self.iter_members(job):
                if member.type == FilesystemType.FILE:
                    assert member.data is not None
                    tarinfo = tarfile.TarInfo(member.name)
                    tarinfo.size = member.content_size
                    tarinfo.mode = member.mode
                    tar.addfile(tarinfo, io.BytesIO(await member.data.read()))
                elif member.type == FilesystemType.SYMLINK:
                    assert member.link_target is not None
                    tarinfo = tarfile.TarInfo(member.name)
                    tarinfo.mode = member.mode
                    tarinfo.type = tarfile.SYMTYPE
                    tarinfo.linkname = member.link_target
                    tar.addfile(tarinfo)
                elif member.type == FilesystemType.DIRECTORY:
                    tarinfo = tarfile.TarInfo(member.name)
                    tarinfo.mode = member.mode
                    tarinfo.type = tarfile.DIRTYPE
                    tar.addfile(tarinfo)
        output_io.seek(0)
        await dest.write(output_io.read())


# https://github.com/Tinche/aiofiles/issues/167
async def _walk_inner(top, onerror, followlinks):
    dirs = []
    nondirs = []

    # We may not have read permission for top, in which case we can't
    # get a list of the files the directory contains.  os.walk
    # always suppressed the exception then, rather than blow up for a
    # minor reason when (say) a thousand readable directories are still
    # left to visit.  That logic is copied here.
    try:
        scandir_it = await aiofiles.os.scandir(top)
    except OSError as error:
        if onerror is not None:
            onerror(error)
        return

    with scandir_it:
        while True:
            try:
                try:
                    entry = next(scandir_it)
                except StopIteration:
                    break
            except OSError as error:
                if onerror is not None:
                    onerror(error)
                return

            try:
                is_dir = entry.is_dir()
            except OSError:
                # If is_dir() raises an OSError, consider that the entry is not
                # a directory, same behaviour than os.path.isdir().
                is_dir = False

            if is_dir:
                dirs.append(entry.name)
            else:
                nondirs.append(entry.name)

    yield top, dirs, nondirs

    # Recurse into sub-directories
    islink, join = aiofiles.os.path.islink, os.path.join  # type: ignore
    for dirname in dirs:
        new_path = join(top, dirname)

        if followlinks or not await islink(new_path):
            async for x in _walk_inner(new_path, onerror, followlinks):
                yield x


async def _walk(top, onerror=None, followlinks=False) -> AsyncIterator[Tuple[str, List[str], List[str]]]:
    async for top, dirs, nondirs in _walk_inner(top, onerror, followlinks):
        yield top, dirs, nondirs


class DirectoryRepository(FilesystemRepository, FileRepositoryBase):
    """A directory repository is a repository which stores its data as a basic on-local-disk filesystem."""

    def __init__(
        self, basedir: Union[str, Path], extension: str = "", case_insensitive: bool = False, discard_empty=True
    ):
        """
        :param discard_empty: Whether only directories containing at least one member should be considered as "present"
                              in the repository.
        """
        super().__init__(basedir=basedir, extension=extension, case_insensitive=case_insensitive)
        self.discard_empty = discard_empty

    def footprint(self):
        yield self

    async def delete(self, job, /):
        if await self.contains(job):
            await aioshutil.rmtree(self.fullpath(job))

    @job_getter
    async def mkdir(self, job: str) -> None:
        """Create an empty directory root for the given key."""
        try:
            await aiofiles.os.mkdir(self.fullpath(job))
        except FileExistsError:
            pass

    async def contains(self, item, /):
        result = await super().contains(item)
        if not self.discard_empty:
            return result
        if not result:
            return False
        return bool(list(await aiofiles.os.listdir(self.fullpath(item))))

    async def unfiltered_iter(self):
        async for item in super().unfiltered_iter():
            if self.discard_empty:
                if bool(list(await aiofiles.os.listdir(self.fullpath(item)))):
                    yield item
            else:
                yield item

    async def readlink(self, job: str, path: str) -> str:
        """Given a path to a symlink, return the path it points to."""
        return os.readlink(self.fullpath(job) / path)

    async def get_regular_meta(self, job: str, path: str) -> Tuple[int, str]:
        """Given a path to a regular file, return the filesize and content hash."""
        size = 0
        h = sha256()
        async with aiofiles.open(self.fullpath(job) / path, "rb") as fp:
            while True:
                data = await fp.read(1024 * 16)
                if not data:
                    break
                h.update(data)
                size += len(data)
        return size, h.hexdigest()

    async def walk(self, job: str) -> AsyncIterator[Tuple[str, List[str], List[str], List[str]]]:
        root = self.fullpath(job)

        def mk_islink(top):
            def islink(path):
                return (Path(top) / path).is_symlink()

            return islink

        async for top, dirs, nondirs in _walk(root):
            ttop = Path(top)
            rtop = ttop.relative_to(root)
            regs, links = partition(mk_islink(top), nondirs)
            yield str(rtop), dirs, list(regs), list(links)

    async def get_type(self, job: str, path: str) -> Optional[FilesystemType]:
        r = os.stat(os.path.join(self.fullpath(job), path))
        if r.st_mode & stat.S_IFLNK:
            return FilesystemType.SYMLINK
        if r.st_mode & stat.S_IFDIR:
            return FilesystemType.DIRECTORY
        if r.st_mode & stat.S_IFREG:
            return FilesystemType.FILE
        return None

    async def get_mode(self, job: str, path: str) -> Optional[int]:
        r = os.lstat(os.path.join(self.fullpath(job), path))
        if r is not None:
            return stat.S_IMODE(r.st_mode)
        return None

    async def dump(self, job: str) -> AsyncGenerator[None, FilesystemEntry]:
        root = self.fullpath(job)
        try:
            while True:
                entry = yield
                subpath = Path(entry.name)
                if subpath.is_absolute():
                    ssubpath = os.path.join(*subpath.parts[1:])
                else:
                    ssubpath = str(subpath)
                fullpath = root / ssubpath
                if entry.type != FilesystemType.DIRECTORY:
                    fullpath.parent.mkdir(exist_ok=True, parents=True)
                    if entry.type == FilesystemType.FILE:
                        assert entry.data is not None
                        async with aopen(fullpath, "wb") as fp:
                            await async_copyfile(entry.data, fp)

                        fullpath.chmod(entry.mode)
                    else:
                        assert entry.link_target is not None
                        try:
                            os.unlink(fullpath)
                        except FileNotFoundError:
                            pass
                        fullpath.symlink_to(entry.link_target)
                else:
                    fullpath.mkdir(exist_ok=True, parents=True)
                    fullpath.chmod(entry.mode)
        except StopAsyncIteration:
            pass

    @asyncasynccontextmanager
    async def open(self, job: str, path: Union[str, Path]) -> AsyncIterator[AReadStreamBase]:
        async with aopen(os.path.join(self.fullpath(job), path), "rb") as fp:
            yield fp


def partition(pred, iterable):
    """Partition entries into false entries and true entries.

    If *pred* is slow, consider wrapping it with functools.lru_cache().
    """
    # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = tee(iterable)
    return filterfalse(pred, t1), filter(pred, t2)


def _splitdirs(directory):
    return partition(lambda elem: elem["type"] == "DIRECTORY", directory)


def _splitlinks(directory):
    return partition(lambda elem: elem["type"] == "SYMLINK", directory)


class ContentAddressedBlobRepository(FilesystemRepository):
    """A repository which uses a blob repository and a metadata repository to store files in a deduplicated
    fashion."""

    def __init__(self, blobs: BlobRepository, meta: MetadataRepository, pathsep="/"):
        super().__init__()
        self.blobs = blobs
        self.meta = meta
        self.pathsep = pathsep

    def footprint(self):
        # This COULD be split out into a more base level footprint. however this produces a more legible backup result
        yield self

    def __getstate__(self):
        return (self.blobs, self.meta, self.pathsep)

    async def delete(self, job: str, /):
        # UHHHHHHHHHHHHHHHHHHHHHHHH WHERE REFCOUNTING
        await self.meta.delete(job)

    def unfiltered_iter(self):
        return self.meta.unfiltered_iter()

    async def walk(self, job: str) -> AsyncIterator[Tuple[str, List[str], List[str], List[str]]]:
        meta = await self.meta.info(job)
        if not isinstance(meta, list):
            raise ValueError("Error: corrupted ContentAddressedBlobRepository meta")

        queue = [(meta, self.pathsep)]
        while queue:
            meta, path = queue.pop()
            nondirs, dirs = _splitdirs(meta)
            links, regs = _splitlinks(nondirs)
            links, dirs, regs = list(links), list(dirs), list(regs)
            link_names = [path + elem["name"] for elem in links]
            reg_names = [path + elem["name"] for elem in regs]
            dir_names = [path + elem["name"] for elem in dirs]
            dir_mapping = dict(zip(dir_names, dirs))
            yield path, dir_names, reg_names, link_names

            for name in dir_names:
                queue.append((dir_mapping[name], name))

    def _follow_path(self, meta: List[Dict[str, Any]], path: List[str], insert: bool = False) -> Dict[str, Any]:
        for i, key in enumerate(path[:-1]):
            for child in meta:
                if child["name"] == key:
                    break
            else:
                if insert:
                    child = {
                        "name": key,
                        "type": "DIRECTORY",
                        "mode": 0o755,
                        "children": [],
                    }
                    meta.append(child)
                else:
                    raise KeyError(f"No such file: {self.pathsep.join(path[:i+1])}")
            if child["type"] != "DIRECTORY":
                raise ValueError(f"{key} is not a directory")
            meta = child["children"]
        for child in meta:
            if child["name"] == path[-1]:
                break
        else:
            if insert:
                child = {"name": path[-1]}
                meta.append(child)
            else:
                raise KeyError(f"Could not lookup {self.pathsep.join(path)}")
        return child

    def _splitpath(self, path: str, relative_to: Optional[List[str]] = None) -> List[str]:
        split = [x for x in path.strip(self.pathsep).split(self.pathsep) if x != "."]
        if relative_to is not None:
            split = relative_to + split
        while ".." in split:
            index = split.index("..")
            if index == 0:
                split.pop(0)
            else:
                split.pop(index - 1)
                split.pop(index - 1)
        return split

    async def get_type(self, job: str, path: str) -> Optional[FilesystemType]:
        info = await self.meta.info(job)
        split = self._splitpath(path)
        try:
            child_info = self._follow_path(info, split)
        except (ValueError, KeyError):
            return None
        return FilesystemType.from_name(child_info["type"])

    async def get_mode(self, job: str, path: str) -> Optional[int]:
        info = await self.meta.info(job)
        split = self._splitpath(path)
        try:
            child_info = self._follow_path(info, split)
        except (ValueError, KeyError):
            return None
        return child_info["mode"]

    async def dump(self, job: str) -> AsyncGenerator[None, FilesystemEntry]:
        meta: List[Dict[str, Any]] = []

        try:
            while True:
                entry = yield
                subpath = self._splitpath(entry.name)
                submeta = self._follow_path(meta, subpath, insert=True)
                submeta["mode"] = entry.mode
                if entry.type == FilesystemType.DIRECTORY:
                    submeta["type"] = "DIRECTORY"
                    submeta["children"] = []
                elif entry.type == FilesystemType.SYMLINK:
                    submeta["type"] = "SYMLINK"
                    submeta["link_target"] = entry.link_target
                elif entry.type == FilesystemType.FILE:
                    assert entry.data is not None
                    submeta["type"] = "FILE"
                    if entry.content_hash is None:
                        all_content = await entry.data.read()  # uhhhhhhhhhhhhhhhhhhhhhh not good
                        hash_content = sha256(all_content).hexdigest()
                        async with await self.blobs.open(hash_content, "wb") as fp:
                            await fp.write(all_content)
                        size = len(all_content)
                    else:
                        hash_content = entry.content_hash
                        h = sha256()
                        size = 0
                        async with await self.blobs.open(hash_content, "wb") as fp:
                            while True:
                                data = await entry.data.read(1024 * 16)
                                if not data:
                                    break
                                h.update(data)
                                size += len(data)
                                await fp.write(data)
                        if h.hexdigest() != hash_content:
                            raise ValueError("Bad content hash")

                    submeta["content"] = hash_content
                    submeta["size"] = size
        except GeneratorExit:
            pass

        await self.meta.dump(job, meta)

    async def open(self, job: str, path: Union[str, Path]) -> AsyncContextManager[AReadStreamBase]:
        return await self._open(job, path)

    async def _open(
        self, job: str, path: Union[str, Path, List[str]], link_level: int = 0
    ) -> AsyncContextManager[AReadStreamBase]:
        if not isinstance(path, list):
            ppath = self._splitpath(str(path))
        else:
            ppath = path
        meta = await self.meta.info(job)
        while True:
            child = self._follow_path(meta, ppath)
            if child["type"] == "DIRECTORY":
                raise ValueError("Tried to open a directory")
            if child["type"] == "SYMLINK":
                cpath = child["link_target"]
                if cpath.startswith("/"):
                    clpath = self._splitpath(cpath)
                else:
                    clpath = self._splitpath(cpath, ppath[:-1])
                if link_level > 10:
                    raise ValueError("Too many levels of symbolic links")
                return await self._open(job, clpath, link_level + 1)
            if child["type"] == "FILE":
                return await self.blobs.open(child["content"], "rb")

    async def readlink(self, job: str, path: str) -> str:
        meta = await self.meta.info(job)
        ppath = self._splitpath(path)
        child = self._follow_path(meta, ppath)
        if child["type"] != "SYMLINK":
            raise ValueError("Not a symlink")

        return child["link_target"]

    async def get_regular_meta(self, job: str, path: str) -> Tuple[int, str]:
        meta = await self.meta.info(job)
        ppath = self._splitpath(path)
        child = self._follow_path(meta, ppath)
        if child["type"] != "FILE":
            raise ValueError("Not a regular file")

        return child["size"], child["content"]


class TarfileFilesystemRepository(FilesystemRepository):
    def __init__(self, inner: BlobRepository):
        super().__init__()
        self.inner = inner

    def footprint(self):
        # This COULD be split out into a more base level footprint. however this produces a more legible backup result
        yield self

    def __getstate__(self):
        return (self.inner,)

    # all of this except for dump_tarball and get_tarball will be hella slow
    # pending https://github.com/dignifiedquire/async-tar/issues/47

    async def walk(self, job: str) -> AsyncIterator[Tuple[str, List[str], List[str], List[str]]]:
        # NOTE: does not implement the part where you can manipulate the yielded dir list to skip subtrees
        members = {}

        async with await self.inner.open(job, "rb") as fp:
            tar_bytes = io.BytesIO(await fp.read())
            tar = tarfile.open(fileobj=tar_bytes, mode="r|*")
            for member in tar:
                if member.isdir():
                    idx = 0
                elif member.isfile():
                    idx = 1
                elif member.issym():
                    idx = 2
                else:
                    continue

                name = member.name
                dirname, basename = name.rsplit("/", 1)
                while not basename:
                    dirname, basename = dirname.rsplit("/", 1)

                if dirname not in members:
                    members[dirname] = ([], [], [])
                members[dirname][idx].append(name)

        for name, (d, r, s) in members.items():
            yield name, d, r, s

    async def get_type(self, job: str, path: str) -> Optional[FilesystemType]:
        async with await self.inner.open(job, "rb") as fp:
            tar_bytes = io.BytesIO(await fp.read())
            tar = tarfile.open(fileobj=tar_bytes, mode="r|*")
            for member in tar:
                if member.name.strip("/") != path.strip("/"):
                    continue

                if member.isdir():
                    return FilesystemType.DIRECTORY
                elif member.isfile():
                    return FilesystemType.FILE
                elif member.issym():
                    return FilesystemType.SYMLINK
                else:
                    return None
        return None

    async def get_mode(self, job: str, path: str) -> Optional[int]:
        async with await self.inner.open(job, "rb") as fp:
            tar_bytes = io.BytesIO(await fp.read())
            tar = tarfile.open(fileobj=tar_bytes, mode="r|*")
            for member in tar:
                if member.name.strip("/") == path.strip("/"):
                    return member.mode
        return None

    async def readlink(self, job: str, path: str) -> str:
        async with await self.inner.open(job, "rb") as fp:
            tar_bytes = io.BytesIO(await fp.read())
            tar = tarfile.open(fileobj=tar_bytes, mode="r|*")
            for member in tar:
                if member.name.strip("/") == path.strip("/"):
                    return member.linkname

        raise ValueError("Member %s not found" % path)

    async def get_regular_meta(self, job: str, path: str) -> Tuple[int, Optional[str]]:
        async with await self.inner.open(job, "rb") as fp:
            tar_bytes = io.BytesIO(await fp.read())
            tar = tarfile.open(fileobj=tar_bytes, mode="r|*")
            for member in tar:
                if member.name.strip("/") == path.strip("/"):
                    return (member.size, None)
        raise ValueError("Member %s not found" % path)

    def dump(self, job: str) -> AsyncGenerator[None, FilesystemEntry]:
        raise NotImplementedError("Not implemented - just dump as a tarball...")

    @asyncasynccontextmanager
    async def open(self, job: str, path: Union[str, Path]) -> AsyncIterator[AReadStreamBase]:
        async with await self.inner.open(job, "rb") as fp:
            tar_bytes = io.BytesIO(await fp.read())
            tar = tarfile.open(fileobj=tar_bytes, mode="r|*")
            for member in tar:
                if member.name.strip("/") == str(path).strip("/"):
                    with tar.extractfile(member) as f:
                        yield ReadSyncStreamAsyncWrapper(f)
                    return

        raise ValueError("Member %s not found" % path)

    async def dump_tarball(self, job: str, stream: AReadStreamBase) -> None:
        async with await self.inner.open(job, "wb") as fp:
            await async_copyfile(stream, fp)

    async def iter_members(self, job: str) -> AsyncIterator[FilesystemEntry]:
        async with await self.inner.open(job, "rb") as fp:
            tar_bytes = io.BytesIO(await fp.read())
            tar = tarfile.open(fileobj=tar_bytes, mode="r|*")
            for member in tar:
                if member.isdir():
                    yield FilesystemEntry(member.name, FilesystemType.DIRECTORY, member.mode)
                elif member.isfile():
                    with tar.extractfile(member) as f:
                        yield FilesystemEntry(
                            member.name,
                            FilesystemType.FILE,
                            member.mode,
                            data=ReadSyncStreamAsyncWrapper(f),
                            content_size=member.size,
                        )
                elif member.issym():
                    yield FilesystemEntry(member.name, FilesystemType.SYMLINK, member.mode, link_target=member.linkname)

    async def get_tarball(self, job: str, dest: AWriteStreamBase) -> None:
        async with await self.inner.open(job, "rb") as fp:
            await async_copyfile(fp, dest)

    def unfiltered_iter(self):
        return self.inner.unfiltered_iter()

    def delete(self, job: str):
        return self.inner.delete(job)

    async def validate(self):
        await self.inner.validate()
