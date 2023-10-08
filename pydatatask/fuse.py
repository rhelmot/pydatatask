# pylint: disable=missing-class-docstring,missing-module-docstring,missing-function-docstring,unused-argument
# pylint: disable=import-error
from typing import TYPE_CHECKING
import os

from pydatafs.errors import FSInvalidError
from pydatafs.files import Directory, File, RWBufferedFile, Symlink
from pydatafs.filesystem import PyDataFS
import yaml

from .pipeline import Pipeline
from .repository import (
    BlobRepository,
    FileRepositoryBase,
    MetadataRepository,
    Repository,
)
from .task import Link, Task

if TYPE_CHECKING:
    from pyfuse3 import FileHandleT, FileNameT, InodeT
else:
    InodeT = int
    FileHandleT = int
    FileNameT = bytes


class TaskListDir(Directory):
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        super().__init__()

    async def get_children(self):
        for name, task in self.pipeline.tasks.items():
            yield name, TaskDir(task)

    async def get_child(self, name):
        task = self.pipeline.tasks.get(name, None)
        if task is None:
            return None
        return TaskDir(task)

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return id(self.pipeline)


class TaskDir(Directory):
    def __init__(self, task: Task):
        self.task = task
        super().__init__()

    async def get_children(self):
        for name, link in self.task.links.items():
            yield name, LinkDir(link)

    async def get_child(self, name):
        link = self.task.links.get(name, None)
        if link is None:
            return None
        return LinkDir(link)

    def __eq__(self, other):
        return type(self) is type(other) and self.task == other.task

    def __hash__(self):
        return hash(self.task)


class RepoDir(Directory):
    def __init__(self, repo: Repository):
        self.repo = repo
        super().__init__()

        if isinstance(repo, MetadataRepository):
            self.job_builder = self.metadata_builder
        elif isinstance(repo, FileRepositoryBase):
            self.job_builder = self.symlink_builder
        elif isinstance(repo, BlobRepository):
            self.job_builder = self.blob_builder
        else:
            self.job_builder = self.empty_builder

    def __eq__(self, other):
        return type(self) is type(other) and self.repo == other.repo

    def __hash__(self):
        return hash(self.repo)

    def metadata_builder(self, job: str):
        async def content():
            assert isinstance(self.repo, MetadataRepository)
            if not await self.repo.contains(job):
                return bytearray()
            data = await self.repo.info(job)
            return bytearray(yaml.safe_dump(data).encode())

        async def writeback(data_bytes):
            assert isinstance(self.repo, MetadataRepository)
            if not data_bytes:
                # we are PROBABLY doing open(x, O_TRUNC)
                # do not sync this. nobody wants the empty string to be valid yaml. fuck you
                return
            data = yaml.safe_load(bytes(data_bytes))
            await self.repo.dump(job, data)

        async def size():
            return 0

        return RWBufferedFile(
            identity=(self.repo, job),
            content=content,
            writeback=writeback,
            size=size,
        )

    def symlink_builder(self, job: str):
        assert isinstance(self.repo, FileRepositoryBase)
        return Symlink((self.repo, job), str(self.repo.fullpath(job)))

    def blob_builder(self, job: str):
        async def content():
            assert isinstance(self.repo, BlobRepository)
            if not await self.repo.contains(job):
                return bytearray()
            async with await self.repo.open(job, "rb") as fp:
                return bytearray(await fp.read())

        async def writeback(data):
            assert isinstance(self.repo, BlobRepository)
            async with await self.repo.open(job, "wb") as fp:
                await fp.write(data)

        async def size():
            return 0

        return RWBufferedFile(
            identity=(self.repo, job),
            content=content,
            size=size,
            writeback=writeback,
        )

    def empty_builder(self, job: str):
        return RWBufferedFile(identity=(self.repo, job), content=b"This repository is not readable\n")

    async def get_children(self):
        async for job in self.repo:
            yield job, self.job_builder(job)

    async def get_child(self, name):
        if await self.repo.contains(name):
            return self.job_builder(name)
        return None

    async def create(self, name: str, mode, flags) -> File:
        if isinstance(self.repo, MetadataRepository):
            return self.job_builder(name)  # type: ignore
        elif isinstance(self.repo, FileRepositoryBase):
            raise FSInvalidError()
        else:
            return self.job_builder(name)  # type: ignore

    async def unlink_child(self, name: str):
        await self.repo.delete(name)


class LinkDir(RepoDir):
    def __init__(self, link: Link):
        self.link = link
        super().__init__(link.repo)


async def main(pipeline: Pipeline, path: str, debug: bool):
    stats = os.stat(path)
    fs = PyDataFS("pydatatask", TaskListDir(pipeline), uid=stats.st_uid, gid=stats.st_gid, debug=debug)
    await fs.run(path)
