from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from pathlib import Path
import argparse
import asyncio
import getpass
import os
import random
import shutil
import string
import sys
import tempfile
import urllib.parse

import aiodocker

from pydatatask.executor.proc_manager import LocalLinuxManager
from pydatatask.staging import Dispatcher, PipelineStaging, RepoClassSpec, find_config

LOCK_ID = "".join(random.choice(string.ascii_lowercase) for _ in range(10))

EPHEMERALS: Dict[Any, Tuple[str, Dispatcher]] = {}


class Allocator:
    def allocate(self, spec: RepoClassSpec) -> Optional[Dispatcher]:
        raise NotImplementedError


class TempAllocator(Allocator):
    def allocate(self, spec: RepoClassSpec) -> Optional[Dispatcher]:
        if spec.cls == "MetadataRepository":
            return Dispatcher("InProcessMetadata", {})
        if spec.cls == "BlobRepository":
            return Dispatcher("InProcessBlob", {})
        if spec.cls == "FilesystemRepository":
            return Dispatcher("Tarfile", {"inner": {"cls": "InProcessBlob", "args": {}}})
        return None


class LocalAllocator(Allocator):
    def allocate(self, spec: RepoClassSpec) -> Optional[Dispatcher]:
        path = Path(f"{os.environ.get('TEMP', '/tmp')}/pydatatask-{getpass.getuser()}/lock-{LOCK_ID}")
        path.mkdir(exist_ok=True, parents=True)
        basedir = tempfile.mkdtemp(dir=path)
        if spec.cls == "MetadataRepository":
            result = Dispatcher("YamlFile", {"basedir": basedir})
        elif spec.cls == "BlobRepository":
            if spec.compress_backend:
                result = Dispatcher(
                    "CompressedBlob", {"inner": {"cls": "File", "args": {"basedir": basedir, "extension": ".gz"}}}
                )
            else:
                result = Dispatcher("File", {"basedir": basedir})
        elif spec.cls == "FilesystemRepository":
            if spec.compress_backend:
                result = Dispatcher("Tarfile", {"inner": {"cls": "File", "args": {"basedir": basedir}}})
            else:
                result = Dispatcher("Directory", {"basedir": basedir})
        else:
            return None
        result.args["compress_backup"] = spec.compress_backup
        result.args["schema"] = spec.schema
        return result


class S3BlobAllocator(Allocator):
    def __init__(self, url: str):
        self.url = url
        try:
            self.scheme, rest = self.url.split("://", 1)
            self.username, rest = rest.split(":", 1)
            self.password, rest = rest.split("@", 1)
            self.host, rest = rest.split("/", 1)
            if "/" in rest:
                self.bucket, self.prefix = rest.split("/", 1)
            else:
                self.bucket = rest
                self.prefix = ""
        except ValueError as e:
            raise Exception("Failed to parse s3 bucket url: see --help for expected format") from e

    def make_ephemeral(self):
        client_key = ("s3", self.scheme, self.username, self.password, self.host)
        if client_key not in EPHEMERALS:
            # could maybe do with a better name than just host...
            EPHEMERALS[client_key] = (
                self.host,
                Dispatcher(
                    "S3Connection",
                    {"endpoint": f"{self.scheme}://{self.host}", "username": self.username, "password": self.password},
                ),
            )
        return EPHEMERALS[client_key][0]

    def allocate(self, spec: RepoClassSpec) -> Optional[Dispatcher]:
        if spec.cls != "BlobRepository":
            return None
        return self._allocate_blob(spec)

    def _allocate_blob(self, spec: RepoClassSpec) -> Dispatcher:
        return Dispatcher(
            "S3Bucket",
            {
                "client": self.make_ephemeral(),
                "bucket": self.bucket,
                "prefix": f"{self.prefix}lock-{LOCK_ID}/{spec.name}/",
                "suffix": spec.suffix,
                "mimetype": spec.mimetype,
            },
        )


class S3FsAllocator(S3BlobAllocator):
    def allocate(self, spec: RepoClassSpec) -> Optional[Dispatcher]:
        if spec.cls != "FilesystemRepository":
            return None

        return Dispatcher(
            "Tarfile",
            {
                "inner": self._allocate_blob(spec),
            },
        )


class MongoMetaAllocator(Allocator):
    def __init__(self, url: str):
        self.url, self.database = url.split(":::", 1)

    def make_ephemeral(self):
        client_key = ("mongo", self.url, self.database)
        if client_key not in EPHEMERALS:
            # see above
            EPHEMERALS[client_key] = (
                urllib.parse.urlparse(self.url).hostname or "",
                Dispatcher("MongoDatabase", {"url": self.url, "database": self.database}),
            )
        return EPHEMERALS[client_key][0]

    def allocate(self, spec: RepoClassSpec) -> Optional[Dispatcher]:
        if spec.cls != "MetadataRepository":
            return None
        return Dispatcher(
            "MongoMetadata",
            {
                "database": self.make_ephemeral(),
                "collection": f"lock-{LOCK_ID}.{spec.name}",
            },
        )


async def get_ip() -> str:
    docker = None
    try:
        docker = aiodocker.Docker()
        return (await (await docker.networks.get("bridge")).show())["IPAM"]["Config"][0]["Gateway"]
    except:
        return "127.0.0.1"
    finally:
        if docker is not None:
            await docker.close()


def walk_obj(obj: Any, duplicates: bool = False, seen: Optional[Set[int]] = None) -> Iterator[Tuple[Any, List[Any]]]:
    if seen is None:
        seen = set()
    results: Dict[int, Any] = {}
    if isinstance(obj, list):
        results.update({id(x): x for x in obj})
    if isinstance(obj, dict):
        results.update({id(x): x for x in obj.keys()})
        results.update({id(x): x for x in obj.values()})
    if hasattr(obj, "__dict__"):
        results.update({id(x): x for x in obj.__dict__.values()})
    if hasattr(obj, "__slots__"):
        results.update({id(x): x for x in obj.__slots__})  # type: ignore
    for ident in seen:
        results.pop(ident, None)
    if duplicates:
        new_seen = seen | set(results)
    else:
        new_seen = seen
        seen.update(results)
    output = list(results.values())
    yield obj, output
    for sub in output:
        yield from walk_obj(sub, duplicates, new_seen)


def main():
    parser = argparse.ArgumentParser(
        description="Produce a lockfile allocating executors and repositories for a given pipeline"
    )
    parser.add_argument(
        "--repo-local",
        action="append_const",
        dest="repo_allocator",
        const=LocalAllocator(),
        help="Allocate repositories on local filesystem",
    )
    parser.add_argument(
        "--repo-blob-s3",
        action="append",
        dest="repo_allocator",
        type=S3BlobAllocator,
        help="Allocate blob repositories on an s3 bucket. Expects url in format scheme://username:password@host/bucket/prefix",
    )
    parser.add_argument(
        "--repo-fs-s3",
        action="append",
        dest="repo_allocator",
        type=S3FsAllocator,
        help="Allocate filesystem repositories as a tarball in an s3 bucket. Expects url in format scheme://username:password@host/bucket/prefix",
    )
    parser.add_argument(
        "--repo-meta-mongo",
        action="append",
        dest="repo_allocator",
        type=MongoMetaAllocator,
        help="Allocate metadata repositories as a tarball in a mongodb collection. Expects url in format MONGO_URL:::database",
    )
    parser.add_argument(
        "--repo-temp",
        action="append_const",
        dest="repo_allocator",
        const=TempAllocator(),
        help="Allocate repositories in-memory",
    )
    parser.add_argument("--name", help="The name of the app for automatically generated executors")
    parser.add_argument(
        "--long-running-timeout",
        help="Cap the execution of long running tasks to the given number of minutes",
        type=float,
    )
    parser.add_argument(
        "--image-prefix",
        help="Force container executors to schedule containers with images that are prefixed with this string.",
        type=str,
        default="",
    )
    parser.add_argument(
        "--unlock",
        action="store_true",
        help="Tear down resources associated with the current lockfile, but don't set up a new lockfile",
    )
    parser.add_argument(
        "--no-launch-agent",
        action="store_true",
        help="Inhibit launching the HTTP agent on localhost",
    )
    parser.add_argument(
        "--no-lockstep",
        action="store_true",
        help="Inhibit running lockstep tasks specified in pipeline files",
    )
    parser.add_argument(
        "--global-template-env",
        action="append",
        default=[],
        help="Add a value (KEY=VALUE) to the template environment for the entire pipeline",
    )
    parser.add_argument(
        "--agent-host",
        help="Specify the default hostname the agent will be running at",
    )
    parser.add_argument(
        "--agent-port",
        type=int,
        help="Specify the port the agent will be running at",
    )
    parser.add_argument(
        "--ignore-required",
        action="store_true",
        help="Allocate all missing repos - do not respect the required: True attribute",
    )
    parsed = parser.parse_args()

    if not parsed.repo_allocator:
        parsed.repo_allocator = [LocalAllocator()]

    def allocators(spec: RepoClassSpec) -> Dispatcher:
        for allocator in parsed.repo_allocator:
            result = allocator.allocate(spec)
            if result is not None:
                return result
        raise ValueError("Could not allocate %s: tried %s" % (spec, parsed.repo_allocator))

    cfgpath = find_config()
    if cfgpath is None:
        print("Cannot find pipeline.yaml", file=sys.stderr)
        return 1
    lockfile = cfgpath.with_suffix(".lock")
    if lockfile.exists():
        locked = PipelineStaging(lockfile)
        pipeline = locked.instantiate()

        # HACK
        executor = LocalLinuxManager(app=cfgpath.name, image_prefix=parsed.image_prefix)
        asyncio.run(executor.teardown_agent())

        # HACK
        for obj, _ in walk_obj(locked.spec.repos):
            if (
                isinstance(obj, (str, Path))
                and str(obj).startswith(f"{os.environ.get('TEMP', '/tmp')}/pydatatask")
                and os.path.exists(obj)
            ):
                shutil.rmtree(obj)

        lockfile.unlink()
        if parsed.unlock:
            return
    else:
        if parsed.unlock:
            print("Error: no lockfile to unlock")
            return 1

    spec = PipelineStaging(cfgpath, is_top=None if parsed.ignore_require else True)
    locked = spec.allocate(
        allocators,
        Dispatcher("LocalLinux", {"app": parsed.name or cfgpath.parent.name, "image_prefix": parsed.image_prefix}),
        run_lockstep=not parsed.no_lockstep,
    )
    locked.filename = lockfile.name
    locked.spec.long_running_timeout = parsed.long_running_timeout
    locked.spec.agent_hosts[None] = asyncio.run(get_ip()) if parsed.agent_host is None else parsed.agent_host
    if parsed.agent_port is not None:
        locked.spec.agent_port = parsed.agent_port
    locked.spec.global_template_env.update(
        {k: v for k, v in [line.split("=", 1) for line in parsed.global_template_env]}
    )
    locked.spec.ephemerals.update(dict(EPHEMERALS.values()))
    locked.save()

    locked = PipelineStaging(locked.basedir / locked.filename)
    pipeline = locked.instantiate()

    # HACK
    if not parsed.no_launch_agent:
        executor = LocalLinuxManager(app=cfgpath.name, image_prefix=parsed.image_prefix)
        asyncio.run(executor.launch_agent(pipeline))

    print(locked.basedir / locked.filename)


if __name__ == "__main__":
    sys.exit(main())
