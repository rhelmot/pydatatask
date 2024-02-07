from typing import Callable, Dict
from pathlib import Path
import argparse
import asyncio
import getpass
import sys
import tempfile

import aiodocker

from pydatatask.executor.proc_manager import LocalLinuxManager
from pydatatask.staging import Dispatcher, PipelineStaging, find_config


def _allocate_temp_meta() -> Dispatcher:
    return Dispatcher("InProcessMetadata", {})


def _allocate_temp_blob() -> Dispatcher:
    return Dispatcher("InProcessBlob", {})


def _allocate_temp_fs() -> Dispatcher:
    return Dispatcher("Tarfile", {"inner": {"cls": "InProcessBlob", "args": {}}})


def _allocate_local_meta() -> Dispatcher:
    Path(f"/tmp/pydatatask-{getpass.getuser()}").mkdir(exist_ok=True)
    basedir = tempfile.mkdtemp(dir=f"/tmp/pydatatask-{getpass.getuser()}")
    return Dispatcher("YamlFile", {"basedir": basedir})


def _allocate_local_blob() -> Dispatcher:
    Path(f"/tmp/pydatatask-{getpass.getuser()}").mkdir(exist_ok=True)
    basedir = tempfile.mkdtemp(dir=f"/tmp/pydatatask-{getpass.getuser()}")
    return Dispatcher("File", {"basedir": basedir})


def _allocate_local_fs() -> Dispatcher:
    Path(f"/tmp/pydatatask-{getpass.getuser()}").mkdir(exist_ok=True)
    basedir = tempfile.mkdtemp(dir=f"/tmp/pydatatask-{getpass.getuser()}")
    return Dispatcher("Tarfile", {"inner": {"cls": "File", "args": {"basedir": basedir}}})


def default_allocators_temp() -> Dict[str, Callable[[], Dispatcher]]:
    return {
        "MetadataRepository": _allocate_temp_meta,
        "BlobRepository": _allocate_temp_blob,
        "FilesystemRepository": _allocate_temp_fs,
    }


def default_allocators_local() -> Dict[str, Callable[[], Dispatcher]]:
    return {
        "MetadataRepository": _allocate_local_meta,
        "BlobRepository": _allocate_local_blob,
        "FilesystemRepository": _allocate_local_fs,
    }


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


def main():
    all_allocators = {
        "local": default_allocators_local(),
        "temp": default_allocators_temp(),
    }

    parser = argparse.ArgumentParser(
        description="Produce a lockfile allocating executors and repositories for a given pipeline"
    )
    parser.add_argument(
        "--repo-local",
        action="store_const",
        dest="repo_allocator",
        const="local",
        default="local",
        help="Allocate repositories on local filesystem",
    )
    parser.add_argument(
        "--repo-temp", action="store_const", dest="repo_allocator", const="temp", help="Allocate repositories in-memory"
    )
    parser.add_argument("--name", help="The name of the app for automatically generated executors")
    parser.add_argument(
        "--long-running-timeout",
        help="Cap the execution of long running tasks to the given numberr of minutes",
        type=float,
    )
    parsed = parser.parse_args()

    allocators = all_allocators[parsed.repo_allocator]

    cfgpath = find_config()
    if cfgpath is None:
        print("Cannot find pipeline.yaml", file=sys.stderr)
        return 1
    spec = PipelineStaging(cfgpath)
    locked = spec.allocate(allocators, Dispatcher("LocalLinux", {"app": parsed.name or cfgpath.parent.name}))
    locked.filename = cfgpath.with_suffix(".lock").name
    locked.spec.long_running_timeout = parsed.long_running_timeout

    locked.spec.agent_hosts[None] = asyncio.run(get_ip())
    locked.save()

    locked = PipelineStaging(locked.basedir / locked.filename)
    pipeline = locked.instantiate()
    executor = LocalLinuxManager(cfgpath.name)
    asyncio.run(executor.launch_agent(pipeline))

    print(locked.basedir / locked.filename)


if __name__ == "__main__":
    sys.exit(main())
