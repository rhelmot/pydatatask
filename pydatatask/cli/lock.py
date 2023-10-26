from typing import Callable, Dict
from pathlib import Path
import asyncio
import sys
import tempfile

import aiodocker

from pydatatask.executor.proc_manager import LocalLinuxManager
from pydatatask.host import LOCAL_HOST, LOCAL_OS, HostOS
from pydatatask.staging import Dispatcher, HostSpec, PipelineStaging, find_config


def _allocate_temp_meta() -> Dispatcher:
    return Dispatcher("InProcessMetadata", {})


def _allocate_temp_blob() -> Dispatcher:
    return Dispatcher("InProcessBlob", {})


def _allocate_local_meta() -> Dispatcher:
    Path("/tmp/pydatatask").mkdir(exist_ok=True)
    basedir = tempfile.mkdtemp(dir="/tmp/pydatatask")
    return Dispatcher("YamlFile", {"basedir": basedir})


def _allocate_local_blob() -> Dispatcher:
    Path("/tmp/pydatatask").mkdir(exist_ok=True)
    basedir = tempfile.mkdtemp(dir="/tmp/pydatatask")
    return Dispatcher("File", {"basedir": basedir})


def _allocate_local_fs() -> Dispatcher:
    Path("/tmp/pydatatask").mkdir(exist_ok=True)
    basedir = tempfile.mkdtemp(dir="/tmp/pydatatask")
    return Dispatcher("Directory", {"basedir": basedir})


def default_allocators_temp() -> Dict[str, Callable[[], Dispatcher]]:
    return {
        "MetadataRepository": _allocate_temp_meta,
        "BlobRepository": _allocate_temp_blob,
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
    allocators = all_allocators[sys.argv[1]]

    cfgpath = find_config()
    if cfgpath is None:
        print("Cannot find pipeline.yaml", file=sys.stderr)
        return 1
    spec = PipelineStaging(cfgpath)
    locked = spec.allocate(allocators, Dispatcher("LocalLinux", {"app": cfgpath.name}))
    locked.filename = cfgpath.with_suffix(".lock").name

    locked.spec.agent_hosts[None] = asyncio.run(get_ip())
    locked.save()

    locked = PipelineStaging(locked.basedir / locked.filename)
    pipeline = locked.instantiate()
    executor = LocalLinuxManager(cfgpath.name)
    asyncio.run(executor.launch_agent(pipeline))

    print(locked.basedir / locked.filename)


if __name__ == "__main__":
    sys.exit(main())
