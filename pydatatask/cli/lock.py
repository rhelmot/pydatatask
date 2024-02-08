from pathlib import Path
import argparse
import asyncio
import getpass
import sys
import tempfile

import aiodocker

from pydatatask.executor.proc_manager import LocalLinuxManager
from pydatatask.staging import Dispatcher, PipelineStaging, RepoClassSpec, find_config


def default_allocators_temp(spec: RepoClassSpec) -> Dispatcher:
    if spec.cls == "MetadataRepository":
        return Dispatcher("InProcessMetadata", {})
    if spec.cls == "BlobRepository":
        return Dispatcher("InProcessBlob", {})
    if spec.cls == "FilesystemRepository":
        return Dispatcher("Tarfile", {"inner": {"cls": "InProcessBlob", "args": {}}})
    raise ValueError("Cannot allocate repository type %s as temporary" % spec.cls)


def default_allocators_local(spec: RepoClassSpec) -> Dispatcher:
    path = Path(f"/tmp/pydatatask-{getpass.getuser()}")
    path.mkdir(exist_ok=True)
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
        raise ValueError("Cannot allocate repository type %s as local" % spec.cls)
    result.args["compress_backup"] = spec.compress_backup
    return result


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
        "local": default_allocators_local,
        "temp": default_allocators_temp,
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
