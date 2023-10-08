from typing import Callable, Dict
from pathlib import Path
import sys
import tempfile

from pydatatask.staging import Dispatcher, PipelineStaging, find_config


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


def default_allocators_temp() -> Dict[str, Callable[[], Dispatcher]]:
    return {
        "MetadataRepository": _allocate_temp_meta,
        "BlobRepository": _allocate_temp_blob,
    }


def default_allocators_local() -> Dict[str, Callable[[], Dispatcher]]:
    return {
        "MetadataRepository": _allocate_local_meta,
        "BlobRepository": _allocate_local_blob,
    }


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
    locked = spec.allocate(allocators, Dispatcher("LocalLinux", {"app": "pydatatask"}))
    locked.filename = cfgpath.with_suffix(".lock").name
    locked.save()
    print(locked.basedir / locked.filename)


if __name__ == "__main__":
    sys.exit(main())
