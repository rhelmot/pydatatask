import sys

from pydatatask.declarative import find_config
from pydatatask.staging import (
    PipelineStaging,
    default_allocators_local,
    default_allocators_temp,
)


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
    locked = spec.allocate(allocators)
    locked.filename = cfgpath.with_suffix(".lock").name
    locked.save()
    print(locked.basedir / locked.filename)


if __name__ == "__main__":
    sys.exit(main())
