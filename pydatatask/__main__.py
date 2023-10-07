"""
This module is called when you run `python -m pydatatask`. Its whole purpose is to parse pipeline.yaml files and then
feed the result into pydatatask.main.main().
"""
from typing import Optional
import sys

from pydatatask.declarative import find_config
from pydatatask.main import main as real_main
from pydatatask.staging import PipelineStaging


def _main() -> Optional[int]:
    cfgpath = find_config()
    if cfgpath is None:
        print("Cannot find pipeline.yaml", file=sys.stderr)
        return 1
    lockfile = cfgpath.with_suffix(".lock")
    if lockfile.is_file():
        spec = PipelineStaging(lockfile)
    else:
        spec = PipelineStaging(cfgpath)
    if not spec.missing().ready():
        raise ValueError(
            "Cannot start this pipeline - it has unsatisfied dependencies. Try locking it with `python -m pydatatask.lock`"
        )

    pipeline = spec.instantiate()
    real_main(pipeline)
    return 0


if __name__ == "__main__":
    sys.exit(_main())
