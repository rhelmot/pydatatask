from typing import Any, Dict, List, Optional, TypeVar, Union
from collections.abc import Callable, Mapping
from datetime import timedelta
import os
import pathlib
import sys

import aiobotocore.session
import kubernetes
import kubernetes_asyncio.config
import yaml

from pydatatask.main import main as real_main
from pydatatask.pipeline import Pipeline
from pydatatask.pod_manager import PodManager
from pydatatask.repository import (
    FileRepository,
    Repository,
    S3BucketRepository,
    YamlMetadataFileRepository,
)
from pydatatask.resource_manager import ResourceManager, Resources
from pydatatask.session import Session
from pydatatask.task import ProcessTask, Task
import pydatatask

_T = TypeVar("_T")
_K = TypeVar("_K", bound=Task)


def find_config() -> Optional[pathlib.Path]:
    thing = os.getenv("PIPELINE_YAML")
    if thing is not None:
        return pathlib.Path(thing)

    root = pathlib.Path.cwd()
    while True:
        thing = root / "pipeline.yaml"
        if thing.exists():
            return thing
        newroot = root.parent
        if newroot == root:
            return None
        else:
            root = newroot


def parse_bool(thing) -> bool:
    if isinstance(thing, bool):
        return thing
    if isinstance(thing, int):
        return bool(thing)
    if isinstance(thing, str):
        if thing.lower() in ("yes", "y", "1", "true"):
            return True
        if thing.lower() in ("no", "n", "0", "false"):
            return False
        raise ValueError(f"Invalid bool value {thing}")
    raise ValueError(f"{type(thing)} is not valid as a bool")


def make_constructor(name: str, constructor: Callable[..., _T], schema: Dict[str, Any]) -> Callable[[Any], _T]:
    def inner(thing):
        if not isinstance(thing, dict):
            raise ValueError(f"{name} must be followed by a mapping")

        kwargs = {}
        for k, v in thing.items():
            if k not in schema:
                raise ValueError(f"Invalid argument to {name}: {k}")
            kwargs[k] = schema[k](v)
        return constructor(**kwargs)

    return inner


def make_task_constructor(
    name: str, constructor: Callable[..., _K], schema: Dict[str, Any], repos: Mapping[str, Repository]
) -> Callable[[Any], _K]:
    inner_constructor = make_constructor(name, constructor, schema)
    repo_picker = make_picker("repo", repos)

    def inner(thing):
        if not isinstance(thing, dict):
            raise ValueError(f"{name} must be followed by a mapping")

        links = thing.pop("links", {})
        task = inner_constructor(thing)
        for lname, link in links.items():
            if not isinstance(link, dict):
                raise ValueError(f"Link definition {lname} for task {name} must be a dict")
            extras = set(link.keys()) - {"repo", "kind"}
            if extras:
                raise ValueError(f"Extra keys in link definition {lname} for task {name}: {', '.join(extras)}")
            needed = {"repo", "kind"} - set(link.keys())
            if needed:
                raise ValueError(f"Missing keys in link definition {lname} for task {name}: {', '.join(needed)}")
            repo = repo_picker(link["repo"])
            if link["kind"] == "Input":
                task.link(lname, repo, is_input=True)
            elif link["kind"] == "Output":
                task.link(lname, repo, is_output=True)
            else:
                raise ValueError(f"Bad link kind: {link['kind']}")
        return task

    return inner


def make_dispatcher(name: str, mapping: Dict[str, Callable[[Any], _T]]) -> Callable[[Any], _T]:
    def inner(thing):
        if not mapping:
            raise ValueError(f"You must provide at least one {name} to reference")
        if not isinstance(thing, dict) or len(thing) != 1:
            raise ValueError(f"{name} must be a dict of one key whose value is e.g. {next(iter(mapping))}")
        key, value = next(iter(thing.items()))
        constructor = mapping.get(key, None)
        if constructor is None:
            raise ValueError(f"{key} is not a valid member of {name}")
        return constructor(value)

    return inner


def make_dict_parser(
    name: str, key_parser: Callable[[str], str], value_parser: Callable[[Any], _T]
) -> Callable[[Any], Dict[str, _T]]:
    def inner(thing):
        if not isinstance(thing, dict):
            raise ValueError(f"{name} must be a dict")
        return {key_parser(key): value_parser(value) for key, value in thing.items()}

    return inner


def make_list_parser(name: str, value_parser: Callable[[Any], _T]) -> Callable[[Any], List[_T]]:
    def inner(thing):
        if not isinstance(thing, list):
            raise ValueError(f"{name} must be a list")
        return [value_parser(value) for value in thing]

    return inner


def make_picker(name: str, options: Mapping[str, _T]) -> Callable[[Any], _T]:
    def inner(thing):
        if not options:
            raise ValueError(f"Must provide at least one {name}")
        if not isinstance(thing, str):
            raise ValueError(f"When picking a {name}, must provide a str")
        if thing not in options:
            raise ValueError(f"{thing} is not a valid option for {options}, you want e.g. {next(iter(options))}")
        return options[thing]

    return inner


def build_podman(namespace: str, app: str = "pipeline-worker"):
    async def podman():
        if os.path.exists(os.path.expanduser(kubernetes_asyncio.config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION)):
            await kubernetes_asyncio.config.load_kube_config()
        else:
            kubernetes_asyncio.config.load_incluster_config()
        podman = PodManager(
            app=app,
            namespace=namespace,
        )
        yield podman
        await podman.close()

    return podman


def build_s3_connection(endpoint: str, username: str, password: str):
    async def minio():
        minio_session = aiobotocore.session.get_session()
        async with minio_session.create_client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=username,
            aws_secret_access_key=password,
        ) as client:
            yield client

    return minio


quota_constructor = make_constructor("quota", Resources.parse, {"cpu": str, "mem": str, "launches": str})
timedelta_constructor = make_constructor(
    "timedelta",
    timedelta,
    {"days": int, "seconds": int, "microseconds": int, "milliseconds": int, "minutes": int, "hours": int, "weeks": int},
)


def parse_pipeline(cfgpath: pathlib.Path, content: dict) -> Optional[Pipeline]:
    extras = set(content.keys()) - {"repos", "tasks", "resources", "quotas", "priorities"}
    if extras:
        raise ValueError(f"Extra keys in root: {', '.join(extras)}")
    needed = {"repos", "tasks"} - set(content.keys())
    if needed:
        raise ValueError(f"Missing keys in root: {', '.join(needed)}")

    priorities_constructor = make_list_parser(
        "priorities", make_constructor("priority", dict, {"task": str, "job": str, "priority": int})
    )
    priorities = priorities_constructor(content.get("priorities", []))

    quotas_constructor = make_dict_parser("quotas", str, quota_constructor)
    quotas = {k: ResourceManager(v) for k, v in quotas_constructor(content.get("quotas", {})).items()}

    session = Session()
    resources_constructor = make_dict_parser(
        "resources",
        str,
        make_dispatcher(
            "Resource",
            {
                "KubernetesCluster": make_constructor(
                    "KubernetesCluster",
                    build_podman,
                    {
                        "app": str,
                        "namespace": str,
                    },
                ),
                "S3Connection": make_constructor(
                    "S3Connection",
                    build_s3_connection,
                    {
                        "endpoint": str,
                        "username": str,
                        "password": str,
                    },
                ),
            },
        ),
    )
    resources_input = content.get("resources", {})
    resources = {k: session.resource(v) for k, v in resources_constructor(resources_input).items()}

    repo_constructor = make_dict_parser(
        "repo",
        str,
        make_dispatcher(
            "Repository",
            {
                "File": make_constructor(
                    "FileRepository",
                    FileRepository,
                    {
                        "basedir": str,
                        "extension": str,
                        "case_insensitive": parse_bool,
                    },
                ),
                "YamlMetadataFile": make_constructor(
                    "YamlMetadataFileRepository",
                    YamlMetadataFileRepository,
                    {
                        "basedir": str,
                        "extension": str,
                        "case_insensitive": parse_bool,
                    },
                ),
                "S3Bucket": make_constructor(
                    "S3BucketRepository",
                    S3BucketRepository,
                    {
                        "client": make_picker("S3Connection", resources),
                        "bucket": str,
                        "prefix": str,
                        "suffix": str,
                        "mimetype": str,
                        "incluster_endpoint": str,
                    },
                ),
            },
        ),
    )

    repos = repo_constructor(content["repos"])

    task_constructor = make_list_parser(
        "task",
        make_dispatcher(
            "Task",
            {
                "Process": make_task_constructor(
                    "ProcessTask",
                    ProcessTask,
                    {
                        "name": str,
                        "template": str,
                        "manager": make_picker("ProcessManager", resources),
                        "resource_manager": make_picker("ResourceManager", quotas),
                        "job_resources": quota_constructor,
                        "pids": make_picker("Repository", repos),
                        "window": timedelta_constructor,
                        "environ": make_dict_parser("environ", str, str),
                        "done": make_picker("Repository", repos),
                        "stdin": make_picker("Repository", repos),
                        "stdout": make_picker("Repository", repos),
                        "stderr": lambda thing: pydatatask.task.STDOUT
                        if thing == "STDOUT"
                        else make_picker("Repository", repos)(thing),
                        "ready": make_picker("Repository", repos),
                    },
                    repos,
                ),
            },
        ),
    )

    tasks = task_constructor(content["tasks"])

    def get_priority(task: str, job: str) -> int:
        result = 0
        for directive in priorities:
            if directive.get("job", job) == job and directive.get("task", task) == task:
                result += directive["priority"]
        return result

    pipeline = Pipeline(tasks, session, quotas.values(), get_priority)
    return pipeline


def main() -> Optional[int]:
    cfgpath = find_config()
    if cfgpath is None:
        print("Cannot find pipeline.yaml", file=sys.stderr)
        return 1

    with open(cfgpath, "r") as fp:
        content = yaml.safe_load(fp)

    pipeline = parse_pipeline(cfgpath, content)
    if pipeline is None:
        return 1

    real_main(pipeline)


if __name__ == "__main__":
    sys.exit(main())
