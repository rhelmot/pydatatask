"""This module is a sister to declarative, housing the functions needed to handle pipeline.yaml files.

Pipeline.yaml is a specification which lets you set up a pipeline or a piece thereof declaratively. Pipeline files can
specify all their dependencies, or they can specify only the classes of the missing dependencies, in which case before
they can be used they need to either be imported by another pipeline file or "locked", a process which automatically
allocates the needed resources and generates a lockfile, which is itself a pipeline file that imports the original
pipeline file.
"""
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    get_args,
    get_origin,
)
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from pathlib import Path
import os

import yaml

from pydatatask.declarative import (
    build_ephemeral_picker,
    build_executor_picker,
    build_repository_picker,
    build_task_picker,
    host_constructor,
)
from pydatatask.executor import Executor
from pydatatask.pipeline import Pipeline
from pydatatask.quota import Quota, QuotaManager
from pydatatask.repository import Repository
from pydatatask.session import Ephemeral, Session

if TYPE_CHECKING:
    from dataclasses import dataclass as _dataclass_serial  # pylint: disable=reimported
else:

    def _reprocess(value, ty):
        if is_dataclass(ty) and isinstance(value, dict):
            return ty(**value)
        elif get_origin(ty) is dict and isinstance(value, dict):
            for k, v in value.items():
                value[k] = _reprocess(v, get_args(ty)[1])
        elif get_origin(ty) is list and isinstance(value, list):
            for i, v in enumerate(value):
                value[i] = _reprocess(v, get_args(ty)[1])
        return value

    def _dataclass_serial(cls):
        def __post_init__(self):
            for myfield in myfields:
                setattr(self, myfield.name, _reprocess(getattr(self, myfield.name), myfield.type))

        cls.__post_init__ = __post_init__
        dcls = dataclass(cls)
        myfields = fields(dcls)
        return dcls


# pylint: disable=missing-class-docstring,missing-function-docstring
@_dataclass_serial
class Dispatcher:
    cls: str
    args: Dict[str, Any] = field(default_factory=dict)


@_dataclass_serial
class PriorityEntry:
    priority: int
    task: Optional[str] = None
    job: Optional[str] = None


@_dataclass_serial
class LinkSpec:
    repo: str
    kind: str
    key: Optional[str] = None


@_dataclass_serial
class TaskSpec:
    executable: Dispatcher
    executor: Optional[str] = None
    done: Optional[str] = None
    links: Dict[str, LinkSpec] = field(default_factory=dict)


@_dataclass_serial
class PipelineChildSpec:
    path: Optional[str] = None
    repos: Dict[str, str] = field(default_factory=dict)  # {child's name: our name}
    executors: Dict[str, str] = field(default_factory=dict)  # {task name: our executor's name}
    imports: Dict[str, "PipelineChildSpec"] = field(default_factory=dict)


@_dataclass_serial
class PipelineChildArgs:
    repos: Dict[str, Dispatcher] = field(default_factory=dict)  # {child's name: dispatcher}
    executors: Dict[str, Dispatcher] = field(default_factory=dict)  # {task name: our executor's name}
    imports: Dict[str, "PipelineChildArgs"] = field(default_factory=dict)

    def specify(self, prefix: str = "") -> Tuple[PipelineChildSpec, Dict[str, Dispatcher], Dict[str, Dispatcher]]:
        result = PipelineChildSpec()
        result_executors = {f"{prefix}{name}": value for name, value in self.executors.items()}
        result_repos = {f"{prefix}{name}": value for name, value in self.repos.items()}
        result.executors = {f"{name}": f"{prefix}{name}" for name in self.executors}
        result.repos = {f"{name}": f"{prefix}{name}" for name in self.repos}
        for imp_name, imp_args in self.imports.items():
            subresult, subrepos, subexecutors = imp_args.specify(prefix=f"{imp_name}_{prefix}")
            result.imports[imp_name] = subresult
            result_executors.update(subexecutors)
            result_repos.update(subrepos)
        return result, result_repos, result_executors


@_dataclass_serial
class PipelineChildArgsMissing:
    repos: Dict[str, str] = field(default_factory=dict)  # {child's name: class name}
    executors: Set[str] = field(default_factory=set)  # {task name}
    imports: Dict[str, "PipelineChildArgsMissing"] = field(default_factory=dict)

    def ready(self):
        return not self.repos and not self.executors and all(child.ready() for child in self.imports.values())

    def allocate(
        self, repo_allocators: Dict[str, Callable[[], Dispatcher]], default_executor: Optional[Dispatcher]
    ) -> PipelineChildArgs:
        new_repos: Dict[str, Dispatcher] = {}
        new_executors: Dict[str, Dispatcher] = {}
        new_imports: Dict[str, PipelineChildArgs] = {}
        for repo_name, repo_spec in self.repos.items():
            if repo_spec not in repo_allocators:
                raise ValueError(f"No allocator available for {repo_spec}")
            new_repos[repo_name] = repo_allocators[repo_spec]()

        for task_name in self.executors:
            if default_executor is None:
                raise ValueError("No default executor was provided")
            new_executors[task_name] = default_executor

        for imp_name, imp_missing in self.imports.items():
            if imp_missing.ready():
                continue
            new_imports[imp_name] = imp_missing.allocate(repo_allocators, default_executor)

        return PipelineChildArgs(new_repos, new_executors, new_imports)


@_dataclass_serial
class HostSpec:
    os: str


@_dataclass_serial
class PipelineSpec:
    hosts: Dict[str, HostSpec] = field(default_factory=dict)
    tasks: Dict[str, TaskSpec] = field(default_factory=dict)
    executors: Dict[str, Dispatcher] = field(default_factory=dict)
    repos: Dict[str, Dispatcher] = field(default_factory=dict)
    repo_classes: Dict[str, str] = field(default_factory=dict)
    priorities: List[PriorityEntry] = field(default_factory=list)
    quotas: Dict[str, Quota] = field(default_factory=dict)
    ephemerals: Dict[str, Dispatcher] = field(default_factory=dict)
    imports: Dict[str, PipelineChildSpec] = field(default_factory=dict)


@_dataclass_serial
class PipelineChild:
    pipeline: "PipelineStaging"
    repo_translation: Dict[str, str] = field(default_factory=dict)  # {imp name: {our name: child's name}}


# pylint: enable=missing-class-docstring,missing-function-docstring


class PipelineStaging:
    """The main manager for pipeline.yaml files.

    Instantiate this with the path to a pipeline.yaml file.
    """

    def __init__(
        self,
        filepath: Optional[Path] = None,
        basedir: Optional[Path] = None,
        params: Optional[PipelineChildArgs] = None,
    ):
        """The basedir and params parameters are for internal use only."""
        self.children: Dict[str, PipelineStaging] = {}
        self.repos_fulfilled_by_parents: Dict[str, Dispatcher] = {}
        self.executors_fulfilled_by_parents: Dict[str, Dispatcher] = {}

        if filepath is None:
            if basedir is None:
                raise TypeError("Must provide basedir if you don't provide filepath")

            self.filename = "pipeline.lock"
            self.basedir = basedir
            self.repos_fulfilled_by_parents = {}
            self.spec = PipelineSpec()
        else:
            if basedir is not None:
                raise TypeError("Cannot provide basedir if filepath is not None")

            self.basedir = filepath.parent
            self.filename = filepath.name

            with open(filepath, "r", encoding="utf-8") as fp:
                spec_dict = yaml.safe_load(fp)
            self.spec = PipelineSpec(**spec_dict)

            if params is None:
                params = PipelineChildArgs()

            self.children = {}
            self.repos_fulfilled_by_parents = params.repos
            self.executors_fulfilled_by_parents = params.executors

            assert not set(self.spec.repos) & set(self.spec.repo_classes)

            for imp_name, imp in self.spec.imports.items():
                if imp.path is None:
                    raise TypeError("Import clause must specify path to import")
                child_params = PipelineChildArgs()
                child_params.repos.update(
                    {
                        param_name: self.spec.repos[sat_name]
                        for param_name, sat_name in imp.repos.items()
                        if sat_name in self.spec.repos
                    }
                )
                child_params.repos.update(
                    {
                        param_name: self.repos_fulfilled_by_parents[sat_name]
                        for param_name, sat_name in imp.repos.items()
                        if sat_name in self.spec.repo_classes and sat_name in self.repos_fulfilled_by_parents
                    }
                )
                if imp_name in params.imports:
                    child_params.repos.update(params.imports[imp_name].repos)

                child_params.executors.update(
                    {
                        param_name: self.spec.executors[sat_name]
                        for param_name, sat_name in imp.executors.items()
                        if sat_name in self.spec.executors
                    }
                )
                if imp_name in params.imports:
                    child_params.executors.update(params.imports[imp_name].executors)

                self.children[imp_name] = PipelineStaging(self.basedir / imp.path, params=child_params)

    def _get_priority(self, task: str, job: str) -> int:
        result = 0
        for directive in self.spec.priorities:
            if (directive.job is None or directive.job == job) and (directive.task is None or directive.task == task):
                result += directive.priority
        return result

    def missing(self) -> PipelineChildArgsMissing:
        """Return a PipelineChildArgsMissing instance for this pipeline.yaml file.

        This object indicates which resources need to be allocated before the pipeline can be used. You can call its
        .ready() function to get a boolean for whether it is properly ready.
        """
        return PipelineChildArgsMissing(
            repos={
                name: cls for name, cls in self.spec.repo_classes.items() if name not in self.repos_fulfilled_by_parents
            },
            executors={
                name
                for name, tspec in self.spec.tasks.items()
                if tspec.executor is None and name not in self.executors_fulfilled_by_parents
            },
            imports={name: child.missing() for name, child in self.children.items()},
        )

    def _iter_children(self) -> Iterator["PipelineStaging"]:
        yield self
        for child in self.children.values():
            yield from child._iter_children()

    def instantiate(self) -> Pipeline:
        """Convert a PipelineStaging into a Pipeline.

        This will fail if any resources need to be allocated.
        """
        missing = self.missing()
        if not missing.ready():
            raise ValueError("Cannot instantiate pipeline - missing definitions for repositories or executors")

        ephemeral_constructor = build_ephemeral_picker()
        repo_cache: Dict[int, Repository] = {}
        ephemeral_cache: Dict["PipelineStaging", Dict[str, Ephemeral[Any]]] = {}
        executor_cache: Dict[int, Executor] = {}
        all_tasks = []
        all_quotas: List[QuotaManager] = []
        session = Session()
        for staging in self._iter_children():
            ephemerals = {
                name: session.ephemeral(ephemeral_constructor(asdict(value)))
                for name, value in staging.spec.ephemerals.items()
            }
            ephemeral_cache[staging] = ephemerals
            repo_constructor = build_repository_picker(ephemerals)
            repos = {name: repo_constructor(asdict(value)) for name, value in staging.spec.repos.items()}
            repo_cache.update({id(staging.spec.repos[name]): repo for name, repo in repos.items()})

            def nameit(d, name):
                d["name"] = name
                return d

            hosts = {name: host_constructor(nameit(asdict(val), name)) for name, val in staging.spec.hosts.items()}

            executor_constructor = build_executor_picker(hosts, ephemerals)
            executors = {name: executor_constructor(asdict(value)) for name, value in staging.spec.executors.items()}
            executor_cache.update({id(staging.spec.executors[name]): executor for name, executor in executors.items()})

        for staging in self._iter_children():
            all_repos = {name: repo_cache[id(dispatch)] for name, dispatch in staging.spec.repos.items()}
            all_repos.update(
                {name: repo_cache[id(dispatch)] for name, dispatch in staging.repos_fulfilled_by_parents.items()}
            )
            quotas = {name: QuotaManager(val) for name, val in staging.spec.quotas.items()}
            all_executors = {name: executor_cache[id(dispatch)] for name, dispatch in staging.spec.executors.items()}
            all_executors.update(
                {
                    name: executor_cache[id(dispatch)]
                    for name, dispatch in staging.executors_fulfilled_by_parents.items()
                }
            )
            all_quotas.extend(quotas.values())
            task_constructor = build_task_picker(all_repos, all_executors, quotas, ephemeral_cache[staging])
            for task_name, task_spec in staging.spec.tasks.items():
                dict_spec = asdict(task_spec)
                if task_name in staging.executors_fulfilled_by_parents:
                    dict_spec["executor"] = task_name
                all_tasks.append(task_constructor(task_name, dict_spec))

        return Pipeline(all_tasks, session, all_quotas, self._get_priority)

    def allocate(
        self, repo_allocators: Dict[str, Callable[[], Dispatcher]], default_executor: Dispatcher
    ) -> "PipelineStaging":
        """Lock a pipeline, generating a new PipelineStaging which imports this one and specifies all of its missing
        dependencies."""
        spec, repos, executors = self.missing().allocate(repo_allocators, default_executor).specify()
        result = PipelineStaging(basedir=self.basedir)
        result.children["locked"] = self
        result.spec.repos.update(repos)
        result.spec.executors.update(executors)
        result.spec.imports["locked"] = spec
        spec.path = str(self.basedir / self.filename)
        return result

    def save(self):
        """Save this spec back to the pipeline.yaml file, in case it has been modified."""
        spec_dict = asdict(self.spec)
        with open(self.basedir / self.filename, "w", encoding="utf-8") as fp:
            yaml.dump(spec_dict, fp)


def find_config() -> Optional[Path]:
    """Discover a pipeline.yaml file in the current filesystem ancestry or $PIPELINE_YAML."""
    thing = os.getenv("PIPELINE_YAML")
    if thing is not None:
        return Path(thing)

    root = Path.cwd()
    while True:
        pth = root / "pipeline.yaml"
        if pth.exists():
            return pth
        newroot = root.parent
        if newroot == root:
            return None
        else:
            root = newroot
