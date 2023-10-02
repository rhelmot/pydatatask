from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Type
from dataclasses import asdict, dataclass, field
from pathlib import Path
import tempfile

import yaml

from pydatatask.declarative import (
    build_repository_picker,
    build_resource_picker,
    build_task_picker,
    make_list_parser,
)
from pydatatask.pipeline import Pipeline
from pydatatask.repository import Repository
from pydatatask.resource_manager import ResourceManager, Resources
from pydatatask.session import Session


@dataclass
class Dispatcher:
    cls: str
    args: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PriorityEntry:
    priority: int
    task: Optional[str] = None
    job: Optional[str] = None


@dataclass
class PipelineImport:
    path: str
    repos: Dict[str, str] = field(default_factory=dict)


@dataclass
class PipelineSpec:
    tasks: List[Dispatcher] = field(default_factory=list)
    repos: Dict[str, Dispatcher] = field(default_factory=dict)
    repo_classes: Dict[str, str] = field(default_factory=dict)
    priorities: List[PriorityEntry] = field(default_factory=list)
    quotas: Dict[str, Resources] = field(default_factory=dict)
    resources: Dict[str, Dispatcher] = field(default_factory=dict)
    imports: Dict[str, PipelineImport] = field(default_factory=dict)


@dataclass
class PipelineChild:
    pipeline: "PipelineStaging"
    translation: Dict[str, str]  # {imp name: {our name: child's name}}


class PipelineStaging:
    def __init__(self, filepath: Optional[Path] = None, basedir: Optional[Path] = None):
        if filepath is None:
            if basedir is None:
                raise TypeError("Must provide basedir if you don't provide filepath")

            self.filename = "pipeline.lock"
            self.basedir = basedir
            self.children = {}
            self.repos_fulfilled_by_parents = {}
            self.repos_needed_by_children = {}
            self.spec = PipelineSpec()
        else:
            if basedir is not None:
                raise TypeError("Cannot provide basedir if filepath is not None")

            self.basedir = filepath.parent
            self.filename = filepath.name

            with open(filepath, "r") as fp:
                spec_dict = yaml.safe_load(fp)
            for i, p in enumerate(spec_dict.get("priorities", [])):
                spec_dict["priorities"][i] = PriorityEntry(**p)
            for i, q in spec_dict.get("quotas", {}).items():
                spec_dict["quotas"][i] = Resources.parse(**q)
            for i, m in enumerate(spec_dict.get("imports", {})):
                spec_dict["imports"][i] = PipelineImport(**m)
            for i, t in enumerate(spec_dict.get("tasks", [])):
                spec_dict["tasks"][i] = Dispatcher(**t)
            for i, r in spec_dict.get("repos", {}).items():
                spec_dict["tasks"][i] = Dispatcher(**r)
            for i, r in spec_dict.get("resources", {}).items():
                spec_dict["resources"][i] = Dispatcher(**r)
            self.spec = PipelineSpec(**spec_dict)

            self.children: Dict[str, PipelineChild] = {}
            self.repos_fulfilled_by_parents: Dict[str, Dispatcher] = {}  # {our name: Dispatcher}
            self.repos_needed_by_children: Dict[str, str] = {}  # {our name: class name}

            assert not (set(self.spec.repos) & set(self.spec.repo_classes))

            for imp_name, imp in self.spec.imports.items():
                translation = {}
                pipeline = PipelineStaging(self.basedir / imp.path)
                parameters: Dict[str, Dispatcher] = {}
                for param_name, sat_name in imp.repos.items():
                    translation[sat_name] = param_name
                    if param_name not in pipeline.spec.repo_classes and param_name not in pipeline.spec.repos:
                        raise ValueError(f"Bad parameter name: {param_name}")
                    if sat_name in self.spec.repos:
                        # we satisfy our child
                        parameters[param_name] = self.spec.repos[sat_name]
                    elif sat_name in self.spec.repo_classes:
                        # we display a dependency which will satisfy our child
                        repo_cls = self.spec.repo_classes[sat_name]
                        if pipeline.spec.repo_classes[param_name] != repo_cls:
                            # TODO subclass relations
                            raise ValueError(
                                f"Bad parameter type: {param_name} is {pipeline.spec.repo_classes[param_name]} but {sat_name} is {self.spec.repo_classes[sat_name]}"
                            )
                    else:
                        raise ValueError(f"Bad parameter: {sat_name}")

                pipeline.parameterize(parameters)
                for name, ty in pipeline.spec.repo_classes.items():
                    if name not in imp.repos:
                        subname = f"{imp_name}_{name}"
                        translation[subname] = name
                        self.repos_needed_by_children[subname] = ty

                self.children[imp_name] = PipelineChild(pipeline, translation)

    def get_priority(self, task: str, job: str) -> int:
        result = 0
        for directive in self.spec.priorities:
            if (directive.job is None or directive.job == job) and (directive.task is None or directive.task == task):
                result += directive.priority
        return result

    def missing(self) -> Dict[str, str]:
        result = self.spec.repo_classes | self.repos_needed_by_children
        for gotten in self.repos_fulfilled_by_parents:
            result.pop(gotten, None)
        return result

    def _iter_children(self) -> Iterator["PipelineStaging"]:
        yield self
        for child in self.children.values():
            yield from child.pipeline._iter_children()

    def instantiate(self) -> Pipeline:
        missing = self.missing()
        if missing:
            raise ValueError(f"Cannot instantiate pipeline - missing definitions for repositories {' '.join(missing)}")

        resource_constructor = build_resource_picker()
        repo_cache: Dict[int, Repository] = {}
        resource_cache: Dict["PipelineStaging", Dict[str, Callable[[], Any]]] = {}
        all_tasks = []
        all_quotas = []
        session = Session()
        for staging in self._iter_children():
            resources = {
                name: session.resource(resource_constructor(asdict(value)))
                for name, value in staging.spec.resources.items()
            }
            resource_cache[staging] = resources
            repo_constructor = build_repository_picker(resources)
            repos = {name: repo_constructor(asdict(value)) for name, value in staging.spec.repos.items()}
            repo_cache.update({id(staging.spec.repos[name]): repo for name, repo in repos.items()})

        for staging in self._iter_children():
            all_repos = {name: repo_cache[id(dispatch)] for name, dispatch in staging.spec.repos.items()}
            all_repos.update(
                {name: repo_cache[id(dispatch)] for name, dispatch in staging.repos_fulfilled_by_parents.items()}
            )
            quotas = {name: ResourceManager(val) for name, val in staging.spec.quotas.items()}
            all_quotas.extend(quotas.values())
            task_constructor = build_task_picker(all_repos, quotas, resource_cache[staging])
            all_tasks.extend(task_constructor(asdict(task_spec)) for task_spec in self.spec.tasks)

        return Pipeline(all_tasks, session, all_quotas, self.get_priority)

    def parameterize(self, parameters: Dict[str, Dispatcher]):
        self.repos_fulfilled_by_parents.update(parameters)

        for child in self.children.values():
            child_parameters = {child.translation[repo_name]: repo for repo_name, repo in parameters.items()}
            if child_parameters:
                child.pipeline.parameterize(child_parameters)

    def allocate(self, allocators: Dict[str, Callable[[], Dispatcher]]) -> "PipelineStaging":
        new_repos: Dict[str, Dispatcher] = {}
        missing = self.missing()
        for repo_name, repo_spec in missing.items():
            if repo_spec not in allocators:
                raise ValueError(f"No allocator available for {repo_spec}")
            new_repos[repo_name] = allocators[repo_spec]()

        self.parameterize(new_repos)
        result = PipelineStaging(basedir=self.basedir)
        result.children["locked"] = PipelineChild(self, {name: name for name in new_repos})
        result.repos_needed_by_children.update(missing)
        result.spec.repos.update(new_repos)
        result.spec.imports["locked"] = PipelineImport(
            str(self.basedir / self.filename), {name: name for name in new_repos}
        )
        return result

    def save(self):
        spec_dict = asdict(self.spec)
        with open(self.basedir / self.filename, "w") as fp:
            yaml.dump(spec_dict, fp)


def allocate_temp_meta() -> Dispatcher:
    return Dispatcher("InProcessMetadata", {})


def allocate_temp_blob() -> Dispatcher:
    return Dispatcher("InProcessBlob", {})


def allocate_local_meta() -> Dispatcher:
    basedir = tempfile.mkdtemp(dir="/tmp/pydatatask")
    return Dispatcher("YamlFile", {"basedir": basedir})


def allocate_local_blob() -> Dispatcher:
    basedir = tempfile.mkdtemp(dir="/tmp/pydatatask")
    return Dispatcher("File", {"basedir": basedir})


def default_allocators_temp() -> Dict[str, Callable[[], Dispatcher]]:
    return {
        "MetadataRepository": allocate_temp_meta,
        "BlobRepository": allocate_temp_blob,
    }


def default_allocators_local() -> Dict[str, Callable[[], Dispatcher]]:
    return {
        "MetadataRepository": allocate_local_meta,
        "BlobRepository": allocate_local_blob,
    }
