"""A pipeline is just an unordered collection of tasks.

Relationships between the tasks are implicit, defined by which repositories they share.
"""

from __future__ import annotations

from typing import (
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
import asyncio
import heapq
import logging

import networkx.algorithms.traversal.depth_first_search
import networkx.classes.digraph

from pydatatask.host import Host
from pydatatask.utils import supergetattr_path

from . import repository as repomodule
from . import task as taskmodule
from .quota import Quota
from .session import Session

l = logging.getLogger(__name__)

already_logged_messages = set()


def debug_log(logger, *args, **kwargs):
    """Emit a log message, but don't emit the same message more than once."""
    cached_kwargs = tuple((k, v) for k, v in sorted(kwargs.items()))
    cached_args = tuple(args)
    if (cached_args, cached_kwargs) in already_logged_messages:
        return
    already_logged_messages.add((cached_args, cached_kwargs))
    logger.debug(*args, **kwargs)


__all__ = ("Pipeline",)


@dataclass
class _JobPrioritizer:
    priority: Callable[[str, str, int], float]
    task: taskmodule.Task
    job: str
    replica: int = 0

    def peek(self) -> float:
        """Return this replica's priority."""
        return self.priority(self.task.name, self.job, self.replica)

    def advance(self):
        """Move on to the next replica."""
        self.replica += 1

    def __lt__(self, other: "_JobPrioritizer") -> bool:
        return self.peek() > other.peek()


@dataclass
class _SchedState:
    initial_jobs: List[Tuple[float, str, str]] = field(default_factory=list)
    replica_heap: List[_JobPrioritizer] = field(default_factory=list)


class Pipeline:
    """The pipeline class."""

    def __init__(
        self,
        tasks: Iterable[taskmodule.Task],
        session: Session,
        priority: Optional[Callable[[str, str, int], float]] = None,
        agent_version: str = "unversioned",
        agent_secret: str = "insecure",
        agent_port: int = 6132,
        agent_hosts: Optional[Dict[Optional[Host], str]] = None,
        source_file: Optional[Path] = None,
        long_running_timeout: Optional[timedelta] = None,
        global_template_env: Optional[Dict[str, str]] = None,
    ):
        """
        :param tasks: The tasks which make up this pipeline.
        :param session: The session to open while this pipeline is active.
        :param priority: Optional: A function which takes a task name, job, and replica, and returns an float priority.
                         The priority *must* be monotonically decreasing as replica increases.
                         No jobs will be scheduled unless all higher-priority jobs (larger numbers) have already been
                         scheduled. No second-third-etc replicas will be scheduled unless all first-replicas have been
                         scheduled.
        """
        self.tasks: Dict[str, taskmodule.Task] = {task.name: task for task in tasks}
        self._opened = False
        self.session = session
        self.priority = priority or (lambda x, y, z: 1.0)
        self._graph: Optional["networkx.classes.digraph.DiGraph"] = None
        self.agent_version: str = agent_version
        self.agent_secret: str = agent_secret
        self.agent_port: int = agent_port
        self.agent_hosts: Dict[Optional[Host], str] = agent_hosts or {None: "localhost"}
        self.source_file = source_file
        self.fail_fast = False
        self.long_running_timeout = long_running_timeout
        self.global_template_env = global_template_env or {}

        for task in tasks:
            if task is not self.tasks[task.name]:
                raise NameError(f"The task name {task.name} is duplicated")

    def cache_flush(self):
        """Flush any in-memory caches."""
        for task in self.tasks.values():
            task.cache_flush()

    def settings(
        self,
        synchronous: Optional[bool] = None,
        metadata: Optional[bool] = None,
        fail_fast: Optional[bool] = None,
        debug_trace: Optional[bool] = None,
        require_success: Optional[bool] = None,
        task_allowlist: Optional[List[str]] = None,
    ):
        """This method can be called to set properties of the current run. Only settings set to non-none will be
        updated.

        :param synchronous: Whether jobs will be started and completed in-process, waiting for their completion before a
            launch phase succeeds.
        :param metadata: Whether jobs will store their completion metadata.
        :param fail_fast: Whether failures during update and launch should result in a raised exception rather than a
            logged message
        :param debug_trace: Whether worker scripts should print out their execution traces
        :param require_success: Whether tasks that fail should raise an exception instead of being marked as complete-
            but-failed. If fail_fast is set, this will abort the pipeline; if fail_fast is unset, this will eventually
            retry the task.
        :param task_allowlist: A list of the only tasks which should be scheduled
        """
        if fail_fast is not None:
            self.fail_fast = fail_fast
        for task in self.tasks.values():
            if synchronous is not None:
                task.synchronous = synchronous
            if metadata is not None:
                task.metadata = metadata
            if fail_fast is not None:
                task.fail_fast = fail_fast
            if debug_trace is not None:
                task.debug_trace = debug_trace
            if require_success is not None:
                task.require_success = require_success
            if task_allowlist is not None:
                task.disabled = task.name not in task_allowlist

    async def _validate(self):
        seen_repos = set()
        for task in self.tasks.values():
            await task.validate()
            for link in task.links.values():
                repo = link.repo
                if repo not in seen_repos:
                    seen_repos.add(repo)
                    await repo.validate()

    async def __aenter__(self):
        await self.open()

    async def open(self) -> None:
        """Opens the pipeline and its associated session.

        This will be automatically when entering an ``async with pipeline:`` block.
        """
        if self._opened:
            raise Exception("Pipeline is alredy used")

        for task in self.tasks.values():
            task.agent_secret = self.agent_secret
            task.agent_url = f"http://{self.agent_hosts.get(task.host, self.agent_hosts[None])}:{self.agent_port}"
            task.global_template_env = self.global_template_env
            if (
                self.long_running_timeout is not None
                and task.long_running
                and (not task.timeout or task.timeout < self.long_running_timeout)
            ):
                task.timeout = self.long_running_timeout

        graph = self.task_graph
        u: taskmodule.Task
        v: taskmodule.Task
        for u, v, attrs in graph.edges(data=True):  # type: ignore[misc]
            if u is v or u.long_running or attrs["multi"]:
                continue
            for name, link in list(u.links.items()):
                link_attrs = {}
                if link.inhibits_output:
                    link_attrs["inhibits_start"] = True
                elif link.required_for_output:
                    link_attrs["required_for_start"] = attrs["vlink"]
                if link_attrs:
                    v.link(
                        f"{u.name}_{name}",
                        link.repo,
                        None,
                        self._make_single_func(attrs["rfollow"]),
                        cokeyed=None,
                        auto_meta=None,
                        auto_values=None,
                        **link_attrs,  # type: ignore[arg-type]
                    )

        await self.session.open()
        await self._validate()
        self._opened = True

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            # should this reraise if fail_fast?
            l.error("Terminated with error", exc_info=exc_val)
        await self.close()

    async def close(self):
        """Closes the pipeline and its associated session.

        This will be automatically called when exiting an ``async with pipeline:`` block.
        """
        await self.session.close()

    async def update(self, launch: bool = True) -> bool:
        """Perform one round of pipeline maintenance, running the update phase and then the launch phase. The
        pipeline must be opened for this function to run.

        :return: Whether there is any activity in the pipeline.
        """
        if not self._opened:
            raise Exception("Pipeline must be opened")

        info = await self._update_only_update()
        result2 = (await self._update_only_launch(info)) if launch else False
        return any(any(live) or any(reaped) for live, reaped in info.values()) or result2

    async def _update_only_update(self) -> Dict[str, Tuple[Dict[Tuple[str, int], datetime], Set[str]]]:
        """Perform one round of the update phase of pipeline maintenance. The pipeline must be opened for this
        function to run.

        :return: Whether there were any live jobs.
        """
        if not self._opened:
            raise Exception("Pipeline must be opened")

        to_gather = (task.update() for task in self.tasks.values())
        gathered = dict(zip(self.tasks, await asyncio.gather(*to_gather, return_exceptions=False)))
        self.cache_flush()
        return gathered

    async def _update_only_launch(self, info: Dict[str, Tuple[Dict[Tuple[str, int], datetime], Set[str]]]) -> bool:
        """Perform one round of the launch phase of pipeline maintenance. The pipeline must be opened for this
        function to run.

        :return: Whether there were any jobs launched or ready to launch.
        """
        if not self._opened:
            raise Exception("Pipeline must be opened")

        task_list = list(self.tasks.values())
        # l.debug("Collecting launchable jobs")
        to_gather = await asyncio.gather(*(self._gather_ready_jobs(task) for task in self.tasks.values()))

        by_manager: DefaultDict[Quota, _SchedState] = defaultdict(_SchedState)
        for job_list, task in zip(to_gather, task_list):
            for job in job_list:
                prio = _JobPrioritizer(self.priority, task, job)
                sched = by_manager[task.resource_limit]
                sched.initial_jobs.append((prio.peek(), task.name, job))
                prio.advance()
                if task.replicable:
                    heapq.heappush(sched.replica_heap, prio)

        if all(not sched.initial_jobs for sched in by_manager.values()):
            self.cache_flush()
            return False

        queue: asyncio.Queue[Optional[Tuple[str, str, int, bool]]] = asyncio.Queue()
        N_WORKERS = 15
        N_LEADERS = len(by_manager)

        async def leader(quota: Quota, sched: _SchedState) -> None:
            used = sum(
                (
                    self.tasks[task].job_quota
                    for task, (live, _) in info.items()
                    for (_, replica) in live
                    if replica == 0
                ),
                Quota.parse(0, 0, 0),
            )
            base_already = {
                (task, job, replica) for task, (live, _) in info.items() for (job, replica) in live if replica == 0
            }
            to_kill = {
                (task, job, replica) for task, (live, _) in info.items() for (job, replica) in live if replica != 0
            }

            sched.initial_jobs.sort(reverse=True)
            excess = None
            for (_, task, job) in sched.initial_jobs:
                if (task, job, 0) in base_already:
                    continue
                alloc = self.tasks[task].job_quota
                excess = (used + alloc).excess(quota)
                if excess is not None:
                    break
                used += alloc
                await queue.put((task, job, 0, False))
            else:
                # schedule replicas
                while sched.replica_heap:
                    next_guy = sched.replica_heap[0]
                    alloc = self.tasks[next_guy.task.name].job_quota
                    excess = (used + alloc).excess(quota)
                    if excess is not None:
                        heapq.heappop(sched.replica_heap)
                    else:
                        used += alloc
                        tup = (next_guy.task.name, next_guy.job, next_guy.replica)
                        if tup not in to_kill:
                            await queue.put((*tup, False))
                        else:
                            to_kill.remove(tup)
                        next_guy.advance()
                        heapq.heapreplace(sched.replica_heap, next_guy)
            # kill!!
            for task, job, replica in to_kill:
                await queue.put((task, job, replica, True))
            # terminate
            for _ in range(N_WORKERS):
                await queue.put(None)

        async def worker() -> None:
            quits = 0
            while True:
                jobt = await queue.get()
                if jobt is None:
                    quits += 1
                    if quits == N_LEADERS:
                        break
                else:
                    task, job, replica, kill = jobt
                    if kill:
                        try:
                            l.info("Killing %s:%s.%d", task, job, replica)
                            await self.tasks[task].kill(job, replica)
                        except:  # pylint: disable=bare-except
                            if self.fail_fast:
                                raise
                            l.exception("Failed to kill %s:%s.%d", task, job, replica)
                    else:
                        try:
                            l.info("Launching %s:%s", task, job)
                            await self.tasks[task].launch(job, replica)
                        except:  # pylint: disable=bare-except
                            if self.fail_fast:
                                raise
                            l.exception("Failed to launch %s:%s", task, job)

        await asyncio.gather(
            *(leader(manager, sched) for manager, sched in by_manager.items()), *(worker() for _ in range(N_WORKERS))
        )

        self.cache_flush()
        return True

    async def _gather_ready_jobs(self, task: taskmodule.Task) -> Set[str]:
        """Collect all jobs that are ready to be launched for a given task."""
        if task.disabled:
            # l.debug("%s is disabled - no jobs will be scheduled", task.name)
            debug_log(l, "%s is disabled - no jobs will be scheduled", task.name)
            return set()
        return set(await task.ready.keys())

    @staticmethod
    def _make_single_func(
        func: Optional[Callable[[str], Awaitable[List[str]]]]
    ) -> Optional[Callable[[str], Awaitable[str]]]:
        if func is None:
            return None

        async def result(job):
            out = await func(job)
            if not out:
                raise ValueError("Produced zero related keys, needed at least one")
            return out[0]

        return result

    def _make_follow_func(
        self, task: taskmodule.Task, link_name: str, along: bool
    ) -> Optional[Callable[[str], Awaitable[List[str]]]]:
        link = task.links[link_name]
        if link.key is None:

            async def result1(job):
                return [job]

            return result1

        if link.key == "ALLOC":

            async def result2(job):
                return [task.derived_hash(job, link_name, along)]

            return result2

        if callable(link.key):
            if not along:
                return None

            async def result4(job):
                assert callable(link.key)
                return [await link.key(job)]

            return result4

        if along ^ link.is_output:
            return None

        splitkey = link.key.split(".")
        related = task._repo_related(splitkey[0])
        if not isinstance(related, repomodule.MetadataRepository):
            raise TypeError("Cannot do key lookup on repository which is not MetadataRepository")

        async def mapper(_job, info):
            return str(supergetattr_path(info, splitkey[1:]))

        mapped = related.map(mapper)

        async def result3(job):
            return [str(await mapped.info(job))]

        return result3

    @property
    def graph(self) -> "networkx.classes.digraph.DiGraph":
        """Generate the dependency graph for a pipeline.

        This is a directed graph containing both repositories and tasks as nodes.
        """
        if self._graph is not None:
            return self._graph

        result: networkx.classes.digraph.DiGraph = networkx.classes.digraph.DiGraph()
        for task in self.tasks.values():
            result.add_node(task)
            for link_name, link in task.links.items():
                multi = link.kind in (
                    taskmodule.LinkKind.StreamingInputFilepath,
                    taskmodule.LinkKind.StreamingOutputFilepath,
                )
                follow = self._make_follow_func(task, link_name, True)
                rfollow = self._make_follow_func(task, link_name, False)
                attrs = dict(vars(link))

                cokeyed = attrs.pop("cokeyed")
                del attrs["auto_values"]

                attrs["link_name"] = link_name
                attrs["follow"] = follow
                attrs["rfollow"] = rfollow
                attrs["multi"] = multi
                repo = attrs.pop("repo")
                edges = []
                if attrs["is_input"] or attrs["required_for_start"] or attrs["inhibits_start"]:
                    edges.append((repo, task))
                    if attrs["is_output"] or attrs["is_status"]:
                        edges.append((task, repo))
                else:
                    edges.append((task, repo))

                for a, b in edges:
                    result.add_edge(a, b, **attrs)
                    for cokey_name, c in cokeyed.items():
                        attrs["link_name"] = f"{link_name}.{cokey_name}"
                        if a is task:
                            result.add_edge(a, c, **attrs)
                        else:
                            result.add_edge(c, b, **attrs)

        self._graph = result
        return result

    @staticmethod
    def _combine_follows(
        a: Optional[Callable[[str], Awaitable[List[str]]]], b: Optional[Callable[[str], Awaitable[List[str]]]]
    ) -> Optional[Callable[[str], Awaitable[List[str]]]]:
        if a is None or b is None:
            return None

        async def result(job: str) -> List[str]:
            assert a is not None and b is not None
            intermediate = await a(job)
            return [final for ijob in intermediate for final in await b(ijob)]

        return result

    @property
    def task_graph(self) -> "networkx.classes.multidigraph.MultiDiGraph":
        """A directed dependency graph of just the tasks, derived from the links between tasks and repositories."""
        result: "networkx.classes.multidigraph.MultiDiGraph" = networkx.MultiDiGraph()
        for repo in [node for node in self.graph if isinstance(node, repomodule.Repository)]:
            in_edges = list(self.graph.in_edges(repo, data=True))
            out_edges = list(self.graph.out_edges(repo, data=True))
            for u, _, udata in in_edges:  # type: ignore[misc]
                assert isinstance(u, taskmodule.Task)
                for _, v, vdata in out_edges:  # type: ignore[misc]
                    assert isinstance(v, taskmodule.Task)
                    follow = self._combine_follows(udata["follow"], vdata["follow"])
                    rfollow = self._combine_follows(vdata["rfollow"], udata["rfollow"])
                    result.add_edge(
                        u,
                        v,
                        follow=follow,
                        rfollow=rfollow,
                        repo=repo,
                        ulink=udata["link_name"],
                        vlink=vdata["link_name"],
                        multi=udata["multi"] or vdata["multi"],
                    )
        return result

    @property
    async def mermaid_graph(self) -> str:
        """A mermaid graph of the pipeline, suitable for rendering with the mermaid library."""
        result = ["graph LR"]
        for maturity, style in MERMAID_TASK_MATURITY_STYLES.items():
            result.append(f"    classDef {maturity} {style}")

        for node in self.graph:
            result.append(f"    {hash(node)}[{repr(node)[1:-1]}]")
            if isinstance(node, taskmodule.Task):
                maturity_class = node.annotations.get("maturity", "unknown")
                assert (
                    maturity_class in MERMAID_TASK_MATURITY_STYLES
                ), f"Unknown maturity class {maturity_class} in {node}"
                result.append(f"    {hash(node)}:::{maturity_class}")
            elif isinstance(node, repomodule.Repository):
                ids = [repo_val_id async for repo_val_id in node]
                result[-1] = result[-1][:-1] + f": {len(ids)}]"
            else:
                assert False, f"Unknown node type {type(node)}"

        for u, v, data in self.graph.edges(data=True):  # type: ignore[misc]
            result.append(f"    {hash(u)} -->|{data['link_name']}| {hash(v)}")
        return "\n".join(result)

    @property
    async def mermaid_task_graph(self) -> str:
        """A mermaid graph of the pipeline, suitable for rendering with the mermaid library."""
        result = ["graph LR"]
        for maturity, style in MERMAID_TASK_MATURITY_STYLES.items():
            result.append(f"    classDef {maturity} {style}")
        for node in self.task_graph:
            if isinstance(node, taskmodule.Task):
                maturity_class = node.annotations.get("maturity", "unknown")
                assert (
                    maturity_class in MERMAID_TASK_MATURITY_STYLES
                ), f"Unknown maturity class {maturity_class} in {node}"
                result.append(f"    {node.name}:::{maturity_class}")
            else:
                result.append(f"    {node.name}({node.name})")

        for u, v, data in self.task_graph.edges(data=True):  # type: ignore[misc]
            if data["ulink"] in {"done", "live", "logs"}:
                continue

            repo: repomodule.Repository = data["repo"]
            ids = [repo_val_id async for repo_val_id in repo]
            result.append(f"    {u.name} -->|{data['ulink']}: {len(ids)}| {v.name}")
        return "\n".join(result)

    def dependants(
        self, node: Union[repomodule.Repository, taskmodule.Task], recursive: bool
    ) -> Iterable[Union[repomodule.Repository, taskmodule.Task]]:
        """Iterate the list of repositories that are dependent on the given node of the dependency graph.

        :param node: The starting point of the graph traversal.
        :param recursive: Whether to return only direct dependencies (False) or transitive dependencies (True) of node.
        """
        if recursive:
            yield from networkx.algorithms.traversal.depth_first_search.dfs_preorder_nodes(self.graph, source=node)
        else:
            yield node
            yield from self.graph.successors(node)


MERMAID_TASK_MATURITY_STYLES = {
    "fullyIntegrated": "fill:green,color:black",
    "missingPydatataskFeatures": "fill:lightgreen,color:black,stroke:red,stroke-width:2px,stroke-dasharray: 5, 5",
    "mostlyComplete": "fill:yellow,color:black",
    "teenager": "fill:yellow,color:black",
    "inProgress": "fill:orange,color:black",
    "noProgress": "fill:red,color:black",
    "unknown": "fill:grey,color:black",
}
