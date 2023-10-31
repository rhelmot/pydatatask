"""A pipeline is just an unordered collection of tasks.

Relationships between the tasks are implicit, defined by which repositories they share.
"""

from typing import (
    Any,
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
import asyncio
import logging

import networkx.algorithms.traversal.depth_first_search
import networkx.classes.digraph

from pydatatask.host import Host
from pydatatask.repository.base import MetadataRepository
from pydatatask.utils import supergetattr_path

from .quota import QuotaManager, localhost_quota_manager
from .repository import Repository
from .session import Session
from .task import Task

l = logging.getLogger(__name__)

__all__ = ("Pipeline",)


class Pipeline:
    """The pipeline class."""

    def __init__(
        self,
        tasks: Iterable[Task],
        session: Session,
        quota_managers: Iterable[QuotaManager],
        priority: Optional[Callable[[str, str], int]] = None,
        agent_version: str = "unversioned",
        agent_secret: str = "insecure",
        agent_port: int = 6132,
        agent_hosts: Optional[Dict[Optional[Host], str]] = None,
    ):
        """
        :param tasks: The tasks which make up this pipeline.
        :param session: The session to open while this pipeline is active.
        :param quota_managers: Any quota managers in use. You need to provide these so the pipeline can reset the
                          rate-limiting at each update.
        :param priority: Optional: A function which takes a task and job name and returns an integer priority. No jobs
                         will be scheduled unless all higher-priority jobs (larger numbers) have already been scheduled.
        """
        self.tasks: Dict[str, Task] = {task.name: task for task in tasks}
        self._opened = False
        self.session = session
        self.quota_managers = list(quota_managers)
        self.priority = priority
        self._graph: Optional["networkx.classes.digraph.DiGraph"] = None
        self.agent_version: str = agent_version
        self.agent_secret: str = agent_secret
        self.agent_port: int = agent_port
        self.agent_hosts: Dict[Optional[Host], str] = agent_hosts or {None: "localhost"}

    def settings(self, synchronous=False, metadata=True):
        """This method can be called to set properties of the current run.

        :param synchronous: Whether jobs will be started and completed in-process, waiting for their completion before a
            launch phase succeeds.
        :param metadata: Whether jobs will store their completion metadata.
        """
        for task in self.tasks.values():
            task.synchronous = synchronous
            task.metadata = metadata

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

        graph = self.task_graph
        u: Task
        v: Task
        for u, v, attrs in graph.edges(data=True):  # type: ignore[misc]
            if u is v or u.long_running:
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
                        multi_meta=None,
                        **link_attrs,
                    )

        await self.session.open()
        await self._validate()
        self._opened = True

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            l.error("Terminated with error", exc_info=exc_val)
        await self.close()

    async def close(self):
        """Closes the pipeline and its associated session.

        This will be automatically called when exiting an ``async with pipeline:`` block.
        """
        await self.session.close()

    async def update(self) -> bool:
        """Perform one round of pipeline maintenance, running the update phase and then the launch phase. The
        pipeline must be opened for this function to run.

        :return: Whether there is any activity in the pipeline.
        """
        if not self._opened:
            raise Exception("Pipeline must be opened")

        l.info("Running update...")
        result1 = await self.update_only_update()
        result2 = await self.update_only_launch()
        for res in self.quota_managers:
            await res.flush()
        await localhost_quota_manager.flush()
        l.debug("Completed update")
        return result1 | result2

    async def update_only_update(self) -> bool:
        """Perform one round of the update phase of pipeline maintenance. The pipeline must be opened for this
        function to run.

        :return: Whether there were any live jobs.
        """
        if not self._opened:
            raise Exception("Pipeline must be opened")

        to_gather = [task.update() for task in self.tasks.values()]
        gathered = await asyncio.gather(*to_gather, return_exceptions=False)
        return any(gathered)

    async def update_only_launch(self) -> bool:
        """Perform one round of the launch phase of pipeline maintenance. The pipeline must be opened for this
        function to run.

        :return: Whether there were any jobs launched or ready to launch.
        """
        if not self._opened:
            raise Exception("Pipeline must be opened")

        task_list = list(self.tasks.values())
        l.debug("Collecting launchable jobs")
        to_gather = await asyncio.gather(*[self.gather_ready_jobs(task) for task in task_list])
        jobs = [
            (
                self.priority(task.name, job) if self.priority is not None else 0,
                task.name,
                job,
            )
            for job_list, task in zip(to_gather, task_list)
            for job in job_list
        ]
        if not jobs:
            return False

        jobs.sort()  # do not reverse - the next op we're doing implicitly reverses it
        jobs_by_priority = [[jobs.pop()]]
        while jobs:
            priority, task, job = jobs.pop()
            if priority != jobs_by_priority[-1][0][0]:
                jobs_by_priority.append([])
            jobs_by_priority[-1].append((priority, task, job))

        queue: asyncio.Queue[Optional[Tuple[int, str, str]]] = asyncio.Queue()
        N_WORKERS = 15

        async def leader(jobs) -> None:
            for job in jobs:
                await queue.put(job)
            for _ in range(N_WORKERS):
                await queue.put(None)

        async def worker() -> None:
            while True:
                jobt = await queue.get()
                if jobt is None:
                    return

                _, task, job = jobt
                try:
                    l.debug("Launching %s:%s", task, job)
                    await self.tasks[task].launch(job)
                except:  # pylint: disable=bare-except
                    l.exception("Failed to launch %s:%s", task, job)

        for prio_queue in jobs_by_priority:
            l.debug("Launching jobs of priority %s", prio_queue[0][0])
            await asyncio.gather(leader(prio_queue), *[worker() for _ in range(N_WORKERS)])

        return True

    async def gather_ready_jobs(self, task: Task) -> Set[str]:
        """Collect all jobs that are ready to be launched for a given task."""
        result: Set[str] = set()
        if task.disabled:
            l.debug("%s is disabled - no jobs will be scheduled", task.name)
            return result
        async for job in task.ready:
            result.add(job)
        return result

    @staticmethod
    def _make_single_func(func: Callable[[str], Awaitable[List[str]]]) -> Callable[[str], Awaitable[str]]:
        async def result(job):
            out = await func(job)
            if not out:
                raise ValueError("Produced zero related keys, needed at least one")
            return out[0]

        return result

    def _make_follow_func(
        self, task: Task, link_name: str, along: bool
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
        if not isinstance(related, MetadataRepository):
            raise TypeError("Cannot do key lookup on repository which is not MetadataRepository")

        async def mapper(info):
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
                follow = self._make_follow_func(task, link_name, True)
                rfollow = self._make_follow_func(task, link_name, False)
                attrs = dict(vars(link))
                attrs["link_name"] = link_name
                attrs["follow"] = follow
                attrs["rfollow"] = rfollow
                repo = attrs.pop("repo")
                if attrs["is_input"] or attrs["required_for_start"] or attrs["inhibits_start"]:
                    result.add_edge(repo, task, **attrs)
                    if attrs["is_output"] or attrs["is_status"]:
                        result.add_edge(task, repo, **attrs)
                else:
                    result.add_edge(task, repo, **attrs)

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
        for repo in [node for node in self.graph if isinstance(node, Repository)]:
            in_edges = list(self.graph.in_edges(repo, data=True))
            out_edges = list(self.graph.out_edges(repo, data=True))
            for u, _, udata in in_edges:  # type: ignore[misc]
                assert isinstance(u, Task)
                for _, v, vdata in out_edges:  # type: ignore[misc]
                    assert isinstance(v, Task)
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
                    )
        return result

    @property
    def mermaid_graph(self) -> str:
        """A mermaid graph of the pipeline, suitable for rendering with the mermaid library."""
        result = ["graph LR"]

        for node in self.graph:
            result.append(f"    {hash(node)}[{repr(node)[1:-1]}]")
        for u, v, data in self.graph.edges(data=True):
            result.append(f"    {hash(u)} -->|{data['link_name']} {hash(v)}")
        return "\n".join(result)

    @property
    def mermaid_task_graph(self, all=False) -> str:
        """A mermaid graph of the pipeline, suitable for rendering with the mermaid library."""
        result = ["graph LR"]
        for node in self.task_graph:
            if isinstance(node, Task):
                result.append(f"    {node.name}")
            else:
                result.append(f"    {node.name}({node.name})")
        for u, v, data in self.task_graph.edges(data=True):
            if not all and data["ulink"] in {'done', 'live', 'logs'}:
                continue
            result.append(f"    {u.name} -->|{data['ulink']}| {v.name}")
        return "\n".join(result)

    def dependants(self, node: Union[Repository, Task], recursive: bool) -> Iterable[Union[Repository, Task]]:
        """Iterate the list of repositories that are dependent on the given node of the dependency graph.

        :param node: The starting point of the graph traversal.
        :param recursive: Whether to return only direct dependencies (False) or transitive dependencies (True) of node.
        """
        if recursive:
            yield from networkx.algorithms.traversal.depth_first_search.dfs_preorder_nodes(self.graph, source=node)
        else:
            yield node
            yield from self.graph.successors(node)
