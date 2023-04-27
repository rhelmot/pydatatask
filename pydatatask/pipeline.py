"""
A pipeline is just an unordered collection of tasks. Relationships between the tasks are implicit, defined by which
repositories they share.
"""

from typing import Callable, Dict, Iterable, Optional, Set, Tuple, Union
import asyncio
import logging

import networkx.algorithms.traversal.depth_first_search
import networkx.classes.digraph

from .repository import Repository
from .resource_manager import ResourceManager
from .session import Session
from .task import Task

l = logging.getLogger(__name__)

__all__ = ("Pipeline",)


class Pipeline:
    """
    The pipeline class.
    """

    def __init__(
        self,
        tasks: Iterable[Task],
        session: Session,
        resources: Iterable[ResourceManager],
        priority: Optional[Callable[[str, str], int]] = None,
    ):
        """
        :param tasks: The tasks which make up this pipeline.
        :param session: The session to open while this pipeline is active.
        :param resources: Any resource managers in use. You need to provide these so the pipeline can reset the
                          rate-limiting at each update.
        :param priority: Optional: A function which takes a task and job name and returns an integer priority. No jobs
                         will be scheduled unless all higher-priority jobs (larger numbers) have already been scheduled.
        """
        self.tasks: Dict[str, Task] = {task.name: task for task in tasks}
        self._opened = False
        self.session = session
        self.resources = list(resources)
        self.priority = priority

    def settings(self, synchronous=False, metadata=True):
        """
        This method can be called to set properties of the current run.

        :param synchronous: Whether jobs will be started and completed in-process, waiting for their completion before
                            a launch phase succeeds.
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

    async def open(self):
        """
        Opens the pipeline and its associated resource session. This will be automatically when entering an
        ``async with pipeline:`` block.
        """
        if self._opened:
            raise Exception("Pipeline is alredy used")
        self._opened = True
        await self.session.open()
        await self._validate()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            l.error("Terminated with error", exc_info=exc_val)
        await self.close()

    async def close(self):
        """
        Closes the pipeline and its associated resource session. This will be automatically called when exiting an
        ``async with pipeline:`` block.
        """
        await self.session.close()

    async def update(self) -> bool:
        """
        Perform one round of pipeline maintenance, running the update phase and then the launch phase. The pipeline must
        be opened for this function to run.

        :return: Whether there is any activity in the pipeline.
        """
        if not self._opened:
            raise Exception("Pipeline must be opened")

        l.info("Running update...")
        result: bool = await self.update_only_update() | await self.update_only_launch()
        l.debug("Completed update")
        return result

    async def update_only_update(self) -> bool:
        """
        Perform one round of the update phase of pipeline maintenance. The pipeline must be opened for this function to
        run.

        :return: Whether there were any live jobs.
        """
        if not self._opened:
            raise Exception("Pipeline must be opened")

        to_gather = [task.update() for task in self.tasks.values()]
        gathered = await asyncio.gather(*to_gather, return_exceptions=False)
        for res in self.resources:
            await res.flush()
        return any(gathered)

    async def update_only_launch(self) -> bool:
        """
        Perform one round of the launch phase of pipeline maintenance. The pipeline must be opened for this function to
        run.

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

        for res in self.resources:
            await res.flush()
        return True

    async def gather_ready_jobs(self, task: Task) -> Set[str]:
        """
        Collect all jobs that are ready to be launched for a given task.
        """
        result: Set[str] = set()
        if task.disabled:
            l.debug("%s is disabled - no jobs will be scheduled", task.name)
            return result
        async for job in task.ready:
            result.add(job)
        return result

    def graph(self) -> "networkx.classes.digraph.DiGraph":
        """
        Generate the dependency graph for a pipeline. This is a directed graph containing both repositories and tasks
        as nodes.
        """
        result: networkx.classes.digraph.DiGraph = networkx.classes.digraph.DiGraph()
        for task in self.tasks.values():
            result.add_node(task)
            for link_name, link in task.links.items():
                attrs = dict(vars(link))
                attrs["link_name"] = link_name
                repo = attrs.pop("repo")
                if attrs["is_input"] or attrs["required_for_start"] or attrs["inhibits_start"]:
                    result.add_edge(repo, task, **attrs)
                    if attrs["is_output"] or attrs["is_status"]:
                        result.add_edge(task, repo, **attrs)
                else:
                    result.add_edge(task, repo, **attrs)

        return result

    def dependants(self, node: Union[Repository, Task], recursive: bool) -> Iterable[Union[Repository, Task]]:
        """
        Iterate the list of repositories that are dependent on the given node of the dependency graph.

        :param node: The starting point of the graph traversal.
        :param recursive: Whether to return only direct dependencies (False) or transitive dependencies (True) of node.
        """
        graph = self.graph()
        if recursive:
            yield from networkx.algorithms.traversal.depth_first_search.dfs_preorder_nodes(graph, source=node)
        else:
            yield node
            yield from graph.successors(node)
