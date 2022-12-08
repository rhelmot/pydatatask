from typing import Dict, Iterable, Union
import asyncio
import logging

import networkx
import networkx.algorithms.traversal.depth_first_search

from .repository import Repository
from .session import Session
from .task import Task

l = logging.getLogger(__name__)

__all__ = ("Pipeline",)


class Pipeline:
    def __init__(self, tasks: Iterable[Task], session: Session):
        self.tasks: Dict[str, Task] = {task.name: task for task in tasks}
        self._opened = False
        self.session = session

    async def validate(self):
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
        if self._opened:
            raise Exception("Pipeline is alredy used")
        self._opened = True
        await self.session.open()
        await self.validate()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            l.error("Terminated with error", exc_info=exc_val)
        await self.close()

    async def close(self):
        await self.session.close()

    async def update(self):
        if not self._opened:
            raise Exception("Pipeline must be opened")

        l.info("Running update...")
        result = await self.update_only_update() | await self.update_only_launch()
        l.debug("Completed update")
        return result

    async def update_only_update(self):
        if not self._opened:
            raise Exception("Pipeline must be opened")

        to_gather = [task.update() for task in self.tasks.values()]
        gathered = await asyncio.gather(*to_gather, return_exceptions=False)
        return any(gathered)

    async def update_only_launch(self):
        if not self._opened:
            raise Exception("Pipeline must be opened")

        to_gather = [task.launch_all() for task in self.tasks.values()]
        gathered = await asyncio.gather(*to_gather, return_exceptions=False)
        return any(gathered)

    def graph(self):
        result = networkx.DiGraph()
        for task in self.tasks.values():
            result.add_node(task)
            for link_name, link in task.links.items():
                attrs = dict(vars(link))
                repo = attrs.pop("repo")
                if attrs["is_input"] or attrs["required_for_start"] or attrs["inhibits_start"]:
                    result.add_edge(repo, task, **attrs)
                    if attrs["is_output"] or attrs["is_status"]:
                        result.add_edge(task, repo, **attrs)
                else:
                    result.add_edge(task, repo, **attrs)

        return result

    def dependants(self, repo: Union[Repository, Task], recursive: bool):
        graph = self.graph()
        if recursive:
            yield from networkx.algorithms.traversal.depth_first_search.dfs_preorder_nodes(graph, source=repo)
        else:
            yield repo
            yield from graph.successors(repo)
