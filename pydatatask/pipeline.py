from typing import Iterable, Union
import logging

import networkx
import networkx.algorithms.traversal.depth_first_search

from .task import Task
from .repository import Repository

l = logging.getLogger(__name__)

__all__ = ('Pipeline',)

class Pipeline:
    def __init__(self, tasks: Iterable[Task]):
        self.tasks = {task.name: task for task in tasks}

    def update(self):
        l.info("Running update...")
        result = False
        for task in self.tasks.values():
            l.debug("Running update for %s", task.name)
            result |= task.update()

        for task in self.tasks.values():
            l.debug("Launching tasks for %s", task.name)
            result |= task.launch_all()

        l.debug("Completed update")
        return result

    def graph(self):
        result = networkx.DiGraph()
        for task in self.tasks.values():
            result.add_node(task)
            for link_name, link in task.links.items():
                attrs = dict(vars(link))
                repo = attrs.pop('repo')
                if attrs['is_input'] or attrs['required_for_start'] or attrs['inhibits_start']:
                    result.add_edge(repo, task, **attrs)
                    if attrs['is_output'] or attrs['is_status']:
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
