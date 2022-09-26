from typing import List

from .task import Task

class Pipeline:
    def __init__(self, tasks: List[Task]):
        self.tasks = tasks

    def update(self):
        for task in self.tasks:
            task.update()

        for task in self.tasks:
            task.launch_all()
