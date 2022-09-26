from typing import Optional, Dict, Any, Union, List
from datetime import timedelta, datetime
import os
from pathlib import Path

import yaml
import jinja2

from .repository import Repository, FileRepository, BlockingRepository, AggregateRepository, LiveKubeRepository
from .pod_manager import PodManager


class Task:
    def __init__(
            self,
            input: Repository,
            live: Repository,
            done: Repository,
    ):
        self.input = input
        self.live = live
        self.done = done

    @property
    def ready(self):
        return BlockingRepository(self.input, AggregateRepository(live=self.live, done=self.done))

    def waiting(self, repo):
        return BlockingRepository(repo, self.live)

    def launch_all(self):
        for job in self.ready:
            self.launch(job)

    def launch(self, job):
        raise NotImplementedError

    def update(self):
        pass


class KubeTask(Task):
    def __init__(
            self,
            podman: PodManager,
            name: str,
            template: Union[str, Path],
            input: Repository,
            logs: Optional[FileRepository],
            done: Optional[FileRepository],
            timeout: Optional[timedelta]=None,
            env: Optional[Dict[str, Any]]=None,
    ):
        if done:
            super_done = done
        elif logs:
            super_done = logs
        else:
            raise TypeError("Must provide logs or done to KubeTask")

        self.name = name
        self.template = template
        self.podman = podman
        self.logs = logs
        self.timeout = timeout
        self.done_file = done
        self.env = env

        super().__init__(input, LiveKubeRepository(self.podman, name), super_done)

    def _build_env(self, env, job):
        return {
            key: val.info(job) if isinstance(val, Repository) else val
            for key, val in env.items()
            if not key.startswith('_')
        }

    def launch(self, job):
        env = self._build_env(vars(self) | (self.env or {}), job)
        env['job'] = job

        j = jinja2.Environment()
        if os.path.isfile(self.template):
            with open(self.template, 'r') as fp:
                template = fp.read()
        else:
            template = self.template
        template = j.from_string(template)
        rendered = template.render(**env)
        parsed = yaml.safe_load(rendered)
        self.podman.launch(job, self.name, parsed)

    def _cleanup(self, pod):
        job = pod.metadata.labels['job']
        if self.logs is not None:
            with self.logs.open(job, 'w') as fp:
                fp.write(self.podman.logs(pod))
        if self.done_file is not None:
            self.done_file.open(job, 'w').close()
        self.podman.delete(pod)

    def update(self):
        super().update()

        pods = self.podman.query(task=self.name)
        for pod in pods:
            if pod.status.phase in ('Succeeded', 'Failed'):
                self._cleanup(pod)
            elif self.timeout is not None and datetime.utcnow() - pod.metadata.creationTimestamp > self.timeout:
                self.handle_timeout(pod)
                self._cleanup(pod)

    def handle_timeout(self, pod):
        pass

#class LocalProcessTask(Task):
#    def __init__(
#            self,
#            input: Repository,
#            pids: FileRepository,
#            done: FileRepository,
#            args: List[str],
#            env: Optional[Dict[str, str]],
#    ):
#        super().__init__(input, pids, done)
#
#        self.pids = pids
#
#    def update(self):
#        pass
#
#    def launch(self, job):
#        pass
#
#    STDOUT = object()
