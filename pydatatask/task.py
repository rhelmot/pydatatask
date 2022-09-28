from typing import Optional, Dict, Any, Union, List
from datetime import timedelta, datetime, timezone
import os
from pathlib import Path
import logging

import yaml
import jinja2

from .repository import Repository, FileRepository, BlockingRepository, AggregateOrRepository, LiveKubeRepository
from .pod_manager import PodManager

l = logging.getLogger(__name__)

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
        return BlockingRepository(self.input, AggregateOrRepository(live=self.live, done=self.done))

    def waiting(self, repo):
        return BlockingRepository(repo, self.live)

    def launch_all(self):
        for job in self.ready:
            try:
                self.launch(job)
            except:
                l.exception("Failed to launch %s:%s", self, job)

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
            try:
                uptime: timedelta = datetime.now(timezone.utc) - pod.metadata.creation_timestamp
                total_min = uptime.seconds // 60
                uptime_hours, uptime_min = divmod(total_min, 60)
                l.debug("Pod %s is alive for %dh%dm", pod.metadata.name, uptime_hours, uptime_min)
                if pod.status.phase in ('Succeeded', 'Failed'):
                    l.debug("...finished: %s", pod.status.phase)
                    self._cleanup(pod)
                elif self.timeout is not None and uptime > self.timeout:
                    l.debug("...timed out")
                    self.handle_timeout(pod)
                    self._cleanup(pod)
            except:
                l.exception("Failed to update kube task %s:%s", self.name, pod.metadata.name)

    def handle_timeout(self, pod):
        pass

class LocalProcessTask(Task):
    """
    A task that runs a script. The interpreter is specified by the shebang, or the default shell if none present.
    """
    def __init__(
            self,
            input: Repository,
            pids: FileRepository,
            done: FileRepository,
            template: str,
            stdout: Optional[FileRepository],
            stderr: Optional[FileRepository],
    ):
        super().__init__(input, pids, done)

        self.pids = pids
        self.template = template

    def update(self):
        pass

    def launch(self, job):
        pass

class StderrIsStdout(FileRepository):
    def __init__(self):
        pass

STDOUT = StderrIsStdout()
