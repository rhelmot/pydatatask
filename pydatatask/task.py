from typing import Optional, Dict, Any, Union, Iterable
from datetime import timedelta, datetime, timezone
import os
from pathlib import Path
import logging
from dataclasses import dataclass

import yaml
import jinja2

from .repository import Repository, FileRepository, BlockingRepository, AggregateOrRepository, LiveKubeRepository, AggregateAndRepository
from .pod_manager import PodManager

l = logging.getLogger(__name__)

__all__ = ('Link', 'Task', 'KubeTask')

@dataclass
class Link:
    repo: Repository
    is_input: bool = False
    is_output: bool = False
    is_status: bool = False
    inhibits_start: bool = False
    required_for_start: bool = False
    inhibits_output: bool = False
    required_for_output: bool = False

class Task:
    def __init__(self, name: str, ready: Optional[Repository]=None):
        self.name = name
        self._ready = ready
        self.links: Dict[str, Link] = {}

    @property
    def ready(self):
        if self._ready is not None:
            return self._ready
        return BlockingRepository(AggregateAndRepository(**self.required_for_start), AggregateOrRepository(**self.inhibits_start))

    def link(
            self,
            name: str,
            repo: Repository,
            is_input=False,
            is_output=False,
            is_status=False,
            inhibits_start=False,
            required_for_start=None,
            inhibits_output=False,
            required_for_output=None,
    ):
        if required_for_start is None:
            required_for_start = is_input
        if required_for_output is None:
            required_for_output = is_output

        self.links[name] = Link(
            repo=repo,
            is_input=is_input,
            is_output=is_output,
            is_status=is_status,
            inhibits_start=inhibits_start,
            required_for_start=required_for_start,
            inhibits_output=inhibits_output,
            required_for_output=required_for_output,
        )

    def plug(self, output: 'Task', output_links: Optional[Iterable[str]]=None):
        for name, link in output.links.items():
            link_attrs = {}
            if link.inhibits_output:
                link_attrs['inhibits_start'] = True
            if link.is_output and (output_links is None or name in output_links):
                link_attrs['is_input'] = True
            if link.required_for_output:
                link_attrs['required_for_start'] = True
            if not link.is_output:
                name = f'{output.name}_{name}'
            if link_attrs:
                self.link(name, link.repo, **link_attrs)

    @property
    def input(self):
        return {name: link.repo for name, link in self.links.items() if link.is_input}

    @property
    def output(self):
        return {name: link.repo for name, link in self.links.items() if link.is_output}

    @property
    def status(self):
        return {name: link.repo for name, link in self.links.items() if link.is_status}

    @property
    def inhibits_start(self):
        return {name: link.repo for name, link in self.links.items() if link.inhibits_start}

    @property
    def required_for_start(self):
        return {name: link.repo for name, link in self.links.items() if link.required_for_start}

    @property
    def inhibits_output(self):
        return {name: link.repo for name, link in self.links.items() if link.inhibits_output}

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
            logs: Optional[FileRepository],
            done: Optional[FileRepository],
            timeout: Optional[timedelta]=None,
            env: Optional[Dict[str, Any]]=None,
            ready: Optional[Repository]=None,
    ):
        super().__init__(name, ready)

        self.template = template
        self.podman = podman
        self.logs = logs
        self.timeout = timeout
        self.done_file = done
        self.env = env

        self.link("live", LiveKubeRepository(podman, name), is_status=True, inhibits_start=True, inhibits_output=True)
        if logs:
            self.link("logs", logs, is_status=True, inhibits_start=True, required_for_output=True)
        if done:
            self.link("done", done, is_status=True, inhibits_start=True, required_for_output=True)

    def _build_env(self, env, job):
        return {
            key: val.info(job) if isinstance(val, Repository) else val
            for key, val in env.items()
            if not key.startswith('_')
        }

    def launch(self, job):
        env = self._build_env(vars(self) | self.links | (self.env or {}), job)
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

class StderrIsStdout:
    pass

STDOUT = StderrIsStdout()

class LocalProcessTask(Task):
    """
    A task that runs a script. The interpreter is specified by the shebang, or the default shell if none present.
    """
    def __init__(
            self,
            name: str,
            pids: FileRepository,
            template: str,
            done: Optional[FileRepository]=None,
            stdout: Optional[FileRepository]=None,
            stderr: Optional[Union[FileRepository, StderrIsStdout]]=None,
            ready: Optional[Repository]=None,
    ):
        super().__init__(name, ready=ready)

        self.pids = pids
        self.template = template
        self.done = done
        self.stdout = stdout
        self.stderr = stderr

    def update(self):
        pass

    def launch(self, job):
        pass
