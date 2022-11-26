from typing import Optional, Dict, Any, Union, Iterable, Callable, Tuple
from asyncio import Future
import traceback
from datetime import timedelta, datetime, timezone
import os
from pathlib import Path
import logging
from dataclasses import dataclass
from concurrent.futures import Executor, wait, FIRST_EXCEPTION

import yaml
import jinja2
from kubernetes.client import V1Pod

from .repository import Repository, FileRepository, BlockingRepository, AggregateOrRepository, LiveKubeRepository, \
    AggregateAndRepository, BlobRepository, MetadataRepository, RelatedItemRepository, ExecutorLiveRepo
from .pod_manager import PodManager

l = logging.getLogger(__name__)

__all__ = ('Link', 'Task', 'KubeTask', 'InProcessSyncTask', 'ExecutorTask')

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

    def __repr__(self):
        return f'<{type(self).__name__} {self.name}>'

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

    def plug(
            self,
            output: 'Task',
            output_links: Optional[Iterable[str]]=None,
            meta: bool=True,
            translator: Optional[Repository]=None,
            translate_allow_deletes=False,
            translate_prefetch_lookup=True,
    ):
        for name, link in output.links.items():
            link_attrs = {}
            if link.inhibits_output and meta:
                link_attrs['inhibits_start'] = True
            if link.is_output and (output_links is None or name in output_links):
                link_attrs['is_input'] = True
            if link.required_for_output and meta:
                link_attrs['required_for_start'] = True
            if not link.is_output:
                name = f'{output.name}_{name}'
            if link_attrs:
                repo = link.repo
                if translator is not None:
                    repo = RelatedItemRepository(repo, translator, allow_deletes=translate_allow_deletes, prefetch_lookup=translate_prefetch_lookup)
                self.link(name, repo, **link_attrs)

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
        result = False
        for job in self.ready:
            result = True
            try:
                l.debug("Launching %s:%s", self.name, job)
                self.launch(job)
            except:
                l.exception("Failed to launch %s:%s", self, job)
        return result

    def launch(self, job):
        raise NotImplementedError

    def update(self) -> bool:
        """
        Performs any maintenance operations on the set of live tasks. Returns True if literally anything interesting happened.
        """
        return False

nobody = object()
class RepositoryInfoTemplate:
    def __init__(self, repo: Repository, job: str):
        self._repo = repo
        self._job = job
        self._cached = nobody

    @property
    def _resolved(self):
        if self._cached is not nobody:
            return self._cached
        self._cached = self._repo.info(self._job)
        return self._cached

    def __getattr__(self, item):
        return getattr(self._resolved, item)

    def __getitem__(self, item):
        return self._resolved[item]

    def __str__(self):
        return str(self._resolved)


class KubeTask(Task):
    def __init__(
            self,
            podman: PodManager,
            name: str,
            template: Union[str, Path],
            logs: Optional[BlobRepository],
            done: Optional[MetadataRepository],
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
            key: RepositoryInfoTemplate(val, job) if isinstance(val, Repository) else RepositoryInfoTemplate(val.repo, job) if isinstance(val, Link) else val
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

    def _cleanup(self, pod: V1Pod, reason: str):
        job = pod.metadata.labels['job']
        if self.logs is not None:
            with self.logs.open(job, 'w') as fp:
                fp.write(self.podman.logs(pod))
        if self.done_file is not None:
            data = {
                'reason': reason,
                'start_time': pod.metadata.creation_timestamp,
                'end_time': datetime.now(timezone.utc),
                'image': pod.status.container_statuses[0].image,
                'node': pod.spec.node_name,
            }
            self.done_file.dump(job, data)
        self.podman.delete(pod)

    def update(self):
        result = super().update()

        pods = self.podman.query(task=self.name)
        for pod in pods:
            result = True
            try:
                uptime: timedelta = datetime.now(timezone.utc) - pod.metadata.creation_timestamp
                total_min = uptime.total_seconds() // 60
                uptime_hours, uptime_min = divmod(total_min, 60)
                l.debug("Pod %s is alive for %dh%dm", pod.metadata.name, uptime_hours, uptime_min)
                if pod.status.phase in ('Succeeded', 'Failed'):
                    l.debug("...finished: %s", pod.status.phase)
                    self._cleanup(pod, pod.status.phase)
                elif self.timeout is not None and uptime > self.timeout:
                    l.debug("...timed out")
                    self.handle_timeout(pod)
                    self._cleanup(pod, 'Timeout')
            except:
                l.exception("Failed to update kube task %s:%s", self.name, pod.metadata.name)
        return result

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
        return False

    def launch(self, job):
        pass

class InProcessSyncTask(Task):
    def __init__(
            self,
            name: str,
            done: MetadataRepository=None,
            ready: Optional[Repository]=None,
            func: Optional[Callable[['InProcessSyncTask', str], None]]=None,
    ):
        super().__init__(name, ready=ready)

        self.done_file = done
        self.func = func
        self.link("done", done, is_status=True, inhibits_start=True, required_for_output=True)

    def __call__(self, f: Callable[['InProcessSyncTask', str], None]) -> 'InProcessSyncTask':
        self.func = f
        return self

    def launch(self, job):
        if self.func is None:
            l.error("InProcessSyncTask missing a function to call")
            return

        start_time = datetime.now()
        l.debug("Launching in-process %s:%s...", self.name, job)
        try:
            self.func(self, job)
        except Exception as e:
            l.info("In-process task %s:%s failed", self.name, job, exc_info=True)
            result = {'result': "exception", "exception": repr(e), 'traceback': traceback.format_tb(e.__traceback__)}
        else:
            l.debug("...success")
            result = {'result': "success"}
        result['start_time'] = start_time
        result['end_time'] = datetime.now()
        self.done_file.dump(job, result)

class ExecutorTask(Task):
    def __init__(self, name: str, executor: Executor, done: MetadataRepository, ready: Optional[Repository]=None, func: Optional[Callable[['ExecutorTask', str], None]]=None):
        super().__init__(name, ready)

        self.executor = executor
        self.func = func
        self.jobs: Dict[Future, Tuple[str, datetime]] = {}
        self.rev_jobs: Dict[str, Future] = {}
        self.live = ExecutorLiveRepo(self)
        self.done = done
        self.link("live", self.live, is_status=True, inhibits_output=True, inhibits_start=True)
        self.link("done", self.done, is_status=True, required_for_output=True, inhibits_start=True)

    def __call__(self, f: Callable[['ExecutorTask', str], None]) -> 'ExecutorTask':
        self.func = f
        return self

    def update(self):
        result = bool(self.jobs)
        done, _ = wait(self.jobs, 0, FIRST_EXCEPTION)
        for finished_job in done:
            job, start_time = self.jobs.pop(finished_job)
            self.rev_jobs.pop(job)
            e = finished_job.exception()
            if e is not None:
                l.info("Executor task %s:%s failed", self.name, job, exc_info=e)
                data = {
                    'result': "exception",
                    "exception": repr(e),
                    'traceback': traceback.format_tb(e.__traceback__),
                    'end_time': datetime.now(),
                }
            else:
                l.debug("...executor task %s:%s success", self.name, job)
                data = {'result': "success", 'end_time': finished_job.result()}
            data['start_time'] = start_time
            self.done.dump(job, data)
        return result

    def launch(self, job):
        if self.func is None:
            raise ValueError("InProcessAsyncTask %s has func None" % self.name)
        l.debug("Launching %s:%s with %s...", self.name, job, self.executor)
        running_job = self.executor.submit(self._timestamped_func, self.func, self, job)
        self.jobs[running_job] = (job, datetime.now())
        self.rev_jobs[job] = running_job

    def cancel(self, job):
        future = self.rev_jobs.pop(job)
        if future is not None:
            future.cancel()
            self.jobs.pop(future)

    @staticmethod
    def _timestamped_func(func, task, job):
        func(task, job)
        return datetime.now()
