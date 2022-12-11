from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Iterable,
    Optional,
    Protocol,
    Tuple,
    Union,
)
from asyncio import Future
from concurrent.futures import FIRST_EXCEPTION, Executor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from pathlib import Path
import asyncio
import inspect
import logging
import os
import sys
import traceback

from kubernetes_asyncio.client import V1Pod
import aiofiles.os
import jinja2.async_utils
import jinja2.compiler
import yaml

from .pod_manager import PodManager
from .repository import (
    AggregateAndRepository,
    AggregateOrRepository,
    BlobRepository,
    BlockingRepository,
    ExecutorLiveRepo,
    FileRepository,
    LiveKubeRepository,
    MetadataRepository,
    RelatedItemRepository,
    Repository,
)

l = logging.getLogger(__name__)

__all__ = (
    "Link",
    "Task",
    "KubeTask",
    "InProcessSyncTask",
    "ExecutorTask",
    "KubeFunctionTask",
    "settings",
)


class RepoHandlingMode(Enum):
    LAZY = auto()
    SMART = auto()
    EAGER = auto()


async def build_env(env, job, mode: RepoHandlingMode):
    result = {}
    for key, val in env.items():
        if isinstance(val, Repository):
            repo = val
        elif isinstance(val, Link):
            repo = val.repo
        else:
            result[key] = val
            continue
        if mode == RepoHandlingMode.SMART:
            result[key] = repo
        elif mode == RepoHandlingMode.LAZY:
            result[key] = repo.info(job)
        else:
            result[key] = await repo.info(job)
    return result


SYNCHRONOUS = False
METADATA = True


def settings(sync, meta):
    global SYNCHRONOUS, METADATA
    SYNCHRONOUS = sync
    METADATA = meta


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
    def __init__(self, name: str, ready: Optional[Repository] = None):
        self.name = name
        self._ready = ready
        self.links: Dict[str, Link] = {}

    def __repr__(self):
        return f"<{type(self).__name__} {self.name}>"

    @property
    def ready(self):
        if self._ready is not None:
            return self._ready
        return BlockingRepository(
            AggregateAndRepository(**self.required_for_start),
            AggregateOrRepository(**self.inhibits_start),
        )

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
        output: "Task",
        output_links: Optional[Iterable[str]] = None,
        meta: bool = True,
        translator: Optional[Repository] = None,
        translate_allow_deletes=False,
        translate_prefetch_lookup=True,
    ):
        for name, link in output.links.items():
            link_attrs = {}
            if link.inhibits_output and meta:
                link_attrs["inhibits_start"] = True
            if link.is_output and (output_links is None or name in output_links):
                link_attrs["is_input"] = True
            if link.required_for_output and meta:
                link_attrs["required_for_start"] = True
            if not link.is_output:
                name = f"{output.name}_{name}"
            if link_attrs:
                repo = link.repo
                if translator is not None:
                    repo = RelatedItemRepository(
                        repo,
                        translator,
                        allow_deletes=translate_allow_deletes,
                        prefetch_lookup=translate_prefetch_lookup,
                    )
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

    async def launch_all(self):
        launchers = [self._launch(job) async for job in self.ready]
        await asyncio.gather(*launchers)
        return bool(launchers)

    async def _launch(self, job):
        try:
            l.debug("Launching %s:%s", self.name, job)
            await self.launch(job)
        except:
            l.exception("Failed to launch %s:%s", self, job)

    async def launch(self, job):
        raise NotImplementedError

    async def update(self) -> bool:
        """
        Performs any maintenance operations on the set of live tasks. Returns True if literally anything interesting happened.
        """
        return False

    async def validate(self):
        """
        Raise an exception if for any reason the task is misconfigured.
        :return:
        """
        pass


class ParanoidAsyncGenerator(jinja2.compiler.CodeGenerator):
    def write_commons(self):
        self.writeline("from jinja2.async_utils import _common_primitives")
        self.writeline("import inspect")
        self.writeline("seen = {}")
        self.writeline("async def auto_await2(value):")
        self.writeline("    if type(value) in _common_primitives:")
        self.writeline("        return value")
        self.writeline("    if inspect.isawaitable(value):")
        self.writeline("        cached = seen.get(value)")
        self.writeline("        if cached is None:")
        self.writeline("            cached = await value")
        self.writeline("            seen[value] = cached")
        self.writeline("        return cached")
        self.writeline("    return value")
        super().write_commons()

    def visit_Name(self, node, frame):
        if self.environment.is_async:
            self.write("(await auto_await2(")

        super().visit_Name(node, frame)

        if self.environment.is_async:
            self.write("))")


class KubeTask(Task):
    def __init__(
        self,
        podman: Callable[[], PodManager],
        name: str,
        template: Union[str, Path],
        logs: Optional[BlobRepository],
        done: Optional[MetadataRepository],
        timeout: Optional[timedelta] = None,
        env: Optional[Dict[str, Any]] = None,
        ready: Optional[Repository] = None,
    ):
        super().__init__(name, ready)

        self.template = template
        self._podman = podman
        self.logs = logs
        self.timeout = timeout
        self.done = done
        self.env = env if env is not None else {}

        self.link(
            "live",
            LiveKubeRepository(self),
            is_status=True,
            inhibits_start=True,
            inhibits_output=True,
        )
        if logs:
            self.link(
                "logs",
                logs,
                is_status=True,
                inhibits_start=True,
                required_for_output=True,
            )
        if done:
            self.link(
                "done",
                done,
                is_status=True,
                inhibits_start=True,
                required_for_output=True,
            )

    @property
    def podman(self):
        return self._podman()

    async def render_template(self, env):
        j = jinja2.Environment(enable_async=True)
        j.code_generator_class = ParanoidAsyncGenerator
        if await aiofiles.os.path.isfile(self.template):
            async with aiofiles.open(self.template, "r") as fp:
                template = await fp.read()
        else:
            template = self.template
        template = j.from_string(template)
        rendered = await template.render_async(**env)
        return yaml.safe_load(rendered)

    async def launch(self, job):
        env_input = dict(vars(self))
        env_input.update(self.links)
        env_input.update(self.env)
        env = await build_env(env_input, job, RepoHandlingMode.LAZY)
        env["job"] = job
        env["task"] = self.name
        env["argv0"] = os.path.basename(sys.argv[0])
        parsed = await self.render_template(env)
        for item in env.values():
            if asyncio.iscoroutine(item):
                item.close()

        await self.podman.launch(job, self.name, parsed)

    async def _cleanup(self, pod: V1Pod, reason: str):
        job = pod.metadata.labels["job"]
        if self.logs is not None:
            async with await self.logs.open(job, "w") as fp:
                try:
                    await fp.write(await self.podman.logs(pod))
                except TimeoutError:
                    await fp.write("<failed to fetch logs>\n")
        if self.done is not None:
            data = {
                "reason": reason,
                "start_time": pod.metadata.creation_timestamp,
                "end_time": datetime.now(timezone.utc),
                "image": pod.status.container_statuses[0].image,
                "node": pod.spec.node_name,
            }
            await self.done.dump(job, data)
        await self.podman.delete(pod)

    async def update(self):
        self.podman.warned.clear()
        result = await super().update()

        pods = await self.podman.query(task=self.name)
        for pod in pods:
            result = True
            try:
                uptime: timedelta = datetime.now(timezone.utc) - pod.metadata.creation_timestamp
                total_min = uptime.total_seconds() // 60
                uptime_hours, uptime_min = divmod(total_min, 60)
                l.debug(
                    "Pod %s is alive for %dh%dm",
                    pod.metadata.name,
                    uptime_hours,
                    uptime_min,
                )
                if pod.status.phase in ("Succeeded", "Failed"):
                    l.debug("...finished: %s", pod.status.phase)
                    await self._cleanup(pod, pod.status.phase)
                elif self.timeout is not None and uptime > self.timeout:
                    l.debug("...timed out")
                    await self.handle_timeout(pod)
                    await self._cleanup(pod, "Timeout")
            except:
                l.exception("Failed to update kube task %s:%s", self.name, pod.metadata.name)
        return result

    async def handle_timeout(self, pod):
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
        done: Optional[FileRepository] = None,
        stdout: Optional[FileRepository] = None,
        stderr: Optional[Union[FileRepository, StderrIsStdout]] = None,
        ready: Optional[Repository] = None,
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


class FunctionTaskProtocol(Protocol):
    def __call__(self, job: str, **kwargs) -> Coroutine:
        ...


class InProcessSyncTask(Task):
    def __init__(
        self,
        name: str,
        done: MetadataRepository,
        ready: Optional[Repository] = None,
        func: Optional[FunctionTaskProtocol] = None,
    ):
        super().__init__(name, ready=ready)

        self.done = done
        self.func = func
        self.link("done", done, is_status=True, inhibits_start=True, required_for_output=True)
        self._env: Dict[str, Any] = {}

    def __call__(self, f: FunctionTaskProtocol) -> "InProcessSyncTask":
        self.func = f
        return self

    async def validate(self):
        if self.func is None:
            raise ValueError("InProcessSyncTask.func is None")

        sig = inspect.signature(self.func, follow_wrapped=True)
        for name in sig.parameters.keys():
            if name == "job":
                self._env[name] = None
            elif name in self.links:
                self._env[name] = self.links[name].repo
            else:
                raise NameError("%s takes parameter %s but no such argument is available" % (self.func, name))

    async def launch(self, job):
        start_time = datetime.now()
        l.debug("Launching in-process %s:%s...", self.name, job)
        args = dict(self._env)
        if "job" in args:
            args["job"] = job
        args = await build_env(args, job, RepoHandlingMode.SMART)
        try:
            await self.func(**args)
        except Exception as e:
            l.info("In-process task %s:%s failed", self.name, job, exc_info=True)
            result = {
                "result": "exception",
                "exception": repr(e),
                "traceback": traceback.format_tb(e.__traceback__),
            }
        else:
            l.debug("...success")
            result = {"result": "success"}
        result["start_time"] = start_time
        result["end_time"] = datetime.now()
        if METADATA:
            await self.done.dump(job, result)


class ExecutorTask(Task):
    def __init__(
        self,
        name: str,
        executor: Executor,
        done: MetadataRepository,
        ready: Optional[Repository] = None,
        func: Optional[Callable] = None,
    ):
        super().__init__(name, ready)

        self.executor = executor
        self.func = func
        self.jobs: Dict[Future, Tuple[str, datetime]] = {}
        self.rev_jobs: Dict[str, Future] = {}
        self.live = ExecutorLiveRepo(self)
        self.done = done
        self._env: Dict[str, Any] = {}
        self.link("live", self.live, is_status=True, inhibits_output=True, inhibits_start=True)
        self.link(
            "done",
            self.done,
            is_status=True,
            required_for_output=True,
            inhibits_start=True,
        )

    def __call__(self, f: Callable) -> "ExecutorTask":
        self.func = f
        return self

    async def update(self):
        result = bool(self.jobs)
        done, _ = wait(self.jobs, 0, FIRST_EXCEPTION)
        for finished_job in done:
            job, start_time = self.jobs.pop(finished_job)
            # noinspection PyAsyncCall
            self.rev_jobs.pop(job)
            await self._cleanup(finished_job, job, start_time)
        return result

    async def _cleanup(self, job_future, job, start_time):
        e = job_future.exception()
        if e is not None:
            l.info("Executor task %s:%s failed", self.name, job, exc_info=e)
            data = {
                "result": "exception",
                "exception": repr(e),
                "traceback": traceback.format_tb(e.__traceback__),
                "end_time": datetime.now(),
            }
        else:
            l.debug("...executor task %s:%s success", self.name, job)
            data = {"result": "success", "end_time": job_future.result()}
        data["start_time"] = start_time
        await self.done.dump(job, data)

    async def validate(self):
        if self.func is None:
            raise ValueError("InProcessAsyncTask %s has func None" % self.name)

        sig = inspect.signature(self.func, follow_wrapped=True)
        for name in sig.parameters.keys():
            if name == "job":
                self._env[name] = None
            elif name in self.links:
                self._env[name] = self.links[name].repo
            else:
                raise NameError("%s takes parameter %s but no such argument is available" % (self.func, name))

    async def launch(self, job):
        l.debug("Launching %s:%s with %s...", self.name, job, self.executor)
        args = dict(self._env)
        if "job" in args:
            args["job"] = job
        args = await build_env(args, job, RepoHandlingMode.SMART)
        start_time = datetime.now()
        running_job = self.executor.submit(self._timestamped_func, self.func, args)
        if SYNCHRONOUS:
            while not running_job.done():
                await asyncio.sleep(0.1)
            await self._cleanup(running_job, job, start_time)
        else:
            self.jobs[running_job] = (job, start_time)
            self.rev_jobs[job] = running_job

    def cancel(self, job):
        future = self.rev_jobs.pop(job)
        if future is not None:
            future.cancel()
            self.jobs.pop(future)

    @staticmethod
    def _timestamped_func(func, args):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(func(**args))
        return datetime.now()


class KubeFunctionTask(KubeTask):
    def __init__(
        self,
        podman: Callable[[], PodManager],
        name: str,
        template: Union[str, Path],
        logs: Optional[BlobRepository],
        kube_done: Optional[MetadataRepository],
        func_done: Optional[MetadataRepository],
        func: Optional[Callable] = None,
    ):
        super().__init__(podman, name, template, logs, kube_done)
        self.func = func
        self.func_done = func_done
        if func_done is not None:
            self.link(
                "func_done",
                func_done,
                required_for_output=True,
                is_status=True,
                inhibits_start=True,
            )
        self._env: Dict[str, Any] = {}

    def __call__(self, f: Callable) -> "KubeFunctionTask":
        self.func = f
        return self

    async def validate(self):
        if self.func is None:
            raise ValueError("KubeFunctionTask %s has func None" % self.name)

        sig = inspect.signature(self.func, follow_wrapped=True)
        for name in sig.parameters.keys():
            if name == "job":
                self._env[name] = None
            elif name in self.links:
                self._env[name] = self.links[name].repo
            else:
                raise NameError("%s takes parameter %s but no such argument is available" % (self.func, name))

    async def launch(self, job):
        if SYNCHRONOUS:
            await self.launch_sync(job)
        else:
            await super().launch(job)

    async def launch_sync(self, job):
        start_time = datetime.now()
        l.debug("Launching --sync %s:%s...", self.name, job)
        args = dict(self._env)
        if "job" in args:
            args["job"] = job
        args = await build_env(args, job, RepoHandlingMode.SMART)
        try:
            await self.func(**args)
        except Exception as e:
            l.info("--sync task %s:%s failed", self.name, job, exc_info=True)
            result = {
                "result": "exception",
                "exception": repr(e),
                "traceback": traceback.format_tb(e.__traceback__),
            }
        else:
            l.debug("...success")
            result = {"result": "success"}
        result["start_time"] = start_time
        result["end_time"] = datetime.now()
        if METADATA:
            await self.func_done.dump(job, result)
