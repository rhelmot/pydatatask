"""
A Task is a unit of execution which can act on multiple repositories.

You define a task by instantiating a Task subclass and passing it to a Pipeline object.

Tasks are related to Repositories by Links. Links are created by
``Task.link("my_link_name", my_repository, **disposition)``. The main disposition kwargs you'll want to use are
``is_input`` and ``is_output``. See `Task.link` for more information.

For a shortcut for linking the output of one task as the input of another task, see `Task.plug`.

.. autodata:: STDOUT
"""
from typing import (
    TYPE_CHECKING,
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
from abc import abstractmethod
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

from kubernetes_asyncio.client import ApiException, V1Pod
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
    LiveKubeRepository,
    MetadataRepository,
    RelatedItemRepository,
    Repository,
)
from .resource_manager import ResourceManager, Resources
from .utils import async_copyfile

if TYPE_CHECKING:
    from .proc_manager import AbstractProcessManager

l = logging.getLogger(__name__)

__all__ = (
    "Link",
    "Task",
    "KubeTask",
    "ProcessTask",
    "InProcessSyncTask",
    "ExecutorTask",
    "KubeFunctionTask",
    "STDOUT",
)

# pylint: disable=broad-except,bare-except


class RepoHandlingMode(Enum):
    """
    Mode for the `build_env` function.

    LAZY = Build repositories and links into repository references
    SMART = Build repositories and links into un-awaited `info` coroutines.
    EAGER = Build repositories and links into awaited `info` values.
    """

    LAZY = auto()
    SMART = auto()
    EAGER = auto()


async def build_env(env: Dict[str, Any], job: str, mode: RepoHandlingMode) -> Dict[str, Any]:
    """
    Given a mapping from environment key names to values that can be used during templating. The way repositories are
    transformed into job-related information is controlled by the `RepoHandlingMode`.
    """
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


async def render_template(template, env: Dict[str, Any]):
    """
    Given a template and an environment, use jinja2 to parameterize the template with the environment.

    :param template:  A template string, or a path to a local template file.
    :param env:       A mapping from environment key names to values.
    :return:          The rendered template, as a string.
    """
    j = jinja2.Environment(
        enable_async=True,
        keep_trailing_newline=True,
    )
    j.code_generator_class = ParanoidAsyncGenerator
    if await aiofiles.os.path.isfile(template):
        async with aiofiles.open(template, "r") as fp:
            template_str = await fp.read()
    else:
        template_str = template
    templating = j.from_string(template_str)
    return await templating.render_async(**env)


@dataclass
class Link:
    """
    The dataclass for holding linked repositories and their disposition metadata. Don't create these manually, instead
    use `Task.link`.
    """

    repo: Repository
    is_input: bool = False
    is_output: bool = False
    is_status: bool = False
    inhibits_start: bool = False
    required_for_start: bool = False
    inhibits_output: bool = False
    required_for_output: bool = False


class Task:
    """
    The Task base class.
    """

    def __init__(self, name: str, ready: Optional[Repository] = None, disabled: bool = False):
        self.name = name
        self._ready = ready
        self.links: Dict[str, Link] = {}
        self.synchronous = False
        self.metadata = True
        self.disabled = disabled

    def __repr__(self):
        return f"<{type(self).__name__} {self.name}>"

    @property
    def ready(self):
        """
        Return the repository whose job membership is used to determine whether a task instance should be launched.
        If an override is provided to the constructor, this is that, otherwise it is
        ``AND(*requred_for_start, NOT(OR(*inhibits_start)))``.
        """
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
        is_input: bool = False,
        is_output: bool = False,
        is_status: bool = False,
        inhibits_start: bool = False,
        required_for_start: Optional[bool] = None,
        inhibits_output: bool = False,
        required_for_output: Optional[bool] = None,
    ):
        """
        Create a link between this task and a repository.

        :param name: The name of the link. Used only in the admin interface.
        :param repo: The repository to link.
        :param is_input: Whether this repository contains data which is consumed by the task. Default False.
        :param is_output: Whether this repository is populated by the task. Default False.
        :param is_status: Whether this task is populated with task-ephemeral data. Default False.
        :param inhibits_start: Whether membership in this repository should be used in the default ``ready`` repository
                               to prevent jobs for being launched. Default False.
        :param required_for_start: Whether membership in this repository should be used in the default ``ready``
                                   repository to allow jobs to be launched. If unspecified, defaults to ``is_input``.
        :param inhibits_output:     Whether this repository should become ``inhibits_start`` in tasks this task is
                                    plugged into. Default False.
        :param required_for_output: Whether this repository should become `required_for_start` in tasks this task is
                                    plugged into. If unspecified, defaults to ``is_output``.
        """
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
        """
        Link the output repositories from `output` as inputs to this task.

        :param output:  The task to plug into this one.
        :param output_links: Optional: An iterable allowlist of links to only use.
        :param meta: Whether to transfer repository inhibit- and required_for- dispositions. Default True.
        :param translator: Optional: A repository used to transform job identifiers. If this is provided, a job will be
                           said to be present in the resulting linked repository if the source repository contains the
                           key ``await translator.info(job)``.
        :param translate_allow_deletes:  If translator is provided, this controls whether attempts to delete from the
                                         translated repository will do anything. Default False.
        :param translate_prefetch_lookup: If translator is provided, this controls whether the translated repository
                                          will pre-fetch the list of translations on first access. Default True.
        """
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
        """
        A mapping from link name to repository for all links marked ``is_input``.
        """
        return {name: link.repo for name, link in self.links.items() if link.is_input}

    @property
    def output(self):
        """
        A mapping from link name to repository for all links marked ``is_output``.
        """
        return {name: link.repo for name, link in self.links.items() if link.is_output}

    @property
    def status(self):
        """
        A mapping from link name to repository for all links marked ``is_status``.
        """
        return {name: link.repo for name, link in self.links.items() if link.is_status}

    @property
    def inhibits_start(self):
        """
        A mapping from link name to repository for all links marked ``inhibits_start``.
        """
        return {name: link.repo for name, link in self.links.items() if link.inhibits_start}

    @property
    def required_for_start(self):
        """
        A mapping from link name to repository for all links marked ``required_for_start``.
        """
        return {name: link.repo for name, link in self.links.items() if link.required_for_start}

    @property
    def inhibits_output(self):
        """
        A mapping from link name to repository for all links marked ``inhibits_output``.
        """
        return {name: link.repo for name, link in self.links.items() if link.inhibits_output}

    @property
    def required_for_output(self):
        """
        A mapping from link name to repository for all links marked ``required_for_output``.
        """
        return {name: link.repo for name, link in self.links.items() if link.required_for_output}

    async def validate(self):
        """
        Raise an exception if for any reason the task is misconfigured. This is guaranteed to be called exactly once per
        pipeline, so it is safe to use for setup and initialization in an async context.
        """

    @abstractmethod
    async def update(self) -> bool:
        """
        Part one of the pipeline maintenance loop. Override this to perform any maintenance operations on the set of
        live tasks. Typically, this entails reaping finished processes.

        Returns True if literally anything interesting happened, or if there are any live tasks.
        """
        raise NotImplementedError

    async def launch_all(self):
        """
        Part two of the pipeline maintenance loop. Do not override this; the default implementation is typically pretty
        good - it enumerates `ready` and calls `launch` for each ready job.

        Returns True if there were any jobs to launch.
        """
        launchers = [self._launch(job) async for job in self.ready]
        await asyncio.gather(*launchers)
        return bool(launchers)

    async def _launch(self, job):
        try:
            l.debug("Launching %s:%s", self.name, job)
            await self.launch(job)
        except:
            l.exception("Failed to launch %s:%s", self, job)

    @abstractmethod
    async def launch(self, job):
        """
        Launch a job. Override this to begin execution of the provided job. Error handling will be done for you.
        """
        raise NotImplementedError


class ParanoidAsyncGenerator(jinja2.compiler.CodeGenerator):
    """
    A class to instrument jinja2 to be more aggressive about awaiting objects and to cache their results. Probably
    don't use this directly.
    """

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
    """
    A task which runs a kubernetes pod.

    Will automatically link a `LiveKubeRepository` as "live" with
    ``inhibits_start, inhibits_output, is_status``
    """

    def __init__(
        self,
        name: str,
        podman: Callable[[], PodManager],
        resources: ResourceManager,
        template: Union[str, Path],
        logs: Optional[BlobRepository],
        done: Optional[MetadataRepository],
        timeout: Optional[timedelta] = None,
        env: Optional[Dict[str, Any]] = None,
        ready: Optional[Repository] = None,
    ):
        """
        :param name: The name of the task.
        :param podman: A callable returning a PodManager to use to connect to the cluster.
        :param resources: A ResourceManager instance. Tasks launched will contribute to its quota and be denied if they
                          would break the quota.
        :param template: YAML markup for a pod manifest template, either as a string or a path to a file.
        :param logs: Optional: A BlobRepository to dump pod logs to on completion. Linked as "logs" with
                               ``inhibits_start, required_for_output, is_status``.
        :param done: A MetadataRepository in which to dump some information about the pod's lifetime and termination on
                     completion. Linked as "done" with ``inhibits_start, required_for_output, is_status``.
        :param timeout: Optional: When a pod is found to have been running continuously for this amount of time, it will
                        be timed out and stopped. The method `handle_timeout` will be called in-process.
        :param env:     Optional: Additional keys to add to the template environment.
        :param ready:   Optional: A repository from which to read task-ready status.

        It is highly recommended to provide one or more of ``done`` or ``logs`` so that at least one link is present
        with ``inhibits_start``.
        """
        super().__init__(name, ready)

        self.template = template
        self.resources = resources
        self._podman = podman
        self.logs = logs
        self.timeout = timeout
        self.done = done
        self.env = env if env is not None else {}
        self.warned = False

        self.resources.register(self._get_load)

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
    def podman(self) -> PodManager:
        """
        The pod manager instance for this task. Will raise an error if the manager is provided by an unopened session.
        """
        return self._podman()

    async def _get_load(self):
        usage = Resources()

        try:
            for pod in (
                await self.podman.v1.list_namespaced_pod(
                    self.podman.namespace, label_selector=f"app={self.podman.app},task={self.name}"
                )
            ).items:
                for container in pod.spec.containers:
                    usage += Resources.parse(
                        container.resources.requests["cpu"], container.resources.requests["memory"]
                    )
        except ApiException as e:
            if e.reason != "Forbidden":
                raise

        return usage

    async def launch(self, job):
        env_input = dict(vars(self))
        env_input.update(self.links)
        env_input.update(self.env)
        env = await build_env(env_input, job, RepoHandlingMode.LAZY)
        env["job"] = job
        env["task"] = self.name
        env["argv0"] = os.path.basename(sys.argv[0])
        manifest = yaml.safe_load(await render_template(self.template, env))
        for item in env.values():
            if asyncio.iscoroutine(item):
                item.close()

        assert manifest["kind"] == "Pod"
        spec = manifest["spec"]
        spec["restartPolicy"] = "Never"

        request = Resources()
        for container in spec["containers"]:
            request += Resources.parse(
                container["resources"]["requests"]["cpu"], container["resources"]["requests"]["memory"]
            )

        limit = await self.resources.reserve(request)
        if limit is None:
            try:
                await self.podman.launch(job, self.name, manifest)
            except:
                await self.resources.relinquish(request)
                raise
        elif not self.warned:
            l.warning("Cannot launch %s: %s limit", self, limit)
            self.warned = True

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

        await self.delete(pod)

    async def delete(self, pod: V1Pod):
        """
        Kill a pod and relinquish its resources without marking the task as complete.
        """
        request = Resources()
        for container in pod.spec.containers:
            request += Resources.parse(container.resources.requests["cpu"], container.resources.requests["memory"])
        await self.podman.delete(pod)
        await self.resources.relinquish(request)

    async def update(self):
        self.warned = False
        result = False

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
            except Exception:
                l.exception("Failed to update kube task %s:%s", self.name, pod.metadata.name)
        return result

    async def handle_timeout(self, pod: V1Pod):
        """
        You may override this method in a subclass, and it will be called whenever a pod times out. You can use this
        method to e.g. scrape in-progress data out of the pod via an exec.
        """


class _StderrIsStdout:
    pass


STDOUT = _StderrIsStdout()


class ProcessTask(Task):
    """
    A task that runs a script. The interpreter is specified by the shebang, or the default shell if none present.
    The execution environment for the task is defined by the ProcessManager instance provided as an argument.
    """

    def __init__(
        self,
        name: str,
        manager: Callable[[], "AbstractProcessManager"],
        resource_manager: "ResourceManager",
        job_resources: "Resources",
        pids: MetadataRepository,
        template: str,
        environ: Optional[Dict[str, str]] = None,
        done: Optional[MetadataRepository] = None,
        stdin: Optional[BlobRepository] = None,
        stdout: Optional[BlobRepository] = None,
        stderr: Optional[Union[BlobRepository, _StderrIsStdout]] = None,
        ready: Optional[Repository] = None,
    ):
        """
        :param name: The name of this task.
        :param manager: A callable returnint the process manager with control over the target execution environment.
        :param resource_manager: A ResourceManager instance. Tasks launched will contribute to its quota and be denied
                                 if they would break the quota.
        :param job_resources: The amount of resources an individual job should contribute to the quota. Note that this
                              is currently **not enforced** target-side, so jobs may actually take up more resources
                              than assigned.
        :param pids: A metadata repository used to store the current live-status of processes. Will automatically be
                     linked as "pids" with ``is_status, inhibits_start, inhibits_output``.
        :param template: YAML markup for the template of a script to run, either as a string or a path to a file.
        :param environ: Additional environment variables to set on the target machine before running the task.
        :param done: Optional: A metadata repository in which to dump some information about the process's lifetime and
                     termination on completion. Linked as "done" with
                     ``inhibits_start, required_for_output, is_status``.
        :param stdin: Optional: A blob repository from which to source the process' standard input. The content will be
                      preloaded and transferred to the target environment, so the target does not need to be
                      authenticated to this repository. Linked as "stdin" with ``is_input``.
        :param stdout: Optional: A blob repository into which to dump the process' standard output. The content will be
                       transferred from the target environment on completion, so the target does not need to be
                       authenticated to this repository. Linked as "stdout" with ``is_output``.
        :param stderr: Optional: A blob repository into which to dump the process' standard error, or the constant
                       `pydatatask.task.STDOUT` to indicate that the stream should be interleaved with stdout.
                       Otherwise, the content will be transferred from the target environment on completion, so the
                       target does not need to be authenticated to this repository. Linked as "stderr" with
                       ``is_output``.
        :param ready:  Optional: A repository from which to read task-ready status.

        It is highly recommended to provide at least one of ``done``, ``stdout``, or ``stderr``, so that at least one
        link is present with ``inhibits_start``.
        """
        super().__init__(name, ready=ready)

        self.pids = pids
        self.template = template
        self.environ = environ
        self.done = done
        self.stdin = stdin
        self.stdout = stdout
        self._stderr = stderr
        self.job_resources = job_resources
        self.resource_manager = resource_manager
        self._manager = manager
        self.warned = False

        self.resource_manager.register(self._get_load)

        self.link("pids", pids, is_status=True, inhibits_start=True, inhibits_output=True)
        if stdin is not None:
            self.link("stdin", stdin, is_input=True)
        if stdout is not None:
            self.link("stdout", stdout, is_output=True)
        if isinstance(stderr, BlobRepository):
            self.link("stderr", stderr, is_output=True)
        if done is not None:
            self.link("done", done, is_status=True, required_for_output=True, inhibits_start=True)

    @property
    def manager(self) -> "AbstractProcessManager":
        """
        The process manager for this task. Will raise an error if the manager comes from a session which is closed.
        """
        return self._manager()

    async def _get_load(self) -> "Resources":
        count = 0
        async for _ in self.pids:
            count += 1
        return self.job_resources * count

    @property
    def stderr(self) -> Optional[BlobRepository]:
        """
        The repository into which stderr will be dumped, or None if it will go to the null device.
        """
        if isinstance(self._stderr, BlobRepository):
            return self._stderr
        else:
            return self.stdout

    @property
    def _unique_stderr(self):
        return self._stderr is not None and not isinstance(self._stderr, _StderrIsStdout)

    @property
    def basedir(self) -> Path:
        """
        The path in the target environment that will be used to store information about this task.
        """
        return self.manager.basedir / self.name

    async def update(self):
        self.warned = False
        job_map = await self.pids.info_all()
        pid_map = {meta["pid"]: job for job, meta in job_map.items()}
        expected_live = set(pid_map)
        try:
            live_pids = await self.manager.get_live_pids(expected_live)
        except Exception:
            l.error("Could not load live PIDs for %s", self, exc_info=True)
        else:
            died = expected_live - live_pids
            for pid in died:
                job = pid_map[pid]
                start_time = job_map[job]["start_time"]
                await self._reap(job, start_time)
        return bool(expected_live)

    async def launch(self, job):
        limit = await self.resource_manager.reserve(self.job_resources)
        if limit is not None:
            if not self.warned:
                l.warning("Cannot launch %s: %s limit", self, limit)
                self.warned = True
            return

        pid = None
        dirmade = False

        try:
            cwd = self.basedir / job / "cwd"
            await self.manager.mkdir(cwd)  # implicitly creates basedir / task / job
            dirmade = True
            stdin = None
            if self.stdin is not None:
                stdin = self.basedir / job / "stdin"
                async with await self.stdin.open(job, "rb") as fpr, await self.manager.open(stdin, "wb") as fpw:
                    await async_copyfile(fpr, fpw)
                stdin = str(stdin)
            stdout = None if self.stdout is None else str(self.basedir / job / "stdout")
            stderr = STDOUT
            if not isinstance(self._stderr, _StderrIsStdout):
                stderr = None if self._stderr is None else str(self.basedir / job / "stderr")
            env_src = dict(self.links)
            env_src["job"] = job
            env_src["task"] = self.name
            env = await build_env(env_src, job, RepoHandlingMode.LAZY)
            exe_path = self.basedir / job / "exe"
            exe_txt = await render_template(self.template, env)
            for item in env.values():
                if asyncio.iscoroutine(item):
                    item.close()
            async with await self.manager.open(exe_path, "w") as fp:
                await fp.write(exe_txt)
            pid = await self.manager.spawn(
                [str(exe_path)], self.environ, str(cwd), str(self.basedir / job / "return_code"), stdin, stdout, stderr
            )
            if pid is not None:
                await self.pids.dump(job, {"pid": pid, "start_time": datetime.now(tz=timezone.utc)})
        except:  # CLEAN UP YOUR MESS
            try:
                await self.resource_manager.relinquish(self.job_resources)
            except:
                pass
            try:
                if pid is not None:
                    await self.manager.kill(pid)
            except:
                pass
            try:
                if dirmade:
                    await self.manager.rmtree(self.basedir / job)
            except:
                pass
            raise

    async def _reap(self, job: str, start_time: datetime):
        try:
            if self.stdout is not None:
                async with await self.manager.open(self.basedir / job / "stdout", "rb") as fpr, await self.stdout.open(
                    job, "wb"
                ) as fpw:
                    await async_copyfile(fpr, fpw)
            if isinstance(self._stderr, BlobRepository):
                async with await self.manager.open(self.basedir / job / "stderr", "rb") as fpr, await self._stderr.open(
                    job, "wb"
                ) as fpw:
                    await async_copyfile(fpr, fpw)
            if self.done is not None:
                async with await self.manager.open(self.basedir / job / "return_code", "r") as fp1:
                    code = int(await fp1.read())
                await self.done.dump(
                    job, {"return_code": code, "start_time": start_time, "end_time": datetime.now(tz=timezone.utc)}
                )
            await self.manager.rmtree(self.basedir / job)
            await self.pids.delete(job)
            await self.resource_manager.relinquish(self.job_resources)
        except Exception:
            l.error("Could not reap process for %s:%s", self, job, exc_info=True)


class FunctionTaskProtocol(Protocol):
    """
    The protocol which is expected by the tasks which take a python function to execute.
    """

    def __call__(self, job: str, **kwargs) -> Coroutine:
        ...


class InProcessSyncTask(Task):
    """
    A task which runs in-process. Typical usage of this task might look like the following:

    .. code:: python

        @pydatatask.InProcessSyncTask("my_task", done_repo)
        async def my_task(job: str, inp: pydatatask.MetadataRepository, out: pydatatask.MetadataRepository):
            await out.dump(job, await inp.info(job))

        my_task.link("inp", repo_input, is_input=True)
        my_task.link("out", repo_output, is_output=True)
    """

    def __init__(
        self,
        name: str,
        done: MetadataRepository,
        ready: Optional[Repository] = None,
        func: Optional[FunctionTaskProtocol] = None,
    ):
        """
        :param name: The name of this task.
        :param done: A metadata repository to store some information about a job's runtime and termination on
                     completion.
        :param ready: Optional: A repository from which to read task-ready status.
        :param func: Optional: The async function to run as the task body, if you don't want to use this task as a
                     decorator.
        """
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

    async def update(self):
        pass

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
        if self.metadata:
            await self.done.dump(job, result)


class ExecutorTask(Task):
    """
    A task which runs python functions in a :external:class:`concurrent.futures.Executor`. This has not been tested on
    anything but the :external:class:`concurrent.futures.ThreadPoolExecutor`, so beware!

    See `InProcessSyncTask` for information on how to use instances of this class as decorators for their bodies.

    It is expected that the executor will perform all necessary resource quota management.
    """

    def __init__(
        self,
        name: str,
        executor: Executor,
        done: MetadataRepository,
        ready: Optional[Repository] = None,
        func: Optional[Callable] = None,
    ):
        """
        :param name: The name of this task.
        :param executor: The executor to run jobs in.
        :param done: A metadata repository to store some information about a job's runtime and termination on
                     completion.
        :param ready: Optional: A repository from which to read task-ready status.
        :param func: Optional: The async function to run as the task body, if you don't want to use this task as a
                     decorator.
        """
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
        if self.synchronous:
            while not running_job.done():
                await asyncio.sleep(0.1)
            await self._cleanup(running_job, job, start_time)
        else:
            self.jobs[running_job] = (job, start_time)
            self.rev_jobs[job] = running_job

    async def cancel(self, job):
        """
        Stop the current job from running, or do nothing if it is not running.
        """
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
    """
    A task which runs a python function on a kubernetes cluster. Requires a pod template which will execute a python
    script calling `pydatatask.main.main`. This works by running ``python3 main.py launch [task] [job] --sync``.

    Sample usage:

    .. code:: python

        @KubeFunctionTask(
            "my_task",
            podman,
            resman,
            '''
                apiVersion: v1
                kind: Pod
                spec:
                  containers:
                    - name: leader
                      image: "docker.example.com/my/image"
                      command:
                        - python3
                        - {{argv0}}
                        - launch
                        - "{{task}}"
                        - "{{job}}"
                        - "--force"
                        - "--sync"
                      resources:
                        requests:
                          cpu: 100m
                          memory: 256Mi
            ''',
            repo_logs,
            repo_done,
            repo_func_done,
        )
        async def my_task(job: str, inp: pydatatask.MetadataRepository, out: pydatatask.MetadataRepository):
            await out.dump(job, await inp.info(job))

        my_task.link("inp", repo_input, is_input=True)
        my_task.link("out", repo_output, is_output=True)
    """

    def __init__(
        self,
        name: str,
        podman: Callable[[], PodManager],
        resources: ResourceManager,
        template: Union[str, Path],
        logs: Optional[BlobRepository] = None,
        kube_done: Optional[MetadataRepository] = None,
        func_done: Optional[MetadataRepository] = None,
        env: Optional[Dict[str, Any]] = None,
        func: Optional[Callable] = None,
    ):
        """
        :param name: The name of this task.
        :param podman: A callable returning a PodManager to use to connect to the cluster.
        :param resources: A ResourceManager instance. Tasks launched will contribute to its quota and be denied if they
                          would break the quota.
        :param template: YAML markup for a pod manifest template that will run `pydatatask.main.main` as
                         ``python3 main.py launch [task] [job] --sync --force``, either as a string or a path to a file.
        :param logs: Optional: A BlobRepository to dump pod logs to on completion. Linked as "logs" with
                               ``inhibits_start, required_for_output, is_status``.
        :param kube_done: Optional: A MetadataRepository in which to dump some information about the pod's lifetime and
                          termination on completion. Linked as "done" with
                          ``inhibits_start, required_for_output, is_status``.
        :param func_done: Optional: A MetadataRepository in which to dump some information about the function's lifetime
                          and termination on completion. Linked as "func_done" with
                          ``inhibits_start, required_for_output, is_status``.
        :param env:     Optional: Additional keys to add to the template environment.
        :param ready:   Optional: A repository from which to read task-ready status.
        :param func: Optional: The async function to run as the task body, if you don't want to use this task as a
                     decorator.

        It is highly recommended to provide at least one of ``kube_done``, ``func_done``, or ``logs``, so that at least
        one link is present with ``inhibits_start``.
        """
        super().__init__(name, podman, resources, template, logs, kube_done, env=env)
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
        self._func_env: Dict[str, Any] = {}

    def __call__(self, f: Callable) -> "KubeFunctionTask":
        self.func = f
        return self

    async def validate(self):
        if self.func is None:
            raise ValueError("KubeFunctionTask %s has func None" % self.name)

        sig = inspect.signature(self.func, follow_wrapped=True)
        for name in sig.parameters.keys():
            if name == "job":
                self._func_env[name] = None
            elif name in self.links:
                self._func_env[name] = self.links[name].repo
            else:
                raise NameError("%s takes parameter %s but no such argument is available" % (self.func, name))

    async def launch(self, job):
        if self.synchronous:
            await self._launch_sync(job)
        else:
            await super().launch(job)

    async def _launch_sync(self, job):
        start_time = datetime.now()
        l.debug("Launching --sync %s:%s...", self.name, job)
        args = dict(self._func_env)
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
        if self.metadata:
            await self.func_done.dump(job, result)
