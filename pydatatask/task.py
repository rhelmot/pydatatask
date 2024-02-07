"""A Task is a unit of execution which can act on multiple repositories.

You define a task by instantiating a Task subclass and passing it to a Pipeline object.

Tasks are related to Repositories by Links. Links are created by
``Task.link("my_link_name", my_repository, LinkKind.Something)``. See `Task.link` for more information.

.. autodata:: STDOUT
"""

from __future__ import annotations

from typing import (
    Any,
    Awaitable,
    Callable,
    DefaultDict,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    Union,
)
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import FIRST_EXCEPTION
from concurrent.futures import Executor as FuturesExecutor
from concurrent.futures import Future as ConcurrentFuture
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from pathlib import Path
import asyncio
import copy
import getpass
import inspect
import logging
import os
import shlex
import string
import sys
import traceback

from kubernetes_asyncio.client import ApiException, V1Pod
import aiofiles.os
import jinja2.async_utils
import jinja2.compiler
import sonyflake
import yaml

from pydatatask.host import LOCAL_HOST, Host, HostOS
import pydatatask

from . import executor as execmodule
from . import repository as repomodule
from .quota import Quota, QuotaManager, localhost_quota_manager
from .utils import (
    STDOUT,
    AReadStreamBase,
    AsyncReaderQueueStream,
    _StderrIsStdout,
    async_copyfile,
    crypto_hash,
    supergetattr,
    supergetattr_path,
)

l = logging.getLogger(__name__)

__all__ = (
    "LinkKind",
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


@dataclass
class TemplateInfo:
    """The data necessary to assemble a template including a link argument."""

    arg: Any
    preamble: Optional[Any] = None
    epilogue: Optional[Any] = None
    file_host: Optional[Host] = None


idgen = sonyflake.SonyFlake()


async def render_template(template, template_env: Dict[str, Any]):
    """Given a template and an environment, use jinja2 to parameterize the template with the environment.

    :param template: A template string, or a path to a local template file.
    :param template_env: A mapping from environment key names to values.
    :return: The rendered template, as a string.
    """
    j = jinja2.Environment(
        undefined=jinja2.StrictUndefined,
        enable_async=True,
        keep_trailing_newline=True,
    )

    def shquote(s):
        return shlex.quote(str(s))

    j.filters["shquote"] = shquote
    j.filters["to_yaml"] = yaml.safe_dump
    j.code_generator_class = ParanoidAsyncGenerator
    if await aiofiles.os.path.isfile(template):
        async with aiofiles.open(template, "r") as fp:
            template_str = await fp.read()
    else:
        template_str = template
    templating = j.from_string(template_str)
    return await templating.render_async(**template_env)


class LinkKind(Enum):
    """The way a given link should be made available as a template parameter."""

    InputRepo = auto()
    InputId = auto()
    InputMetadata = auto()
    InputFilepath = auto()
    OutputRepo = auto()
    OutputId = auto()
    OutputFilepath = auto()
    StreamingOutputFilepath = auto()
    StreamingInputFilepath = auto()
    RequestedInput = auto()


INPUT_KINDS: Set[LinkKind] = {
    LinkKind.InputId,
    LinkKind.InputMetadata,
    LinkKind.InputFilepath,
    LinkKind.InputRepo,
    LinkKind.StreamingInputFilepath,
    LinkKind.RequestedInput,
}
OUTPUT_KINDS: Set[LinkKind] = {
    LinkKind.OutputId,
    LinkKind.OutputFilepath,
    LinkKind.OutputRepo,
    LinkKind.StreamingOutputFilepath,
}


@dataclass
class Link:
    """The dataclass for holding linked repositories and their disposition metadata.

    Don't create these manually, instead use `Task.link`.
    """

    repo: "repomodule.Repository"
    kind: Optional[LinkKind] = None
    key: Optional[Union[str, Callable[[str], Awaitable[str]]]] = None
    cokeyed: Dict[str, "repomodule.Repository"] = field(default_factory=dict)
    auto_meta: Optional[str] = None
    auto_values: Optional[Any] = None
    is_input: bool = False
    is_output: bool = False
    is_status: bool = False
    inhibits_start: bool = False
    required_for_start: Union[bool, str] = False
    inhibits_output: bool = False
    required_for_output: bool = False


class Task(ABC):
    """The Task base class."""

    def __init__(
        self,
        name: str,
        done: "repomodule.MetadataRepository",
        ready: Optional["repomodule.Repository"] = None,
        disabled: bool = False,
        failure_ok: bool = False,
        long_running: bool = False,
        timeout: Optional[timedelta] = None,
        queries: Optional[Dict[str, pydatatask.query.query.Query]] = None,
    ):
        self.name = name
        self._ready = ready
        self.links: Dict[str, Link] = {}
        self.synchronous = False
        self.metadata = True
        self.disabled = disabled
        self.agent_url = ""
        self.agent_secret = ""
        self._related_cache: Dict[str, Any] = {}
        self.long_running = long_running
        self.annotations: Dict[str, str] = {}
        self.fail_fast = False
        self.timeout = timeout
        self.queries = queries or {}
        self.done = done
        self.failure_ok = failure_ok
        self.success = pydatatask.query.repository.QueryRepository(
            "done.filter[.getsuccess]()", {"success": pydatatask.query.parser.QueryValueType.Bool}, {"done": done}
        )

        self.link(
            "done",
            done,
            None,
            is_status=True,
            inhibits_start=True,
            required_for_output=failure_ok,
        )

        self.link(
            "success",
            self.success,
            None,
            is_status=True,
            required_for_output=not failure_ok,
        )

    def __repr__(self):
        return f"<{type(self).__name__} {self.name}>"

    def cache_flush(self):
        """Flush any in-memory caches."""
        self._related_cache.clear()
        for link in self.links.values():
            link.repo.cache_flush()

    @property
    @abstractmethod
    def host(self) -> Host:
        """Return the host which will eventually execute the task.

        This is used to determine what resources will and will not be available locally during task execution.
        """
        raise NotImplementedError

    def mktemp(self, identifier: str) -> str:
        """Generate a temporary filepath for the task host system."""
        return self.host.mktemp(identifier)

    def mk_http_get(self, filename: str, url: str, headers: Dict[str, str]) -> Any:
        """Generate logic to perform an http download for the task host system.

        For shell script-based tasks, this will be a shell script, but for other tasks it may be other objects.
        """
        return self.host.mk_http_get(filename, url, headers)

    def mk_http_post(
        self, filename: str, url: str, headers: Dict[str, str], output_filename: Optional[str] = None
    ) -> Any:
        """Generate logic to perform an http upload for the task host system.

        For shell script-based tasks, this will be a shell script, but for other tasks it may be other objects.
        """
        return self.host.mk_http_post(filename, url, headers, output_filename)

    def mk_repo_get(self, filename: str, link_name: str, job: str) -> Any:
        """Generate logic to perform an repository download for the task host system.

        For shell script-based tasks, this will be a shell script, but for other tasks it may be other objects.
        """
        is_filesystem = isinstance(self.links[link_name].repo, repomodule.FilesystemRepository)
        payload_filename = self.mktemp(job + "-zip") if is_filesystem else filename
        result = self.host.mk_http_get(
            payload_filename,
            f"{self.agent_url}/data/{self.name}/{link_name}/{job}",
            {"Cookie": "secret=" + self.agent_secret},
        )
        if is_filesystem:
            result += self.host.mk_unzip(filename, payload_filename)
        return result

    def mk_repo_put(self, filename: str, link_name: str, job: str, hostjob: Optional[str] = None) -> Any:
        """Generate logic to perform an repository insert for the task host system.

        For shell script-based tasks, this will be a shell script, but for other tasks it may be other objects.
        """
        is_filesystem = isinstance(self.links[link_name].repo, repomodule.FilesystemRepository)
        payload_filename = self.mktemp(job + "-zip") if is_filesystem else filename
        result = self.host.mk_http_post(
            payload_filename,
            f"{self.agent_url}/data/{self.name}/{link_name}/{job}"
            + (f"?hostjob={hostjob}" if hostjob is not None else ""),
            {"Cookie": "secret=" + self.agent_secret},
        )
        if is_filesystem:
            result = self.host.mk_zip(payload_filename, filename) + result
        return result

    def mk_query_function(self, query_name: str) -> Tuple[Any, Any]:
        """Generate a shell function (???

        abstraction leak) for the given query.
        """
        parameters = self.queries[query_name].parameters
        return (
            f"""
        query_{query_name}() {{
            INPUT_FILE=$(mktemp)
            while [ -n "$1" ]; do
                case "$1" in
                    {" ".join(f'--{p}) echo "{p}: $2">>$INPUT_FILE ;;' for p in parameters)}
                    *) echo "Bad query parameter $1" >&2; exit 1 ;;
                esac
                shift 2
            done
            {self.host.mk_http_post(
                "$INPUT_FILE",
                f"{self.agent_url}/query/{self.name}/{query_name}",
                {"Cookie": "secret=" + self.agent_secret},
                "/dev/stdout"
            )}
            rm $INPUT_FILE
        }}
        """,
            f"query_{query_name}",
        )

    def mk_download_function(self, link_name: str) -> Tuple[Any, Any]:
        """Generate a shell function (???

        abstraction leak) to download a parameterized entry from the given repository.
        """
        return (
            f"""
        download_{link_name}() {{
            {self.mk_repo_get("/dev/stdout", link_name, "$1")}
        }}
        """,
            f"download_{link_name}",
        )

    def mk_repo_put_cokey(
        self, filename: str, link_name: str, cokey_name: str, job: str, hostjob: Optional[str] = None
    ) -> Any:
        """Generate logic to upload a cokeyed link."""
        is_filesystem = isinstance(self.links[link_name].repo, repomodule.FilesystemRepository)
        payload_filename = self.mktemp(job + "-zip") if is_filesystem else filename
        result = self.host.mk_http_post(
            payload_filename,
            f"{self.agent_url}/cokeydata/{self.name}/{link_name}/{cokey_name}/{job}"
            + (f"?hostjob={hostjob}" if hostjob is not None else ""),
            {"Cookie": "secret=" + self.agent_secret},
        )
        if is_filesystem:
            result = self.host.mk_zip(payload_filename, filename) + result
        return result

    def mk_mkdir(self, filepath: str) -> Any:
        """Generate logic to perform a directory creation for the task host system.

        For shell script-based tasks, this will be a shell script, but for other tasks it may be other objects.
        """
        return self.host.mk_mkdir(filepath)

    def mk_watchdir_upload(
        self, filepath: str, link_name: str, hostjob: Optional[str]
    ) -> Tuple[Any, Any, Dict[str, str]]:
        """Misery and woe.

        If you're trying to debug something and you run across this, just ask rhelmot for help.
        """
        # leaky abstraction!
        if self.host.os != HostOS.Linux:
            raise ValueError("Not sure how to do this off of linux")
        upload = self.host.mktemp("upload")
        scratch = self.host.mktemp("scratch")
        finished = self.host.mktemp("finished")
        lock = self.host.mktemp("lock")
        cokeyed = {name: self.host.mktemp(name) for name in self.links[link_name].cokeyed}
        dict_result = dict(cokeyed)
        dict_result["lock"] = lock
        # auto_values = self.links[link_name].auto_values is not None
        auto_cokey = self.links[link_name].auto_meta

        def prep_upload(cokey, cokeydir):
            if auto_cokey == cokey:
                return f'(test -e "{cokeydir}/$f" && ln -s "{cokeydir}/$f" {upload} || ln -s /dev/null {upload}) && '
            else:
                return f"ln -s {cokeydir}/$f {upload}"

        return (
            f"""
        {self.host.mk_mkdir(filepath)}
        {self.host.mk_mkdir(scratch)}
        {self.host.mk_mkdir(lock)}
        {'; '.join(self.host.mk_mkdir(cokey_dir) for cokey_dir in cokeyed.values())}
        idgen() {{
          echo $(($(shuf -i0-255 -n1) +
                  $(shuf -i0-255 -n1)*0x100 +
                  $(shuf -i0-255 -n1)*0x10000 +
                  $(shuf -i0-255 -n1)*0x1000000 +
                  $(shuf -i0-255 -n1)*0x100000000 +
                  $(shuf -i0-255 -n1)*0x10000000000 +
                  $(shuf -i0-255 -n1)*0x1000000000000 +
                  $(shuf -i0-127 -n1)*0x100000000000000))
        }}
        watcher() {{
          WATCHER_LAST=
          cd {filepath}
          while [ -z "$WATCHER_LAST" ]; do
            sleep 5
            if [ -f {finished} ]; then
                WATCHER_LAST=1
            fi
            for f in *; do
              if [ -e "$f" ] && ! [ -e "{scratch}/$f" ] && ! [ -e "{lock}/$f" ]; then
                ID=$(idgen)
                ln -sf "$PWD/$f" {upload}
                {self.mk_repo_put(upload, link_name, "$ID", hostjob)}
                rm {upload}
                {'; '.join(
                  f'{prep_upload(cokey, cokeydir)}'
                  f'({self.mk_repo_put_cokey(upload, link_name, cokey, "$ID", hostjob)}); '
                  f'rm {upload}'
                  for cokey, cokeydir
                  in cokeyed.items())}
                echo $ID >"{scratch}/$f"
              fi
            done
          done
        }}
        watcher &
        WATCHER_PID=$!
        """,
            f"""
        echo "Finishing up"
        echo 1 >{finished}
        wait $WATCHER_PID
        """,
            dict_result,
        )

    def mk_watchdir_download(self, filepath: str, link_name: str, job: str) -> Any:
        """Misery and woe.

        If you're trying to debug something and you run across this, just ask rhelmot for help.
        """
        # leaky abstraction!
        if self.host.os != HostOS.Linux:
            raise ValueError("Not sure how to do this off of linux")
        scratch = self.host.mktemp("scratch")
        download = self.host.mktemp("download")
        stream_url = f"{self.agent_url}/stream/{self.name}/{link_name}/{job}"
        stream_headers = {"Cookie": f"secret={self.agent_secret}"}

        return f"""
        {self.host.mk_mkdir(filepath)}
        {self.host.mk_mkdir(scratch)}
        watcher() {{
            cd {filepath}
            while true; do
                sleep 5
                {self.mk_http_get(download, stream_url, stream_headers)}
                for ID in $(cat {download}); do
                    if ! [ -e "{scratch}/$ID" ]; then
                        {self.mk_repo_get(download, link_name, "$ID")}
                        mv {download} $ID
                        echo >{scratch}/$ID
                    fi
                done
            done
        }}
        watcher &
        """

    def _make_ready(self):
        """Return the repository whose job membership is used to determine whether a task instance should be
        launched.

        If an override is not provided to the constructor, this is that, otherwise it is
        ``AND(*requred_for_start, NOT(OR(*inhibits_start)))``.
        """
        base_listing = []
        scoped_listing = defaultdict(list)
        for link_name, (repo, scope) in self.required_for_start.items():
            if scope is True:
                base_listing.append((link_name, repo))
            else:
                scoped_listing[scope].append((link_name, repo))
        for scope, scope_listing in scoped_listing.items():
            if len(scope_listing) > 1:
                base_listing.append((str(scope), repomodule.AggregateOrRepository(**dict(scope_listing))))
            else:
                base_listing.append(scope_listing[0])

        return repomodule.BlockingRepository(
            repomodule.AggregateAndRepository(**dict(base_listing)),
            repomodule.AggregateOrRepository(**self.inhibits_start),
        )

    def derived_hash(self, job: str, link_name: str, along: bool = True) -> str:
        """For allocate-key links, determine the allocated job id.

        If along=False, determine the input job id which produced the allocated job id.
        """
        hashed = crypto_hash([self.name, link_name, self.links[link_name].repo])
        sign = 1 if along else -1
        if job.isdigit() and 0 <= int(job) < 2**64:
            return str((int(job) + sign * hashed) % 2**64)
        if all(c in string.hexdigits for c in job) and len(job) % 2 == 0:
            bytesjob = bytes.fromhex(job)
            intjob = int.from_bytes(bytesjob, "big")
            length = len(bytesjob)
            modulus = 2 ** (len(bytesjob) * 8)
            return ((intjob + sign * hashed) % modulus).to_bytes(length, "big").hex()
        raise ValueError("Fatal: Job must be structured (long decimal or arbitrary hex) to derive a hash")

    def link(
        self,
        name: str,
        repo: "repomodule.Repository",
        kind: Optional[LinkKind],
        key: Optional[Union[str, Callable[[str], Awaitable[str]]]] = None,
        cokeyed: Optional[Dict[str, "repomodule.Repository"]] = None,
        auto_meta: Optional[str] = None,
        auto_values: Optional[Any] = None,
        is_input: Optional[bool] = None,
        is_output: Optional[bool] = None,
        is_status: bool = False,
        inhibits_start: Optional[bool] = None,
        required_for_start: Optional[Union[bool, str]] = None,
        inhibits_output: Optional[bool] = None,
        required_for_output: Optional[bool] = None,
    ):
        """Create a link between this task and a repository.

        All the nullable paramters have their defaults picked based on other parameters.

        :param name: The name of the link. Used during templating.
        :param repo: The repository to link.
        :param kind: The way to transform this repository during templating.
        :param key: A related item from a different link to use instead of the current job when doing repository lookup.
            Or, a function taking the task's job and producing the desired job to do a lookup for.
        :param cokeyed: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.
        :param auto_meta: BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB.
        :param auto_values: CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC.
        :param is_input: Whether this repository contains data which is consumed by the task. Default based on kind.
        :param is_output: Whether this repository is populated by the task. Default based on kind.
        :param is_status: Whether this task is populated with task-ephemeral data. Default False.
        :param inhibits_start: Whether membership in this repository should be used in the default ``ready`` repository
            to prevent jobs for being launched. Default False.
        :param required_for_start: Whether membership in this repository should be used in the default ``ready``
            repository to allow jobs to be launched. If unspecified, defaults to ``is_input``.
        :param inhibits_output: Whether this repository should become ``inhibits_start`` in tasks this task is plugged
            into. Default False.
        :param required_for_output: Whether this repository should become `required_for_start` in tasks this task is
            plugged into. If unspecified, defaults to ``is_output``.
        """
        if is_input is None:
            is_input = kind in INPUT_KINDS
            if required_for_start is None and kind in (LinkKind.StreamingInputFilepath, LinkKind.RequestedInput):
                required_for_start = False
        if is_output is None:
            is_output = kind in OUTPUT_KINDS
        if required_for_start is None:
            required_for_start = is_input
        if inhibits_start is None:
            inhibits_start = False
        if required_for_output is None:
            required_for_output = False
        if inhibits_output is None:
            inhibits_output = False
        if key == "ALLOC" and is_input:
            raise ValueError("Link cannot be allocated key and input")

        self.links[name] = Link(
            repo=repo,
            key=key,
            kind=kind,
            cokeyed=cokeyed or {},
            auto_meta=auto_meta,
            auto_values=auto_values,
            is_input=is_input,
            is_output=is_output,
            is_status=is_status,
            inhibits_start=inhibits_start,
            required_for_start=required_for_start,
            inhibits_output=inhibits_output,
            required_for_output=required_for_output,
        )

    def _repo_related(self, linkname: str, seen: Optional[Set[str]] = None) -> "repomodule.Repository":
        if linkname in self._related_cache:
            return self._related_cache[linkname]

        if seen is None:
            seen = set()
        if linkname in seen:
            raise ValueError("Infinite recursion in repository related key lookup")
        link = self.links[linkname]
        if link.key is None:
            return link.repo

        if link.key == "ALLOC":
            mapped: repomodule.MetadataRepository = repomodule.FunctionCallMetadataRepository(
                lambda job: self.derived_hash(job, linkname), repomodule.AggregateAndRepository(**self.input)
            )
            prefetch_lookup = False

        elif callable(link.key):
            mapped = repomodule.FunctionCallMetadataRepository(
                link.key, repomodule.AggregateAndRepository(**self.input)
            )
            prefetch_lookup = False
        else:
            splitkey = link.key.split(".")
            related = self._repo_related(splitkey[0], seen)
            if not isinstance(related, repomodule.MetadataRepository):
                raise TypeError("Cannot do key lookup on repository which is not MetadataRepository")

            async def mapper(_job, info):
                try:
                    return str(supergetattr_path(info, splitkey[1:]))
                except:
                    return ""

            mapped = related.map(mapper)
            prefetch_lookup = True

        if isinstance(link.repo, repomodule.MetadataRepository):
            result: repomodule.Repository = repomodule.RelatedItemMetadataRepository(
                link.repo, mapped, prefetch_lookup=prefetch_lookup
            )
        else:
            result = repomodule.RelatedItemRepository(link.repo, mapped, prefetch_lookup=prefetch_lookup)

        self._related_cache[linkname] = result
        return result

    def _repo_filtered(self, job: str, linkname: str) -> "repomodule.Repository":
        link = self.links[linkname]
        if link.kind != LinkKind.StreamingInputFilepath:
            raise TypeError("Cannot automatically produce a filtered repo unless it's StreamingInput")
        if link.key is None or link.key == "ALLOC" or callable(link.key):
            raise TypeError("Bad key for repository filter")

        splitkey = link.key.split(".")
        related = self._repo_related(splitkey[0])
        if not isinstance(related, repomodule.MetadataRepository):
            raise TypeError("Cannot do key lookup on repository which is not MetadataRepository")

        async def mapper(_job, info):
            return str(supergetattr_path(info, splitkey[1:]))

        mapped = related.map(mapper)

        async def filterer(subjob: str) -> bool:
            return await mapped.info(subjob) == job

        if not isinstance(link.repo, repomodule.MetadataRepository):
            result = repomodule.FilterRepository(link.repo, filterer)
        else:
            result = repomodule.FilterMetadataRepository(link.repo, filterer)

        return result

    @property
    def ready(self):
        """Return the repository whose job membership is used to determine whether a task instance should be
        launched.

        If an override is provided to the constructor, this is that, otherwise it is
        ``AND(*requred_for_start, NOT(OR(*inhibits_start)))``.
        """
        if self._ready is None:
            self._ready = self._make_ready()
        return self._ready

    @property
    def input(self):
        """A mapping from link name to repository for all links marked ``is_input``."""
        result = {name: self._repo_related(name) for name, link in self.links.items() if link.is_input}
        if not result:
            raise ValueError("Every task must have at least one input")
        return result

    @property
    def output(self):
        """A mapping from link name to repository for all links marked ``is_output``."""
        return {name: self._repo_related(name) for name, link in self.links.items() if link.is_output}

    @property
    def status(self):
        """A mapping from link name to repository for all links marked ``is_status``."""
        return {name: self._repo_related(name) for name, link in self.links.items() if link.is_status}

    @property
    def inhibits_start(self):
        """A mapping from link name to repository for all links marked ``inhibits_start``."""
        return {name: self._repo_related(name) for name, link in self.links.items() if link.inhibits_start}

    @property
    def required_for_start(self):
        """A mapping from link name to repository for all links marked ``required_for_start``."""
        return {
            name: (self._repo_related(name), link.required_for_start)
            for name, link in self.links.items()
            if link.required_for_start
        }

    @property
    def inhibits_output(self):
        """A mapping from link name to repository for all links marked ``inhibits_output``."""
        return {name: self._repo_related(name) for name, link in self.links.items() if link.inhibits_output}

    @property
    def required_for_output(self):
        """A mapping from link name to repository for all links marked ``required_for_output``."""
        return {name: self._repo_related(name) for name, link in self.links.items() if link.required_for_output}

    async def validate(self):
        """Raise an exception if for any reason the task is misconfigured.

        This is guaranteed to be called exactly once per pipeline, so it is safe to use for setup and initialization in
        an async context.
        """

    @abstractmethod
    async def update(self) -> bool:
        """Part one of the pipeline maintenance loop. Override this to perform any maintenance operations on the set
        of live tasks. Typically, this entails reaping finished processes.

        Returns True if literally anything interesting happened, or if there are any live tasks.
        """
        raise NotImplementedError

    @abstractmethod
    async def launch(self, job):
        """Launch a job.

        Override this to begin execution of the provided job. Error handling will be done for you.
        """
        raise NotImplementedError

    # This should almost certainly instead return a context manager returning a object wrapping the jinja environment,
    # the preamble, and the epilogue which cleans up the coroutines on close.
    async def build_template_env(
        self,
        orig_job: str,
        env_src: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], List[Any], List[Any]]:
        """Transform an environment source into an environment according to Links.

        Returns the environment, a list of preambles, and a list of epilogues. These may be of any type depending on the
        task type.
        """
        if env_src is None:
            env_src = {}
        env_src.update(self.links)
        env_src["job"] = orig_job
        env_src["task"] = self.name

        def subkey(items: List[str]):
            src = result[items[0]]
            for i, subkey in enumerate(items[1:]):
                try:
                    src = supergetattr(src, subkey)
                except KeyError as e:
                    raise Exception(f"Cannot access {'.'.join(items[:i+2])} during link subkeying") from e
            return src

        preamble = []
        epilogue = []

        result = {}
        pending: DefaultDict[str, List[Tuple[str, Link]]] = defaultdict(list)
        for env_name, link in env_src.items():
            if not isinstance(link, Link):
                result[env_name] = link
                continue
            if link.kind is None:
                continue

            subjob = orig_job
            if link.key is not None and link.kind != LinkKind.StreamingInputFilepath:
                if link.key == "ALLOC":
                    subjob = self.derived_hash(orig_job, env_name)
                elif callable(link.key):
                    subjob = await link.key(orig_job)
                else:
                    items = link.key.split(".")
                    if items[0] in result:
                        subjob = subkey(items)
                    else:
                        pending[items[0]].append((env_name, link))
                        continue

            arg = self.instrument_arg(
                orig_job, await link.repo.template(subjob, self, link.kind, env_name, orig_job), link.kind
            )
            result[env_name] = arg.arg
            if arg.preamble is not None:
                preamble.append(arg.preamble)
            if arg.epilogue is not None:
                epilogue.append(arg.epilogue)
            if env_name in pending:
                for env_name2, link in pending[env_name]:
                    assert isinstance(link.key, str)
                    assert link.kind is not None
                    subjob = subkey(link.key.split("."))
                    arg = self.instrument_arg(
                        orig_job, await link.repo.template(subjob, self, link.kind, env_name2, orig_job), link.kind
                    )
                    result[env_name2] = arg.arg
                    if arg.preamble is not None:
                        preamble.append(arg.preamble)
                    if arg.epilogue is not None:
                        epilogue.append(arg.epilogue)
                pending.pop(env_name)

        if pending:
            raise Exception(f"Cannot access {' and '.join(pending)} during link subkeying")

        for query_name in self.queries:
            a_preamble, a_func = self.mk_query_function(query_name)
            if a_preamble is not None:
                preamble.append(a_preamble)
            result[query_name] = a_func

        return result, preamble, epilogue

    # pylint: disable=unused-argument
    def instrument_arg(self, job: str, arg: TemplateInfo, kind: LinkKind) -> TemplateInfo:
        """Do any task-specific processing on a template argument.

        This is called during build_template_env on each link, after Repository.template() runs on it.
        """
        return arg

    async def instrument_dump(
        self, content: AReadStreamBase, link: str, cokey: Optional[str], job: str, hostjob: Optional[str]
    ) -> AReadStreamBase:
        """Called when data is injected from a task through a link (currently only throgh the agent).

        Should return either the original stream or a new stream which is derivative of the old stream's content. By
        default, it applies auto_meta link data for metadata repositories.
        """
        if self.links[link].auto_meta == cokey and self.links[link].auto_values is not None and hostjob is not None:
            # just assume it's yaml. this is fine.
            data_str = await content.read()
            data = yaml.safe_load(data_str)
            env, _, _ = await self.build_template_env(hostjob)
            rendered = await self._render_auto_values(env, self.links[link].auto_values)
            if isinstance(rendered, dict):
                if data is None:
                    pass
                elif isinstance(data, dict):
                    rendered.update(data)
                else:
                    raise TypeError(f"Cannot combine auto-meta, bad types: dict and {type(data)}")
            elif isinstance(rendered, list):
                if data is None:
                    pass
                elif isinstance(data, list):
                    rendered.extend(data)
                else:
                    raise TypeError(f"Cannot combine auto-meta, bad types: list and {type(data)}")
            else:
                raise TypeError(f"Bad rendered auto-meta type: {type(rendered)}")

            for item in env.values():
                if asyncio.iscoroutine(item):
                    item.close()

            result = AsyncReaderQueueStream()
            result.write(yaml.safe_dump(rendered).encode())
            result.close()
            return result
        return content

    async def _render_auto_values(self, template_env: Dict[str, Any], template: Any) -> Any:
        if isinstance(template, str):
            return yaml.safe_load(await render_template(template, template_env))
        elif isinstance(template, dict):
            return {k: await self._render_auto_values(template_env, v) for k, v in template.items()}
        elif isinstance(template, list):
            return [await self._render_auto_values(template_env, i) for i in template]
        else:
            return template


class ParanoidAsyncGenerator(jinja2.compiler.CodeGenerator):
    """A class to instrument jinja2 to be more aggressive about awaiting objects and to cache their results.

    Probably don't use this directly.
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
    """A task which runs a kubernetes pod.

    Will automatically link a `LiveKubeRepository` as "live" with
    ``inhibits_start, inhibits_output, is_status``
    """

    def __init__(
        self,
        name: str,
        executor: "execmodule.Executor",
        quota_manager: QuotaManager,
        template: Union[str, Path],
        logs: Optional["repomodule.BlobRepository"],
        done: "repomodule.MetadataRepository",
        window: timedelta = timedelta(minutes=1),
        timeout: Optional[timedelta] = None,
        long_running: bool = False,
        template_env: Optional[Dict[str, Any]] = None,
        ready: Optional["repomodule.Repository"] = None,
        queries: Optional[Dict[str, pydatatask.query.query.Query]] = None,
        failure_ok: bool = False,
    ):
        """
        :param name: The name of the task.
        :param executor: The executor to use for this task.
        :param quota: A QuotaManager instance. Tasks launched will contribute to its quota and be denied if they would
            break the quota.
        :param template: YAML markup for a pod manifest template, either as a string or a path to a file.
        :param logs: Optional: A BlobRepository to dump pod logs to on completion. Linked as "logs" with
            ``inhibits_start, required_for_output, is_status``.
        :param done: A MetadataRepository in which to dump some information about the pod's lifetime and
            termination on completion. Linked as "done" with ``inhibits_start, required_for_output, is_status``.
        :param window:  Optional: How far back into the past to look in order to determine whether we have recently
            launched too many pods too quickly.
        :param timeout: Optional: When a pod is found to have been running continuously for this amount of time, it
            will be timed out and stopped. The method `handle_timeout` will be called in-process.
        :param template_env:     Optional: Additional keys to add to the template environment.
        :param ready:   Optional: A repository from which to read task-ready status.
        """
        super().__init__(
            name, done, ready, long_running=long_running, timeout=timeout, queries=queries, failure_ok=failure_ok
        )

        self.template = template
        self.quota_manager = quota_manager
        self._executor = executor
        self._podman: Optional["execmodule.PodManager"] = None
        self.logs = logs
        self.template_env = template_env if template_env is not None else {}
        self.warned = False
        self.window = window

        self.quota_manager.register(self._get_load)

        self.link(
            "live",
            repomodule.LiveKubeRepository(self),
            None,
            is_status=True,
            inhibits_start=True,
            inhibits_output=True,
        )
        if logs:
            self.link(
                "logs",
                logs,
                None,
                is_status=True,
                inhibits_start=True,
                required_for_output=True,
            )

    @property
    def host(self):
        return self.podman.host

    @property
    def podman(self) -> execmodule.PodManager:
        """The pod manager instance for this task.

        Will raise an error if the manager is provided by an unopened session.
        """
        if self._podman is None:
            self._podman = self._executor.to_pod_manager()
        return self._podman

    async def _get_load(self):
        usage = Quota()
        cutoff = datetime.now(tz=timezone.utc) - self.window

        try:
            for pod in await self.podman.query(task=self.name):
                if pod.metadata.creation_timestamp > cutoff:
                    usage += Quota(launches=1)
                for container in pod.spec.containers:
                    usage += Quota.parse(container.resources.requests["cpu"], container.resources.requests["memory"], 0)
        except ApiException as e:
            if e.reason != "Forbidden":
                raise

        return usage

    async def build_template_env(self, orig_job, env_src=None):
        if env_src is None:
            env_src = {}
        env_src.update(vars(self))
        env_src.update(self.template_env)
        template_env, preamble, epilogue = await super().build_template_env(orig_job, env_src)
        template_env["argv0"] = os.path.basename(sys.argv[0])
        return template_env, preamble, epilogue

    async def launch(self, job):
        template_env, preamble, epilogue = await self.build_template_env(job)
        manifest = yaml.safe_load(await render_template(self.template, template_env))
        for item in template_env.values():
            if asyncio.iscoroutine(item):
                item.close()

        assert manifest["kind"] == "Pod"
        spec = manifest["spec"]
        spec["restartPolicy"] = "Never"

        request = Quota(launches=1)
        for container in spec["containers"]:
            request += Quota.parse(
                container["resources"]["requests"]["cpu"],
                container["resources"]["requests"]["memory"],
                0,
            )
            # horrible and incomplete
            command = container.pop("command", [])
            args = container.pop("args", [])
            execute = shlex.join(command + args)
            container["command"] = [
                "sh",
                "-c",
                f"""
            set -e
            {"; ".join(preamble)}
            set +e
            (
            set -e
            {execute}
            )
            RETCODE=$?
            set -e
            {"; ".join(epilogue)}
            exit $RETCODE
            """,
            ]

        limit = await self.quota_manager.reserve(request)
        if limit is None:
            try:
                await self.podman.launch(job, self.name, manifest)
            except:
                await self.quota_manager.relinquish(request)
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
                except (TimeoutError, ApiException):
                    await fp.write("<failed to fetch logs>\n")
        data = {
            "reason": reason,
            "start_time": pod.metadata.creation_timestamp,
            "end_time": datetime.now(tz=timezone.utc),
            "image": pod.status.container_statuses[0].image,
            "node": pod.spec.node_name,
            "timeout": reason == "Timeout",
            "success": reason == "Succeeded",
        }
        await self.done.dump(job, data)

        await self.delete(pod)

    async def delete(self, pod: V1Pod):
        """Kill a pod and relinquish its resources without marking the task as complete."""
        request = Quota(launches=1)
        for container in pod.spec.containers:
            request += Quota.parse(container.resources.requests["cpu"], container.resources.requests["memory"], 0)
        await self.podman.delete(pod)
        await self.quota_manager.relinquish(request)

    async def update(self):
        self.warned = False
        result = False

        pods = await self.podman.query(task=self.name)
        result = bool(pods)
        jobs = [self._update_one(pod) for pod in pods]
        await asyncio.gather(*jobs)
        return result

    async def _update_one(self, pod: V1Pod):
        try:
            uptime: timedelta = datetime.now(tz=timezone.utc) - pod.metadata.creation_timestamp
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
            elif self.timeout and uptime > self.timeout:
                l.debug("...timed out")
                await self.handle_timeout(pod)
                await self._cleanup(pod, "Timeout")
        except Exception:
            if self.fail_fast:
                raise
            l.exception("Failed to update kube task %s:%s", self.name, pod.metadata.name)

    async def handle_timeout(self, pod: V1Pod):
        """You may override this method in a subclass, and it will be called whenever a pod times out.

        You can use this method to e.g. scrape in-progress data out of the pod via an exec.
        """


class ProcessTask(Task):
    """A task that runs a script.

    The interpreter is specified by the shebang, or the default shell if none present. The execution environment for the
    task is defined by the ProcessManager instance provided as an argument.
    """

    def __init__(
        self,
        name: str,
        template: str,
        done: "repomodule.MetadataRepository",
        executor: Optional[execmodule.Executor] = None,
        quota_manager: Optional["QuotaManager"] = None,
        job_quota: Optional["Quota"] = None,
        timeout: Optional[timedelta] = None,
        window: timedelta = timedelta(minutes=1),
        pids: Optional["repomodule.MetadataRepository"] = None,
        environ: Optional[Dict[str, str]] = None,
        long_running: bool = False,
        stdin: Optional["repomodule.BlobRepository"] = None,
        stdout: Optional["repomodule.BlobRepository"] = None,
        stderr: Optional[Union["repomodule.BlobRepository", _StderrIsStdout]] = None,
        ready: Optional["repomodule.Repository"] = None,
        queries: Optional[Dict[str, pydatatask.query.query.Query]] = None,
        failure_ok: bool = False,
    ):
        """
        :param name: The name of this task.
        :param executor: The executor to use for this task.
        :param quota_manager: A QuotaManager instance. Tasks launched will contribute to its quota and be denied
                                 if they would break the quota.
        :param job_quota: The amount of resources an individual job should contribute to the quota. Note that this
                              is currently **not enforced** target-side, so jobs may actually take up more resources
                              than assigned.
        :param timeout: The amount of time that a job may take before it is terminated, or None for no timeout.
        :param pids: A metadata repository used to store the current live-status of processes. Will automatically be
                     linked as "live" with ``is_status, inhibits_start, inhibits_output``.
        :param template: YAML markup for the template of a script to run, either as a string or a path to a file.
        :param environ: Additional environment variables to set on the target machine before running the task.
        :param window: How recently a process must have been launched in order to contribute to the process
                       rate-limiting.
        :param done: metadata repository in which to dump some information about the process's lifetime and
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
        """
        super().__init__(
            name, done, ready=ready, long_running=long_running, timeout=timeout, queries=queries, failure_ok=failure_ok
        )

        if pids is None:
            pids = repomodule.YamlMetadataFileRepository(f"/tmp/pydatatask-{getpass.getuser()}/{name}_pids")

        self.pids = pids
        self.template = template
        self.environ = environ or {}
        self.stdin = stdin
        self.stdout = stdout
        self._stderr = stderr
        self.job_quota = copy.copy(job_quota or Quota.parse(1, "256Mi", 1))
        self.job_quota.launches = 1
        self.quota_manager = quota_manager or localhost_quota_manager
        self._executor = executor or execmodule.localhost_manager
        self._manager: Optional[execmodule.AbstractProcessManager] = None
        self.warned = False
        self.window = window

        self.quota_manager.register(self._get_load)

        self.link("live", pids, None, is_status=True, inhibits_start=True, inhibits_output=True)
        if stdin is not None:
            self.link("stdin", stdin, None, is_input=True)
        if stdout is not None:
            self.link("stdout", stdout, None, is_output=True)
        if isinstance(stderr, repomodule.BlobRepository):
            self.link("stderr", stderr, None, is_output=True)

    @property
    def host(self):
        return self.manager.host

    @property
    def manager(self) -> execmodule.AbstractProcessManager:
        """The process manager for this task.

        Will raise an error if the manager comes from a session which is closed.
        """
        if self._manager is None:
            self._manager = self._executor.to_process_manager()
        return self._manager

    async def _get_load(self) -> "Quota":
        cutoff = datetime.now(tz=timezone.utc) - self.window
        count = 0
        recent = 0
        job_map = await self.pids.info_all()
        for v in job_map.values():
            count += 1
            if v["start_time"] > cutoff:
                recent += 1

        return self.job_quota * count - Quota(launches=count - recent)

    @property
    def stderr(self) -> Optional["repomodule.BlobRepository"]:
        """The repository into which stderr will be dumped, or None if it will go to the null device."""
        if isinstance(self._stderr, repomodule.BlobRepository):
            return self._stderr
        else:
            return self.stdout

    @property
    def _unique_stderr(self):
        return self._stderr is not None and not isinstance(self._stderr, _StderrIsStdout)

    @property
    def basedir(self) -> Path:
        """The path in the target environment that will be used to store information about this task."""
        return self.manager.basedir / self.name

    async def update(self):
        self.warned = False
        job_map = await self.pids.info_all()
        pid_map = {meta["pid"]: job for job, meta in job_map.items()}
        now = datetime.now(tz=timezone.utc)
        timedout_pid_set = {
            meta["pid"] for job, meta in job_map.items() if self.timeout and now - meta["start_time"] > self.timeout
        }
        expected_live = set(pid_map)
        try:
            live_pids = await self.manager.get_live_pids(expected_live)
        except Exception:
            if self.fail_fast:
                raise
            l.error("Could not load live PIDs for %s", self, exc_info=True)
        else:
            died = expected_live - live_pids
            coros = []
            for pid in died:
                job = pid_map[pid]
                start_time = job_map[job]["start_time"]
                coros.append(self._reap(job, start_time))
            for pid in timedout_pid_set - died:
                job = pid_map[pid]
                start_time = job_map[job]["start_time"]
                l.info("%s:%s timed out", self.name, job)
                coros.append(self._timeout_reap(job, pid, start_time))
            await asyncio.gather(*coros)

        return bool(expected_live)

    async def launch(self, job: str):
        limit = await self.quota_manager.reserve(self.job_quota)
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
            stdin: Optional[str] = None
            if self.stdin is not None:
                stdin = str(self.basedir / job / "stdin")
                async with await self.stdin.open(job, "rb") as fpr, await self.manager.open(Path(stdin), "wb") as fpw:
                    await async_copyfile(fpr, fpw)
                stdin = str(stdin)
            stdout = None if self.stdout is None else str(self.basedir / job / "stdout")
            stderr: Optional[Union[str, Path, _StderrIsStdout]] = STDOUT
            if not isinstance(self._stderr, _StderrIsStdout):
                stderr = None if self._stderr is None else str(self.basedir / job / "stderr")
            template_env, preamble, epilogue = await self.build_template_env(job)
            exe_path = self.basedir / job / "exe"
            exe_txt = await render_template(self.template, template_env)
            for item in template_env.values():
                if asyncio.iscoroutine(item):
                    item.close()
            async with await self.manager.open(exe_path, "w") as fp:
                await fp.write("set -e\n")
                for p in preamble:
                    await fp.write(p)
                await fp.write("set +e\n(\nset -e\n")
                await fp.write(exe_txt)
                await fp.write(")\nRETCODE=$?\nset -e\n")
                for p in epilogue:
                    await fp.write(p)
                await fp.write("exit $RETCODE")
            pid = await self.manager.spawn(
                [str(exe_path)], self.environ, str(cwd), str(self.basedir / job / "return_code"), stdin, stdout, stderr
            )
            if pid is not None:
                await self.pids.dump(job, {"pid": pid, "start_time": datetime.now(tz=timezone.utc)})
        except:  # CLEAN UP YOUR MESS
            try:
                await self.quota_manager.relinquish(self.job_quota)
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

    async def _reap(self, job: str, start_time: datetime, timeout: bool = False):
        try:
            if self.stdout is not None:
                async with await self.manager.open(self.basedir / job / "stdout", "rb") as fpr, await self.stdout.open(
                    job, "wb"
                ) as fpw:
                    await async_copyfile(fpr, fpw)
            if isinstance(self._stderr, repomodule.BlobRepository):
                async with await self.manager.open(self.basedir / job / "stderr", "rb") as fpr, await self._stderr.open(
                    job, "wb"
                ) as fpw:
                    await async_copyfile(fpr, fpw)
            async with await self.manager.open(self.basedir / job / "return_code", "r") as fp1:
                code = int(await fp1.read())
            await self.done.dump(
                job,
                {
                    "exit_code": code,
                    "start_time": start_time,
                    "end_time": datetime.now(tz=timezone.utc),
                    "timeout": timeout,
                    "success": code == 0 and not timeout,
                },
            )
            await self.manager.rmtree(self.basedir / job)
            await self.pids.delete(job)
            await self.quota_manager.relinquish(self.job_quota)
        except Exception:
            if self.fail_fast:
                raise
            l.error("Could not reap process for %s:%s", self, job, exc_info=True)

    async def _timeout_reap(self, job: str, pid: str, start_time: datetime):
        try:
            await self.manager.kill(pid)
        finally:
            await self._reap(job, start_time, True)


class FunctionTaskProtocol(Protocol):
    """The protocol which is expected by the tasks which take a python function to execute."""

    def __call__(self, **kwargs) -> Awaitable[None]:
        ...


class InProcessSyncTask(Task):
    """A task which runs in-process. Typical usage of this task might look like the following:

    .. code:: python

        @InProcessSyncTask("my_task", done_repo)
        async def my_task(inp, out):
            await out.dump(await inp.info())

        my_task.link("inp", repo_input, LinkKind.InputRepo)
        my_task.link("out", repo_output, LinkKind.OutputRepo)
    """

    def __init__(
        self,
        name: str,
        done: "repomodule.MetadataRepository",
        ready: Optional["repomodule.Repository"] = None,
        func: Optional[FunctionTaskProtocol] = None,
        failure_ok: bool = False,
    ):
        """
        :param name: The name of this task.
        :param done: A metadata repository to store some information about a job's runtime and termination on
                     completion.
        :param ready: Optional: A repository from which to read task-ready status.
        :param func: Optional: The async function to run as the task body, if you don't want to use this task as a
                     decorator.
        """
        super().__init__(name, done, ready=ready, failure_ok=failure_ok)

        self.func = func
        self._env: Dict[str, Any] = {}

    @property
    def host(self):
        return LOCAL_HOST

    def __call__(self, f: FunctionTaskProtocol) -> "InProcessSyncTask":
        self.func = f
        return self

    async def validate(self):
        if self.func is None:
            raise ValueError("InProcessSyncTask.func is None")

        # sig = inspect.signature(self.func, follow_wrapped=True)
        # for name in sig.parameters.keys():
        #    if name == "job":
        #        self._env[name] = None
        #    elif name in self.links:
        #        self._env[name] = self.links[name].repo
        #    else:
        #        raise NameError("%s takes parameter %s but no such argument is available" % (self.func, name))

    async def update(self):
        pass

    async def launch(self, job: str):
        assert self.func is not None
        start_time = datetime.now(tz=timezone.utc)
        l.info("Launching in-process %s:%s...", self.name, job)
        args, preamble, epilogue = await self.build_template_env(job)
        try:
            for p in preamble:
                p(**args)
            await self.func(**args)
            for e in epilogue:
                e(**args)
        except Exception as e:
            l.info("In-process task %s:%s failed", self.name, job, exc_info=True)
            result: Dict[str, Any] = {
                "success": False,
                "exception": repr(e),
                "traceback": traceback.format_tb(e.__traceback__),
            }
        else:
            l.debug("...success")
            result = {
                "success": True,
                "exception": "",
                "traceback": "",
            }
        result["start_time"] = start_time
        result["end_time"] = datetime.now(tz=timezone.utc)
        result["timeout"] = False
        if self.metadata:
            await self.done.dump(job, result)


class ExecutorTask(Task):
    """A task which runs python functions in a :external:class:`concurrent.futures.Executor`. This has not been
    tested on anything but the :external:class:`concurrent.futures.ThreadPoolExecutor`, so beware!

    See `InProcessSyncTask` for information on how to use instances of this class as decorators for their bodies.

    It is expected that the executor will perform all necessary resource quota management.
    """

    def __init__(
        self,
        name: str,
        executor: FuturesExecutor,
        done: "repomodule.MetadataRepository",
        host: Optional[Host] = None,
        long_running: bool = False,
        ready: Optional["repomodule.Repository"] = None,
        failure_ok: bool = False,
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
        super().__init__(name, done, ready, long_running=long_running, failure_ok=failure_ok)

        if host is None:
            if isinstance(executor, ThreadPoolExecutor):
                host = LOCAL_HOST
            else:
                raise ValueError(
                    "Can't figure out what host this task runs on automatically - "
                    "please provide ExecutorTask with the host parameter"
                )

        self.executor = executor
        self._host = host
        self.func = func
        self.jobs: Dict[ConcurrentFuture[datetime], Tuple[str, datetime]] = {}
        self.rev_jobs: Dict[str, ConcurrentFuture[datetime]] = {}
        self.live = repomodule.ExecutorLiveRepo(self)
        self._env: Dict[str, Any] = {}
        self.link("live", self.live, None, is_status=True, inhibits_output=True, inhibits_start=True)

    def __call__(self, f: Callable) -> "ExecutorTask":
        self.func = f
        return self

    @property
    def host(self):
        return self._host

    async def update(self):
        result = bool(self.jobs)
        done, _ = wait(self.jobs, 0, FIRST_EXCEPTION)
        coros = []
        for finished_job in done:
            job, start_time = self.jobs.pop(finished_job)
            # noinspection PyAsyncCall
            self.rev_jobs.pop(job)
            coros.append(self._cleanup(finished_job, job, start_time))
        await asyncio.gather(*coros)
        return result

    async def _cleanup(self, job_future, job, start_time):
        e = job_future.exception()
        if e is not None:
            l.info("Executor task %s:%s failed", self.name, job, exc_info=e)
            data = {
                "success": False,
                "exception": repr(e),
                "traceback": traceback.format_tb(e.__traceback__),
                "end_time": datetime.now(tz=timezone.utc),
            }
        else:
            l.debug("...executor task %s:%s success", self.name, job)
            data = {
                "success": True,
                "end_time": job_future.result(),
                "exception": "",
                "traceback": "",
            }
        data["start_time"] = start_time
        data["timeout"] = False
        await self.done.dump(job, data)

    async def validate(self):
        if self.func is None:
            raise ValueError("InProcessAsyncTask %s has func None" % self.name)

        sig = inspect.signature(self.func, follow_wrapped=True)
        for name, param in sig.parameters.items():
            if param.kind == param.VAR_KEYWORD:
                pass
            elif name in self.links:
                self._env[name] = self.links[name].repo
            else:
                raise NameError("%s takes parameter %s but no such argument is available" % (self.func, name))

    async def launch(self, job):
        l.info("Launching %s:%s with %s...", self.name, job, self.executor)
        args, preamble, epilogue = await self.build_template_env(job)
        start_time = datetime.now(tz=timezone.utc)
        running_job = self.executor.submit(self._timestamped_func, self.func, preamble, args, epilogue)
        if self.synchronous:
            while not running_job.done():
                await asyncio.sleep(0.1)
            await self._cleanup(running_job, job, start_time)
        else:
            self.jobs[running_job] = (job, start_time)
            self.rev_jobs[job] = running_job

    async def cancel(self, job):
        """Stop the current job from running, or do nothing if it is not running."""
        future = self.rev_jobs.pop(job)
        if future is not None:
            future.cancel()
            self.jobs.pop(future)

    @staticmethod
    def _timestamped_func(func, preamble, args, epilogue) -> datetime:
        loop = asyncio.new_event_loop()
        for p in preamble:
            loop.run_until_complete(p(**args))
        loop.run_until_complete(func(**args))
        for p in epilogue:
            loop.run_until_complete(p(**args))
        return datetime.now(tz=timezone.utc)


class KubeFunctionTask(KubeTask):
    """A task which runs a python function on a kubernetes cluster. Requires a pod template which will execute a.

    python script calling `pydatatask.main.main`. This works by running ``python3 main.py launch [task] [job]
    --sync``.

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
            repo_done,
            repo_func_done,
            repo_logs,
        )
        async def my_task(inp, out):
            await out.dump(await inp.info())

        my_task.link("inp", repo_input, LinkKind.InputRepo)
        my_task.link("out", repo_output, LinkKind.OutputRepo)
    """

    def __init__(
        self,
        name: str,
        executor: "execmodule.Executor",
        quota_manager: QuotaManager,
        template: Union[str, Path],
        kube_done: "repomodule.MetadataRepository",
        func_done: "repomodule.MetadataRepository",
        logs: Optional["repomodule.BlobRepository"] = None,
        template_env: Optional[Dict[str, Any]] = None,
        failure_ok: bool = False,
        func: Optional[Callable] = None,
    ):
        """
        :param name: The name of this task.
        :param executor: The executor to use for this task.
        :param quota_manager: A QuotaManager instance. Tasks launched will contribute to its quota and be denied if they
                          would break the quota.
        :param template: YAML markup for a pod manifest template that will run `pydatatask.main.main` as
                         ``python3 main.py launch [task] [job] --sync --force``, either as a string or a path to a file.
        :param logs: Optional: A BlobRepository to dump pod logs to on completion. Linked as "logs" with
                               ``inhibits_start, required_for_output, is_status``.
        :param kube_done: A MetadataRepository in which to dump some information about the pod's lifetime and
                          termination on completion. Linked as "done" with
                          ``inhibits_start, required_for_output, is_status``.
        :param func_done: Optional: A MetadataRepository in which to dump some information about the function's lifetime
                          and termination on completion. Linked as "func_done" with
                          ``inhibits_start, required_for_output, is_status``.
        :param template_env:     Optional: Additional keys to add to the template environment.
        :param ready:   Optional: A repository from which to read task-ready status.
        :param func: Optional: The async function to run as the task body, if you don't want to use this task as a
                     decorator.
        """
        super().__init__(
            name, executor, quota_manager, template, logs, kube_done, template_env=template_env, failure_ok=failure_ok
        )
        self.func = func
        self.func_done = func_done
        if func_done is not None:
            self.link(
                "func_done",
                func_done,
                None,
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
            if name in self.links:
                self._func_env[name] = self.links[name].repo
            else:
                raise NameError("%s takes parameter %s but no such argument is available" % (self.func, name))

    async def launch(self, job: str):
        if self.synchronous:
            await self._launch_sync(job)
        else:
            await super().launch(job)

    async def _launch_sync(self, job: str):
        assert self.func is not None
        start_time = datetime.now(tz=timezone.utc)
        l.info("Launching --sync %s:%s...", self.name, job)
        args, preamble, epilogue = await self.build_template_env(job)
        try:
            for p in preamble:
                await p(**args)
            await self.func(**args)
            for p in epilogue:
                await p(**args)
        except Exception as e:
            l.info("--sync task %s:%s failed", self.name, job, exc_info=True)
            result: Dict[str, Any] = {
                "success": False,
                "exception": repr(e),
                "traceback": traceback.format_tb(e.__traceback__),
            }
        else:
            l.debug("...success")
            result = {
                "success": True,
                "exception": "",
                "traceback": "",
            }
        result["start_time"] = start_time
        result["end_time"] = datetime.now(tz=timezone.utc)
        result["timeout"] = False
        if self.metadata and self.func_done is not None:
            await self.func_done.dump(job, result)


class ContainerTask(Task):
    """A task that runs a container."""

    def __init__(
        self,
        name: str,
        image: str,
        template: str,
        done: "repomodule.MetadataRepository",
        entrypoint: Iterable[str] = ("/bin/sh", "-c"),
        executor: Optional[execmodule.Executor] = None,
        quota_manager: Optional["QuotaManager"] = None,
        job_quota: Optional["Quota"] = None,
        window: timedelta = timedelta(minutes=1),
        timeout: Optional[timedelta] = None,
        environ: Optional[Dict[str, str]] = None,
        long_running: bool = False,
        logs: Optional["repomodule.BlobRepository"] = None,
        ready: Optional["repomodule.Repository"] = None,
        privileged: Optional[bool] = False,
        tty: Optional[bool] = False,
        queries: Optional[Dict[str, pydatatask.query.query.Query]] = None,
        failure_ok: bool = False,
    ):
        """
        :param name: The name of this task.
        :param image: The name of the docker image to use to run this task.
        :param executor: The executor to use for this task.
        :param quota_manager: A QuotaManager instance. Tasks launched will contribute to its quota and be denied
                                 if they would break the quota.
        :param job_quota: The amount of resources an individual job should contribute to the quota. Note that this
                              is currently **not enforced** target-side, so jobs may actually take up more resources
                              than assigned.
        :param template: YAML markup for the template of a script to run, either as a string or a path to a file.
        :param environ: Additional environment variables to set on the target container before running the task.
        :param window: How recently a container must have been launched in order to contribute to the container
                       rate-limiting.
        :param timeout: How long a task can take before it is killed for taking too long, or None for no timeout.
        :param done: Optional: A metadata repository in which to dump some information about the container's lifetime
                               and termination on completion. Linked as "done" with
                               ``inhibits_start, required_for_output, is_status``.
        :param logs: Optional: A blob repository into which to dump the container's logs. Linked as "logs" with
                               ``is_output``.
        :param ready:  Optional: A repository from which to read task-ready status.
        """
        super().__init__(
            name, done, ready=ready, long_running=long_running, timeout=timeout, queries=queries, failure_ok=failure_ok
        )

        self.template = template
        self.entrypoint = entrypoint
        self.image = image
        self.environ = environ or {}
        self.logs = logs
        self.job_quota = copy.copy(job_quota or Quota.parse(1, "256Mi", 1))
        self.job_quota.launches = 1
        self.quota_manager = quota_manager or localhost_quota_manager
        self._executor = executor or execmodule.localhost_docker_manager
        self._manager: Optional[execmodule.AbstractContainerManager] = None
        self.warned = False
        self.window = window
        self.privileged = privileged
        self.tty = tty
        self.mount_directives: DefaultDict[str, List[Tuple[str, str]]] = defaultdict(list)

        self.quota_manager.register(self._get_load)

        if logs is not None:
            self.link("logs", logs, None, is_output=True)
        self.link(
            "live",
            repomodule.LiveContainerRepository(self),
            None,
            is_status=True,
            inhibits_start=True,
            inhibits_output=True,
        )

    @property
    def host(self):
        return self.manager.host

    @property
    def manager(self) -> execmodule.AbstractContainerManager:
        """The process manager for this task.

        Will raise an error if the manager comes from a session which is closed.
        """
        if self._manager is None:
            self._manager = self._executor.to_container_manager()
        return self._manager

    async def _get_load(self) -> "Quota":
        cutoff = datetime.now(tz=timezone.utc) - self.window
        containers = await self.manager.live(self.name)
        count = len(containers)
        recent = sum(c > cutoff for c in containers.values())
        return self.job_quota * count - Quota(launches=count - recent)

    async def update(self) -> bool:
        has_any, reaped = await self.manager.update(self.name, timeout=self.timeout)
        for job, (log, done) in reaped.items():
            if self.logs is not None:
                async with await self.logs.open(job, "wb") as fp:
                    await fp.write(log)
            await self.done.dump(job, done)
        return has_any

    async def launch(self, job: str):
        limit = await self.quota_manager.reserve(self.job_quota)
        if limit is not None:
            if not self.warned:
                l.warning("Cannot launch %s: %s limit", self, limit)
                self.warned = True
            return

        template_env, preamble, epilogue = await self.build_template_env(job)
        exe_txt = await render_template(self.template, template_env)
        exe_txt = "\n".join(
            [
                "set -e",
                *preamble,
                "set +e",
                "(",
                "set -e",
                exe_txt,
                ")",
                "RETCODE=$?",
                "set -e",
                *epilogue,
                "exit $RETCODE",
            ]
        )
        for item in template_env.values():
            if asyncio.iscoroutine(item):
                item.close()

        mounts = self.mount_directives.pop(job, [])

        privileged = bool(self.privileged)
        tty = bool(self.tty)
        await self.manager.launch(
            self.name,
            job,
            self.image,
            list(self.entrypoint),
            exe_txt,
            self.environ,
            self.job_quota,
            mounts,
            privileged,
            tty,
        )
