"""The top-level script you write using pydatatask should call `pydatatask.main.main` in its ``if __name__ ==
'__main__'`` block. This will parse ``sys.argv`` and display the administration interface for the pipeline.

The help screen should look something like this:

.. code::

    $ python3 main.py --help
      usage: main.py [-h] {update,run,status,trace,rm,ls,cat,inject,launch,shell} ...

    positional arguments:
      {update,run,status,trace,rm,ls,cat,inject,launch,shell}
        update              Keep the pipeline in motion
        run                 Run update in a loop until everything is quiet
        status              View the pipeline status
        trace               Track a job's progress through the pipeline
        rm                  Delete data from the pipeline
        ls                  List jobs in a repository
        cat                 Print data from a repository
        inject              Dump data into a repository
        launch              Manually start a task
        shell               Launch an interactive shell to interrogate the pipeline

    options:
      -h, --help            show this help message and exit
"""

from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Optional, Set, Union
from pathlib import Path
import argparse
import asyncio
import logging
import os
import re

from aiohttp import web
from networkx.drawing.nx_pydot import write_dot
import aiofiles
import IPython

from pydatatask.agent import build_agent_app
from pydatatask.agent import cat_data as cat_data_inner
from pydatatask.agent import inject_data as inject_data_inner
from pydatatask.utils import async_copyfile

from . import repository as repomodule
from . import task as taskmodule
from .pipeline import Pipeline
from .quota import localhost_quota_manager
from .visualize import run_viz


try:
    from . import fuse
except ModuleNotFoundError:
    fuse = None  # type: ignore[assignment]

log = logging.getLogger(__name__)
token_re = re.compile(r"\w+\.\w+")

__all__ = (
    "main",
    "update",
    "cat_data",
    "list_data",
    "delete_data",
    "inject_data",
    "print_status",
    "print_trace",
    "launch",
    "shell",
    "run",
)

# pylint: disable=missing-function-docstring,missing-class-docstring


def main(
    pipeline: Pipeline,
    instrument: Optional[Callable[[argparse._SubParsersAction], None]] = None,
    has_local_fs: bool = True,
):
    """The pydatatask main function! Call this with the pipeline you've constructed to parse ``sys.argv`` and
    display the pipeline administration interface.

    If you like, you can pass as the ``instrument`` argument a function which will add additional commands to the menu.
    """
    logging.basicConfig()
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest=argparse.SUPPRESS, required=True)

    parser_update = subparsers.add_parser("update", help="Keep the pipeline in motion")
    parser_update.set_defaults(func=update)

    parser_run = subparsers.add_parser("run", help="Run update in a loop until everything is quiet")
    parser_run.add_argument("--forever", action="store_true", help="Run forever")
    parser_run.add_argument("--launch-once", action="store_true", help="Only evaluates tasks-to-launch once")
    parser_run.add_argument("--verbose", action="store_true", help="Only evaluates tasks-to-launch once")
    parser_run.add_argument(
        "--fail-fast", action="store_true", help="Do not catch exceptions thrown during routine operations"
    )
    parser_run.add_argument("--task", "-t", dest="tasks", action="append", default=[], help="Only manage these tasks")
    parser_run.set_defaults(func=run)
    parser_run.set_defaults(timeout=None)

    parser_status = subparsers.add_parser("status", help="View the pipeline status")
    parser_status.add_argument(
        "--all",
        "-a",
        dest="all_repos",
        action="store_true",
        help="Show internal repositories",
    )
    parser_status.set_defaults(func=print_status)

    parser_trace = subparsers.add_parser("trace", help="Track a job's progress through the pipeline")
    parser_trace.add_argument(
        "--all",
        "-a",
        dest="all_repos",
        action="store_true",
        help="Show internal repositories",
    )
    parser_trace.add_argument("job", nargs="+", help="Name of job to trace")
    parser_trace.set_defaults(func=print_trace)

    parser_delete = subparsers.add_parser("rm", help="Delete data from the pipeline")
    parser_delete.add_argument("--recursive", "-r", action="store_true", help="Delete dependant data too")
    parser_delete.add_argument(
        "data",
        type=str,
        help="Name of repository [task.repo] or task from which to delete data",
    )
    parser_delete.add_argument("job", type=str, nargs="+", help="Name of job of which to delete data")
    parser_delete.set_defaults(func=delete_data)

    parser_ls = subparsers.add_parser("ls", help="List jobs in a repository")
    parser_ls.add_argument(
        "data",
        type=str,
        nargs="+",
        help="Name of repository [task.repo] from which to list data",
    )
    parser_ls.set_defaults(func=list_data)

    parser_cat = subparsers.add_parser("cat", help="Print data from a repository")
    parser_cat.add_argument("data", type=str, help="Name of repository [task.repo] from which to print data")
    parser_cat.add_argument("job", type=str, help="Name of job of which to delete data")
    parser_cat.set_defaults(func=cat_data)

    parser_inject = subparsers.add_parser("inject", help="Dump data into a repository")
    parser_inject.add_argument("data", type=str, help="Name of repository [task.repo] to which to inject data")
    parser_inject.add_argument("job", type=str, help="Name of job of which to inject data")
    parser_inject.set_defaults(func=inject_data)

    parser_launch = subparsers.add_parser("launch", help="Manually start a task")
    parser_launch.add_argument(dest="task_name", type=str, help="Name of task to launch")
    parser_launch.add_argument("job", type=str, help="Name of job to launch task on")
    parser_launch.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Launch even if start is inhibited by data",
    )
    parser_launch.add_argument("--sync", action="store_true", help="Run the task in-process, if possible")
    parser_launch.add_argument(
        "--meta",
        action="store_true",
        default=False,
        help="Store metadata related to task completion",
    )
    parser_launch.add_argument(
        "--no-meta",
        action="store_false",
        help="Do not store metadata related to task completion",
        dest="meta",
    )
    parser_launch.add_argument(
        "--fail-fast", action="store_true", help="Do not catch exceptions thrown during routine operations"
    )
    parser_launch.set_defaults(func=launch)

    parser_shell = subparsers.add_parser("shell", help="Launch an interactive shell to interrogate the pipeline")
    parser_shell.set_defaults(func=shell)

    if has_local_fs:
        parser_graph = subparsers.add_parser("graph", help="Generate a the pipeline graph visualizations")
        parser_graph.add_argument(
            "--out-dir", "-o", help="The directory to write the graphs to", type=Path, default=None
        )
        parser_graph.set_defaults(func=graph)

        parser_backup = subparsers.add_parser("backup", help="Copy contents of repositories to a given folder")
        parser_backup.add_argument("--all", dest="all_repos", action="store_true", help="Backup all repositories")
        parser_backup.add_argument("backup_dir", help="The directory to backup to")
        parser_backup.add_argument("repos", nargs="*", help="The repositories to back up")
        parser_backup.set_defaults(func=action_backup)

        parser_restore = subparsers.add_parser("restore", help="Copy contents of repositories from a given folder")
        parser_restore.add_argument("--all", dest="all_repos", action="store_true", help="Restore all repositories")
        parser_restore.add_argument("backup_dir", help="The directory to restore from")
        parser_restore.add_argument("repos", nargs="*", help="The repositories to restore")
        parser_restore.set_defaults(func=action_restore)

    parser_http_agent = subparsers.add_parser(
        "agent-http", help="Launch an http server to accept reads and writes from repositories"
    )
    parser_http_agent.set_defaults(func=http_agent)
    parser_http_agent.add_argument("--host", help="The host to listen on", default="0.0.0.0")

    parser_viz = subparsers.add_parser("viz", help="Show Visualization of Running Pipeline")
    parser_viz.set_defaults(func=run_viz)

    if fuse is not None:
        parser_fuse = subparsers.add_parser("fuse", help="Mount a fuse filesystem to explore the pipeline's repos")
        parser_fuse.set_defaults(func=fuse.main)
        parser_fuse.add_argument("path", help="The mountpoint")
        parser_fuse.add_argument("--verbose", "-v", dest="debug", action="store_true", help="Show FUSE debug logging")

    if instrument is not None:
        instrument(subparsers)

    args = parser.parse_args()
    ns = vars(args)
    func = ns.pop("func")
    result_or_coro = func(pipeline, **ns)
    if asyncio.iscoroutine(result_or_coro):
        return asyncio.run(main_inner(pipeline, result_or_coro))
    else:
        return result_or_coro


async def main_inner(pipeline, coro):
    async with pipeline:
        await coro


def shell(pipeline: Pipeline):
    pydatatask = __import__("pydatatask")
    assert pipeline
    assert pydatatask
    IPython.embed(using="asyncio")


async def graph(pipeline: Pipeline, out_dir: Optional[Path]):
    if out_dir is None:
        out_dir = Path.cwd() / "latest_graphs"

    os.makedirs(out_dir, exist_ok=True)

    assert os.path.isdir(out_dir)
    with open(out_dir / "task_graph.md", "w", encoding="utf-8") as f:
        f.write("# Task Graph\n\n")
        f.write("```mermaid\n")
        f.write(await pipeline.mermaid_task_graph)
        f.write("\n```\n\n")

    with open(out_dir / "graph.md", "w", encoding="utf-8") as f:
        f.write("# Data Graph\n\n")
        f.write("```mermaid\n")
        f.write(await pipeline.mermaid_graph)
        f.write("\n```\n\n")

    with open(out_dir / "task_graph.dot", "w", encoding="utf-8") as f:
        write_dot(pipeline.task_graph, f)

    with open(out_dir / "graph.dot", "w", encoding="utf-8") as f:
        write_dot(pipeline.graph, f)


async def update(pipeline: Pipeline):
    await pipeline.update()


def http_agent(pipeline: Pipeline, host: str) -> None:
    app = build_agent_app(pipeline, True)
    web.run_app(app, host=host, port=pipeline.agent_port)


async def run(
    pipeline: Pipeline,
    forever: bool,
    launch_once: bool,
    timeout: Optional[float],
    verbose: bool = False,
    fail_fast: bool = False,
    tasks: Optional[List[str]] = None,
):
    pipeline.settings(fail_fast=fail_fast, task_allowlist=tasks)

    async def update_only_update_flush():
        await pipeline.update_only_update()
        for res in pipeline.quota_managers:
            await res.flush()
        await localhost_quota_manager.flush()

    if verbose:
        logging.getLogger("pydatatask").setLevel("DEBUG")
    func = pipeline.update
    start = asyncio.get_running_loop().time()
    while await func() or forever:
        if launch_once:
            func = update_only_update_flush
        await asyncio.sleep(1)
        if timeout is not None and asyncio.get_running_loop().time() - start > timeout:
            raise TimeoutError("Pipeline run timeout")


def get_links(pipeline: Pipeline, all_repos: bool) -> Iterable[taskmodule.Link]:
    seen = set()
    for task in pipeline.tasks.values():
        for link in task.links.values():
            if not all_repos and not link.is_status and not link.is_input and not link.is_output:
                continue
            if id(link) in seen:
                continue
            seen.add(id(link))
            yield link


async def print_status(pipeline: Pipeline, all_repos: bool):
    async def inner(repo: repomodule.Repository):
        the_sum = 0
        async for _ in repo:
            the_sum += 1
        return the_sum

    link_list = list(get_links(pipeline, all_repos))
    repo_list = list(set(link.repo for link in link_list))
    repo_sizes = dict(zip(repo_list, await asyncio.gather(*(inner(repo) for repo in repo_list))))

    for task in pipeline.tasks.values():
        print(task.name)
        for link_name, link in sorted(
            task.links.items(),
            key=lambda x: 0 if x[1].is_input else 1 if x[1].is_status else 2,
        ):
            if link in link_list:
                print(f"{task.name}.{link_name} {repo_sizes[link.repo]}")
        print()


async def print_trace(pipeline: Pipeline, all_repos: bool, job: List[str]):
    async def inner(repo: repomodule.Repository):
        return [j for j in job if await repo.contains(j)]

    link_list = list(get_links(pipeline, all_repos))
    repo_list = list(set(link.repo for link in link_list))
    repo_members = dict(zip(repo_list, await asyncio.gather(*(inner(repo) for repo in repo_list))))

    for task in pipeline.tasks.values():
        print(task.name)
        for link_name, link in sorted(
            task.links.items(),
            key=lambda x: 0 if x[1].is_input else 1 if x[1].is_status else 2,
        ):
            if link in link_list:
                print(f"{task.name}.{link_name} {' '.join(repo_members[link.repo])}")
        print()


async def delete_data(pipeline: Pipeline, data: str, recursive: bool, job: List[str]):
    item: Union[taskmodule.Task, repomodule.Repository]
    if "." in data:
        taskname, reponame = data.split(".")
        item = pipeline.tasks[taskname].links[reponame].repo
    else:
        item = pipeline.tasks[data]

    for dependant in pipeline.dependants(item, recursive):
        if isinstance(dependant, repomodule.Repository):
            if job[0] == "__all__":
                jobs = [x async for x in dependant]
                check = False
            else:
                jobs = job
                check = True

            async def del_job(j, dep):
                await dep.delete(j)
                print(j, dep)

            await asyncio.gather(*[del_job(j, dependant) for j in jobs if not check or await dependant.contains(j)])


async def list_data(pipeline: Pipeline, data: List[str]):
    input_text = " ".join(data)
    tokens = token_re.findall(input_text)
    namespace = {name.replace("-", "_"): TaskNamespace(task) for name, task in pipeline.tasks.items()}
    for token in tokens:
        task, repo = token.split(".")
        await namespace[task].consume(repo)
    result = eval(input_text, {}, namespace)  # pylint: disable=eval-used

    for job in sorted(result):
        print(job)


class TaskNamespace:
    def __init__(self, task: taskmodule.Task):
        self.task = task
        self.repos: Dict[str, Set[str]] = {}

    def __getattr__(self, item):
        return self.repos[item]

    async def consume(self, repo: str):
        result = set()
        async for x in self.task.links[repo].repo:
            result.add(x)
        self.repos[repo] = result


async def cat_data(pipeline: Pipeline, data: str, job: str):
    taskname, reponame = data.split(".")
    item = pipeline.tasks[taskname].links[reponame].repo

    try:
        await cat_data_inner(item, job, aiofiles.stdout_bytes)
    except TypeError:
        print("Error: cannot serialize a job in a repository which is not a blob or metadata or filesystem")
        return 1


async def inject_data(pipeline: Pipeline, data: str, job: str):
    taskname, reponame = data.split(".")
    item = pipeline.tasks[taskname].links[reponame].repo

    try:
        await inject_data_inner(item, job, aiofiles.stdin_bytes)
    except TypeError:
        print("Error: cannot deserialize a job in a repository which is not a blob or metadata")
        return 1
    except ValueError as e:
        print(f"Bad data structure: {e.args[0]}")
        return 1


async def launch(pipeline: Pipeline, task_name: str, job: str, sync: bool, meta: bool, force: bool, fail_fast: bool):
    task = pipeline.tasks[task_name]
    pipeline.settings(sync, meta, fail_fast)

    if force or await task.ready.contains(job):
        await task.launch(job)
    else:
        log.warning("Task is not ready to launch - use -f to force")
        return 1


async def action_backup(pipeline: Pipeline, backup_dir: str, repos: List[str], all_repos: bool = False):
    backup_base = Path(backup_dir)
    if all_repos:
        if repos:
            raise ValueError("Do you want specific repos or all repos? Make up your mind!")
        repos = list(pipeline.tasks)
    new_repos = []
    for repo_name in repos:
        if "." in repo_name:
            new_repos.append(repo_name)
        else:
            task = pipeline.tasks[repo_name]
            new_repos.extend(
                {
                    link.repo: f"{repo_name}.{linkname}" for linkname, link in task.links.items() if not link.is_status
                }.values()
            )
    repos = new_repos

    jobs = []
    for repo_name in repos:
        repo_base = backup_base / repo_name
        task_name, repo_basename = repo_name.split(".")
        repo = pipeline.tasks[task_name].links[repo_basename].repo
        if isinstance(repo, repomodule.BlobRepository):
            new_repo_file = repomodule.FileRepository(
                repo_base, extension=getattr(repo, "extension", getattr(repo, "suffix", ""))
            )
            await new_repo_file.validate()
            jobs.append(_repo_copy_blob(repo, new_repo_file))
        elif isinstance(repo, repomodule.MetadataRepository):
            new_repo_meta = repomodule.YamlMetadataFileRepository(repo_base, extension=".yaml")
            await new_repo_meta.validate()
            jobs.append(_repo_copy_meta(repo, new_repo_meta))
        elif isinstance(repo, repomodule.FilesystemRepository):
            new_repo_fs = repomodule.DirectoryRepository(
                repo_base, extension=getattr(repo, "extension", getattr(repo, "suffix", ""))
            )
            await new_repo_fs.validate()
            jobs.append(_repo_copy_fs(repo, new_repo_fs))
        else:
            print("Warning: cannot backup", repo)

    await asyncio.gather(*jobs)


async def action_restore(pipeline: Pipeline, backup_dir: str, repos: List[str], all_repos: bool = False):
    backup_base = Path(backup_dir)
    if all_repos:
        if repos:
            raise ValueError("Do you want specific repos or all repos? Make up your mind!")
        repos = [x.name for x in backup_base.iterdir()]

    jobs = []
    for repo_name in repos:
        repo_base = backup_base / repo_name
        task_name, repo_basename = repo_name.split(".")
        repo = pipeline.tasks[task_name].links[repo_basename].repo
        if isinstance(repo, repomodule.BlobRepository):
            new_repo_file = repomodule.FileRepository(
                repo_base, extension=getattr(repo, "extension", getattr(repo, "suffix", ""))
            )
            await new_repo_file.validate()
            jobs.append(_repo_copy_blob(new_repo_file, repo))
        elif isinstance(repo, repomodule.MetadataRepository):
            new_repo_meta = repomodule.YamlMetadataFileRepository(repo_base, extension=".yaml")
            await new_repo_meta.validate()
            jobs.append(_repo_copy_meta(new_repo_meta, repo))
        elif isinstance(repo, repomodule.FilesystemRepository):
            new_repo_fs = repomodule.DirectoryRepository(
                repo_base, extension=getattr(repo, "extension", getattr(repo, "suffix", ""))
            )
            await new_repo_fs.validate()
            jobs.append(_repo_copy_fs(new_repo_fs, repo))
        else:
            print("Warning: cannot backup", repo)

    await asyncio.gather(*jobs)


async def _repo_copy_blob(repo_src: repomodule.BlobRepository, repo_dst: repomodule.BlobRepository):
    async for ident in repo_src:
        async with await repo_src.open(ident, "rb") as fp_r, await repo_dst.open(ident, "wb") as fp_w:
            await async_copyfile(fp_r, fp_w)


async def _repo_copy_meta(repo_src: repomodule.MetadataRepository, repo_dst: repomodule.MetadataRepository):
    for ident, data in (await repo_src.info_all()).items():
        await repo_dst.dump(ident, data)


async def _repo_copy_fs(repo_src: repomodule.FilesystemRepository, repo_dst: repomodule.FilesystemRepository):
    async for ident in repo_src:
        cursor = repo_dst.dump(ident)
        await cursor.__anext__()
        async for member in repo_src.iter_members(ident):
            await cursor.asend(member)
        await cursor.aclose()
