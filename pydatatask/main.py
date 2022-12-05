from typing import Union, Callable, Optional, List
import asyncio
import argparse
import sys
import re

import aiofiles.os
import yaml
import logging
import IPython

from . import BlobRepository, MetadataRepository
from .pipeline import Pipeline
from .repository import Repository
from .task import Task, settings
import pydatatask

l = logging.getLogger(__name__)
token_re = re.compile(r'\w+\.\w+')

__all__ = ('main', 'update', 'cat_data', 'list_data', 'delete_data', 'inject_data', 'print_status', 'launch', 'shell', 'run')

def main(pipeline: Pipeline, instrument: Optional[Callable[[argparse._SubParsersAction], None]]=None):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest=argparse.SUPPRESS, required=True)

    parser_update = subparsers.add_parser("update", help="Keep the pipeline in motion")
    parser_update.set_defaults(func=update)

    parser_run = subparsers.add_parser("run", help="Run update in a loop until everything is quiet")
    parser_run.add_argument("--forever", action="store_true", help="Run forever")
    parser_run.add_argument("--launch-once", action="store_true", help="Only evaluates tasks-to-launch once")
    parser_run.set_defaults(func=run)

    parser_status = subparsers.add_parser("status", help="View the pipeline status")
    parser_status.add_argument("--all", "-a", dest='all_repos', action="store_true", help="Show internal repositories")
    parser_status.set_defaults(func=print_status)

    parser_delete = subparsers.add_parser("rm", help="Delete data from the pipeline")
    parser_delete.add_argument("--recursive", "-r", action="store_true", help="Delete dependant data too")
    parser_delete.add_argument("data", type=str, help="Name of repository [task.repo] or task from which to delete data")
    parser_delete.add_argument("job", type=str, nargs='+', help="Name of job of which to delete data")
    parser_delete.set_defaults(func=delete_data)

    parser_ls = subparsers.add_parser("ls", help="List jobs in a repository")
    parser_ls.add_argument("data", type=str, nargs='+', help="Name of repository [task.repo] from which to list data")
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
    parser_launch.add_argument("task", type=str, help="Name of task to launch")
    parser_launch.add_argument("job", type=str, help="Name of job to launch task on")
    parser_launch.add_argument("--force", "-f", action="store_true", help="Launch even if start is inhibited by data")
    parser_launch.add_argument("--sync", action="store_true", help="Run the task in-process, if possible")
    parser_launch.add_argument("--meta", action=argparse.BooleanOptionalAction, default=False, help="Store metadata related to task completion")
    parser_launch.set_defaults(func=launch)

    parser_shell = subparsers.add_parser("shell", help="Launch an interactive shell to interrogate the pipeline")
    parser_shell.set_defaults(func=shell)

    if instrument is not None:
        instrument(subparsers)

    args = parser.parse_args()
    ns = vars(args)
    func = ns.pop('func')
    result_or_coro = func(pipeline, **ns)
    if asyncio.iscoroutine(result_or_coro):
        return asyncio.run(main_inner(pipeline, result_or_coro))
    else:
        return result_or_coro

async def main_inner(pipeline, coro):
    async with pipeline:
        await coro

def shell(pipeline: Pipeline):
    assert pipeline
    assert pydatatask
    IPython.embed()

async def update(pipeline: Pipeline):
    await pipeline.update()

async def run(pipeline: Pipeline, forever: bool, launch_once: bool, timeout: Optional[float]):
    func = pipeline.update
    start = asyncio.get_running_loop().time()
    while await func() or forever:
        if launch_once:
            func = pipeline.update_only_update
        await asyncio.sleep(1)
        if timeout is not None and asyncio.get_running_loop().time() - start > timeout:
            raise TimeoutError("Pipeline run timeout")

async def print_status(pipeline: Pipeline, all_repos: bool):
    for task in pipeline.tasks.values():  # TODO parallelize - waiting on repos to be a top-level abstraction
        print(task.name)
        for link_name, link in sorted(task.links.items(), key=lambda x: 0 if x[1].is_input else 1 if x[1].is_status else 2):
            if not all_repos and not link.is_status and not link.is_input and not link.is_output:
                continue
            the_sum = 0
            async for _ in link.repo:
                the_sum += 1
            print(f'{task.name}.{link_name} {the_sum}')
        print()

async def delete_data(pipeline: Pipeline, data: str, recursive: bool, job: List[str]):
    item: Union[Task | Repository]
    if '.' in data:
        taskname, reponame = data.split('.')
        item = pipeline.tasks[taskname].links[reponame].repo
    else:
        item = pipeline.tasks[data]

    for dependant in pipeline.dependants(item, recursive):
        if isinstance(dependant, Repository):
            if job[0] == '__all__':
                jobs = list(x async for x in dependant)
                check = False
            else:
                jobs = job
                check = True
            async def del_job(j):
                await dependant.delete(j)
                print(j, dependant)
            await asyncio.gather(*(del_job(j) for j in jobs if not check or await dependant.contains(j)))

async def list_data(pipeline: Pipeline, data: List[str]):
    input_text = ' '.join(data)
    tokens = token_re.findall(input_text)
    namespace = {name: TaskNamespace(task) for name, task in pipeline.tasks.items()}
    for token in tokens:
        task, repo = token.split('.')
        await namespace[task].consume(repo)
    result = eval(input_text, {}, namespace)

    for job in sorted(result):
        print(job)

class TaskNamespace:
    def __init__(self, task: Task):
        self.task = task
        self.repos = {}

    def __getattr__(self, item):
        return self.repos[item]

    async def consume(self, repo: str):
        result = set()
        async for x in self.task.links[repo].repo:
            result.add(x)
        self.repos[repo] = result

async def cat_data(pipeline: Pipeline, data: str, job: str):
    taskname, reponame = data.split('.')
    item = pipeline.tasks[taskname].links[reponame].repo

    if isinstance(item, BlobRepository):
        async with await item.open(job, 'rb') as fp:
            print_bytes = aiofiles.os.wrap(sys.stdout.buffer.write)
            while True:
                data = await fp.read(4096)
                if not data:
                    break
                await print_bytes(data)
    elif isinstance(item, MetadataRepository):
        data = await item.info(job)
        data_str = yaml.safe_dump(data, None)
        await aiofiles.os.wrap(sys.stdout.write)(data_str)
    else:
        print("Error: cannot cat a repository which is not a blob or metadata")
        return 1

async def inject_data(pipeline: Pipeline, args: argparse.Namespace):
    taskname, reponame = args.data.split('.')
    item = pipeline.tasks[taskname].links[reponame].repo

    if isinstance(item, BlobRepository):
        with item.open(args.job, 'wb') as fp:
            fp.write(sys.stdin.buffer.read())
    elif isinstance(item, MetadataRepository):
        try:
            data = yaml.safe_load(sys.stdin)
        except yaml.YAMLError:
            print("Error: could not parse stdin as yaml")
            return 1
        await item.dump(args.job, data)
    else:
        print("Error: cannot inject data into a repository which is not a blob or metadata")
        return 1

async def launch(pipeline: Pipeline, task: str, job: str, sync: bool, meta: bool, force: bool):
    task = pipeline.tasks[task]
    job = job
    settings(sync, meta)

    if force or job in task.ready:
        await task.launch(job)
    else:
        l.warning("Task is not ready to launch - use -f to force")
        return 1
