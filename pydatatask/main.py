from argparse import _SubParsersAction, ArgumentParser
from typing import Union, Callable, Optional, Any
import argparse
import sys
import yaml
import logging
import time
import IPython

from . import BlobRepository, MetadataRepository
from .pipeline import Pipeline
from .repository import Repository
from .task import Task, settings

l = logging.getLogger(__name__)

def main(pipeline: Pipeline, instrument: Optional[Callable[[argparse._SubParsersAction], None]]=None):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    parser_update = subparsers.add_parser("update", help="Keep the pipeline in motion")
    parser_update.set_defaults(func=update)

    parser_run = subparsers.add_parser("run", help="Run update in a loop until everything is quiet")
    parser_run.add_argument("--forever", action="store_true", help="Run forever")
    parser_run.add_argument("--launch-once", action="store_true", help="Only evaluates tasks-to-launch once")
    parser_run.set_defaults(func=run)

    parser_status = subparsers.add_parser("status", help="View the pipeline status")
    parser_status.add_argument("--all", "-a", action="store_true", help="Show internal repositories")
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
    return args.func(pipeline, args)

def shell(pipeline: Pipeline, args: argparse.Namespace):
    IPython.embed()

def update(pipeline: Pipeline, args: argparse.Namespace):
    pipeline.update()

def run(pipeline: Pipeline, args: argparse.Namespace):
    func = pipeline.update
    while func() or args.forever:
        if args.launch_once:
            func = pipeline.update_only_update
        time.sleep(1)

def print_status(pipeline: Pipeline, args: argparse.Namespace):
    for task in pipeline.tasks.values():
        print(task.name)
        for link_name, link in sorted(task.links.items(), key=lambda x: 0 if x[1].is_input else 1 if x[1].is_status else 2):
            if not args.all and not link.is_status and not link.is_input and not link.is_output:
                continue
            print(f'{task.name}.{link_name} {sum(1 for _ in link.repo)}')
        print()

def delete_data(pipeline: Pipeline, args: argparse.Namespace):
    item: Union[Task | Repository]
    if '.' in args.data:
        taskname, reponame = args.data.split('.')
        item = pipeline.tasks[taskname].links[reponame].repo
    else:
        item = pipeline.tasks[args.data]

    for dependant in pipeline.dependants(item, args.recursive):
        if isinstance(dependant, Repository):
            if args.job[0] == '__all__':
                jobs = list(dependant)
                check = False
            else:
                jobs = args.job
                check = True
            for job in jobs:
                if not check or job in dependant:
                    del dependant[job]
                    print(job, dependant)

def list_data(pipeline: Pipeline, args: argparse.Namespace):
    input_text = ' '.join(args.data)
    namespace = {name: TaskNamespace(task) for name, task in pipeline.tasks.items()}
    result = eval(input_text, {}, namespace)

    for job in sorted(result):
        print(job)

class TaskNamespace:
    def __init__(self, task: Task):
        self.task = task

    def __getattr__(self, item):
        return set(self.task.links[item].repo)

def cat_data(pipeline: Pipeline, args: argparse.Namespace):
    taskname, reponame = args.data.split('.')
    item = pipeline.tasks[taskname].links[reponame].repo

    if isinstance(item, BlobRepository):
        with item.open(args.job, 'rb') as fp:
            sys.stdout.buffer.write(fp.read())
    elif isinstance(item, MetadataRepository):
        data = item.info(args.job)
        yaml.safe_dump(data, sys.stdout)
    else:
        print("Error: cannot cat a repository which is not a blob or metadata")
        return 1

def inject_data(pipeline: Pipeline, args: argparse.Namespace):
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
        item.dump(args.job, data)
    else:
        print("Error: cannot inject data into a repository which is not a blob or metadata")
        return 1

def launch(pipeline: Pipeline, args: argparse.Namespace):
    task = pipeline.tasks[args.task]
    job = args.job
    settings(args.sync, args.meta)

    if args.force or job in task.ready:
        task.launch(job)
    else:
        l.warning("Task is not ready to launch - use -f to force")
        return 1
