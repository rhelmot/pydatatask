import argparse
import sys
from typing import Union
import yaml

from . import BlobRepository, MetadataRepository
from .pipeline import Pipeline
from .repository import Repository
from .task import Task

def main(pipeline: Pipeline):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    parser_update = subparsers.add_parser("update", help="Keep the pipeline in motion")

    parser_run = subparsers.add_parser("run", help="Run update in a loop until everything is quiet")
    parser_run.add_argument("--forever", action="store_true", help="Run forever")

    parser_status = subparsers.add_parser("status", help="View the pipeline status")
    parser_status.add_argument("--all", "-a", action="store_true", help="Show internal repositories")

    parser_delete = subparsers.add_parser("rm", help="Delete data from the pipeline")
    parser_delete.add_argument("--recursive", "-r", action="store_true", help="Delete dependant data too")
    parser_delete.add_argument("data", type=str, help="Name of repository [task.repo] or task from which to delete data")
    parser_delete.add_argument("job", type=str, nargs='+', help="Name of job of which to delete data")

    parser_ls = subparsers.add_parser("ls", help="List jobs in a repository")
    parser_ls.add_argument("data", type=str, nargs='+', help="Name of repository [task.repo] from which to list data")

    parser_cat = subparsers.add_parser("cat", help="Print data from a repository")
    parser_cat.add_argument("data", type=str, help="Name of repository [task.repo] from which to print data")
    parser_cat.add_argument("job", type=str, help="Name of job of which to delete data")

    args = parser.parse_args()

    if args.cmd == "update":
        pipeline.update()
    elif args.cmd == "run":
        run(pipeline, args)
    elif args.cmd == "status":
        print_status(pipeline, args)
    elif args.cmd == "rm":
        delete_data(pipeline, args)
    elif args.cmd == "ls":
        list_data(pipeline, args)
    elif args.cmd == "cat":
        cat_data(pipeline, args)
    else:
        assert False, "This should be unreachable"

def run(pipeline: Pipeline, args: argparse.Namespace):
    while pipeline.update() or args.forever:
        pass

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
        exit(1)
