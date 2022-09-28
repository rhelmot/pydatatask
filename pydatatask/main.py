import argparse
import sys

from . import FileRepository
from .pipeline import Pipeline
from .repository import Repository

def main(pipeline: Pipeline):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    parser_update = subparsers.add_parser("update", help="Keep the pipeline in motion")

    parser_status = subparsers.add_parser("status", help="View the pipeline status")
    parser_status.add_argument("--all", "-a", action="store_true", help="Show internal repositories")

    parser_delete = subparsers.add_parser("rm", help="Delete data from the pipeline")
    parser_delete.add_argument("--recursive", "-r", action="store_true", help="Delete dependant data too")
    parser_delete.add_argument("data", type=str, help="Name of repository [task.repo] or task from which to delete data")
    parser_delete.add_argument("job", type=str, help="Name of job of which to delete data")

    parser_ls = subparsers.add_parser("ls", help="List jobs in a repository")
    parser_ls.add_argument("data", type=str, help="Name of repository [task.repo] from which to list data")

    parser_cat = subparsers.add_parser("cat", help="Print data from a repository")
    parser_cat.add_argument("data", type=str, help="Name of repository [task.repo] from which to print data")
    parser_cat.add_argument("job", type=str, help="Name of job of which to delete data")

    args = parser.parse_args()

    if args.cmd == "update":
        pipeline.update()
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

def print_status(pipeline: Pipeline, args: argparse.Namespace):
    for task in pipeline.tasks.values():
        print(task.name)
        for link_name, link in sorted(task.links.items(), key=lambda x: 0 if x[1].is_input else 1 if x[1].is_status else 2):
            if not args.all and not link.is_status and not link.is_input and not link.is_output:
                continue
            print(f'{task.name}.{link_name} {sum(1 for _ in link.repo)}')
        print()

def delete_data(pipeline: Pipeline, args: argparse.Namespace):
    if '.' in args.data:
        taskname, reponame = args.data.split('.')
        item = pipeline.tasks[taskname].links[reponame].repo
    else:
        item = pipeline.tasks[args.data]

    for dependant in pipeline.dependants(item, args.recursive):
        if isinstance(dependant, Repository):
            del dependant[args.job]
            print(args.job, dependant)

def list_data(pipeline: Pipeline, args: argparse.Namespace):
    taskname, reponame = args.data.split('.')
    item = pipeline.tasks[taskname].links[reponame].repo

    for job in item:
        print(job)

def cat_data(pipeline: Pipeline, args: argparse.Namespace):
    taskname, reponame = args.data.split('.')
    item = pipeline.tasks[taskname].links[reponame].repo

    if not isinstance(item, FileRepository):
        print("Error: cannot cat a repository which is not a file")
        exit(1)

    with item.open(args.job, 'rb') as fp:
        sys.stdout.buffer.write(fp.read())
