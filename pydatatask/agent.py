"""Tools for interacting with repositories in an extremely blunt way.

This is the only hammer you will ever need, if you are okay with that hammer kind of sucking.
"""
from __future__ import annotations

from aiohttp import web
import yaml

from pydatatask.utils import AReadStreamBase, AWriteStreamBase, async_copyfile

from . import repository as repomodule
from .pipeline import Pipeline


def build_agent_app(pipeline: Pipeline, owns_pipeline: bool = False) -> web.Application:
    """Given a pipeline, generate an aiohttp web app to serve its repositories."""
    app = web.Application()

    def authorize(f):
        async def inner(request: web.Request) -> web.StreamResponse:
            if request.cookies.get("secret", None) != pipeline.agent_secret:
                raise web.HTTPForbidden()
            return await f(request)

        return inner

    def parse(f):
        async def inner(request: web.Request) -> web.StreamResponse:
            try:
                repo = pipeline.tasks[request.match_info["task"]].links[request.match_info["link"]].repo
            except KeyError as e:
                raise web.HTTPNotFound() from e
            return await f(request, repo, request.match_info["job"])

        return inner

    @authorize
    @parse
    async def get(request: web.Request, repo: repomodule.Repository, job: str) -> web.StreamResponse:
        response = web.StreamResponse()
        await response.prepare(request)
        await cat_data(repo, job, response)
        await response.write_eof()
        return response

    @authorize
    @parse
    async def post(request: web.Request, repo: repomodule.Repository, job: str) -> web.StreamResponse:
        await inject_data(repo, job, request.content)
        return web.Response(text=job)

    @authorize
    async def stream(request: web.Request) -> web.StreamResponse:
        try:
            task = pipeline.tasks[request.match_info["task"]]
            repo = task._repo_filtered(request.match_info["job"], request.match_info["link"])
        except KeyError as e:
            raise web.HTTPNotFound() from e
        response = web.StreamResponse()
        await response.prepare(request)
        async for result in repo:
            await response.write(result.encode() + b"\n")
        await response.write_eof()
        return response

    @authorize
    async def query(request: web.Request) -> web.StreamResponse:
        try:
            task = pipeline.tasks[request.match_info["task"]]
            query = task.queries[request.match_info["query"]]
        except KeyError as e:
            raise web.HTTPNotFound() from e
        try:
            params = yaml.safe_load(await request.read())
        except yaml.error.YAMLError as e:
            raise web.HTTPBadRequest() from e

        result = await query.execute(params)

        response = web.StreamResponse()
        await response.prepare(request)
        await query.format_response(result, response)
        await response.write_eof()
        return response

    @authorize
    async def cokey_post(request: web.Request) -> web.StreamResponse:
        try:
            task = pipeline.tasks[request.match_info["task"]]
            link = task.links[request.match_info["link"]]
            repo = link.cokeyed[request.match_info["cokey"]]
            job = request.match_info["job"]
        except KeyError as e:
            raise web.HTTPNotFound() from e
        await inject_data(repo, job, request.content)
        return web.Response(text=job)

    app.add_routes([web.get("/data/{task}/{link}/{job}", get), web.post("/data/{task}/{link}/{job}", post)])
    app.add_routes([web.get("/stream/{task}/{link}/{job}", stream)])
    app.add_routes([web.post("/query/{task}/{query}", query)])
    app.add_routes([web.post("/cokeydata/{task}/{link}/{cokey}/{job}", cokey_post)])

    async def on_startup(_app):
        await pipeline.open()

    async def on_shutdown(_app):
        await pipeline.close()

    if owns_pipeline:
        app.on_startup.append(on_startup)
        app.on_shutdown.append(on_shutdown)
    return app


async def cat_data(item: repomodule.Repository, job: str, stream: AWriteStreamBase):
    """Copy one job of a repository to a stream."""
    if isinstance(item, repomodule.BlobRepository):
        async with await item.open(job, "rb") as fp:
            await async_copyfile(fp, stream)
    elif isinstance(item, repomodule.MetadataRepository):
        data_bytes = await item.info(job)
        data_str = yaml.safe_dump(data_bytes, None)
        if isinstance(data_str, str):
            await stream.write(data_str.encode())
        else:
            await stream.write(data_str)
    elif isinstance(item, repomodule.FilesystemRepository):
        await item.get_tarball(job, stream)
    else:
        raise TypeError(type(item))


async def inject_data(item: repomodule.Repository, job: str, stream: AReadStreamBase):
    """Ingest one job of a repository from a stream."""
    if isinstance(item, repomodule.BlobRepository):
        async with await item.open(job, "wb") as fp:
            await async_copyfile(stream, fp)
    elif isinstance(item, repomodule.MetadataRepository):
        data = await stream.read()
        try:
            data_obj = yaml.safe_load(data)
        except yaml.YAMLError as e:
            raise ValueError(e.args[0]) from e
        await item.dump(job, data_obj)
    elif isinstance(item, repomodule.FilesystemRepository):
        await item.dump_tarball(job, stream)
    else:
        raise TypeError(type(item))
