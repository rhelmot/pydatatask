from typing import cast
import asyncio
import pathlib
import unittest

from pydatatask.cli.lock import default_allocators_local
from pydatatask.repository.base import BlobRepository, MetadataRepository
from pydatatask.staging import Dispatcher, PipelineStaging
from pydatatask.task import ProcessTask

test_root = pathlib.Path(__file__).parent


class TestStreaming(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

    async def test_streaming(self):
        staging = PipelineStaging(test_root / "content" / "streaming_input.yaml")
        allocated = staging.allocate(default_allocators_local(), Dispatcher("LocalLinux", {"app": "test_streaming"}))
        allocated.save()
        allocated = PipelineStaging(test_root / "content" / "streaming_input.lock")
        pipeline = allocated.instantiate()
        task = cast(ProcessTask, pipeline.tasks["task"])
        scope = cast(MetadataRepository, task.links["scope"].repo)
        live = task.links["live"].repo
        inputScope = cast(MetadataRepository, task.links["inputScope"].repo)
        inputBlob = cast(BlobRepository, task.links["input"].repo)
        outputBlob = cast(BlobRepository, task.links["output"].repo)
        outputMeta = cast(MetadataRepository, task.links["outputMeta"].repo)
        # logs = cast(BlobRepository, task.links["stdout"].repo)
        await task.manager.launch_agent(pipeline)
        try:
            async with pipeline:
                assert not await pipeline.update()
                await scope.dump("1", "hoo hoo hoo")
                assert await pipeline.update()
                assert not await pipeline.update_only_launch()
                assert await live.contains("1")
                await inputScope.dump("100", {"target": 1})
                await inputScope.dump("101", {"target": 1})
                await inputScope.dump("102", {"target": 2})
                await inputScope.dump("103", {"target": 1})
                await inputScope.dump("104", {"target": 1})
                await inputBlob.blobdump("100", "hey")
                # await inputBlob.dump("101", "don't inject me")
                await inputBlob.blobdump("102", "hi")
                await inputBlob.blobdump("103", "done")
                await inputBlob.blobdump("104", "ho")
                await asyncio.sleep(0.5)
                assert await pipeline.update()
                i = 0
                while await pipeline.update():
                    await asyncio.sleep(0.5)
                    i += 1
                    if i > 100:
                        import subprocess

                        subprocess.run("find /tmp/pydatatask", shell=True, check=True)
                        subprocess.run("cat /tmp/pydatatask/test_streaming/task/1/stdout", shell=True, check=True)
                        subprocess.run("cat /tmp/pydatatask/agent-stdout", shell=True, check=True)
                        assert False, "Pipeline timeout"
                await asyncio.sleep(1)
                keys = {x async for x in outputBlob}
                allBlob = {await outputBlob.blobinfo(key) for key in keys}
                allMeta = [await outputMeta.info(key) for key in keys]
                assert allBlob == {b"hoo\nhey", b"hoo\ndone", b"hoo\nho"}
                assert len(allMeta) == 3
                # assert allMeta == [{"filename": "100", "parent": "1"}, {"filename": "103", "parent": "1"}, {"filename": "104", "parent": "1"}]
        finally:
            await task.manager.teardown_agent()
