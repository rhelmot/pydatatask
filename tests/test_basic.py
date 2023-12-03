import contextlib
import io
import unittest

import aiofiles.os

from pydatatask.task import LinkKind
import pydatatask


class TestBasic(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

    async def test_metadata(self):
        session = pydatatask.Session()
        repo0 = pydatatask.InProcessMetadataRepository()
        repo1 = pydatatask.InProcessBlobRepository()
        done = pydatatask.InProcessMetadataRepository()
        assert repr(repo0)
        assert repr(repo1)

        for i in range(100):
            repo0.data[str(i)] = i

        @pydatatask.InProcessSyncTask("task", done)
        async def task(repo0, repo1, **kwargs):
            async with aiofiles.open("/dev/urandom", "rb") as fp:
                data = await fp.read(await repo0.info())
            async with await repo1.open("w") as fp:
                await fp.write(data.hex()[: len(data)])

        task.link("repo0", repo0, kind=LinkKind.InputRepo)
        task.link("repo1", repo1, kind=LinkKind.OutputRepo)

        pipeline = pydatatask.Pipeline([task], session, [])
        async with pipeline:
            await pydatatask.run(pipeline, forever=False, launch_once=False, timeout=None)

        assert len(repo0.data) == len(repo1.data)
        for job, i in repo0.data.items():
            assert len(repo1.data[job]) == i

        assert len([x async for x in repo0]) == 100
        await repo1.delete("0")
        assert len([x async for x in repo1]) == 99

        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            await pydatatask.print_status(pipeline, False)
        assert captured.getvalue() == "task\n  task.repo0 100\n  task.done 100\n  task.repo1 99\n\n\n"

        captured = io.StringIO()
        with contextlib.redirect_stdout(captured):
            await pydatatask.print_trace(pipeline, False, ["0", "1", "foo"])
        assert captured.getvalue() == "task\ntask.repo0 0 1\ntask.done 0 1\ntask.repo1 1\n\n"


if __name__ == "__main__":
    unittest.main()
