import unittest
import pydatatask
import aiofiles.os

class TestBasic(unittest.IsolatedAsyncioTestCase):
    def __init__(self, method):
        super().__init__(method)

    async def test_basic(self):
        session = pydatatask.Session()
        repo0 = pydatatask.InProcessMetadataRepository()
        repo1 = pydatatask.InProcessMetadataRepository()
        done = pydatatask.InProcessMetadataRepository()

        for i in range(100):
            repo0.data[str(i)] = i

        @pydatatask.InProcessSyncTask("task", done)
        async def task(job, repo0: pydatatask.MetadataRepository, repo1: pydatatask.MetadataRepository):
            async with aiofiles.open("/dev/urandom", "rb") as fp:
                data = await fp.read(await repo0.info(job))
            await repo1.dump(job, data)

        task.link("repo0", repo0, is_input=True)
        task.link("repo1", repo1, is_output=True)

        pipeline = pydatatask.Pipeline([task], session)
        async with pipeline:
            await pydatatask.run(pipeline, forever=False, launch_once=False, timeout=None)

        assert len(repo0.data) == len(repo1.data)
        for job, i in repo0.data.items():
            assert len(repo1.data[job]) == i

if __name__ == '__main__':
    unittest.main()
