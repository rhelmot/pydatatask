from concurrent.futures import ThreadPoolExecutor
import unittest

import pydatatask


class TestExecutor(unittest.IsolatedAsyncioTestCase):
    async def test_executor(self):
        session = pydatatask.Session()
        repo0 = pydatatask.InProcessMetadataRepository({"foo": "weh"})
        repo1 = pydatatask.InProcessMetadataRepository()
        repo2 = pydatatask.InProcessMetadataRepository()
        phase0_done = pydatatask.InProcessMetadataRepository()
        phase1_done = pydatatask.InProcessMetadataRepository()

        executor = ThreadPoolExecutor(1)

        @pydatatask.ExecutorTask("phase0", executor, phase0_done)
        async def phase0(
            job: str,
            repo_zero: pydatatask.InProcessMetadataRepository,
            repo_one: pydatatask.InProcessMetadataRepository,
        ):
            await repo_one.dump(job, await repo_zero.info(job) + "!")

        @pydatatask.ExecutorTask("phase1", executor, phase1_done)
        async def phase1(
            job: str, repo_one: pydatatask.InProcessMetadataRepository, repo_two: pydatatask.InProcessMetadataRepository
        ):
            await repo_two.dump(job, await repo_one.info(job) + "?")

        phase0.link("repo_zero", repo0, is_input=True)
        phase0.link("repo_one", repo1, is_output=True)

        phase1.plug(phase0)
        phase1.link("repo_two", repo2, is_output=True)

        pipeline = pydatatask.Pipeline([phase0, phase1], session)

        async with pipeline:
            await pydatatask.run(pipeline, forever=False, launch_once=False, timeout=120)
            await pydatatask.launch(pipeline, "phase0", "bar", sync=True, meta=True, force=True)

        assert repo2.data["foo"] == "weh!?"
        assert "exception" in phase0_done.data["bar"]


if __name__ == "__main__":
    unittest.main()
