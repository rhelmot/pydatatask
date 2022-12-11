import unittest

import pydatatask


class TestRelated(unittest.IsolatedAsyncioTestCase):
    async def test_related(self):
        translator = pydatatask.InProcessMetadataRepository({"456": "123"})

        base1 = pydatatask.InProcessBlobRepository({"123": b"asdf"})
        related1 = pydatatask.RelatedItemRepository(base1, translator)
        async with await related1.open("456", "rb") as fp:
            assert await fp.read() == b"asdf"
        await related1.delete("456")
        assert [x async for x in base1] == ["123"]
        assert [x async for x in related1] == ["456"]
        assert await related1.contains("456")
        assert not await related1.contains("lksfdlkfd")
        with self.assertRaises(LookupError):
            await related1.info("lkjfdslkj")
        with self.assertRaises(LookupError):
            await related1.open("lkjfdslkj", "w")

        base2 = pydatatask.InProcessMetadataRepository({"123": "weh!"})
        related2 = pydatatask.RelatedItemRepository(base2, translator, allow_deletes=True, prefetch_lookup=False)
        assert await related2.info("456") == "weh!"

        await related2.delete("456")
        assert [x async for x in base2] == []
        await related2.delete("lsdlkjfj")


if __name__ == "__main__":
    unittest.main()
