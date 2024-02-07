from pathlib import Path
import asyncio

from common import acquire, transform, transform_unpack

import pydatatask.staging


@transform(transform_unpack(acquire("https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.7.4.tar.xz")), "mkrepo")
def linux_repo_path(in_path: Path, out_path: Path):
    out_path.mkdir()
    (out_path / "67").symlink_to(in_path)


async def main():
    repo = pydatatask.DirectoryRepository(linux_repo_path)
    sum = 0
    async for member in repo.iter_members("67"):
        if member.data is not None:
            sum += len(await member.data.read())
    return sum


if __name__ == "__main__":
    asyncio.run(main())
