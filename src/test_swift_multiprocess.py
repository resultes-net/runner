import asyncio as _asyncio
import logging as _log
import pathlib as _pl
import uuid as _uuid

import pytest as _pt
import resultes_pydantic_models.pytrnsys as _mpytrnsys

import swift_multiprocess as _swmp


@_pt.mark.asyncio
async def test_download() -> None:
    _log.basicConfig(format=_swmp.LOG_FORMAT, level=_swmp.LOG_LEVEL)

    async with _swmp.Swift(n_processes=4, max_queue_size=8) as swift:
        try:
            async with _asyncio.timeout(60):
                coroutines = [download_trnsys(swift) for _ in range(3)]
                await _asyncio.gather(*coroutines)
        except TimeoutError:
            pass


async def download_trnsys(swift: _swmp.Swift) -> None:
    object_storage_path = _mpytrnsys.ObjectStoragePath(
        container="resultes", path="build-runner-image/TRNSYS18_resultes.zip"
    )
    uuid = _uuid.uuid4()
    output_file_path = (
        _pl.Path(__file__).parent / "test_output" / f"TRNSYS18_resultes-{uuid}.zip"
    )
    await swift.download(
        object_storage_path.container, object_storage_path.path, output_file_path
    )


if __name__ == "__main__":
    _asyncio.run(test_download())
