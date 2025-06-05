import asyncio as _asyncio
import logging as _log
import pathlib as _pl
import uuid as _uuid

import pytest as _pt
import resultes_pydantic_models.pytrnsys as _mpytrnsys

import swift_multithreaded as _swmt


@_pt.mark.parametrize(
    ("n_tasks", "n_threads", "timeout"),
    (
        # (3, 4, 20.0),
        (7, 4, 20.0),
        # (4, 4, 20.0),
    ),
)
@_pt.mark.asyncio
async def test_download(n_tasks: int, n_threads: int, timeout: float) -> None:
    _log.basicConfig(format=_swmt.LOG_FORMAT, level=_swmt.LOG_LEVEL)

    async with _swmt.Swift(n_threads) as swift:
        try:
            async with _asyncio.timeout(timeout):
                coroutines = [download_trnsys(swift) for _ in range(n_tasks)]
                await _asyncio.gather(*coroutines)
        except TimeoutError:
            pass


async def download_trnsys(swift: _swmt.Swift) -> None:
    object_storage_zip_path = _mpytrnsys.ObjectStorageZipPath(
        container="resultes", path="build-runner-image/TRNSYS18_resultes.zip"
    )
    uuid = _uuid.uuid4()
    output_file_path = (
        _pl.Path(__file__).parent / "test_output" / f"TRNSYS18_resultes-{uuid}.zip"
    )
    await swift.download(object_storage_zip_path, output_file_path)


if __name__ == "__main__":
    _asyncio.run(test_download(3, 4, 60.0))
