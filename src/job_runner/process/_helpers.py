import asyncio as _asyncio
import datetime as _dt
import logging as _log
import pathlib as _pl

from .. import executor as _ex

_LOGGER = _log.getLogger(__name__)


async def is_file_created_within(
    job_id: str, file_path: _pl.Path, seconds_to_wait: float, executor: _ex.Executor
) -> bool:
    assert file_path

    _LOGGER.info("%s - Waiting for file %s to be created...", job_id, file_path)

    start = _dt.datetime.now()
    max_delta = _dt.timedelta(seconds=seconds_to_wait)

    sleep_seconds = 1.0
    while (delta := _dt.datetime.now() - start) < max_delta:
        if await executor.run(file_path.is_file):
            _LOGGER.info(
                "...DONE. Was created after %f seconds.", delta.total_seconds()
            )
            return True

        await _asyncio.sleep(sleep_seconds)

    _LOGGER.error(
        "File %s was not created after %f seconds.",
        file_path,
        delta.total_seconds(),
    )

    return False
