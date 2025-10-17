import asyncio as _asyncio
import asyncio.subprocess as _asp
import logging as _log
import pathlib as _pl

from . import line_builder as _lb

_LOGGER = _log.getLogger(__name__)


class ProcessWaiter:
    def __init__(
        self,
        job_id: str,
        process: _asp.Process,
        output_dir_path: _pl.Path,
        relative_log_file_path: _pl.PureWindowsPath | None,
    ) -> None:
        self._job_id = job_id
        self._process = process
        self._is_process_done = False
        self._output_dir_path = output_dir_path
        self._relative_log_file_path = relative_log_file_path

    async def wait(
        self,
    ) -> int:
        if not self._relative_log_file_path:
            return await self._process.wait()

        log_file_path = self._output_dir_path / self._relative_log_file_path

        return await self._wait_for_subprocess_and_forward_logging(log_file_path)

    async def _wait_for_subprocess_and_forward_logging(
        self,
        log_file_path: _pl.Path,
    ) -> int:
        await self._wait_till_file_exists(log_file_path)

        log_file_reader_task = _asyncio.create_task(
            self._log_file_reader(log_file_path)
        )

        return_code = await self._process.wait()
        self._is_process_done = True
        await log_file_reader_task

        return return_code

    async def _wait_till_file_exists(self, log_file_path: _pl.Path) -> None:
        log_file_creation_timeout_seconds = 10
        async with _asyncio.timeout(log_file_creation_timeout_seconds):
            while True:
                if await _asyncio.to_thread(log_file_path.is_file):
                    break

                sleep_seconds = 1.0
                await _asyncio.sleep(sleep_seconds)

    async def _log_file_reader(self, log_file_path: _pl.Path) -> None:
        line_builder = _lb.LineBuilder()
        with log_file_path.open("rt") as log_file:
            while not self._is_process_done:
                bytes = await _asyncio.to_thread(log_file.read)
                new_lines = line_builder.add_bytes_and_get_new_lines(bytes)
                for new_line in new_lines:
                    _LOGGER.debug(
                        "%s - %s: %s", self._job_id, log_file_path.name, new_line
                    )

                sleep_seconds = 1.0
                await _asyncio.sleep(sleep_seconds)
