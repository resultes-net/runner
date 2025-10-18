import asyncio as _asyncio
import asyncio.subprocess as _asp
import datetime as _dt
import logging as _log
import pathlib as _pl

from . import line_builder as _lb

_LOGGER = _log.getLogger(__name__)


class ProcessWaiter:
    def __init__(
        self,
        job_id: str,
        process: _asp.Process,
        working_dir_path: _pl.Path,
        relative_log_file_path: _pl.PureWindowsPath | None,
    ) -> None:
        self._job_id = job_id
        self._process = process
        self._is_process_done = False
        self._working_dir_path = working_dir_path
        self._relative_log_file_path = relative_log_file_path

    async def wait(
        self,
    ) -> int:
        if not self._relative_log_file_path:
            return await self._process.wait()

        log_file_path = self._working_dir_path / self._relative_log_file_path

        return await self._wait_for_subprocess_and_forward_logging(log_file_path)

    async def _wait_for_subprocess_and_forward_logging(
        self,
        log_file_path: _pl.Path,
    ) -> int:
        seconds_to_wait = 10.0

        was_log_file_created = await self._is_file_created_within(
            log_file_path, seconds_to_wait
        )

        log_file_reader_task: _asyncio.Task[None] | None = None

        if was_log_file_created:
            log_file_reader_task = _asyncio.create_task(
                self._log_file_reader(log_file_path)
            )

        return_code = await self._process.wait()
        self._is_process_done = True

        if log_file_reader_task:
            await log_file_reader_task

        if return_code == 0 and not was_log_file_created:
            # If the log file was not created but the program failed anyway, then we just report the
            # program's failure as that will give us more insight.
            raise TimeoutError(
                f"Log file {log_file_path} was not created within {seconds_to_wait} second(s)."
            )

        return return_code

    async def _is_file_created_within(
        self, log_file_path: _pl.Path, seconds_to_wait: float
    ) -> bool:
        _LOGGER.info("Waiting for file %s to be created...", log_file_path)

        start = _dt.datetime.now()
        max_delta = _dt.timedelta(seconds=seconds_to_wait)

        sleep_seconds = 1.0
        while (delta := _dt.datetime.now() - start) < max_delta:
            if await _asyncio.to_thread(log_file_path.is_file):
                _LOGGER.info(
                    "...DONE. Was created after %f seconds.", delta.total_seconds()
                )
                return True

            await _asyncio.sleep(sleep_seconds)

        _LOGGER.error(
            "Log file %s was not created after %f seconds.",
            log_file_path,
            delta.total_seconds(),
        )

        return False

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
