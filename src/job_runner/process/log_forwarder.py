import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import logging as _log
import pathlib as _pl
import typing as _tp

import resultes_pydantic_models.runner as _mrunner

from .. import executor as _ex
from . import _helpers
from . import _line_builder as _lb
from . import process as _proc

_LOGGER = _log.getLogger(__name__)


class LogForwarder(_proc.RunAlongBase):
    def __init__(
        self,
        job_id: str,
        command_number: int,
        log_file_path: _pl.Path | None,
        log_level: int,
        executor: _ex.Executor,
    ) -> None:
        self._job_id = job_id
        self._command_number = command_number
        self._log_file_path_or_none = log_file_path
        self._log_level = log_level
        self._executor = executor

        self._shall_stop = False

    @property
    def _log_file_path(self) -> _pl.Path:
        if not self._log_file_path_or_none:
            raise RuntimeError("Log file path is None.")

        return self._log_file_path_or_none

    @_tp.override
    @_ctx.asynccontextmanager
    async def run_along(self, queue: _proc.Queue) -> _cabc.AsyncIterator[None]:
        if not self._log_file_path_or_none:
            yield
            return

        seconds_to_wait = 10.0

        was_log_file_created = await _helpers.is_file_created_within(
            self._job_id, self._log_file_path, seconds_to_wait, self._executor
        )
        if not was_log_file_created:
            yield
            return

        log_file_reader_task = _asyncio.create_task(self._log_file_reader(queue))

        yield

        _LOGGER.info("Request stop reading from log file %s.", self._log_file_path)
        self._shall_stop = True
        await log_file_reader_task

    async def _log_file_reader(self, queue: _proc.Queue) -> None:
        line_builder = _lb.LineBuilder()
        with self._log_file_path.open("rt") as log_file:
            _LOGGER.info("Start reading from file %s...", self._log_file_path)

            while True:
                bytes = await self._executor.run(log_file.read)
                new_lines = line_builder.add_bytes_and_get_new_lines(bytes)
                for new_line in new_lines:
                    message = f"{self._log_file_path.name} - {new_line}"
                    log_message = _mrunner.LogMessage(
                        level=self._log_level,
                        message=message,
                        command_number=self._command_number,
                    )
                    await queue.put(log_message)

                if self._shall_stop:
                    break

                sleep_seconds = 1.0
                await _asyncio.sleep(sleep_seconds)

            _LOGGER.info("...DONE reading from log file %s", self._log_file_path)

    @_tp.override
    async def get_error_message_or_none(self) -> str | None:
        if not self._log_file_path_or_none:
            return None

        if not await self._executor.run(self._log_file_path_or_none.is_file):
            error_message = f"{self._job_id} - Unexpectedly, log file {self._log_file_path_or_none} has not been created."
            return error_message
