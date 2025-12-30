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


class ProgressForwarder(_proc.RunAlongBase):
    def __init__(
        self,
        job_id: str,
        command_number: int,
        n_total_time_steps: int,
        time_step_prt_file_path: _pl.Path,
        executor: _ex.Executor,
    ) -> None:
        self._job_id = job_id
        self._command_number = command_number
        self._n_total_time_steps = n_total_time_steps
        self._time_step_prt_file_path = time_step_prt_file_path
        self._executor = executor

        self._shall_stop = False

    @_tp.override
    @_ctx.asynccontextmanager
    async def run_along(self, queue: _proc.Queue) -> _cabc.AsyncIterator[None]:
        seconds_to_wait = 60.0

        was_log_file_created = await _helpers.is_file_created_within(
            self._job_id, self._time_step_prt_file_path, seconds_to_wait, self._executor
        )
        if not was_log_file_created:
            yield
            return

        coroutine = self._progress_forwarder(queue)
        progress_forwarder_task = _asyncio.create_task(coroutine)

        yield

        _LOGGER.info(
            "Request stop forwarding progress from file %s.",
            self._time_step_prt_file_path,
        )
        self._shall_stop = True
        await progress_forwarder_task

    async def _progress_forwarder(self, queue: _proc.Queue) -> None:
        line_builder = _lb.LineBuilder()
        with self._time_step_prt_file_path.open("rt") as prt_file:
            _LOGGER.info("Start reading from file %s...", self._time_step_prt_file_path)

            n_lines = 0
            progress = 0
            while True:
                bytes = await self._executor.run(prt_file.read)
                new_lines = line_builder.add_bytes_and_get_new_lines(bytes)

                n_new_lines = len(new_lines)
                if n_new_lines > 0:
                    n_lines += n_new_lines

                    # -1 for the header line and another -1 for the second line, giving
                    # the values at simulation start time.
                    n_time_steps = n_lines - 2

                    new_progress = round(n_time_steps / self._n_total_time_steps * 100)

                    if new_progress > progress:
                        progress = new_progress

                        job_progress = _mrunner.JobProgress(
                            progress=progress, command_number=self._command_number
                        )

                        await queue.put(job_progress)


                if self._shall_stop:
                    break

                sleep_seconds = 1.0
                await _asyncio.sleep(sleep_seconds)

            _LOGGER.info(
                "...DONE reading from log file %s", self._time_step_prt_file_path
            )

    @_tp.override
    async def check_error_and_possibly_raise(self) -> None:
        if not await self._executor.run(self._time_step_prt_file_path.is_file):
            raise RuntimeError(
                f"{self._job_id} - Unexpectedly, Prt file {self._time_step_prt_file_path} has not been created."
            )
