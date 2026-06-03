import abc as _abc
import asyncio as _asyncio
import asyncio.subprocess as _asp
import collections.abc as _cabc
import contextlib as _ctx
import logging as _log
import pathlib as _pl
import typing as _tp

import resultes_pydantic_models.runner as _mrunner

_LOGGER = _log.getLogger(__name__)


class _ProcessDone:
    pass


type Queue = _asyncio.Queue[_mrunner.JobPayload | _ProcessDone]


class RunAlongBase(_abc.ABC):
    @_abc.abstractmethod
    def run_along(
        self,
        queue: Queue,
    ) -> _tp.AsyncContextManager[None]:
        raise NotImplementedError()

    @_abc.abstractmethod
    async def get_error_message_or_none(self) -> str | None:
        raise NotImplementedError()


class Process:
    def __init__(
        self,
        job_id: str,
        command_number: int,
        program: _pl.PureWindowsPath,
        args: _cabc.Sequence[str],
        working_dir_path: _pl.Path,
        run_alongs: _cabc.Sequence[RunAlongBase] | None = None,
    ) -> None:
        self._job_id = job_id
        self._command_number = command_number
        self._program = program
        self._args = args
        self._working_dir_path = working_dir_path
        self._run_alongs = run_alongs if run_alongs else list[RunAlongBase]()

        self._queue = _asyncio.Queue[_mrunner.JobPayload | _ProcessDone]()

    async def run(self) -> _cabc.AsyncIterable[_mrunner.JobPayload]:
        process = await _asyncio.create_subprocess_exec(
            self._program,
            *self._args,
            cwd=self._working_dir_path,
            stderr=_asp.PIPE,
        )

        async with self._run_run_alongs():
            _LOGGER.info(
                "Running %s with args %s in working dir %s...",
                self._program,
                self._args,
                self._working_dir_path,
            )

            coroutine = self._wait_for_process_and_get_stderr_if_any(process)
            stderr_task = _asyncio.create_task(coroutine)

            while payload := await self._queue.get():
                match payload:
                    case _ProcessDone():
                        break
                    case _:
                        yield payload

        for payload in self._flush_queue():
            yield payload

        stderr = await stderr_task

        if stderr is not None:
            _LOGGER.error(
                "%s - An error occurred running command %s: '%s'.",
                self._job_id,
                self._program,
                stderr,
            )

            error_message = (
                f"An error occurred running command {self._program}: {stderr}"
            )
            job_error = _mrunner.JobError(
                command_number=self._command_number, message=error_message
            )
            yield job_error
            return

        for run_along in self._run_alongs:
            error_message_or_none = await run_along.get_error_message_or_none()
            if error_message_or_none is not None:
                job_error = _mrunner.JobError(
                    command_number=self._command_number, message=error_message_or_none
                )
                yield job_error
                return

    @_ctx.asynccontextmanager
    async def _run_run_alongs(self) -> _cabc.AsyncIterator[None]:
        async with _ctx.AsyncExitStack() as exit_stack:
            for run_along in self._run_alongs:
                context = run_along.run_along(self._queue)
                await exit_stack.enter_async_context(context)

            yield

    async def _wait_for_process_and_get_stderr_if_any(self, process: _asp.Process) -> str | None:
        _, stderr_bytes = await process.communicate()
        await self._queue.put(_ProcessDone())

        if process.returncode == 0:
            return None

        return stderr_bytes.decode()

    def _flush_queue(self) -> _cabc.Iterator[_mrunner.JobPayload]:
        try:
            while payload := self._queue.get_nowait():
                assert not isinstance(payload, _ProcessDone)
                yield payload
        except _asyncio.QueueEmpty:
            pass
