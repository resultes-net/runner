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


type Queue = _asyncio.Queue[_mrunner.JobSuccessfulPayload | _ProcessDone]


class RunAlongBase(_abc.ABC):
    @_abc.abstractmethod
    def run_along(
        self,
        queue: Queue,
    ) -> _tp.AsyncContextManager[None]:
        raise NotImplementedError()

    @_abc.abstractmethod
    async def check_error_and_possibly_raise(self) -> None:
        raise NotImplementedError()


class Process:
    def __init__(
        self,
        job_id: str,
        program: _pl.PureWindowsPath,
        args: _cabc.Sequence[str],
        working_dir_path: _pl.Path,
        run_alongs: _cabc.Sequence[RunAlongBase] | None = None,
    ) -> None:
        self._job_id = job_id
        self._program = program
        self._args = args
        self._working_dir_path = working_dir_path
        self._run_alongs = run_alongs if run_alongs else list[RunAlongBase]()

        self._queue = _asyncio.Queue[_mrunner.JobSuccessfulPayload | _ProcessDone]()

    async def run(self) -> _cabc.AsyncIterable[_mrunner.JobSuccessfulPayload]:
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

            coroutine = self._wait_for_process(process)
            process_waiter = _asyncio.create_task(coroutine)

            while payload := await self._queue.get():
                match payload:
                    case _ProcessDone():
                        break
                    case _:
                        yield payload

            return_code = await process.wait()

        if return_code != 0:
            assert process.stderr
            stderr_bytes = await process.stderr.read()
            stderr = stderr_bytes.decode()

            _LOGGER.error(
                "%s - An error occurred running command %s: '%s'.",
                self._job_id,
                self._program,
                stderr,
            )

            raise RuntimeError(
                f"An error occurred running command {self._program}: {stderr}"
            )

        for run_along in self._run_alongs:
            await run_along.check_error_and_possibly_raise()

    @_ctx.asynccontextmanager
    async def _run_run_alongs(self) -> _cabc.AsyncIterator[None]:
        async with _ctx.AsyncExitStack() as exit_stack:
            for run_along in self._run_alongs:
                context = run_along.run_along(self._queue)
                await exit_stack.enter_async_context(context)

            yield

    async def _wait_for_process(self, process: _asp.Process) -> None:
        await process.wait()
        await self._queue.put(_ProcessDone())
