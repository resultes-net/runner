import asyncio as _asyncio
import asyncio.subprocess as _asp
import collections.abc as _cabc
import dataclasses as _dc
import logging as _log
import pathlib as _pl
import shutil as _su
import subprocess as _sp
import typing as _tp

import jsonrpcserver as _jrpcs
import jsonrpcserver.codes as _jrpcc
import resultes_pydantic_models.runner as _mrunner

import context as _con
import line_builder as _lb

_LOGGER = _log.getLogger(__name__)


def _unzip(input_file_path: _pl.Path, output_dir_path: _pl.Path) -> None:
    _su.unpack_archive(input_file_path, output_dir_path)


def _zip_dir(input_dir_path: _pl.Path, output_file_path: _pl.Path) -> None:
    base_name = str(output_file_path.with_suffix(""))
    format = output_file_path.suffix.removeprefix(".")
    root_dir = input_dir_path

    _su.make_archive(base_name, format, root_dir)


def _get_return_paths(
    output_dir_path: _pl.Path, results_glob_pattern: str | None
) -> _cabc.Sequence[str] | None:
    if not results_glob_pattern:
        return None

    result_paths = [
        p.relative_to(output_dir_path)
        for p in output_dir_path.glob(results_glob_pattern)
    ]

    result_path_strings = [str(p) for p in result_paths]

    return result_path_strings


@_dc.dataclass
class _CommandError:
    message: str


class _ProcessWaiter:
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


class JobRunner:
    def __init__(
        self,
        runner_job: _mrunner.RunnerJob,
        context: _con.Context,
        loop: _asyncio.AbstractEventLoop,
    ) -> None:
        self._runner_job = runner_job
        self._context = context
        self._loop = loop

        self._job_dir_path = context.jobs_dir_path / self._runner_job.id

        output_file_name = runner_job.object_storage_input_path.path.split("/")[-1]
        output_file_path = self._job_dir_path / output_file_name
        self._base_dir_path = self._job_dir_path / output_file_path.stem

    @property
    def _job_id(self) -> str:
        return self._runner_job.id

    async def run(self) -> _jrpcs.Result:
        self._log_info("Job started.")

        job_dir_exists = await self._run_in_executor(self._job_dir_path.exists)

        if job_dir_exists:
            return _jrpcs.Error(
                code=_jrpcc.ERROR_SERVER_ERROR,
                message=f"Have seen job ID {self._job_id} before. Job IDs must be unique, forever.",
            )

        await self._download_input()

        await self._run_commands()

        await self._upload_results()

        return_paths = await self._get_return_paths()

        if self._context.shall_remove_completed_jobs:
            await self._run_in_executor(_su.rmtree, self._job_dir_path)

        if return_paths is not None:
            return _jrpcs.Success(return_paths)

        return _jrpcs.Success()

    def _log_info(self, message: str, *args: _tp.Any, **kwargs: _tp.Any) -> None:
        augmented_message = f"%s - {message}"
        _LOGGER.info(augmented_message, self._job_id, *args, **kwargs)

    async def _run_in_executor[*A, T](
        self,
        method: _cabc.Callable[[*A], T],
        *args: *A,
    ) -> T:
        return await self._loop.run_in_executor(self._context.executor, method, *args)

    async def _download_input(
        self,
    ) -> None:
        output_file_path = self._base_dir_path.with_suffix(".zip")

        await self._context.swift.download(
            self._runner_job.object_storage_input_path, output_file_path
        )

        await self._run_in_executor(self._base_dir_path.mkdir)
        await self._run_in_executor(_unzip, output_file_path, self._base_dir_path)

    async def _run_commands(self) -> None:
        for command in self._runner_job.commands:
            error = await self._run_command(command)

            if error:
                raise _jrpcs.JsonRpcError(
                    code=_jrpcc.ERROR_SERVER_ERROR,
                    message=f"Job program exited with non-zero exit code: {error.message}",
                )

    async def _run_command(
        self,
        command: _mrunner.Command,
    ) -> None | _CommandError:
        working_dir_path = (
            command.program.parent
            if command.working_dir is None
            else self._base_dir_path / command.working_dir
        )

        self._log_info("Running %s in subprocess...", command)

        process = await _asyncio.create_subprocess_exec(
            command.program, *command.args, cwd=working_dir_path, stderr=_sp.PIPE
        )

        process_waiter = _ProcessWaiter(
            self._job_id, process, self._base_dir_path, command.relative_log_file_path
        )

        return_code = await process_waiter.wait()

        self._log_info("Done.")

        if return_code != 0:
            assert process.stderr
            stderr_bytes = await process.stderr.read()
            stderr = stderr_bytes.decode()

            _LOGGER.warning(
                "%s - An error occurred running command %s: '%s'.",
                self._job_id,
                command,
                stderr,
            )

            return _CommandError(stderr)

        return None

    async def _upload_results(
        self,
    ):
        result_file_name = f"{self._job_id}.zip"
        result_file_path = self._job_dir_path / result_file_name
        await self._run_in_executor(_zip_dir, self._base_dir_path, result_file_path)

        result_object_storage_path = _mrunner.ObjectStorageOutputZipFilePath(
            container="resultes-results",
            path=f"results/{result_file_name}",
        )
        await self._context.swift.upload(result_file_path, result_object_storage_path)

    async def _get_return_paths(self) -> _cabc.Sequence[str] | None:
        return_paths = await self._run_in_executor(
            _get_return_paths,
            self._base_dir_path,
            self._runner_job.return_paths_glob_pattern,
        )

        return return_paths
