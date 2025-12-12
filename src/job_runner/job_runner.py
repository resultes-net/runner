import asyncio as _asyncio
import collections.abc as _cabc
import json as _json
import logging as _log
import pathlib as _pl
import shutil as _su
import subprocess as _sp
import typing as _tp

import resultes_pydantic_models.runner as _mrunner

import context as _con

from . import executor as _ex
from . import process_waiter as _pw
from . import result_uploader as _ru

_LOGGER = _log.getLogger(__name__)


def _unzip(input_file_path: _pl.Path, output_dir_path: _pl.Path) -> None:
    _su.unpack_archive(input_file_path, output_dir_path)


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

        self._executor = _ex.Executor(self._loop, self._context.executor)

        self._job_dir_path = context.jobs_dir_path / self._runner_job.id

        self._downlad_dir_path = self._job_dir_path / "download"
        self._upload_dir_path = self._job_dir_path / "upload"

        self._working_dir_path = self._job_dir_path / "workdir"

        self._parameters_file_path = self._working_dir_path / "parameters.json"

    @property
    def _job_id(self) -> str:
        return self._runner_job.id

    async def run(self) -> _cabc.AsyncIterable[_mrunner.JobSuccessfulPayload]:
        self._log_info("Job started.")

        job_dir_exists = await self._executor.run(self._job_dir_path.exists)

        if job_dir_exists:
            raise RuntimeError(
                "Have seen job ID before. Job IDs must be unique, forever.",
            )

        await self._executor.run(self._create_directories)

        await self._maybe_save_parameters()

        await self._download_input()

        await self._run_commands()

        await self._upload_results()

        return_paths = await self._get_return_paths()

        if self._context.shall_remove_completed_jobs:
            await self._executor.run(_su.rmtree, self._job_dir_path)

        if return_paths is not None:
            yield _mrunner.JobSuccess(result=return_paths)
            return

        yield _mrunner.JobSuccess()

    def _log_info(self, message: str, *args: _tp.Any, **kwargs: _tp.Any) -> None:
        augmented_message = f"%s - {message}"
        _LOGGER.info(augmented_message, self._job_id, *args, **kwargs)

    def _create_directories(self):
        self._job_dir_path.mkdir()
        self._downlad_dir_path.mkdir()
        self._working_dir_path.mkdir()
        self._upload_dir_path.mkdir()

    async def _maybe_save_parameters(self) -> None:
        parameters = self._runner_job.parameters

        if not parameters:
            return

        json = _json.dumps(parameters, indent=4)

        await self._executor.run(self._parameters_file_path.write_text, json)

    async def _download_input(
        self,
    ) -> None:
        downloaded_file_name = self._runner_job.object_storage_input_path.path.split(
            "/"
        )[-1]
        downloaded_file_path = self._downlad_dir_path / downloaded_file_name

        await self._context.swift.download(
            self._runner_job.object_storage_input_path, downloaded_file_path
        )

        await self._executor.run(_unzip, downloaded_file_path, self._working_dir_path)

    async def _run_commands(self) -> None:
        for command in self._runner_job.commands:
            await self._run_command(command)

    async def _run_command(
        self,
        command: _mrunner.GeneralCommand,
    ) -> None:
        working_dir_path = (
            command.program.parent
            if command.working_dir is None
            else self._working_dir_path / command.working_dir
        )

        self._log_info(
            "Running %s in subprocess with full working dir %s...",
            command,
            working_dir_path,
        )

        process = await _asyncio.create_subprocess_exec(
            command.program, *command.args, cwd=working_dir_path, stderr=_sp.PIPE
        )

        process_waiter = _pw.ProcessWaiter(
            self._job_id,
            process,
            self._working_dir_path,
            command.relative_log_file_path,
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

            raise RuntimeError(f"An error occurred running command {command}: {stderr}")

    async def _upload_results(
        self,
    ):
        result_uploader = _ru.ResultUploader(
            self._working_dir_path,
            self._upload_dir_path,
            self._context.swift,
            self._executor,
        )

        for result in self._runner_job.results:
            await result_uploader.upload(result)

    async def _get_return_paths(self) -> _cabc.Sequence[str] | None:
        return_paths = await self._executor.run(
            _get_return_paths,
            self._working_dir_path,
            self._runner_job.return_paths_glob_pattern,
        )

        return return_paths
