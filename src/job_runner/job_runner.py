import asyncio as _asyncio
import collections.abc as _cabc
import json as _json
import logging as _log
import pathlib as _pl
import shutil as _su
import typing as _tp

import resultes_pydantic_models.runner as _mrunner

import context as _con

from . import executor as _ex
from . import result_uploader as _ru
from .process import log_forwarder as _lf
from .process import process as _proc
from .process import progress_forwarder as _pf

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

        async for payload in self._run_commands():
            yield payload

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

    async def _run_commands(self) -> _cabc.AsyncIterable[_mrunner.JobSuccessfulPayload]:
        for command_number, command in enumerate(self._runner_job.commands):
            async for payload in self._run_command(command, command_number):
                yield payload

    async def _run_command(
        self,
        command: _mrunner.GeneralCommand | _mrunner.RunTrnsysCommand,
        command_number: int,
    ) -> _cabc.AsyncIterable[_mrunner.JobSuccessfulPayload]:
        match command:
            case _mrunner.GeneralCommand():
                iterable = self._run_general_command(command, command_number)
            case _mrunner.RunTrnsysCommand():
                iterable = self._run_trnsys_command(command, command_number)
            case _:
                _tp.assert_never(_)

        async for payload in iterable:
            yield payload

    async def _run_trnsys_command(
        self, trnsys_command: _mrunner.RunTrnsysCommand, command_number: int
    ) -> _cabc.AsyncIterable[_mrunner.JobSuccessfulPayload]:

        deck_file_path = self._working_dir_path / trnsys_command.relative_dck_file_path

        self._log_info("Running TRNSYS in subprocess with deck file %s", deck_file_path)

        working_dir_path = deck_file_path.parent
        log_file_path = deck_file_path.with_suffix(".log")

        log_forwarder = _lf.LogForwarder(
            self._job_id, command_number, log_file_path, self._executor
        )

        progress_forwarder = self._create_progress_forwarder(
            working_dir_path, trnsys_command, command_number
        )

        process = _proc.Process(
            self._job_id,
            trnsys_command.trnsys_exe_path,
            [str(deck_file_path)],
            working_dir_path,
            run_alongs=[log_forwarder, progress_forwarder],
        )

        async for payload in process.run():
            yield payload

    def _create_progress_forwarder(
        self,
        working_dir_path: _pl.Path,
        trnsys_command: _mrunner.RunTrnsysCommand,
        command_number: int,
    ) -> _pf.ProgressForwarder:
        temperatures_step_prt_file_path = (
            working_dir_path.parent
            / trnsys_command.relative_temperatures_step_prt_file_path
        )

        progress_forwarder = _pf.ProgressForwarder(
            self._job_id,
            command_number,
            trnsys_command.n_total_timesteps,
            temperatures_step_prt_file_path,
            self._executor,
        )

        return progress_forwarder

    async def _run_general_command(
        self, general_command: _mrunner.GeneralCommand, command_number: int
    ) -> _cabc.AsyncIterable[_mrunner.JobSuccessfulPayload]:
        working_dir_path = _pl.Path(
            general_command.program.parent
            if general_command.working_dir is None
            else self._working_dir_path / general_command.working_dir
        )

        self._log_info(
            "Running %s in subprocess with full working dir %s...",
            general_command,
            working_dir_path,
        )

        relative_log_file_path = general_command.relative_log_file_path

        log_file_path = (
            self._working_dir_path / relative_log_file_path
            if relative_log_file_path
            else None
        )

        log_forwarder = _lf.LogForwarder(
            self._job_id, command_number, log_file_path, self._executor
        )
        process = _proc.Process(
            self._job_id,
            general_command.program,
            general_command.args,
            working_dir_path,
            run_alongs=[log_forwarder],
        )

        async for payload in process.run():
            yield payload

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
