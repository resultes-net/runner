import asyncio as _asyncio
import collections.abc as _cabc
import logging as _log
import pathlib as _pl
import shutil as _su
import subprocess as _sp
import sys as _sys

import jsonrpcserver as _jrpcs
import jsonrpcserver.codes as _jrpcc
import resultes_pydantic_models.pytrnsys as _mpytrnsys

import context as _con

_LOGGER = _log.getLogger(__name__)


def _unzip(input_file_path: _pl.Path, output_dir_path: _pl.Path) -> None:
    _su.unpack_archive(input_file_path, output_dir_path)


def _zip_dir(input_dir_path: _pl.Path, output_file_path: _pl.Path) -> None:
    base_name = str(output_file_path.with_suffix(""))
    format = output_file_path.suffix.removeprefix(".")
    root_dir = input_dir_path

    _su.make_archive(base_name, format, root_dir)


def _get_result_paths(
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


async def run_python_script_in_pytrnsys_venv(
    context: _con.Context, runner_job: _mpytrnsys.RunnerJob
) -> _jrpcs.Result:
    _LOGGER.info("Running job %s.", runner_job.id)

    object_storage_path = runner_job.object_storage_path

    jobs_dir_path = context.jobs_dir_path / runner_job.id

    loop = _asyncio.get_event_loop()

    job_dir_exists = await loop.run_in_executor(context.executor, jobs_dir_path.exists)
    if job_dir_exists:
        return _jrpcs.Error(
            code=_jrpcc.ERROR_SERVER_ERROR,
            message=f"Have seen job ID {runner_job.id} before. Job IDs must be unique, forever.",
        )

    output_file_name = object_storage_path.path.split("/")[-1]

    output_file_path = jobs_dir_path / output_file_name

    await context.swift.download(object_storage_path, output_file_path)

    output_dir_path = jobs_dir_path / output_file_path.stem
    await loop.run_in_executor(context.executor, output_dir_path.mkdir)

    await loop.run_in_executor(
        context.executor, _unzip, output_file_path, output_dir_path
    )

    script_file_path = output_dir_path / runner_job.script_to_run
    working_dir_path = (
        script_file_path.parent
        if runner_job.working_dir is None
        else output_dir_path / runner_job.working_dir
    )

    process = await _asyncio.create_subprocess_exec(
        _sys.executable, script_file_path, cwd=working_dir_path, stderr=_sp.PIPE
    )

    return_code = await process.wait()
    if return_code != 0:
        await loop.run_in_executor(context.executor, _su.rmtree, jobs_dir_path)

        assert process.stderr
        stderr_bytes = await process.stderr.read()
        stderr = stderr_bytes.decode()

        _log.warning(
            "An error ocurred running client provided script for request %s: %s",
            runner_job.id,
            stderr,
        )

        return _jrpcs.Error(
            code=_jrpcc.ERROR_SERVER_ERROR,
            message=f"Script exited with non-zero exit code: {stderr}",
        )

    result_file_name = f"{runner_job.id}.zip"
    result_file_path = jobs_dir_path / result_file_name
    await loop.run_in_executor(
        context.executor, _zip_dir, output_dir_path, result_file_path
    )

    result_object_storage_path = _mpytrnsys.ObjectStorageZipPath(
        container="resultes-results",
        path=f"results/{result_file_name}",
    )
    await context.swift.upload(result_file_path, result_object_storage_path)

    results_dirs = await loop.run_in_executor(
        context.executor,
        _get_result_paths,
        output_dir_path,
        runner_job.results_glob_pattern,
    )

    await loop.run_in_executor(context.executor, _su.rmtree, jobs_dir_path)

    if results_dirs is not None:
        return _jrpcs.Success(results_dirs)

    return _jrpcs.Success()
