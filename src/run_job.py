import asyncio as _asyncio
import asyncio.subprocess as _asp
import collections.abc as _cabc
import logging as _log
import pathlib as _pl
import shutil as _su
import subprocess as _sp

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


async def run_job(
    context: _con.Context, runner_job: _mrunner.RunnerJob
) -> _jrpcs.Result:
    _LOGGER.info("Running job %s.", runner_job)

    object_storage_path = runner_job.object_storage_path

    job_dir_path = context.jobs_dir_path / runner_job.id

    loop = _asyncio.get_event_loop()

    job_dir_exists = await loop.run_in_executor(context.executor, job_dir_path.exists)
    if job_dir_exists:
        return _jrpcs.Error(
            code=_jrpcc.ERROR_SERVER_ERROR,
            message=f"Have seen job ID {runner_job.id} before. Job IDs must be unique, forever.",
        )

    output_file_name = object_storage_path.path.split("/")[-1]

    output_file_path = job_dir_path / output_file_name

    await context.swift.download(object_storage_path, output_file_path)

    output_dir_path = job_dir_path / output_file_path.stem
    await loop.run_in_executor(context.executor, output_dir_path.mkdir)

    await loop.run_in_executor(
        context.executor, _unzip, output_file_path, output_dir_path
    )

    working_dir_path = (
        runner_job.program.parent
        if runner_job.working_dir is None
        else output_dir_path / runner_job.working_dir
    )

    _LOGGER.info("Running %s in subprocess...", runner_job)

    process = await _asyncio.create_subprocess_exec(
        runner_job.program, *runner_job.args, cwd=working_dir_path, stderr=_sp.PIPE
    )

    return_code = await _wait_for_subprocess(
        process, output_dir_path, runner_job.relative_log_file_path
    )

    _LOGGER.info("Done.")

    if return_code != 0:
        await loop.run_in_executor(context.executor, _su.rmtree, job_dir_path)

        assert process.stderr
        stderr_bytes = await process.stderr.read()
        stderr = stderr_bytes.decode()

        _LOGGER.warning(
            "An error occurred running job %s: %s",
            runner_job.id,
            stderr,
        )

        return _jrpcs.Error(
            code=_jrpcc.ERROR_SERVER_ERROR,
            message=f"Job program exited with non-zero exit code: {stderr}",
        )

    result_file_name = f"{runner_job.id}.zip"
    result_file_path = job_dir_path / result_file_name
    await loop.run_in_executor(
        context.executor, _zip_dir, output_dir_path, result_file_path
    )

    result_object_storage_path = _mrunner.ObjectStorageZipPath(
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

    await loop.run_in_executor(context.executor, _su.rmtree, job_dir_path)

    if results_dirs is not None:
        return _jrpcs.Success(results_dirs)

    return _jrpcs.Success()


async def _wait_for_subprocess(
    process: _asp.Process,
    output_dir_path: _pl.Path,
    relative_log_file_path: _pl.PureWindowsPath | None,
) -> int:
    if not relative_log_file_path:
        return await process.wait()

    return await _wait_for_subprocess_and_forward_logging(
        process, output_dir_path, relative_log_file_path
    )


async def _wait_for_subprocess_and_forward_logging(
    process: _asp.Process,
    output_dir_path: _pl.Path,
    relative_log_file_path: _pl.PureWindowsPath,
) -> int:
    log_file_path = output_dir_path / relative_log_file_path

    await _wait_till_file_exists(log_file_path)

    coroutine = process.wait()

    line_builder = _lb.LineBuilder()
    with log_file_path.open("rt") as log_file:
        return_code = None
        while return_code is None:
            timeout_seconds = 3
            try:
                return_code = await _asyncio.wait_for(coroutine, timeout_seconds)
            except _asyncio.TimeoutError:
                pass

            bytes = await _asyncio.to_thread(log_file.read)

            new_lines = line_builder.add_bytes_and_get_new_lines(bytes)
            for new_line in new_lines:
                _LOGGER.info("%s: %s", log_file_path.name, new_line)

        return return_code


async def _wait_till_file_exists(log_file_path: _pl.Path) -> None:
    log_file_creation_timeout_seconds = 10
    async with _asyncio.timeout(log_file_creation_timeout_seconds):
        while True:
            if await _asyncio.to_thread(log_file_path.is_file):
                break
