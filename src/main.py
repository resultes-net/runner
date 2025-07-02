import asyncio as _asyncio
import collections.abc as _cabc
import concurrent.futures as _cf
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import shutil as _su
import signal as _sig
import subprocess as _sp
import sys as _sys

import jsonrpcserver as _jrpcs
import jsonrpcserver.codes as _jrpcc
import pydantic as _pyd
import resultes_pydantic_models.pytrnsys as _mpytrnsys

import server as _srv
import swift_multithreaded as _swmt

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(message)s"


PORT = 3000

MAX_WORKERS = 8

LOG_LEVEL = _os.environ.get("LOG_LEVEL", "INFO")


_JOBS_DIR_PATH = _pl.Path(__file__).parents[1] / "jobs"

_shutdown_event = _asyncio.Event()


def _on_ctrl_c(signal, stack_frame) -> None:
    if _shutdown_event.is_set():
        _log.info("Received Ctrl-C second time: raising keyboard interrupt.")
        raise KeyboardInterrupt()

    _log.info("Received Ctrl-C first time.")
    _shutdown_event.set()


def _setup_logging() -> None:
    stream_handler = _log.StreamHandler()

    log_file_path = _pl.Path(__file__).parent / "runner.log"
    file_handler = _handlers.RotatingFileHandler(
        log_file_path, maxBytes=5 * 1024 * 1024, backupCount=10
    )

    handlers: list[_log.Handler] = [stream_handler, file_handler]

    _log.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL, handlers=handlers)


@_jrpcs.method()
async def run_python_script_in_pytrnsys_venv(
    server: _srv.Server, runner_job: dict[str, _pyd.JsonValue]
) -> _jrpcs.Result:
    try:
        job = _mpytrnsys.RunnerJob(**runner_job)
    except _pyd.ValidationError as validation_error:
        errors = validation_error.errors()
        return _jrpcs.InvalidParams(errors)

    return await _run_python_script_in_pytrnsys_venv(server, job)


async def _run_python_script_in_pytrnsys_venv(
    server: _srv.Server, runner_job: _mpytrnsys.RunnerJob
) -> _jrpcs.Result:
    object_storage_path = runner_job.object_storage_path

    job_dir_path = _JOBS_DIR_PATH / runner_job.id

    loop = _asyncio.get_event_loop()

    job_dir_exists = await loop.run_in_executor(server.executor, job_dir_path.exists)
    if job_dir_exists:
        return _jrpcs.Error(
            code=_jrpcc.ERROR_SERVER_ERROR,
            message=f"Have seen job ID {runner_job.id} before. Job IDs must be unique, forever.",
        )

    output_file_name = object_storage_path.path.split("/")[-1]

    output_file_path = job_dir_path / output_file_name

    await server.swift.download(object_storage_path, output_file_path)

    output_dir_path = job_dir_path / output_file_path.stem
    await loop.run_in_executor(server.executor, output_dir_path.mkdir)

    await loop.run_in_executor(server.executor, _unzip, output_file_path, output_dir_path)

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
        await loop.run_in_executor(server.executor, _su.rmtree, job_dir_path)

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
    result_file_path = job_dir_path / result_file_name
    await loop.run_in_executor(server.executor, _zip_dir, output_dir_path, result_file_path)

    result_object_storage_path = _mpytrnsys.ObjectStorageZipPath(
        container="resultes-results",
        path=f"results/{result_file_name}",
    )
    await server.swift.upload(result_file_path, result_object_storage_path)

    results_dirs = await loop.run_in_executor(server.executor, 
        _get_result_paths, output_dir_path, runner_job.results_glob_pattern
    )

    await loop.run_in_executor(server.executor, _su.rmtree, job_dir_path)

    if results_dirs is not None:
        return _jrpcs.Success(results_dirs)

    return _jrpcs.Success()


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


async def main() -> None:
    with _cf.ThreadPoolExecutor(MAX_WORKERS) as executor:
        async with _swmt.Swift(executor, MAX_WORKERS) as swift:
            server = _srv.Server(PORT, swift, executor, _shutdown_event)
            await server.serve()


if __name__ == "__main__":
    _sig.signal(_sig.SIGINT, _on_ctrl_c)
    _setup_logging()

    if _JOBS_DIR_PATH.exists():
        _su.rmtree(_JOBS_DIR_PATH)

    _asyncio.run(main())
