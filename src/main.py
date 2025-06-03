import asyncio as _asyncio
import collections.abc as _cabc
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import shutil as _su
import signal as _sig
import subprocess as _sp
import sys as _sys
import typing as _tp

import jsonrpcserver as _jrpcs
import jsonrpcserver.codes as _jrpcc
import pydantic as _pyd
import resultes_pydantic_models.pytrnsys as _mpytrnsys
import websockets as _ws
import websockets.asyncio.server as _wsas

import swift_multiprocess as _swmp

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(message)s"


PORT = 3000

LOG_LEVEL = _os.environ.get("LOG_LEVEL", "INFO")


_JOBS_DIR_PATH = _pl.Path(__file__).parents[1] / "jobs"

_shutdown_event = _asyncio.Event()


def _on_ctrl_c(signal, stack_frame) -> None:
    if _shutdown_event.is_set():
        _log.info("Received Ctrl-C second time: raising keyboard interrupt.")
        raise KeyboardInterrupt()

    _log.info("Received Ctrl-C first time.")
    _shutdown_event.set()


class _TerminateTaskGroupException(Exception):
    pass


def _setup_logging() -> None:
    stream_handler = _log.StreamHandler()

    log_file_path = _pl.Path(__file__).parent / "runner.log"
    file_handler = _handlers.RotatingFileHandler(
        log_file_path, maxBytes=5 * 1024 * 1024, backupCount=10
    )

    handlers: list[_log.Handler] = [stream_handler, file_handler]

    _log.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL, handlers=handlers)


class _Server:
    def __init__(self, port: int, swift: _swmp.Swift) -> None:
        self._port = port
        self.swift = swift
        self._tasks = set[_asyncio.Task[None]]()

    async def _terminate_task_group(self) -> _tp.NoReturn:
        raise _TerminateTaskGroupException()

    async def _terminate_task_group_on_shutdown_event(self) -> _tp.NoReturn:
        await _shutdown_event.wait()
        raise _TerminateTaskGroupException()

    async def _handle_connection(
        self, server_connection: _wsas.ServerConnection
    ) -> None:
        _log.info("Client %s connected.", server_connection.id)

        try:
            async with _asyncio.TaskGroup() as task_group:
                task_group.create_task(self._terminate_task_group_on_shutdown_event())

                try:
                    while not _shutdown_event.is_set():
                        data = await server_connection.recv(decode=True)
                        await self._handle_request(data, server_connection, task_group)

                except _ws.ConnectionClosedOK:
                    _log.info("Client %s disconnected.", server_connection.id)
                    await task_group.create_task(self._terminate_task_group())

        except* _TerminateTaskGroupException:
            pass

    def _on_task_done(self, task: _asyncio.Task[None]) -> None:
        _log.debug("Task %s is done.", task.get_name())

    async def _handle_request(
        self,
        data: str,
        server_connection: _wsas.ServerConnection,
        task_group: _asyncio.TaskGroup,
    ) -> None:
        coroutine = self._dispatch_request(data, server_connection)
        task = task_group.create_task(coroutine)
        _log.debug("Task %s has been started.", task.get_name())
        task.add_done_callback(self._on_task_done)

    async def _dispatch_request(
        self, data: str, server_connection: _wsas.ServerConnection
    ) -> None:
        if result := await _jrpcs.async_dispatch(data, context=self):
            await server_connection.send(result)

    async def serve(self) -> None:
        async with _ws.serve(self._handle_connection, port=self._port):
            await _shutdown_event.wait()


@_jrpcs.method()
async def run_python_script_in_pytrnsys_venv(
    server: _Server, runner_job: dict[str, _pyd.JsonValue]
) -> _jrpcs.Result:
    try:
        job = _mpytrnsys.RunnerJob(**runner_job)
    except _pyd.ValidationError as validation_error:
        errors = validation_error.errors()
        return _jrpcs.InvalidParams(errors)

    return await _run_python_script_in_pytrnsys_venv(server, job)


async def _run_python_script_in_pytrnsys_venv(
    server: _Server, runner_job: _mpytrnsys.RunnerJob
) -> _jrpcs.Result:
    object_storage_path = runner_job.object_storage_path

    job_dir_path = _JOBS_DIR_PATH / runner_job.id

    job_dir_exists = await _asyncio.to_thread(job_dir_path.exists)
    if job_dir_exists:
        return _jrpcs.Error(
            code=_jrpcc.ERROR_SERVER_ERROR,
            message=f"Have seen job ID {runner_job.id} before. Job IDs must be unique, forever.",
        )

    output_file_name = object_storage_path.path.split("/")[-1]

    output_file_path = job_dir_path / output_file_name

    await server.swift.download(object_storage_path, output_file_path)

    output_dir_path = job_dir_path / output_file_path.stem
    await _asyncio.to_thread(output_dir_path.mkdir)

    await _asyncio.to_thread(_unzip, output_file_path, output_dir_path)

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
        await _asyncio.to_thread(_su.rmtree, job_dir_path)

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
    await _asyncio.to_thread(_zip_dir, output_dir_path, result_file_path)

    result_object_storage_path = _mpytrnsys.ObjectStorageZipPath(
        container="resultes",
        path=f"results/{result_file_name}",
    )
    await server.swift.upload(result_file_path, result_object_storage_path)

    results_dirs = await _asyncio.to_thread(
        _get_result_paths, output_dir_path, runner_job.results_glob_pattern
    )

    await _asyncio.to_thread(_su.rmtree, job_dir_path)

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
    async with _swmp.Swift(n_processes=8) as swift:
        server = _Server(PORT, swift)
        await server.serve()


if __name__ == "__main__":
    _sig.signal(_sig.SIGINT, _on_ctrl_c)
    _setup_logging()

    if _JOBS_DIR_PATH.exists():
        _su.rmtree(_JOBS_DIR_PATH)

    _asyncio.run(main())
