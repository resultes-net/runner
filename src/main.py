import asyncio as _asyncio
import concurrent.futures as _cf
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import secrets as _secs
import signal as _sig
import typing as _tp

import jsonrpcserver as _jrpcs
import pydantic as _pyd
import resultes_pydantic_models.pytrnsys as _mpytrnsys
import websockets as _ws
import websockets.asyncio.server as _wsas

import swift_multiprocess as _swmp

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(message)s"


PORT = 3000

LOG_LEVEL = _os.environ.get("LOG_LEVEL", "INFO")

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
        log_file_path, maxBytes=5 * 1024, backupCount=10
    )

    handlers: list[_log.Handler] = [stream_handler, file_handler]

    _log.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL, handlers=handlers)


class Server:
    def __init__(self, port: int, executor: _cf.Executor) -> None:
        self._port = port

        self.executor = executor

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
async def run_python_in_pytrnsys_venv(
    server: Server, runner_job: dict[str, _pyd.JsonValue]
) -> _jrpcs.Result:
    try:
        job = _mpytrnsys.RunnerJob(**runner_job)
    except _pyd.ValidationError as validation_error:
        errors = validation_error.errors()
        return _jrpcs.InvalidParams(errors)

    return await _run_python_in_pytrnsys_venv(server, job)


async def _run_python_in_pytrnsys_venv(
    server: Server, runner_job: _mpytrnsys.RunnerJob
) -> _jrpcs.Result:
    object_storage_path = runner_job.object_storage_path

    output_file_path = (
        _pl.Path(__file__).parent
        / object_storage_path.container
        / object_storage_path.path
    )

    loop = _asyncio.get_event_loop()
    await loop.run_in_executor(
        server.executor,
        _swmp.download_storage_object,
        object_storage_path,
        output_file_path,
    )

    result = [_secs.token_hex(nbytes=6) for _ in range(4)]

    return _jrpcs.Success(result)


if __name__ == "__main__":
    _sig.signal(_sig.SIGINT, _on_ctrl_c)
    _setup_logging()

    with _cf.ProcessPoolExecutor(max_workers=8) as executor:
        server = Server(PORT, executor)
        _asyncio.run(server.serve())
