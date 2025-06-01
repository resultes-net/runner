import asyncio as _asyncio
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import secrets as _secs
import signal as _sig
import typing as _tp

import jsonrpcserver as _jrpcs
import resultes_pydantic_models.simulations.parameters.ttes as _pttes
import websockets as _ws
import websockets.asyncio.server as _wsas

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


_is_first = True


@_jrpcs.method()
async def create_variations(parameters: _pttes.TtesParameters) -> _jrpcs.Result:
    global _is_first

    seconds_to_sleep = 60 if _is_first else 0
    if _is_first:
        _is_first = False

    await _asyncio.sleep(seconds_to_sleep)

    result = [_secs.token_hex(nbytes=6) for _ in range(4)]

    return _jrpcs.Success(result)


class _TerminateTaskGroupException(Exception):
    pass


async def _terminate_task_group() -> _tp.NoReturn:
    raise _TerminateTaskGroupException()


async def _terminate_task_group_on_shutdown_event() -> _tp.NoReturn:
    await _shutdown_event.wait()
    raise _TerminateTaskGroupException()


async def _handle_connection(server_connection: _wsas.ServerConnection) -> None:
    _log.info("Client %s connected.", server_connection.id)

    try:
        async with _asyncio.TaskGroup() as task_group:
            task_group.create_task(_terminate_task_group_on_shutdown_event())

            try:
                while not _shutdown_event.is_set():
                    data = await server_connection.recv(decode=True)
                    await _handle_request(data, server_connection, task_group)

            except _ws.ConnectionClosedOK:
                _log.info("Client %s disconnected.", server_connection.id)
                await task_group.create_task(_terminate_task_group())

    except* _TerminateTaskGroupException:
        pass


_tasks = set[_asyncio.Task[None]]()


def _on_task_done(task: _asyncio.Task[None]) -> None:
    _log.debug("Task %s is done.", task.get_name())


async def _handle_request(
    data: str, server_connection: _wsas.ServerConnection, task_group: _asyncio.TaskGroup
) -> None:
    coroutine = _dispatch_request(data, server_connection)
    task = task_group.create_task(coroutine)
    _log.debug("Task %s has been started.", task.get_name())
    task.add_done_callback(_on_task_done)


async def _dispatch_request(
    data: str, server_connection: _wsas.ServerConnection
) -> None:
    if result := await _jrpcs.async_dispatch(data):
        await server_connection.send(result)


async def _server() -> None:
    async with _ws.serve(_handle_connection, port=PORT):
        await _shutdown_event.wait()


def _setup_logging() -> None:
    stream_handler = _log.StreamHandler()

    log_file_path = _pl.Path(__file__).parent / "runner.log"
    file_handler = _handlers.RotatingFileHandler(
        log_file_path, maxBytes=5 * 1024, backupCount=10
    )

    handlers: list[_log.Handler] = [stream_handler, file_handler]

    _log.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL, handlers=handlers)


if __name__ == "__main__":
    _sig.signal(_sig.SIGINT, _on_ctrl_c)
    _setup_logging()
    _asyncio.run(_server())
