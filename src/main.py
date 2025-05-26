import asyncio as _asyncio
import logging as _log
import os as _os
import secrets as _secs
import signal as _sig
import socket as _soc

import jsonrpcserver as _jrpcs
import resultes_pydantic_models.simulations.parameters.ttes as _pttes
import websockets as _ws
import websockets.asyncio.server as _wsas

TERMINATION_TIMEOUT_SECONDS = 15

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(message)s"


def _get_host() -> str:
    dev_host = f"{_soc.gethostname()}.local"
    host = _os.environ.get("HOST", dev_host)
    return host


HOST = _get_host()
PORT = 3000

LOG_LEVEL = _os.environ.get("LOG_LEVEL", "DEBUG")

_shutdown_event = _asyncio.Event()


def _on_sigterm(signal, stack_frame) -> None:
    _log.info("Received SIGTERM. Shutting down.")
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



async def _handle_connection(server_connection: _wsas.ServerConnection) -> None:
    _log.info("Client %s connected.", server_connection.id)

    try:
        while True:
            data = await server_connection.recv(decode=True)
            await _handle_request(data, server_connection)
    except _ws.ConnectionClosedOK:
        _log.info("Client %s disconnected.", server_connection.id)
        pass


_tasks = set[_asyncio.Task[None]]()


def _on_task_done(task: _asyncio.Task[None]) -> None:
    _log.debug("Task %s is done.", task.get_name())
    _tasks.remove(task)


async def _handle_request(data: str, server_connection: _wsas.ServerConnection) -> None:
    coroutine = _dispatch_request(data, server_connection)
    task = _asyncio.create_task(coroutine)
    _log.debug("Task %s has been started.", task.get_name())
    _tasks.add(task)
    task.add_done_callback(_on_task_done)


async def _dispatch_request(
    data: str, server_connection: _wsas.ServerConnection
) -> None:
    if result := await _jrpcs.async_dispatch(data):
        await server_connection.send(result)


async def _server() -> None:
    async with _ws.serve(_handle_connection, HOST, PORT):
        await _shutdown_event.wait()

        _log.info("Received shutdown event")

        if _tasks:
            _log.info("Cancelling %d task(s).", len(_tasks))

            for task in _tasks:
                _log.debug("Cancelling task %s.", task.get_name())
                task.cancel()

            _log.info("Sent cancellation to tasks. Waiting for termination...")

            _, incomplete_tasks = await _asyncio.wait(
                _tasks, timeout=TERMINATION_TIMEOUT_SECONDS
            )

            if incomplete_tasks:
                _log.warning(
                    "%d task(s) did not terminate after %f second(s).",
                    len(incomplete_tasks),
                    TERMINATION_TIMEOUT_SECONDS,
                )
                formatted_incomplete_task_names = ", ".join(
                    t.get_name() for t in incomplete_tasks
                )

                _log.debug(
                    "The following tasks did not terminate: %s.",
                    formatted_incomplete_task_names,
                )


if __name__ == "__main__":
    _sig.signal(_sig.SIGTERM, _on_sigterm)
    _log.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)
    _asyncio.run(_server())
