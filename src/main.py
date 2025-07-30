import asyncio as _asyncio
import concurrent.futures as _cf
import dataclasses as _dc
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import shutil as _su
import signal as _sig
import typing as _tp

import resultes_jsonrpc.jsonrpc.client as _rjjc
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.websockets.server as _rjws

# This module needs to be imported to define the JSON-RPC methods
import jsonrpc_logging as _jrpcl
import log_config as _logc
import context as _con
import swift_multithreaded as _swmt

PORT = 3000

MAX_WORKERS = 8

LOG_LEVEL = _os.environ.get("LOG_LEVEL", "INFO")


_JOBS_DIR_PATH = _pl.Path(__file__).parents[1] / "jobs"


def _setup_logging() -> None:
    stream_handler = _log.StreamHandler()

    log_file_path = _pl.Path(__file__).parent / "runner.log"
    file_handler = _handlers.RotatingFileHandler(
        log_file_path, maxBytes=5 * 1024 * 1024, backupCount=10
    )

    handlers: list[_log.Handler] = [stream_handler, file_handler]

    _log.basicConfig(format=_logc.LOG_FORMAT, level=LOG_LEVEL, handlers=handlers)


_was_ctrl_c_seen_before = False
_websocket_server: _rjws.Server


def _on_ctrl_c(signal, stack_frame) -> None:
    global _was_ctrl_c_seen_before

    if _was_ctrl_c_seen_before:
        _log.info("Received Ctrl-C second time: raising keyboard interrupt.")
        raise KeyboardInterrupt()

    _log.info("Received Ctrl-C first time.")
    _was_ctrl_c_seen_before = True

    if _websocket_server:
        _websocket_server.stop()


class _Stoppable(_tp.Protocol):
    def stop(self) -> None: ...


@_dc.dataclass
class _StoppableWithTask:
    stoppable: _Stoppable
    task: _asyncio.Task[None]

    async def stop(self) -> None:
        self.stoppable.stop()
        await self.task


async def main() -> None:
    global _websocket_server

    paths = ["/requests", "/logging"]
    async with _rjws.Server.start(PORT, paths) as websocket_server:
        async with _rjjs.TaskSpawningDispatcher() as dispatcher:
            with _cf.ThreadPoolExecutor(MAX_WORKERS) as executor:
                async with _swmt.Swift(executor, MAX_WORKERS) as swift:
                    context = _con.Context(_JOBS_DIR_PATH, swift, executor)

                    _websocket_server = websocket_server

                    requests_server: _StoppableWithTask | None = None
                    logging_client: _StoppableWithTask | None = None
                    logging_handler: _StoppableWithTask | None = None

                    try:
                        async for websocket in websocket_server.websockets():

                            if websocket.path == "/requests":
                                if requests_server:
                                    raise RuntimeError(
                                        "Requests are already connected."
                                    )

                                jsonrpc_server = _rjjs.JsonRpcServer(
                                    websocket.websocket,
                                    dispatcher,
                                    message_dispatch_context=context,
                                )
                                coroutine = jsonrpc_server.start()
                                task = _asyncio.create_task(coroutine)
                                requests_server = _StoppableWithTask(
                                    jsonrpc_server, task
                                )

                            elif websocket.path == "/logging":
                                if logging_client:
                                    raise RuntimeError("Logging is already connected.")

                                jsonrpc_client = _rjjc.JsonRpcClient(
                                    websocket.websocket
                                )
                                coroutine = jsonrpc_client.start()
                                task = _asyncio.create_task(coroutine)
                                logging_client = _StoppableWithTask(
                                    jsonrpc_client, task
                                )

                                jsonrcp_log_handler = _jrpcl.JsonRpcLogHandler(
                                    jsonrpc_client, _log.INFO
                                )

                                root_logger = _log.getLogger()
                                root_logger.addHandler(jsonrcp_log_handler)

                                coroutine = jsonrcp_log_handler.start()
                                task = _asyncio.create_task(coroutine)
                                logging_handler = _StoppableWithTask(
                                    jsonrcp_log_handler, task
                                )

                    finally:

                        if requests_server:
                            await requests_server.stop()

                        if logging_client:
                            await logging_client.stop()

                        if logging_handler:
                            await logging_handler.stop()


if __name__ == "__main__":
    _setup_logging()

    _sig.signal(_sig.SIGINT, _on_ctrl_c)

    if _JOBS_DIR_PATH.exists():
        _su.rmtree(_JOBS_DIR_PATH)

    _asyncio.run(main())
