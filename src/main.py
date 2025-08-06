import asyncio as _asyncio
import collections.abc as _cabc
import concurrent.futures as _cf
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import shutil as _su
import signal as _sig
import sys as _sys

import resultes_jsonrpc.websockets.server as _rjws

import context as _con

# This module needs to be imported to define the JSON-RPC methods
import jrpcs_methods as _jrpcm
import log_config as _logc
import message_receiver_factories as _facs
import swift_multithreaded as _swmt

PORT = 3000

MAX_WORKERS = 8

LOG_LEVEL = _os.environ.get("LOG_LEVEL", "INFO")


_JOBS_DIR_PATH = _pl.Path(__file__).parents[1] / "jobs"

_LOGGER = _log.getLogger(__name__)


_shutdown_event = _asyncio.Event()


def _on_ctrl_c(signal, stack_frame) -> None:
    if _shutdown_event.is_set():
        _log.info("Received Ctrl-C second time: raising keyboard interrupt.")
        raise KeyboardInterrupt()

    _log.info("Received Ctrl-C first time.")
    _shutdown_event.set()


async def main() -> None:
    with _cf.ThreadPoolExecutor(MAX_WORKERS) as executor:
        async with _swmt.Swift(executor, MAX_WORKERS) as swift:
            context = _con.Context(_JOBS_DIR_PATH, swift, executor)

            logging_message_receiver_factory = (
                _facs.LoggingMessageReceiverSingletonFactory()
            )

            request_receiver_factory = _facs.RequestReceiverSingletonFactory(context)

            message_receiver_factories: _cabc.Mapping[
                str, _rjws.MessageReceiverFactory
            ] = {
                "/requests": request_receiver_factory,
                "/logging": logging_message_receiver_factory,
            }

            server = _rjws.Server(PORT, message_receiver_factories)

            async with server.run(), logging_message_receiver_factory.run():
                await _shutdown_event.wait()
                await request_receiver_factory.cancel_and_join_requests()


def _setup_logging() -> None:
    stream_handler = _log.StreamHandler()

    log_file_path = _pl.Path(__file__).parent / "runner.log"
    file_handler = _handlers.RotatingFileHandler(
        log_file_path, maxBytes=5 * 1024 * 1024, backupCount=10
    )

    handlers: list[_log.Handler] = [stream_handler, file_handler]

    _log.basicConfig(format=_logc.LOG_FORMAT, level=LOG_LEVEL, handlers=handlers)


if __name__ == "__main__":
    _jrpcm.configure()

    _setup_logging()

    _sig.signal(_sig.SIGINT, _on_ctrl_c)

    _LOGGER.info("Python executable is at %s.", _sys.executable)

    if _JOBS_DIR_PATH.exists():
        _su.rmtree(_JOBS_DIR_PATH)

    _asyncio.run(main())
