import asyncio as _asyncio
import collections.abc as _cabc
import concurrent.futures as _cf
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import shutil as _su
import signal as _sig
import typing as _tp

import resultes_jsonrpc.websockets.server as _rjws

import context as _con
# This module needs to be imported to define the JSON-RPC methods
import jrpcs_methods as _jrpcm
import log_config as _logc
import message_receiver_factories as _facs
import swift_multithreaded as _swmt

PORT = 3000

MAX_WORKERS = 8

N_THREADS = 32

LOG_LEVEL = _os.environ.get("LOG_LEVEL", "INFO")

DEFAULT_LOG_FILE_PATH = _pl.Path(__file__).parent / "runner.log"

LOG_FILE_PATH = _pl.Path(_os.environ.get("LOG_FILE_PATH", DEFAULT_LOG_FILE_PATH))


DEFAULT_JOBS_DIR_PATH = _pl.Path(__file__).parents[1] / "jobs"

JOBS_DIR_PATH = _pl.Path(_os.environ.get("JOBS_DIR_PATH", DEFAULT_JOBS_DIR_PATH))

_LOGGER = _log.getLogger(__name__)


class _CancelTaskGroupException(Exception):
    pass


async def _cancel_task_group() -> _tp.NoReturn:
    raise _CancelTaskGroupException()


_shutdown_event = _asyncio.Event()


def _on_ctrl_c(signal, stack_frame) -> None:
    if _shutdown_event.is_set():
        _log.info("Received Ctrl-C second time: raising keyboard interrupt.")
        raise KeyboardInterrupt()

    _LOGGER.info("Received Ctrl-C first time.")
    _shutdown_event.set()


async def main() -> None:
    _LOGGER.info("Log file path: %s", LOG_FILE_PATH)
    _LOGGER.info("Jobs dir path: %s", JOBS_DIR_PATH)

    with _cf.ThreadPoolExecutor(N_THREADS) as executor:
        async with _swmt.Swift(executor, MAX_WORKERS) as swift:
            try:
                async with _asyncio.TaskGroup() as task_group:
                    context = _con.Context(JOBS_DIR_PATH, swift, executor)

                    logging_message_receiver_factory = (
                        _facs.LoggingMessageReceiverSingletonFactory(executor)
                    )

                    request_receiver_factory = _facs.RequestReceiverSingletonFactory(
                        task_group, context
                    )

                    message_receiver_factories: _cabc.Mapping[
                        str, _rjws.MessageReceiverFactory
                    ] = {
                        "/requests": request_receiver_factory,
                        "/logging": logging_message_receiver_factory,
                    }

                    server = _rjws.Server(PORT, message_receiver_factories)

                    async with server.run(), logging_message_receiver_factory.run():
                        await _shutdown_event.wait()
                        await task_group.create_task(_cancel_task_group())

            except* _CancelTaskGroupException:
                pass


def _setup_logging() -> None:
    stream_handler = _log.StreamHandler()

    file_handler = _handlers.RotatingFileHandler(
        LOG_FILE_PATH, maxBytes=5 * 1024 * 1024, backupCount=10
    )

    handlers: list[_log.Handler] = [stream_handler, file_handler]

    _log.basicConfig(format=_logc.LOG_FORMAT, level=LOG_LEVEL, handlers=handlers)


if __name__ == "__main__":
    _jrpcm.configure()

    _setup_logging()

    _sig.signal(_sig.SIGINT, _on_ctrl_c)

    if JOBS_DIR_PATH.exists():
        _su.rmtree(JOBS_DIR_PATH)

    _asyncio.run(main())
