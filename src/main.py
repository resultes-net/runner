import asyncio as _asyncio
import collections.abc as _cabc
import concurrent.futures as _cf
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import signal as _sig
import typing as _tp

import resultes_jsonrpc.websockets.server as _rjws
import resultes_openstack_utils.swift_multithreaded as _swmt

import context as _con
import job_runner.job_runner as _jr
# This module needs to be imported to define the JSON-RPC methods
import jrpcs_methods as _jrpcm
import log_config as _logc
import message_receiver_factory as _facs

PORT = 3000

MAX_WORKERS = 8

N_THREADS = 32

DEFAULT_LOG_FILE_PATH = _pl.Path(__file__).parent / "runner.log"

LOG_FILE_PATH = _pl.Path(_os.environ.get("LOG_FILE_PATH", DEFAULT_LOG_FILE_PATH))


DEFAULT_JOBS_DIR_PATH = _pl.Path(__file__).parents[1] / "jobs"

JOBS_DIR_PATH = _pl.Path(_os.environ.get("JOBS_DIR_PATH", DEFAULT_JOBS_DIR_PATH))

CLOUDS_YAML_FILE_PATH = _pl.Path(
    _pl.Path(__file__).parents[1] / "config" / "clouds.yaml"
)


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
        async with _swmt.Swift(CLOUDS_YAML_FILE_PATH, executor, MAX_WORKERS) as swift:
            try:
                async with _asyncio.TaskGroup() as task_group:
                    shall_remove_completed_jobs = True
                    job_runner_config = _jr.Config(
                        JOBS_DIR_PATH, executor, swift, shall_remove_completed_jobs
                    )

                    context = _con.Context(task_group, job_runner_config)

                    message_receiver_singleton_factory = (
                        _facs.MessageReceiverSingletonFactory(
                            task_group, context, executor
                        )
                    )

                    message_receiver_factories: _cabc.Mapping[
                        str, _rjws.MessageReceiverFactory
                    ] = {
                        "/jsonrpc": message_receiver_singleton_factory,
                    }

                    server = _rjws.Server(PORT, message_receiver_factories)

                    async with server.run(), message_receiver_singleton_factory.run():
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

    _log.basicConfig(format=_logc.LOG_FORMAT, level=_log.INFO, handlers=handlers)


if __name__ == "__main__":
    _jrpcm.configure()

    _setup_logging()

    _sig.signal(_sig.SIGINT, _on_ctrl_c)

    if not JOBS_DIR_PATH.is_dir():
        JOBS_DIR_PATH.mkdir()

    _LOGGER.info("Running in directory %s.", _pl.Path().absolute())

    _asyncio.run(main())
