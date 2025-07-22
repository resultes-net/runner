import asyncio as _asyncio
import concurrent.futures as _cf
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import shutil as _su
import signal as _sig


# This module needs to be imported to define the JSON-RPC methods
import jrpcs_methods as _jrpcsm  # type: ignore

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


async def main() -> None:
    with _cf.ThreadPoolExecutor(MAX_WORKERS) as executor:
        async with _swmt.Swift(executor, MAX_WORKERS) as swift:
            server = _srv.Server(PORT, _JOBS_DIR_PATH, swift, executor, _shutdown_event)
            await server.serve()


if __name__ == "__main__":
    _sig.signal(_sig.SIGINT, _on_ctrl_c)
    _setup_logging()

    if _JOBS_DIR_PATH.exists():
        _su.rmtree(_JOBS_DIR_PATH)

    _asyncio.run(main())
