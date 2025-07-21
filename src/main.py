import asyncio as _asyncio
import concurrent.futures as _cf
import logging as _log
import logging.handlers as _handlers
import os as _os
import pathlib as _pl
import shutil as _su
import signal as _sig

import jsonrpcserver as _jrpcs
import loki_logger_handler.loki_logger_handler as _llh
import pydantic as _pyd
import resultes_pydantic_models.pytrnsys as _mpytrnsys

import run_python_script_in_pytrnsys_venv as _rps
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

    return await _rps.run_python_script_in_pytrnsys_venv(server, job)


@_jrpcs.method()
async def set_loki_ip_address(loki_ip_address: str) -> _jrpcs.Result:
    logger = _log.getLogger()

    existing_loki_log_handlers = [
        h for h in logger.handlers if isinstance(h, _llh.LokiLoggerHandler)
    ]
    if existing_loki_log_handlers:
        return _jrpcs.Error(
            -32000,
            "Loki IP address already set.",
            "The Loki IP address can only be set once.",
        )

    url = f"{loki_ip_address}:80/loki/api/v1/push"

    loki_log_handler = _llh.LokiLoggerHandler(
        url=url,
        labels={"application": "Test", "environment": "Develop"},
        label_keys={},
        timeout=10,
    )

    logger.addHandler(loki_log_handler)

    return _jrpcs.Success()


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
