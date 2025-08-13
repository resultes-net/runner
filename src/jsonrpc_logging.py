import asyncio as _asyncio
import concurrent.futures as _cf
import dataclasses as _dc
import logging as _log
import queue as _queue
import typing as _tp

import resultes_jsonrpc.jsonrpc.client as _rjjc
import resultes_jsonrpc.jsonrpc.types as _tps

_LOGGER = _log.getLogger(__name__)


@_dc.dataclass
class FormattedRecord:
    level: int
    message: str

    @property
    def json(self) -> _tps.JsonStructured:
        return {"level": self.level, "message": self.message}


class JsonRpcLogHandler(_log.Handler):
    _METHOD = "post_log_message"

    def __init__(
        self, logging_client: _rjjc.JsonRpcClient, level: int = _log.NOTSET
    ) -> None:
        super().__init__(level)

        self._logging_client = logging_client

        self._queue = _queue.Queue[FormattedRecord]()
        self._is_queue_shut_down = False

        self._sender_task: _asyncio.Task[_tp.Any] | None = None

    async def start(self, executor: _cf.Executor) -> None:
        _LOGGER.info("Starting.")

        self._sender_task = _asyncio.current_task()

        loop = _asyncio.get_running_loop()

        try:
            while True:
                formatted_record = await loop.run_in_executor(executor, self._queue.get)

                params = formatted_record.json
                await self._logging_client.send_notification(
                    self._METHOD, params=params
                )
        except _queue.ShutDown:
            pass

    def stop(self) -> None:
        self._queue.shutdown()

    def emit(self, record: _log.LogRecord) -> None:
        if self._is_queue_shut_down:
            return

        # Make sure sending of log messages doesn't trigger other log messages - ad infinitum
        try:
            event_loop = _asyncio.get_running_loop()
        except RuntimeError:
            event_loop = None
        if event_loop:
            current_task = _asyncio.current_task(event_loop)
            if (
                current_task is not None
                and self._sender_task is not None
                and current_task == self._sender_task
            ):
                return

        message = self.format(record)
        formatted_record = FormattedRecord(record.levelno, message)

        try:
            self._queue.put_nowait(formatted_record)
        except _queue.ShutDown:
            self._is_queue_shut_down = True
