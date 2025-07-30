import asyncio as _asyncio
import dataclasses as _dc
import logging as _log

import resultes_jsonrpc.jsonrpc.client as _rjjc
import resultes_jsonrpc.jsonrpc.types as _tps


@_dc.dataclass
class FormattedRecord:
    level: str
    message: str

    @property
    def json(self) -> _tps.JsonStructured:
        return {"level": self.level, "message": self.message}


class JsonRpcLogHandler(_log.Handler):
    _METHOD = "post_log_message"

    def __init__(self, logging_client: _rjjc.JsonRpcClient, level=_log.NOTSET) -> None:
        super().__init__(level)

        self._logging_client = logging_client

        self._task: _asyncio.Task[None] | None = None
        self._queue = _asyncio.Queue[FormattedRecord]()

    async def start(self) -> None:
        try:
            while True:
                formatted_record = await self._queue.get()
                params = formatted_record.json
                await self._logging_client.send_notification(
                    self._METHOD, params=params
                )
        except _asyncio.QueueShutDown:
            pass

    def stop(self) -> None:
        self._queue.shutdown()

    def emit(self, record: _log.LogRecord) -> None:
        message = self.format(record)
        formatted_record = FormattedRecord(record.levelname, message)
        self._queue.put_nowait(formatted_record)
