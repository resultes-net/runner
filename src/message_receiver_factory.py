import asyncio as _asyncio
import collections.abc as _cabc
import concurrent.futures as _cf
import contextlib as _ctx
import dataclasses as _dc
import logging as _log

import resultes_jsonrpc.jsonrpc.connection as _rjjc
import resultes_jsonrpc.jsonrpc.jsonrpc_logging as _jrpcl
import resultes_jsonrpc.websockets.types as _rjwt

import context as _con
import log_config as _logc

_LOGGER = _log.getLogger(__name__)


@_dc.dataclass
class _Payload:
    jsonrpc_connection: _rjjc.Connection
    log_handler: _jrpcl.JsonRpcLogHandler


class MessageReceiverSingletonFactory:
    def __init__(
        self,
        task_group: _asyncio.TaskGroup,
        context: _con.Context,
        executor: _cf.Executor,
    ) -> None:
        self._context = context
        self._executor = executor

        self._dispatcher = _rjjc.AsyncTaskSpawningDispatcher(task_group)
        self._payload: _Payload | None = None
        self._payload_created_event = _asyncio.Event()

    def __call__(self, write_websocket: _rjwt.WriteWebsocket) -> _rjwt.MessageReceiver:
        if self._payload:
            raise RuntimeError("Already connected.")

        jsonrpc_connection = _rjjc.Connection(
            self._dispatcher, self._context, write_websocket
        )
        log_handler = self._setup_and_get_log_handler(jsonrpc_connection)

        self._payload = _Payload(jsonrpc_connection, log_handler)
        self._payload_created_event.set()

        self._context.set_jsonrpc_connection(jsonrpc_connection)
        
        return jsonrpc_connection

    @staticmethod
    def _setup_and_get_log_handler(
        jsonrpc_connection: _rjjc.Connection,
    ) -> _jrpcl.JsonRpcLogHandler:
        jsonrcp_log_handler = _jrpcl.JsonRpcLogHandler(jsonrpc_connection)
        formatter = _log.Formatter(_logc.LOG_FORMAT)
        jsonrcp_log_handler.setFormatter(formatter)
        root_logger = _log.getLogger()
        root_logger.addHandler(jsonrcp_log_handler)
        return jsonrcp_log_handler

    @_ctx.asynccontextmanager
    async def run(self) -> _cabc.AsyncIterator[None]:
        task = _asyncio.create_task(self._run_async())
        yield
        if self._payload:
            self._payload.log_handler.stop()

    async def _run_async(self) -> None:
        await self._payload_created_event.wait()

        if not self._payload:
            return

        _LOGGER.info("Starting log handler.")

        await self._payload.log_handler.start(self._executor)
