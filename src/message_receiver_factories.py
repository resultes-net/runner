import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import dataclasses as _dc
import logging as _log
import typing as _tp

import resultes_jsonrpc.jsonrpc.client as _rjjc
import resultes_jsonrpc.jsonrpc.server as _rjjs
import resultes_jsonrpc.websockets.types as _rjwt

import context as _con
import jsonrpc_logging as _jrpcl
import log_config as _logc

_LOGGER = _log.getLogger(__name__)


class RequestReceiverSingletonFactory(
    _ctx.AbstractAsyncContextManager["RequestReceiverSingletonFactory"]
):
    def __init__(self, context: _con.Context) -> None:
        self._context = context
        self._dispatcher = _rjjs.TaskSpawningDispatcher()
        self._requests_server: _rjjs.JsonRpcServer | None = None

    def __call__(self, write_websocket: _rjwt.WriteWebsocket) -> _rjwt.MessageReceiver:
        if self._requests_server:
            raise RuntimeError("Requests already connected.")

        self._requests_server = _rjjs.JsonRpcServer(
            write_websocket, self._dispatcher, self._context
        )
        return self._requests_server

    async def __aenter__(self) -> _tp.Self:
        await self._dispatcher.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> bool | None:
        return await self._dispatcher.__aexit__(exc_type, exc_value, traceback)

    def cancel_requests(self) -> None:
        self._dispatcher.cancel_tasks()


@_dc.dataclass
class _LoggingFactoryPayload:
    client: _rjjc.JsonRpcClient
    log_handler: _jrpcl.JsonRpcLogHandler


class LoggingMessageReceiverSingletonFactory:
    def __init__(self) -> None:
        self._payload: _LoggingFactoryPayload | None = None
        self._payload_created_event = _asyncio.Event()

    def __call__(self, write_websocket: _rjwt.WriteWebsocket) -> _rjwt.MessageReceiver:
        if self._payload:
            raise RuntimeError("Logging already connected.")

        logging_client = _rjjc.JsonRpcClient(write_websocket)
        log_handler = self._setup_and_get_log_handler(logging_client)

        self._payload = _LoggingFactoryPayload(logging_client, log_handler)
        self._payload_created_event.set()

        return self._payload.client

    @staticmethod
    def _setup_and_get_log_handler(
        jsonrpc_client: _rjjc.JsonRpcClient,
    ) -> _jrpcl.JsonRpcLogHandler:
        jsonrcp_log_handler = _jrpcl.JsonRpcLogHandler(jsonrpc_client, _log.INFO)
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

        await self._payload.log_handler.start()
