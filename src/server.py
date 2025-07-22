import asyncio as _asyncio
import concurrent.futures as _cf
import logging as _log
import pathlib as _pl
import typing as _tp

import jsonrpcserver as _jrpcs
import websockets as _ws
import websockets.asyncio.server as _wsas

import swift_multithreaded as _swmt


class _TerminateTaskGroupException(Exception):
    pass


class Server:
    def __init__(
        self,
        port: int,
        jobs_dir_path: _pl.Path,
        swift: _swmt.Swift,
        executor: _cf.Executor,
        shutdown_event: _asyncio.Event,
    ) -> None:
        self._port = port
        self.jobs_dir_path = jobs_dir_path
        self.swift = swift
        self.executor = executor
        self._shutdown_event = shutdown_event

        self._tasks = set[_asyncio.Task[None]]()

    async def _terminate_task_group(self) -> _tp.NoReturn:
        raise _TerminateTaskGroupException()

    async def _terminate_task_group_on_shutdown_event(self) -> _tp.NoReturn:
        await self._shutdown_event.wait()
        raise _TerminateTaskGroupException()

    async def _handle_connection(
        self, server_connection: _wsas.ServerConnection
    ) -> None:
        _log.info("Client %s connected.", server_connection.id)

        try:
            async with _asyncio.TaskGroup() as task_group:
                task_group.create_task(self._terminate_task_group_on_shutdown_event())

                try:
                    while not self._shutdown_event.is_set():
                        data = await server_connection.recv(decode=True)
                        await self._handle_request(data, server_connection, task_group)

                except _ws.ConnectionClosedOK:
                    _log.info("Client %s disconnected.", server_connection.id)
                    await task_group.create_task(self._terminate_task_group())

        except* _TerminateTaskGroupException:
            pass

    def _on_task_done(self, task: _asyncio.Task[None]) -> None:
        _log.debug("Task %s is done.", task.get_name())

    async def _handle_request(
        self,
        data: str,
        server_connection: _wsas.ServerConnection,
        task_group: _asyncio.TaskGroup,
    ) -> None:
        coroutine = self._dispatch_request(data, server_connection)
        task = task_group.create_task(coroutine)
        _log.debug("Task %s has been started.", task.get_name())
        task.add_done_callback(self._on_task_done)

    async def _dispatch_request(
        self, data: str, server_connection: _wsas.ServerConnection
    ) -> None:
        if result := await _jrpcs.async_dispatch(data, context=self):
            await server_connection.send(result)

    async def serve(self) -> None:
        async with _ws.serve(self._handle_connection, port=self._port):
            await self._shutdown_event.wait()
