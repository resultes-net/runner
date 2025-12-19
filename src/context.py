import asyncio as _asyncio

import resultes_jsonrpc.jsonrpc.connection as _rjjc

import job_runner.job_runner as _jr


class Context:
    def __init__(
        self, task_group: _asyncio.TaskGroup, job_runner_config: _jr.Config
    ) -> None:
        self.task_group = task_group
        self.job_runner_config = job_runner_config

        self._jsonrpc_connection: _rjjc.Connection | None = None

    def set_jsonrpc_connection(self, jsonrpc_connection: _rjjc.Connection) -> None:
        if self._jsonrpc_connection:
            raise RuntimeError("Connection already set.")

        self._jsonrpc_connection = jsonrpc_connection

    @property
    def jsonrpc_connection(self) -> _rjjc.Connection:
        if not self._jsonrpc_connection:
            raise RuntimeError("Connection not set.")

        return self._jsonrpc_connection
