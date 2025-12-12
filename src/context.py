import concurrent.futures as _cf
import dataclasses as _dc
import pathlib as _pl

import resultes_jsonrpc.jsonrpc.connection as _rjjc
import resultes_openstack_utils.swift_multithreaded as _swmt


@_dc.dataclass
class Context:
    def __init__(
        self,
        jobs_dir_path: _pl.Path,
        shall_remove_completed_jobs: bool,
        swift: _swmt.Swift,
        executor: _cf.Executor,
    ) -> None:
        self.jobs_dir_path = jobs_dir_path
        self.shall_remove_completed_jobs = shall_remove_completed_jobs
        self.swift = swift
        self.executor = executor

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
