import concurrent.futures as _cf
import pathlib as _pl

import resultes_jsonrpc.jsonrpc.server as _rjjs

import swift_multithreaded as _swmt


class Context(_rjjs.ContextBase):
    def __init__(
        self,
        jobs_dir_path: _pl.Path,
        swift: _swmt.Swift,
        executor: _cf.Executor,
    ) -> None:
        super().__init__()
        self.jobs_dir_path = jobs_dir_path
        self.swift = swift
        self.executor = executor
