import asyncio as _asyncio
import concurrent.futures as _cf
import contextlib as _ctx
import logging as _log
import pathlib as _pl
import threading as _thread
import types as _tps
import typing as _tp

import resultes_pydantic_models.pytrnsys as _mpytrnsys
import swiftclient as _sclient

import swift as _swift

_LOGGER = _log.getLogger()

LOG_LEVEL = _log.INFO
LOG_FORMAT = (
    "%(process)d:%(thread)d: %(asctime)s - %(levelname)s - %(module)s - %(message)s"
)


class Swift(_ctx.AbstractAsyncContextManager["Swift"]):
    def __init__(self, executor: _cf.Executor, max_workers: int) -> None:
        self._executor = executor
        self._max_workers = max_workers
        self._shutdown_event = _thread.Event()

    async def __aenter__(self) -> _tp.Self:
        self._connections_contexts = {
            _swift.create_connection() for _ in range(self._max_workers)
        }
        self._free_connections = _asyncio.Queue[_sclient.Connection](
            maxsize=self._max_workers
        )
        for context in self._connections_contexts:
            connection = context.__enter__()
            await self._free_connections.put(connection)

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: _tps.TracebackType | None,
    ) -> bool:
        self._shutdown_event.set()

        self._executor.shutdown(wait=True)

        for context in self._connections_contexts:
            context.__exit__(exc_type, exc_value, traceback)

        return False

    @_ctx.asynccontextmanager
    async def _free_connection(self) -> _tp.AsyncIterator[_sclient.Connection]:
        connection = await self._free_connections.get()
        yield connection
        await self._free_connections.put(connection)

    async def download(
        self,
        input_object_storage_zip_path: _mpytrnsys.ObjectStorageZipPath,
        output_file_path: _pl.Path,
    ) -> None:
        await self._run_in_executor_with_connection(
            self._download,
            input_object_storage_zip_path,
            output_file_path,
        )

    def _download(
        self,
        input_object_storage_zip_path: _mpytrnsys.ObjectStorageZipPath,
        output_file_path: _pl.Path,
        connection: _sclient.Connection,
    ) -> None:
        _swift.download_storage_object(
            input_object_storage_zip_path,
            output_file_path,
            connection,
            self._shutdown_event,
        )

    async def upload(
        self,
        input_file_path: _pl.Path,
        output_object_storage_zip_path: _mpytrnsys.ObjectStorageZipPath,
    ) -> None:
        await self._run_in_executor_with_connection(
            _swift.upload_storage_object,
            input_file_path,
            output_object_storage_zip_path,
        )

    async def _run_in_executor_with_connection[*S, T](
        self,
        func: _tp.Callable[[*S, _sclient.Connection], T],
        *args: *S,
    ) -> T:
        async with self._free_connection() as connection:
            loop = _asyncio.get_running_loop()
            result = await loop.run_in_executor(self._executor, func, *args, connection)
            return result
