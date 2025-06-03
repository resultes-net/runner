import abc as _abc
import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import dataclasses as _dc
import logging as _log
import multiprocessing as _mp
import multiprocessing.queues as _mqueues
import multiprocessing.synchronize as _msync
import pathlib as _pl
import queue as _queue
import types as _tps
import typing as _tp
import uuid as _uuid

import resultes_pydantic_models.pytrnsys as _mpytrnsys

import swift as _swift


@_dc.dataclass
class _RequestBase(_abc.ABC):
    id: _uuid.UUID


@_dc.dataclass
class _DownloadRequest(_RequestBase):
    input_object_storage_zip_path: _mpytrnsys.ObjectStorageZipPath
    output_file_path: _pl.Path


@_dc.dataclass
class _UploadRequest(_RequestBase):
    input_file_path: _pl.Path
    output_object_storage_zip_path: _mpytrnsys.ObjectStorageZipPath


class _Stop:
    pass


type _Request = _RequestBase


@_dc.dataclass
class _Reponse:
    id: _uuid.UUID


_LOGGER = _log.getLogger()

LOG_LEVEL = _log.INFO
LOG_FORMAT = (
    "%(process)d:%(thread)d: %(asctime)s - %(levelname)s - %(module)s - %(message)s"
)


class _RequestHandler:
    def __init__(
        self,
        request_queue: _mqueues.Queue[_Request | _Stop],
        response_queue: _mqueues.Queue[_Reponse | _Stop],
        shutdown_event: _msync.Event,
    ) -> None:
        self._request_queue = request_queue
        self._response_queue = response_queue
        self._shutdown_event = shutdown_event

    def handle_requests(self) -> None:
        _log.basicConfig(format=LOG_FORMAT, level=LOG_LEVEL)

        if not self._request_queue or not self._response_queue:
            raise ValueError("Swift client has not been `aentere`d yet.")

        _LOGGER.info("Entering request handling loop.")

        with _swift.create_connection() as connection:
            while not self._shutdown_event.is_set():
                request = self._request_queue.get()

                match request:
                    case _DownloadRequest():
                        _LOGGER.info("Got download request %s.", request.id)

                        _swift.download_storage_object(
                            request.input_object_storage_zip_path,
                            request.output_file_path,
                            connection,
                            self._shutdown_event,
                        )

                        if self._shutdown_event.is_set():
                            _LOGGER.info("Received shutdown event.")
                            break

                        _LOGGER.info(
                            "Done handling request %s. Sending response.", request.id
                        )

                        response = _Reponse(request.id)
                        self._response_queue.put(response)

                    case _UploadRequest():
                        _LOGGER.info("Got upload request %s.", request.id)

                        _swift.upload_storage_object(
                            request.input_file_path,
                            request.output_object_storage_zip_path,
                            connection,
                        )

                        if self._shutdown_event.is_set():
                            _LOGGER.info("Received shutdown event.")
                            break

                        _LOGGER.info(
                            "Done handling request %s. Sending response.", request.id
                        )

                        response = _Reponse(request.id)
                        self._response_queue.put(response)                        

                    case _Stop():
                        _LOGGER.info("Got stop request.")
                        break
                    case _:
                        _tp.assert_never(_)

            _LOGGER.info("Exiting request handling loop.")


class Swift(_ctx.AbstractAsyncContextManager["Swift"]):
    def __init__(self, n_processes: int, max_queue_size: int | None = None) -> None:
        actual_max_queue_size = (
            n_processes if max_queue_size is None else max_queue_size
        )

        if n_processes > actual_max_queue_size:
            raise ValueError(
                "The number of processes must be less than or equal to the maximum queue size."
            )

        self._n_processes = n_processes
        self._max_queue_size = actual_max_queue_size

        self._request_queue: _mp.Queue[_Request | _Stop] | None = None
        self._handle_request_processes: _cabc.Sequence[_mp.Process] | None = None

        self._response_queue: _mp.Queue[_Reponse | _Stop] | None = None
        self._handle_responses_task: _asyncio.Task[None] | None = None
        self._current_response: _Reponse | None = None
        self._got_response_event = _asyncio.Event()

        self._shutdown_event = _mp.Event()

    async def __aenter__(self) -> _tp.Self:
        self._request_queue = _mp.Queue(self._max_queue_size)
        self._response_queue = _mp.Queue(self._max_queue_size)

        self._handle_request_processes = [
            self._create_process() for _ in range(self._n_processes)
        ]

        for process in self._handle_request_processes:
            process.start()

        self._handle_responses_task = _asyncio.create_task(self._handle_responses())

        return self

    def _create_process(self) -> _mp.Process:
        assert self._request_queue and self._response_queue

        handler_process = _RequestHandler(
            self._request_queue, self._response_queue, self._shutdown_event
        )
        process = _mp.Process(target=handler_process.handle_requests)
        return process

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: _tps.TracebackType | None,
    ) -> None:
        if (
            not self._handle_responses_task
            or not self._handle_request_processes
            or not self._request_queue
            or not self._response_queue
        ):
            raise ValueError("Swift client hasn't been `aenter`d.")

        self._shutdown_event.set()
        self._got_response_event.set()

        self._try_put_stop_no_wait(
            self._request_queue, "request", n_stops=self._n_processes
        )
        self._try_put_stop_no_wait(self._response_queue, "response")

        await _asyncio.wait([self._handle_responses_task])

        for process in self._handle_request_processes:
            process.join()

    def _try_put_stop_no_wait[T](
        self, queue: _mqueues.Queue[T | _Stop], queue_name: str, n_stops: int = 1
    ) -> None:
        # If the queue is empty, we might have to awake processes so that they can run to termination

        try:
            for _ in range(n_stops):
                _LOGGER.info("Sending stop to %s queue.", queue_name)
                queue.put_nowait(_Stop())
        except _queue.Full:
            pass

    async def _handle_responses(self) -> None:
        _LOGGER.info("Entering response handling loop.")

        while not self._shutdown_event.is_set():
            _LOGGER.debug("Going to wait for response.")
            response_or_stop = await _asyncio.to_thread(self._get_response_or_stop)

            match response_or_stop:
                case _Reponse() as response:
                    self._current_response = response
                    _LOGGER.info(
                        "Got response %s. Setting got response event.",
                        self._current_response.id,
                    )
                    self._got_response_event.set()
                    self._got_response_event.clear()
                case _Stop():
                    break

        _LOGGER.info("Exiting response handling loop.")

    async def _wait_for_response(self, request_id: _uuid.UUID) -> None:
        _LOGGER.info("Start waiting for response %s.", request_id)

        while True:
            await self._got_response_event.wait()
            if self._shutdown_event.is_set():
                _LOGGER.info("Shutdown: stop waiting for response %s.", request_id)
                return

            assert self._current_response
            if self._current_response.id == request_id:
                _LOGGER.info(
                    "Waiting for response %s paid off: received response.", request_id
                )
                return

            _LOGGER.debug(
                "Woke up for nothing: waiting for response %s but this is %s.",
                request_id,
                self._current_response.id,
            )

    async def download(
        self,
        input_object_storage_zip_path: _mpytrnsys.ObjectStorageZipPath,
        output_file_path: _pl.Path,
    ) -> None:
        request_id = _uuid.uuid4()
        request = _DownloadRequest(
            request_id, input_object_storage_zip_path, output_file_path
        )
        await _asyncio.to_thread(self._put_request, request)

        await self._wait_for_response(request_id)

    async def upload(
        self,
        input_file_path: _pl.Path,
        output_object_storage_zip_path: _mpytrnsys.ObjectStorageZipPath,
    ) -> None:
        request_id = _uuid.uuid4()
        request = _UploadRequest(
            request_id, input_file_path, output_object_storage_zip_path
        )
        await _asyncio.to_thread(self._put_request, request)

        await self._wait_for_response(request_id)

    def _put_request(self, request_or_stop: _Request | _Stop) -> None:
        if not self._request_queue:
            raise ValueError("Swift client has not been `aentere`d yet.")

        self._request_queue.put(request_or_stop)

    def _get_response_or_stop(self) -> _Reponse | _Stop:
        if not self._response_queue:
            raise ValueError("Swift client has not been `aentere`d yet.")

        _LOGGER.debug("Start waiting for response.")
        response = self._response_queue.get()
        return response
