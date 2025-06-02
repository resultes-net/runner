import contextlib as _ctx
import multiprocessing.synchronize as _msync
import pathlib as _pl
import typing as _tp

import resultes_pydantic_models.pytrnsys as _mpytrnsys
import swiftclient.client as _sclient

import keystone as _ks

_CHUNK_SIZE = 512 * 1024


@_ctx.contextmanager
def create_connection() -> _tp.Generator[_sclient.Connection]:
    with _ks.create_session() as session:
        connection = _sclient.Connection(session=session)
        yield connection
        connection.close()


def download_storage_object(
    object_storage_path: _mpytrnsys.ObjectStoragePath,
    output_file_path: _pl.Path,
    connection: _sclient.Connection,
    shutdown_event: _msync.Event | None = None,
) -> None:
    output_dir_path = output_file_path.parent

    if not output_dir_path.exists():
        output_dir_path.mkdir(parents=True)

    _, chunks = connection.get_object(
        object_storage_path.container,
        object_storage_path.path,
        resp_chunk_size=_CHUNK_SIZE,
    )

    with output_file_path.open("bw") as stream:
        for chunk in chunks:
            if shutdown_event and shutdown_event.is_set():
                return

            stream.write(chunk)
