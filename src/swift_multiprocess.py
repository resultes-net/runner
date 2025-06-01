import os as _os
import pathlib as _pl

import keystoneauth1.session as _ksess
import resultes_pydantic_models.pytrnsys as _mpytrnsys
import swiftclient.client as _sclient

import keystone as _ks
import swift as _swift


def _init_connection() -> _sclient.Connection:
    auth = _ks.create_application_credential()
    session = _ksess.Session(auth=auth)
    connection = _sclient.Connection(session=session)
    return connection


_connection = _init_connection()

del _init_connection


def download_storage_object(
    object_storage_path: _mpytrnsys.ObjectStoragePath,
    output_file_path: _pl.Path,
) -> None:
    output_file_path = output_file_path.with_stem(
        f"{output_file_path.stem}-{_os.getpid()}"
    )
    _swift.download_storage_object(object_storage_path, output_file_path, _connection)
