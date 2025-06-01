import pathlib as _pl

import resultes_pydantic_models.pytrnsys as _mpytrnsys

import swift as _swift


def test_create_connection() -> None:
    with _swift.create_connection():
        pass


def test_download_trnsys() -> None:
    with _swift.create_connection() as connection:
        object_storage_path = _mpytrnsys.ObjectStoragePath(
            container="resultes", path="build-runner-image/TRNSYS18_resultes.zip"
        )
        output_file_path = (
            _pl.Path(__file__).parent / "test_output" / "TRNSYS18_resultes.zip"
        )
        _swift.download_storage_object(
            object_storage_path, output_file_path, connection
        )
