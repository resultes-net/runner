import pathlib as _pl
import zipfile as _zf

import resultes_pydantic_models.runner as _mrunner

import swift_multithreaded as _sm

from . import executor as _ex


class ResultUploader:
    def __init__(
        self,
        working_dir_path: _pl.Path,
        upload_dir_path: _pl.Path,
        swift: _sm.Swift,
        executor: _ex.Executor,
    ) -> None:
        self._working_dir_path = working_dir_path
        self._upload_dir_path = upload_dir_path
        self._swift = swift
        self._executor = executor

    async def upload_result(self, result: _mrunner.Result) -> None:
        match result:
            case _mrunner.SingleFileResult():
                await self._upload_single_file_result(result)
            case _mrunner.MultipleFilesResult():
                await self._upload_multiple_files_result(result)
            case _:
                _tp.assert_never(_)

    async def _upload_single_file_result(
        self, result: _mrunner.SingleFileResult
    ) -> None:
        result_file_path = self._working_dir_path / result.file_path

        await self._swift.upload(
            result_file_path, result.object_storage_output_file_path
        )

    async def _upload_multiple_files_result(
        self, result: _mrunner.MultipleFilesResult
    ) -> None:
        zip_file_path = await self._executor.run(self._create_zip_file, result)

        await self._swift.upload(zip_file_path, result.object_storage_output_file_path)

    def _create_zip_file(self, result: _mrunner.MultipleFilesResult) -> _pl.Path:
        paths = [
            p for g in result.glob_patterns for p in self._working_dir_path.glob(g)
        ]

        sorted_paths = sorted(paths)

        relative_result_zip_file_path = (
            _pl.PurePath(result.object_storage_output_file_path.container)
            / result.object_storage_output_file_path.path
        )
        result_zip_file_path = self._upload_dir_path / relative_result_zip_file_path

        result_zip_file_containing_dir_path = result_zip_file_path.parent

        if not result_zip_file_containing_dir_path.is_dir():
            result_zip_file_path.parent.mkdir(parents=True)

        with _zf.ZipFile(result_zip_file_path, mode="w") as zip_file:
            for path in sorted_paths:
                assert path.exists()

                relative_path = path.relative_to(self._working_dir_path)
                if path.is_dir():
                    zip_file.mkdir(str(relative_path))
                else:
                    zip_file.write(path, relative_path)

        return result_zip_file_path
