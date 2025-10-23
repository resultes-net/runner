import collections.abc as _cabc
import logging as _log
import pathlib as _pl
import zipfile as _zf

import resultes_openstack_utils.swift_multithreaded as _sm
import resultes_pydantic_models.runner as _mrunner

from . import executor as _ex

_LOGGER = _log.getLogger(__name__)


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

    async def upload(self, result: _mrunner.Result) -> None:
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
        paths = self._get_globbed_paths(result.glob_patterns)

        relative_result_zip_file_path = (
            _pl.PurePath(result.object_storage_output_file_path.container)
            / result.object_storage_output_file_path.path
        )
        result_zip_file_path = self._upload_dir_path / relative_result_zip_file_path

        result_zip_file_containing_dir_path = result_zip_file_path.parent

        if not result_zip_file_containing_dir_path.is_dir():
            result_zip_file_path.parent.mkdir(parents=True)

        _LOGGER.info("Creating zip file...")
        with _zf.ZipFile(
            result_zip_file_path, compression=_zf.ZIP_DEFLATED, mode="w"
        ) as zip_file:
            for path in paths:
                assert path.exists()

                relative_path = path.relative_to(self._working_dir_path)
                if path.is_dir():
                    _LOGGER.debug(
                        "Creating directory %s in zip file %s.",
                        relative_path,
                        result_zip_file_path,
                    )
                    zip_file.mkdir(str(relative_path))
                else:
                    _LOGGER.debug(
                        "Adding file %s to zip file %s.",
                        relative_path,
                        result_zip_file_path,
                    )
                    zip_file.write(path, relative_path)

            n_files = sum(1 for i in zip_file.infolist() if not i.is_dir())

        stat = result_zip_file_path.stat()
        size_mb = round(stat.st_size / 1024 / 1024)

        _LOGGER.info(
            "...DONE. The created file is %.2f MiB big and contains %i files.",
            size_mb,
            n_files,
        )

        return result_zip_file_path

    def _get_globbed_paths(
        self, glob_patterns: _mrunner.GlobPatterns
    ) -> _cabc.Sequence[_pl.Path]:
        include_paths = self._get_paths(glob_patterns.include)
        formatted_include_paths = "\n".join(f"\t{p}" for p in include_paths)
        _LOGGER.debug(
            "Include glob patterns expanded to following paths:\n%s",
            formatted_include_paths,
        )

        if glob_patterns.exclude:
            exclude_paths = self._get_paths(glob_patterns.exclude)
            formatted_exclude_paths = "\n".join(f"\t{p}" for p in exclude_paths)
            _LOGGER.debug(
                "Exclude glob patterns expanded to following paths:\n%s",
                formatted_exclude_paths,
            )
        else:
            exclude_paths = list[_pl.Path]()
            _LOGGER.debug("No exclude patterns were given.")

        remaining_paths = sorted(set(include_paths) - set(exclude_paths))
        formatted_remaining_paths = "\n".join(f"\t{p}" for p in remaining_paths)
        _LOGGER.debug("Remaining, final paths are:\n%s", formatted_remaining_paths)

        return include_paths

    def _get_paths(
        self, glob_patterns: _cabc.Sequence[str]
    ) -> _cabc.Sequence[_pl.Path]:
        paths = [p for g in glob_patterns for p in self._working_dir_path.glob(g)]

        sorted_paths = sorted(paths)
        return sorted_paths
