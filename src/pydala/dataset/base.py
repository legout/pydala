import pyarrow.dataset as pads
from fsspec import spec
from pyarrow.fs import FileSystem
import duckdb
import pyarrow as pa
import polars as pl
from ..filesystem import (
    get_fsspec_filesystem,
    get_fsspec_dir_filesystem,
    get_pyarrow_subtree_filesystem,
)
from fsspec.implementations.arrow import ArrowFSWrapper
from ..utils.base import get_ddb_sort_str


class Dataset:
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        schema: pa.Schema | None = None,
        format: str = "parquet",
        filesystem: spec.AbstractFileSystem | FileSystem | None = None,
        partitioning: pads.Partitioning | list | str | None = None,
        exclude_invalid_files: bool = True,
        ddb: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self._path = path
        self._bucket = bucket
        self._format = format
        self._partitioning = partitioning
        self._exclude_invalid_files = exclude_invalid_files
        self._schema = schema
        self._ddb = ddb

        if filesystem is None:
            if bucket is None:
                self._filesystem = get_fsspec_filesystem(protocol="file")
            else:
                self._filesystem = get_fsspec_dir_filesystem(
                    path=bucket, protocol="file"
                )
        else:
            if isinstance(filesystem, spec.AbstractFileSystem):
                self._ddb.register_filesystem(filesystem=filesystem)

                if bucket is not None:
                    self._filesystem = get_fsspec_dir_filesystem(
                        path=bucket, fs=filesystem
                    )
            elif isinstance(filesystem, FileSystem):
                self._ddb.register_filesystem(filesystem=ArrowFSWrapper(filesystem))

                if bucket is not None:
                    self._filesystem = get_pyarrow_subtree_filesystem(
                        path=bucket, filesystem=filesystem
                    )

    def sort(self, by: str | list | None = None, ascending: bool | list | None = None):
        self._sort_by = by

        ascending = ascending or True
        self._ascending = ascending

        self._sort_by_ddb = ""

        if self._sort_by:  # is not None:
            self._sort_by_ddb = get_ddb_sort_str(sort_by=by, ascending=ascending)

        return self

    def distinct(
        self,
        value: bool | None = None,
        subset: list | None = None,
        presort_by: list | None = None,
        postsort_by: list | None = None,
    ):
        if not value:
            value = False
        self._distinct = value
        self._distinct_params = {}
        self._distinct_params["subset"] = subset
        self._distinct_params["presort_by"] = presort_by
        self._distinct_params["postsort_by"] = postsort_by

        return self

    def drop(self, columns: str | list | None = None):
        self._drop = columns

        return self
