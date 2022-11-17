import os

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from fsspec import spec
from pyarrow.fs import FileSystem

from ..filesystem.base import BaseFileSystem
from ..utils.base import get_ddb_sort_str
from ..utils.table import distinct_table, drop_columns, sort_table


class BaseDataSet(BaseFileSystem):
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        name: str | None = None,
        partitioning: ds.Partitioning | list | str | None = None,
        partitioning_flavor: str | None = None,
        format: str | None = "parquet",
        compression: str | None = "zstd",
        ddb: duckdb.DuckDBPyRelation | None = None,
        ddb_memory_limit: str = "-1",
        caching: bool = False,
        cache_storage: str | None = "/tmp/pydala/",
        protocol: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        storage_options: dict = {},
        fsspec_fs: spec.AbstractFileSystem | None = None,
        pyarrow_fs: FileSystem | None = None,
        use_pyarrow_fs: bool = False,
    ):

        super().__init__(
            path=path,
            bucket=bucket,
            name=name,
            caching=caching,
            cache_storage=cache_storage,
            protocol=protocol,
            profile=profile,
            endpoint_url=endpoint_url,
            storage_options=storage_options,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
            use_pyarrow_fs=use_pyarrow_fs,
        )

        self._format = format
        self._partitioning = partitioning
        self._partitioning_flavor = partitioning_flavor
        self._compression = compression
        self._tables = {}
        self.sort()
        self.distinct()
        self.drop()

        if ddb:  # is not None:
            self.ddb = ddb
        else:
            self.ddb = duckdb.connect()
        self.ddb.execute(
            f"SET temp_directory='{os.path.join(cache_storage, 'duckdb')}'"
        )
        self._ddb_memory_limit = ddb_memory_limit
        self.ddb.execute(f"SET memory_limit='{self._ddb_memory_limit}'")

    def sort(self, by: str | list | None, ascending: bool | list | None = None):
        self._sort_by = by

        ascending = ascending or True
        self._ascending = ascending

        self._sort_by_ddb = ""

        if self._sort_by:  # is not None:
            self._sort_by_ddb = get_ddb_sort_str(sort_by=by, ascending=ascending)

        return self

    def distinct(
        self,
        value: bool | None,
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

    def drop(self, columns: str | list | None = "__index_level_0__"):
        self._drop = columns

        return self

    def _drop_sort_distinct(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | pl.DataFrame
        | pd.DataFrame
        | ds.FileSystemDataset,
    ):
        if self._drop:
            table = drop_columns(table=table, columns=self._drop)
        if self._distinct:
            table = distinct_table(table=table, ddb=self.ddb, **self._distinct_params)

        if self._sort_by:
            table = sort_table(
                table=table, sort_by=self._sort_by, ascending=self._ascending
            )
        return table
