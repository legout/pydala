import datetime as dt
import os

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.feather as pf
import pyarrow.parquet as pq
from fsspec import spec
from fsspec.utils import infer_storage_options
from pyarrow.fs import FileSystem

# from ..filesystem.filesystem import FileSystem
from .reader import Reader
from .helper import (
    get_ddb_sort_str,
    get_filesystem,
    random_id,
    to_relation,
    get_tables_diff,
)


class Writer:
    def __init__(
        self,
        base_path: str,
        bucket: str | None = None,
        partitioning: list | str | None = None,
        format: str = "parquet",
        compression: str = "zstd",
        mode: str | None = "append",  # can be 'append', 'overwrite', 'raise'
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = "__index_level_0__",
        ddb: duckdb.DuckDBPyConnection | None = None,
        cache_storage: str | None = "/tmp/pydala/",
        protocol: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        storage_options: dict = {},
        fsspec_fs: spec.AbstractFileSystem | None = None,
        pyarrow_fs: FileSystem | None = None,
        base_name: str = "data",
    ):
        self._profile = profile
        self._endpoint_url = endpoint_url
        self._storage_options = storage_options
        self._mode = mode

        self._set_path(base_path=base_path, bucket=bucket, protocol=protocol)

        self._filesystem = get_filesystem(
            bucket=self._bucket,
            protocol=self._protocol,
            profile=self._profile,
            endpoint_url=self._endpoint_url,
            storage_options=self._storage_options,
            caching=None,
            cache_bucker=None,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
        )
        self._fs = self._filesystem["fsspec_main"]
        self._pafs = self._filesystem["pyarrow_main"]

        self._partiotioning = (
            [partitioning] if isinstance(partitioning, str) else partitioning
        )
        self._format = format
        self._compression = compression
        self._base_name = base_name

        _ = self.sort(by=sort_by, ascending=ascending)
        _ = self.distinct(value=distinct)
        _ = self.drop(columns=drop)

        if ddb is not None:
            self.ddb = ddb
        else:
            self.ddb = duckdb.connect()
        self.ddb.execute(f"SET temp_directory='{cache_storage}'")

    def _set_path(self, base_path: str, bucket: str | None, protocol: str | None):
        if bucket is not None:
            self._bucket = infer_storage_options(bucket)["path"]
        else:
            self._bucket = None

        self._base_path = infer_storage_options(base_path)["path"]

        if self._bucket is not None:
            self._protocol = protocol or infer_storage_options(bucket)["protocol"]

        else:
            self._protocol = protocol or infer_storage_options(base_path)["protocol"]

    def sort(self, by: str | list | None, ascending: bool | list | None = None):
        self._sort_by = by

        if ascending is None:
            ascending = True
        self._ascending = ascending

        if self._sort_by is not None:
            self._sort_by_ddb = get_ddb_sort_str(sort_by=by, ascending=ascending)

        return self

    def distinct(self, value: bool | None):
        if value is None:
            value = False
        self._distinct = value

        return self

    def drop(self, columns: str | list | None):
        self._drop = columns
        return self

    def _gen_path(
        self,
        partition_names: tuple | None = None,
    ):
        if os.path.splitext(self._base_path)[-1] != "":
            return self._base_path

        if partition_names is not None:
            path = os.path.join(self._base_path, *partition_names)
        else:
            path = self._base_path

        filename = f"{self._base_name}-{dt.datetime.now().strftime('%Y%m%d_%H%M%S%f')[:-3]}-{random_id()}.{self._format}"

        if self._protocol == "file":
            if not self._fs.exists(path):
                self._fs.mkdir(path, create_parents=True)

        if self._fs.exists(path):
            if self._mode == "raise":
                self._base_path_empty = False
                raise FileExistsError(
                    f"""{path} exists. 
                    Use mode='overwrite' to overwrite {path} or mode='append' to append table to {path}"""
                )

            elif self._mode == "overwrite":
                self._fs.rm(path, recursive=True)
                self._base_path_empty = True
            else:
                self._base_path_empty = False
        else:
            self._base_path_empty = True

        return os.path.join(path, filename)

    def _get_partition_filters(self):

        filters = []
        all_partitions = (
            self._table.project(",".join(self._partitioning)).distinct().fetchall()
        )

        for partition_names in all_partitions:

            filter_ = []
            for p in zip(self._partitioning, partition_names):
                filter_.append(f"{p[0]}='{p[1]}'")

            filters.append(" AND ".join(filter_))

        return zip(filters, all_partitions)

    def write_table(
        self,
        table: pa.Table,
        path: str,
        row_group_size: int | None = None,
        # compression: str | None = None,
        **kwargs,
    ):
        format = self._format.replace("ipc", "feather").replace("arrow", "feather")

        if not self._base_path_empty and self._distinct:
            base_path = os.path.dirname(path)
            existing_table=Reader(
                path=base_path,
                bucket=self._bucket,
                format=self._format,
                sort_by=self._sort_by,
                ascending=self._ascending,
                distinct=self._distinct,
                drop=self._drop,
                ddb=self.ddb,
                fsspec_fs=self._fs,
                pyarrow_fs=self._pafs
            )
            if existing_table.disk_usage / 1024**2 <=100:
                table = get_tables_diff(table1=table, table2=existing_table.mem_table, ddb=self.ddb
                    )
            else:
                table = get_tables_diff(table1=table, table2=existing_table.dataset, ddb=self.ddb)

        if format == "feather":

            with self._fs.open(path, "wb") as f:
                pl.from_arrow(table).write_ipc(
                    file=f, compression=self._compression, **kwargs
                )

        elif format == "csv":
            with self._fs.open(path, "wb") as f:
                pl.from_arrow(table).write_csv(file=f, **kwargs)

        else:
            pq.write_table(
                table,
                path,
                row_group_size=row_group_size,
                compression=self._compression,
                filesystem=self._pafs,
                **kwargs,
            )

    def write_dataset(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.FileSystemDataset
        | pd.DataFrame
        | pl.DataFrame
        | str,
        rows_per_file: int | None = None,
        row_group_size: int | None = None,
        **kwargs,
    ):

        self._table = to_relation(
            table=table,
            ddb=self.ddb,
            sort_by=self._sort_by,
            ascending=self._ascending,
            distinct=self._distinct,
            drop=self._drop,
        )

        if self._partitioning is not None:
            filters = self._get_partition_filters()

            for partition_filter, partition_names in filters:

                table_part = self._table.filter(partition_filter)

                for i in range(table_part.shape[0] // rows_per_file + 1):
                    self.write_table(
                        table=table_part.limit(
                            rows_per_file, offset=i * rows_per_file
                        ).arrow(),
                        path=self._gen_path(
                            partition_names=partition_names,
                        ),
                        format=self._format,
                        compression=self._compression,
                        row_group_size=row_group_size,
                        **kwargs,
                    )

        else:
            for i in range(self._table.shape[0] // rows_per_file + 1):
                self.write_table(
                    table=self._table.limit(
                        rows_per_file, offset=i * rows_per_file
                    ).arrow(),
                    path=self._gen_path(
                        partition_names=None,
                    ),
                    format=self._format,
                    compression=self._compression,
                    row_group_size=row_group_size,
                    **kwargs,
                )
