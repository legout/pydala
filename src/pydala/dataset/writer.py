import datetime as dt
import os
import re

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from fsspec import spec
from pyarrow.fs import FileSystem

from .helper import (
    get_ddb_sort_str,
    get_filesystem,
    get_storage_path_options,
    get_tables_diff,
    random_id,
    to_relation,
)

# from ..filesystem.filesystem import FileSystem
from .reader import Reader


class Writer:
    def __init__(
        self,
        base_path: str,
        bucket: str | None = None,
        partitioning: list | str | None = None,
        partitioning_flavor: str | None = None,
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
        base_name: str = "data",
        fsspec_fs: spec.AbstractFileSystem | None = None,
        pyarrow_fs: FileSystem | None = None,
        use_pyarrow_fs: bool = False,
    ):
        self._profile = profile
        self._endpoint_url = endpoint_url
        self._storage_options = storage_options
        self._mode = mode
        self._use_pyarrow_fs = use_pyarrow_fs

        self._bucket, self._base_path, self._protocol = get_storage_path_options(
            bucket=bucket, path=base_path, protocol=protocol
        )

        self._filesystem = get_filesystem(
            bucket=self._bucket,
            protocol=self._protocol,
            profile=self._profile,
            endpoint_url=self._endpoint_url,
            storage_options=self._storage_options,
            caching=None,
            cache_bucket=None,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
            use_pyarrow_fs=self._use_pyarrow_fs,
        )
        self._fs = self._filesystem["fsspec_main"]
        if self._use_pyarrow_fs:
            self._pafs = self._filesystem["pyarrow_main"]
        else:
            self._pafs = None

        self._partitioning = (
            [partitioning] if isinstance(partitioning, str) else partitioning
        )
        self._partitioning_flavor = partitioning_flavor
        if self._partitioning_flavor is None and self._partitioning is not None:
            self._partitioning_flavor = "hive"

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

    def partitioning(
        self, columns: str | list | None = None, flavor: str | None = None
    ):
        if columns is not None:
            if isinstance(columns, str):
                columns = [columns]
            self._partitioning = columns

        if flavor is not None:
            self._partitioning_flavor = flavor

        return self

    def compression(self, value: str | None = None):
        self._compression = value

        return self

    def format(self, value: str | None = None):
        if value is not None:
            self._format = value

        return self

    def mode(self, value: str | None):
        if value is not None:
            if value not in ["overwrite", "append", "raise"]:
                raise ValueError(
                    "Value for mode must be 'overwrite', 'raise' or 'append'."
                )
            else:
                self._mode = value

        return self

    def _gen_path(self, partition_names: tuple | None = None):
        if os.path.splitext(self._base_path)[-1] != "":
            return self._base_path

        if partition_names is not None:
            if self._partitioning_flavor == "hive":
                hive_partitions = [
                    "=".join(part) for part in zip(self._partitioning, partition_names)
                ]
                path = os.path.join(self._base_path, *hive_partitions)
            else:
                path = os.path.join(self._base_path, *partition_names)
        else:
            path = self._base_path

        filename = f"{self._base_name}-{dt.datetime.now().strftime('%Y%m%d_%H%M%S%f')[:-3]}-{random_id()}.{self._format}"

        if self._protocol == "file":
            if not self._fs.exists(path):
                self._fs.mkdir(path, create_parents=True)

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

    def _handle_write_mode(self, table: duckdb.DuckDBPyRelation, path: str):
        self._fs.invalidate_cache()
        if self._fs.exists(path):

            if self._mode == "raise":
                raise FileExistsError(
                    f"Path '{path}' already exists. "
                    f"Use mode='overwrite' to overwrite '{path}' "
                    f"or mode='append' to append table to '{path}'"
                )

            elif self._mode == "overwrite":
                self._fs.rm(path, recursive=True)
                return table

            elif self._mode == "append":
                if self._distinct:

                    existing_table = Reader(
                        path=path,
                        format=self._format,
                        sort_by=self._sort_by,
                        ascending=self._ascending,
                        distinct=self._distinct,
                        drop=self._drop,
                        ddb=self.ddb,
                        fsspec_fs=self._fs,
                        pyarrow_fs=self._pafs,
                        use_pyarrow_fs=self._use_pyarrow_fs,
                    )

                    if existing_table.disk_usage / 1024**2 <= 100:
                        return get_tables_diff(
                            table1=table,
                            table2=existing_table.mem_table,
                            ddb=self.ddb,
                        )

                    return get_tables_diff(
                        table1=table,
                        table2=existing_table.dataset,
                        ddb=self.ddb,
                    )

                return table

            else:
                raise ValueError(
                    "Value for mode must be 'overwrite', 'raise' or 'append'."
                )

        return table

    def iter_batches(
        self,
        table: duckdb.DuckDBPyRelation,
        batch_size: int | str | None,
        datetime_column: str | None = None,
    ):

        if isinstance(batch_size, int):
            for i in range(table.shape[0] // batch_size + 1):
                yield table.limit(batch_size, offset=i * batch_size)

        elif isinstance(batch_size, str):
            if datetime_column is None:
                raise TypeError("datetime_column must be not None")
            if datetime_column not in table.columns:
                raise ValueError(
                    f"{datetime_column} not found. Possible table columns are {', '.join(table.columns)}."
                )

            unit = re.findall("[a-z]+", batch_size.lower())
            if len(unit)>0:
                unit = unit[0]
            else:
                unit="y"
                
            val = re.findall("[0-9]+", batch_size)
            if len(val)>0:
                val = int(val[0])
            else:
                val=1

            if unit in ["microseconds", "micro", "u"]:
                interval = f"to_microseconds({val})"

            elif unit in ["milliseconds", "milli", "ms"]:
                interval = f"to_milliseconds({val})"

            elif unit in ["seconds", "sec", "s"]:
                interval = f"to_seconds({val})"

            elif unit in ["miuntes", "min", "t"]:
                interval = f"to_minutes({val})"

            elif unit in ["hours", "h"]:
                interval = f"to_hours({val})"

            elif unit in ["days", "d"]:
                interval = f"to_days({val})"

            elif unit in ["months", "mo", "m"]:
                interval = f"to_months({val})"

            elif unit in ["years", "y", "a"]:
                interval = f"to_years({val})"


            start_time = table.min(datetime_column).fetchone()[0]
            end_time = table.max(datetime_column).fetchone()[0]

            i = 0
            end = False
            while not end:
                filter = (
                    f"{datetime_column} >= TIMESTAMP '{start_time}' + {i}*{interval} "
                    f"AND {datetime_column} < TIMESTAMP '{start_time}' + {i+1}*{interval}"
                )

                table_part = table.filter(filter)
                i += 1
                if table_part.max(datetime_column).fetchone()[0] == end_time:
                    end = True
                elif i==1000:
                    end=True

                yield table_part

        else:
            yield table

    def write_table(
        self,
        table: pa.Table,
        path: str,
        row_group_size: int | None = None,
        **kwargs,
    ):
        if table.shape[0] > 0:
            format = self._format.replace("ipc", "feather").replace("arrow", "feather")

            if format == "feather":
                if self._use_pyarrow_fs:
                    with self._pafs.open_output_stream(path) as f:
                        pl.from_arrow(table).write_ipc(
                            file=f, compression=self._compression, **kwargs
                        )
                else:
                    with self._fs.open(path, "wb") as f:
                        pl.from_arrow(table).write_ipc(
                            file=f, compression=self._compression, **kwargs
                        )

            elif format == "csv":
                if self._use_pyarrow_fs:
                    with self._pafs.open_output_stream(path) as f:
                        pl.from_arrow(table).write_csv(file=f, **kwargs)
                else:
                    with self._fs.open(path, "wb") as f:
                        pl.from_arrow(table).write_csv(file=f, **kwargs)

            else:
                pq.write_table(
                    table,
                    path,
                    row_group_size=row_group_size,
                    compression=self._compression,
                    filesystem=self._pafs if self._use_pyarrow_fs else self._fs,
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
        batch_size: int | str | None = None,
        row_group_size: int | None = None,
        datetime_column: str | None = None,
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

        if batch_size is None:
            batch_size = min(
                self._table.shape[0], 1024**2 * 64 // self._table.shape[1]
            )

        if self._partitioning is not None:
            filters = self._get_partition_filters()

            for partition_filter, partition_names in filters:

                table_part = self._table.filter(partition_filter)

                # check if base_path for partition is empty and act depending on mode and distinct.
                partition_base_path = os.path.dirname(
                    self._gen_path(partition_names=partition_names)
                )

                table_part = self._handle_write_mode(
                    table=table_part, path=partition_base_path
                )

                # Write table for partition

                if table_part.shape[0] > 0:
                    # for i in range(table_part.shape[0] // batch_size + 1):
                    for table_ in self.iter_batches(
                        table=table_part,
                        batch_size=batch_size,
                        datetime_column=datetime_column,
                    ):
                        self.write_table(
                            table=table_.arrow(),
                            path=self._gen_path(partition_names=partition_names),
                            row_group_size=row_group_size,
                            **kwargs,
                        )

        else:
            # check if base_path for partition is empty and act depending on mode and distinct.
            base_path = os.path.dirname(self._gen_path(partition_names=None))
            self._table = self._handle_write_mode(table=self._table, path=base_path)

            # write table

            if self._table.shape[0] > 0:
                for table_ in self.iter_batches(
                    table=self._table,
                    batch_size=batch_size,
                    datetime_column=datetime_column,
                ):
                    self.write_table(
                        table=table_.arrow(),
                        path=self._gen_path(partition_names=None),
                        row_group_size=row_group_size,
                        **kwargs,
                    )
