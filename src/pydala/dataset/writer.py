import datetime as dt
import os
import re

import duckdb
import pandas as pd
import polars as pl
import progressbar
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from fsspec import spec
from pyarrow.fs import FileSystem

from ..utils.base import random_id
from ..utils.logging import log_decorator
from ..utils.table import get_tables_diff, to_relation
from .base import BaseDataSet
from .reader import Reader
from .timefly import TimeFly


class Writer(BaseDataSet):
    def __init__(
        self,
        base_path: str,
        bucket: str | None = None,
        partitioning: list | str | None = None,
        partitioning_flavor: str | None = None,
        format: str = "parquet",
        compression: str = "zstd",
        mode: str | None = "delta",  # can be 'delta', 'append', 'overwrite', 'raise'
        ddb: duckdb.DuckDBPyConnection | None = None,
        ddb_memory_limit: str = "-1",
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
        super().__init__(
            path=base_path,
            bucket=bucket,
            name=None,
            caching=False,
            partitioning=partitioning,
            partitioning_flavor=partitioning_flavor,
            format=format,
            compression=compression,
            ddb=ddb,
            ddb_memory_limit=ddb_memory_limit,
            cache_storage=cache_storage,
            protocol=protocol,
            profile=profile,
            endpoint_url=endpoint_url,
            storage_options=storage_options,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
            use_pyarrow_fs=use_pyarrow_fs,
        )
        self._base_path = self._path.copy()
        del self._path

        if isinstance(self._partitioning, str):
            self._partitioning = [self._partitioning]

        if not self._partitioning_flavor and self._partitioning:  # is not None:
            self._partitioning_flavor = "hive"

        self.compression()
        self.partitioning()
        self.mode()

        self._base_name = base_name

    def partitioning(
        self, columns: str | list | None = None, flavor: str | None = None
    ):
        if columns:  # is not None:
            if isinstance(columns, str):
                columns = [columns]
            self._partitioning = columns

        if flavor:  # is not None:
            self._partitioning_flavor = flavor
            
        self.logger.info(f"{self._partitioning} - {self._partitioning_flavor}")
        
        return self

    def compression(self, value: str | None = None):
        self._compression = value
        self.logger.info(self._compression)

        return self

    def format(self, value: str | None = None):
        if value:  # is not None:
            self._format = value
            
        self.logger.info(self._format)
        
        return self

    def mode(self, value: str | None):
        if value:  # is not None:
            if value not in ["overwrite", "delta", "append", "raise"]:
                raise ValueError(
                    "Value for mode must be 'overwrite', 'delta', 'raise' or 'append'."
                )
            else:
                self._mode = value

        self.logger.info(self._mode)

        return self

    def _gen_path(self, partition_names: tuple | None = None):
        if os.path.splitext(self._base_path)[-1] != "":
            return self._base_path

        if partition_names:  # is not None:
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

    def _handle_write_mode(
        self,
        table: duckdb.DuckDBPyRelation,
        path: str,
        delta_subset: list | None = None,
        datetime_column: str | None = None,
        start_time: str | dt.datetime | None = None,
    ):
        self._fs.invalidate_cache()

        if datetime_column:  # is not None:
            if not start_time:
                start_time = table.max(datetime_column).fetchone()[0]

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
                return table

            elif self._mode == "delta":

                existing_table = Reader(
                    path=path,
                    format=self._format,
                    ddb=self.ddb,
                    fsspec_fs=self._fs,
                    pyarrow_fs=self._pafs,
                    use_pyarrow_fs=self._use_pyarrow_fs,
                )

                if existing_table.disk_usage / 1024**2 <= 100:
                    existing_table.load_pa_table()

                tables_diff = get_tables_diff(
                    table1=table.filter(f"{datetime_column}>='{start_time}'")
                    if datetime_column  # is not None
                    else table,
                    table2=existing_table.rel.filter(
                        f"{datetime_column}>='{start_time}'"
                    )
                    if datetime_column  # is not None
                    else existing_table.rel,
                    subset=delta_subset,
                    ddb=self.ddb,
                )

                del existing_table

                return tables_diff

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
        start_time: str | dt.datetime | None = None,
    ):

        if isinstance(batch_size, int):
            for i in progressbar.progressbar(range(table.shape[0] // batch_size + 1)):
                yield table.limit(batch_size, offset=i * batch_size)

        elif isinstance(batch_size, str):
            if not datetime_column:
                raise TypeError("datetime_column must be not None")
            if datetime_column not in table.columns:
                raise ValueError(
                    f"{datetime_column} not found. Possible table columns are {', '.join(table.columns)}."
                )

            unit = re.findall("[a-z]+", batch_size.lower())
            if len(unit) > 0:
                unit = unit[0]
            else:
                unit = "y"

            val = re.findall("[0-9]+", batch_size)
            if len(val) > 0:
                val = int(val[0])
            else:
                val = 1

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

            if not start_time:  # is not None:
                start_time = table.min(datetime_column).fetchone()[0]

            end_time = table.max(datetime_column).fetchone()[0]

            print(start_time, end_time, interval)
            timestamps = (
                self.ddb.query(
                    f"SELECT * FROM generate_series(TIMESTAMP '{start_time}', TIMESTAMP '{end_time}' + {interval}, {interval})"
                )
                .arrow()["generate_series"]
                .to_pylist()
            )

            for sd, ed in progressbar.progressbar(
                list(zip(timestamps[:-1], timestamps[1:]))
            ):
                filter = f"{datetime_column} >= TIMESTAMP '{sd}' AND {datetime_column} < TIMESTAMP '{ed}'"

                table_part = table.filter(filter)

                yield table_part

        else:
            yield table

    @log_decorator(show_arguments=False)
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

    @log_decorator(show_arguments=False)
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
        start_time: str | dt.datetime | None = None,
        delta_subset: list | None = None,
        transform_func: object | None = None,
        transform_func_kwargs: dict | None = None,
        **kwargs,
    ):

        table = self._drop_sort_distinct(table=table)

        self._table = to_relation(
            table=table,
            ddb=self.ddb,
        )

        if not batch_size:
            batch_size = min(
                self._table.shape[0], 1024**2 * 64 // self._table.shape[1]
            )

        if self._partitioning:  # is not None:
            filters = self._get_partition_filters()

            for partition_filter, partition_names in filters:

                table_part = self._table.filter(partition_filter)

                # check if base_path for partition is empty and act depending on mode and distinct.
                partition_base_path = os.path.dirname(
                    self._gen_path(partition_names=partition_names)
                )

                table_part = self._handle_write_mode(
                    table=table_part,
                    path=partition_base_path,
                    delta_subset=delta_subset,
                    datetime_column=datetime_column,
                    start_time=start_time,
                )

                # Write table for partition

                if table_part.shape[0] > 0:
                    # for i in range(table_part.shape[0] // batch_size + 1):
                    for table_ in self.iter_batches(
                        table=table_part,
                        batch_size=batch_size,
                        datetime_column=datetime_column,
                        start_time=start_time,
                    ):
                        if transform_func:  # is not None:
                            table_ = to_relation(
                                transform_func(table_, **transform_func_kwargs),
                                ddb=self.ddb,
                            )

                        self.write_table(
                            table=table_.arrow(),
                            path=self._gen_path(partition_names=partition_names),
                            row_group_size=row_group_size,
                            **kwargs,
                        )

        else:
            # check if base_path for partition is empty and act depending on mode and distinct.
            base_path = os.path.dirname(self._gen_path(partition_names=None))
            self._table = self._handle_write_mode(
                table=self._table,
                path=base_path,
                delta_subset=delta_subset,
                datetime_column=datetime_column,
                start_time=start_time,
            )

            # write table

            if self._table.shape[0] > 0:
                for table_ in self.iter_batches(
                    table=self._table,
                    batch_size=batch_size,
                    datetime_column=datetime_column,
                    start_time=start_time,
                ):
                    if transform_func:  # is not None:
                        table_ = to_relation(
                            transform_func(table_, **transform_func_kwargs),
                            ddb=self.ddb,
                        )

                    self.write_table(
                        table=table_.arrow(),
                        path=self._gen_path(partition_names=None),
                        row_group_size=row_group_size,
                        **kwargs,
                    )


class TimeFlyWriter(Writer):
    def __init__(
        self,
        base_path: str,
        timefly: str | dt.datetime | None = None,
        bucket: str | None = None,
        partitioning: list | str | None = None,
        partitioning_flavor: str | None = None,
        format: str = "parquet",
        compression: str = "zstd",
        mode: str | None = "delta",  # can be 'delta', 'append', 'overwrite', 'raise'
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
        self._base_path_parent = base_path
        self.timefly = TimeFly(
            path=base_path,
            bucket=bucket,
            fsspec_fs=fsspec_fs,
            protocol=protocol,
            profile=profile,
            endpoint_url=endpoint_url,
            storage_options=storage_options,
        )
        if timefly is not None:
            self._timefly = (
                timefly
                if isinstance(timefly, dt.datetime)
                else dt.datetime.fromisoformat(timefly)
            )
        else:
            self._timefly = None

        self._snapshot_path = self.timefly._find_snapshot_subpath(timefly=self._timefly)
        self._base_path = os.path.join(self._base_path_parent, self._snapshot_path)

        super().__init__(
            base_path=self._base_path,
            bucket=bucket,
            partitioning=partitioning,
            partitioning_flavor=partitioning_flavor,
            format=format,
            compression=compression,
            mode=mode,
            ddb=ddb,
            cache_storage=cache_storage,
            protocol=protocol,
            base_name=base_name,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
            use_pyarrow_fs=use_pyarrow_fs,
        )

    def set_snapshot(self, snapshot):
        self._snapshot_path = self.timefly._find_snapshot_subpath(snapshot)
        self._base_path = os.path.join(self._base_path_parent, self._snapshot_path)
