import datetime as dt
import os
import uuid
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.feather as pf
import pyarrow.parquet as pq

#from ..filesystem.filesystem import FileSystem
from .utils import get_ddb_sort_str, to_relation


class Writer:
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        base_name: str = "data",
        partitioning: ds.Partitioning | list | str | None = None,
        filesystem: str | None = None,
        format: str | None = "parquet",
        compression: str | None = "zstd",
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = "__index_level_0__",
        ddb: duckdb.DuckDBPyConnection | None = None,
        cache_prefix: str | None = "/tmp/pydala/",
    ):

        self._set_filesystems(filesystem=filesystem, bucket=bucket)

        self._path = path
        self._base_name = base_name
        self._partitioning = (
            [partitioning] if isinstance(partitioning, str) else partitioning
        )

        self._format = format
        self._compression = compression
        _ = self.sort(by=sort_by, ascending=ascending)
        _ = self.distinct(value=distinct)
        _ = self.drop(columns=drop)

        if ddb is not None:
            self.ddb = ddb
        else:
            self.ddb = duckdb.connect()
        self.ddb.execute(f"SET temp_directory='{cache_prefix}'")
        self._tables = dict()

    def _set_filesystems(self, filesystem: str | None, bucket: str | None):
        self._filesystem = filesystem or FileSystem(
            type_="local", bucket=bucket, use_s5cmd=False
        )
        self._filesystem_org = filesystem
        self._bucket = bucket or self._filesystem._bucket

    def _gen_path(
        self,
        path: str,
        partition_names: tuple | None = None,
        with_time_partition: bool = False,
    ):

        path = Path(path)
        if path.suffix != "":
            path = path.parent
            name = path.suffix
        else:
            name = None

        if partition_names is not None:
            path = os.path.join(path, *partition_names)

        if with_time_partition:
            path = os.path.join(path, dt.datetime.now().strftime("%Y%m%d_%H%M%S"))

        if name is None:
            name = f"{self._base_name}-{uuid.uuid4().hex}.{self._format}"

        path = os.path.join(path, name)

        if self._filesystem._type == "local" or self._filesystem._type == None:
            path.mkdir.parent(exist_ok=True, parents=True)

        return path

    def _add_table(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.FileSystemDataset
        | pd.DataFrame
        | pl.DataFrame
        | str,
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = None,
    ):

        self.sort(by=sort_by, ascending=ascending)
        self.distinct(value=distinct)
        self.drop(columns=drop)

        self._table = to_relation(
            table=table,
            ddb=self.ddb,
            sort_by=self._sort_by,
            ascending=self._ascending,
            distinct=self._distinct,
            drop=self._drop,
        )

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

        return filters

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

    def write_table(
        self,
        table: pa.Table,
        path: Path | str,
        row_group_size: int | None = None,
        compression: str | None = None,
        **kwargs,
    ):

        filesystem = kwargs.pop("filesystem", self._filesystem)
        compression = self._compression if compression is None else compression

        format = (
            kwargs.pop("format", self._format)
            .replace("arrow", "feather")
            .replace("ipc", "feather")
        )

        if format == "feather":
            if filesystem is not None:
                with open_(str(path), filesystem) as f:
                    pf.write_feather(
                        table,
                        f,
                        compression=compression,
                        chunksize=row_group_size,
                        **kwargs,
                    )

                pf.write_feather(
                    table,
                    path,
                    compression=compression,
                    chunksize=row_group_size,
                    **kwargs,
                )
        else:
            pq.write_table(
                table,
                path,
                row_group_size=row_group_size,
                compression=compression,
                filesystem=filesystem,
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
        path: str | None = None,
        format: str | None = None,
        compression: str | None = None,
        partitioning: list | str | None = None,
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool = False,
        drop: str | list | None = None,
        rows_per_file: int | None = None,
        row_group_size: int | None = None,
        with_time_partition: bool = False,
        **kwargs,
    ):
        if path is not None:
            self._path = path

        if format is not None:
            self._format = format

        if compression is not None:
            self._compression = compression

        if partitioning is not None:
            if isinstance(partitioning, str):
                partitioning = [partitioning]
            self._partitioning = partitioning

        self._add_table(
            table=table,
            sort_by=sort_by,
            ascending=ascending,
            distinct=distinct,
            drop=drop,
        )

        if self._partitioning is not None:
            partition_filters = self._get_partition_filters()

            for partition_filter in partition_filters:
                table_part = table.filter(partition_filter)

                if rows_per_file is None:

                    self.write_table(
                        table=table_part.arrow(),
                        path=self._gen_path(
                            path=path,
                            partition_names=partition_names,
                            with_time_partition=with_time_partition,
                        ),
                        format=self._format,
                        compression=self._compression,
                        row_group_size=row_group_size,
                        **kwargs,
                    )
                else:
                    for i in range(table_part.shape[0] // rows_per_file + 1):
                        self.write_table(
                            table=table_part.limit(
                                rows_per_file, offset=i * rows_per_file
                            ).arrow(),
                            path=self._gen_path(
                                path=path,
                                partition_names=partition_names,
                                with_time_partition=with_time_partition,
                            ),
                            format=self._format,
                            compression=self._compression,
                            row_group_size=row_group_size,
                            **kwargs,
                        )

        else:
            if rows_per_file is None:

                self.write_table(
                    table=table.arrow(),
                    path=self._gen_path(
                        path=path,
                        partition_names=None,
                        with_time_partition=with_time_partition,
                    ),
                    format=format,
                    compression=compression,
                    row_group_size=row_group_size,
                    **kwargs,
                )
            else:
                for i in range(table.shape[0] // rows_per_file + 1):
                    self.write_table(
                        table=table.limit(
                            rows_per_file, offset=i * rows_per_file
                        ).arrow(),
                        path=self._gen_path(
                            path=path,
                            partition_names=None,
                            with_time_partition=with_time_partition,
                        ),
                        format=format,
                        compression=compression,
                        row_group_size=row_group_size,
                        **kwargs,
                    )
