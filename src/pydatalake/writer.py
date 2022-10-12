import uuid

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.feather as pf
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import s3fs
import datetime as dt

from .utils import open as open_
from .utils import to_ddb_relation
from pathlib import Path


class Writer:
    def __init__(
        self,
        path: str,
        base_name: str = "data",
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: pafs.FileSystem | s3fs.S3FileSystem | None = None,
        format: str | None = "parquet",
        compression: str | None = "zstd",
        sort_by: str | list | None = None,
        ddb: duckdb.DuckDBPyConnection | None = None,
    ):
        self._path = path
        self._base_name = base_name
        self._partitioning = (
            [partitioning] if isinstance(partitioning, str) else partitioning
        )
        self._filesystem = filesystem
        self._format = format
        self._compression = compression
        self._sort_by = sort_by
        if ddb is not None:
            self.ddb = ddb
        else:
            self.ddb = duckdb.connect()
        self.ddb.execute("SET temp_directory='/tmp/duckdb/'")

    def _gen_path(
        self,
        path: str,
        partition_names: tuple | None = None,
        with_time_partition: bool = False,
    ):
        path = Path(path)
        if path.suffix != "":
            parts = [path.parent]
        else:
            parts = [path]

        if partition_names is not None:
            parts.extend(partition_names)

        if with_time_partition:
            parts.append(str(dt.datetime.today()))

        if path.suffix == "":
            parts.append(self._base_name + f"-{uuid.uuid4().hex}.{self._format}")
        else:
            parts.append(path.suffix)

        path = Path(*parts)

        if self._filesystem is None:
            path.mkdir.parent(exist_ok=True, parents=True)

        return path

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
                    pf.write_feather(table, f, compression=compression, **kwargs)

                pf.write_feather(
                    table,
                    path,
                    compression=compression,
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
        distinct: bool = False,
        rows_per_file: int | None = None,
        row_group_size: int | None = None,
        with_time_partition: bool = False,
        **kwargs,
    ):
        if path is not None:
            self._path = path
        else:
            path = self._path

        if format is not None:
            self._format = format
        else:
            format = self._format

        if compression is not None:
            self._compression = compression
        else:
            compression = self._compression

        if sort_by is not None:
            self._sort_by = sort_by
        else:
            sort_by = self._sort_by

        table = to_ddb_relation(
            table=table, ddb=self.ddb, sort_by=sort_by, distinct=distinct
        )

        if partitioning is not None:
            if isinstance(partitioning, str):
                partitioning = [partitioning]
        else:
            partitioning = (
                self._partitioning
                if isinstance(self._partitioning, list)
                else [self._partitioning]
            )

        if partitioning is not None:
            partitions = table.project(",".join(partitioning)).distinct().fetchall()

            for partition_names in partitions:

                filter_ = []
                for p in zip(partitioning, partition_names):
                    filter_.append(f"{p[0]}='{p[1]}'")
                filter_ = " AND ".join(filter_)

                table_part = table.filter(filter_)

                if rows_per_file is None:

                    self.write_table(
                        table=table_part.arrow(),
                        path=self._gen_path(
                            path=path,
                            partition_names=partition_names,
                            with_time_partition=with_time_partition,
                        ),
                        format=format,
                        compression=compression,
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
                            format=format,
                            compression=compression,
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
