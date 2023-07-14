from sqlite3 import SQLITE_LIMIT_VARIABLE_NUMBER
from isort import file
from .reader import dataset
from .writer import write_dataset
import duckdb
import pyarrow as pa
from fsspec.spec import AbstractFileSystem
import pyarrow.dataset as pds
import datetime as dt


def compact(
    path: str,
    bucket: str | None = None,
    schema: pa.Schema | dict[str, str] | None = None,
    format: str = "parquet",
    filesystem: AbstractFileSystem | None = None,
    partitioning: pds.Partitioning | list[str] | str | None = None,
    timestamp_column: str | None = None,
    ddb: duckdb.DuckDBPyConnection | None = None,
    name: str | None = None,
    cached: bool = False,
    cache_base_dir: str = "/tmp/pydala",
    verbose: bool = True,
    time_range: dt.datetime
    | str
    | list[str | None]
    | list[dt.datetime | None]
    | None = None,
    last_modified: dt.datetime
    | str
    | list[str | None]
    | list[dt.datetime | None]
    | None = None,
    row_count: int | list[int | None] | None = None,
    batch_size: int = 1_000_000,
    file_size: int| str | None = None,
    partition_flavor: str = "dir",  # "hive" or "dir"
    row_group_size: int = 150_000,
    compression: str = "zstd",
    sort_by: str | list[str] | None = None,
    ascending: bool | list[bool] = True,
    distinct: bool = False,
    subset: str | list[str] | None = None,
    keep: str = "first",
    presort: bool = False,
    preload_partitions: bool = False,
    **storage_options,
):
    ds = dataset(
        path=path,
        bucket=bucket,
        schema=schema,
        format=format,
        filesystem=filesystem,
        partitioning=partitioning if partition_flavor != "hive" else "hive",
        timestamp_column=timestamp_column,
        ddb=ddb,
        name=name,
        time_range=time_range,
        file_size=file_size,
        last_modified=last_modified,
        row_count=row_count,
        last_modified=last_modified,
        cache_base_dir=cache_base_dir,
        cached=cached,
        verbose=verbose**storage_options,
    )

    write_dataset(
        table=ds.ddb_rel,
        path=path,
        bucket=bucket,
        schema=schema,
        format=format,
        filesystem=filesystem,
        partitioning=ds._partition_columns,
        partition_flavor=ds._partition_flavor,
        timestamp_column=timestamp_column,
        ddb=ddb,
        name=name,
        mode="overwrite",
        batch_size=batch_size,
        file_size=file_size,
        row_group_size=row_group_size,
        compression=compression,
        sort_by=sort_by,
        ascending=ascending,
        distinct=distinct,
        subset=subset,
        keep=keep,
        presort=presort,
        preload_partitions=preload_partitions,
        **storage_options,
    )


def zorder():
    pass
