from .dataset import Dataset
import duckdb
from fsspec import filesystem as fsspec_filesystem
from .sync import sync_datasets
import pyarrow as pa
from fsspec.spec import AbstractFileSystem
from typing import List, Dict, Union
import pyarrow.dataset as pds
import datetime as dt


def filesystem_dataset(
    path: str,
    bucket: str | None = None,
    schema: pa.Schema | Dict[str, str] | None = None,
    format: str = "parquet",
    filesystem: AbstractFileSystem | None = None,
    partitioning: pds.Partitioning | List[str] | str | None = None,
    timestamp_column: str | None = None,
    ddb: duckdb.DuckDBPyConnection | None = None,
    name: str | None = None,
    time_range: dt.datetime
    | str
    | List[Union[str, None]]
    | List[Union[dt.datetime, None]]
    | None = None,
    file_size: int
    | str
    | List[Union[int, None]]
    | List[Union[str, None]]
    | None = None,
    last_modified: dt.datetime
    | str
    | List[Union[str, None]]
    | List[Union[dt.datetime, None]]
    | None = None,
    row_count: int | List[Union[int, None]] | None = None,
    materialize: bool = False,
    combine_chunks: bool = False,
    chunk_size: int = 1_000_000,
    **storage_options,
) -> Dataset:
    ds = Dataset(
        path=path,
        bucket=bucket,
        schema=schema,
        format=format,
        filesystem=filesystem,
        partitioning=partitioning,
        timestamp_column=timestamp_column,
        ddb=ddb,
        name=name,
        **storage_options,
    )

    ds._load_arrow_dataset(
        time_range=time_range,
        file_size=file_size,
        last_modified=last_modified,
        row_count=row_count,
    )

    if materialize:
        ds.materialize(combine_chunks=combine_chunks, chunk_size=chunk_size)

    return ds


def cache_dataset(
    dataset: Dataset,
    cache_base_dir: str = "/tmp/pydala",
) -> Dataset:
    cache_dataset_ = filesystem_dataset(
        path=dataset._path,
        bucket=cache_base_dir,
        format=dataset._format,
        partitioning=dataset._partitioning,
        filesystem=fsspec_filesystem(protocol="file"),
        timestamp_column=dataset._timestamp_column,
        name=f"{dataset.name}_cache_",
        ddb=dataset.ddb,
    )
    sync_datasets(dataset1=dataset, dataset2=cache_dataset_, delete=True)

    cache_dataset_.reload()
    return cache_dataset_


def dataset(
    path: str,
    bucket: str | None = None,
    schema: pa.Schema | Dict[str, str] | None = None,
    format: str = "parquet",
    filesystem: AbstractFileSystem | None = None,
    partitioning: pds.Partitioning | List[str] | str | None = None,
    timestamp_column: str | None = None,
    ddb: duckdb.DuckDBPyConnection | None = None,
    name: str | None = None,
    time_range: dt.datetime
    | str
    | List[Union[str, None]]
    | List[Union[dt.datetime, None]]
    | None = None,
    file_size: int
    | str
    | List[Union[int, None]]
    | List[Union[str, None]]
    | None = None,
    last_modified: dt.datetime
    | str
    | List[Union[str, None]]
    | List[Union[dt.datetime, None]]
    | None = None,
    row_count: int | List[Union[int, None]] | None = None,
    materialize: bool = False,
    combine_chunks: bool = False,
    chunk_size: int = 1_000_000,
    cached: bool = False,
    cache_base_dir: str = "/tmp/pydala",
    **storage_options,
) -> Dataset:
    materialize_fs = False if cached else materialize
    ds = filesystem_dataset(
        path=path,
        bucket=bucket,
        schema=schema,
        format=format,
        filesystem=filesystem,
        partitioning=partitioning,
        timestamp_column=timestamp_column,
        ddb=ddb,
        name=name,
        time_range=time_range,
        file_size=file_size,
        last_modified=last_modified,
        row_count=row_count,
        materialize=materialize_fs,
        combine_chunks=combine_chunks,
        chunk_size=chunk_size,
        **storage_options,
    )
    if cached:
        ds = cache_dataset(dataset=ds, cache_base_dir=cache_base_dir)
        if materialize:
            ds.materialize(combine_chunks=combine_chunks, chunk_size=chunk_size)
        return ds

    return ds
