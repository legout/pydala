import os
from typing import Dict, List

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pds
from fsspec import AbstractFileSystem
from isort import file

from .dataset import BaseDataset
from .utils.table import (
    concat_tables,
    distinct_table,
    get_table_delta,
    get_timestamp_column,
    sort_table,
    to_relation,
)


class Writer(BaseDataset):
    def __init__(
        self,
        table: pa.Table
        | pl.DataFrame
        | pd.DataFrame
        | pa.dataset.Dataset
        | duckdb.DuckDBPyRelation
        | List[pa.Table]
        | List[pl.DataFrame]
        | List[pd.DataFrame]
        | List[pa.dataset.Dataset]
        | List[duckdb.DuckDBPyRelation],
        path: str,
        bucket: str | None = None,
        schema: pa.Schema | Dict[str, str] | None = None,
        format: str = "parquet",
        filesystem: AbstractFileSystem | None = None,
        partitioning: pds.Partitioning | List[str] | str | None = None,
        timestamp_column: str | None = None,
        ddb: duckdb.DuckDBPyConnection | None = None,
        name: str | None = None,
        mode: str = "delta",
        **storage_options,
    ):
        super().__init__(
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

        if isinstance(table, list | tuple):
            table = concat_tables(tables=table, schema=schema)

        self._table = to_relation(table, self.ddb)
        self._mode = mode
        if self._timestamp_column:
            self.select_files(time_range=table)

    def sort(self, by: str | List[str], ascending: bool = True):
        self._table = sort_table(self._table, sort_by=by, ascending=ascending)

    def distinct(self, subset: str | List[str] | None = None, keep: str = "first"):
        self._table = distinct_table(self._table, subset=subset, keep=keep)

    def _gen_partition_path(
        self,
        partitions: str
        | int
        | float
        | List[str]
        | List[int]
        | List[float]
        | Dict[str, str]
        | Dict[str, int]
        | Dict[str, float]
        | None,
    ):
        if partitions is None:
            return self._path

        if self._partitioning == "hive":
            return os.path.join(
                self._path, *[f"{k}={v}" for k, v in partitions.items()]
            )

        if not isinstance(partitions, list | tuple):
            partitions = [partitions]

        return os.path.join(self._path, *[str(part) for part in partitions])

    def _get_table_delta(
        self, table: duckdb.DuckDBPyRelation, subset: str | List[str] | None = None
    ):
        if not self._path_empty:
            if self._timestamp_column:
                min_timestamp, max_timestamp = table.aggregate(
                    f"min({self._timestamp_column}), max({self._timestamp_column})"
                ).fetchone()

                return get_table_delta(
                    table1=table,
                    table2=self.ddb_rel.filter(
                        f"{self._timestamp_column}>='{min_timestamp}' AND {self._timestamp_column}<='{max_timestamp}'"
                    ),
                    subset=subset,
                )

            else:
                return get_table_delta(
                    table1=self.ddb.from_arrow(table),
                    table2=self.ddb_rel,
                    subset=subset,
                )

        return table

    def partition_by(self):
        pass


def write_dataset(
    tables: pa.Table
    | pl.DataFrame
    | pd.DataFrame
    | pa.dataset.Dataset
    | duckdb.DuckDBPyRelation
    | List[pa.Table]
    | List[pl.DataFrame]
    | List[pd.DataFrame]
    | List[pa.dataset.Dataset]
    | List[duckdb.DuckDBPyRelation]
    | None = None,
    path: str | None = None,
    bucket: str | None = None,
    filesystem: AbstractFileSystem | None = None,
    partitioning: List[str] | str | None = None,
    timestamp_column: str | None = None,
    mode: str = "delta",
    subset: str | List[str] | None = None,
):
    if isinstance(table, list | tuple):
        if isinstance(table[0], pds.Dataset):
            table = pds.Dataset(table)

        elif isinstance(table[0], duckdb.DuckDBPyRelation):
            table0 = table[0]
            for table_ in table[1:]:
                table0 = table0.union(table_)

            table = table0

        else:
            table = pa.concat_tables(
                [to_arrow(table_) for table_ in table], promote=True
            )

    else:
        table = to_arrow(table)

    if timestamp_column is None:
        timestamp_column = get_timestamp_column(table)

    ds = dataset(
        path=path,
        bucket=bucket,
        filesystem=filesystem,
        partitioning=partitioning,
        timestamp_column=timestamp_column,
    )

    if mode == "delta":
        if timestamp_column is not None:
            min_timestamp, max_timestamp = (
                table.aggregate(
                    f"min({timestamp_column}), max({timestamp_column})"
                ).fetchone()
                if isinstance(table, duckdb.DuckDBPyRelation)
                else (
                    duckdb.from_arrow(table)
                    .aggregate(f"min({timestamp_column}), max({timestamp_column})")
                    .fetchone()
                )
            )
            table = get_table_delta(
                table1=ds.ddb.from_arrow(table),
                table2=ds.ddb_rel.filter(
                    f"{ds._timestamp_column}>='{min_timestamp}' AND {ds._timestamp_column}<='{max_timestamp}'"
                ),
                subset=subset,
            )
        else:
            table = get_table_delta(
                table1=ds.ddb.from_arrow(table),
                table2=ds.ddb_rel,
                subset=subset,
            )
