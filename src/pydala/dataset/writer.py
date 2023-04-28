import os
from typing import Dict, List

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pds
from fsspec import AbstractFileSystem
from isort import file

from .reader import Dataset
from .utils.table import (
    concat_tables,
    distinct_table,
    get_table_delta,
    get_timestamp_column,
    sort_table,
    to_relation,
    with_strftime_column,
    with_timebucket_column,
)


class Writer(Dataset):
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
        delta_subset: str | List[str] | None = None,
        **storage_options,
    ):
        if isinstance(table, list | tuple):
            table = concat_tables(tables=table, schema=schema)

        self._table = to_relation(table, self.ddb)
        self._mode = mode

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
        self._min_timestamp = None
        self._max_timestamp = None

        if self._timestamp is not None:
            self._min_timestamp, self._max_timestamp = self._table.aggregate(
                f"min({self._timestamp_column}), max({self._timestamp_column})"
            ).fetchone()
            self.select_files(time_range=[self._min_timestamp, self._max_timestamp])

        self._load_dataset(time_range=[self._min_timestamp, self._max_timestamp])
        if mode == "delta":
            self._gen_table_delta(subset=delta_subset)

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
    ) -> str:
        if partitions is None:
            return self._path

        if self._partitioning == "hive":
            return os.path.join(
                self._path, *[f"{k}={v}" for k, v in partitions.items()]
            )

        if not isinstance(partitions, list | tuple):
            partitions = [partitions]

        return os.path.join(self._path, *[str(part) for part in partitions])

    def _gen_table_delta(
        self, subset: str | List[str] | None = None
    ) -> duckdb.DuckDBPyRelation:
        if not self._path_empty:
            if self._timestamp_column:
                self._table = get_table_delta(
                    table1=self._table,
                    table2=self.ddb_rel.filter(
                        f"{self._timestamp_column}>='{self._min_timestamp}' AND {self._timestamp_column}<='{self._max_timestamp}'"
                    ),
                    subset=subset,
                )

            else:
                self._table = get_table_delta(
                    table1=self._table,
                    table2=self.ddb_rel,
                    subset=subset,
                )

    def add_date_columns(
        self,
        year: bool = False,
        month: bool = False,
        week: bool = False,
        yearday: bool = False,
        monthday: bool = False,
        weekday: bool = False,
        strftime: str | None = None,
    ):
        if strftime:
            if isinstance(strftime, str):
                strftime = [strftime]
            column_names = [
                f"_strftime_{strftime_.replace('%', '').replace('-', '_')}_"
                for strftime_ in strftime
            ]
        else:
            strftime = []
            column_names = []

        if year:
            strftime.append("%Y")
            column_names.append("Year")
        if month:
            strftime.append("%m")
            column_names.append("Month")
        if week:
            strftime.append("%W")
            column_names.append("Week")
        if yearday:
            strftime.append("%j")
            column_names.append("YearDay")
        if monthday:
            strftime.append("%d")
            column_names.append("MonthDay")
        if weekday:
            strftime.append("%a")
            column_names.append("WeekDay")

        self._table = with_strftime_column(
            table=self._table,
            timestamp_column=self._timestamp_column,
            strftime=strftime,
            column_names=column_names,
        )

    def write(
        self,
        batch_size: int = 1_000_000,
        file_size: str | None = None,
        partitioning: str | List[str] | None = None,
        partition_flavor: str = "dir",  # "hive" or "dir"
        mode: str = "delta",
        sort_by: str | List[str] | None = None,
        ascending: bool | List[bool] = True,
        distinct: bool = False,
        subset: str | List[str] | None = None,
        keep: str = "first",
        presort: bool = False,
        preload_batches: bool = False,
    ):
        if file_size:
            batch_size = self._estimate_batch_size(file_size=file_size)

        batches = self._partition_by(
            which="_table", n_rows=batch_size, file_size=file_size, columns=partitioning, as_dict=True, drop=True, sort_by=sort_by, ascending=ascending, distinct=distinct, subset=subset,
            keep=keep, presort=preload_batches
        )
        if preload_batches:
            batches = dict(batches)
            
        def _write(batch, )
            
        


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
