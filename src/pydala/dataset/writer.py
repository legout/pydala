import datetime as dt
import os
from typing import Any, Dict, List

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pds
import tqdm
from fsspec import AbstractFileSystem
from joblib import Parallel, delayed

from pydala.utils import random_id  # , run_parallel

from .reader import Dataset
from .utils.table import (
    concat_tables,
    distinct_table,
    get_table_delta,
    get_timestamp_column,
    sort_table,
    to_relation,
    with_strftime_column,
    write_table,
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

        self._table = to_relation(table, ddb=self.ddb)
        self._mode = mode

        self._min_timestamp = None
        self._max_timestamp = None

        if self._base_dataset and self._timestamp_column is not None:
            self._min_timestamp, self._max_timestamp = self._table.aggregate(
                f"min({self._timestamp_column}), max({self._timestamp_column})"
            ).fetchone()
            self.select_files(time_range=[self._min_timestamp, self._max_timestamp])
        else:
            self._timestamp_column = get_timestamp_column(table=self._table)

        self._load_arrow_dataset(time_range=[self._min_timestamp, self._max_timestamp])
        self.register(f"{self.name}_table" if self.name else "_table", self._table)

    def sort(self, by: str | List[str], ascending: bool = True):
        self._table = sort_table(self._table, sort_by=by, ascending=ascending)

    def distinct(self, subset: str | List[str] | None = None, keep: str = "first"):
        self._table = distinct_table(self._table, subset=subset, keep=keep)

    def _gen_partition_path(
        self,
        partitioning: str | List[str] | None,
        partitions: str | int | float | List[Any] | None,
        flavor: str | None = None,
    ) -> str:
        if partitioning is None:
            return self._path

        if not isinstance(partitions, list | tuple):
            partitions = [partitions]

        if flavor == "hive":
            return os.path.join(
                self._path,
                *[f"{k}={v}" for k, v in zip(partitioning, partitions)],
                f"data-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M%S')}-{random_id()}.{self._format}",
            )

        return os.path.join(
            self._path,
            *[str(part) for part in partitions[: len(partitioning)]],
            f"data-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M%S')}-{random_id()}.{self._format}",
        )

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
                    ddb=self.ddb,
                    subset=subset,
                )

            else:
                self._table = get_table_delta(
                    table1=self._table,
                    table2=self.ddb_rel,
                    subset=subset,
                    ddb=self.ddb,
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
            column_names.append("year")
        if month:
            strftime.append("%m")
            column_names.append("month")
        if week:
            strftime.append("%W")
            column_names.append("week")
        if yearday:
            strftime.append("%j")
            column_names.append("year_day")
        if monthday:
            strftime.append("%d")
            column_names.append("month_day")
        if weekday:
            strftime.append("%a")
            column_names.append("week_day")

        self._table = with_strftime_column(
            table=self._table,
            timestamp_column=self._timestamp_column,
            strftime=strftime,
            column_names=column_names,
        )

    def iter_partitions(
        self,
        batch_size: int = 1_000_000,
        file_size: int | str | None = None,
        partitioning: str | List[str] | None = None,
        sort_by: str | List[str] | None = None,
        ascending: bool = True,
        distinct: bool = False,
        subset: str | None = None,
        keep: str = "first",
        preload: bool = False,
        iter_from:str="ddb_rel"
    ):
        
        partitioning = partitioning or self._partitioning

        if self._timestamp_column is not None:
            date_columns_to_add = {
                col: col in [part.lower() for part in partitioning]
                for col in [
                    "year",
                    "month",
                    "week",
                    "yearday",
                    "monthday",
                    "weekday",
                    "strftime",
                ]
                if col not in [table_col.lower() for table_col in self._table.columns]
            }
            if len(date_columns_to_add):
                self.add_date_columns(**date_columns_to_add)

        if file_size:
            batch_size = self._estimate_batch_size(file_size=file_size)

        if iter_from == "arrow":
            self._table = self._table.arrow()

        elif iter_from == "polars":
            self._table = self._table.pl()

        if partitioning:
            batches = self._partition_by(
                which="_table",
                n_rows=batch_size,
                columns=partitioning.copy(),
                as_dict=True,
                drop=True,
                sort_by=sort_by,
                ascending=ascending,
                distinct=distinct,
                subset=subset,
                presort=False,
                keep=keep,
            )
        if preload:
            batches = list(batches)

        return batches

    def write(
        self,
        batch_size: int = 1_000_000,
        file_size: str | None = None,
        partitioning: str | List[str] | None = None,
        partition_flavor: str = "dir",  # "hive" or "dir"
        mode: str | None = None,
        format: str | None = None,
        schema: pa.Schema | None = None,
        sort_by: str | List[str] | None = None,
        ascending: bool | List[bool] = True,
        distinct: bool = False,
        subset: str | List[str] | None = None,
        keep: str = "first",
        presort: bool = False,
        preload_partitions: bool = False,
        iter_from:str="ddb_rel"
    ):  # sourcery skip: avoid-builtin-shadow
        mode = mode or self._mode
        format = format or self._format
        schema = schema or self.schema if format != "csv" else None
        partitioning = partitioning or self._partitioning

        if mode == "delta":
            self._gen_table_delta(subset=subset)

        if presort:
            self.sort(by=sort_by, ascending=ascending)
            if distinct:
                self.distinct()

        partitions = self.iter_partitions(
            batch_size=batch_size,
            file_size=file_size,
            partitioning=partitioning,
            sort_by=sort_by,
            ascending=ascending,
            distinct=distinct,
            subset=subset,
            keep=keep,
            preload=preload_partitions,
            iter_from=iter_from
        )

        def _write(names, table):
            # names, table = partition
            path = self._gen_partition_path(
                partitioning=partitioning,
                partitions=list(names)[: len(partitioning)],
                flavor=partition_flavor,
            )
            write_table(
                table=table,
                path=path,
                format=format,
                filesystem=self._dir_filesystem,
                schema=schema,
            )

        _ = Parallel(n_jobs=-1, backend="threading")(
            delayed(_write)(partition[0], partition[1].arrow())
            for partition in tqdm.tqdm(partitions)
        )
        # for partition in partitions:
        #     _write(partition)

        if mode == "overwrite":
            self._dir_filesystem.rm(self.file_details["path"].to_list())


def write_dataset(
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
    batch_size: int = 1_000_000,
    file_size: str | None = None,
    partition_flavor: str = "dir",  # "hive" or "dir"
    sort_by: str | List[str] | None = None,
    ascending: bool | List[bool] = True,
    distinct: bool = False,
    subset: str | List[str] | None = None,
    keep: str = "first",
    presort: bool = False,
    preload_batches: bool = False,
    **storage_options,
):
    writer = Writer(
        table=table,
        path=path,
        bucket=bucket,
        schema=schema,
        format=format,
        filesystem=filesystem,
        partitioning=partitioning,
        timestamp_column=timestamp_column,
        ddb=ddb,
        name=name,
        mode=mode,
        **storage_options,
    )

    if isinstance(writer._partitioning, str):
        writer._partitioning = [writer._partitioning]

    # if writer._timestamp_column is not None:
    #     date_columns_to_add = {
    #         col: col in [part.lower() for part in writer._partitioning]
    #         for col in [
    #             "year",
    #             "month",
    #             "week",
    #             "yearday",
    #             "monthday",
    #             "weekday",
    #             "strftime",
    #         ]
    #         if col not in [table_col.lower() for table_col in writer._table.columns]
    #     }
    #     if len(date_columns_to_add):
    #         writer.add_date_columns(**date_columns_to_add)

    writer.write(
        batch_size=batch_size,
        file_size=file_size,
        partition_flavor=partition_flavor,
        sort_by=sort_by,
        ascending=ascending,
        distinct=distinct,
        subset=subset,
        keep=keep,
        presort=presort,
        preload_batches=preload_batches,
    )
