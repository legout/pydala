# %%
import os
import re
from time import strftime
from typing import Dict, List, Tuple

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.dataset as pds
import pyarrow.feather as pf
import pyarrow.parquet as pq
from fsspec import filesystem as fsspec_filesystem
from fsspec.spec import AbstractFileSystem

from ...utils import sort_as_sql
from .dataset import get_partitions


def get_timedelta_str(timedelta: str, to: str = "polars"):
    polars_timedelta_units = [
        "ns",
        "us",
        "ms",
        "s",
        "m",
        "h",
        "d",
        "w",
        "mo",
        "y",
    ]
    duckdb_timedelta_units = [
        "nanosecond",
        "microsecond",
        "millisecond",
        "second",
        "minute",
        "hour",
        "day",
        "week",
        "month",
        "year",
    ]

    unit = re.sub("[0-9]", "", timedelta).strip()
    val = timedelta.replace(unit, "").strip()
    if to == "polars":
        return (
            timedelta
            if unit in polars_timedelta_units
            else val
            + dict(zip(duckdb_timedelta_units, polars_timedelta_units))[
                re.sub("s$", "", unit)
            ]
        )

    if unit in polars_timedelta_units:
        return (
            f"{val} " + dict(zip(polars_timedelta_units, duckdb_timedelta_units))[unit]
        )

    return f"{val} " + re.sub("s$", "", unit)


def to_arrow(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
) -> pl.DataFrame:
    """Converts a polars dataframe, pandas dataframe or duckdb relation
    into a pyarrow table/dataset.
    """

    if isinstance(table, pl.DataFrame):
        return table.to_arrow()

    elif isinstance(table, pd.DataFrame):
        return pa.Table.from_pandas(table)

    elif isinstance(table, pa.dataset.Dataset):
        return table

    elif isinstance(table, duckdb.DuckDBPyRelation):
        return table.arrow()

    else:
        return table


def to_polars(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
) -> pl.DataFrame:
    """Converts a pyarrow table/dataset, pandas dataframe or duckdb relation
    into a polars dataframe.
    """

    if isinstance(table, pa.Table):
        return pl.from_arrow(table)

    elif isinstance(table, pd.DataFrame):
        return pl.from_pandas(table)

    elif isinstance(table, pa.dataset.Dataset):
        return pl.scan_pyarrow_dataset(table)

    elif isinstance(table, duckdb.DuckDBPyRelation):
        return table.pl()

    else:
        return table


def to_pandas(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
) -> pd.DataFrame:
    """Converts a pyarrow table/dataset, polars dataframe or duckdb relation
    into a pandas dataframe
    """
    if isinstance(table, pa.Table):
        return table.to_pandas()

    elif isinstance(table, pl.DataFrame):
        return table.to_pandas()

    elif isinstance(table, pa.dataset.Dataset):
        return table.to_table().to_pandas()

    elif isinstance(table, duckdb.DuckDBPyRelation):
        return table.df()

    else:
        return table


def to_relation(
    table: duckdb.DuckDBPyRelation
    | pa.Table
    | pa.dataset.Dataset
    | pd.DataFrame
    | pl.DataFrame
    | str,
    # ddb: duckdb.DuckDBPyConnection | None = None,
    name: str | None = None,
    **kwargs,
) -> duckdb.DuckDBPyRelation:
    """Converts a pyarrow table/dataset, pandas dataframe or polars dataframe
    into a duckdb relation
    """
    # if ddb is None:
    #    ddb = duckdb.connect()

    if isinstance(table, pa.Table):
        if name is not None:
            return duckdb.from_arrow(table).set_alias(name)
        else:
            return duckdb.from_arrow(table)

    elif isinstance(table, pa.dataset.Dataset):
        if name is not None:
            return duckdb.from_arrow(table).set_alias(name)
        else:
            return duckdb.from_arrow(table)

    elif isinstance(table, pd.DataFrame):
        if name is not None:
            return duckdb.from_df(table).set_alias(name)
        else:
            return duckdb.from_df(table)

    elif isinstance(table, pl.DataFrame):
        if name is not None:
            return duckdb.from_arrow(table.to_arrow()).set_alias(name)
        else:
            return duckdb.from_arrow(table.to_arrow())

    elif isinstance(table, str):
        if ".parquet" in table:
            table = (
                duckdb.from_parquet(table, **kwargs).set_alias(name)
                if name is not None
                else duckdb.from_parquet(table, **kwargs)
            )
        elif ".csv" in table:
            if name is not None:
                table = duckdb.from_csv_auto(table, **kwargs).set_alias(name)
            else:
                table = duckdb.from_csv_auto(table, **kwargs)
        elif name is not None:
            table = duckdb.from_query(f"SELECT * FROM '{table}'").set_alias(name)
        else:
            table = duckdb.from_query(f"SELECT * FROM '{table}'")

        return table

    elif isinstance(table, duckdb.DuckDBPyRelation):
        return table


def sort_table(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
    sort_by: str | List[str] | Tuple[str] | None,
    ascending: bool | List[bool] | Tuple[bool] | None,
) -> pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation:
    "Sort a pyarrow table, pandas or polars dataframe or duckdb relation."
    if not sort_by:
        return table

    if ascending is None:
        ascending = True

    descending = (
        not ascending if isinstance(ascending, bool) else [not el for el in ascending]
    )
    if isinstance(table, pa.Table):
        return to_polars(table=table).sort(by=sort_by, descending=descending).to_arrow()

    elif isinstance(table, pd.DataFrame):
        return (
            to_polars(table=table).sort(by=sort_by, descending=descending).to_pandas()
        )

    elif isinstance(table, pa.dataset.Dataset):
        return pds.dataset(
            to_polars(table=table)
            .sort(by=sort_by, descending=descending)
            .collect()
            .to_arrow(),
        )

    elif isinstance(table, pl.DataFrame):
        return table.sort(by=sort_by, descending=descending)

    elif isinstance(table, duckdb.DuckDBPyRelation):
        sort_by_sql = sort_as_sql(sort_by=sort_by, ascending=ascending)
        return table.order(sort_by_sql)


def get_table_delta(
    table1: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset
    | str,
    table2: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset
    | str,
    # ddb: duckdb.DuckDBPyConnection,
    subset: list | None = None,
    cast_as_str: bool = False,
) -> pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation:
    # if not ddb:  # is None:
    #    ddb = duckdb.connect()

    table1_ = to_relation(table1)
    table2_ = to_relation(table2)

    if subset:
        if cast_as_str:
            subset_types = table1_.project(",".join(subset)).types
            subset_table1_ = table1_.project(
                ",".join([f"CAST({col} as STRING) as {col}" for col in subset])
            )
            subset_table2_ = table2_.project(
                ",".join([f"CAST({col} as STRING) as {col}" for col in subset])
            )

            diff_ = subset_table1_.except_(subset_table2_).project(
                ",".join(
                    [
                        f"CAST({col} as {type_}) as {col}"
                        for col, type_ in zip(subset, subset_types)
                    ]
                )
            )

        else:
            subset_types = None
            subset_table1_ = table1_.project(",".join(subset))
            subset_table2_ = table2_.project(",".join(subset))

            diff_ = subset_table1_.except_(subset_table2_)

        diff = to_polars(table1).filter(
            pl.struct(subset).is_in(diff_.arrow().to_pylist())
        )

    else:
        diff = table1_.except_(table2_.project(",".join(table1_.columns)))

    if isinstance(table1, (pa.Table, pa.dataset.Dataset)):
        return (
            pds.dataset(diff.to_arrow())
            if isinstance(diff, pl.DataFrame)
            else pds.dataset(diff.arrow())
        )

    elif isinstance(table1, pd.DataFrame):
        return to_pandas(diff)

    elif isinstance(table1, pl.DataFrame):
        return to_polars(diff)

    elif isinstance(table1, duckdb.DuckDBPyRelation):
        return diff

    else:
        return diff.to_arrow() if isinstance(diff, pl.DataFrame) else diff.arrow()


def distinct_table(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
    # ddb: duckdb.DuckDBPyConnection,
    subset: list | None = None,
    keep: str = "first",
    sort_by: str | list | None = None,
    ascending: bool | List[bool] = True,
    presort: bool = False,
) -> pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation:
    if isinstance(table, (pa.Table, pd.DataFrame, pl.DataFrame, pa.dataset.Dataset)):
        table_ = to_polars(table=table)
        if presort and sort_by:
            table_ = sort_table(
                table=table_, sort_by=sort_by, ascending=ascending
            )  # , ddb=ddb)

        if subset:
            columns = [col for col in table_.columns if col not in subset]

        table_ = table_.unique(subset=subset, keep=keep, maintain_order=True)

        if sort_by:
            table_ = sort_table(
                table=table_, sort_by=sort_by, ascending=ascending
            )  # , ddb=ddb)

        if isinstance(table, pd.DataFrame):
            return table_.to_pandas()
        elif isinstance(table, pa.dataset.Dataset):
            return pds.dataset(table_.to_arrow())
        elif isinstance(table, pa.Table):
            return table_.to_arrow()

        else:
            return table_

    else:
        table_ = table
        if presort and sort_by:
            table_ = table_.order(sort_as_sql(sort_by=sort_by, ascending=ascending))
        if not subset:
            table_ = table.distinct()
        else:
            subset = ",".join(subset)
            columns = [
                f"FIRST({col}) as {col}"
                if keep.lower() == "first"
                else f"LAST({col}) as {col}"
                for col in table.columns
                if col not in subset
            ]
            table_ = table_.aggregate(f"{subset},{','.join(columns)}", subset)
        if sort_by:
            table_ = table_.order(sort_as_sql(sort_by=sort_by, ascending=ascending))

        return table_


def drop_columns(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
    columns: str | List[str] | None = None,
) -> pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation:
    if isinstance(columns, str):
        columns = [columns]

    if columns:
        if isinstance(table, pa.Table):
            if columns := [col for col in columns if col in table.column_names]:
                return table.drop(columns=columns)

        elif isinstance(table, pl.DataFrame | pd.DataFrame):
            if columns := [col for col in columns if col in table.columns]:
                return table.drop(columns=columns)

        elif isinstance(table, duckdb.DuckDBPyRelation):
            columns = [
                f"'{col}'" if " " in col else col
                for col in table.columns
                if col not in columns
            ]
            if columns:
                return table.project(",".join(columns))

        elif isinstance(table, pa.dataset.Dataset):
            if columns := [col for col in columns if col in table.schema.names]:
                return pds.dataset(table.to_table().drop(columns))

    return table


def with_strftime_column(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
    timestamp_column: str,
    strftime: str | List[str],
):
    if isinstance(strftime, str):
        strftime = [strftime]

    if isinstance(table, duckdb.DuckDBPyRelation):
        return table.project(
            ",".join(
                table.columns
                + [
                    f"strftime({timestamp_column}, '{strftime_}') as _strftime_{strftime_.replace('%', '').replace('-','_')}_"
                    for strftime_ in strftime
                ]
            )
        )

    table_ = to_polars(table)

    table_ = table_.with_columns(
        [
            pl.col(timestamp_column)
            .dt.strftime(strftime_)
            .alias(f"_strftime_{strftime_.replace('%', '').replace('-','_')}_")
            for strftime_ in strftime
        ]
    )
    if isinstance(table, pa.Table | pa._dataset.Dataset):
        return table_.to_arrow()
    elif isinstance(table, pd.DataFrame):
        return table_.to_pandas()
    else:
        return table_


def with_timebucket_column(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa._dataset.Dataset,
    timestamp_column: str,
    timedelta: str | List[str],
):
    if isinstance(timedelta, str):
        timedelta = [timedelta]

    if isinstance(table, duckdb.DuckDBPyRelation):
        timedelta = [
            get_timedelta_str(timedelta_, to="duckdb") for timedelta_ in timedelta
        ]
        return table.project(
            ",".join(
                table.columns
                + [
                    f"time_bucket(INTERVAL '{timedelta_}', {timestamp_column}) as _timebucket_{timedelta_.replace(' ', '_')}_"
                    for timedelta_ in timedelta
                ]
            )
        )

    table_ = to_polars(table)
    timedelta = [get_timedelta_str(timedelta_, to="polars") for timedelta_ in timedelta]
    table_ = table_.with_columns(
        [
            pl.col(timestamp_column)
            .dt.truncate(timedelta_)
            .alias(f"_timebucket_{timedelta_.replace(' ', '_')}_")
            for timedelta_ in timedelta
        ]
    )
    if isinstance(table, pa.Table | pa._dataset.Dataset):
        return table_.to_arrow()
    elif isinstance(table, pd.DataFrame):
        return table_.to_pandas()
    else:
        return table_


def with_row_count(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa._dataset.Dataset,
    over: str | List[str] | None = None,
):
    if over:
        if len(over) == 0:
            over = None

    if isinstance(over, str):
        over = [over]

    if isinstance(table, duckdb.DuckDBPyRelation):
        if over:
            return table.project(
                f"*,row_number() over(partition by {','.join(over)}) as row_nr"
            )

        return table.project("*, row_number() over() as row_nr")

    table_ = to_polars(table)

    if over:
        table_ = table_.with_columns(pl.lit(1).alias("row_nr")).with_columns(
            pl.col("row_nr").cumsum().over(over)
        )
    else:
        table_ = table_.with_columns(pl.lit(1).alias("row_nr")).with_columns(
            pl.col("row_nr").cumsum()
        )

    if isinstance(table, pa.Table | pa._dataset.Dataset):
        return table_.to_arrow()
    elif isinstance(table, pd.DataFrame):
        return table_.to_pandas()

    return table_


def partition_by(
    table: pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation,
    timestamp_column: str | None = None,
    columns: str | List[str] | None = None,
    strftime: str | List[str] | None = None,
    timedelta: str | List[str] | None = None,
    n_rows: int | None = None,
    as_dict: bool = False,
    drop: bool = False,
    sort_by: str | List[str] | None = None,
    ascending: bool | List[bool] = True,
    distinct: bool = True,
    subset: str | List[str] | None = None,
    keep: str = "first",
    presort: bool = False,
):
    if columns is None:
        columns = []

    if isinstance(columns, str):
        columns = [columns]

    drop_columns_ = columns.copy() if drop else []

    table_ = table

    if distinct:
        table_ = distinct_table(table=table_, subset=subset, keep=keep)

    if presort:
        table_ = sort_table(table=table_, sort_by=sort_by, ascending=ascending)

    if strftime is not None:
        if isinstance(strftime, str):
            strftime = [strftime]

        table_ = with_strftime_column(
            table_, timestamp_column=timestamp_column, strftime=strftime
        )
        strftime_columns = [
            f"_strftime_{strftime_.replace('%', '').replace('-','_')}_"
            for strftime_ in strftime
        ]
        columns += strftime_columns
        drop_columns_ += strftime_columns

    if timedelta is not None:
        if isinstance(timedelta, str):
            timedelta = [timedelta]

        table_ = with_timebucket_column(
            table_, timestamp_column=timestamp_column, timedelta=timedelta
        )
        timebucket_columns = [
            f"_timebucket_{get_timedelta_str(timedelta_, to='duckdb' if isinstance(table, duckdb.DuckDBPyRelation) else 'polars').replace(' ', '_')}_"
            for timedelta_ in timedelta
        ]
        columns += timebucket_columns
        drop_columns_ += timebucket_columns

    if n_rows:
        table_ = with_row_count(table_, over=columns)
        columns.append("row_nr")
        drop_columns_.append("row_nr")

    if distinct:
        table_ = distinct_table(table=table_, subset=subset, keep=keep)

    if presort:
        table_ = sort_table(table=table_, sort_by=sort_by, ascending=ascending)

    if not isinstance(table, duckdb.DuckDBPyRelation):
        table_ = to_polars(table_)
        if n_rows:
            table_ = table_.with_columns(pl.col("row_nr") // (n_rows + 1))

        table_parts = table_.partition_by(columns, as_dict=as_dict)

        table_parts = (
            {
                k: sort_table(
                    drop_columns(table_parts[k], columns=drop_columns_),
                    sort_by=sort_by,
                    ascending=ascending,
                )
                for k in table_parts
            }
            if as_dict
            else [
                sort_table(
                    drop_columns(p, columns=drop_columns_),
                    sort_by=sort_by,
                    ascending=ascending,
                )
                for p in table_parts
            ]
        )

        if isinstance(table, pa.Table):
            table_parts = (
                {k: table_parts[k].to_arrow() for k in table_parts}
                if as_dict
                else [p.to_arrow() for p in table_parts]
            )
        elif isinstance(table, pd.DataFrame):
            table_parts = (
                {k: table_parts[k].to_pandas() for k in table_parts}
                if as_dict
                else [p.to_pandas() for p in table_parts]
            )

        yield from table_parts.items() if as_dict else table_parts

    else:
        if n_rows:
            table_ = table_.project(f"* exclude(row_nr), row_nr // {n_rows} as row_nr")

        partitions = duckdb.from_arrow(
            table_.project(", ".join(columns))
            .pl()
            .unique(maintain_order=True)
            .to_arrow()
        ).fetchall()

        yield from {
            partition[0]
            if len(partition) == 1
            else partition: sort_table(
                drop_columns(
                    table_.filter(
                        " AND ".join(
                            [
                                f"{col}='{partition_}'"
                                for col, partition_ in zip(columns, partition)
                            ]
                        )
                    ),
                    columns=drop_columns_,
                ),
                sort_by=sort_by,
                ascending=ascending,
            )
            for partition in partitions
        }.items() if as_dict else [
            sort_table(
                drop_columns(
                    table_.filter(
                        " AND ".join(
                            [
                                f"{col}='{partition_}'"
                                for col, partition_ in zip(columns, partition)
                            ]
                        )
                    ),
                    columns=drop_columns_,
                ),
                sort_by=sort_by,
                ascending=ascending,
            )
            for partition in partitions
        ]


def read_table(
    path: str,
    schema: pa.Schema | None = None,
    format: str | None = None,
    filesystem: AbstractFileSystem | None = None,
    partitioning: str | List[str] | None = None,
) -> pa.Table:  # sourcery skip: avoid-builtin-shadow
    if filesystem is None:
        filesystem = fsspec_filesystem("file")

    format = format or os.path.splitext(path)[-1]

    if re.sub("\.", "", format) == "parquet":
        table = pq.read_table(
            pa.BufferReader(filesystem.read_bytes(path)), schema=schema
        )

    elif re.sub("\.", "", format) == "csv":
        table = pc.read_csv(pa.BufferReader(filesystem.read_bytes(path)), schema=schema)

    elif re.sub("\.", "", format) in ["arrow", "ipc", "feather"]:
        table = pf.read_table(
            pa.BufferReader(filesystem.read_bytes(path)), schema=schema
        )

    if partitioning is not None:
        partitions = get_partitions(path, partitioning=partitioning)

        for key, values in partitions:
            table = table.append_column(
                field_=key, column=pa.array([values] * len(table))
            )

    return table


def write_table(
    table: pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation,
    path: str,
    schema: pa.Schema | None = None,
    format: str | None = None,
    filesystem: AbstractFileSystem | None = None,
    **kwargs,
):
    if filesystem is None:
        filesystem = fsspec_filesystem("file")

    table = to_arrow(table)
    format = format or os.path.splitext(path)[-1]
    schema = kwargs.pop(schema, None) or schema or table.schema

    if re.sub("\.", "", format) == "parquet":
        pq.write_table(table, path, filesystem=filesystem, **kwargs)

    elif re.sub("\.", "", format) == "csv":
        with filesystem.open_output_stream(path) as f:
            table = pa.table.from_batches(table.to_batches(), schema=schema)
            pc.write_csv(table, f, **kwargs)

    elif re.sub("\.", "", format) in ["arrow", "ipc", "feather"]:
        compression = kwargs.pop("compression", None) or "uncompressed"
        with filesystem.open_output_scream(path) as f:
            table = pa.table.from_batches(table.to_batches(), schema=schema)
            pf.write_feather(f, compression=compression, **kwargs)
