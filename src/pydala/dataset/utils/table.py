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
from numpy import isin

from ...utils import sort_as_sql
from .dataset import get_partitions_from_path
from .schema import (
    _convert_schema_pandas_to_polars,
    _convert_schema_pandas_to_pyarrow,
    _convert_schema_polars_to_pandas,
    _convert_schema_polars_to_pyarrow,
    _convert_schema_pyarrow_to_pandas,
    _convert_schema_pyarrow_to_polars,
)


def get_timedelta_str(timedelta: str, to: str = "polars") -> str:
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


def get_timestamp_column(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
) -> str:
    if isinstance(table, duckdb.DuckDBPyRelation):
        table = table.limit(10)
        table = to_arrow(table)
    elif isinstance(table, pl.DataFrame | pl.LazyFrame):
        table = table.head(10)
        table = to_arrow(table)

    timestamp_columns = [
        col.name
        for col in table.schema
        if col.type
        in [
            pa.timestamp("s"),
            pa.timestamp("ms"),
            pa.timestamp("us"),
            pa.timestamp("ns"),
            pa.date32(),
            pa.date64(),
        ]
    ]
    if len(timestamp_columns):
        return timestamp_columns[0]


def get_timestamp_min_max(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
    timestamp_column: str | None = None,
):
    if not timestamp_column:
        timestamp_column = get_timestamp_column(table=table)
    if timestamp_column:
        if isinstance(table, duckdb.DuckDBPyRelation):
            return table.aggregate(
                f"min({timestamp_column}), max({timestamp_column})"
            ).fetchone()

        elif isinstance(table, pa.Table | pa.dataset.Dataset):
            return (
                duckdb.from_arrow(table)
                .aggregate(f"min({timestamp_column}), max({timestamp_column})")
                .fetchone()
            )
        elif isinstance(table, pd.DataFrame):
            return (
                duckdb.from_df(table)
                .aggregate(f"min({timestamp_column}), max({timestamp_column})")
                .fetchone()
            )
        else:
            return (
                duckdb.from_arrow(to_arrow(table.select(timestamp_column)))
                .aggregate(f"min({timestamp_column}), max({timestamp_column})")
                .fetchone()
            )


def get_column_names(
    table: pa.Table
    | pa.dataset.Dataset
    | pl.DataFrame
    | pl.LazyFrame
    | pd.DataFrame
    | duckdb.DuckDBPyRelation,
):
    if isinstance(
        table, pl.DataFrame | pl.LazyFrame | pd.DataFrame | duckdb.DuckDBPyRelation
    ):
        return table.columns
    elif isinstance(table, pa.Table):
        return table.column_names
    else:
        return table.schema.names


def to_arrow(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
) -> pa.Table | pa.dataset.Dataset:
    """Converts a polars dataframe, pandas dataframe or duckdb relation
    into a pyarrow table/dataset.
    """

    if isinstance(table, pl.DataFrame):
        return table.to_arrow()

    if isinstance(table, pl.LazyFrame):
        return table.collect(streaming=True).to_arrow()

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
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
    lazy: bool = False,
) -> pl.DataFrame:
    """Converts a pyarrow table/dataset, pandas dataframe or duckdb relation
    into a polars dataframe.
    """

    if isinstance(table, pa.Table):
        return pl.from_arrow(table).lazy() if lazy else pl.from_arrow(table)

    elif isinstance(table, pd.DataFrame):
        return pl.from_pandas(table).lazy() if lazy else pl.from_pandas(table)

    elif isinstance(table, pa.dataset.Dataset):
        if isinstance(table, pa.dataset.FileSystemDataset):
            return (
                pl.scan_pyarrow_dataset(table)
                if lazy
                else pl.scan_pyarrow_dataset.collect(streaming=True)
            )
        else:
            return (
                pl.from_arrow(table.to_table()).lazy()
                if lazy
                else pl.from_arrow(table.to_table())
            )

    elif isinstance(table, duckdb.DuckDBPyRelation):
        return table.pl().lazy() if lazy else table.pl()

    else:
        if lazy:
            return table if isinstance(table, pl.LazyFrame) else table.lazy()
        return table


def to_pandas(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
) -> pd.DataFrame:
    """Converts a pyarrow table/dataset, polars dataframe or duckdb relation
    into a pandas dataframe
    """
    if isinstance(table, pa.Table):
        return table.to_pandas(types_mapper=pd.ArrowDtype)

    elif isinstance(table, pl.DataFrame):
        return table.to_pandas(use_pyarrow_extension_array=True)

    elif isinstance(table, pl.LazyFrame):
        return table.collect(streaming=True).to_pandas(use_pyarrow_extension_array=True)

    elif isinstance(table, pa.dataset.Dataset):
        return table.to_table().to_pandas(types_mapper=pd.ArrowDtype)

    elif isinstance(table, duckdb.DuckDBPyRelation):
        return table.arrow().to_pandas(types_mapper=pd.ArrowDtype)

    else:
        return table


def to_relation(
    table: duckdb.DuckDBPyRelation
    | pa.Table
    | pa.dataset.Dataset
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | str,
    # ddb: duckdb.DuckDBPyConnection | None = None,
    ddb: duckdb.DuckDBPyConnection | None = None,
    **kwargs,
) -> duckdb.DuckDBPyRelation:
    """Converts a pyarrow table/dataset, pandas dataframe or polars dataframe
    into a duckdb relation
    """
    if ddb is None:
        ddb = duckdb.connect()

    if isinstance(table, pa.Table):
        return ddb.from_arrow(table)

    elif isinstance(table, pa.dataset.Dataset):
        return ddb.from_arrow(table)

    elif isinstance(table, pd.DataFrame):
        return ddb.from_df(table)

    elif isinstance(table, pl.DataFrame | pl.LazyFrame):
        ddb.register("_pl_", table)
        return ddb.from_query("FROM _pl_")

    elif isinstance(table, str):
        if ".parquet" in table:
            table = ddb.from_parquet(table, **kwargs)

        elif ".csv" in table:
            table = ddb.from_csv_auto(table, **kwargs)
        else:
            table = ddb.from_query(f"SELECT * FROM '{table}'")

        return table

    elif isinstance(table, duckdb.DuckDBPyRelation):
        # ddb.register("_table_", table)
        # return ddb.from_query("FROM _table_")
        return table


def sort_table(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
    sort_by: str | List[str] | Tuple[str] | None,
    ascending: bool | List[bool] | Tuple[bool] | None,
) -> (
    pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset
):
    "Sort a pyarrow table, pandas or polars dataframe or duckdb relation."
    if not sort_by:
        return table

    if ascending is None:
        ascending = True

    if isinstance(sort_by, str):
        sort_by = [sort_by]

    if isinstance(table, pa.Table | pa.dataset.Dataset):
        order = "ascending" if ascending else "descending"
        return table.sort_by([(col, order) for col in sort_by])

    elif isinstance(table, pd.DataFrame):
        return table.sort_values(by=sort_by, ascending=ascending).to_pandas()

    elif isinstance(table, pl.DataFrame | pl.LazyFrame):
        descending = (
            not ascending
            if isinstance(ascending, bool)
            else [not el for el in ascending]
        )
        return table.sort(by=sort_by, descending=descending)

    elif isinstance(table, duckdb.DuckDBPyRelation):
        sort_by_sql = sort_as_sql(sort_by=sort_by, ascending=ascending)
        return table.order(sort_by_sql)


def concat_tables(
    tables: List[pa.Table]
    | List[pl.DataFrame]
    | List[pl.LazyFrame]
    | List[pd.DataFrame]
    | List[pa.dataset.Dataset]
    | List[duckdb.DuckDBPyRelation],
    schema: pa.Schema | dict | None = None,
):
    if (
        isinstance(tables[0], pa.Table | pd.DataFrame | pl.DataFrame | pl.LazyFrame)
        and schema is not None
    ):
        tables = [cast_schema(table) for table in tables]

    if isinstance(tables[0], pds.Dataset):
        return pds.Dataset(tables)

    elif isinstance(tables[0], pa.Table):
        return pa.concat_tables(tables, promote=True)

    elif isinstance(tables[0], duckdb.DuckDBPyRelation):
        table = tables[0]
        for table_ in tables[1:]:
            table = table.union(table_)

        return table

    elif isinstance(tables[0], pl.DataFrame | pl.LazyFrame):
        return pl.concat(tables, how="diagonal")

    elif isinstance(tables[0], pd.DataFrame):
        return pd.concat([tables])


def reorder_columns(
    table: pa.Table
    | pl.DataFrame
    | pd.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation,
    columns: list,
):
    if isinstance(table, pd.DataFrame):
        return table[columns]
    if isinstance(table, pa.Table | pl.DataFrame | pl.LazyFrame):
        return table.select(columns)
    if isinstance(table, duckdb.DuckDBPyRelation):
        return table.project(",".join(columns))

    return table


def cast_schema(
    table: pa.Table | pd.DataFrame | pl.DataFrame, schema: pa.Schema | dict
):
    if isinstance(table, pa.Table):
        if isinstance(schema, dict):
            if isinstance(list(schema.value())[0], pl.datatyes.DataTypeClass):
                schema = _convert_schema_polars_to_pyarrow(schema)
            else:
                schema = _convert_schema_pandas_to_pyarrow(schema)

        if len(table.schema.names) < len(schema.names):
            missing_names = list(set(table.schema.names) - set(schema.names))
            for missing_name in missing_names:
                table = table.add_column(
                    schema.names.index(missing_name),
                    missing_name,
                    pa.array([None] * len(table)),
                )
        if schema == table.schema:
            return table

        return table.select(schema.names).cast(schema)

    if isinstance(table, pd.DataFrame):
        if isinstance(schema, pa.Schema):
            schema = _convert_schema_pyarrow_to_pandas(schema)

        if isinstance(list(schema.values)[0], pl.datatypes.DataTypeClass):
            schema = _convert_schema_polars_to_pandas(schema)

        if len(table.columns) < len(schema):
            missing_names = list(set(table.columns) - set(schema.keys()))
            for missing_name in missing_names:
                table = table.insert(
                    list(schema.keys().index(missing_name)), missing_name, None
                )
        return table if table.dtypes.to_dict == schema else table.astype(schema)
    if isinstance(table, pl.DataFrame | pl.LazyFrame):
        if isinstance(schema, pa.Schema):
            schema = _convert_schema_pyarrow_to_polars(schema)

        if isinstance(list(schema.values)[0], pl.datatypes.DataTypeClass):
            schema = _convert_schema_pandas_to_polars(schema)

        if len(table.columns) < len(schema):
            missing_names = list(set(table.columns) - set(schema.keys()))
            for missing_name in missing_names:
                table = table.insert_at_idx(
                    list(schema.keys().index(missing_name)),
                    missing_name,
                    pl.Series([None] * len(table)),
                )
        if table.schema == schema:
            return table

        return table.with_columns(
            [pl.col(col).cast(type_) for col, type_ in schema.items()]
        )


def unify_schema(
    tables: List[pa.Table]
    | List[pl.DataFrame]
    | List[pl.LazyFrame]
    | List[pd.DataFrame],
    schema: pa.Schema | dict,
):
    return [cast_schema(table, schema) for table in tables]


def get_table_delta(
    table1: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset
    | str,
    table2: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset
    | str,
    ddb: duckdb.DuckDBPyConnection,
    subset: list | None = None,
    # cast_as_str: bool = False,
) -> (
    pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset
):
    if not ddb:  # is None:
        ddb = duckdb.connect()

    table1_ = to_relation(table1, ddb=ddb)
    table2_ = to_relation(table2, ddb=ddb)

    if subset:
        # if cast_as_str:
        #     subset_types = table1_.project(",".join(subset)).types
        #     subset_table1_ = table1_.project(
        #         ",".join([f"CAST({col} as STRING) as {col}" for col in subset])
        #     )
        #     subset_table2_ = table2_.project(
        #         ",".join([f"CAST({col} as STRING) as {col}" for col in subset])
        #     )

        #     diff_ = subset_table1_.except_(subset_table2_).project(
        #         ",".join(
        #             [
        #                 f"CAST({col} as {type_}) as {col}"
        #                 for col, type_ in zip(subset, subset_types)
        #             ]
        #         )
        #     )

        # else:
        # subset_types = None

        subset_table1_ = table1_.project(",".join(subset))
        subset_table2_ = table2_.project(",".join(subset))

        diff_ = subset_table1_.except_(subset_table2_)

        diff = to_polars(table1, lazy=False).filter(
            pl.struct(subset).is_in(diff_.arrow().to_pylist())
        )

    else:
        diff = table1_.except_(table2_.project(",".join(table1_.columns)))

    if isinstance(table1, pa.Table | pa.dataset.Dataset):
        return (
            diff.arrow() if isinstance(table1, pa.Table) else pds.dataset(diff.arrow())
        )

    elif isinstance(table1, pd.DataFrame):
        return to_pandas(diff)

    elif isinstance(table1, pl.DataFrame):
        return to_polars(diff, lazy=False)

    elif isinstance(table1, pl.LazyFrame):
        return to_polars(diff, lazy=True)

    elif isinstance(table1, duckdb.DuckDBPyRelation):
        return diff

    else:
        return diff.to_arrow() if isinstance(diff, pl.DataFrame) else diff.arrow()


def distinct_table(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
    subset: list | None = None,
    keep: str = "first",
    sort_by: str | list | None = None,
    ascending: bool | List[bool] = True,
    presort: bool = False,
) -> pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation:
    if isinstance(
        table, (pa.Table, pd.DataFrame, pl.DataFrame, pa.dataset.Dataset, pl.LazyFrame)
    ):
        table_ = to_polars(table=table, lazy=True)
        if presort and sort_by:
            table_ = sort_table(table=table_, sort_by=sort_by, ascending=ascending)

        if subset:
            columns = [col for col in table_.columns if col not in subset]

        table_ = table_.unique(subset=subset, keep=keep, maintain_order=True)

        if sort_by:
            table_ = sort_table(table=table_, sort_by=sort_by, ascending=ascending)

        if isinstance(table, pd.DataFrame):
            return to_pandas(table_)
        elif isinstance(table, pa.dataset.Dataset):
            return pds.dataset(table_.collect(streaming=True).to_arrow())
        elif isinstance(table, pa.Table):
            return to_arrow(table_)
        elif isinstance(table_, pl.DataFrame):
            return to_polars(table_, lazy=False)
        elif isinstance(table_, pl.LazyFrame):
            return to_polars(table_, lazy=True)

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
    | pl.LazyFrame
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

        elif isinstance(table, pl.DataFrame | pd.DataFrame | pl.LazyFrame):
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
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation
    | pa.dataset.Dataset,
    timestamp_column: str,
    strftime: str | List[str],
    column_names: str | List[str] | None = None,
):
    if isinstance(strftime, str):
        strftime = [strftime]
    if isinstance(column_names, str):
        column_names = [column_names]

    if column_names is None:
        column_names = [
            f"_strftime_{strftime_.replace('%', '').replace('-', '_')}_"
            for strftime_ in strftime
        ]

    if isinstance(table, duckdb.DuckDBPyRelation):
        return table.project(
            ",".join(
                table.columns
                + [
                    f"strftime({timestamp_column}, '{strftime_}') as {column_name}"
                    for strftime_, column_name in zip(strftime, column_names)
                ]
            )
        )

    table_ = to_polars(table)

    table_ = table_.with_columns(
        [
            pl.col(timestamp_column).dt.strftime(strftime_).alias(column_name)
            for strftime_, column_name in zip(strftime, column_names)
        ]
    )
    if isinstance(table, pa.Table):
        return to_arrow(table_)

    elif isinstance(table, pa._dataset.Dataset):
        return pds.dataset(table_.collect(streaming=True).to_arrow())

    elif isinstance(table, pd.DataFrame):
        return to_pandas(table_)
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
    column_names: str | List[str] | None = None,
):
    if isinstance(timedelta, str):
        timedelta = [timedelta]

    if isinstance(column_names, str):
        column_names = [column_names]

    if column_names is None:
        column_names = [
            f"_timebucket_{timedelta_.replace(' ', '_')}_" for timedelta_ in timedelta
        ]

    if isinstance(table, duckdb.DuckDBPyRelation):
        timedelta = [
            get_timedelta_str(timedelta_, to="duckdb") for timedelta_ in timedelta
        ]
        return table.project(
            ",".join(
                table.columns
                + [
                    f"time_bucket(INTERVAL '{timedelta_}', {timestamp_column}) as {column_name}"
                    for timedelta_, column_name in zip(timedelta, column_names)
                ]
            )
        )

    table_ = to_polars(table)
    timedelta = [get_timedelta_str(timedelta_, to="polars") for timedelta_ in timedelta]
    table_ = table_.with_columns(
        [
            pl.col(timestamp_column).dt.truncate(timedelta_).alias(column_name)
            for timedelta_, column_name in zip(timedelta, column_names)
        ]
    )
    if isinstance(table, pa.Table):
        return to_arrow(table_)
    elif isinstance(table, pa.dataset.Dataset):
        return pds.dataset(table_.collect("streaming").to_arrow())
    elif isinstance(table, pd.DataFrame):
        return to_pandas(table_)
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

    if isinstance(table, pa.Table):
        return to_arrow(table_)
    elif isinstance(table, pa.dataset.Dataset):
        return pds.dataset(table_.collect(streaming=True).to_arrow())
    elif isinstance(table, pd.DataFrame):
        return to_pandas(table_)

    return table_


def add_date_columns(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | pa._dataset.Dataset,
    timestamp_column: str | None = None,
    year: bool = False,
    month: bool = False,
    week: bool = False,
    yearday: bool = False,
    monthday: bool = False,
    weekday: bool = False,
    strftime: str | None = None,
):
    if not timestamp_column:
        timestamp_column = get_timestamp_column(table)

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

    return with_strftime_column(
        table=table,
        timestamp_column=timestamp_column,
        strftime=strftime,
        column_names=column_names,
    )


def partition_by(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | pl.LazyFrame
    | duckdb.DuckDBPyRelation,
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

    date_columns_to_add = {
        col: col in [col.lower() for col in columns]
        for col in [
            "year",
            "month",
            "week",
            "yearday",
            "monthday",
            "weekday",
            "strftime",
        ]
        if col
        not in [table_col.lower() for table_col in get_column_names(table=table_)]
    }
    if len(date_columns_to_add):
        table_ = add_date_columns(
            table=table_, timestamp_column=timestamp_column, **date_columns_to_add
        )

    if n_rows:
        table_ = with_row_count(table_, over=columns)
        columns.append("row_nr")
        drop_columns_.append("row_nr")

    if not isinstance(table, duckdb.DuckDBPyRelation):
        table_ = to_polars(table_, lazy=True)
        if n_rows:
            table_ = table_.with_columns(pl.col("row_nr") // (n_rows + 1))

        table_parts = table_.collect(streaming=True).partition_by(
            columns, as_dict=as_dict
        )

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
                {k: to_arrow(table_parts[k]) for k in table_parts}
                if as_dict
                else [to_arrow(p) for p in table_parts]
            )
        if isinstance(table, pa.dataset.Dataset):
            table_parts = (
                {k: pds.dataset(table_parts[k].to_arrow()) for k in table_parts}
                if as_dict
                else [pds.dataset(p.to_arrow()) for p in table_parts]
            )

        elif isinstance(table, pd.DataFrame):
            table_parts = (
                {k: to_pandas(table_parts[k]) for k in table_parts}
                if as_dict
                else [to_pandas(p) for p in table_parts]
            )

        yield from table_parts.items() if as_dict else table_parts

    else:
        if n_rows:
            table_ = table_.project(
                f"* exclude(row_nr), cast(floor(row_nr / {n_rows}) as int) as row_nr"
            )

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
        partitions = get_partitions_from_path(path, partitioning=partitioning)

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
    row_group_size: int | None = None,
    compression: str = "zstd",
    **kwargs,
):  # sourcery skip: avoid-builtin-shadow
    if filesystem is None:
        filesystem = fsspec_filesystem("file")

    table = to_arrow(table)
    format = format or os.path.splitext(path)[-1]
    schema = kwargs.pop(schema, None) or schema or table.schema

    if re.sub("\.", "", format) == "parquet":
        pq.write_table(
            table,
            path,
            filesystem=filesystem,
            row_group_size=row_group_size,
            compression=compression,
            **kwargs,
        )

    elif re.sub("\.", "", format) == "csv":
        with filesystem.open_output_stream(path) as f:
            table = pa.table.from_batches(table.to_batches(), schema=schema)
            pc.write_csv(table, f, **kwargs)

    elif re.sub("\.", "", format) in ["arrow", "ipc", "feather"]:
        with filesystem.open_output_scream(path) as f:
            table = pa.table.from_batches(table.to_batches(), schema=schema)
            pf.write_feather(
                f, compression=compression, chunksize=row_group_size, **kwargs
            )


import pyarrow.dataset as pds
import duckdb

from fsspec import filesystem


fs = filesystem(key="volker", secret="s78anwg9", endpoint_url="http://192.168.2.50:9000", protocol="s3")
con=duckdb.connect()
ds=pds.dataset("yfin-db/symbol_info", partitioning="hive", filesystem=fs)
symbols=con.sql("FROM ds").filter("exchange='NMS'").project("symbol").distinct().pl()["symbol"].to_list()