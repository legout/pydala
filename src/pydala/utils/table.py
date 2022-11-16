import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds


def to_polars(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | ds.FileSystemDataset,
) -> pl.DataFrame:

    if isinstance(table, pa.Table):
        pl_dataframe = pl.from_arrow(table)

    elif isinstance(table, pd.DataFrame):
        pl_dataframe = pl.from_pandas(table)

    elif isinstance(table, ds.FileSystemDataset):
        pl_dataframe = pl.from_arrow(table.to_table())

    elif isinstance(table, duckdb.DuckDBPyRelation):
        pl_dataframe = pl.from_arrow(table.arrow())

    else:
        pl_dataframe = table

    return pl_dataframe


def to_pandas(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | ds.FileSystemDataset,
) -> pd.DataFrame:

    if isinstance(table, pa.Table):
        pd_dataframe = table.to_pandas()

    elif isinstance(table, pl.DataFrame):
        pd_dataframe = table.to_pandas()

    elif isinstance(table, ds.FileSystemDataset):
        pd_dataframe = table.to_table().to_pandas()

    elif isinstance(table, duckdb.DuckDBPyRelation):
        pd_dataframe = table.df()

    else:
        pd_dataframe = table

    return pd_dataframe


def to_relation(
    table: duckdb.DuckDBPyRelation
    | pa.Table
    | ds.FileSystemDataset
    | pd.DataFrame
    | pl.DataFrame
    | str,
    ddb: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:

    if isinstance(table, pa.Table):

        return ddb.from_arrow(table)

    elif isinstance(table, ds.FileSystemDataset):

        table = ddb.from_arrow(table)

        return table

    elif isinstance(table, pd.DataFrame):

        return ddb.from_df(table)

    elif isinstance(table, pl.DataFrame):

        return ddb.from_arrow(table.to_arrow())

    elif isinstance(table, str):
        if ".parquet" in table:
            table = ddb.from_parquet(table)
        elif ".csv" in table:
            table = ddb.from_csv_auto(table)
        else:
            table = ddb.query(f"SELECT * FROM '{table}'")

        return table

    elif isinstance(table, duckdb.DuckDBPyRelation):

        return table


def sort_table(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | ds.FileSystemDataset,
    sort_by: str | list | tuple | None,
    ascending: bool | list | tuple | None,
    ddb: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation:

    if sort_by:
        ascending = ascending or True

        if isinstance(ascending, bool):
            reverse = not ascending
        else:
            reverse = [not el for el in ascending]

        if isinstance(table, pa.Table):

            return to_polars(table=table).sort(by=sort_by, reverse=reverse).to_arrow()

        elif isinstance(table, pd.DataFrame):
            return to_polars(table=table).sort(by=sort_by, reverse=reverse).to_pandas()

        elif isinstance(table, ds.FileSystemDataset):
            return to_polars(table=table).sort(by=sort_by, reverse=reverse).to_arrow()

        elif isinstance(table, pl.DataFrame):
            return table.sort(by=sort_by, reverse=reverse)

        elif isinstance(table, duckdb.DuckDBPyRelation):
            return ddb.from_arrow(
                to_polars(table).sort(by=sort_by, reverse=reverse).to_arrow()
            )
    else:
        return table


def get_tables_diff(
    table1: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | ds.FileSystemDataset
    | str,
    table2: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | ds.FileSystemDataset
    | str,
    subset: list | None = None,
    cast_as_str: bool = False,
    ddb: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation:

    if not ddb:  # is None:
        ddb = duckdb.connect()

    table1_ = to_relation(table1, ddb=ddb)
    table2_ = to_relation(table2, ddb=ddb)

    if subset:
        print(subset)
        if cast_as_str:
            subset_types = table1_.project(",".join(subset)).types
            subset_table1_ = table1_.project(
                ",".join([f"CAST({col} as STRING) as {col}" for col in subset])
            )
            subset_table2_ = table2_.project(
                ",".join([f"CAST({col} as STRING) as {col}" for col in subset])
            )
        else:
            subset_types = None
            subset_table1_ = table1_.project(",".join(subset))
            subset_table2_ = table2_.project(",".join(subset))

        diff_ = subset_table1_.except_(subset_table2_)  # .arrow().to_pylist()
        if subset_types:
            diff_ = diff_.project(
                ",".join(
                    [
                        f"CAST({col} as {type_}) as {col}"
                        for col, type_ in zip(subset, subset_types)
                    ]
                )
            )

        diff = to_polars(table1).filter(
            pl.struct(subset).is_in(diff_.arrow().to_pylist())
        )

    else:
        print("No subset given")
        diff = table1_.except_(table2_.project(",".join(table1_.columns)))

    if isinstance(table1, (pa.Table, ds.FileSystemDataset)):
        if isinstance(diff, pl.DataFrame):
            return diff.to_arrow()
        else:
            return diff.arrow()

    elif isinstance(table1, pd.DataFrame):
        return to_pandas(diff)

    elif isinstance(table1, pl.DataFrame):
        return to_polars(diff)

    elif isinstance(table1, duckdb.DuckDBPyRelation):
        return to_relation(diff, ddb=ddb)

    else:
        if isinstance(diff, pl.DataFrame):
            return diff.to_arrow()
        else:
            return diff.arrow()

    # if type(table1) != type(table2):
    #    raise TypeError

    # else:
    #     if isinstance(table1, pa.Table):
    #         return ddb.from_arrow(table1).except_(ddb.from_arrow(table2)).arrow()
    #     elif isinstance(table1, pd.DataFrame):
    #         return ddb.from_df(table1).except_(ddb.from_df(table2)).df()
    #     elif isinstance(table1, pl.DataFrame):
    #         return pl.concat([table1.with_row_count(), table2.with_row_count()]).filter(
    #             pl.count().over(table1.columns) == 1
    #         )
    #     elif isinstance(table1, str):
    #         return ddb.execute(
    #             f"SELECT * FROM {table1} EXCEPT SELECT * FROM {table2}"
    #         ).arrow()


def distinct_table(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | ds.FileSystemDataset,
    subset: list | None = None,
    keep: str = "first",
    presort_by: str | list | None = None,
    postsort_by: str | list | None = None,
    ddb: duckdb.DuckDBPyConnection | None = None,
) -> pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation:

    if isinstance(table, (pa.Table, pd.DataFrame, pl.DataFrame, ds.FileSystemDataset)):
        table = to_polars(table=table)
        if presort_by:
            table = sort_table(table=table, sort_by=presort_by, ddb=ddb)

        if not subset:
            if not table.is_unique().all():
                table = table.unique()

        else:
            columns = [col for col in table.columns if col not in subset]
            agg = (
                pl.col(columns).first()
                if keep.lower() == "first"
                else pl.col(columns).last()
            )
            table = table.groupby(subset).agg(agg)

        if postsort_by:
            table = sort_table(table=table, sort_by=presort_by, ddb=ddb)

        if isinstance(table, pd.DataFrame):
            return table.to_pandas()
        elif isinstance(table, (pa.Table, ds.FileSystemDataset)):
            return table.to_arrow()
        else:
            return table

    elif isinstance(table, duckdb.DuckDBPyRelation):
        if presort_by:
            table = table.order(",".join(presort_by))
        if not subset:
            table = table.distinct()
        else:
            subset = ",".join(subset)
            columns = [
                f"FIRST({col}) as {col}"
                if keep.lower() == "first"
                else f"LAST({col}) as {col}"
                for col in table.columns
                if col not in subset
            ]
            table = table.aggregate(f"{subset},{','.join(columns)}", subset)
        if postsort_by:
            table = table.order(",".join(postsort_by))

        return table


def drop_columns(
    table: pa.Table
    | pd.DataFrame
    | pl.DataFrame
    | duckdb.DuckDBPyRelation
    | ds.FileSystemDataset,
    columns: str | list | None = None,
) -> pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation:
    if isinstance(columns, str):
        columns = [columns]

    if columns:
        if isinstance(table, pa.Table):
            columns = [col for col in columns if col in table.column_names]
            if len(columns) > 0:
                return table.drop(columns=columns)
            return table

        elif isinstance(table, (pl.DataFrame, pd.DataFrame)):
            columns = [col for col in columns if col in table.columns]
            if len(columns) > 0:
                return table.drop(columns=columns)
            return table

        elif isinstance(table, ds.FileSystemDataset):
            columns = [col for col in table.schema.names if col not in columns]
            if len(columns) > 0:
                return table.to_table(columns=columns)
            return table.to_table()

        elif isinstance(table, duckdb.DuckDBPyRelation):
            columns = [
                f"'{col}'" if " " in col else col
                for col in table.columns
                if col not in columns
            ]
            if len(columns) > 0:
                return table.project(",".join(columns))
            return table
    else:
        return table
