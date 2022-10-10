from pathlib import Path
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pandas as pd
import polars as pl
import s3fs
import duckdb


def path_exists(
    path: str, filesystem: fs.FileSystem | s3fs.S3FileSystem | None = None
) -> bool:
    if filesystem is not None:
        if hasattr(filesystem, "exists"):
            return filesystem.exists(path)
        else:
            return filesystem.get_file_info(path).type > 0
    else:
        return Path(path).exists()


def is_file(
    path: str,
    filesystem: fs.FileSystem | s3fs.S3FileSystem | None = None,
) -> bool:
    if filesystem is not None:
        if hasattr(filesystem, "isfile"):
            return filesystem.isfile(path)
        else:
            return filesystem.get_file_info(path).type == 2
    else:
        return Path(path).is_file()


def open(path: str, filesystem: fs.FileSystem | s3fs.S3FileSystem):
    if hasattr(filesystem, "open"):
        return filesystem.open(path)
    else:
        return filesystem.open_input_file(path)


def copy_to_tmp_directory(src:str, dest:str, filesystem:fs.FileSystem | s3fs.S3FileSystem|None=None):
    if filesystem is None:
        



def sort_table(
    table_: pa.Table | pd.DataFrame | pl.DataFrame,
    sort_by: str | list,
    ddb: duckdb.DuckDBPyConnection,
) -> pa.Table | pd.DataFrame | pl.DataFrame:

    if isinstance(table_, pa.Table):
        if isinstance(sort_by, list):
            sort_by = ",".join(sort_by)
        return ddb.execute(f"SELECT * FROM table_ ORDER BY {sort_by}").arrow()

    elif isinstance(table_, pd.DataFrame):
        return table_.sort_values(sort_by)

    elif isinstance(table_, pl.DataFrame):
        return table_.sort(sort_by)




def to_ddb_relation(
    table: duckdb.DuckDBPyRelation
    | pa.Table
    | ds.FileSystemDataset
    | pd.DataFrame
    | pl.DataFrame
    | str,
    ddb: duckdb.DuckDBPyConnection,
    sort_by: str | list | None = None,
    distinct:bool=False
) -> duckdb.DuckDBPyRelation:

    if isinstance(sort_by, list):
        sort_by = ",".join(sort_by)

    if isinstance(table, pa.Table):
        if sort_by is not None:
            table = sort_table(table_=table, sort_by=sort_by, ddb=ddb)
        table_ = ddb.from_arrow(table)
    elif isinstance(table, ds.FileSystemDataset):
        table_ = ddb.from_arrow(table)
    elif isinstance(table, pd.DataFrame):
        if sort_by is not None:
            table = sort_table(table_=table, sort_by=sort_by, ddb=ddb)
        table_ = ddb.from_df(table)
    elif isinstance(table, pl.DataFrame):
        if sort_by is not None:
            table = sort_table(table_=table, sort_by=sort_by, ddb=ddb)
        table_ = ddb.from_arrow(table.to_arrow())
    elif isinstance(table, str):
        if ".parquet" in table:
            table_ = ddb.from_parquet(table)
        elif ".csv" in table:
            table_ = ddb.from_csv_auto(table)
        else:
            table_ = ddb.query(f"SELECT * FROM '{table}'")
    else:
        table_ = table
    
    if sort_by is not None:
        table_ = table_.order(sort_by)
    if distinct is not None:
        table_ = table_.distinct()
    
    return table_
