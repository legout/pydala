import shutil
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import s3fs
from joblib import Parallel, delayed


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


def delete(path: str, filesystem: fs.FileSystem | s3fs.S3FileSystem | None):
    if filesystem is None:
        shutil.rmtree(path)
    else:
        if hasattr(filesystem, open):
            filesystem.delete(path, recursive=True)
        else:
            filesystem.delete_dir(path)


def copy_to_tmp_directory(
    src: str | list,
    dest: str,
    filesystem: fs.FileSystem | s3fs.S3FileSystem | None = None,
):
    if filesystem is None:
        src = Path(src)
        dest = Path(dest)
        if src.is_file:
            shutil.copyfile(src=src, dst=dest)
        else:
            shutil.copytree(src=src, dst=dest)
    else:
        if isinstance(src, str):
            _ = filesystem.get(src, dest, recursive=True)
        else:
            dest = [f"{dest}/{src_}" for src_ in src]
            _ = Parallel()(
                delayed(filesystem.get)(src_, dest_) for src_, dest_ in zip(src, dest)
            )


def sort_table(
    table: pa.Table | pd.DataFrame | pl.DataFrame | ddb.DuckDBPyRelation,
    sort_by: str | list | tuple,
    ascending: bool | list | tuple | None,
    ddb: duckdb.DuckDBPyConnection,
    engine: str = "polars",
) -> pa.Table | pd.DataFrame | pl.DataFrame:

    if ascending is None:
        ascending = True
    if isinstance(ascending, bool):
        reverse = not ascending
    else:
        reverse = [not el for el in ascending]

    if isinstance(table, pa.Table):

        return pl.from_arrow(table).sort(by=sort_by, reverse=reverse).to_arrow()

    elif isinstance(table, pd.DataFrame):
        return table.sort(by=sort_by, reverse=reverse).to_arrow()

    elif isinstance(table, pl.DataFrame):
        return pl.from_pandas(table).sort(by=sort_by, reverse=reverse).to_pandas()

    elif isinstance(table, ddb.DuckDBPyRelation):
        return ddb.from_arrow(
            pl.from_arrow(table.arrow()).sort(by=sort_by, reverse=reverse).arrow()
        )


def to_ddb_relation(
    table: duckdb.DuckDBPyRelation
    | pa.Table
    | ds.FileSystemDataset
    | pd.DataFrame
    | pl.DataFrame
    | str,
    ddb: duckdb.DuckDBPyConnection,
    sort_by: str | list | None = None,
    ascending: bool | list | None = None,
    distinct: bool = False,
) -> duckdb.DuckDBPyRelation:

    if isinstance(sort_by, list):
        sort_by = ",".join(sort_by)

    if isinstance(table, pa.Table):
        if sort_by is not None:
            table = sort_table(
                table=table, sort_by=sort_by, ascending=ascending, ddb=ddb
            )
        table = ddb.from_arrow(table)

    elif isinstance(table, ds.FileSystemDataset):
        table = ddb.from_arrow(table)

        if sort_by is not None:
            if ascending is None:
                ascending = True

            if isinstance(sort_by, list):

                if isinstance(ascending, bool):
                    ascending = [ascending] * len(sort_by)

                sort_by = [
                    f"{col} ASC" if asc else f"{col} DESC"
                    for col, asc in zip(sort_by, ascending)
                ]
                sort_by = ",".join(sort_by)

            else:
                sort_by = sort_by + " ASC" if ascending else col + " DESC"

            table = table.order(sort_by)

    elif isinstance(table, pd.DataFrame):
        if sort_by is not None:
            table = sort_table(
                table=table, sort_by=sort_by, ascending=ascending, ddb=ddb
            )
        table = ddb.from_df(table)

    elif isinstance(table, pl.DataFrame):
        if sort_by is not None:
            table = sort_table(
                table=table, sort_by=sort_by, ascending=ascending, ddb=ddb
            )
        table = ddb.from_arrow(table.to_arrow())

    elif isinstance(table, str):
        if ".parquet" in table:
            table = ddb.from_parquet(table)
        elif ".csv" in table:
            table = ddb.from_csv_auto(table)
        else:
            table = ddb.query(f"SELECT * FROM '{table}'")
    else:
        table = table

    if distinct is not None:
        table = table.distinct()

    return table
