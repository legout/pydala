from typing import List

import duckdb
from fsspec import AbstractFileSystem
from isort import file
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pds

from .reader import dataset
from .utils.table import to_arrow, get_timestamp_column, get_table_delta




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
    filesystem:AbstractFileSystem|None=None,
    partitioning: List[str] | str | None = None,
    timestamp_column:str|None=None,
    mode:str="delta",
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
    
    
    ds = dataset(path=path, bucket=bucket, filesystem=filesystem, partitioning=partitioning, timestamp_column=timestamp_column,
                      )
           
    if mode == "delta" :
        if timestamp_column is not None:
            min_timestamp, max_timestamp = (
                        table.aggregate(
                            f"min({timestamp_column}), max({timestamp_column})"
                        ).fetchone()
                        if isinstance(table, duckdb.DuckDBPyRelation)
                        else (
                            duckdb.from_arrow(table)
                            .aggregate(
                                f"min({timestamp_column}), max({timestamp_column})"
                            )
                            .fetchone()
                        )
                    )
            table = (
                    get_table_delta(
                        table1=ds.ddb.from_arrow(table),
                        table2=ds.ddb_rel.filter(
                            f"{ds._timestamp_column}>='{min_timestamp}' AND {ds._timestamp_column}<='{max_timestamp}'"
                        ),
                        subset=subset,
                    ))
        else:
            table = get_table_delta(
                    table1=ds.ddb.from_arrow(table),
                    table2=ds.ddb_rel,
                    subset=subset,
                )
        

    