import pyarrow as pa
import polars as pl
import pandas as pd
import duckdb
from typing import List
from .dataset import Dataset


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
    **dataset_kwargs
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
