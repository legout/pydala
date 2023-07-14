import datetime as dt
import os
import re
from typing import Dict, List, Union

import duckdb
import pandas as pd
import polars as pl_
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
from fsspec import filesystem as fsspec_filesystem
from fsspec.implementations import dirfs
from fsspec.spec import AbstractFileSystem
from fsspec.utils import infer_storage_options

from ..utils import humanize_size, humanized_size_to_bytes, run_parallel
from .utils.dataset import get_arrow_schema, get_file_details, get_unified_schema
from .utils.schema import convert_schema, sort_schema
from .utils.table import (
    add_date_columns,
    distinct_table,
    get_table_delta,
    get_timestamp_column,
    partition_by,
    read_table,
    sort_table,
    to_arrow,
    write_table,
)


class BaseDataset:
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        schema: pa.Schema | Dict[str, str] | None = None,
        format: str = "parquet",
        filesystem: AbstractFileSystem | None = None,
        partitioning: pds.Partitioning | List[str] | str | None = None,
        timestamp_column: str | None = None,
        ddb: duckdb.DuckDBPyConnection | None = None,
        name: str | None = None,
        verbose: bool = True,
        **storage_options,
    ):
        self._verbose = verbose
        so = infer_storage_options(path)
        self._protocol = storage_options.pop("protocol", None) or so["protocol"]
        bucket = bucket or ""
        self._path = so["path"].replace(bucket, "")
        self._bucket = bucket
        self._full_path = os.path.join(self._bucket, self._path)
        self._uri = (
            os.path.join(f"{self._protocol}://", self._full_path)
            if self._protocol != "file"
            else self._full_path
        )

        self._format = re.sub("\.", "", format)
        self._partitioning = partitioning
                    
        self.ddb = (
            ddb.cursor()
            if isinstance(ddb, duckdb.DuckDBPyConnection)
            else duckdb.connect()
        )
        self.name = name
        if name is not None:
            self.ddb.sql(f"CREATE SCHEMA IF NOT EXISTS {name}")

        if schema is not None:
            self._schema = (
                convert_schema(schema) if isinstance(schema, dict) else schema
            )
        else:
            self._schema = None

        self._set_filesystem(filesystem=filesystem, **storage_options)

        self._check_path_exists()
        if not self._path_exists:
            self._dir_filesystem.mkdirs(self._path, exist_ok=True)

        self._set_base_dataset()

        self._timestamp_column = timestamp_column
        if not self._path_empty and self._timestamp_column is None:
            self._timestamp_column = get_timestamp_column(self._base_dataset)

        self._set_file_details()
        if self._partitioning == "hive":
            self._partition_flavor="hive"
            if self.file_details is not None:
                partition_columns=[part.split("=")[0] for part in self.file_details["path"][0].split("/") if "=" in part]
                self._partition_columns = [part for part in partition_columns if f"{part}=" not in self._path]
                
            else:
                self._partition_columns = []
        else:
            self._partition_flavor="dir"
            self._partition_columns = self._partitioning

    def _check_path_exists(self):
        self._filesystem.invalidate_cache()
        self._dir_filesystem.invalidate_cache()
        self._path_exists = self._dir_filesystem.exists(self._path)
        self._path_empty = (
            self._dir_filesystem.du(self._path) == 0 if self._path_exists else True
        )

    def _set_filesystem(
        self, filesystem: AbstractFileSystem | None = None, **storage_options
    ):
        if filesystem is None:
            self._filesystem = fsspec_filesystem(
                protocol=self._protocol, **storage_options
            )
        else:
            self._filesystem = filesystem

        if self._bucket is not None:
            self._dir_filesystem = dirfs.DirFileSystem(
                path=self._bucket, fs=self._filesystem
            )
        else:
            self._dir_filesystem = self._filesystem

        self._arrow_filesystem = pafs.PyFileSystem(
            pafs.FSSpecHandler(self._dir_filesystem)
        )

    def _set_base_dataset(self):
        self._check_path_exists()
        if not self._path_empty:
            self._base_dataset = pds.dataset(
                self._path,
                format=self._format,
                filesystem=self._dir_filesystem,
                partitioning=self._partitioning,
                schema=self._arrow_schema
                if hasattr(self, "_arrow_schema")
                else self._schema,
            )
            self.selected_files = self._base_dataset.files
        else:
            self._base_dataset = None

    def _set_file_details(self):
        self._check_path_exists()
        if not self._path_empty:
            self.file_details = get_file_details(
                self._base_dataset,
                timestamp_column=self._timestamp_column,
                filesystem=self._dir_filesystem,
                verbose=self._verbose
            )

            self.size = self.file_details["size"].sum()
            self.size_h = humanize_size(self.size, unit="MB")

            self.selected_file_details = self.file_details
        else:
            self.file_details = None
            self.selected_file_details = None
            self.size = 0
            self.size_h = 0

    def select_files(
        self,
        time_range: dt.datetime
        | str
        | List[Union[str, None]]
        | List[Union[dt.datetime, None]]
        | None = None,
        file_size: int
        | str
        | List[Union[int, None]]
        | List[Union[str, None]]
        | None = None,
        last_modified: dt.datetime
        | str
        | List[Union[str, None]]
        | List[Union[dt.datetime, None]]
        | None = None,
        row_count: int | List[Union[int, None]] | None = None,
    ):
        self._check_path_exists()

        if self._path_empty:
            return None

        if self.file_details is None:
            self._set_base_dataset()
            self._set_file_details()

        file_details = self.file_details
        filter_ = []

        if time_range is None:
            start_time = None
            end_time = None

        elif isinstance(time_range, str | dt.datetime):
            start_time = time_range
            end_time = None

        else:
            start_time, end_time = time_range

        if start_time:
            filter_.append(f"timestamp_max>='{start_time}'")

        if end_time:
            filter_.append(f"timestamp_min<='{end_time}'")

        if file_size is None:
            min_file_size = None
            max_file_size = None

        elif isinstance(file_size, str | int):
            min_file_size = None
            max_file_size = file_size

        else:
            min_file_size, max_file_size = file_size

        if isinstance(min_file_size, str):
            min_file_size = humanized_size_to_bytes(min_file_size)

        if isinstance(max_file_size, str):
            max_file_size = humanized_size_to_bytes(max_file_size)

        if min_file_size:
            filter_.append(f"size>={min_file_size}")

        if max_file_size:
            filter_.append(f"size<={max_file_size}")

        if last_modified is None:
            min_last_modified = None
            max_last_modified = None

        elif isinstance(last_modified, str | dt.datetime):
            min_last_modified = last_modified
            max_last_modified = None

        else:
            min_last_modified, max_last_modified = last_modified

        if min_last_modified:
            filter_.append(f"last_modified>='{min_last_modified}'")

        if max_last_modified:
            filter_.append(f"last_modified<='{max_last_modified}'")

        if row_count is None:
            min_row_count = None
            max_row_count = None

        elif isinstance(row_count, str | int):
            min_row_count = None
            max_row_count = row_count

        else:
            min_row_count, max_row_count = row_count

        if min_row_count:
            filter_.append(f"row_count>={min_row_count}")

        if max_row_count:
            filter_.append(f"row_count<={max_row_count}")

        if filter_:
            self.selected_file_details = (
                self.ddb.sql("FROM file_details").filter(" AND ".join(filter_)).pl()
            )
        else:
            self.selected_file_details = file_details
        self.selected_files = self.selected_file_details["path"].to_list()

    def _load_arrow_dataset(
        self,
        time_range: dt.datetime
        | str
        | List[Union[str, None]]
        | List[Union[dt.datetime, None]]
        | None = None,
        file_size: int
        | str
        | List[Union[int, None]]
        | List[Union[str, None]]
        | None = None,
        last_modified: dt.datetime
        | str
        | List[Union[str, None]]
        | List[Union[dt.datetime, None]]
        | None = None,
        row_count: int | List[Union[int, None]] | None = None,
        **kwargs,
    ):  
        self._check_path_exists()

        if self._path_empty:
            return None

        if self.file_details is None:
            self._set_base_dataset()
            self._set_file_details()

        self.select_files(
            time_range=time_range,
            file_size=file_size,
            last_modified=last_modified,
            row_count=row_count,
        )
        if len(self.selected_files):
            self._arrow_dataset = pds.dataset(
                self.selected_files,
                format=self._format,
                filesystem=self._dir_filesystem,
                partitioning=self._partitioning,
                partition_base_dir=self._path,
                schema=self._arrow_schema
                if hasattr(self, "_arrow_schema")
                else self._schema,
                **kwargs,
            )
            self.register("arrow_dataset", self._arrow_dataset)
        else:
            self._arrow_dataset = None

        

    @property
    def arrow_schemas(self):
        if self._base_dataset is None:
            self._set_base_dataset()
            if not self._base_dataset:
                return None
        if not hasattr(self, "_arrow_schemas"):
            self._arrow_schemas = get_arrow_schema(self._base_dataset)

        return self._arrow_schemas

    @property
    def pl_schemas(self):
        if self._base_dataset is None:
            self._set_base_dataset()
            if not self._base_dataset:
                return None
        if not hasattr(self, "_pl_schemas"):
            self._pl_schemas = {
                f: convert_schema(schema) for f, schema in self.arrow_schemas
            }
        return self._pl_schemas

    @property
    def schemas(self):
        return self.arrow_schemas

    @property
    def arrow_schema(self):
        if self.arrow_schemas is None:
            return None
        if not hasattr(self, "_arrow_schema"):
            self._arrow_schema, self._schemas_equal = get_unified_schema(
                self.arrow_schemas
            )

        return self._arrow_schema

    @property
    def pl_schema(self):
        if self.arrow_schema is None:
            return None
        if not hasattr(self, "_pl_schema"):
            self._pl_schema = convert_schema(self.arrow_schema)
        return self._pl_schema

    @property
    def schema(self):
        return self.arrow_schema

    @property
    def schemas_equal(self):
        if self.arrow_schemas is None:
            return None
        if not hasattr(self, "_schemas_equal"):
            self._arrow_schema, self._schemas_equal = get_unified_schema(
                self.arrow_schemas
            )
        return self._schemas_equal

    @property
    def arrow_schema_sorted(self):
        if self.arrow_schema is None:
            return None
        if not hasattr(self, "_arrow_schema_sorted"):
            self._arrow_schema_sorted = sort_schema(self.arrow_schema)
        return self._arrow_schema_sorted

    @property
    def pl_schema_sorted(self):
        if self._base_dataset is None:
            self._set_base_dataset()
            if not self._base_dataset:
                return None
        if not hasattr(self, "_pl_schema_sorted"):
            self._pl_schema_sorted = sort_schema(self.pl_schema)
        return self._pl_schema_sorted

    @property
    def schema_sorted(self):
        return self.arrow_schema_sorted

    def repair_schema(self):
        def _repair_schema(path):
            self._dir_filesystem.invalidate_cache()
            schema = self.schemas[path]
            if schema != self.schema:
                table = read_table(
                    path=path, schema=self.schema, filesystem=self._dir_filesystem
                )
                write_table(
                    table=table,
                    path=path,
                    # schema=self.schema,
                    filesystem=self._dir_filesystem,
                )

        if self.schemas_equal:
            return
        else:
            run_parallel(
                _repair_schema, self.schemas, backend="threading", verbose=self._verbose
            )


class Dataset(BaseDataset):
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        schema: pa.Schema | Dict[str, str] | None = None,
        format: str = "parquet",
        filesystem: AbstractFileSystem | None = None,
        partitioning: pds.Partitioning | List[str] | str | None = None,
        timestamp_column: str | None = None,
        ddb: duckdb.DuckDBPyConnection | None = None,
        name: str | None = None,
        verbose: bool = True,
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
            verbose=verbose,
            **storage_options,
        )

    @property
    def is_materialized(self):
        return hasattr(self, "_arrow_table")

    def materialize(
        self, *args, combine_chunks: bool = False, chunk_size=1_000_000, **kwargs
    ):
        if args or kwargs or not hasattr(self, "_arrow_table"):
            if args or kwargs:
                self.select_files(*args, **kwargs)

            self._arrow_table = pa.concat_tables(
                run_parallel(
                    read_table,
                    self.selected_files,
                    schema=self.schema,
                    format=self._format,
                    filesystem=self._dir_filesystem,
                    partitioning=self._partitioning,
                    backend="threading",
                    verbose=self._verbose,
                )
            )

            if combine_chunks:
                self._arrow_table = self._arrow_table.combine_chunks()
            elif chunk_size:
                self._arrow_table = pa.Table.from_batches(
                    self._arrow_table.to_batches(max_chunksize=chunk_size)
                )

            self.register("arrow_table", self._arrow_table)
            self._del(pl=True, df=True, ddb_rel=True)

    def _del(
        self,
        pl: bool = False,
        df: bool = False,
        ddb_rel: bool = False,
        arrow_table: bool = False,
        arrow_dataset: bool = False,
        ddb_table: bool = False,
    ):
        if hasattr(self, "_pl") and pl:
            del self._pl

        if hasattr(self, "_df") and df:
            del self._df

        if hasattr(self, "_ddb_rel") and ddb_rel:
            del self._ddb_rel

        if hasattr(self, "_arrow_table") and arrow_table:
            del self._arrow_table
            self.unregister("arrow_table")

        if hasattr(self, "_arrow_dataset") and arrow_dataset:
            del self._arrow_dataset
            self.unregister("arrow_dataset")

        if ddb_table:
            tables = self.sql("SHOW TABLES")
            if tables is not None:
                tables = tables.pl()["name"].to_list()
                for table in tables:
                    if "arrow" in table:
                        self.sql(f"DROP VIEW {table}")
                    else:
                        self.sql(f"DROP TABLE {table}")

    def reload(self, *args, **kwargs):
        was_materialized = self.is_materialized

        self._del(
            pl=True,
            df=True,
            ddb_rel=True,
            arrow_table=True,
            arrow_dataset=True,
            ddb_table=True,
        )
        self._set_base_dataset()
        self._set_file_details()
        self._load_arrow_dataset(*args, **kwargs)
        if was_materialized:
            self.materialize(*args, **kwargs)

    def sql(self, sql: str, materialize: bool = False, *args, **kwargs):
        if materialize:
            self.materialize(*args, **kwargs)
        return self.ddb.sql(sql)

    def register(self, view_name: str, py_obj: object):
        if self.name:
            self.ddb.sql(f"CREATE OR REPLACE schema {self.name}")
            self.ddb.register(f"{self.name}.{view_name}", py_obj)
            self.ddb.sql(
                f"CREATE OR REPLACE view {self.name}.{view_name} AS FROM '{self.name}.{view_name}'"
            )
        else:
            self.ddb.register(view_name, py_obj)

    def unregister(self, view_name: str):
        self.ddb.unregister(
            f"{self.name}.{view_name}"
        ) if self.name else self.ddb.unregister(view_name)

    def sort(self, by: str | List[str], ascending: bool | List[bool] = True):
        if not self.is_materialized:
            self._ddb_rel = sort_table(self.ddb_rel, sort_by=by, ascending=ascending)
        else:
            self._arrow_table = sort_table(
                self._arrow_table, sort_by=by, ascending=ascending
            )
            self._del(ddb_rel=True)

        # if self.has_ddb_table:
        #    self.ddb.sql()

        self._del(pl=True, df=True, ddb_table=True)

    def distinct(self, subset: str | List[str] | None = None, keep: str = "first"):
        if not hasattr(self, "_arrow_table"):
            self._ddb_rel = distinct_table(self.ddb_rel, subset=subset, keep=keep)
        else:
            self._arrow_table = distinct_table(
                self._arrow_table, subset=subset, keep=keep
            )
            self._del(ddb_rel=True)

        self._del(pl=True, df=True, ddb_table=True)

    @property
    def _has_arrow_dataset(self):
        return hasattr(self, "_arrow_dataset")

    @property
    def arrow_dataset(self):
        if not self._has_arrow_dataset:
            self._load_arrow_dataset()
        return self._arrow_dataset

    @property
    def arrow_table(self):
        if not self.is_materialized:
            self.materialize()

        return self._arrow_table

    @property
    def _has_ddb_rel(self):
        return hasattr(self, "_ddb_rel")

    @property
    def ddb_rel(self):
        if not self._has_ddb_rel:
            if self.is_materialized:
                self._ddb_rel = self.ddb.from_arrow(self._arrow_table)
            else:
                self._ddb_rel = self.ddb.from_arrow(self.arrow_dataset)

        return self._ddb_rel

    @property
    def _has_pl(self):
        return hasattr(self, "_pl")

    @property
    def pl(self):
        if not self._has_pl:
            self._pl = self.ddb_rel.pl()
        return self._pl

    @property
    def _has_pl_scan(self):
        return hasattr(self, "_pl_scan")

    @property
    def pl_scan(self):
        if not self._has_pl_scan:
            self._pl_scan = pl_.scan_pyarrow_dataset(self.arrow_dataset)
        return self._pl_scan

    @property
    def _has_df(self):
        return hasattr(self, "_df")

    @property
    def df(self):
        if not self._has_df:
            self._df = self.ddb_rel.arrow().to_pandas(types_mapper=pd.ArrowDtype)

        return self._df

    @property
    def ddb_tables(self):
        if not hasattr(self, "_tables"):
            if self.ddb.sql("SHOW TABLES"):
                self._tables = [
                    table_[0] for table_ in self.ddb.sql("SHOW TABLES").fetchall()
                ]
            else:
                self._tables = []
        return self._tables

    @property
    def has_ddb_table(self):
        if not self.ddb_tables:
            return False
        if self.name:
            return (
                f"{self.name}.table_temp" in self.ddb_tables
                or f"{self.name}.table_" in self.ddb_tables
            )
        else:
            return "table_temp" in self.ddb_tables or "table_" in self.ddb_tables

    def _create_ddb_table(self, temp: bool = False):
        temp = "temp" if temp else ""
        if hasattr(self, "_arrow_table"):
            self.sql(
                f"CREATE OR REPLACE {temp} table {self.name}.table_{temp} AS FROM {self.name}.arrow_table"
            ) if self.name else self.sql(
                f"CREATE OR REPLACE {temp} table table_{temp} AS FROM arrow_table"
            )
        else:
            _ = self.arrow_dataset
            self.sql(
                f"CREATE OR REPLACE {temp} table {self.name}.table_{temp} As FROM {self.name}.arrow_dataset"
            ) if self.name else self.sql(
                f"CREATE OR REPLACE {temp} table table_{temp} As FROM arrow_dataset"
            )

    def _update_ddb_table(self, temp: bool = False):
        temp = "temp" if temp else ""
        # existing_tables = [table_[0] for table_ in self.ddb.sql("SHOW TABLES").fetchall()]
        if hasattr(self, "_arrow_table"):
            self.sql(
                f"INSERT INTO {temp} table {self.name}.table_{temp} FROM {self.name}.arrow_table"
            ) if self.name else self.sql(f"INSERT INTO table_{temp} FROM arrow_table")
        else:
            _ = self.arrow_dataset
            self.sql(
                f"INSERT INTO {temp} table {self.name}.table_{temp} FROM {self.name}.arrow_dataset"
            ) if self.name else self.sql(f"INSERT INTO table_{temp} FROM arrow_dataset")

    def to_ddb_table(self, temp: bool = False):
        temp = "temp" if temp else ""
        self._update_ddb_table(
            temp=temp
        ) if self.has_ddb_table else self._create_ddb_table(temp=temp)

    def _estimate_batch_size(self, file_size: str = "10MB"):
        unit = re.findall(["[k,m,g,t,p]{0,1}b"], file_size.lower())
        val = float(file_size.lower().split(unit)[0].strip())
        return int(
            val
            / (
                humanize_size(self.file_details["size"].sum(), unit=unit)
                / self.file_details["row_counts"].sum()
            )
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
        if not self.is_materialized:
            self._ddb_rel = add_date_columns(
                table=self.ddb_rel,
                year=year,
                month=month,
                week=week,
                yearday=yearday,
                monthday=monthday,
                weekday=weekday,
                strftime=strftime,
            )
        else:
            self._arrow_table = add_date_columns(
                table=self._arrow_table,
                year=year,
                month=month,
                week=week,
                yearday=yearday,
                monthday=monthday,
                weekday=weekday,
                strftime=strftime,
            )
            self._del(ddb_rel=True)

        self._del(pl=True, df=True, ddb_table=True)

    def _partition_by(
        self,
        which: str = "ddb_rel",
        columns: str | List[str] | None = None,
        strftime: str | List[str] | None = None,
        timedelta: str | List[str] | None = None,
        n_rows: int | None = None,
        as_dict: bool = False,
        drop: bool = False,
        sort_by: str | List[str] | None = None,
        ascending: bool | List[bool] = True,
        distinct: bool = False,
        subset: str | List[str] | None = None,
        keep: str = "first",
        presort: bool = False,
    ):
        table_ = eval(f"self.{which}")

        yield from partition_by(
            table_,
            self._timestamp_column,
            columns=columns,
            strftime=strftime,
            timedelta=timedelta,
            n_rows=n_rows,
            as_dict=as_dict,
            drop=drop,
            sort_by=sort_by,
            ascending=ascending,
            distinct=distinct,
            subset=subset,
            keep=keep,
            presort=presort,
        )

    def iter_batches(
        self,
        which: str = "ddb_rel",
        batch_size: int | None = 1_000_000,
        file_size: str | None = None,
        columns: str | List[str] | None = None,
        partition_by_strftime: str | List[str] | None = None,
        partition_by_timedelta: str | List[str] | None = None,
        drop: bool = False,
        sort_by: str | List[str] | None = None,
        ascending: bool | List[bool] = True,
        distinct: bool = False,
        subset: str | List[str] | None = None,
        keep: str = "first",
        presort: bool = False,
        as_dict: bool = False,
    ):
        if file_size:
            batch_size = self._estimate_batch_size(file_size=file_size)

        yield from self._partition_by(
            which=which,
            columns=columns,
            strftime=partition_by_strftime,
            timedelta=partition_by_timedelta,
            n_rows=batch_size,
            as_dict=as_dict,
            drop=drop,
            sort_by=sort_by,
            ascending=ascending,
            distinct=distinct,
            subset=subset,
            keep=keep,
            presort=presort,
        )

    def to_batches(
        self,
        which: str = "ddb_rel",
        batch_size: int | None = 1_000_000,
        file_size: str | None = None,
        columns: str | List[str] | None = None,
        partition_by_strftime: str | List[str] | None = None,
        partition_by_timedelta: str | List[str] | None = None,
        drop: bool = False,
        sort_by: str | List[str] | None = None,
        ascending: bool | List[bool] = True,
        distinct: bool = False,
        subset: str | List[str] | None = None,
        keep: str = "first",
        presort: bool = False,
        as_: str | None = None,
        as_dict: bool = False,
    ):
        batches = self.iter_batches(
            which=which,
            batch_size=batch_size,
            file_size=file_size,
            columns=columns,
            partition_by_strftime=partition_by_strftime,
            partition_by_timedelta=partition_by_timedelta,
            sort_by=sort_by,
            ascending=ascending,
            distinct=distinct,
            subset=subset,
            keep=keep,
            presort=presort,
            as_=as_,
            as_dict=as_dict,
            drop=drop,
        )
        return dict(batches) if as_dict else list(batches)

    def append(
        self,
        table: pa.Table
        | pd.DataFrame
        | pl_.DataFrame
        | pa.dataset.Dataset
        | duckdb.DuckDBPyRelation
        | List[pa.Table]
        | List[pl_.DataFrame]
        | List[pd.DataFrame]
        | List[pa.dataset.Dataset]
        | List[duckdb.DuckDBPyRelation],
        mode: str = "delta",
        subset: str | List[str] | None = None,
        sort_by: str | List[str] | None = None,
        ascending: bool | List[bool] = True,
        distinct: bool = False,
        keep: str = "first",
    ):  # sourcery skip: low-code-quality
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
            # if isinstance(table, pl_.DataFrame | pd.DataFrame):
            table = to_arrow(table)

        if mode == "delta" and self._has_arrow_dataset:
            if self._timestamp_column:
                min_timestamp, max_timestamp = (
                    table.aggregate(
                        f"min({self._timestamp_column}), max({self._timestamp_column})"
                    ).fetchone()
                    if isinstance(table, duckdb.DuckDBPyRelation)
                    else (
                        self.ddb.from_arrow(table)
                        .aggregate(
                            f"min({self._timestamp_column}), max({self._timestamp_column})"
                        )
                        .fetchone()
                    )
                )
            table = (
                get_table_delta(
                    table1=self.ddb.from_arrow(table),
                    table2=self.ddb_rel.filter(
                        f"{self._timestamp_column}>='{min_timestamp}' AND {self._timestamp_column}<='{max_timestamp}'"
                    ),
                    subset=subset,
                )
                if self._timestamp_column
                else get_table_delta(
                    table1=self.ddb.from_arrow(table),
                    table2=self.ddb_rel,
                    subset=subset,
                )
            )

        table = to_arrow(table)

        if sort_by:
            table = sort_table(table, sort_by=sort_by, ascending=ascending)
        if distinct:
            table = distinct_table(table, subset=subset, keep=keep)

        self._delta_table = table

        if len(table):
            self._arrow_dataset = (
                pds.dataset([self._arrow_dataset, pds.dataset(table)])
                if self._has_arrow_dataset
                else pds.dataset(table)
            )
            self.register(
                f"{self.name}.arrow_dataset", self._arrow_dataset
            ) if self.name else self.register("arrow_dataset", self._arrow_dataset)

            if self.is_materialized:
                self._arrow_table = pa.concat_tables(self._arrow_table, table)

                self.register(
                    f"{self.name}.arrow_table", self._arrow_table
                ) if self.name else self.register("arrow_table", self._arrow_table)

            self._del(pl=True, df=True, ddb_rel=True, ddb_table=True)


# class Writer(Dataset):
#     def __init__(
#         self,
#         path: str,
#         bucket: str | None = None,
#         schema: pa.Schema | Dict[str, str] | None = None,
#         format: str = "parquet",
#         filesystem: AbstractFileSystem | None = None,
#         partitioning: pds.Partitioning | List[str] | str | None = None,
#         timestamp_column: str | None = None,
#         ddb: duckdb.DuckDBPyConnection | None = None,
#         name: str | None = None,
#         **storage_options,
#     ):
#         super().__init__(
#             path=path,
#             bucket=bucket,
#             schema=schema,
#             format=format,
#             filesystem=filesystem,
#             partitioning=partitioning,
#             timestamp_column=timestamp_column,
#             ddb=ddb,
#             name=name,
#             **storage_options,
#         )

#     def _estimate_batch_size(self, file_size: str = "10MB"):
#         unit = re.findall(["[k,m,g,t,p]{0,1}b"], file_size.lower())
#         val = float(file_size.lower().split(unit)[0].strip())
#         return int(
#             val
#             / (
#                 humanize_size(self.file_details["size"].sum(), unit=unit)
#                 / self.file_details["row_counts"].sum()
#             )
#         )

#     def write(
#         self,
#         tables: Union[pa.Table, pd.DataFrame, pl_.DataFrame, duckdb.DuckDBPyRelation]
#         | List[Union[pa.Table, pd.DataFrame, pl_.DataFrame, duckdb.DuckDBPyRelation]],
#         mode: str = "delta",
#         batch_size: int | str = 1_000_000,
#         file_size: str | None = None,
#         group_by_datetime: Dict[str, str] | None = None,
#         row_group_size: int = 100_00,
#         sort: str | List[str] | None = None,
#         distinct: bool = False,
#         subset: str | List[str] | None = None,
#         keep: str = "first",
#     ):
#         if not isinstance(tables, list | tuple):
#             tables = [tables]
#         if sort is not None:
#             tables = run_parallel(sort_table, tables, sort_by=sort, backend="threading")

#         if distinct is not None:
#             tables = run_parallel(
#                 distinct_table, subset=subset, keep=keep, backend="threading"
#             )

#         if file_size is not None:
#             batch_size = self._estimate_batch_size(file_size=file_size)

#         # get table batches
#         batches = run_parallel(
#             to_batches, tables, batch_size, group_by_datetime, backend="threading"
#         )
#         # tables to arrow
#         tables = []
#         for batches_ in batches:
#             tables.extend([to_arrow(batch) for batch in batches_])

#         # cast large_string partition cols to str
#         if self._partitioning is not None:
#             tables_ = []
#             for table in tables:
#                 table = table.cast(
#                     [
#                         f.with_type(pa.string()) if f.type == pa.large_string() else f
#                         for f in table.schema
#                     ]
#                 )
#                 tables_.append(table)
#             tables = tables_

#         basename_template = f"data-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d_%H%M%S')}-{random_id()}-{{i}}.{self._format}"

#         if self._format == "parquet":
#             file_options = pds.ParquetFileFormat().make_write_options(
#                 compression=self._compression
#             )
#         elif self._format in [".arrow", ".ipc", ".feather"]:
#             file_options = pds.IpcFileFormat().make_write_options(
#                 compression=self._compression
#             )
#         else:
#             file_options = None

#         pds.write_dataset(
#             data=tables,
#             base_dir=self._path,
#             basename_template=basename_template,
#             format=self._format,
#             partitioning=self._partitioning,
#             schema=self.schema,
#             filesystem=self._dir_filesystem,
#             min_rows_per_group=row_group_size,
#             max_rows_per_group=row_group_size * 2,
#             file_options=file_options,
#             existing_data_behavior="overwrite_or_ignore",
#         )


# def sync_datasets(
#     dataset1: Dataset,
#     dataset2: Dataset,
#     delete: bool = True,
#     multipart_threshold: int = 10,
# ):
#     fs1 = dataset1._dir_filesystem
#     fs2 = dataset2._dir_filesystem

#     def transfer_file(f, multipart_threshold=10):
#         fs2.makedirs(os.path.dirname(f), exist_ok=True)
#         n_parts = fs1.size(f) // (1024**2 * multipart_threshold) + 1
#         with fs2.open(f, "ab") as ff:
#             for part in range(n_parts):
#                 ff.write(
#                     fs1.read_block(
#                         f,
#                         offset=part * 1024**2 * multipart_threshold,
#                         length=1024**2 * multipart_threshold,
#                     )
#                 )

#     def delete_file(f):
#         fs2.rm(f)

#     if dataset2.selected_file_details is None:
#         new_files = dataset1.selected_file_details["path"].to_list()
#     else:
#         new_files = (
#             duckdb.from_arrow(
#                 dataset1.selected_file_details.select(
#                     ["path", "name", "size"]
#                 ).to_arrow()
#             )
#             .except_(
#                 duckdb.from_arrow(
#                     dataset2.selected_file_details.select(
#                         ["path", "name", "size"]
#                     ).to_arrow()
#                 )
#             )
#             .pl()["path"]
#             .to_list()
#         )

#     if len(new_files):
#         _ = run_parallel(
#             transfer_file,
#             new_files,
#             multipart_threshold=multipart_threshold,
#             backend="loky",
#         )

#     if delete and dataset2.selected_file_details is not None:
#         rm_files = (
#             duckdb.from_arrow(
#                 dataset2.selected_file_details.select(
#                     ["path", "name", "size"]
#                 ).to_arrow()
#             )
#             .except_(
#                 duckdb.from_arrow(
#                     dataset1.selected_file_details.select(
#                         ["path", "name", "size"]
#                     ).to_arrow()
#                 )
#             )
#             .pl()["path"]
#             .to_list()
#         )

#         if len(rm_files):
#             _ = run_parallel(delete_file, rm_files, backend="loky")
