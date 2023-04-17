import datetime as dt
import os
import re
from typing import Dict, List, Tuple, Union

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.dataset as pds
import pyarrow.feather as pf
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from fsspec import filesystem as fsspec_filesystem
from fsspec.implementations import dirfs
from fsspec.spec import AbstractFileSystem
from fsspec.utils import infer_storage_options

from ..utils.base import humanize_size, run_parallel, random_id
from ..utils.schema import convert_schema, sort_schema, unify_schema
from ..utils.table import to_arrow, sort_table, distinct_table, partition_by
from ..utils.dataset import get_pa_schemas, get_unified_schema, get_file_details


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
        **storage_options,
    ):
        so = infer_storage_options(path)
        self._protocol = storage_options.pop("protocol", None) or so["protocol"]
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
            ddb if isinstance(ddb, duckdb.DuckDBPyConnection) else duckdb.connect()
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

        self._timestamp_column = timestamp_column
        self._set_filesystem(filesystem=filesystem, **storage_options)
        self._set_basedataset()

    def _check_path_exists(self):
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

        self._pa_filesystem = pafs.PyFileSystem(
            pafs.FSSpecHandler(self._dir_filesystem)
        )

    def _set_basedataset(self):
        self._check_path_exists()
        if not self._path_empty:
            self._basedataset = pds.dataset(
                self._path,
                format=self._format,
                filesystem=self._dir_filesystem,
                partitioning=self._partitioning,
                schema=self._pa_schema if hasattr(self, "_pa_schema") else self._schema,
            )
            self._select_files = self._basedataset.files
        else:
            self._basedataset = None

    @staticmethod
    def _read_file(
        path: str,
        schema: pa.Schema | None = None,
        format: str | None = None,
        filesystem: pafs.FileSystem | None = None,
    ) -> pa.Table:  # sourcery skip: avoid-builtin-shadow
        format = format or os.path.splitext(path)[-1]

        if re.sub("\.", "", format) == "parquet":
            table = pq.read_table(path, filesystem=filesystem, schema=schema)

        elif re.sub("\.", "", format) == "csv":
            with filesystem.open_input_file(path) as f:
                table = pc.read_csv(f, schema=schema)

        elif re.sub("\.", "", format) in ["arrow", "ipc", "feather"]:
            with filesystem.open_input_file(path) as f:
                table = pf.read_table(f)

        return table

    @staticmethod
    def _write_file(
        table: pa.Table | pd.DataFrame | pl.DataFrame | duckdb.DuckDBPyRelation,
        path: str,
        schema: pa.Schema | None = None,
        format: str | None = None,
        filesystem: pafs.FileSystem | None = None,
        **kwargs,
    ):  # sourcery skip: avoid-builtin-shadow
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

    @property
    def pa_schemas(self):
        if self._basedataset is None:
            self._set_basedataset()
            if self._basedataset:
                return None
        if not hasattr(self, "_pa_schemas"):
            self._pa_schemas = get_pa_schemas(self._basedataset)

        return self._pa_schemas

    @property
    def pl_schemas(self):
        if self._basedataset is None:
            self._set_basedataset()
            if self._basedataset:
                return None
        if not hasattr(self, "_pl_schemas"):
            self._pl_schemas = {
                f: convert_schema(schema) for f, schema in self.pa_schemas
            }
        return self._pl_schemas

    @property
    def schemas(self):
        return self.pa_schemas

    @property
    def pa_schema(self):
        if self._basedataset is None:
            self._set_basedataset()
            if self._basedataset:
                return None
        if not hasattr(self, "_pa_schema"):
            self._pa_schema, self._schemas_equal = get_unified_schema(self.pa_schemas)

        return self._pa_schema

    @property
    def pl_schema(self):
        if self._basedataset is None:
            self._set_basedataset()
            if self._basedataset:
                return None
        if not hasattr(self, "_pl_schema"):
            self._pl_schema = convert_schema(self.pa_schema)
        return self._pl_schema

    @property
    def schema(self):
        return self.pa_schema

    @property
    def schemas_equal(self):
        if self._basedataset is None:
            return None
        if not hasattr(self, "_schemas_equal"):
            self._pa_schema, self._schemas_equal = get_unified_schema(self.pa_schemas)
        return self._schemas_equal

    @property
    def pa_schema_sorted(self):
        if self._basedataset is None:
            self._set_basedataset()
            if self._basedataset:
                return None
        if not hasattr(self, "_pa_schema_sorted"):
            self._pa_schema_sorted = sort_schema(self.pa_schema)
        return self._pa_schema_sorted

    @property
    def pl_schema_sorted(self):
        if self._basedataset is None:
            self._set_basedataset()
            if self._basedataset:
                return None
        if not hasattr(self, "_pl_schema_sorted"):
            self._pl_schema_sorted = sort_schema(self.pl_schema)
        return self._pl_schema_sorted

    @property
    def schema_sorted(self):
        return self.pa_schema_sorted

    def repair_schema(self):
        def _repair_schema(path):
            schema = self.schemas[path]
            if schema != self.schema:
                table = self._read_file(
                    path=path, schema=self.schema, filesystem=self._pa_filesystem
                )
                self._write_file(
                    table=table,
                    path=path,
                    # schema=self.schema,
                    filesystem=self._pa_filesystem,
                )

        if self.schemas_equal:
            return
        else:
            run_parallel(_repair_schema, self.schemas, backend="threading")
        # for path in self.schemas:
        #     schema = self.schemas[path]
        #     if schema != self.schema:
        #         table = self._read_file(
        #             path=path, schema=self.schema, filesystem=self._pa_filesystem
        #         )
        #         self._write_file(
        #             table=table,
        #             path=path,
        #             # schema=self.schema,
        #             filesystem=self._pa_filesystem,
        #         )


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
        self._set_basedataset()
        self._set_file_details()

        if self._timestamp_column is None:
            timestamp_columns = [
                col.name
                for col in self._basedataset.schema
                if col.type
                in [
                    pa.timestamp("ns"),
                    pa.timestamp("us"),
                    pa.timestamp("ms"),
                    pa.timestamp("s"),
                ]
            ]
            self._timestamp_column = timestamp_columns[0]

    def _set_file_details(self):
        self._check_path_exists()
        if not self._path_empty:
            self.file_details = get_file_details(
                self._basedataset,
                timestamp_column=self._timestamp_column,
                filesystem=self._dir_filesystem,
            )

            self.size = self.file_details["size"].sum()
            self.size_h = humanize_size(self.size, unit="MB")

            self.selected_file_details = self.file_details
        else:
            self.file_details = None
            self.selected_file_details = None
            self.size = 0
            self.size_h = 0

    def filter_file_details(
        self,
        start_time: dt.datetime | str | None = None,
        end_time: dt.datetime | str | None = None,
        min_file_size: int | str | None = None,
        max_file_size: int | str | None = None,
        min_last_modified: dt.datetime | str | None = None,
        max_last_modified: dt.datetime | str | None = None,
        min_count_rows: int | None = None,
        max_count_rows: int | None = None,
    ):
        self._check_path_exists()

        if self._path_empty:
            return None

        if self.file_details is None:
            self._set_basedataset()
            self._set_file_details()

        file_details = self.file_details

        if start_time is not None:
            if isinstance(start_time, str):
                start_time = dt.datetime.fromisoformat(start_time)
            file_details = file_details.filter(pl.col("timestamp_max") >= start_time)

        if end_time is not None:
            if isinstance(end_time, str):
                end_time = dt.datetime.fromisoformat(end_time)
            file_details = file_details.filter(pl.col("timestamp_min") <= end_time)

        if min_file_size is not None:
            file_details = file_details.filter(pl.col("size") >= min_file_size)

        if max_file_size is not None:
            file_details = file_details.filter(pl.col("size") <= max_file_size)

        if min_last_modified is not None:
            if isinstance(min_last_modified, str):
                min_last_modified = dt.datetime.fromisoformat(min_last_modified)
            file_details = file_details.filter(
                pl.col("last_modified") >= min_last_modified
            )

        if max_last_modified is not None:
            if isinstance(max_last_modified, str):
                max_last_modified = dt.datetime.fromisoformat(max_last_modified)
            file_details = file_details.filter(
                pl.col("last_modified") <= max_last_modified
            )

        if min_count_rows is not None:
            file_details = file_details.filter(pl.col("conunt_rows") >= min_count_rows)

        if max_count_rows is not None:
            file_details = file_details.filter(pl.col("count_rows") <= max_count_rows)

        self.selected_file_details = file_details

    def load_pa_dataset(
        self,
        start_time: dt.datetime | str | None = None,
        end_time: dt.datetime | str | None = None,
        min_file_size: int | str | None = None,
        max_file_size: int | str | None = None,
        min_last_modified: dt.datetime | str | None = None,
        max_last_modified: dt.datetime | str | None = None,
        min_count_rows: int | None = None,
        max_count_rows: int | None = None,
        **kwargs,
    ):
        self._check_path_exists()

        if self._path_empty:
            return None

        if self.file_details is None:
            self._set_basedataset()
            self._set_file_details()

        self.filter_file_details(
            start_time=start_time,
            end_time=end_time,
            min_file_size=min_file_size,
            max_file_size=max_file_size,
            min_last_modified=min_last_modified,
            max_last_modified=max_last_modified,
            min_count_rows=min_count_rows,
            max_count_rows=max_count_rows,
        )

        self._pa_dataset = pds.dataset(
            self.selected_file_details["path"].to_list(),
            format=self._format,
            filesystem=self._dir_filesystem,
            partitioning=self._partitioning,
            schema=self._pa_schema if hasattr(self, "_pa_schema") else self._schema,
            **kwargs,
        )

        self.ddb.register(
            f"{self.name}.pa_dataset", self._pa_dataset
        ) if self.name else self.ddb.register(
            f"{self.name}.pa_dataset", self._pa_dataset
        )

    def load_pa_table(
        self,
        start_time: dt.datetime | str | None = None,
        end_time: dt.datetime | str | None = None,
        min_file_size: int | str | None = None,
        max_file_size: int | str | None = None,
        min_last_modified: dt.datetime | str | None = None,
        max_last_modified: dt.datetime | str | None = None,
        min_count_rows: int | None = None,
        max_count_rows: int | None = None,
        **kwargs,
    ):
        self._check_path_exists()

        if self._path_empty:
            return None

        if self.file_details is None:
            self._set_basedataset()
            self._set_file_details()

        self.filter_file_details(
            start_time=start_time,
            end_time=end_time,
            min_file_size=min_file_size,
            max_file_size=max_file_size,
            min_last_modified=min_last_modified,
            max_last_modified=max_last_modified,
            min_count_rows=min_count_rows,
            max_count_rows=max_count_rows,
        )

        if self._format == "parquet":
            table = pq.read_table(
                self.selected_file_details["path"].to_list(),
                filesystem=self._dir_filesystem,
                schema=self._schema or self.schema,
                **kwargs,
            )

        else:
            table = pa.concat_tables(
                run_parallel(
                    self._read_file,
                    self.selected_file_details["path"].to_list,
                    schema=self._schema or self.schema,
                    format=self._format,
                    filesystem=self._pa_filesystem,
                    backend="threading",
                    **kwargs,
                )
            )

        self._pa_table = table
        # self._ddb_rel = self.ddb.from_arrow(self._pa_table)

        self.ddb.register(
            f"{self.name}.pa_table", self._pa_table
        ) if self.name else self.ddb.register(f"{self.name}.pa_table", self._pa_table)

    def sql(self, sql: str):
        return self.ddb.sql(sql)

    @property
    def pa_dataset(self):
        if not hasattr(self, "_pa_dataset"):
            self.load_pa_dataset()
        return self._pa_dataset

    @property
    def pa_table(self):
        if not hasattr(self, "_pa_table"):
            self.load_pa_table()
        return self._pa_table

    @property
    def ddb_rel(self):
        # if not hasattr(self, "_ddb_rel"):
        if hasattr(self, "_pa_table"):
            self._ddb_rel = self.ddb.from_arrow(self._pa_table)
        elif hasattr(self, "_pa_dataset"):
            self._ddb_rel = self.ddb.from_arrow(self._pa_dataset)
        else:
            self._ddb_rel = self.ddb.from_arrow(self.pa_dataset)

        return self._ddb_rel

    @property
    def pl(self):
        if not hasattr(self, "_pl"):
            if hasattr(self, "_pa_table"):
                self._pl = pl.from_arrow(self._pa_table)
            else:
                self._pl = self.ddb_rel.pl()
        return self._pl

    @property
    def pl_scan(self):
        if not hasattr(self, "_pl_scan"):
            self._pl_scan = pl.scan_pyarrow_dataset(self.pa_dataset)
        return self._pl_scan

    def create_ddb_table(self, temp: bool = False):
        temp = "temp" if temp else ""
        if hasattr(self, "_pa_table"):
            self.sql(
                f"CREATE OR REPLACE {temp} table {self.name}.table_ FROM pa_table"
            ) if self.name else self.sql("CREATE OR REPLACE table_ FROM pa_table")
        else:
            _ = self.pa_dataset
            self.sql(
                f"CREATE OR REPLACE {temp} table {self.name}.table_ FROM pa_dataset"
            ) if self.name else self.sql("CREATE OR REPLACE table_ FROM pa_dataset")

    def _estimate_batch_size(self, file_size: str = "10MB"):
        unit = re.findall(["[k,m,g,t,p]{0,1}b"], file_size.lower())
        val = float(file_size.lower().split(unit)[0].strip())
        return int(
            val
            / (
                humanize_size(self.file_details["size"].sum(), unit=unit)
                / self.file_details["count_rows"].sum()
            )
        )

    def partition_by(
        self,
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
        res = partition_by(
            self._pa_table if hasattr(self, "_pa_table") else self.ddb_rel,
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
        return dict(res) if as_dict else list(res)

    def iter_batches(
        self,
        batch_size: int | None = 1_000_000,
        file_size: str | None = None,
        partition_by_strftime: str | List[str] | None = None,
        partition_by_timedelta: str | List[str] | None = None,
        sort_by: str | List[str] | None = None,
        ascending: bool | List[bool] = True,
        distinct: bool = True,
        subset: str | List[str] | None = None,
        keep: str = "first",
        presort: bool = False,
    ):
        if file_size:
            batch_size = self._estimate_batch_size(file_size=file_size)

        yield from partition_by(
            self.ddb_rel,
            self._timestamp_column,
            strftime=partition_by_strftime,
            timedelta=partition_by_timedelta,
            n_rows=batch_size,
            as_dict=False,
            drop=True,
            sort_by=sort_by,
            ascending=ascending,
            distinct=distinct,
            subset=subset,
            keep=keep,
            presort=presort,
        )

    # def write(
    #     table: Union[pa.Table, pd.DataFrame, pl.DataFrame, duckdb.DuckDBPyRelation]
    #     | List[Union[pa.Table, pd.DataFrame, pl.DataFrame, duckdb.DuckDBPyRelation]]
    # ):
    #     pass

    # def append(self, table:, )


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
#                 / self.file_details["count_rows"].sum()
#             )
#         )

#     def write(
#         self,
#         tables: Union[pa.Table, pd.DataFrame, pl.DataFrame, duckdb.DuckDBPyRelation]
#         | List[Union[pa.Table, pd.DataFrame, pl.DataFrame, duckdb.DuckDBPyRelation]],
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


def sync_datasets(dataset1: Dataset, dataset2: Dataset, delete: bool = True):
    fs1 = dataset1._dir_filesystem
    fs2 = dataset2._dir_filesystem

    def transfer_file(f):
        with fs2.open(f, "wb") as ff:
            ff.write(fs1.read_bytes(f))

    def delete_file(f):
        fs2.rm(f)

    new_files = (
        duckdb.from_arrow(dataset1.files.select(["path", "name", "size"]).to_arrow())
        .except_(
            duckdb.from_arrow(
                dataset2.files.select(["path", "name", "size"]).to_arrow()
            )
        )
        .pl()["path"]
        .to_list()
    )

    _ = run_parallel(transfer_file, new_files)

    if delete:
        rm_files = duckdb.from_arrow(
            dataset2.files.select(["path", "name", "size"]).to_arrow()
        ).except_(
            duckdb.from_arrow(
                dataset1.files.select(["path", "name", "size"]).to_arrow()
            )
        )

        _ = run_parallel(delete_file, rm_files)
