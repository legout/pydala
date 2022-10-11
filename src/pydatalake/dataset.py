import datetime as dt
import uuid
from distutils import dist
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.feather as pf
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import s3fs

from .reader import Reader
from .utils import is_file
from .utils import open as open_
from .utils import path_exists, sort_table, to_ddb_relation
from .writer import Writer


class Dataset:
    def __init__(
        self,
        path: str,
        base_name: str = "data",
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: dict | pafs.FileSystem | s3fs.S3FileSystem | None = None,
        format: str | None = "parquet",
        compression: str = "zstd",
        sort_by: str | list | None = None,
    ):
        self._path = path
        self._base_name = base_name
        self._filesystem = filesystem
        self._format = format
        self._partitioning = partitioning
        self._compression = compression
        self._sort_by = sort_by
        self.ddb = duckdb.connect()
        self.ddb.execute("SET temp_directory='/tmp/duckdb/'")
        self._set_reader()
        self._set_writer()

    def _set_reader(self):
        self.reader = Reader(
            path=self._path,
            partitioning=self._partitioning,
            filesystem=self._filesystem["reader"]
            if isinstance(self._filesystem, dict)
            else self._filesystem,
            format=self._format,
            sort_by=self._sort_by,
            ddb=self.ddb,
        )

    def _set_writer(
        self,
    ):
        self.writer = Writer(
            path=self._path,
            base_name=self._base_name,
            partitioning=self._partitioning,
            filesystem=self._filesystem["writer"]
            if isinstance(self._filesystem, dict)
            else self._filesystem,
            format=self._format,
            compression=self._compression,
            sort_by=self._sort_by,
            ddb=self.ddb,
        )

    def load_dataset(self, name="pa_dataset", **kwargs):
        self.reader.load_dataset(name=name, **kwargs)

    def load_table(self, name="pa_table", sort_by: str | list | None = None, **kwargs):
        if sort_by is not None:
            self._sort_by = sort_by
        else:
            sort_by = self._sort_by
        self.reader.load_table(name=name, sort_by=sort_by, **kwargs)

    # def set_table(
    #     self,
    #     table: duckdb.DuckDBPyRelation
    #     | pa.Table
    #     | ds.FileSystemDataset
    #     | pd.DataFrame
    #     | pl.DataFrame
    #     | str,
    #     sort_by: str | int | None = None,
    #     distinct:bool=False
    # ):
    #     del self._pa_table
    #     del self._pa_dataset
    #     self._table = to_ddb_relation(table, ddb=self.ddb, sort_by=sort_by, distinct=distinct)

    def query(self, *args, **kwargs) -> duckdb.DuckDBPyRelation:
        return self.ddb.query(*args, **kwargs)

    def execute(self, *args, **kwargs) -> duckdb.DuckDBPyConnection:
        return self.ddb.execute(*args, **kwargs)

    def filter(self, *args, **kwargs) -> duckdb.DuckDBPyRelation:
        return self.table.filter(*args, **kwargs)

    def write_table(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.FileSystemDataset
        | pd.DataFrame
        | pl.DataFrame
        | str
        | None = None,
        path: str | None = None,
        compression: str | None = None,
        format: str | None = None,
        row_group_size: int | None = None,
        sort_by: str | list | None = None,
        distinct: bool = False,
        **kwargs,
    ):
        if table is not None:
            table = to_ddb_relation(
                table=table, ddb=self.ddb, sort_by=sort_by, distinct=distinct
            ).arrow()

        self._path = self._path if path is None else path
        self._partitioning = None
        self._set_writer()
        self.writer.write_table(
            table=table,
            path=self._path,
            compression=compression,
            format=format,
            row_group_size=row_group_size,
            **kwargs,
        )

    def write_dataset(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.FileSystemDataset
        | pd.DataFrame
        | pl.DataFrame
        | str
        | None = None,
        path: str | None = None,
        partitioning: list | str | None = None,
        compression: str | None = None,
        format: str | None = None,
        rows_per_file: int | None = None,
        row_group_size: int | None = None,
        sort_by: str | list | None = None,
        distinct: bool = False,
        with_time_partition: bool = False,
        **kwargs,
    ):
        if sort_by is None:
            sort_by = self._sort_by
        else:
            self._sort_by = sort_by

        if path is None:
            path = self._path
        else:
            self._path = path

        if partitioning is None:
            partitioning = self._partitioning
        else:
            self._partitioning = partitioning

        self._set_writer()

        self.writer.write_dataset(
            table=table,
            path=path,
            format=format,
            compression=compression,
            partitioning=partitioning,
            sort_by=sort_by,
            distinct=distinct,
            rows_per_file=rows_per_file,
            row_group_size=row_group_size,
            with_time_partition=with_time_partition,
            **kwargs,
        )

    @property
    def pa_dataset(self) -> ds.FileSystemDataset:
        if hasattr(self, "_pa_dataset"):
            return self._pa_dataset
        else:
            if self.reader._path_exists:
                self._pa_dataset = self.reader.pa_dataset
                return self._pa_dataset

    @property
    def dataset(self) -> ds.FileSystemDataset:
        if hasattr(self, "_pa_dataset"):
            return self._pa_dataset
        else:
            if self.reader._path_exists:
                return self.pa_dataset

    @property
    def pa_table(self) -> pa.Table:
        if hasattr(self, "_pa_table"):
            return self._pa_table
        elif hasattr(self, "_table"):
            self._pa_table = self._table.arrow()
            return self._pa_table
        else:
            if self.reader._path_exists:
                self._pa_table = self.reader.pa_table
                return self._pa_table

    @property
    def table(self) -> duckdb.DuckDBPyRelation:
        if hasattr(self, "_table"):
            return self._table
        else:
            if self.reader._path_exists:
                self._table = self.reader.table
                return self._table

    @property
    def pl_dataframe(self) -> pl.DataFrame:
        if hasattr(self, "_pl_dataframe"):
            return self._pl_dataframe
        elif hasattr(self, "_pa_table"):
            self._pl_dataframe = pl.from_arrow(self._pa_table)
            return self._pl_dataframe
        else:
            if self.reader._path_exists:
                return self.reader.pl_dataframe

    @property
    def pd_dataframe(self) -> pd.DataFrame:
        if hasattr(self, "_pd_dataframe"):
            return self._pd_dataframe
        elif hasattr(self, "_table"):
            self._pd_dataframe = self._table.df()
            return self._pd_dataframe
        else:
            if self.reader._path_exists:
                return self.reader.pd_dataframe

    def repartition(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.FileSystemDataset
        | pd.DataFrame
        | pl.DataFrame
        | str
        | None = None,
        path: str | None = None,
        partitioning: list | str | None = None,
        compression: str | None = None,
        format: str | None = None,
        rows_per_file: int | None = None,
        row_group_size: int | None = None,
        sort_by: str | list | None = None,
        with_time_partition: bool = False,
        with_temp_table: bool = False,
        with_mem_table: bool = False,
        use_tmp_directory: bool = False,
        delete_old_files: bool = False,
        **kwargs,
    ):

        if table is None:
            if with_mem_table:
                table = self.pa_table
            if with_temp_table:
                if hasattr(self, "_pa_table"):
                    self.reader.create_temp_table(sort_by=sort_by, distinct=distinct)

        if dest is None:
            dest = self._path

        if src == dest:
            if with_temp_table is False and with_mem_table:
                use_tmp_directory = True
                delete_old_files = True

        # if with_temp_table

        # ToDo:
        # Add parameters for:
        # - sorting table before repartition
        # - distinct repartition
        # - write to new path
