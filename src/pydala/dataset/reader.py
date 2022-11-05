import datetime as dt
import os
from tempfile import mkdtemp

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.feather as pf
import pyarrow.parquet as pq
from fsspec import spec
from fsspec.utils import infer_storage_options
from pyarrow.fs import FileSystem

from .helper import (
    distinct_table,
    drop_columns,
    get_ddb_sort_str,
    sort_table,
    to_pandas,
    to_polars,
    to_relation,
    get_filesystem,
    convert_size_unit
)


class Reader:
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        name: str | None = None,
        partitioning: ds.Partitioning | list | str | None = None,
        format: str | None = "parquet",
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = "__index_level_0__",
        ddb: duckdb.DuckDBPyConnection | None = None,
        caching: bool = False,
        cache_storage: str | None = "/tmp/pydala/",
        protocol: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        storage_options: dict = {},
        fsspec_fs: spec.AbstractFileSystem | None = None,
        pyarrow_fs: FileSystem | None = None,
    ):
        self._name = name
        self._tables = dict()
        self._cached = False
        self._profile = profile
        self._endpoint_url = endpoint_url
        self._storage_options = storage_options

        self._set_paths(
            path=path,
            bucket=bucket,
            protocol=protocol,
            caching=caching,
            cache_storage=cache_storage,
        )

        self._filesystem = get_filesystem(
            bucket=self._bucket,
            protocol=self._protocol,
            profile=self._profile,
            endpoint_url=self._endpoint_url,
            storage_options=self._storage_options,
            caching=self._caching,
            cache_bucker=self._cache_bucket,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
        )
        self._set_filesystem()

        self._format = format
        self._partitioning = partitioning

        _ = self.sort(by=sort_by, ascending=ascending)
        _ = self.distinct(distinct)
        _ = self.drop(columns=drop)

        if ddb is not None:
            self.ddb = ddb
        else:
            self.ddb = duckdb.connect()
        self.ddb.execute(f"SET temp_directory='{cache_storage}'")

    def _set_paths(
        self,
        path: str,
        bucket: str | None,
        protocol: str | None,
        caching: bool,
        cache_storage: str | None,
    ):
        if bucket is not None:
            self._bucket = infer_storage_options(bucket)["path"]
        else:
            self._bucket = None

        self._path = infer_storage_options(path)["path"]

        if self._bucket is not None:
            self._protocol = protocol or infer_storage_options(bucket)["protocol"]

        else:
            self._protocol = protocol or infer_storage_options(path)["protocol"]

        self._caching = caching
        self._cache_storage = cache_storage

        if self._caching:

            if cache_storage is not None:
                os.makedirs(cache_storage, exist_ok=True)
                self._cache_bucket = mkdtemp(prefix=cache_storage)
            else:
                self._cache_bucket = mkdtemp()
        else:
            self._cache_bucket = None

    def _set_filesystem(self):
        if self._cached:
            self._fs = self._filesystem["fsspec_cache"]
            self._pafs = self._filesystem["pyarrow_cache"]
        else:
            self._fs = self._filesystem["fsspec_main"]
            self._pafs = self._filesystem["pyarrow_main"]

    def sort(self, by: str | list | None, ascending: bool | list | None = None):
        self._sort_by = by

        if ascending is None:
            ascending = True
        self._ascending = ascending

        if self._sort_by is not None:
            self._sort_by_ddb = get_ddb_sort_str(sort_by=by, ascending=ascending)

        return self

    def distinct(self, value: bool | None):
        if value is None:
            value = False
        self._distinct = value

        return self

    def drop(self, columns: str | list | None):
        self._drop = columns
        return self

    def _gen_name(self, name: str | None):
        return f"{self._name}_{name}" if self._name is not None else name

    def _to_cache(self):
        recursive = (
            False if self._filesystem["fsspec_main"].isfile(self._path) else True
        )

        if hasattr(self._fs, "has_s5cmd"):
            if self._fs.has_s5cmd and self._profile is not None:
                self._fs.sync(
                    "s3://" + os.path.join(self._bucket or "", self._path),
                    os.path.join(
                        self._cache_bucket,
                        os.path.dirname(self._path) if not recursive else self._path,
                    ),
                    recursive=recursive,
                )
            else:
                self._fs.get(
                    self._path,
                    os.path.join(self._cache_bucket, self._path),
                    recursive=recursive,
                )
        else:
            self._fs.get(
                self._path,
                os.path.join(self._cache_bucket, self._path),
                recursive=recursive,
            )

        # self._path = os.path.join(self._cache_bucket, self._path)
        self._cached = True
        self._set_filesystem()

    def _load_feather(self, **kwargs):
        if self._fs.exists(self._path):
            if self._fs.isfile(self._path):

                # with self._pafs.open_input_file(self._path) as f:
                with self._fs.open(self._path) as f:
                    # self._mem_table = pl.read_ipc(f).to_arrow()
                    # else:
                    #    with self._fs.open(self._path) as f:
                    self._mem_table = pf.read_table(f, **kwargs)

            else:

                if not hasattr(self, "_dataset"):
                    self.set_dataset()
                self._mem_table = self._dataset.to_table(**kwargs)
        else:
            raise FileNotFoundError(f"{self._path} not found.")

    def _load_parquet(self, **kwargs):
        if self._fs.exists(self._path):
            if self._fs.isfile(self._path):
                with self._fs.open(self._path) as f:
                    self._mem_table = pl.read_parquet(source=f, **kwargs).to_arrow()
            else:
                self._mem_table = pq.read_table(
                    self._path,
                    partitioning=self._partitioning,
                    filesystem=self._fs,
                    **kwargs,
                )
        else:
            raise FileNotFoundError(f"{self._path} not found.")

    def _load_csv(self, **kwargs):

        if self._fs.exists(self._path):
            if self._fs.isfile(self._path):
                # use polars, if storage_options available
                # with self._fs.open_input_file(self._path) as f:
                with self._fs.open(self._path) as f:
                    self._mem_table = pl.read_csv(f).to_arrow()

                # else:
                #    with self._fs.open(self._path) as f:
                #        self._mem_table = pf.read_table(f, **kwargs)

            else:

                if not hasattr(self, "_dataset"):
                    self.set_dataset()
                self._mem_table = self._dataset.to_table(**kwargs)

        else:
            raise FileNotFoundError(f"{self._path} not found.")

    def set_dataset(self, name: str = "dataset", **kwargs):
        if self._caching and not self.cached:
            self._to_cache()

        name = self._gen_name(name=name)

        if self._fs.exists(self._path):
            self._dataset = ds.dataset(
                source=self._path,
                format=self._format,
                filesystem=self._fs,
                partitioning=self._partitioning,
                **kwargs,
            )

            # self._dataset = name
            self._tables["dataset"] = name
            self.ddb.register(name, self._dataset)
        else:
            raise FileNotFoundError(f"{self._path} not found.")

    def load_mem_table(
        self,
        name: str = "mem_table",
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = None,
        **kwargs,
    ):
        if self._caching and not self.cached:
            self._to_cache()

        name = self._gen_name(name=name)

        if sort_by is not None:
            self.sort(by=sort_by, ascending=ascending)

        if distinct is not None:
            self.distinct(distinct)

        if drop is not None:
            self.drop(columns=drop)

        if self._format == "parquet":
            self._load_parquet(**kwargs)

        elif (
            self._format == "feather"
            or self._format == "ipc"
            or self._format == "arrow"
        ):
            self._load_feather(**kwargs)

        elif self._format == "csv":
            self._load_csv(**kwargs)

        self._mem_table = sort_table(
            drop_columns(self._mem_table, columns=self._drop),
            sort_by=self._sort_by,
            ascending=self._ascending,
        )

        if self._distinct:
            self._mem_table = distinct_table(self._mem_table)

        self._tables["mem_table"] = name
        self.ddb.register(name, self._mem_table)

    def create_temp_table(
        self,
        name: str = "temp_table",
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool = False,
        drop: str | list | None = None,
    ):
        if self._caching and not self.cached:
            self._to_cache()

        name = self._gen_name(name=name)

        if sort_by is not None:
            self.sort(by=sort_by, ascending=ascending)

        if distinct:
            self.distinct(distinct)

        if drop is not None:
            self.drop(columns=drop)

        if self.has_mem_table:

            self._mem_table = sort_table(
                drop_columns(self._mem_table, columns=self._drop),
                sort_by=self._sort_by,
                ascending=self._ascending,
            )
            if distinct:
                self._mem_table = distinct_table(self._mem_table)

            sql = f"CREATE OR REPLACE TEMP TABLE {name} AS  SELECT * FROM {self._tables['mem_table']}"

        else:
            if not hasattr(self, "_dataset"):
                self.set_dataset()

            sql = f"CREATE OR REPLACE TEMP TABLE {name} AS  SELECT * FROM {self._tables['dataset']}"

            if self._sort_by is not None:

                sort_by = get_ddb_sort_str(
                    sort_by=self._sort_by, ascending=self._ascending
                )

                sql += f" ORDER BY {sort_by}"

            if self._drop is not None:
                if isinstance(drop, str):
                    drop = [drop]
                drop = [col for col in drop if col in self._dataset.schema.names]

                sql = sql.replace("SELECT *", f"SELECT * exclude({','.join(drop)})")

            if self._distinct:
                sql = sql.replace("SELECT *", "SELECT DISTINCT *")

        self._tables["temp_table"] = name
        self.ddb.execute(sql)

    def to_relation(
        self,
        create_temp_table: bool = False,
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool = False,
        drop: str | list | None = None,
    ):
        if self._caching and not self.cached:
            self._to_cache()

        if sort_by is not None:
            self.sort(by=sort_by, ascending=ascending)

        if distinct:
            self.distinct(distinct)

        if drop is not None:
            self.drop(columns=drop)

        if create_temp_table:
            self.create_temp_table(sort_by=sort_by, distinct=distinct)

        if self.has_mem_table:
            self._rel = to_relation(
                table=self._mem_table,
                ddb=self.ddb,
                sort_by=self._sort_by,
                ascending=self._ascending,
                distinct=self._distinct,
                drop=self._drop,
            )

        elif self.has_temp_table:

            self._rel = self.ddb.query(f"SELECT * FROM {self._tables['temp_table']}")

            if distinct:
                self._rel = self._rel.distinct()

            if drop is not None:
                self._rel = drop_columns(self._rel, columns=self._drop)

            if sort_by is not None:
                self._rel.order(self._sort_by_ddb)

        else:
            if not self.has_dataset:
                self.set_dataset()

            self._rel = to_relation(
                table=self._dataset,
                ddb=self.ddb,
                sort_by=self._sort_by,
                ascending=self._ascending,
                distinct=self._distinct,
                drop=self._drop,
            )

        return self._rel

    def to_polars(
        self,
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = None,
    ):
        if self._caching and not self.cached:
            self._to_cache()

        self.sort(by=sort_by, ascending=ascending)
        self.drop(drop)

        if self.has_mem_table:
            table = self._mem_table

        elif self.has_temp_table:
            sql = f"SELECT * FROM {self._tables['temp_table']}"

            if sort_by is not None:

                sort_by = get_ddb_sort_str(sort_by=sort_by, ascending=ascending)

                sql += f" ORDER BY {sort_by}"

            if drop is not None:
                if isinstance(drop, str):
                    drop = [drop]
                    drop = [
                        f"'{col}'" if " " in col else col
                        for col in drop
                        if col in self._dataset.schema.names
                    ]

                sql = sql.replace("SELECT *", f"SELECT * exclude({','.join(drop)})")

            if distinct:
                self.distinct(distinct)
                sql = sql.replace("SELECT *", "SELECT DISTINCT *")

            table = self.ddb.execute(sql).arrow()
        else:
            table = self._rel

        self._pl_dataframe = sort_table(
            drop_columns(to_polars(table=table), columns=drop),
            sort_by=self._sort_by,
            ascending=self._ascending,
        )

        if distinct:
            self._pl_dataframe = distinct_table(table=self._pl_dataframe)

        return self._pl_dataframe

    def to_pandas(
        self,
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = None,
    ):
        if self._caching and not self.cached:
            self._to_cache()

        self.sort(by=sort_by, ascending=ascending)
        self.drop(drop)

        if self.has_mem_table:
            table = self._mem_table

        elif self.has_temp_table:
            sql = f"SELECT * FROM {self._tables['temp_table']}"

            if sort_by is not None:

                sort_by = get_ddb_sort_str(sort_by=sort_by, ascending=ascending)

                sql += f" ORDER BY {sort_by}"

            if drop is not None:
                if isinstance(drop, str):
                    drop = [drop]
                    drop = [
                        f"'{col}'" if " " in col else col
                        for col in drop
                        if col in self._dataset.schema.names
                    ]

                sql = sql.replace("SELECT *", f"SELECT * exclude({','.join(drop)})")

            if distinct:
                self.distinct(distinct)
                sql = sql.replace("SELECT *", "SELECT DISTINCT *")

            table = self.ddb.execute(sql).arrow()
        else:
            table = self._rel

        self._pd_dataframe = sort_table(
            drop_columns(to_pandas(table=table), columns=drop),
            sort_by=self._sort_by,
            ascending=self._ascending,
        )

        if distinct:
            self._pd_dataframe = distinct_table(table=self._pd_dataframe)

        return self._pd_dataframe

    def execute(self, *args, **kwargs):
        return self.ddb.execute(*args, **kwargs)

    def query(self, *args, **kwargs):
        return self.ddb.query(*args, **kwargs)

    @property
    def dataset(self) -> ds.FileSystemDataset:
        if not self.has_dataset:
            self.set_dataset()

        return self._dataset

    @property
    def mem_table(self) -> pa.Table:
        if not hasattr(self, "_mem_table"):
            if self.ddb is not None:
                if self.has_temp_table:
                    self._mem_table = self.ddb.query(
                        f"SELECT * FROM {self._tables['temp_table']}"
                    ).arrow()
                else:
                    self.load_mem_table()
            else:
                self.load_mem_table()

        return self._mem_table

    @property
    def rel(self) -> duckdb.DuckDBPyRelation:
        if not self.has_relation:
            self.to_relation()

        return self._rel

    @property
    def table(self) -> duckdb.DuckDBPyRelation:
        if not self.has_relation:
            self.to_relation()

        return self._rel

    @property
    def pl_dataframe(self) -> pl.DataFrame:
        if not self.has_pl_dataframe:
            self.to_polars()
        return self._pl_dataframe

    @property
    def pd_dataframe(self) -> pd.DataFrame:
        if not self.has_pd_dataframe:
            self.to_pandas()
        return self._pd_dataframe

    @property
    def has_temp_table(self) -> bool:
        return "temp_table" in self._tables

    @property
    def has_mem_table(self) -> bool:
        return "mem_table" in self._tables

    @property
    def has_dataset(self) -> bool:
        return "dataset" in self._tables

    @property
    def has_relation(self) -> bool:
        return hasattr(self, "_rel")

    @property
    def has_pl_dataframe(self) -> bool:
        return hasattr(self, "_pl_dataframe")

    @property
    def has_pd_dataframe(self) -> bool:
        return hasattr(self, "_pd_dataframe")

    @property
    def cached(self) -> bool:
        return self._cached
    
    @property
    def buffer_size(self):
        if not hasattr(self,"_buffer_size"):
            self._buffer_size = self.mem_table.get_total_buffer_size()

        return self._buffer_size
    
     
    def get_buffer_size(self, unit:str="MB"):
        return f"{convert_size_unit(self.buffer_size, unit=unit):.1f} {unit}"

    
    @property
    def disk_usage(self):
        if not hasattr(self, "_disk_usage"):
            self._disk_usage = self._fs.du(self._path, total=True)
        return self._disk_usage

    def get_disk_usage(self, unit:str="MB"):
        return f"{convert_size_unit(self.disk_usage, unit=unit):.1f} {unit}"
   