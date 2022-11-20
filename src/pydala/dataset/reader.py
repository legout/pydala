import datetime as dt
import os

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.feather as pf
import pyarrow.parquet as pq
from fsspec import spec
from pyarrow.fs import FileSystem

from ..utils.base import convert_size_unit
from ..utils.dataset import get_unified_schema
from ..utils.logging import log_decorator
from ..utils.table import to_pandas, to_polars, to_relation
from .base import BaseDataSet
from .timefly import TimeFly


class Reader(BaseDataSet):
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        name: str | None = None,
        partitioning: ds.Partitioning | list | str | None = None,
        format: str | None = "parquet",
        ddb: duckdb.DuckDBPyConnection | None = None,
        ddb_memory_limit: str = "-1",
        caching: bool = False,
        cache_storage: str | None = "/tmp/pydala/",
        protocol: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        storage_options: dict = {},
        fsspec_fs: spec.AbstractFileSystem | None = None,
        pyarrow_fs: FileSystem | None = None,
        use_pyarrow_fs: bool = False,
    ):
        super().__init__(
            path=path,
            bucket=bucket,
            name=name,
            partitioning=partitioning,
            partitioning_flavor=None,
            format=format,
            compression=None,
            ddb=ddb,
            ddb_memory_limit=ddb_memory_limit,
            caching=caching,
            cache_storage=cache_storage,
            protocol=protocol,
            profile=profile,
            endpoint_url=endpoint_url,
            storage_options=storage_options,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
            use_pyarrow_fs=use_pyarrow_fs,
        )

    def get_pyarrow_schema(self, **kwargs):

        if self._format == "parquet":
            dataset = self._get_dataset(**kwargs)
            return get_unified_schema(dataset=dataset)

    def set_pyarrow_schema(self):
        self._schema, self._schemas_equal = self.get_pyarrow_schema()

    def _gen_name(self, name: str | None):
        return f"{self._name}_{name}" if self._name else name  # is not None else name

    def _to_cache(self):
        self._fs.invalidate_cache()
        recursive = False if self._fs.isfile(self._path) else True

        if hasattr(self._fs, "has_s5cmd"):
            if self._fs.has_s5cmd and self._profile:  # is not None:
                self._fs.fs.sync(
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

                if self._use_pyarrow_fs:
                    with self._pafs.open_input_file(self._path) as f:
                        pa_table = pf.read_table(f, **kwargs)

                else:
                    with self._fs.open(self._path) as f:
                        pa_table = pf.read_table(f, **kwargs)

            else:

                if not hasattr(self, "_dataset"):
                    self.load_dataset()

                pa_table = self._dataset.to_table(**kwargs)

            return pa_table

        else:
            raise FileNotFoundError(f"{self._path} not found.")

    def _load_parquet(self, **kwargs):
        if self._fs.exists(self._path):

            use_pyarrow = False
            if hasattr(self, "_schema"):
                if self._schema and not self._schemas_equal:
                    use_pyarrow = True

            if self._fs.isfile(self._path):

                if self._use_pyarrow_fs:
                    with self._pafs.open_input_file(self._path) as f:
                        if use_pyarrow:
                            pa_table = pl.read_parquet(
                                source=f,
                                use_pyarrow=True,
                                pyarrow_options=dict(schema=self._schema),
                                **kwargs,
                            ).to_arrow()
                        else:
                            pa_table = pl.read_parquet(source=f, **kwargs).to_arrow()

                else:
                    with self._fs.open(self._path) as f:
                        if use_pyarrow:
                            pa_table = pl.read_parquet(
                                source=f,
                                use_pyarrow=True,
                                pyarrow_options=dict(schema=self._schema),
                                **kwargs,
                            ).to_arrow()
                        else:
                            pa_table = pl.read_parquet(source=f, **kwargs).to_arrow()

            else:
                try:
                    pa_table = pq.read_table(
                        self._path,
                        partitioning=self._partitioning,
                        filesystem=self._pafs if self._use_pyarrow_fs else self._fs,
                        **kwargs,
                    )
                except pa.ArrowInvalid:
                    self.set_pyarrow_schema()
                    pa_table = pq.read_table(
                        self._path,
                        partitioning=self._partitioning,
                        filesystem=self._pafs if self._use_pyarrow_fs else self._fs,
                        schema=self._schema,
                        **kwargs,
                    )

            return pa_table

        else:
            raise FileNotFoundError(f"{self._path} not found.")

    def _load_csv(self, **kwargs):
        if self._fs.exists(self._path):
            if self._fs.isfile(self._path):

                if self._use_pyarrow_fs:
                    with self._pafs.open_input_file(self._path) as f:
                        pa_table = pl.read_csv(f).to_arrow()

                else:
                    with self._fs.open(self._path) as f:
                        pa_table = pl.read_csv(f).to_arrow()

            else:

                if not hasattr(self, "_dataset"):
                    self.load_dataset()
                pa_table = self._dataset.to_table(**kwargs)

            return pa_table

        else:
            raise FileNotFoundError(f"{self._path} not found.")

    def _get_dataset(self, schema: pa.Schema | None = None, **kwargs):
        if self._fs.exists(self._path):
            dataset = ds.dataset(
                source=self._path,
                schema=schema,
                format=self._format,
                filesystem=self._pafs if self._use_pyarrow_fs else self._fs,
                partitioning=self._partitioning,
                **kwargs,
            )

            return dataset

        else:
            raise FileNotFoundError(f"{self._path} not found.")

    @log_decorator()
    def load_dataset(
        self, name: str = "dataset", schema: pa.Schema | None = None, **kwargs
    ):
        if self._caching and not self.cached:
            self._to_cache()

        if schema:  # is not None:
            self._schema = schema
            self._schemas_equal = True
        else:
            self.set_pyarrow_schema()

        name = self._gen_name(name=name)

        self._dataset = self._get_dataset(schema=self._schema, **kwargs)

        # self._dataset = name
        self._tables["dataset"] = name
        self.ddb.register(name, self._dataset)

    @log_decorator()
    def load_pa_table(
        self,
        name: str = "pa_table",
        **kwargs,
    ):
        if self._caching and not self.cached:
            self._to_cache()

        name = self._gen_name(name=name)

        if self._format == "parquet":
            self._pa_table = self._load_parquet(**kwargs)

        elif (
            self._format == "feather"
            or self._format == "ipc"
            or self._format == "arrow"
        ):
            self._pa_table = self._load_feather(**kwargs)

        elif self._format == "csv":
            self._pa_table = self._load_csv(**kwargs)

        self._pa_table = self._drop_sort_distinct(table=self._pa_table)

        self._tables["pa_table"] = name
        self.ddb.register(name, self._pa_table)

        return self._pa_table

    def _create_ddb_table(
        self,
        name: str,
        temp: bool = True,
    ):
        if self._caching and not self.cached:
            self._to_cache()
        if temp:

            name = self._gen_name(name=name)
            sql = f"CREATE OR REPLACE TEMP TABLE {name} AS  SELECT * FROM "
            self._tables["temp_table"] = name
        else:
            name = self._gen_name(name=name)
            sql = f"CREATE OR REPLACE TABLE {name} AS  SELECT * FROM "
            self._tables["table_"] = name

        if self.has_pa_table:
            sql += f"{self._tables['pa_table']}"
            columns = self.pa_table.column_names

        else:
            if not self.has_dataset:
                self.load_dataset()

            sql += f"{self._tables['dataset']}"
            columns = self.dataset.schema.names

        if self._sort_by:  # is not None:

            sql += f" ORDER BY {self._sort_by_ddb}"

        if self._drop:  # is not None:
            if isinstance(self._drop, str):
                self._drop = [self._drop]
            drop = [col for col in self._drop if col in columns]

            if len(drop) > 0:
                sql = sql.replace("SELECT *", f"SELECT * exclude({','.join(drop)})")

        if self._distinct:
            sql = sql.replace("SELECT *", "SELECT DISTINCT *")

        self.ddb.execute(sql)

    @log_decorator()
    def create_temp_table(
        self,
        name: str = "temp_table",
    ):
        self._create_ddb_table(name=name, temp=True)

    @log_decorator()
    def create_table(self, name: str = "table_"):
        self._create_ddb_table(name=name, temp=False)

    @log_decorator()
    def set_existing_ddb_table(self, existing_table:str):
        self._tables["table_"] = existing_table


    @log_decorator()
    def to_relation(
        self,
    ):
        if self._caching and not self.cached:
            self._to_cache()

        elif self.has_table_:
            self._rel = self._drop_sort_distinct(
                table=self.ddb.query(f"SELECT * FROM {self._tables['table_']}")
            )

        elif self.has_temp_table:
            self._rel = self._drop_sort_distinct(
                table=self.ddb.query(f"SELECT * FROM {self._tables['temp_table']}")
            )

        elif self.has_pa_table:
            self._rel = to_relation(
                self._drop_sort_distinct(table=self.pa_table),
                ddb=self.ddb,
            )

        else:
            if not self.has_dataset:
                self.load_dataset()

            self._rel = to_relation(
                self._drop_sort_distinct(table=self.dataset),
                ddb=self.ddb,
            )

        return self._rel

    @log_decorator()
    def to_polars(
        self,
    ):
        if self._caching and not self.cached:
            self._to_cache()

        if self.has_table_:
            self._pl_dataframe = to_polars(
                self._drop_sort_distinct(
                    table=self.ddb.query(f"SELECT * FROM {self._tables['table_']}")
                )
            )

        elif self.has_pa_table:
            self._pl_dataframe = to_polars(
                self._drop_sort_distinct(table=self.pa_table)
            )

        elif self.has_temp_table:
            self._pl_dataframe = to_polars(
                self._drop_sort_distinct(
                    table=self.ddb.query(f"SELECT * FROM {self._tables['temp_table']}")
                )
            )

        elif self.has_relation:
            self._pd_dataframe = to_polars(self._drop_sort_distinct(table=self.rel))

        else:
            if not self.has_dataset:
                self.load_dataset()
            self._pl_dataframe = to_polars(
                self._drop_sort_distinct(table=self.dataset)
            )

        return self._pl_dataframe

    @log_decorator()
    def to_pandas(
        self,
    ):
        if self._caching and not self.cached:
            self._to_cache()

        if self.has_table_:
            self._pd_dataframe = to_pandas(
                self._drop_sort_distinct(
                    table=self.ddb.query(f"SELECT * FROM {self._tables['table_']}")
                )
            )

        elif self.has_pa_table:
            self._pd_dataframe = to_pandas(
                self._drop_sort_distinct(table=self.pa_table)
            )

        elif self.has_temp_table:

            self._pd_dataframe = to_pandas(
                self._drop_sort_distinct(
                    table=self.ddb.query(f"SELECT * FROM {self._tables['temp_table']}")
                )
            )

        elif self.has_relation:
            self._pd_dataframe = to_pandas(self._drop_sort_distinct(table=self.rel))

        else:
            if not self.has_dataset:
                self.load_dataset()
            self._pd_dataframe = to_pandas(self._drop_sort_distinct(table=self.dataset))

        return self._pd_dataframe

    @log_decorator()
    def execute(self, *args, **kwargs):
        return self.ddb.execute(*args, **kwargs)

    @log_decorator()
    def query(self, *args, **kwargs):
        return self.ddb.query(*args, **kwargs)

    @property
    def dataset(self) -> ds.FileSystemDataset:
        if not self.has_dataset:
            self.load_dataset()

        return self._dataset

    @property
    def pa_table(self) -> pa.Table:
        if not hasattr(self, "_pa_table"):
            self.load_pa_table()

        return self._pa_table

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
    def has_table_(self) -> bool:
        return "table_" in self._tables

    @property
    def has_pa_table(self) -> bool:
        return "pa_table" in self._tables

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
    def buffer_size(self):
        if not hasattr(self, "_buffer_size"):
            self._buffer_size = self.pa_table.get_total_buffer_size()

            return self._buffer_size

    @property
    def cached(self) -> bool:
        return self._cached

    @property
    def disk_usage(self):
        if not hasattr(self, "_disk_usage"):
            self._disk_usage = self._fs.du(self._path, total=True)
        return self._disk_usage

    def get_disk_usage(self, unit: str = "MB"):
        return f"{convert_size_unit(self.disk_usage, unit=unit):.1f} {unit}"

    @property
    def tables(self):
        return self._tables

    def get_buffer_size(self, unit: str = "MB"):
        return f"{convert_size_unit(self.buffer_size, unit=unit):.1f} {unit}"


class TimeFlyReader(Reader):
    def __init__(
        self,
        base_path: str,
        timefly: str | dt.datetime | None = None,
        bucket: str | None = None,
        name: str | None = None,
        partitioning: ds.Partitioning | list | str | None = None,
        format: str | None = "parquet",
        ddb: duckdb.DuckDBPyConnection | None = None,
        caching: bool = False,
        cache_storage: str | None = "/tmp/pydala/",
        protocol: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        storage_options: dict = {},
        fsspec_fs: spec.AbstractFileSystem | None = None,
        pyarrow_fs: FileSystem | None = None,
        use_pyarrow_fs: bool = False,
    ) -> None:
        self._base_path = base_path
        self.timefly = TimeFly(
            path=self._base_path,
            bucket=bucket,
            fsspec_fs=fsspec_fs,
            protocol=protocol,
            profile=profile,
            endpoint_url=endpoint_url,
            storage_options=storage_options,
        )
        if timefly is not None:
            self._timefly = (
                timefly
                if isinstance(timefly, dt.datetime)
                else dt.datetime.fromisoformat(timefly)
            )
        else:
            self._timefly = None
        self._snapshot_path = self.timefly._find_snapshot_subpath(timefly=self._timefly)

        super().__init__(
            path=os.path.join(self._base_path, self._snapshot_path),
            bucket=bucket,
            name=name,
            partitioning=partitioning,
            format=format,
            ddb=ddb,
            caching=caching,
            cache_storage=cache_storage,
            protocol=protocol,
            profile=profile,
            endpoint_url=endpoint_url,
            storage_options=storage_options,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
            use_pyarrow_fs=use_pyarrow_fs,
        )

    def set_snapshot(self, snapshot):
        self._snapshot_path = self.timefly._find_snapshot_subpath(snapshot)
        self._path = os.path.join(self._base_path, self._snapshot_path)
        if self.has_dataset:
            del self._dataset
            self.load_dataset()
        if self.has_pa_table:
            del self._pa_table
            self.load_pa_table()
        if self.has_pd_dataframe:
            del self._pd_dataframe
            self.to_pandas()
        if self.has_pl_dataframe:
            del self._pl_dataframe
            self.to_polars()
        if self.has_relation:
            del self._rel
            self.to_relation()
        if self.has_temp_table:
            self.ddb.query(f"DROP TABLE {self._tables['temp_table']}")
            self.create_temp_table()
