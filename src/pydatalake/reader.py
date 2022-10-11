import duckdb
import pandas as pd
import polars as pl
import pyarrow.dataset as ds
import pyarrow.feather as pf
import pyarrow.filesystem as pafs
import pyarrow.paquet as pq
import s3fs

from .utils import is_file
from .utils import open as open_
from .utils import path_exists, sort_table, to_ddb_relation


class Reader:
    def __init__(
        self,
        path: str,
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: pafs.FileSystem | s3fs.S3FileSystem | None = None,
        format: str | None = "parquet",
        sort_by: str | list | None = None,
        ddb: duckdb.DuckDBPyConnection | None = None,
    ):
        self._path = path
        self._filesystem = filesystem
        self._format = format
        self._partitioning = partitioning
        self._sort_by = sort_by
        if ddb is not None:
            self.ddb = ddb
        else:
            self.ddb = duckdb.connect()
        self.ddb.execute("SET temp_directory='/tmp/duckdb/'")

    @property
    def _path_exists(self) -> bool:
        return path_exists(self._path, self._filesystem)

    @property
    def _is_file(self) -> bool:
        return is_file(self._path, self._filesystem)

    def load_dataset(self, name="pa_dataset", **kwargs):

        self._pa_dataset = ds.dataset(
            source=self._path,
            format=self._format,
            filesystem=self._filesystem,
            partitioning=self._partitioning,
            **kwargs,
        )

        self._pa_dataset_name = name
        self.ddb.register(name, self._pa_dataset)

    def load_table(self, name="pa_table", sort_by: str | list | None = None, **kwargs):
        if sort_by is not None:
            self._sort_by = sort_by

        if self._format == "parquet":
            self._pa_table = pq.read_table(
                self._path,
                partitioning=self._partitioning,
                filesystem=self._filesystem,
                **kwargs,
            )

        else:
            if self._is_file:
                if self._filesystem is not None:
                    with open_(self._path, self._filesystem) as f:
                        self._pa_table = pf.read_feather(f, **kwargs)
                else:
                    self._pa_table = pf.read_feather(self._path, **kwargs)
            else:
                if not hasattr(self, "_pa_dataset"):
                    self.load_dataset()
                self._pa_table = self._pa_dataset.to_table(**kwargs)

        if self._sort_by is not None:
            self._sort_pa_table(sort_by=self._sort_by)

        self._pa_table_name = name
        self.ddb.register(name, self._pa_table)

    def _sort_pa_table(self, sort_by: str | list | None = None):
        if not hasattr(self, "_pa_table"):
            self.load_table()

        if sort_by is not None:
            self._sort_by = sort_by

        self._pa_table = sort_table(
            table_=self._pa_table, sort_by=self._sort_by, ddb=self.ddb
        )

    def create_temp_table(
        self,
        name: str = "temp_table",
        sort_by: str | list | None = None,
        distinct: bool = False,
    ):

        if hasattr(self, "_pa_table"):
            table_name = self._pa_table_name
        else:
            if not hasattr(self, "_pa_dataset"):
                self.load_dataset()
            table_name = self._pa_dataset_name

        sql = f"CREATE OR REPLACE TEMP TABLE {name} AS  SELECT * FROM {table_name}"

        if sort_by is not None:
            self._sort_by = sort_by

            if isinstance(sort_by, list):
                sort_by = ",".join(sort_by)

            sql += f" ORDER BY {sort_by}"

        if distinct:
            sql = sql.replace("SELECT *", "SELECT DISTINCT *")

        self.ddb.execute(sql)

    def set_table(
        self,
        create_temp_table: bool = False,
        sort_by: str | list | None = None,
        distinct: bool = False,
    ):

        if create_temp_table:
            self.create_temp_table(sort_by=sort_by)

        if "temp_table" in self.ddb.execute("SHOW TABLES").df()["name"].tolist():
            self._table = self.ddb.query(f"SELECT * FROM temp_table")

        elif hasattr(self, "_pa_table"):
            if sort_by is not None:
                self._pa_table = self._sort_pa_table(sort_by=sort_by)
            self._table = to_ddb_relation(
                table=self._pa_table, ddb=self.ddb, sort_by=sort_by, distinct=distinct
            )

        else:
            if not hasattr(self, "_pa_dataset"):
                self.load_dataset()
            self._table = to_ddb_relation(
                table=self._pa_dataset, ddb=self.ddb, sort_by=sort_by, distinct=distinct
            )

    def execute(self, *args, **kwargs):
        return self.execute(*args, **kwargs)

    def query(self, *args, **kwargs):
        return self.query(*args, **kwargs)

    @property
    def pa_dataset(self) -> ds.FileSystemDataset:
        if not hasattr(self, "_pa_dataset"):
            self.load_dataset()

        return self._pa_dataset

    @property
    def dataset(self) -> ds.FileSystemDataset:
        return self.pa_dataset

    @property
    def pa_table(self) -> pa.Table:
        if not hasattr(self, "_pa_table"):
            if self.ddb is not None:
                if (
                    "temp_table"
                    in self.ddb.execute("SHOW TABLES").df()["name"].tolist()
                ):
                    self._pa_table = self.ddb.query(f"SELECT * FROM temp_table").arrow()
                else:
                    self.load_table()
            else:
                self.load_table()

        return self._pa_table

    @property
    def table(self) -> duckdb.DuckDBPyRelation:

        if not hasattr(self, "_table"):
            self.set_table()

        return self._table

    @property
    def pl_dataframe(self) -> pl.DataFrame:
        if not hasattr(self, "_pl_dataframe"):
            self._pl_dataframe = pl.from_arrow(self.pa_table)
        return self._pl_dataframe

    @property
    def pd_dataframe(self) -> pd.DataFrame:
        if not hasattr(self, "_pd_dataframe"):
            self._pd_dataframe = self.table.df()
        return self._pd_dataframe
