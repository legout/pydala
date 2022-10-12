import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.feather as pf
import pyarrow.filesystem as pafs
import pyarrow.parquet as pq
import s3fs

from ..utils import is_file
from ..utils import open as open_
from ..utils import path_exists, sort_table, to_ddb_relation


class Reader:
    def __init__(
        self,
        path: str,
        partitioning: ds.Partitioning | str | None = None,
        filesystem: pafs.FileSystem | s3fs.S3FileSystem | None = None,
        format: str | None = "parquet",
        sort_by: str | list | None = None,
        ascending: str | list | None = None,
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

    def set_dataset(self, name: str = "pa_dataset", **kwargs):
        if self._path_exists:
            self._pa_dataset = ds.dataset(
                source=self._path,
                format=self._format,
                filesystem=self._filesystem,
                partitioning=self._partitioning,
                **kwargs,
            )

            self._pa_dataset_name = name
            self.ddb.register(name, self._pa_dataset)

    def _load_feather(self, **kwargs):
        if self._path_exists:
            if self._is_file:
                if self._filesystem is not None:
                    with open_(self._path, self._filesystem) as f:
                        self._pa_table = pf.read_feather(f, **kwargs)
                else:
                    self._pa_table = pf.read_feather(self._path, **kwargs)
            else:
                if not hasattr(self, "_pa_dataset"):
                    self.set_dataset()
                self._pa_table = self._pa_dataset.to_table(**kwargs)

    def _load_parquet(self, **kwargs):
        if self._path_exists:
            self._pa_table = pq.read_table(
                self._path,
                partitioning=self._partitioning,
                filesystem=self._filesystem,
                **kwargs,
            )

    def _load_csv(self, **kwargs):

        pass

    def sort(
        self, which: str, sort_by: str | list, ascending: bool | list | None = None
    ):
        table = sort_table(
            table=self.__getattribute__("_" + which),
            sort_by=sort_by,
            ascending=ascending,
        )
        self.__setattr__("_" + which, table)

    def load_pa_table(
        self,
        name: str = "pa_table",
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        **kwargs,
    ):
        if sort_by is not None:
            self._sort_by = sort_by

        if ascending is not None:
            self._ascending = ascending

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

        if self._sort_by is not None:
            self._pa_table = sort_table(
                self._pa_table, sort_by=self._sort_by, ascending=self._ascending
            )

        self._pa_table_name = name
        self.ddb.register(name, self._pa_table)

    def create_temp_table(
        self,
        name: str = "temp_table",
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool = False,
    ):

        if hasattr(self, "_pa_table"):
            table_name = self._pa_table_name
        else:
            if not hasattr(self, "_pa_dataset"):
                self.set_dataset()
            table_name = self._pa_dataset_name

        sql = f"CREATE OR REPLACE TEMP TABLE {name} AS  SELECT * FROM {table_name}"

        if sort_by is not None:
            self._sort_by = sort_by

            if ascending is None:
                self._ascending = ascending
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

            sql += f" ORDER BY {sort_by}"

        if distinct:
            sql = sql.replace("SELECT *", "SELECT DISTINCT *")

        self.ddb.execute(sql)

    def create_relation(
        self,
        create_temp_table: bool = False,
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool = False,
    ):
        if sort_by is not None:
            self._sort_by = sort_by

        if ascending is not None:
            self._ascending = ascending

        if create_temp_table:
            self.create_temp_table(sort_by=self._sort_by, distinct=distinct)

        if self.has_temp_table:
            self._table = self.ddb.query("SELECT * FROM temp_table")

        elif hasattr(self, "_pa_table"):

            self._table = to_ddb_relation(
                table=self._pa_table,
                ddb=self.ddb,
                sort_by=sort_by,
                ascending=self._ascending,
                distinct=distinct,
            )

        else:
            if not hasattr(self, "_pa_dataset"):
                self.set_dataset()
            self._table = to_ddb_relation(
                table=self._pa_dataset,
                ddb=self.ddb,
                sort_by=sort_by,
                ascending=self._ascending,
                distinct=distinct,
            )

    def execute(self, *args, **kwargs):
        return self.execute(*args, **kwargs)

    def query(self, *args, **kwargs):
        return self.query(*args, **kwargs)

    @property
    def pa_dataset(self) -> ds.FileSystemDataset:
        if not hasattr(self, "_pa_dataset"):
            self.set_dataset()

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
                    self._pa_table = self.ddb.query("SELECT * FROM temp_table").arrow()
                else:
                    self.load_pa_table()
            else:
                self.load_pa_table()

        return self._pa_table

    @property
    def table(self) -> duckdb.DuckDBPyRelation:

        if not hasattr(self, "_table"):
            self.create_relation()

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

    @property
    def has_temp_table(self) -> bool:
        return "temp_table" in self.ddb.execute("SHOW TABLES").df()["name"].tolist()
