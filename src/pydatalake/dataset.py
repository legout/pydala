import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.feather as pf
import pyarrow.parquet as pq
import polars as pl
import pandas as pd
from pathlib import Path
import duckdb
import uuid
import datetime as dt


class Reader:
    def __init__(
        self,
        path: str,
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: fs.FileSystem | None = None,
        format: str | None = "parquet",
        ddb: duckdb.DuckDBPyConnection | None = None,
    ):
        self._path = path
        self._filesystem = filesystem
        self._format = format
        self._partitioning = partitioning
        self.ddb = ddb
        if self.ddb is not None:
            self.ddb.execute("SET temp_directory='/tmp/duckdb/'")

    @property
    def _path_exists(self) -> bool:
        if self._filesystem is not None:
            if hasattr(self._filesystem, "exists"):
                return self._filesystem.exists(self._path)
            else:
                return self._filesystem.get_file_info(self._path).type > 0
        else:
            return Path(self._path).exists()

    @property
    def _is_file(self) -> bool:
        if self._filesystem is not None:
            if hasattr(self._filesystem, "isfile"):
                return self._filesystem.isfile(self._path)
            else:
                return self._filesystem.get_file_info(self._path).type == 2
        else:
            return Path(self._path).is_file()

    def load_dataset(self, name="pa_dataset", **kwargs):

        self._pa_dataset = ds.dataset(
            source=self._path,
            format=self._format,
            filesystem=self._filesystem,
            partitioning=self._partitioning,
            **kwargs,
        )
        if self.ddb is not None:
            self._pa_dataset_name = name
            self.ddb.register(name, self._pa_dataset)

    def load_table(self, name="pa_table", **kwargs):
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
                    if hasattr(self._filesystem, "open"):
                        with self._filesystem.open(self._path) as f:
                            self._pa_table = pf.read_feather(f, **kwargs)
                    else:
                        with self._filesystem.open_input_file(self._path) as f:
                            self._pa_table = pf.read_feather(f, **kwargs)
                else:
                    self._pa_table = pf.read_feather(self._path, **kwargs)
            else:
                if not hasattr(self, "_pa_dataset"):
                    self.load_dataset()
                self._pa_table = self._pa_dataset.to_table(**kwargs)

        if self.ddb is not None:
            self._pa_table_name = name
            self.ddb.register(name, self._pa_table)

    def _create_temp_table(self, name: str = "temp_table"):
        if self.ddb is not None:
            if hasattr(self, "_pa_table"):
                self.ddb.execute(
                    f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM {self._pa_table_name}"
                )
            elif hasattr(self, "_pa_dataset"):
                self.ddb.execute(
                    f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM {self._pa_dataset_name}"
                )

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
                if "temp_table" in self.ddb.execute("SHOW TABLES").df()["name"].tolist():
                    self._pa_table = self.query(f"SELECT * FROM temp_table").arrow()
                else:
                    self.load_table()
            else:
                self.load_table()

        return self._pa_table

    @property
    def table(self) -> duckdb.DuckDBPyRelation:
        if self.ddb is not None:

            if "temp_table" in self.ddb.execute("SHOW TABLES").df()["name"].tolist():
                self._table = self.ddb.query(f"SELECT * FROM temp_table")

            elif hasattr(self, "_pa_table"):
                self._table = self.ddb.from_arrow(self._pa_table)

            elif hasattr(self, "_pa_dataset"):
                self._table = self.ddb.from_arrow(self._pa_dataset)
                
            else:
                self.load_dataset()
                self._table = self.ddb.from_arrow(self._pa_dataset)

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


class Writer:
    def __init__(
        self,
        path: str,
        base_name: str = "data",
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: fs.FileSystem | None = None,
        format: str | None = "parquet",
        compression: str | None = "zstd",
        ddb: duckdb.DuckDBPyConnection | None = None,
    ):
        # self._table = table
        self._path = path
        self._base_name = base_name
        self._partitioning = partitioning
        self._filesystem = filesystem
        self._format = format
        self._compression = compression
        self.ddb = ddb
        if ddb is not None:
            self.ddb.execute("SET temp_directory='/tmp/duckdb/'")

    def _gen_path(
        self, partition_names: tuple | None = None, with_time_partition: bool = False
    ):

        parts = [self._path]

        if partition_names is not None:
            parts.extend(partition_names)

        if with_time_partition:
            parts.append(str(dt.datetime.today()))

        parts.append(self._base_name)  # + f"-{uuid.uuid4().hex}.{self._format}")

        path = Path(*parts)

        if self._filesystem is None:
            path.mkdir.parents(exist_ok=True, parents=True)

        return path

    def write_table(
        self,
        table: pa.Table,
        path: Path | str,
        **kwargs,
    ):

        filesystem = kwargs.get("filesystem", self._filesystem)
        compression = kwargs.get("compression", self._compression)
        format = (
            kwargs.get("format", self._format)
            .replace("arrow", "feather")
            .replace("ipc", "feather")
        )
        if isinstance(path, str):
            path = Path(path)

        if path.suffix == "":
            path = path / (self._base_name + f"-{uuid.uuid4().hex}.{self._format}")

        if format == "feather":
            if filesystem is not None:
                if hasattr(filesystem, "open"):
                    with filesystem.open(str(path)) as f:
                        pf.write_feather(table, f, compression=compression, **kwargs)
                else:
                    with filesystem.open_output_stream(str(path)) as f:
                        pf.write_feather(table, f, compression=compression, **kwargs)

            else:
                pf.write_feather(
                    table,
                    path,
                    compression=compression,
                    **kwargs,
                )
        else:
            pq.write_table(
                table,
                path,
                compression=compression,
                filesystem=filesystem,
                **kwargs,
            )

    def write_dataset(
        self,
        table: duckdb.DuckDBPyRelation,
        path: str | None = None,
        partitioning: list | str | None = None,
        with_time_partition: bool = False,
        n_rows: int | None = None,
        **kwargs,
    ):

        format = kwargs.get("format", self._format)
        compression = kwargs.get("compression", self._compression)

        if partitioning is not None:
            if isinstance(repartitioning, str):
                partitioning = [partitioning]
        else:
            partitioning = self._partitioning

        if partitioning is not None:
            partitions = table.project(",".join(partitioning)).distinct().fetchall()

            for partition_names in partitions:
                path_ = self._gen_path(
                    path=path,
                    partition_names=partition_names,
                    with_time_partition=with_time_partition,
                )

                filter_ = []
                for p in zip(partitioning, partition_names):
                    filter_.append(f"{p[0]}='{p[1]}'")
                filter_ = " AND ".join(filter_)

                table_part = table.filter(filter_)

                if n_rows is None:
                    self.write_table(
                        table=table_part.arrow(),
                        path=path_,
                        format=format,
                        compression=compression,
                        **kwargs,
                    )
                else:
                    for i in range(table_part.shape[0] // n_rows + 1):
                        self.write_table(
                            table=table_part.limit(n_rows, offset=i * n_rows).arrow(),
                            path=path_,
                            format=format,
                            compression=compression,
                            **kwargs,
                        )

        else:
            path_ = self._gen_path(partition_names=None, with_time_partition=False)
            self.write_table(table=table.arrow(), path=path_, **kwargs)


class Dataset:
    def __init__(
        self,
        path: str,
        base_name: str = "data",
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: dict | fs.FileSystem | None = None,
        format: str | None = "parquet",
    ):
        self._path = path
        self._base_name = base_name
        self._filesystem = filesystem
        self._format = format
        self._partitioning = partitioning
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
            ddb=self.ddb,
        )

    def _set_writer(
        self,
    ):
        self.writer = Writer(
            path=self._path,
            base_name=self._base_name,
            partitioning=self._partitioning,
            filesystem=elf._filesystem["writer"]
            if isinstance(self._filesystem, dict)
            else self._filesystem,
            format=self._format,
            ddb=self.ddb,
        )

    def _to_ddb_relation(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.FileSystemDataset
        | pd.DataFrame
        | pl.DataFrame
        | str,
        name="table_",
    ):

        if isinstance(table, pa.Table) or isinstance(table, ds.FileSystemDataset):
            table_ = self.ddb.from_arrow(table)
        elif isinstance(table, pd.DataFrame):
            table_ = self.ddb.from_df(table)
        elif isinstance(table, pl.DataFrame):
            table_ = self.ddb.from_arrow(table.to_arrow())
        elif isinstance(table, str):
            if ".parquet" in table:
                table_ = self.ddb.from_parquet(table)
            elif ".csv" in table:
                table_ = self.ddb.from_csv_auto(table)
            else:
                table_ = self.query(f"SELECT * FROM '{table}'")
        else:
            table_ = table

        table_.create_view(name)

        return table_

    def add_table(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.FileSystemDataset
        | pd.DataFrame
        | pl.DataFrame
        | str,
        name: str = "table_",
    ):
        self._table = self._to_ddb_relation(table, name=name)

    def query(self, *args, **kwargs) -> duckdb.DuckDBPyRelation:
        return self.ddb.query(*args, **kwargs)

    def execute(self, *args, **kwargs) -> duckdb.DuckDBPyResult:
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
        **kwargs,
    ):
        if table is not None:
            self.add_table(table)

        self._path = self._path if path is None else path
        self._partitioning = None
        self._set_writer()
        self.writer.write_table(table=self.pa_table, path=self._path, **kwargs)

    def write_dataset(
        self,
        table: duckdb.DuckDBPyRelation | None = None,
        path: str | None = None,
        partitioning: list | str | None = None,
        with_time_partition: bool = False,
        n_rows: int | None = None,
        **kwargs,
    ):
        if table is not None:
            self.add_table(table)

        self._path = path if path is None else self._path
        self._partitioning = (
            partitioning if partitioning is not None else self._partitioning
        )
        self._set_writer()

        self.writer.write_dataset(
            table=self.table,
            with_time_partition=with_time_partition,
            n_rows=n_rows,
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
