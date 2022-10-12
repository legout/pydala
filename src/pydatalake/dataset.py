import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import s3fs
import pyarrow.feather as pf
import pyarrow.parquet as pq
import polars as pl
import pandas as pd
from pathlib import Path
import duckdb
import uuid
import datetime as dt
from .utils import path_exists, is_file, sort_table, to_ddb_relation, open as open_


class Reader:
    def __init__(
        self,
        path: str,
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: fs.FileSystem | s3fs.S3FileSystem | None = None,
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

    def load_parquet(self, name:str, sort_by: str | list | None = None, **kwargs):
        
    
    def load_table(self, name:str="pa_table", sort_by: str | list | None = None, **kwargs):
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
        self, name: str = "temp_table", sort_by: str | list | None = None, distinct:bool=False
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
        self, create_temp_table: bool = False, sort_by: str | list | None = None, distinct:bool=False
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


class Writer:
    def __init__(
        self,
        path: str,
        base_name: str = "data",
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: fs.FileSystem | None = None,
        format: str | None = "parquet",
        compression: str | None = "zstd",
        sort_by: str | list | None = None,
        ddb: duckdb.DuckDBPyConnection | None = None,
    ):
        self._path = path
        self._base_name = base_name
        self._partitioning = (
            [partitioning] if isinstance(partitioning, str) else partitioning
        )
        self._filesystem = filesystem
        self._format = format
        self._compression = compression
        self._sort_by = sort_by
        if ddb is not None:
            self.ddb = ddb
        else:
            self.ddb = duckdb.connect()
        self.ddb.execute("SET temp_directory='/tmp/duckdb/'")

    def _gen_path(
        self, partition_names: tuple | None = None, with_time_partition: bool = False
    ):
        path = Path(self._path)
        if path.suffix != "":
            parts = [path.parent]
        else:
            parts = [path]

        if partition_names is not None:
            parts.extend(partition_names)

        if with_time_partition:
            parts.append(str(dt.datetime.today()))

        if path.suffix == "":
            parts.append(self._base_name + f"-{uuid.uuid4().hex}.{self._format}")
        else:
            parts.append(path.suffix)

        path = Path(*parts)

        if self._filesystem is None:
            path.mkdir.parent(exist_ok=True, parents=True)

        return path

    def write_table(
        self,
        table: pa.Table,
        path: Path | str,
        row_group_size: int | None = None,
        compression: str | None = None,
        **kwargs,
    ):

        filesystem = kwargs.pop("filesystem", self._filesystem)
        compression = self._compression if compression is None else compression

        format = (
            kwargs.pop("format", self._format)
            .replace("arrow", "feather")
            .replace("ipc", "feather")
        )

        if format == "feather":
            if filesystem is not None:
                with open_(str(path), self._filesystem) as f:
                    pf.write_feather(table, f, compression=compression, **kwargs)

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
                row_group_size=row_group_size,
                compression=compression,
                filesystem=filesystem,
                **kwargs,
            )

    def write_dataset(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.FileSystemDataset
        | pd.DataFrame
        | pl.DataFrame
        | str,
        path: str | None = None,
        format: str | None = None,
        compression: str | None = None,
        partitioning: list | str | None = None,
        sort_by: str | list | None = None,
        distinct:bool=False,
        rows_per_file: int | None = None,
        row_group_size: int | None = None,
        with_time_partition: bool = False,
        **kwargs,
    ):
        self._path = path if path is not None else self._path
        format = self._format if format is None else format
        compression = self._compression if compression is None else compression

        if sort_by is None:
            sort_by = self._sort_by
        else:
            self._sort_by = sort_by

        table = to_ddb_relation(table=table, ddb=self.ddb, sort_by=sort_by, distinct=distinct)

        if partitioning is not None:
            if isinstance(partitioning, str):
                partitioning = [partitioning]
        else:
            partitioning = self._partitioning

        if partitioning is not None:
            partitions = table.project(",".join(partitioning)).distinct().fetchall()

            for partition_names in partitions:

                filter_ = []
                for p in zip(partitioning, partition_names):
                    filter_.append(f"{p[0]}='{p[1]}'")
                filter_ = " AND ".join(filter_)

                table_part = table.filter(filter_)

                if rows_per_file is None:

                    self.write_table(
                        table=table_part.arrow(),
                        path=self._gen_path(
                            partition_names=partition_names,
                            with_time_partition=with_time_partition,
                        ),
                        format=format,
                        compression=compression,
                        row_group_size=row_group_size,
                        **kwargs,
                    )
                else:
                    for i in range(table_part.shape[0] // rows_per_file + 1):
                        self.write_table(
                            table=table_part.limit(
                                rows_per_file, offset=i * rows_per_file
                            ).arrow(),
                            path=self._gen_path(
                                partition_names=partition_names,
                                with_time_partition=with_time_partition,
                            ),
                            format=format,
                            compression=compression,
                            row_group_size=row_group_size,
                            **kwargs,
                        )

        else:
            if rows_per_file is None:

                self.write_table(
                    table=table.arrow(),
                    path=self._gen_path(
                        partition_names=None,
                        with_time_partition=with_time_partition,
                    ),
                    format=format,
                    compression=compression,
                    row_group_size=row_group_size,
                    **kwargs,
                )
            else:
                for i in range(table.shape[0] // rows_per_file + 1):
                    self.write_table(
                        table=table.limit(
                            rows_per_file, offset=i * rows_per_file
                        ).arrow(),
                        path=self._gen_path(
                            partition_names=None,
                            with_time_partition=with_time_partition,
                        ),
                        format=format,
                        compression=compression,
                        row_group_size=row_group_size,
                        **kwargs,
                    )


class Dataset:
    def __init__(
        self,
        path: str,
        base_name: str = "data",
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: dict | fs.FileSystem | None = None,
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
        sort_by:str|list|None=None,
        distinct:bool=False,
        **kwargs,
    ):
        if table is not None:
           table = to_ddb_relation(table=table, ddb=self.ddb, sort_by=sort_by, distinct=distinct).arrow()

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
        distinct:bool=False,
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
        
        if path is None:
            if with_mem_table is False and with_temp_table is False:
                use_tmp_directory=True
                
        if use_tmp_directory:
             
            
                

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
