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


class Dataset:
    def __init__(
        self,
        path: str,
        partitioning: ds.Partitioning | list[str] | str | None = None,
        filesystem: fs.FileSystem | None = None,
        format: str | None = None,
    ):
        self._path = path
        self._filesystem = filesystem
        self._format = format
        self._partitioning = partitioning
        self._db = duckdb.connect()
        self._db.execute("SET temp_director='/tmp/duckdb/'")

    def _load_dataset(self, name: str = "dataset", **kwargs):

        self._ds = ds.dataset(
            source=self._path,
            format=self._format,
            filesystem=self._filesystem,
            partitioning=self._partitioning,
            **kwargs,
        )
        self._db.register(name, self._ds)

    def _load_table(self, name: str = "mem_table", **kwargs):
        if self._format == "parquet":
            self._table = pq.read_table(
                self._path,
                partitioning=self._partitioning,
                filesystem=self._filesystem,
                **kwargs,
            )

        # elif self._format == "feather":
        else:
            if self._filesystem is not None:
                if self._filesystem.get_file_info(self._path).is_file:
                    with self._filesystem.open_input_file(self._path) as f:
                        self._table = pf.read_feather(f, **kwargs)
                else:
                    if not hasattr(self, "_ds"):
                        self._load_dataset()

                    self._table = self._ds.to_table(**kwargs)

            else:
                if Path(self._path).is_file():
                    self._table = pf.read_feather(self._path, **kwargs)
                else:
                    if not hasattr(self, "_ds"):
                        self._load_dataset()

                    self._table = self._ds.to_table(**kwargs)

        self._db.register(name, self._table)

    def _gen_path(
        self,
        path: str | None = None,
        base_name: str = "data",
        partition_names: tuple | None = None,
        partition_by_date: bool = False,
    ):
        if path is None:
            path = self._path
        parts = [path]

        if partition_names is not None:
            parts.extend(partition_names)

        if partition_by_date:
            parts.append(str(dt.date.today()))

        parts.append(base_name + f"-{uuid.uuid4().hex}")

        path_ = Path(*parts)

        if self._filesystem is None:
            path_.parent.mkdir(exist_ok=True, parents=True)

        return path

    def write_table(
        self,
        table: pa.Table,
        path: Path | str,
        compression: str = "zstd",
        **kwargs,
    ):

        filesystem = kwargs.get("filesystem", self._filesystem)
        format = (
            kwargs.get("format", self._format)
            .replace("arrow", "feather")
            .replace("ipc", "feather")
        )
        path = str(path) + f".{format}"

        if format == "feather":
            if filesystem is not None:
                if hasattr(filesystem, "open"):
                    with filesystem.open(path) as f:
                        pf.write_feather(table, f, compression=compression, **kwargs)
                else:
                    with filesystem.open_output_stream(path) as f:
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

    def _to_duckdbrelation(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.Dataset
        | pd.DataFrame
        | pl.DataFrame
        | str,
        use_temp_table: bool,
    ):
        if isinstance(table, pa.Table):
            table_ = self._db.from_arrow(table)
        elif isinstance(table, ds.Dataset):
            _table = table
            table_ = self._db.query("SELECT * FROM _table")
        elif isinstance(table, pd.DataFrame):
            table_ = self._db.from_df(table)
        elif isinstance(table, pl.DataFrame):
            table_ = self._db.from_arrow(table.to_arrow())
        elif isinstance(table, str):
            if ".parquet" in table:
                table_ = self._db.from_parquet(table)
            else:
                table_ = self._db.query(f"SELECT * FROM '{table}'")
        else:
            table_ = table

        if use_temp_table:
            self._db.execute(
                "CREATE OR REPLACE TEMP TABLE temp_table AS SELECT * FOM table_"
            )
            table_ = self._db.query("SELECT * FROM temp_table")

        return table_
    
    def temp_table(self, table_: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.Dataset
        | pd.DataFrame
        | pl.DataFrame
        | str, name:str="temp_table"):
        
        self._db.execute(
                "CREATE OR REPLACE TEMP TABLE temp_table AS SELECT * FOM table_"
            )
            table_ = self._db.query(f"SELECT * FROM {name}")
        
    
    

    def write_dataset(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.Dataset
        | pd.DataFrame
        | pl.DataFrame
        | str,
        path: str | None,
        repartitioning: list | str | None = None,
        partition_by_date: bool = False,
        n_rows: int | None = None,
        use_temp_table: bool = True,
        **kwargs,
    ):
        table_ = self._to_duckdbrelation(table=table, use_temp_table=use_temp_table)

        if repartitioning is not None:
            if isinstance(repartitioning, str):
                self._partitioning = [repartitioning]
            else:
                self._partitioning = repartitioning

        if self._partitioning is not None:
            partitions = (
                table_.project(",".join(self._partitioning)).distinct().fetchall()
            )

            for partition_names in partitions:
                path_ = self._gen_path(
                    path=path,
                    partition_names=partition_names,
                    partition_by_date=partition_by_date,
                )

                filter_ = []
                for p in zip(self._partitioning, partition_names):
                    filter_.append(f"{p[0]}='{p[1]}'")
                filter_ = " AND ".join(filter_)

                table_part = table_.filter(filter_)

                if n_rows is None:
                    self.write_table(table=table_part.arrow(), path=path_, **kwargs)
                else:
                    for i in range(table_part.shape[0] // n_rows + 1):
                        self.write_table(
                            table=table_part.limit(n_rows, offset=i * n_rows).arrow(),
                            path=str(path_) + f"-{i}",
                            **kwargs,
                        )

        else:
            path_ = self._gen_path(
                path=path, partition_names=None, partition_by_date=partition_by_date
            )
            self.write_table(table=table_.arrow(), path=path_, **kwargs)

        if use_temp_table:
            self._db.execute("DROP TABLE temp_table")

    def append(
        self,
        table: duckdb.DuckDBPyRelation
        | pa.Table
        | ds.Dataset
        | pd.DataFrame
        | pl.DataFrame
        | str,
        repartitioning: list | str | None = None,
        partition_by_date: bool = False,
        n_rows: int | None = None,
        use_temp_table: bool = True,
        **kwargs,
    ):
        

        self.write_dataset(
            table=table,
            path=None,
            repartitioning=repartitioning,
            partition_by_date=partition_by_date,
            n_rows=n_rows,
            use_temp_table=use_temp_table,
            **kwargs,
        )

    @property
    def dataset(self, **kwargs):
        if not hasattr(self, "_ds") or len(kwargs) > 0:
            self._load_dataset(**kwargs)

        return self._ds

    @property
    def table(self, **kwargs):
        if not hasattr(self, "_table") or len(kwargs) > 0:
            self._load_table(**kwargs)

        return self._table
