import uuid
import datetime as dt
import duckdb
import pyarrow as pa
import pyarrow.feather as pf
import pyarrow.parquet as pq
import pyarrow.fs as fs
import pandas as pd
from pathlib import Path


class DatasetWriter:
    def __init__(
        self,
        base_dir: str,
        base_name: str = "data",
        partitioning_cols: list = None,
        date_partition: bool = False,
        n_rows: int = 100000,
        sorting: list | None = None,
        format: str = "feather",
        compression: str = "zstd",
        filesystem: fs.S3FileSystem | None = None,
    ):
        self._base_dir = base_dir
        self._base_name = base_name
        self._partitioning_cols = partitioning_cols
        self._date_partition = date_partition
        self._format = format
        self._n_rows = n_rows
        self._sorting = sorting
        self._compression = compression
        self._filesystem = filesystem

    def _gen_path(
        self,
        partitioning_names: list | None = None,
    ):
        parts = [self._base_dir]

        if partitioning_names is not None:
            parts.extend(partitioning_names)

        if self._date_partition:
            parts.append(str(dt.date.today()))

        parts.append(self._base_name + f"-{uuid.uuid4().hex}.{self._format}")

        path = Path(*parts)

        if self._filesystem is None:
            path.parent.mkdir(exist_ok=True, parents=True)

        return path

    def _write_from_duckdb(
        self,
        table: duckdb.DuckDBPyRelation,
        partitioning_names: list | None,
        **kwargs,
    ):
        path = self._gen_path(partitioning_names=partitioning_names)

        if partitioning_names is not None:
            partitioning_names = [f"'{_pn}'" for _pn in partitioning_names]
            _filter = " AND ".join(
                [
                    "=".join(cn)
                    for cn in list(zip(self._partitioning_cols, partitioning_names))
                ]
            )

            table_ = table.filter(_filter)

        if self._n_rows is not None:
            for i in range(table_.shape[0] // self._n_rows + 1):
                path_ = str(path).replace(".", f"-{i}.")
                self._write(
                    table_.limit(self._n_rows, offset=i * self._n_rows).arrow(),
                    path=path_,
                    **kwargs,
                )

    def _write_from_pandas(
        self,
        table: pd.DataFrame,
        partitioning_names: list | None,
        **kwargs,
    ):
        path = self._gen_path(partitioning_names=partitioning_names)

        if partitioning_names is not None:

            table_ = pa.Table.from_pandas(
                table.set_index(self._partitioning_cols).loc[partitioning_names],
                preserve_index=False,
            )

        else:
            table_ = pa.Table.from_pandas(table, preserve_index=False)

        if self._n_rows is not None:

            table_ = [
                pa.Table.from_batches([t], schema=table_.schema)
                for t in table_.to_batches(max_chunksize=self._n_rows)
            ]
        else:
            table_ = [table_]

        self._write(table=table_, path=path, **kwargs)

    def _write(self, table: pa.Table | list[pa.Table], path: str, **kwargs):
        if isinstance(table, pa.Table):
            table = [table]
        len_table = len(table)
        for i in range(len_table):
            path_ = str(path).replace(".", f"-{i}.") if len_table > 1 else str(path)
            if self._format == "feather":
                if self._filesystem is not None:

                    with self._filesystem.open_output_stream(path_) as f:
                        pf.write_feather(
                            table[i], f, compression=self._compression, **kwargs
                        )
                else:
                    pf.write_feather(
                        table[i],
                        path_,
                        compression=self._compression,
                        **kwargs,
                    )
            else:
                pq.write_table(
                    table[i],
                    path_,
                    compression=self._compression,
                    filesystem=self._filesystem,
                    **kwargs,
                )

    def write_from_pandas(
        self,
        table: pa.Table | pd.DataFrame,
        # verbose: int = 10,
        **kwargs,
    ):

        if isinstance(table, pa.Table):
            table = table.to_pandas()
        if self._sorting is not None:
            table = table.sort_values(by=self._sorting)
        if self._partitioning_cols is not None:

            partitioning_names = (
                duckdb.from_df(table)
                .project(",".join(self._partitioning_cols))
                .distinct()
                .fetchall()
            )

            for partitioning_names_ in partitioning_names:
                self._write_from_pandas(
                    table=table, partitioning_names=partitioning_names_, **kwargs
                )

        else:
            self._write_from_pandas(table=table, partitioning_names=None)

    def write_from_duckdb(self, table: duckdb.DuckDBPyRelation, **kwargs):

        if self._sorting is not None:
            table = table.order(",".join(self._sorting))
        if self._partitioning_cols is not None:
            partitioning_names = (
                table.project(",".join(self._partitioning_cols)).distinct().fetchall()
            )
            for partitioning_names_ in partitioning_names:
                self._write_from_duckdb(
                    table=table, partitioning_names=partitioning_names_, **kwargs
                )
        else:
            self._write_from_duckdb(table=table, partitioning_names=None)

    def write(self, table: pa.Table | pd.DataFrame | duckdb.DuckDBPyRelation, **kwargs):
        if isinstance(table, duckdb.DuckDBPyRelation):
            self.write_from_duckdb(table=table, **kwargs)
        else:
            self.write_from_pandas(table=table, **kwargs)

    def repartition(
        self,
        n_rows: list | None = None,
        partitioning_cols: list | None = None,
    ):
        if partitioning_cols is not None:
            self._partitioning_cols = partitioning_cols
        if n_rows is not None:
            self._n_rows = n_rows
        return self


def write(
    table: pa.Table | pd.DataFrame | duckdb.DuckDBPyRelation,
    base_dir: str,
    base_name: str = "data",
    partitioning_cols: list = None,
    date_partition: bool = False,
    n_rows: int = 100000,
    sorting: list | None = None,
    format: str = "feather",
    compression: str = "zstd",
    filesystem: fs.S3FileSystem | None = None,
    **kwargs,
):
    dsw = DatasetWriter(
        base_dir=base_dir,
        base_name=base_name,
        partitioning_cols=partitioning_cols,
        date_partition=date_partition,
        n_rows=n_rows,
        sorting=sorting,
        format=format,
        compression=compression,
        filesystem=filesystem,
    )
    dsw.write(table=table, **kwargs)
