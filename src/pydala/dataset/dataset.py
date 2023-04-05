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
from ..utils.table import to_arrow, sort_table, distinct_table, to_batches


class Dataset:
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        schema: pa.Schema | Dict[str, str] | None = None,
        format: str = "parquet",
        filesystem: AbstractFileSystem | None = None,
        partitioning: pds.Partitioning | List[str] | str | None = None,
        ddb: duckdb.DuckDBPyConnection | None = None,
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

        self._format = format
        self._partitioning = partitioning
        self._ddb = (
            ddb if isinstance(ddb, duckdb.DuckDBPyConnection) else duckdb.connect()
        )
        if schema is not None:
            self._schema = (
                convert_schema(schema) if isinstance(schema, dict) else schema
            )
        else:
            self._schema = None

        self._set_filesystem(filesystem=filesystem, **storage_options)
        self._set_basedataset()
        self._get_file_details()

    def _set_filesystem(
        self, filesystem: AbstractFileSystem | None = None, **storage_options
    ):
        if filesystem is None:
            self._filesystem = fsspec_filesystem(
                protocol=self._protocol, **storage_options
            )
        if self._bucket is not None:
            self._dir_filesystem = dirfs.DirFileSystem(
                path=self._bucket, fs=self._filesystem
            )
        else:
            self._dir_filesystem = self._filesystem

        self._pa_filesystem = pafs.PyFileSystem(
            pafs.FSSpecHandler(self._dir_filesystem)
        )

    def _get_file_details(self) -> None:
        self._files = dict()
        self._files["path"] = self._dir_filesystem.glob(
            self._path + f"/**.{self._format.replace('.','')}"
        )
        self._files["name"] = {f: os.path.basename(f) for f in self._files["path"]}
        self._files["size"] = self._dir_filesystem.du(self._path, total=False)

        self._files["last_modified"] = {
            f: self._dir_filesystem.modified(f) for f in self._files["path"]
        }

        def _get_count_rows(frag):
            return frag.path, frag.count_rows()

        count_rows = run_parallel(
            _get_count_rows, self._basedataset.get_fragments(), backend="threading"
        )

        self._files["count_rows"] = dict(zip(count_rows))
        self.files = pd.concat(
            [pd.Series(self._files[k]) for k in ["name", "size", "last_modified"]],
            axis=1,
            keys=["name", "size", "last_modified"],
        )

        self.files.index.names = ["path"]
        self.files = pl.from_pandas(self.files.reset_index())
        self.size = self.files["size"].sum()
        self.size_h = humanize_size(self.size, unit="MB")

    def _set_basedataset(self):
        self._basedataset = pds.dataset(
            self._path,
            format=self._format,
            filesystem=self._dir_filesystem,
            partitioning=self._partitioning,
            schema=self._pa_schema if hasattr(self, "_pa_schema") else self._schema,
        )

    def _get_pa_schemas(self):
        def _get_physical_schema(frag):
            return frag.path, frag.physical_schema

        pa_schemas = run_parallel(
            _get_physical_schema, self._basedataset.get_fragments(), backend="threading"
        )
        return dict(zip(pa_schemas))

    def _get_unified_schema(self):
        # if not hasattr(self, "_pa_schemas"):
        #    self._get_pa_schemas()

        schemas_equal = True
        all_schemas = list(self.pa_schemas.values())
        unified_schema = all_schemas[0]
        for schema in all_schemas[1:]:
            unified_schema, schemas_equal_ = unify_schema(unified_schema, schema)

            schemas_equal *= schemas_equal_

        return unified_schema, schemas_equal_

    @property
    def pa_schemas(self):
        if not hasattr(self, "_pa_schemas"):
            self._pa_schemas = self._get_pa_schemas()

        return self._pa_schemas

    @property
    def pl_schemas(self):
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
        if not hasattr(self, "_pa_schema"):
            self._pa_schema, self._schemas_equal = self._get_unified_schema()

        return self._pa_schema

    @property
    def pl_schema(self):
        if not hasattr(self, "_pl_schema"):
            self._pl_schema = convert_schema(self.pa_schema)
        return self._pl_schema

    @property
    def schema(self):
        return self.pa_schema

    @property
    def schemas_equal(self):
        if not hasattr(self, "_schemas_equal"):
            self._pa_schema, self._schemas_equal = self._get_unified_schema()
        return self._schemas_equal

    @property
    def pa_schema_sorted(self):
        if not hasattr(self, "_pa_schema_sorted"):
            self._pa_schema_sorted = sort_schema(self.pa_schema)
        return self._pa_schema_sorted

    @property
    def pl_schema_sorted(self):
        if not hasattr(self, "_pl_schema_sorted"):
            self._pl_schema_sorted = sort_schema(self.pl_schema)
        return self._pl_schema_sorted

    @property
    def schema_sorted(self):
        return self.pa_schema_sorted

    @staticmethod
    def _read_file(
        path: str,
        schema: pa.Schema | None = None,
        format: str | None = None,
        filesystem: pafs.FileSystem | None = None,
    ) -> pa.Table:
        format = format or os.path.splitext(path)[-1]

        if format == ".parquet":
            table = pq.read_table(path, filesystem=filesystem, schema=schema)

        elif format == ".csv":
            with filesystem.open_input_file(path) as f:
                table = pc.read_csv(f, schema=schema)

        elif format == ".arrow" or format == ".ipc" or format == ".feather":
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
    ):
        table = to_arrow(table)
        format = format or os.path.splitext(path)[-1]
        schema = schema or table.schema

        if format == ".parquet":
            pq.write_table(table, path, filesystem=filesystem, schema=schema)

        elif format == ".csv":
            with filesystem.open_output_stream(path) as f:
                table = pa.table.from_batches(table.to_batches(), schema=schema)
                pc.write_csv(table, f)

        elif format == ".arrow" or format == ".ipc" or format == ".feather":
            with filesystem.open_output_scream(path) as f:
                table = pa.table.from_batches(table.to_batches(), schema=schema)
                pf.write_feather(f, compression="uncompressed")

    def estimate_batch_size(self, file_size: str = "10MB"):
        unit = re.findall(["[k,m,g,t,p]{0,1}b"], file_size.lower())
        val = float(file_size.lower().split(unit)[0].strip())
        batch_size = int(
            val
            / (
                humanize_size(self.files["size"].sum(), unit=unit)
                / self.files["count_rows"].sum()
            )
        )
        return batch_size

    def write(
        self,
        tables: Union[pa.Table, pd.DataFrame, pl.DataFrame, duckdb.DuckDBPyRelation]
        | List[Union[pa.Table, pd.DataFrame, pl.DataFrame, duckdb.DuckDBPyRelation]],
        mode: str = "delta",
        batch_size: int | str = 1_000_000,
        file_size: str | None = None,
        group_by_datetime: Dict[str, str] | None = None,
        row_group_size: int = 100_00,
        sort: str | List[str] | None = None,
        distinct: bool = False,
        subset: str | List[str] | None = None,
        keep: str = "first",
    ):
        if not isinstance(tables, list | tuple):
            tables = [tables]
        if sort is not None:
            tables = run_parallel(sort_table, tables, sort_by=sort, backend="threading")

        if distinct is not None:
            tables = run_parallel(
                distinct_table, subset=subset, keep=keep, backend="threading"
            )

        if file_size is not None:
            batch_size = self.estimate_batch_size(file_size=file_size)

        # get table batches
        batches = run_parallel(
            to_batches, tables, batch_size, group_by_datetime, backend="threading"
        )
        # tables to arrow
        tables = []
        for batches_ in batches:
            tables.extend([to_arrow(batch) for batch in batches_])

        basename_template = f"data-{dt.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}-{random_id()}-{{i}}.{self._format}"

        pds.write_dataset(
            data=tables,
            base_dir=self._path,
            basename_template=basename_template,
            format=self._format,
            partitioning=self._partitioning,
            schema=self.schema,
            filesystem=self._dir_filesystem,
            max_rows_per_group=row_group_size,
            existing_data_behavior="overwrite_or_ignore",
        )

    def select_files(
        start: dt.datetime | str | None = None, end: dt.datetime | str | None = None
    ):
        pass

    def dataset(self):
        pass

    def repair_schemas(self):
        if self.schemas_equal:
            return
        else:
            for path in self.schemas:
                schema = self.schemas[path]
                if schema != self.schema:
                    table = self._read_file(
                        path=path, schema=self.schema, filesystem=self._pa_filesystem
                    )
                    self._write_file(
                        table=table,
                        path=path,
                        schema=self.schema,
                        filesystem=self._pa_filesystem,
                    )


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
