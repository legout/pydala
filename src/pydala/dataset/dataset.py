import os

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as pds
from fsspec import filesystem as fsspec_filesystem
from fsspec.implementations import dirfs
from fsspec.spec import AbstractFileSystem
from fsspec.utils import infer_storage_options
from ..utils.base import run_parallel

from ..utils.dataset import unify_schema, sort_schema, convert_schema


class BaseDataset:
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        schema: pa.Schema | None = None,
        format: str = "parquet",
        filesystem: AbstractFileSystem | None = None,
        partitioning: pds.Partitioning | list | str | None = None,
        exclude_invalid_files: bool = True,
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
        self._exclude_invalid_files = exclude_invalid_files
        self._schema = schema
        self._ddb = (
            ddb if isinstance(ddb, duckdb.DuckDBPyConnection) else duckdb.connect()
        )

        if filesystem is None:
            self._filesystem = fsspec_filesystem(
                protocol=self._protocol, **storage_options
            )
            if bucket is not None:
                self._dir_filesystem = dirfs.DirFileSystem(
                    path=bucket, fs=self._filesystem
                )
            else:
                self._dir_filesystem = self._filesystem

        self._files = dict()
        self._files["path"] = self._dir_filesystem.glob(
            self._path + f"/**.{format.replace('.','')}"
        )
        self._files["name"] = {f: os.path.basename(f) for f in self._files["path"]}
        self._files["size"] = self._dir_filesystem.du(self._path, total=False)
        self._files["last_modified"] = {
            f: self._dir_filesystem.modified(f) for f in self._files["path"]
        }
        self.files = pd.concat(
            [pd.Series(self._files[k]) for k in ["name", "size", "last_modified"]],
            axis=1,
            keys=["name", "size", "last_modified"],
        )
        self.files.index.names = ["path"]
        self.files = pl.from_pandas(self.files.reset_index())
        self.size = self.files["size"].sum()


class ParquetDataset(BaseDataset):
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        schema: pa.Schema | None = None,
        format: str = "parquet",
        filesystem: AbstractFileSystem | None = None,
        partitioning: pds.Partitioning | list | str | None = None,
        exclude_invalid_files: bool = True,
        ddb: duckdb.DuckDBPyConnection | None = None,
        **storage_options,
    ):
        super().__init__(
            path=path,
            bucket=bucket,
            schema=schema,
            format=format,
            filesystem=filesystem,
            partitioning=partitioning,
            exclude_invalid_files=exclude_invalid_files,
            ddb=ddb,
            **storage_options,
        )

    def _get_pa_schemas(self):
        def _read_schema(f, fs):
            return f, pq.read_schema(f, filesystem=fs)

        pa_schemas = run_parallel(
            _read_schema, self._files["path"], self._dir_filesystem
        )
        return {f: schema for f, schema in pa_schemas}

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


# TODO:
# - remove empty files for self.files
# - repair schema
#   1. get unified schema
#   2. find schemas that are not equal to the unified schema
#   3. repair schema for files with not equal schema
#       a. write file with unified schema into new file name
#       b. remove files with not equal schema
# - use run_parallel in _get_pyarrow_schemas
# - use run_parallel in init for file infos
