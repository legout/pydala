import duckdb
import pyarrow as pa
import pyarrow.dataset as pads
from fsspec import spec
from pyarrow.fs import FileSystem

from .base import Dataset
from ..utils.dataset import get_unified_schema, pyarrow_schema_from_dict


class Reader(Dataset):
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        schema: pa.Schema | None = None,
        format: str = "parquet",
        filesystem: spec.AbstractFileSystem | FileSystem | None = None,
        partitioning: pads.Partitioning | list | str | None = None,
        exclude_invalid_files: bool = True,
        ddb: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        super().__init__(
            path,
            bucket,
            schema,
            format,
            filesystem,
            partitioning,
            exclude_invalid_files,
            ddb,
        )

        self._dataset = pads.dataset(
            source=path,
            schema=schema,
            format=format,
            filesystem=self._filesystem,
            partitioning=partitioning,
            exclude_invalid_files=exclude_invalid_files,
        )

        self._files = self._dataset.files
        
        self._is_empty = len(self._files)>0
        self._is_file = len(self._files)==1
       

    def get_unified_schema(self):
        return get_unified_schema(dataset=self._dataset)
    
    def set_schema(self, schema:pa.Schema|None=None):
        
        if schema is None:
            schema, schema_equal = self.get_unified_schema()
            
        self._schema = schema
        
    def _gen_name(self, name: str | None):
        return f"{self._name}_{name}" if self._name else name
    
    
    