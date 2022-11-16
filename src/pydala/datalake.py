import duckdb
import os
from tempfile import mkdtemp
from .dataset.reader import TimeFlyReader
from .dataset.writer import TimeFlyWriter
from fsspec import spec
from pyarrow.fs import FileSystem
from .filesystem.base import BaseFileSystem
from .utils.base import convert_size_unit


    
    
                

class DataLake(BaseFileSystem):
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        name: str | None = None,
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
        ):
        super().__init__(path=path, bucket=bucket, name=name, ddb=ddb,caching=caching, cache_storage=cache_storage, protocol=protocol, profile=profile,
                         endpoint_url=endpoint_url, storage_options=storage_options, fsspec_fs=fsspec_fs, pyarrow_fs=pyarrow_fs, use_pyarrow_fs=use_pyarrow_fs)

    def read_config(self):
        

    def add_dataset(self, )