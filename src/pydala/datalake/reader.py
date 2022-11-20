from ..dataset.reader import TimeFlyReader
from ..filesystem.base import BaseFileSystem
import duckdb
from fsspec import spec
from pyarrow.fs import FileSystem
import os
from .manager import Manager

class Reader(BaseFileSystem):

    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        name: str | None = None,
        ddb: duckdb.DuckDBPyConnection | None = None,
        ddb_memory_limit: str = "-1",
        caching: bool = False,
        cache_storage: str | None = "/tmp/pydala/",
        protocol: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        storage_options: dict = {},
        fsspec_fs: spec.AbstractFileSystem | None = None,
        pyarrow_fs: FileSystem | None = None,
        use_pyarrow_fs: bool = False,
        log_file: str | None = None,
        log_sub_dir: str | None = None,
    ):
        super().__init__(
            path=path,
            bucket=bucket,
            name=name,
            caching=caching,
            cache_storage=cache_storage,
            protocol=protocol,
            profile=profile,
            endpoint_url=endpoint_url,
            storage_options=storage_options,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
            use_pyarrow_fs=use_pyarrow_fs,
            log_file=log_file,
            log_sub_dir=log_sub_dir,
        )
        if use_pyarrow_fs:
            self.manager = Manager(path=path, bucket=bucket, pyarrow_fs=self._fs, use_pyarrow_fs=True)
        else:
            self.manager = Manager(path=path, bucket=bucket, fsspec_fs=self._fs)

            
        if ddb:  # is not None:
            self.ddb = ddb
        else:
            self.ddb = duckdb.connect()
        self.ddb.execute(
            f"SET temp_directory='{os.path.join(cache_storage, 'duckdb')}'"
        )
        self._ddb_memory_limit = ddb_memory_limit
        self.ddb.execute(f"SET memory_limit='{self._ddb_memory_limit}'")

    def _load_dataset_configs(self, paths:list|None=None):
        self.manager.load(paths=paths)

    def load(self, patsh:list|None=None):
        self._load_dataset_configs(paths=paths)
        self.reader = {}
        for dataset in self.manager.datasets:
            dataset_path = self.manager[dataset].config["dataset"]["path"]
            dataset_name = self.manager[dataset].config["dataset"]["name"]
            self.reader[name] = TimeFlyReader(base_path=dataset_path, fsspec_fs=self._fs, pyarrow_fs=self._pafs, use_pyarrow_fs=self._use_pyarrow_fs)
            #self.