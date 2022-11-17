import os
from tempfile import mkdtemp

import duckdb
from fsspec import spec
from pyarrow.fs import FileSystem

from .dataset.reader import TimeFlyReader
from .dataset.timefly import TimeFly
from .dataset.writer import TimeFlyWriter
from .filesystem.base import BaseFileSystem
from .utils.base import read_toml, write_toml


class Manager(BaseFileSystem):
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
        )
        self._config_path = os.path.join(path, "_pydala.toml")
        self.read_config()
        if ddb:  # is not None:
            self.ddb = ddb
        else:
            self.ddb = duckdb.connect()
        self.ddb.execute(
            f"SET temp_directory='{os.path.join(cache_storage, 'duckdb')}'"
        )
        self._ddb_memory_limit = ddb_memory_limit
        self.ddb.execute(f"SET memory_limit='{self._ddb_memory_limit}'")

    def read_config(self) -> None:
        if self._fs.exists(self._config_path):
            self._config = read_toml(path=self._config_path, filesystem=self._fs)
        else:
            self.new()

    def write_config(self, pretty: bool = False) -> None:
        write_toml(
            config=self._config,
            path=self._config_path,
            filesystem=self._fs,
            pretty=pretty,
        )

    def new(
        self, name: str | None = None, description: str | None = None, save: bool = True
    ) -> None:

        self.config = {}
        pydala = {
            "name": name or self._name,
            "init": TimeFly._now(),
            "description": description or "",
            "bucket": self._bucket,
            "path": self._path,
            "protocol": self._protocol,
            "profile": self._profile,
            "ddb_memory_limit": self._ddb_memory_limit,
            "cache_storage": self._cache_storage,
        }

        self.config["pydala"] = pydala
        self.config["dataset"] = {}

        if save:
            self.write_config()

    def init(
        self,
        name: str | None = None,
        description: str | None = None,
        paths: list | None = None,
        clean: bool = False,
    ):
        self.new(name=name, description=description, save=False)
        if not paths:
            paths = self._fs.glob(os.path.join(self._path, "**_dataset.toml"))

        for path in paths:
            self.add_dataset(path, clean=clean)

    def add_dataset(self, path: str, clean: bool = False, **kwargs):
        if self._fs.exists(path):
            if self._use_pyarrow_fs:
                tf = TimeFly(
                    path=path,
                    pyarrow_fs=self._fs,
                    use_pyarrow_fs=self._use_pyarrow_fs,
                )
            else:
                tf = TimeFly(path=path, fsspec_fs=self._fs)

            if not tf.is_initialized:
                if not "name" in kwargs:
                    kwargs["name"] = "-".join(path.split("/"))
                if not "description" in kwargs:
                    kwargs[
                        "description"
                    ] = f"PyDala {self._name} -> Dataset {kwargs['name']}"

                tf.init(**kwargs)

                name = kwargs["name"]
                description = kwargs["description"]

            else:
                name = tf.config["dataset"]["name"]
                description = tf.config["dataset"]["description"]
                tf.config["dataset"]["bucket"] = self._bucket
                tf.config["dataset"]["protocol"] = self._protocol
                tf.config["dataset"]["profile"] = self._profile

            if clean:
                for snapshot in tf.available_snapshots:
                    tf.delete_snapshot(snapshot=snapshot)

            tf.write_config()

            self._tables[name] = path

            self.config["dataset"][name] = {}
            self.config["dataset"][name]["name"] = name
            self.config["dataset"][name]["path"] = path
            self.config["dataset"][name]["description"] = description
            self.write_config()
        else:
            raise OSError(f"{path} not found.")

    def remove_dataset(self, name: str, clean: bool = False):
        if clean:
            self._fs.rm(self.config["dataset"][name]["path"], recursive=True)
        self.config["dataset"].pop(name)
        self.write_config()

    def repartition(self, name:str, snapshot:str|None=None, add_snapshot:bool=False):
        reader = TimeFlyReader(path=self.config["dataset"][name], )

    def add_snapshot(self, name):
        pass

    def load_snapshot(self, name):
        pass

    # TODO:
    # Add TimeFly Instance for every dataset
    # Add TimeFlyReader Instance for every dataset
    # Add TimeFlyWriter Instance for every dataset