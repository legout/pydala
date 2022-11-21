import os
from tempfile import mkdtemp

import duckdb
from fsspec import spec
from pyarrow.fs import FileSystem

from ..dataset.reader import TimeFlyReader
from ..dataset.timefly import TimeFly
from ..dataset.writer import TimeFlyWriter
from ..filesystem.base import BaseFileSystem
from ..utils.base import read_toml, write_toml
from ..utils.logging import log_decorator


class Manager(BaseFileSystem):
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        name: str | None = None,
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
            caching=False,
            cache_storage=None,
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
        self._config_path = os.path.join(path, "_pydala.toml")
        self.datasets = {}

        self.read_config()

    def read_config(self) -> None:
        if self._fs.exists(self._config_path):
            self.config = read_toml(path=self._config_path, filesystem=self._fs)
        else:
            self.new()

    def write_config(self, pretty: bool = False) -> None:
        write_toml(
            config=self.config,
            path=self._config_path,
            filesystem=self._fs,
            pretty=pretty,
        )

    @log_decorator()
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

    @log_decorator()
    def load(self, paths: list | None = None):
        if not paths:
            paths: list = [
                os.path.dirname(path)
                for path in self._fs.glob(os.path.join(self._path, "**_dataset.toml"))
            ]

        for path in paths:
            if self._use_pyarrow_fs:
                tf = TimeFly(
                    path=path,
                    fsspec_fs=self._fs,
                    pyarrow_fs=self._pafs,
                    use_pyarrow_fs=True,
                )
            else:
                tf = TimeFly(path=path, fsspec_fs=self._fs)

            if "name" in tf.config["dataset"]:
                name = tf.config["dataset"]["name"]
            else:
                name = path.replace("/", ".")

            self.datasets[name] = tf

    @log_decorator()
    def create(
        self,
        name: str | None = None,
        description: str | None = None,
        paths: list | None = None,
        clean: bool = False,
    ):
        if not self.is_initialized:
            self.new(name=name, description=description, save=False)

        if not paths:
            paths: list = [
                os.path.dirname(path)
                for path in self._fs.glob(os.path.join(self._path, "**_dataset.toml"))
            ]

        for path in paths:
            self.add_dataset(path, clean=clean)

    @log_decorator()
    def add_dataset(self, path: str, clean: bool = False, **kwargs):
        name = path.replace("/", ".")
        # self.datasets[name] = {}

        self.datasets[name] = TimeFly(
            path=path,
            fsspec_fs=self._fs,
            pyarrow_fs=self._pafs,
            use_pyarrow_fs=self._use_pyarrow_fs,
        )

        if self._fs.exists(path):

            if self.datasets[name].datafiles_in_root:
                if not "name" in kwargs:
                    kwargs["name"] = name
                if not "description" in kwargs:
                    kwargs[
                        "description"
                    ] = f"PyDala {self._name} -> Dataset {kwargs['name']}"

                self.datasets[name].create(**kwargs)

                name = kwargs["name"]
                description = kwargs["description"]

            else:
                name = self.datasets[name].config["dataset"]["name"]
                description = self.datasets[name].config["dataset"]["description"]
                self.datasets[name].config["dataset"]["bucket"] = self._bucket
                self.datasets[name].config["dataset"]["protocol"] = self._protocol
                self.datasets[name].config["dataset"]["profile"] = self._profile

            if clean:
                for snapshot in self.datasets[name].available_snapshots:
                    self.datasets[name].delete_snapshot(snapshot=snapshot)

            self.datasets[name].write_config()

            dataset = {}  # create_nested_dict(name, {}, sep=".")
            dataset["name"] = name
            dataset["path"] = path
            dataset["description"] = description
            self.config["dataset"][name.replace(".", "-")] = dataset
            self.write_config()

    @log_decorator()
    def remove_dataset(self, name: str, clean: bool = False):
        name_ = name.replace(".", "-")
        if clean:
            self._fs.rm(self.datasets[name_].config["dataset"]["path"], recursive=True)
        self.config["dataset"].pop(name_)
        self.datasets.pop(name_)

        self.write_config()

    @property
    def tables(self):
        return [
            self.config["dataset"][k]["name"] for k in self.config["dataset"].keys()
        ]

    @property
    def name(self):
        return self._name

    @property
    def is_initialized(self):
        return self._fs.exists(self._config_path)

    # TODO:
    # Add TimeFly Instance for every dataset
    # Add TimeFlyReader Instance for every dataset
    # Add TimeFlyWriter Instance for every dataset
