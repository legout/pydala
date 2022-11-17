import datetime as dt
import os

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import rtoml
from fsspec import spec
from pyarrow.fs import FileSystem

from ..filesystem.base import BaseFileSystem
from ..utils.base import read_toml, write_toml


class TimeFly(BaseFileSystem):
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
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
            name=None,
            caching=False,
            cache_storage=None,
            protocol=protocol,
            profile=profile,
            endpoint_url=endpoint_url,
            storage_options=storage_options,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
            use_pyarrow_fs=use_pyarrow_fs,
        )

        self._config_path = os.path.join(path, "_dataset.toml")
        self.read_config()

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

    @staticmethod
    def _now() -> tuple[dt.datetime, str]:
        now = dt.datetime.utcnow().replace(microsecond=0)
        return now

    @staticmethod
    def _snapshot_to_timestamp(snapshot: str) -> dt.datetime:
        return dt.datetime.strptime(snapshot, "%Y%m%d_%H%M%S")

    @staticmethod
    def _timestamp_to_snapshot(ts: dt.datetime) -> str:
        return ts.strftime("%Y%m%d_%H%M%S")

    def infer_format(self):
        return self._fs.glob(os.path.join(self._path, "**"))[-1].split(".")[-1]

    def infer_partitioning(self) -> str | None:
        last_file = self._fs.glob(os.path.join(self._path, "**"))[-1]

        if not self._fs.isfile(last_file):
            if "=" in last_file:
                return "hive"

    def infer_columns(self, format: str | None = None) -> list:
        return ds.dataset(
            self._fs.glob(os.path.join(self._path, f"**{format}")),
            format=format,
            filesystem=self._fs,
        ).schema.names

    def infer_compression(self) -> str | None:
        last_file = self._fs.glob(os.path.join(self._path, "**"))[-1]

        with self._fs.open(last_file) as f:
            compression = pq.ParquetFile(f).metadata.row_group(0).column(0).compression

        if compression is not None:
            return compression.lower()

    def new(
        self, name: str | None = None, description: str | None = None, save: bool = True
    ) -> None:

        if name is None:
            name = os.path.basename(self._path)

        self.config = {}
        dataset = {
            "name": name,
            "init": self._now(),
            "description": description or "",
            "bucket": self._bucket,
            "path": self._path,
            "protocol": self._protocol,
            "profile":self._profile
        }

        self.config["dataset"] = dataset

        if save:
            self.write_config()

    def init(
        self,
        name: str | None = None,
        description: str | None = None,
        format: str | None = None,
        compression: str | None = None,
        partitioning: str | list | None = None,
        sort_by: str | None = None,
        ascending: str | None = None,
        distinct: bool | None = None,
        columns: list | None = None,
        batch_size: int | str | None = None,
    ) -> None:
        self.new(name=name, description=description, save=False)

        format = format or self.infer_format()
        columns = columns or self.infer_columns(format=format)
        partitioning = partitioning or self.infer_partitioning()

        if compression is None:
            if format == "parquet":
                compression = self.infer_compression()

        current = {
            "created": self._now(),
            "format": format or None,
            "compression": compression or None,
            "partitioning": partitioning or None,
            "sort_by": sort_by or None,
            "ascending": ascending or True,
            "distinct": distinct or False,
            "columns": columns or [],
            "batch_size": batch_size or None,
            "comment": "initialized",
        }

        # move files to current
        self._mv(self._path, os.path.join(self._path, "current"), format=format)

        self.config["current"] = current
        self.write_config()

    def update(self, snapshot: str | None = None, **kwargs) -> None:
        snapshot = snapshot or "current"
        self.config[snapshot].update(**kwargs)
        self.write_config()

    def add_snapshot(
        self,
        format: str | None = None,
        compression: str | None = None,
        partitioning: str | list | None = None,
        sort_by: str | None = None,
        ascending: str | None = None,
        distinct: bool | None = None,
        columns: list | None = None,
        batch_size: int | str | None = None,
        comment: str | None = None,
    ) -> None:
        if not "snapshot" in self.config:
            self.config["snapshot"] = {}
            self.config["snapshot"]["available"] = []
            self.config["snapshot"]["deleted"] = []

        now = self._now()

        snapshot = {
            "creaded": now,
            "format": format or self.config["current"]["format"],
            "compression": compression or self.config["current"]["compression"],
            "partitioning": partitioning or self.config["current"]["partitioning"],
            "sort_by": sort_by or self.config["current"]["sort_by"],
            "ascending": ascending or self.config["current"]["ascending"],
            "distinct": distinct or self.config["current"]["distinct"],
            "columns": columns or self.config["current"]["columns"],
            "batch_size": batch_size or self.config["current"]["batch_size"],
            "comment": comment or "",
        }

        self.config["snapshot"][self._timestamp_to_snapshot(now)] = snapshot
        self.config["snapshot"]["available"].append(self._timestamp_to_snapshot(now))

        self._cp(
            os.path.join(self._path, "current"),
            os.path.join(self._path, "snapshot", self._timestamp_to_snapshot(now)),
            format=format or self.config["current"]["format"],
        )
        self.write_config()

    def delete_snapshot(self, snapshot: str) -> None:
        path = os.path.join(self._path, "snapshot", snapshot)
        self._rm(path=path)
        self.config["snapshot"].pop(snapshot)
        self.config["snapshot"]["available"].remove(snapshot)
        self.config["snapshot"]["deleted"].append(snapshot)

    @property
    def available_snapshots(self):
        if "snapshot" in self.config:
            return self.config["snapshot"]["available"]

    @property
    def deleted_snapshots(self):
        if "snapshot" in self.config:
            return self.config["snapshot"]["deleted"]

    def _find_snapshot_subpath(self, timefly: dt.datetime | None):

        if timefly is not None:
            available_snapshots = sorted(
                map(
                    self._snapshot_to_timestamp,
                    self.config["snapshot"]["available"],
                )
            )
            snapshot_subpath = self._timestamp_to_snapshot(
                [snapshot for snapshot in available_snapshots if snapshot > timefly][0]
            )

        else:
            snapshot_subpath = "current"

        return snapshot_subpath

    def load_snapshot(self, snapshot: str | dt.datetime):
        snapshot = self._find_snapshot_subpath(snapshot)

        current = {
            "created": self.config["snapshot"][snapshot]["created"],
            "format": self.config["snapshot"][snapshot]["format"],
            "compression": self.config["snapshot"][snapshot]["compression"],
            "partitioning": self.config["snapshot"][snapshot]["partitioning"],
            "sort_by": self.config["snapshot"][snapshot]["sort_by"],
            "ascending": self.config["snapshot"][snapshot]["ascending"],
            "distinct": self.config["snapshot"][snapshot]["distinct"],
            "columns": self.config["snapshot"][snapshot]["columns"],
            "batch_size": self.config["snapshot"][snapshot]["batch_size"],
            "comment": f"Restored from snapshot {snapshot}",
        }
        self.config["current"] = current
        self._cp(
            os.path.join(self._path, "snapshot", snapshot),
            os.path.join(self._path, "current"),
            format=format or self.config["snapshot"][snapshot]["format"],
        )
        self.write_config()

    def _mv(self, path1: str, path2: str, format: str) -> None:
        if (
            hasattr(self._fs, "has_s5cmd")
            and self._fs.has_s5cmd
            and self._protocol != "file"
        ):
            try:
                self._fs.s5mv(
                    "s3://" + os.path.join(self._bucket or "", path1),
                    "s3://" + os.path.join(self._bucket or "", path2),
                    recursive=True,
                    exclude="_dataset.toml",
                )
            except:
                files = self._fs.glob(os.path.join(path1, f"**.{format}"))
                path2 = path2.lstrip("/") + "/"
                if not self._fs.exists(path2):
                    self._fs.mkdir(path2)
                self._fs.mv(files, path2, recursive=True)
        else:
            files = self._fs.glob(os.path.join(path1, f"**.{format}"))
            path2 = path2.lstrip("/") + "/"
            if not self._fs.exists(path2):
                self._fs.mkdir(path2)
            self._fs.mv(files, path2, recursive=True)

    def _cp(self, path1: str, path2: str, format: str) -> None:
        if (
            hasattr(self._fs, "has_s5cmd")
            and self._fs.has_s5cmd
            and self._protocol != "file"
        ):
            try:
                self._fs.s5cp(
                    "s3://" + os.path.join(self._bucket or "", path1),
                    "s3://" + os.path.join(self._bucket or "", path2),
                    recursive=True,
                    exclude="_dataset.toml",
                )
            except:
                files = self._fs.glob(os.path.join(path1, f"**.{format}"))
                path2 = path2.lstrip("/") + "/"
                if not self._fs.exists(path2):
                    self._fs.mkdir(path2)
                self._fs.cp(files, path2, recursive=True)

        else:
            files = self._fs.glob(os.path.join(path1, f"**.{format}"))
            path2 = path2.lstrip("/") + "/"
            if not self._fs.exists(path2):
                self._fs.mkdir(path2)
            self._fs.cp(files, path2, recursive=True)

    def _rm(self, path: str) -> None:
        self._fs.rm(path, recursive=True)

    @property
    def is_initialized(self):
        return self._fs.exists(self._config_path)
