import datetime as dt
import os

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import rtoml
from fsspec import spec
from pyarrow.fs import FileSystem

from ..filesystem.base import BaseFileSystem
from ..utils.base import read_toml, write_toml
from ..utils.logging import log_decorator


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
        log_file: str | None = None,
        log_sub_dir: str | None = None,
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
            log_file=log_file,
            log_sub_dir=log_sub_dir,
        )

        self._config_path = os.path.join(path, "_dataset.toml")
        self.read_config()

    def read_config(self) -> None:
        if self.is_initialized:
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

    def get_format(self):
        if "current" in self.config:
            return self.config["current"]["format"]
        else:
            return self.infer_format()

    def set_format(self, format:str|None):
        if fomat is None:
            format = self.get_format()
        self._format = format

    def infer_format(self):
        all_ext = sorted(
            {
                os.path.splitext(f)[1]
                for f in self._fs.glob(os.path.join(self._path, "**"))
            }
        )
        if "" in all_ext:
            all_ext.remove("")
        if ".toml" in all_ext:
            all_ext.remove(".toml")
        if len(all_ext) > 0:
            return all_ext[-1][1:]

    def infer_partitioning(self) -> str | None:
        last_file = self._fs.glob(os.path.join(self._path, "**"))[-1]

        if not self._fs.isfile(last_file):
            if "=" in last_file:
                return "hive"
            elif (last_file.split(self._path)[-1].split("/")) > 1:
                return "directory"

    def infer_columns(self, format: str | None = None) -> list:
        if not format:
            format = self.get_format()

        if format:
            columns = ds.dataset(
                self._fs.glob(os.path.join(self._path, f"**{format}")),
                format=format,
                filesystem=self._fs,
            ).schema.names
            return columns

    def infer_compression(self) -> str | None:
        last_file = self._fs.glob(os.path.join(self._path, "**"))[-1]

        with self._fs.open(last_file) as f:
            compression = pq.ParquetFile(f).metadata.row_group(0).column(0).compression

        if compression is not None:
            return compression.lower()

    @log_decorator()
    def new(
        self, name: str | None = None, description: str | None = None, save: bool = True
    ) -> None:

        if name is None:
            name = self._path.replace("/", ".")

        self.config = {}
        dataset = {
            "name": name,
            "init": self._now(),
            "description": description or "",
            "bucket": self._bucket,
            "path": self._path,
            "protocol": self._protocol,
            "profile": self._profile,
        }

        self.config["dataset"] = dataset
        self._fs.mkdir(os.path.join(self._path, "current"))
        self._fs.mkdir(os.path.join(self._path, "snapshot"))

        if save:
            self.write_config()

    @log_decorator()
    def create(
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
        if not self.is_initialized:
            self.new(name=name, description=description, save=False)

        if self.datafiles_in_root:
            format = format or self.get_format()
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

    @log_decorator()
    def update(self, snapshot: str | None = None, **kwargs) -> None:
        snapshot = snapshot or "current"
        self.config[snapshot].update(**kwargs)
        self.write_config()

    @log_decorator()
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
        if not self.current_empty:
            format = self.get_format()
            snapshot = {
                "creaded": now,
                "format": format,
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
            self.config["snapshot"]["available"].append(
                self._timestamp_to_snapshot(now)
            )

            self._cp(
                os.path.join(self._path, "current"),
                os.path.join(self._path, "snapshot", self._timestamp_to_snapshot(now)),
                format=format,
            )
            self.write_config()

        else:
            raise FileNotFoundError(
                f"Can not add snapshot '{snapshot}'. No files found in {os.path.join(self._path, 'current')}."
            )

    @log_decorator()
    def delete_snapshot(self, snapshot: str) -> None:

        path = os.path.join(self._path, "snapshot", snapshot)
        if self._fs.exists(path):
            self._rm(path=path)
            self.config["snapshot"].pop(snapshot)
            self.config["snapshot"]["available"].remove(snapshot)
            self.config["snapshot"]["deleted"].append(snapshot)
            self.write_config()

        else:
            raise FileNotFoundError(
                f"Can not delete snapshot '{snapshot}'. {os.path.join(self._path, 'snapshot', snapshot)} not found."
            )

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

    @log_decorator()
    def load_snapshot(self, snapshot: str | dt.datetime, match: str = "exact"):
        if match != "exact":
            snapshot = self._find_snapshot_subpath(snapshot)

        snapshot_path = os.path.join(self._path, "snapshot", snapshot)
        current_path = os.path.join(self._path, "current")

        if self._fs.exists(snapshot_path):
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
                snapshot_path,
                current_path,
                format=format or self.config["snapshot"][snapshot]["format"],
            )
            self.write_config()
        else:
            raise FileNotFoundError(
                f"Can not load snapshot '{snapshot}'. {os.path.join(self._path, 'snapshot', snapshot)} not found."
            )

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

    @property
    def has_snapshot(self):
        return self._fs.exists(os.path.join(self._path, "snapshot"))

    @property
    def has_current(self):
        return self._fs.exists(os.path.join(self._path, "current"))

    @property
    def current_empty(self):
        format = self.get_format()
        if format:
            return (
                len(self._fs.glob(os.path.join(self._path, f"current/*.{format}")))
                == 0
            )
        else:
            return True

    @property
    def snapshot_empty(self):
        format = self.get_format()
        if format:
            return (
                len(self._fs.glob(os.path.join(self._path, f"snapshot/**.{format}")))
                == 0
            )
        else:
            return True

    @property
    def datafiles_in_root(self):
        all_ext = sorted(
            {
                os.path.splitext(f)[1]
                for f in self._fs.glob(os.path.join(self._path, "*"))
            }
        )
        datafile_ext = [
            ".parquet",
            ".feather",
            ".arrow",
            ".ipc",
            ".csv",
            ".txt",
            ".tsv",
            ".parq",
        ]

        return any([ext in datafile_ext for ext in all_ext])
