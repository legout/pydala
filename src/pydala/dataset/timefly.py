import datetime as dt
import os
import pprint

import pyarrow as pa
import pyarrow.parquet as pq
from fsspec import spec
from pyarrow.fs import FileSystem

from ..filesystem.base import BaseFileSystem
from ..utils.base import read_toml, write_toml
from ..utils.dataset import get_unified_schema, pyarrow_schema_to_dict
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
        caching: bool = False,
        cache_storage: str | None = "/tmp/pydala/",
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

        self._config_path = os.path.join(path, "_dataset.toml")
        self.read_config()

    def read_config(self) -> None:
        if self.is_initialized:
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

    def get_format(self, sub_path: str = ""):
        if "current" in self._config:
            return self._config["current"]["format"]
        else:
            return self.infer_format(sub_path=sub_path)

    def set_format(self, format: str | None):
        if format is None:
            format = self.get_format()
        self._format = format

    def infer_format(self, sub_path: str = ""):
        all_ext = sorted(
            {
                os.path.splitext(f)[1]
                for f in self._fs.glob(os.path.join(self._path, sub_path, "**"))
            }
        )
        if "" in all_ext:
            all_ext.remove("")
        if ".toml" in all_ext:
            all_ext.remove(".toml")
        if len(all_ext) > 0:
            return all_ext[-1][1:]

    def infer_partitioning(self, sub_path: str = "") -> str | None:
        last_file = self._fs.glob(os.path.join(self._path, sub_path, "**"))[-1]

        if not self._fs.isfile(last_file):
            if "=" in last_file:
                return "hive"
            elif (last_file.split(self._path)[-1].split("/")) > 1:
                return "directory"

    def infer_schema(self, format: str | None = None, sub_path: str = "") -> list:
        if not format:
            format = self.get_format()

        if format:
            return get_unified_schema(path=self._path, filesystem=self._fs)

    def infer_compression(self, sub_path: str = "") -> str | None:
        last_file = self._fs.glob(os.path.join(self._path, sub_path, "**"))[-1]

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

        self._config = {}
        dataset = {
            "name": name,
            "init": self._now(),
            "description": description or "",
            "bucket": self._bucket,
            "path": self._path,
            "protocol": self._protocol,
            "profile": self._profile,
        }

        self._config["dataset"] = dataset
        try:
            self._fs.makedirs(os.path.join(self._path, "current"), exist_ok=True)
            self._fs.makedirs(os.path.join(self._path, "snapshot"), exist_ok=True)
        except PermissionError:
            pass

        if save:
            self.write_config()

    @log_decorator()
    def create_current(
        self,
        format: str | None = None,
        compression: str | None = None,
        partitioning: str | list | None = None,
        sort_by: str | None = None,
        ascending: str | None = None,
        distinct: bool | None = None,
        schema: dict | None = None,
        schema_unique: bool | None = None,
        batch_size: int | str | None = None,
        comment: str = None,
    ):

        if self.datafiles_in_root:
            format = self.get_format()
            self._mv(self._path, os.path.join(self._path, "current"), format=format)

        if self.has_current:
            if not self.current_empty:
                format = format or self.get_format(sub_path="current")
                if not schema:
                    schema, schema_unique_ = self.infer_schema(
                        format=format, sub_path="current"
                    )
                if not schema_unique:
                    schema_unique = schema_unique_

                partitioning = partitioning or self.infer_partitioning(
                    sub_path="current"
                )

                if compression is None:
                    if format == "parquet":
                        compression = self.infer_compression(sub_path="current")

            if schema:
                schema = pyarrow_schema_to_dict(schema)

            current = {
                "created": self._now(),
                "format": format or None,
                "compression": compression or None,
                "partitioning": partitioning or None,
                "sort_by": sort_by or None,
                "ascending": ascending or True,
                "distinct": distinct or False,
                "schema": schema or {},
                "schema_unique": schema_unique or None,
                "batch_size": batch_size or None,
                "comment": comment or "initialized",
                "latest_update": self._now(),
            }

            if "current" in self._config:
                self._config["current"].update(current)
            else:
                self._config["current"] = current

            self.write_config()

    @log_decorator()
    def update_current(self, **kwargs):
        if not self.has_current:
            self.create_current(**kwargs)
        else:
            if self.current_empty:
                self.create_current(**kwargs)
        if not "current" in self._config:
            self.create_current(**kwargs)

        kwargs["latest_update"] = self._now()

        schema = kwargs.pop("schema", None)
        if schema:
            schema = pyarrow_schema_to_dict(schema)
            self._config["current"].update(dict(schema=schema))

        self._config["current"].update(kwargs)
        self.write_config()

    @log_decorator()
    def update(self, snapshot: str | None = None, **kwargs) -> None:
        snapshot = snapshot or "current"
        self._config[snapshot].update(**kwargs)
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
        schema: dict | None = None,
        schema_unique: bool | None = None,
        batch_size: int | str | None = None,
        comment: str | None = None,
    ) -> None:
        if not "snapshot" in self._config:
            self._config["snapshot"] = {}
            self._config["snapshot"]["available"] = []
            self._config["snapshot"]["deleted"] = []

        now = self._now()
        if not self.current_empty:
            if "current" not in self._config:
                self.set_current(
                    format=format,
                    compression=compression,
                    partitioning=partitioning,
                    sort_by=sort_by,
                    ascending=ascending,
                    distinct=distinct,
                    schema=schema,
                    schema_unique=schema_unique,
                    batch_size=batch_size,
                    comment=comment,
                )
            format = self.get_format(sub_path="current")
            snapshot = {
                "creaded": now,
                "format": format,
                "compression": compression or self._config["current"]["compression"],
                "partitioning": partitioning or self._config["current"]["partitioning"],
                "sort_by": sort_by or self._config["current"]["sort_by"],
                "ascending": ascending or self._config["current"]["ascending"],
                "distinct": distinct or self._config["current"]["distinct"],
                "schema": schema or self._config["current"]["schema"],
                "schema_unique": schema_unique
                or self._config["current"]["schema_unique"],
                "batch_size": batch_size or self._config["current"]["batch_size"],
                "comment": comment or "",
            }

            self._config["snapshot"][self._timestamp_to_snapshot(now)] = snapshot
            self._config["snapshot"]["available"].append(
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
            self._config["snapshot"].pop(snapshot)
            self._config["snapshot"]["available"].remove(snapshot)
            self._config["snapshot"]["deleted"].append(snapshot)
            self.write_config()

        else:
            raise FileNotFoundError(
                f"Can not delete snapshot '{snapshot}'. {os.path.join(self._path, 'snapshot', snapshot)} not found."
            )

    @property
    def available_snapshots(self):
        if "snapshot" in self._config:
            return self._config["snapshot"]["available"]

    @property
    def deleted_snapshots(self):
        if "snapshot" in self._config:
            return self._config["snapshot"]["deleted"]

    def _find_snapshot_subpath(self, timefly: dt.datetime | None):

        if timefly is not None:
            available_snapshots = sorted(
                map(
                    self._snapshot_to_timestamp,
                    self._config["snapshot"]["available"],
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
                "created": self._config["snapshot"][snapshot]["created"],
                "format": self._config["snapshot"][snapshot]["format"],
                "compression": self._config["snapshot"][snapshot]["compression"],
                "partitioning": self._config["snapshot"][snapshot]["partitioning"],
                "sort_by": self._config["snapshot"][snapshot]["sort_by"],
                "ascending": self._config["snapshot"][snapshot]["ascending"],
                "distinct": self._config["snapshot"][snapshot]["distinct"],
                "columns": self._config["snapshot"][snapshot]["columns"],
                "batch_size": self._config["snapshot"][snapshot]["batch_size"],
                "comment": f"Restored from snapshot {snapshot}",
            }
            self._config["current"] = current
            self._cp(
                snapshot_path,
                current_path,
                self._config["snapshot"][snapshot]["format"],
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
                # if not self._fs.exists(path2):
                self._fs.makedirs(path2, exist_ok=True)
                self._fs.mv(files, path2, recursive=True)
        else:
            files = self._fs.glob(os.path.join(path1, f"**.{format}"))
            path2 = path2.lstrip("/") + "/"
            # if not self._fs.exists(path2):
            try:
                self._fs.makedirs(path2, exist_ok=True)
                self._fs.mv(files, path2, recursive=True)
            except PermissionError:
                pass

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
                # if not self._fs.exists(path2):
                self._fs.makedirs(path2, exist_ok=True)
                self._fs.cp(files, path2, recursive=True)

        else:
            files = self._fs.glob(os.path.join(path1, f"**.{format}"))
            path2 = path2.lstrip("/") + "/"
            # if not self._fs.exists(path2):
            try:
                self._fs.makedirs(path2, exist_ok=True)
                self._fs.cp(files, path2, recursive=True)
            except PermissionError:
                pass

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
        format = self.get_format(sub_path="current")
        if format:
            return (
                len(self._fs.glob(os.path.join(self._path, f"current/*.{format}"))) == 0
            )
        else:
            return True

    @property
    def snapshot_empty(self):
        format = self.get_format(sub_path="snapshot")
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

    @property
    def config(self):
        pprint.pprint(
            self._config,
            sort_dicts=True,
            indent=2,
            compact=True,
            width=80,
        )
