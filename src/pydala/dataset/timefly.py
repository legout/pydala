import datetime as dt
import os

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import rtoml
from fsspec import spec
from pyarrow.fs import FileSystem

from .helper import NestedDictReplacer, get_filesystem, get_storage_path_options
from .reader import Reader as DatasetReader
from .writer import Writer


class TimeFly:
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        fsspec_fs: spec.AbstractFileSystem | None = None,
        protocol: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        storage_options: dict = {},
    ):
        self._bucket, self._path, self._protocol = get_storage_path_options(
            bucket=bucket, path=path, protocol=protocol
        )
        self._config_path = os.path.join(path, "_timefly.toml")
        self._profile = profile
        self._endpoint_url = endpoint_url
        self._storage_options = storage_options
        self._fs = get_filesystem(
            bucket=self._bucket,
            protocol=self._protocol,
            profile=self._profile,
            endpoint_url=self._endpoint_url,
            storage_options=self._storage_options,
            caching=False,
            cache_bucket=None,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=None,
            use_pyarrow_fs=False,
        )["fsspec_main"]

        self.read()

    def read(self) -> None:
        if self._fs.exists(self._config_path):
            with self._fs.open(self._config_path, "r") as f:
                self.config = NestedDictReplacer(rtoml.load(f)).replace("None", None)
        else:
            self.new()

    def write(self, pretty: bool = False) -> None:
        with self._fs.open(self._config_path, "w") as f:

            rtoml.dump(
                NestedDictReplacer(self.config).replace(None, "None"),
                f,
                pretty=pretty,
            )

    @staticmethod
    def _now() -> tuple[dt.datetime, str]:
        now = dt.datetime.utcnow().replace(microsecond=0)
        return now, now.strftime("%Y%m%d_%H%M%S")

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
            "init": self._now()[0],
            "description": description or "",
            "bucket": self._bucket or None,
            "path": self._path or None,
        }

        self.config["dataset"] = dataset

        if save:
            self.write()

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
            "created": self._now()[0],
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
        self.write()

    def update(self, snapshot: str | None = None, **kwargs) -> None:
        snapshot = snapshot or "current"
        self.config[snapshot].update(**kwargs)
        self.write()

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
            "creaded": now[0],
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

        self.config["snapshot"][now[1]] = snapshot
        self.config["snapshot"]["available"].append(now[1])

        self._cp(
            os.path.join(self._path, "current"),
            os.path.join(self._path, "snapshot", now[1]),
            format=format or self.config["current"]["format"],
        )
        self.write()

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

    def load_snapshot(self, snapshot: str):
        pass

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
                    exclude="_timefly.toml",
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
                    exclude="_timefly.toml",
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


# class Reader(DatasetReader):
#     def __init__(
#         self,
#         snapshot: str,
#         path: str,
#         bucket: str | None = None,
#         name: str | None = None,
#         partitioning: ds.Partitioning | list | str | None = None,
#         format: str | None = "parquet",
#         sort_by: str | list | None = None,
#         ascending: bool | list | None = None,
#         distinct: bool | None = None,
#         drop: str | list | None = "__index_level_0__",
#         ddb: duckdb.DuckDBPyConnection | None = None,
#         caching: bool = False,
#         cache_storage: str | None = "/tmp/pydala/",
#         protocol: str | None = None,
#         profile: str | None = None,
#         endpoint_url: str | None = None,
#         storage_options: dict = {},
#         fsspec_fs: spec.AbstractFileSystem | None = None,
#         pyarrow_fs: FileSystem | None = None,
#         use_pyarrow_fs: bool = False,
#     ):

#         # self.config = ConfigReader(filesystem=filesystem, path=path, bucket=bucket)

#         if self.config is not None:
#             subpath = self._find_timefly_subpath(timefly=timefly)
#             path = os.path.join(path, subpath)
#         else:
#             self.config = False

#         super().__init__(
#             path=path,
#             bucket=bucket,
#             name=name,
#             partitioning=partitioning,
#             format=format,
#             sort_by=sort_by,
#             ascending=ascending,
#             distinct=distinct,
#             drop=drop,
#             ddb=ddb,
#             caching=caching,
#             cache_storage=cache_storage,
#             protocol=protocol,
#             profile=profile,
#             endpoint_url=endpoint_url,
#             storage_options=storage_options,
#             fsspec_fs=fsspec_fs,
#             pyarrow_fs=pyarrow_fs,
#             use_pyarrow_fs=use_pyarrow_fs,
#         )

#     def _find_snapshot_subpath(self, timefly: str | dt.datetime | None):

#         timefly = timefly or "current"

#         if timefly != "current":
#             if isinstance(timefly, str):
#                 timefly_timestamp = dt.datetime.strptime(timefly, "%Y%m%d_%H%M%S")
#             else:
#                 timefly_timestamp = timefly

#             all_timestamps = [
#                 dt.datetime.strptime(subpath, "%Y%m%d_%H%M%S")
#                 for subpath in self._"dataset"]["subpaths"]["all"]
#             ]
#             timefly_diff = sorted(
#                 [
#                     timefly_timestamp - timestamp
#                     for timestamp in all_timestamps
#                     if timestamp < timefly_timestamp
#                 ]
#             )[0]
#             timefly_subpath = (timefly_timestamp - timefly_diff).strftime(
#                 "%Y%m%d_%H%M%S"
#             )

#         else:
#             timefly_subpath = "current"

#         return os.path.join(self._path, timefly_subpath)


# class DatasetWriter(Writer):
#     pass
