import datetime as dt
import os

import duckdb
import pyarrow.dataset as ds
import toml

from ..filesystem.filesystem import FileSystem
from .reader import Reader
from .writer import Writer


class ConfigReader:
    ...


class DatasetReader(Reader):
    def __init__(
        self,
        path: str,
        timefly: str | dt.datetime | None = None,
        bucket: str | None = None,
        name: str | None = None,
        partitioning: ds.Partitioning | str | None = None,
        filesystem: FileSystem | None = None,
        format: str | None = "parquet",
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = "__index_level_0__",
        ddb: duckdb.DuckDBPyConnection | None = None,
        caching: bool = False,
        cache_prefix: str | None = "/tmp/pydala/",
    ):

        self._timefly = self._read_timefly(
            filesystem=filesystem, path=path, bucket=bucket
        )

        if self._timefly is not None:
            subpath = self._find_timefly_subpath(timefly=timefly)
            path = os.path.join(path, subpath)
        else:
            self._timefly = False

        super().__init__(
            path=path,
            bucket=bucket,
            name=name,
            partitioning=partitioning,
            filesystem=filesystem,
            format=format,
            sort_by=sort_by,
            ascending=ascending,
            distinct=distinct,
            drop=drop,
            ddb=ddb,
            caching=caching,
            cache_prefix=cache_prefix,
        )

    def _read_timefly(
        self, filesystem: FileSystem | None, path: str, bucket: str | None = None
    ):
        if filesystem is None:
            filesystem = FileSystem(type_="local")

        bucket = bucket or ""

        if filesystem.exists(os.path.join(bucket, path, "timefly.toml")):

            with open(os.path.join(path, "timefly.toml")) as f:
                return toml.load(f)

    def _find_timefly_subpath(self, timefly: str | dt.datetime | None):

        timefly = timefly or "current"

        if timefly != "current":
            if isinstance(timefly, str):
                timefly_timestamp = dt.datetime.strptime(timefly, "%Y%m%d_%H%M%S")
            else:
                timefly_timestamp = timefly

            all_timestamps = [
                dt.datetime.strptime(subpath, "%Y%m%d_%H%M%S")
                for subpath in self._timefly["dataset"]["subpaths"]["all"]
            ]
            timefly_diff = sorted(
                [
                    timefly_timestamp - timestamp
                    for timestamp in all_timestamps
                    if timestamp < timefly_timestamp
                ]
            )[0]
            timefly_subpath = (timefly_timestamp - timefly_diff).strftime(
                "%Y%m%d_%H%M%S"
            )

        else:
            timefly_subpath = "current"

        return os.path.join(self._path, timefly_subpath)


class DatasetWriter(Writer):
    pass
