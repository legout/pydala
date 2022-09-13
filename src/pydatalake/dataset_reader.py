import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.feather as pf
import pyarrow.parquet as pq
from pathlib import Path
import duckdb


class DatasetReader:
    def __init__(
        self,
        path: str,
        partitioning: list | None = None,
        filesystem: fs.S3FileSystem | None = None,
        format: str | None = None,
    ):
        self._path = path
        self._filesystem = filesystem
        self._format = format
        self._partitioning = partitioning
        self._db = duckdb.connect()

    def _set_dataset(self, name: str = "dataset", **kwargs):

        self._ds = ds.dataset(
            source=self._path,
            format=self._format,
            filesystem=self._filesystem,
            partitioning=self._partitioning,
            **kwargs
        )
        self._db.register(name, self._ds)

    def _load_table(self, name: str = "table_", **kwargs):
        if self._format == "parquet":
            self._table = pq.read_table(
                self._path,
                partitioning=self._partitioning,
                filesystem=self._filesystem,
                **kwargs
            )

        elif self._format == "feather":
            if self._filesystem is not None:
                if self._filesystem.get_file_info(self._path).is_file:
                    with self._filesystem.open_input_file(self._path) as f:
                        self._table = pf.read_feather(f, **kwargs)
                else:
                    if not hasattr(self, "_ds"):
                        self._set_dataset()

                    self._table = self._ds.to_table(**kwargs)

            else:
                if Path(self._path).is_file():
                    self._table = pf.read_feather(self._path, **kwargs)
                else:
                    if not hasattr(self, "_ds"):
                        self._set_dataset()

                    self._table = self._ds.to_table(**kwargs)

        self._db.register(name, self._table)

    @property
    def dataset(self, **kwargs):
        if not hasattr(self, "_ds") or len(kwargs) > 0:
            self._set_dataset(**kwargs)

        return self._ds

    @property
    def table(self, **kwargs):
        if not hasattr(self, "_table") or len(kwargs) > 0:
            self._load_table(**kwargs)

        return self._table
