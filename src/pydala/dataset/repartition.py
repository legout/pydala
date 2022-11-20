import pyarrow as pa

from .reader import Reader
from .writer import Writer


class Repartition:
    def __init__(
        self,
        reader: Reader,
        writer: Writer,
        caching_method: str = "local",  # or temp_table
        source_table: str = "pa_table",  # or temp_table or dataset
        schema_auto_conversion: bool = True,
    ):
        self._reader = reader
        self._writer = writer

        self._caching_method = caching_method
        self._source_table = source_table
        self._row_group_size = None
        self._batch_size = None
        self._overwrite = False
        self._schema = None
        self._mode = "delta"
        self._schema_auto_conversion = schema_auto_conversion

    def read(self):
        if self._schema_auto_conversion:
            self._reader.set_pyarrow_schema()
            self._schema = self._reader._schema

        if self._reader._path == self._writer._base_path:
            self._writer._mode = "overwrite"

            if self._caching_method == "local":
                self._reader._to_cache()

            elif self._caching_method == "temp_table":
                self._source_table = "temp_table"
                self._reader.create_temp_table()

            else:
                raise ValueError(
                    f"{self._caching_method} is not a valid value for caching_method. "
                    "Must be 'local' or 'pa_table'."
                )

        if self._source_table == "pa_table":
            if not self._reader.has_pa_table:
                self._reader.load_pa_table()

        elif self._source_table == "dataset":
            if not self._reader.has_dataset:
                self._source_table = self._reader.load_dataset()

        elif self._source_table == "temp_table":
            if self._reader.has_temp_table:
                self._reader.create_temp_table()

        else:
            raise ValueError(
                f"{self._source_table} is not a valid value for source_table. "
                "Must be 'local' or 'pa_table'."
            )

        self._source = self._reader.rel

    def _delete_source(self):
        if self._reader.cached:
            self._reader._fs.rm(self._reader._path, recursive=True)

    def sort(self, by: str | list | None, ascending: bool | list | None = None):
        self._writer.sort(by=by, ascending=ascending)

        return self

    def distinct(
        self,
        value: bool | None,
        subset: list | None = None,
        presort_by: list | None = None,
        postsort_by: list | None = None,
    ):
        self._writer.distinct(
            value=value, subset=subset, presort_by=presort_by, postsort_by=postsort_by
        )

        return self

    def drop(self, columns: str | list | None):
        self._writer.drop(columns=columns)

        return self

    def partitioning(
        self, columns: str | list | None = None, flavor: str | None = None
    ):
        self._writer.partitioning(columns=columns, flavor=flavor)

        return self

    def compression(self, value: str | None = None):
        self._writer.compression(value=value)

        return self

    def format(self, value: str | None = None):
        self._writer.format(value=value)

        return self

    def mode(self, value: str | None):
        self._writer.mode(value=value)

        return self

    def batch_size(self, value: int | str | None = None):
        if value is not None:
            self._batch_size = value

        return self

    def row_group_size(self, value: int | None = None):
        if value is not None:
            self._row_group_size = value

        return self

    def schema(self, schema: pa.Schema | None = None):
        if schema is not None:
            self._schema = schema
        return self

    def write(
        self,
        batch_size: str | int | None = None,
        row_group_size: int | None = 250000,
        sort_by: str | None = None,
        ascending: bool | None = None,
        distinct: bool | None = None,
        drop: str | list | None = None,
        partitioning: str | list | None = None,
        partitioning_flavor: str | None = None,
        compression: str | None = None,
        format: str | None = None,
        schema: pa.Schema | None = None,
        mode: str | None = None,
        delete_source: bool = True,
        datetime_column: str | None = None,
    ):
        self.sort(by=sort_by, ascending=ascending)
        self.distinct(value=distinct)
        self.drop(columns=drop)
        self.partitioning(columns=partitioning, flavor=partitioning_flavor)
        self.compression(value=compression)
        self.format(value=format)
        self.schema(schema=schema)
        self.mode(value=mode)
        self.batch_size(value=batch_size)
        self.row_group_size(value=row_group_size)
        self._writer.write_dataset(
            table=self._source,
            batch_size=self._batch_size,
            row_group_size=self._row_group_size,
            datetime_column=datetime_column,
        )
        if delete_source:
            self._delete_source()
