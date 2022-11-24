import pyarrow as pa

from .reader import Reader
from .writer import Writer


class Repartition:
    def __init__(
        self,
        reader: Reader,
        writer: Writer,
        caching_method: str = "local",  # or temp_table or table_
        source_table: str = "pa_table",  # or temp_table or dataset
        schema_auto_conversion: bool = True,
        delete_source: bool = False,
        add_snapshot: bool = True,
    ):
        self._reader = reader
        self._writer = writer
        self._writer.ddb = self._reader.ddb

        self._caching_method = caching_method
        self._source_table = source_table
        self._row_group_size = None
        self._batch_size = None
        self._overwrite = False
        self._schema = None
        self._mode = "delta"
        self._schema_auto_conversion = schema_auto_conversion
        self._delete_source = delete_source

        self._add_snapshot = False
        if hasattr(self, "timefly") and add_snapshot:
            self._add_snapshot = True

    def read(self):
        if self._schema_auto_conversion:
            self._reader.set_pyarrow_schema()
            self._schema = self._reader._schema

        if self._reader._path == self._writer._base_path:
            if self._caching_method not in ["local", "temp_table", "table_"]:
                raise ValueError(
                    f"Overwriting {self._reader._path} is not allowed without a valid caching_method. "
                    "Must be 'local', 'table_' or 'temp_table'."
                )

            self._writer._mode = "overwrite"
            self._delete_source = False

        # Create cache
        if self._caching_method == "local":
            if self._reader._protocol == "file":
                self._caching_method == "temp_table"
            else:
                if not self._reader._cached:
                    self._reader._to_cache()

        elif self._caching_method == "temp_table":
            self._source_table = "temp_table"
            self._reader.create_temp_table()

        elif self._caching_method == "table_":
            self._source_table = "table_"
            self._reader.create_table()

        # Set source table
        if self._source_table == "pa_table":
            if not self._reader.has_pa_table:
                self._reader.load_pa_table()

        elif self._source_table == "dataset":
            if not self._reader.has_dataset:
                self._source_table = self._reader.load_dataset()

        elif self._source_table == "temp_table":
            if not self._reader.has_temp_table:
                self._reader.create_temp_table()

        elif self._source_table == "table_":
            if not self._reader.has_table_:
                self._reader.create_table()

        else:
            raise ValueError(
                f"{self._source_table} is not a valid value for source_table. "
                "Must be 'dataset', 'pa_table', 'table_' or 'temp_table'."
            )

        # add snapshot
        if self._add_snapshot:
            self._reader.timefly.add_snapshot()

        self._source = self._reader.rel

    def _rm(self):
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

    # def schema(self, schema: pa.Schema | None = None):
    #     if schema is not None:
    #         self._schema = schema
    #     return self

    def write(
        self,
        batch_size: str | int | None = None,
        row_group_size: int | None = 100000,
        start_time: str | None = None,
        end_time: str | None = None,
        sort_by: str | None = None,
        ascending: bool | None = None,
        distinct: bool | None = None,
        drop: str | list | None = None,
        partitioning: str | list | None = None,
        partitioning_flavor: str | None = None,
        compression: str | None = None,
        format: str | None = None,
        mode: str | None = None,
        delete_source: bool = False,
        datetime_column: str | None = None,
        transform_func: object | None = None,
        transform_func_kwargs: dict | None = None,
        **kwargs,
    ):
        self.sort(by=sort_by, ascending=ascending)
        self.distinct(value=distinct)
        self.drop(columns=drop)
        self.partitioning(columns=partitioning, flavor=partitioning_flavor)
        self.compression(value=compression)
        self.format(value=format)
        # self.schema(schema=schema)
        self.mode(value=mode)
        self.batch_size(value=batch_size)
        self.row_group_size(value=row_group_size)
        self._writer.write_dataset(
            table=self._source,
            batch_size=self._batch_size,
            start_time=start_time,
            end_time=end_time,
            row_group_size=self._row_group_size,
            datetime_column=datetime_column,
            transform_func=transform_func,
            transform_func_kwargs=transform_func_kwargs,
            **kwargs,
        )
        if delete_source:
            self._delete_source = delete_source

        if self._delete_source:
            self._rm()
