from re import S
import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import s3fs
import uuid

from .reader import Reader
from .writer import Writer
from ..filesystem import path_exists, delete
from .utils import get_tables_diff


class Repartition:
    def __init__(
        self,
        reader: Reader,
        writer: Writer,
        caching_method: str = "local",  # or temp_table
        source_table: str = "mem_table",  # or temp_table or dataset
    ):
        self._reader = reader
        self._writer = writer

        self._caching_method = caching_method
        self._source_table = source_table
        self._row_group_size = None
        self._batch_size = None

    def read(self):

        if self._reader._path == self._writer._base_path:

            if self._caching_method == "local":
                self._reader._to_cache()

            elif self._caching_method == "temp_table":
                self._reader.create_temp_table()

            else:
                raise ValueError(
                    f"{self._caching_method} is not a valid value for caching_method. "
                    "Must be 'local' or 'mem_table'."
                )

        if self._source_table == "mem_table":
            if not self._reader.has_mem_table:
                self._reader.load_mem_table()

        elif self._source_table == "dataset":
            if not self._reader.has_dataset:
                self._source_table = self._reader.set_dataset()

        elif self._source_table == "temp_table":
            if self._reader.has_temp_table:
                self._reader.create_temp_table()

        else:
            raise ValueError(
                f"{self._source_table} is not a valid value for source_table. "
                "Must be 'local' or 'mem_table'."
            )

        self._source = self._reader.rel

    def sort(self, by: str | list | None, ascending: bool | list | None = None):
        self._writer._sort_by = by

        if ascending is None:
            ascending = True
        self._writer._ascending = ascending

        if self._writer._sort_by is not None:
            self._writer._sort_by_ddb = get_ddb_sort_str(
                sort_by=by, ascending=ascending
            )

        return self

    def distinct(self, value: bool | None):
        if value is None:
            value = False
        self._writer._distinct = value

        return self

    def drop(self, columns: str | list | None):
        self._writer._drop = columns

        return self

    def partitioning(
        self, columns: str | list | None = None, flavor: str | None = None
    ):
        if columns is not None:
            if isinstance(columns, str):
                columns = [columns]
            self._writer._partitioning = columns

        if flavor is not None:
            self._writer._partitioning_flavor = flavor

        return self

    def compression(self, value: str | None = None):
        self._writer._compression = value

        return self

    def format(self, value: str | None = None):
        if value is not None:
            self._writer._format = value

        return self

    def mode(self, value: str | None):
        if value is not None:
            if value not in ["overwrite", "append", "raise"]:
                raise ValueError(
                    "Value for mode must be 'overwrite', 'raise' or 'append'."
                )
            else:
                self._writer._mode = value

        return self

    def batch_size(self, value: int | str | None = None):
        if value is not None:
            self._batch_size = value

        return self

    def row_group_size(self, value: int | None = None):
        if value is not None:
            self._row_group_size = value

        return self

    def write(
        self,
        batch_size: str | int | None = None,
        row_group_size: int | None = None,
        sort_by: str | None = None,
        ascending: bool | None = None,
        distinct: bool | None = None,
        drop: str | list | None = None,
        partitioning: str | list | None = None,
        partitioning_flavor: str | None = None,
        compression: str | None = None,
        format: str | None = None,
        mode: str | None = None,
    ):
        self.sort(by=sort_by, ascending=ascending)
        self.distinct(value=distinct)
        self.drop(columns=drop)
        self.partitioning(columns=partitioning, flavor=partitioning_flavor)
        self.compression(value=compression)
        self.format(value=format)
        self.mode(value=mode)
        self.batch_size(value=batch_size)
        self.row_group_size(value=row_group_size)
        self._writer.write_dataset(
            table=self._source,
            batch_size=self._batch_size,
            row_group_size=self._row_group_size,
        )


# class Repartition:
#     def __init__(
#         self,
#         src: str,
#         dest: str,
#         bucket: str | None = None,
#         base_name: str = "data",
#         partitioning: list | str | dict | None = None,
#         format: str | list | tuple | dict | None = "parquet",
#         filesystem: pafs.FileSystem
#         | s3fs.S3FileSystem
#         | list
#         | tuple
#         | dict
#         | None = None,
#         sort_by: str | list | None = None,
#         ascending: bool | list | None = None,
#         distinct: bool = False,
#         drop: str | list | None = None,
#         compression: str | None = "zstd",
#         rows_per_file: int | None = None,
#         row_group_size: int | None = None,
#         append: bool = True,
#         with_time_partition: bool = False,
#         with_temp_table: bool = False,
#         with_mem_table: bool = False,
#         to_local: bool = False,
#         tmp_path:str="/tmp/duckdb",
#         delete_after: bool = False,
#         reader_kwargs: dict | None = None,
#         writer_kwargs: dict | None = None,
#     ):
#         self._src = src
#         self._dest = dest
#         self._bucket = bucket
#         self._base_name = base_name
#         self._partitioning = self._set_reader_writer_params(partitioning)
#         self._format = self._set_reader_writer_params(format)
#         self._filesystem = self._set_reader_writer_params(filesystem)
#         self._compression = compression
#         self._sort_by = sort_by
#         self._ascending = ascending
#         self._distinct = distinct
#         self._drop = drop
#         self._rows_per_file = rows_per_file
#         self._row_group_size = row_group_size
#         self._append = append
#         self._with_time_partition = with_time_partition
#         self._with_temp_table = with_temp_table
#         self._wit_mem_table = with_mem_table
#         self._to_local = to_local
#         self._delete_after = delete_after
#         self._tmp_path = tmp_path
#         self._reader_kwargs = reader_kwargs
#         self._writer_kwargs = writer_kwargs

#         self.ddb = duckdb.connect()
#         self.ddb.execute(f"SET temp_directory='{tmp_path}'")


#     def _set_reader_writer_params(value:str|list|tuple|dict):
#         if isinstance(value, dict):
#             return value
#         elif isinstance(value, (list,tuple)):
#             return dict(reader=value[0], writer=value[1])
#         else:
#             return dict(reader=value, writer=value)


#     def set_reader(
#         self,
#     ):

#         self.src_reader = Reader(
#             path=self._src,
#             bucket=self._bucket,
#             name="src",
#             partitioning=self._partitioning["reader"],
#             filesystem=self._filesystem["reader"],
#             format=self._format["reader"],
#             sort_by=self._sort_by,
#             ascending=self._ascending,
#             distinct=self._distinct,
#             drop=self._drop,
#             to_local=self._to_local
#             tmp_path=self._tmp_path
#             ddb=self.ddb
#         )
#         if path_exists(self._dest, self._filesystem["writer"]):

#             self.dest_reader =  Reader(
#                 path=self._dest,
#                 bucket=self._bucket,
#                 name="dest",
#                 partitioning=self._partitioning["writer"],
#                 filesystem=self._filesystem["writer"],
#                 format=self._format["writer"],
#                 sort_by=self._sort_by,
#                 ascending=self._ascending,
#                 distinct=self._distinct,
#                 drop=self._drop,
#                 to_local=self._to_local
#                 tmp_path=self._tmp_path
#                 ddb=self.ddb
#             )

#         if self._with_temp_table:
#             self.src_reader.create_temp_table(
#                 sort_by=self._sort_by,
#                 ascending=self._ascending,
#                 distinct=self._distinct,
#                 drop=self._drop,
#             )
#             if hasattr(self, "dest_reader"):
#                 self.dest_reader.create_temp_table(sort_by=self._sort_by,
#                     ascending=self._ascending,
#                     distinct=self._distinct,
#                     drop=self._drop,)

#         if self._with_mem_table:
#             self.src_reader.load_mem_table(
#                 sort_by=self._sort_by,
#                 ascending=self._ascending,
#                 distinct=self._distinct,
#                 drop=self._drop,
#                 **kwargs,
#             )
#             if hasattr(self, "dest_reader"):
#                 self.dest_reader.load_mem_table(
#                     sort_by=self._sort_by,
#                     ascending=self._ascending,
#                     distinct=self._distinct,
#                     drop=self._drop,
#                     **kwargs,
#                 )


#     def distinct(self):
#         if path_exists(self._dest, filesystem=self._filesystem["writer"]):

#             if self._append:


#             else:
#                 delete(self._dist, filesystem=self._filesystem["writer"])


# # def repartition(
# #     src: str,
# #     dest: str,
# #     base_name: str = "data",
# #     partitioning: list | str | dict | None = None,
# #     format: str | list | tuple | dict | None = "parquet",
# #     filesystem: pafs.FileSystem | s3fs.S3FileSystem | list | tuple | dict | None = None,
# #     compression: str | None = "zstd",
# #     rows_per_file: int | None = None,
# #     row_group_size: int | None = None,
# #     sort_by: str | list | None = None,
# #     ascending: bool | list | None = None,
# #     distinct: bool = False,
# #     drop:str|list|None = None,
# #     append: bool = True,
# #     with_time_partition: bool = False,
# #     with_time_partition: bool = False,
# #     with_temp_table: bool = False,
# #     with_mem_table: bool = False,
# #     use_tmp_directory: bool = False,
# #     delete_old_files: bool = False,
# #     reader_kwargs: dict | None = None,
# #     writer_kwargs: dict | None = None,
# # ):
# #     ddb = duckdb.connect()


# #     if src == dest:
# #         use_tmp_directory = True

# #     if use_tmp_directory:
# #         temp_directory = (f"/tmp/{uuid.uuid4().hex}/" + src).replace("//", "/")
# #         copy_to_tmp_directory(
# #             srd=src, dest=temp_directory, filesystem=filesystem_reader
# #         )

# #         reader = Reader(
# #             name="source",
# #             path=temp_directory,
# #             filesystem=filesystem_reader,
# #             partitioning=partitioning_reader,
# #             format=format_reader,
# #             sort_by=sort_by,
# #             ascending=ascending,
# #             distinct=distinct,
# #             drop=drop,
# #             ddb=ddb,
# #             **reader_kwargs,
# #         )
# #     else:
# #         reader = Reader(
# #             name="source",
# #             path=src,
# #             filesystem=filesystem_reader,
# #             partitioning=partitioning_reader,
# #             format=format_reader,
# #             sort_by=sort_by,
# #             ascending=ascending,
# #             distinct=distinct,
# #             drop=drop,
# #             ddb=ddb,
# #             **reader_kwargs,
# #         )

# #     if with_temp_table:
# #         reader.create_temp_table(
# #             sort_by=sort_by, ascending=ascending, distinct=distinct
# #         )

# #     if with_mem_table:
# #         self.load_pa_table(sort_by=sort_by, ascending=ascending)

# #     if not append:
# #         if path_exists(path=dest, filesystem=filesystem["writer"]):
# #             delete(path=dest, filesystem=filesystem_reader)

# #     else:
# #         if distinct:
# #             reader2 = Reader(name="dest",path=dest, partitioning=partitioning_writer, filesystem=filesystem["writer"], format=format_writer, ascending=ascending, ddb=ddb)


# #     writer = Writer(
# #         path=dest,
# #         base_name=base_name,
# #         filesystem=filesystem["writer"],
# #         partitioning=partitioning_writer,
# #         format=format_writer,
# #         compression=compression,
# #         sort_by=sort_by,
# #         ascending=ascending,
# #         ddb=ddb,
# #         **writer_kwargs,
# #     )

# #     writer.write_dataset(
# #         table=reader.table,
# #         distinct=distinct,
# #         rows_per_file=rows_per_file,
# #         row_group_size=row_group_size,
# #         with_time_partition=with_time_partition,
# #     )
