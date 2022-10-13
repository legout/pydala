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
from ..filesystem import copy_to_tmp_directory, path_exists, delete


class Repartition:
    def __init__(
        self,
        src: str,
        dest: str,
        bucket: str | None = None,
        base_name: str = "data",
        partitioning: list | str | dict | None = None,
        format: str | list | tuple | dict | None = "parquet",
        filesystem: pafs.FileSystem
        | s3fs.S3FileSystem
        | list
        | tuple
        | dict
        | None = None,
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool = False,
        drop: str | list | None = None,
        compression: str | None = "zstd",
        rows_per_file: int | None = None,
        row_group_size: int | None = None,
        append: bool = True,
        with_time_partition: bool = False,
        with_temp_table: bool = False,
        with_mem_table: bool = False,
        use_tmp_directory: bool = False,
        delete_after: bool = False,
        reader_kwargs: dict | None = None,
        writer_kwargs: dict | None = None,
    ):
        self._src = src
        self._dest = (dest,)
        self._bucket = bucket
        self._base_name = (base_name,)
        
        self._set_reader_params(partitioning=partitioning, format=format,  filesystem=filesystem)
        self._set_writer_params(partitioning=partitioning,filesystem=filesystem,format=format)

        self._compression = compression
        self._sort_by = sort_by
        self._ascending = ascending
        self._distinct = distinct
        self._drop = drop
        self._rows_per_file = rows_per_file
        self._row_group_size = row_group_size
        self._append = append
        self._with_time_partition = with_time_partition
        self._with_temp_table = with_temp_table
        self._wit_mem_table = with_mem_table
        self._use_tmp_directory = use_tmp_directory
        self._delete_after = delete_after
        
        self._reader_kwargs = reader_kwargs
        self._writer_kwargs = writer_kwargs
        
        self.ddb = duckdb.connect()
        self.ddb.execute("SET temp_directory='/tmp/duckdb/'")

        if src == dest:
            self._use_tmp_directory = True
            self._tmp_directory = {"src":(f"/tmp/duckdb/{uuid.uuid4().hex}/" + src).replace(      "//", "/")
            }
            self._tmp_directory["dest"] = (f"/tmp/duckdb/{uuid.uuid4().hex}/" + dest).replace(
                "//", "/")

    def _set_reader_params(self,partitioning: list | str | dict | None = None,
        format: str | list | tuple | dict | None = "parquet",
        filesystem: pafs.FileSystem
        | s3fs.S3FileSystem
        | list
        | tuple
        | dict
        | None = None,):
        if isinstance(partitioning, (list, tuple)):
            self._partitioning_reader = partitioning[0]
        elif isinstance(partitioning, dict):
            self._partitioning_reader = partitioning["reader"]
        else:
            self._partitioning_reader = partitioning

        if isinstance(format, (list, tuple)):
            self._format_reader = format[0]
        elif isinstance(partitioning, dict):
            self._format_reader = format["reader"]
        else:
            self._format_reader = format

        if isinstance(filesystem, (list, tuple)):
            self._filesystem_reader = filesystem[0]
        elif isinstance(filesystem, dict):
            self._filesystem_reader = filesystem["reader"]
        else:
            self._filesystem_reader = filesystem

    def _set_writer_params(self,partitioning: list | str | dict | None = None,
        format: str | list | tuple | dict | None = "parquet",
        filesystem: pafs.FileSystem
        | s3fs.S3FileSystem
        | list
        | tuple
        | dict
        | None = None,):
        if isinstance(partitioning, (list, tuple)):
            self._partitioning_writer = partitioning[0]
        elif isinstance(partitioning, dict):
            self._partitioning_writer = partitioning["writer"]
        else:
            self._partitioning_writer = partitioning

        if isinstance(format, (list, tuple)):
            self._format_writer = format[0]
        elif isinstance(partitioning, dict):
            self._format_writer = format["writer"]
        else:
            self._format_writer = format

        if isinstance(filesystem, (list, tuple)):
            self._filesystem_writer = filesystem[0]
        elif isinstance(filesystem, dict):
            self._filesystem_writer = filesystem["writer"]
        else:
            self._filesystem_writer= filesystem
    
    def _load_src(
        self,
        partitioning: ds.Partitioning | str | None = None,
        filesystem: pafs.FileSystem | s3fs.S3FileSystem | None = None,
        format: str | None = "parquet",
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = "__index_level_0__",
        **kwargs,
    ):


        if partitioning is None:
            partitioning = self._partitioning_reader
        if filesystem is  None:
            filesystem = self._filesystem_reader
        if format is  None:
            format = self._format_reader
            
        self._set_reader_params(partitioning=partitioning, filesystem=filesystem,format=format)
        
        if sort_by is not None:
            self._sort_by = sort_by
        if ascending is not None:
            self._ascending = ascending
        if distinct is not None:
            self._distinct = distinct
        if drop is not None:
            self._drop = drop

        kwargs.update(self._reader_kwargs)

        if self._use_tmp_directory:
            src = (
                f"{self._bucket}/{self._src}".replace("//", "/")
                if self._bucket is not None
                else self._src
            )
            copy_to_tmp_directory(
                src=src, dest=self._tmp_directory["src"], filesystem=self._filesystem_reader
            )
            path = self._tmp_directory["src"]

        else:
            path = src

        self.src_reader = Reader(
            path=path,
            bucket=self._bucket,
            name="src",
            partitioning=self._partitioning_reader,
            filesystem=self._filesystem_reader,
            format=self._format_reader,
            sort_by=self._sort_by,
            ascending=self._ascending,
            distinct=self._distinct,
            drop=self._drop,
            ddb=self.ddb,
        )

        if self._with_temp_table:
            self.src_reader.create_temp_table(
                sort_by=self._sort_by,
                ascending=self._ascending,
                distinct=self._distinct,
                drop=self._drop,
            )

        if self._with_mem_table:
            self.src_reader.load_mem_table(
                sort_by=self._sort_by,
                ascending=self._ascending,
                distinct=self._distinct,
                drop=self._drop,
                **kwargs,
            )

    def _load_dest(
        self,
        partitioning: ds.Partitioning | str | None = None,
        filesystem: pafs.FileSystem | s3fs.S3FileSystem | None = None,
        format: str | None = "parquet",
        sort_by: str | list | None = None,
        ascending: bool | list | None = None,
        distinct: bool | None = None,
        drop: str | list | None = "__index_level_0__",
        **kwargs,
    ):

        if partitioning is None:
            partitioning = self._partitioning_writer
        if filesystem is  None:
            filesystem = self._filesystem_writer
        if format is  None:
            format = self._format_writer
        
        self._set_reader_params(partitioning=partitioning, filesystem=filesystem,format=format)
        
        if sort_by is not None:
            self._sort_by = sort_by
        if ascending is not None:
            self._ascending = ascending
        if distinct is not None:
            self._distinct = distinct
        if drop is not None:
            self._drop = drop

        kwargs.update(self._writer_kwargs)

        if self._use_tmp_directory:
            src = (
                f"{self._bucket}/{self._src}".replace("//", "/")
                if self._bucket is not None
                else self._src
            )
            copy_to_tmp_directory(
                src=src, dest=self._tmp_directory["src"], filesystem=self._filesystem_reader
            )
            path = self._tmp_directory["src"]

        else:
            path = src

        self.dest_reader = Reader(
            path=path,
            bucket=self._bucket,
            name="dest",
            partitioning=self._partitioning_reader,
            filesystem=self._filesystem_reader,
            format=self._format_reader,
            sort_by=self._sort_by,
            ascending=self._ascending,
            distinct=self._distinct,
            drop=self._drop,
            ddb=self.ddb,
        )

        if self._with_temp_table:
            self.dest_reader.create_temp_table(
                sort_by=self._sort_by,
                ascending=self._ascending,
                distinct=self._distinct,
                drop=self._drop,
            )

        if self._with_mem_table:
            self.dest_reader.load_mem_table(
                sort_by=self._sort_by,
                ascending=self._ascending,
                distinct=self._distinct,
                drop=self._drop,
                **kwargs,
            )
        
    def setup(self, *args, **kwargs):
        self._load_src(*args, **kwargs)
        
        if self._append:
            self._load_dest(*args, **kwargs)
            
        else:
            delete(self._dest, filesystem=self._filesyste_writer)
            
            
        
        


# def repartition(
#     src: str,
#     dest: str,
#     base_name: str = "data",
#     partitioning: list | str | dict | None = None,
#     format: str | list | tuple | dict | None = "parquet",
#     filesystem: pafs.FileSystem | s3fs.S3FileSystem | list | tuple | dict | None = None,
#     compression: str | None = "zstd",
#     rows_per_file: int | None = None,
#     row_group_size: int | None = None,
#     sort_by: str | list | None = None,
#     ascending: bool | list | None = None,
#     distinct: bool = False,
#     drop:str|list|None = None,
#     append: bool = True,
#     with_time_partition: bool = False,
#     with_time_partition: bool = False,
#     with_temp_table: bool = False,
#     with_mem_table: bool = False,
#     use_tmp_directory: bool = False,
#     delete_old_files: bool = False,
#     reader_kwargs: dict | None = None,
#     writer_kwargs: dict | None = None,
# ):
#     ddb = duckdb.connect()


#     if src == dest:
#         use_tmp_directory = True

#     if use_tmp_directory:
#         temp_directory = (f"/tmp/{uuid.uuid4().hex}/" + src).replace("//", "/")
#         copy_to_tmp_directory(
#             srd=src, dest=temp_directory, filesystem=filesystem_reader
#         )

#         reader = Reader(
#             name="source",
#             path=temp_directory,
#             filesystem=filesystem_reader,
#             partitioning=partitioning_reader,
#             format=format_reader,
#             sort_by=sort_by,
#             ascending=ascending,
#             distinct=distinct,
#             drop=drop,
#             ddb=ddb,
#             **reader_kwargs,
#         )
#     else:
#         reader = Reader(
#             name="source",
#             path=src,
#             filesystem=filesystem_reader,
#             partitioning=partitioning_reader,
#             format=format_reader,
#             sort_by=sort_by,
#             ascending=ascending,
#             distinct=distinct,
#             drop=drop,
#             ddb=ddb,
#             **reader_kwargs,
#         )

#     if with_temp_table:
#         reader.create_temp_table(
#             sort_by=sort_by, ascending=ascending, distinct=distinct
#         )

#     if with_mem_table:
#         self.load_pa_table(sort_by=sort_by, ascending=ascending)

#     if not append:
#         if path_exists(path=dest, filesystem=filesystem_writer):
#             delete(path=dest, filesystem=filesystem_reader)

#     else:
#         if distinct:
#             reader2 = Reader(name="dest",path=dest, partitioning=partitioning_writer, filesystem=filesystem_writer, format=format_writer, ascending=ascending, ddb=ddb)


#     writer = Writer(
#         path=dest,
#         base_name=base_name,
#         filesystem=filesystem_writer,
#         partitioning=partitioning_writer,
#         format=format_writer,
#         compression=compression,
#         sort_by=sort_by,
#         ascending=ascending,
#         ddb=ddb,
#         **writer_kwargs,
#     )

#     writer.write_dataset(
#         table=reader.table,
#         distinct=distinct,
#         rows_per_file=rows_per_file,
#         row_group_size=row_group_size,
#         with_time_partition=with_time_partition,
#     )
