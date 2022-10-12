import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import s3fs

from .reader import Reader
from .writer import Writer
from .utils import to_ddb_relation, copy_to_tmp_directory, path_exists, delete

class Repartition:
    
    
def repartition(
    src: str,
    dest: str,
    base_name: str = "data",
    partitioning: list | str | dict | None = None,
    format: str | list | tuple | dict | None = "parquet",
    filesystem: pafs.FileSystem | s3fs.S3FileSystem | list | tuple | dict | None = None,
    compression: str | None = "zstd",
    rows_per_file: int | None = None,
    row_group_size: int | None = None,
    sort_by: str | list | None = None,
    ascending: bool | list | None = None,
    distinct: bool = False,
    append: bool = True,
    with_time_partition: bool = False,
    with_time_partition: bool = False,
    with_temp_table: bool = False,
    with_mem_table: bool = False,
    use_tmp_directory: bool = False,
    delete_old_files: bool = False,
    reader_kwargs: dict | None = None,
    writer_kwargs: dict | None = None,
):
    ddb = duckdb.connect()

    if isinstance(partitioning, (list, tuple)):
        partitioning_reader = partitioning[0]
        partitioning_writer = partitioning[1]
    elif isinstance(partitioning, dict):
        partitioning_reader = partitioning["reader"]
        partitioning_writer = partitioning["writer"]
    else:
        partitioning_reader = partitioning
        partitioning_writer = partitioning

    if isinstance(format, (list, tuple)):
        format_reader = format[0]
        format_writer = format[1]
    elif isinstance(partitioning, dict):
        format_reader = format["reader"]
        format_writer = format["writer"]
    else:
        format_reader = format
        format_writer = format

    if isinstance(filesystem, (list, tuple)):
        filesystem_reader = filesystem[0]
        filesystem_writer = filesystem[1]
    elif isinstance(filesystem, dict):
        filesystem_reader = filesystem["reader"]
        filesystem_writer = filesystem["writer"]
    else:
        filesystem_reader = filesystem
        filesystem_writer = filesystem

    if src == dest:
        use_tmp_directory = True

    if use_tmp_directory:
        temp_directory = (f"/tmp/{uuid.uuid4().hex}/" + src).replace("//", "/")
        copy_to_tmp_directory(
            srd=src, dest=temp_directory, filesystem=filesystem_reader
        )

        reader = Reader(
            path=temp_directory,
            filesystem=filesystem_reader,
            partitioning=partitioning_reader,
            format=format_reader,
            sort_by=sort_by,
            ascending=ascending,
            ddb=ddb,
            **reader_kwargs,
        )
    else:
        reader = Reader(
            path=src,
            filesystem=filesystem_reader,
            partitioning=partitioning_reader,
            format=format_reader,
            sort_by=sort_by,
            ascending=ascending,
            ddb=ddb,
            **reader_kwargs,
        )

    if with_temp_table:
        reader.create_temp_table(
            sort_by=sort_by, ascending=ascending, distinct=distinct
        )

    if with_mem_table:
        self.load_pa_table(sort_by=sort_by, ascending=ascending)

    if not append:
        if path_exists(path=dest, filesystem=filesystem_writer):
            delete(path=dest, filesystem=filesystem_reader)

    else:
        if distinct:
            reader2 = Reader

    writer = Writer(
        path=dest,
        base_name=base_name,
        filesystem=filesystem_writer,
        partitioning=partitioning_writer,
        format=format_writer,
        compression=compression,
        sort_by=sort_by,
        ascending=ascending,
        ddb=ddb,
        **writer_kwargs,
    )

    writer.write_dataset(
        table=reader.table,
        distinct=distinct,
        rows_per_file=rows_per_file,
        row_group_size=row_group_size,
        with_time_partition=with_time_partition,
    )
