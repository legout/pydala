import duckdb
import pyarrow.fs as pafs
import s3fs

# from ..filesystem import delete, path_exists
from .reader import Reader


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
        to_local: bool = False,
        tmp_path: str = "/tmp/duckdb",
        delete_after: bool = False,
        reader_kwargs: dict | None = None,
        writer_kwargs: dict | None = None,
    ):
        self._src = src
        self._dest = dest
        self._bucket = bucket
        self._base_name = base_name
        self._partitioning = self._set_reader_writer_params(partitioning)
        self._format = self._set_reader_writer_params(format)
        self._filesystem = self._set_reader_writer_params(filesystem)
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
        self._to_local = to_local
        self._delete_after = delete_after
        self._tmp_path = tmp_path
        self._reader_kwargs = reader_kwargs
        self._writer_kwargs = writer_kwargs

        self.ddb = duckdb.connect()
        self.ddb.execute(f"SET temp_directory='{tmp_path}'")

    def _set_reader_writer_params(value: str | list | tuple | dict):
        if isinstance(value, dict):
            return value
        elif isinstance(value, (list, tuple)):
            return dict(reader=value[0], writer=value[1])
        else:
            return dict(reader=value, writer=value)

    def set_reader(
        self,
    ):

        self.src_reader = Reader(
            path=self._src,
            bucket=self._bucket,
            name="src",
            partitioning=self._partitioning["reader"],
            filesystem=self._filesystem["reader"],
            format=self._format["reader"],
            sort_by=self._sort_by,
            ascending=self._ascending,
            distinct=self._distinct,
            drop=self._drop,
            to_local=self._to_local,
            tmp_path=self._tmp_path,
            ddb=self.ddb,
        )
        if path_exists(self._dest, self._filesystem["writer"]):

            self.dest_reader = Reader(
                path=self._dest,
                bucket=self._bucket,
                name="dest",
                partitioning=self._partitioning["writer"],
                filesystem=self._filesystem["writer"],
                format=self._format["writer"],
                sort_by=self._sort_by,
                ascending=self._ascending,
                distinct=self._distinct,
                drop=self._drop,
                to_local=self._to_local,
                tmp_path=self._tmp_path,
                ddb=self.ddb,
            )

        if self._with_temp_table:
            self.src_reader.create_temp_table(
                sort_by=self._sort_by,
                ascending=self._ascending,
                distinct=self._distinct,
                drop=self._drop,
            )
            if hasattr(self, "dest_reader"):
                self.dest_reader.create_temp_table(
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
            if hasattr(self, "dest_reader"):
                self.dest_reader.load_mem_table(
                    sort_by=self._sort_by,
                    ascending=self._ascending,
                    distinct=self._distinct,
                    drop=self._drop,
                    **kwargs,
                )

    def distinct(self):
        if path_exists(self._dest, filesystem=self._filesystem["writer"]):

            if self._append:
                ...

            else:
                delete(self._dist, filesystem=self._filesystem["writer"])


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
#         if path_exists(path=dest, filesystem=filesystem["writer"]):
#             delete(path=dest, filesystem=filesystem_reader)

#     else:
#         if distinct:
#             reader2 = Reader(name="dest",path=dest, partitioning=partitioning_writer, filesystem=filesystem["writer"], format=format_writer, ascending=ascending, ddb=ddb)


#     writer = Writer(
#         path=dest,
#         base_name=base_name,
#         filesystem=filesystem["writer"],
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
