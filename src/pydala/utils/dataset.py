from typing import List, Tuple

import duckdb
import polars as pl
import pyarrow as pa

# from ..utils.base import run_parallel


def _unify_schema_pyarrow(schema1: pa.Schema, schema2: pa.Schema) -> Tuple[dict, bool]:
    """Returns a unified pyarrow schema.

    Args:
        schema1 (pa.Schema): pyarrow schema 1
        schema2 (pa.Schema): pyarrow schema 2

    Returns:
        Tuple[dict, bool]: unified pyarrow schema, bool value if schemas were equal
    """

    dtype_rank = [
        pa.null(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.float16(),
        pa.float32(),
        pa.float64(),
        pa.string(),
    ]

    # check for equal columns and column order
    if schema1.names == schema2.names:
        if schema1.types == schema2.types:
            return schema1, True

        else:
            schemas_equal = False
            all_names = schema1.names

    elif sorted(schema1.names) == sorted(schema2.names):
        schemas_equal = False
        all_names = sorted(schema1.names)

    else:
        schemas_equal = False
        all_names = sorted(set(schema1.names + schema2.names))

    unified_schema = []
    for name in all_names:
        if name in schema1.names:
            type1 = schema1.field(name).type
        else:
            type1 = schema2.field(name).type
        if name in schema2.names:
            type2 = schema2.field(name).type
        else:
            type2 = schema1.field(name).type

        if type1 != type2:
            schemas_equal = False
            if type1 in dtype_rank:
                rank1 = dtype_rank.index(type1)
            else:
                rank1 = 0
            if type2 in dtype_rank:
                rank2 = dtype_rank.index(type2)
            else:
                rank2 = 0

            unified_schema.append(pa.field(name, type1 if rank1 > rank2 else type2))

        else:
            unified_schema.append(pa.field(name, type1))

    return pa.schema(unified_schema), schemas_equal


def _unify_schema_polars(schema1: dict, schema2: dict) -> Tuple[dict, bool]:
    """Returns a unified polars schema.

    Args:
        schema1 (dict): polars schema 1
        schema2 (dict): polars schema2

    Returns:
        Tuple[dict, bool]: unified polars schema
    """
    dtype_rank = [
        pl.Null(),
        pl.Int8(),
        pl.Int16(),
        pl.Int32(),
        pl.Int64(),
        pl.Float32(),
        pl.Float64(),
        pl.Utf8(),
    ]
    if list(schema1.keys()) == list(schema2.keys()):
        if list(schema1.values()) == list(schema2.values()):
            return schema1, True

        else:
            schemas_equal = False
            all_names = list(schema1.keys())

    elif sorted(schema1.keys()) == sorted(schema2.keys()):
        schemas_equal = False
        all_names = sorted(schema1.keys())

    else:
        schemas_equal = False
        all_names = sorted(set(list(schema1.keys()) + list(schema2.keys())))

    unified_schema = dict()
    for name in all_names:
        if name in schema1:
            type1 = schema1[name]
        else:
            type1 = schema2[name]
        if name in schema2:
            type2 = schema2[name]
        else:
            type2 = schema1[name]

        if type1 != type2:
            schemas_equal = False
            if type1 in dtype_rank:
                rank1 = dtype_rank.index(type1)
            else:
                rank1 = 0
            if type2 in dtype_rank:
                rank2 = dtype_rank.index(type2)
            else:
                rank2 = 0

            unified_schema[name] = type1 if rank1 > rank2 else type2

        else:
            unified_schema[name] = type1
    return unified_schema, schemas_equal


def unify_schema(
    schema1: pa.Schema | dict, schema2: pa.Schema | dict
) -> Tuple[pa.Schema, bool] | Tuple[dict, bool]:
    """Returns a unified pyarrow or polars schema.

    Args:
        schema1 (pa.Schema | dict): pyarrow or polars schema 1
        schema2 (pa.Schema | dict): pyarrow or polars schema 2

    Returns:
        Tuple[pa.Schema, bool] | Tuple[dict, bool]: unified pyarrow or polars schema and
    """
    unified_schema, schemas_equal = (
        _unify_schema_pyarrow(schema1, schema2)
        if isinstance(schema1, pa.Schema)
        else _unify_schema_polars(schema1, schema2)
    )
    return unified_schema, schemas_equal


def _sort_schema_pyarrow(schema: pa.Schema) -> pa.Schema:
    return pa.schema(
        [
            pa.field(name, type_)
            for name, type_ in sorted(zip(schema.names, schema.types))
        ]
    )


def _sort_schema_polars(schema: dict) -> dict:
    return {name: schema[name] for name in sorted(schema.keys())}


def sort_schema(schema: pa.Schema | dict) -> pa.Schema | dict:
    sorted_schema = (
        _sort_schema_pyarrow(schema)
        if isinstance(schema, pa.Schema)
        else _sort_schema_polars(schema)
    )
    return sorted_schema


def _convert_dtype_polars_to_pyarrow(dtype: pl.DataType) -> pa.lib.DataType:
    return pl.utils.convert.dtype_to_arrow_type(dtype)


def _convert_dtype_pyarrow_to_polars(dtype: pa.lib.DataType) -> pl.DataType:
    dtype_mapping = {
        pa.int8(): pl.Int8(),
        pa.int16(): pl.Int16(),
        pa.int32(): pl.Int32(),
        pa.int64(): pl.Int64(),
        pa.uint8(): pl.UInt8(),
        pa.uint16(): pl.UInt16(),
        pa.uint32(): pl.UInt32(),
        pa.uint64(): pl.UInt64(),
        pa.float16(): pl.Float32(),
        pa.float32(): pl.Float32(),
        pa.float64(): pl.Float64(),
        pa.bool_(): pl.Boolean(),
        pa.large_utf8(): pl.Utf8(),
        pa.utf8(): pl.Utf8(),
        pa.date32(): pl.Date(),
        pa.timestamp("us"): pl.Datetime("us"),
        pa.timestamp("ms"): pl.Datetime("ms"),
        pa.timestamp("us"): pl.Datetime("us"),
        pa.timestamp("ns"): pl.Datetime("ns"),
        pa.duration("us"): pl.Duration("us"),
        pa.duration("ms"): pl.Duration("ms"),
        pa.duration("us"): pl.Duration("us"),
        pa.duration("ns"): pl.Duration("ns"),
        pa.time64("us"): pl.Time(),
        pa.null(): pl.Null(),
    }
    tz = None
    if isinstance(dtype, pa.lib.TimestampType):
        dtype, tz = pa.timestamp(dtype.unit), dtype.tz

    pl_dtype = dtype_mapping[dtype]
    if tz:
        pl_dtype.tz = tz

    return pl_dtype


def convert_dtype(
    dtype: pa.lib.DataType | pl.DataType,
) -> pl.DataType | pa.lib.DataType:
    dtype = (
        _convert_dtype_pyarrow_to_polars(dtype)
        if isinstance(dtype, pa.lib.DataType)
        else _convert_dtype_polars_to_pyarrow(dtype)
    )
    return dtype


def _convert_schema_pyarrow_to_polars(schema: pa.Schema) -> dict:
    return {
        field.name: _convert_dtype_pyarrow_to_polars(field.type) for field in schema
    }


def _convert_schema_polars_to_pyarrow(schema: dict) -> pa.Schema:
    return pa.Schema([pa.field(name, dtype) for name, dtype in schema.items()])


def convert_schema(schema: pa.Schema | dict) -> dict | pa.Schema:
    return (
        _convert_schema_pyarrow_to_polars(schema)
        if isinstance(schema, pa.Schema)
        else _convert_schema_polars_to_pyarrow(schema)
    )


def sync_datasets(dataset1, dataset2, delete=True):
    def transfer_file(f):
        with dataset2._dir_filesystem.open(f, "wb") as ff:
            ff.write(dataset1._dir_filesystem.read_bytes(f))

    def delete_file(f):
        dataset2._dir_filesystem.rm(f)

    new_files = duckdb.from_arrow(
        dataset1.files.select(["path", "name", "size"]).to_arrow()
    ).except_(
        duckdb.from_arrow(dataset2.files.select(["path", "name", "size"]).to_arrow())
    ).pl()["path"].to_list()

    _ = run_parallel(transfer_file, new_files)

    if delete:
        rm_files = duckdb.from_arrow(
            dataset2.files.select(["path", "name", "size"]).to_arrow()
        ).except_(
            duckdb.from_arrow(
                dataset1.files.select(["path", "name", "size"]).to_arrow()
            )
        )

        _ = run_parallel(delete_file, rm_files)
