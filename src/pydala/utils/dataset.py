import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
from fsspec import spec
from pyarrow.fs import FileSystem


def _pyarrow_unified_schema(
    schema1: pa.Schema, schema2: pa.Schema
) -> tuple[dict, bool]:
    schema = []
    schemas_equal = True
    dtype_rank = [
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.float16(),
        pa.float32(),
        pa.float64(),
        pa.string(),
    ]
    all_names = sorted(set(schema1.names + schema2.names))
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

            schema.append(pa.field(name, type1 if rank1 > rank2 else type2))

        else:
            schema.append(pa.field(name, type1))

    return pa.schema(schema), schemas_equal


def _polars_unified_schema(schema1: dict, schema2: dict) -> tuple[dict, bool]:
    schema = {}
    schemas_equal = True
    dtype_rank = [
        pl.Int8(),
        pl.Int16(),
        pl.Int32(),
        pl.Int64(),
        pl.Float32(),
        pl.Float64(),
        pl.Utf8(),
    ]
    all_names = sorted(set(list(schema1.keys()) + list(schema2.keys())))
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

            schema[name] = type1 if rank1 > rank2 else type2

        else:
            schema[name] = type1
    return schema, schemas_equal


def list_schemas(
    path: str | None = None,
    dataset: pa._dataset.Dataset | None = None,
    filesystem: spec.AbstractFileSystem | FileSystem | None = None,
):
    if path:
        dataset = ds.dataset(path, filesystem=filesystem)
    else:
        if not dataset:
            raise ValueError("Either path or dataset must be not None.")

    all_schemas = [frag.physical_schema for frag in dataset.get_fragments()]
    return all_schemas


def get_unified_schema(
    schemas: list[pa.Schema] | list[dict] | None = None,
    path: str | None = None,
    dataset: pa._dataset.Dataset | None = None,
    filesystem: spec.AbstractFileSystem | FileSystem | None = None,
):
    if not schemas:
        schemas = list_schemas(path=path, dataset=dataset, filesystem=filesystem)

    schemas_equal = True
    schema = schemas[0]
    for schema2 in schemas[1:]:
        schema, schemas_equal_ = (
            _pyarrow_unified_schema(schema, schema2)
            if isinstance(schema, pa.Schema)
            else _polars_unified_schema(schema, schema2)
        )

        if not schemas_equal_:
            schemas_equal = schemas_equal_

    return schema, schemas_equal


def pyarrow_schema_to_dict(schema: pa.Schema):
    return dict(zip(schema.names, map(_pyarrow_datatype_to_str, schema.types)))


def _pyarrow_datatype_to_str(data_type: pa.DataType):
    if isinstance(data_type, pa.DataType):
        return str(data_type)
    return data_type


def _str_to_pyarrow_datatype(data_type: str):
    if "timestamp" in data_type and "tz" in data_type:
        tz = data_type.split("tz=")[-1].split("]")[0]
        unit = data_type.split("[")[-1].split(",")[0].split("]")[0]

        return pa.timestamp(unit=unit, tz=tz)

    return pa.type_for_alias(data_type)


def pyarrow_schema_from_dict(schema: dict):
    return pa.schema(
        dict(
            zip(
                list(schema.keys()),
                map(_str_to_pyarrow_datatype, list(schema.values())),
            )
        )
    )
