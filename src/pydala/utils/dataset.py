import os
from typing import Dict, List, Tuple

import pandas as pd
import polars as pl
import pyarrow as pa
from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.spec import AbstractFileSystem

from .base import run_parallel
from .schema import unify_schema


def get_arrow_schema(dataset: pa.dataset.Dataset) -> Dict[str, pa.Schema]:
    def _get_physical_schema(frag):
        return frag.path, frag.physical_schema

    pa_schemas = run_parallel(
        _get_physical_schema, dataset.get_fragments(), backend="threading"
    )
    return dict(pa_schemas)


def get_unified_schema(
    schemas: List[pa.Schema] | Dict[str, pa.Schema]
) -> Tuple[pa.Schema, bool]:
    schemas_equal = True
    all_schemas = list(schemas.values()) if isinstance(schemas, dict) else schemas
    unified_schema = all_schemas[0]
    for schema in all_schemas[1:]:
        unified_schema, schemas_equal_ = unify_schema(unified_schema, schema)

        schemas_equal *= schemas_equal_

    return unified_schema, schemas_equal_


def get_file_details(
    dataset: pa.dataset.Dataset,
    timestamp_column: str | None = None,
    filesystem: AbstractFileSystem | None = None,
) -> pl.DataFrame:
    if filesystem is None:
        filesystem = ArrowFSWrapper(dataset.filesystem)

    details = {}
    details["path"] = dataset.files
    details["name"] = {f: os.path.basename(f) for f in details["path"]}

    dirnames = {os.path.dirname(f) for f in details["path"]}

    sizes = run_parallel(filesystem.du, dirnames, total=False, backend="threading")
    details["size"] = {}
    for s in sizes:
        details["size"].update(s)

    details["last_modified"] = {f: filesystem.modified(f) for f in details["path"]}

    def _get_count_rows(frag):
        return frag.path, frag.count_rows()

    details["row_count"] = dict(
        run_parallel(_get_count_rows, dataset.get_fragments(), backend="threading")
    )

    details = pd.concat(
        [
            pd.Series(details[k]).rename(k)
            for k in ["name", "size", "last_modified", "row_count"]
        ],
        axis=1,
    )

    if timestamp_column is not None:

        def _get_timestamp_min_max(frag, col):
            col_num = frag.physical_schema.names.index(col)
            num_row_groups = frag.metadata.num_row_groups
            max_ = [
                frag.metadata.row_group(i).column(col_num).statistics.max
                for i in range(num_row_groups)
            ]
            min_ = [
                frag.metadata.row_group(i).column(col_num).statistics.min
                for i in range(num_row_groups)
            ]
            return frag.path, {"timestamp_max": max(max_), "timestamp_min": min(min_)}

        timestamp_min_max = dict(
            run_parallel(
                _get_timestamp_min_max,
                dataset.get_fragments(),
                timestamp_column,
                backend="threading",
            )
        )

        details = details.merge(
            pd.DataFrame(timestamp_min_max).T, right_index=True, left_index=True
        )

    details.index.names = ["path"]
    details = pl.from_pandas(details.reset_index())
    return details
