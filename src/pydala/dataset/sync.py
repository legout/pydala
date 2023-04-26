from typing import List

import duckdb

from ..utils import run_parallel
from .dataset import Dataset


def sync_datasets(dataset1: Dataset, dataset2: Dataset, delete: bool = True):
    def sync(key: str):
        m2[key] = m1[key]

    def del_(keys: List[str]):
        m2.delitems(keys)

    m1 = dataset1._filesystem.get_mapper(dataset1._full_path)
    m2 = dataset2._filesystem.get_mapper(dataset2._full_path)

    if dataset2.selected_file_details is None:
        new_files = dataset1.selected_file_details["path"].to_list()
    else:
        new_files = (
            duckdb.from_arrow(
                dataset1.selected_file_details.select(
                    ["path", "name", "size"]
                ).to_arrow()
            )
            .except_(
                duckdb.from_arrow(
                    dataset2.selected_file_details.select(
                        ["path", "name", "size"]
                    ).to_arrow()
                )
            )
            .pl()["path"]
            .to_list()
        )

    new_files = [f.split(dataset1._path)[1] for f in new_files]
    new_files = [f[1:] if f[0] == "/" else f for f in new_files]

    if len(new_files):
        _ = run_parallel(
            sync,
            new_files,
            backend="loky",
        )

    if delete and dataset2.selected_file_details is not None:
        rm_files = (
            duckdb.from_arrow(
                dataset2.selected_file_details.select(
                    ["path", "name", "size"]
                ).to_arrow()
            )
            .except_(
                duckdb.from_arrow(
                    dataset1.selected_file_details.select(
                        ["path", "name", "size"]
                    ).to_arrow()
                )
            )
            .pl()["path"]
            .to_list()
        )

        rm_files = [f.split(dataset2._path)[1] for f in rm_files]
        rm_files = [f[1:] if f[0] == "/" else f for f in rm_files]

        if len(rm_files):
            del_(keys=rm_files)
