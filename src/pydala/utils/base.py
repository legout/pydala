import logging
import os
import random
import string
import sys
from logging import handlers
from typing import Any

import rtoml
from fsspec import spec
from pyarrow.fs import FileSystem


def get_logger(name: str, log_file: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    # stdout_handler.setFormatter(formatter)

    # ath(log_file).mkdir(parents=True, exist_ok=True)
    # file_handler = logging.FileHandler(log_file)
    file_handler = handlers.TimedRotatingFileHandler(
        log_file, when="D", interval=1, backupCount=2
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

    return logger


def get_ddb_sort_str(sort_by: str | list, ascending: bool | list | None = None) -> str:
    ascending = ascending or True
    if isinstance(sort_by, list):

        if isinstance(ascending, bool):
            ascending = [ascending] * len(sort_by)

        sort_by_ddb = [
            f"{col} ASC" if asc else f"{col} DESC"
            for col, asc in zip(sort_by, ascending)
        ]
        sort_by_ddb = ",".join(sort_by_ddb)

    else:
        sort_by_ddb = sort_by + " ASC" if ascending else sort_by + " DESC"

    return sort_by_ddb


def random_id():
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choices(alphabet, k=8))


def convert_size_unit(size, unit="MB"):
    if unit == "B":
        return round(size, 1)
    elif unit == "KB":
        return round(size / 1024, 1)
    elif unit == "MB":
        return round(size / 1024**2, 1)
    elif unit == "GB":
        return round(size / 1024**3, 1)
    elif unit == "TB":
        return round(size / 1024**4, 1)
    elif unit == "PB":
        return round(size / 1024**5, 1)


class NestedDictReplacer:
    def __init__(self, d: dict) -> None:
        self._d = d

    def _dict_replace_value(self, d: dict, old: str | None, new: str | None) -> dict:
        x = {}
        for k, v in d.items():
            if isinstance(v, dict):
                v = self._dict_replace_value(v, old, new)
            elif isinstance(v, list):
                v = self._list_replace_value(v, old, new)
            else:
                v = v if v != old else new
            x[k] = v
        return x

    def _list_replace_value(self, l: list, old: str | None, new: str | None) -> list:
        x = []
        for e in l:
            if isinstance(e, list):
                e = self._list_replace_value(e, old, new)
            elif isinstance(e, dict):
                e = self._dict_replace_value(e, old, new)
            else:
                e = e if e != old else new
            x.append(e)
        return x

    def replace(self, old: str | None, new: str | None) -> dict:
        d = self._d
        return self._dict_replace_value(d, old, new)


def read_toml(path: str, filesystem: FileSystem | spec.AbstractFileSystem) -> dict:
    if filesystem.exists(path):
        with filesystem.open(path, "r") as f:
            return NestedDictReplacer(rtoml.load(f)).replace("None", None)
    raise OSError(f"path {path} not exists.")


def write_toml(
    config: dict,
    path: str,
    filesystem: FileSystem | spec.AbstractFileSystem,
    pretty: bool = False,
) -> None:

    if not filesystem.exists(path):
        try:
            filesystem.mkdirs(path=os.path.dirname(path), exist_ok=True)
        except PermissionError:
            pass

    with filesystem.open(path, "w") as f:
        rtoml.dump(
            NestedDictReplacer(config).replace(None, "None"),
            f,
            pretty=pretty,
        )


def create_nested_dict(key: str, val: Any, sep: str = ".") -> dict:
    if sep not in key:
        return {key: val}
    key, new_key = key.split(sep, 1)
    return {key: create_nested_dict(new_key, val, sep)}


# def get_
