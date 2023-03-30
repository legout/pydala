import logging
import random
import string
import sys
from loguru import logger
from typing import List, Tuple, Callable, Any
from joblib import Parallel, delayed


def get_logger(name: str, log_file: str):
    logger
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)

    file_handler = handlers.TimedRotatingFileHandler(
        log_file, when="D", interval=1, backupCount=2
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

    return logger


def sort_as_sql(
    sort_by: str | List[str], ascending: bool | List[bool] | None = None
) -> str:
    """Generats a SQL string for the given columns.

    Args:
        sort_by (str | List[str]): Columns to sort.
        ascending (bool | List[bool] | None, optional): Wheter to sort
        ascending or descending. Defaults to None.

    Returns:
        str: SQL string
    """
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


def random_id() -> str:
    "Returns a random id."
    alphabet = string.ascii_lowercase + string.digits
    return "".join(random.choices(alphabet, k=8))


def humanize_size(size: int, unit="MB") -> float:
    "Human-readable size"
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


def run_parallel(
    func: Callable,
    func_params: list | List[List] | List[Tuple],
    *args,
    n_jobs: int = -1,
    backend: str = "loky",
    **kwargs,
)->List[Any]:
    """Runs a function for a list of parameters in parallel.

    Args:
        func (Callable): function to run in parallel.
        func_params (list | List[List] | List[Tuple]): parameters for the function
        n_jobs (int, optional): Number of joblib workers. Defaults to -1.
        backend (str, optional): joblib backend. Valid options are
        `loky`,`threading`, `mutliprocessing` or `sequential`.  Defaults to "loky".

    Returns:
        List[Any]: function output.
    """
    res = Parallel(n_jobs=n_jobs, backend=backend)(
        delayed(func)(fp, *args, **kwargs) for fp in func_params
    )
    return res
