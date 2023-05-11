# %%
# %load_ext memory_profiler
import sys

# sys.path.append("/Volumes/WD_Blue_1TB/coding/libs/pydala/")
# sys.path.append("/home/ubuntu/myPython/pydala/")


import tqdm
from pydala.dataset.reader import dataset
from pydala.dataset.writer import Writer
import duckdb
from pydala.dataset.utils.table import (
    to_relation,
    to_arrow,
    partition_by,
    with_strftime_column,
)
from pydala.utils import run_parallel
from joblib import Parallel, delayed

ddb = duckdb.connect()

# %%

ds = dataset(
    path="EWN/MMS/raw/val/APL_TYP=SC",
    bucket="dswb-nes-data",
    protocol="s3",
    # partitioning=["exchange"],
    # time_range="2022-01-01",
    ddb=ddb,
    materialize=False,
)


# partitions = ds._partition_by(
#     which="arrow_dataset",
#     columns=["exchange", "year"],
#     sort_by=["time", "symbol"],
#     distinct=True,
#     ascending=True,
#     as_dict=True,
#     drop=True
# )

# _ = [(n, to_arrow(p)) for n, p in tqdm.tqdm(partitions)]

##%%


# %%

writer = Writer(
    table=ds.arrow_table,
    path="EWN/MMS/raw/val4",
    bucket="dswb-nes-data",
    protocol="s3",
    format="parquet",
    mode="append",
    timestamp_column="AE_DATUM",
    partitioning=["APL_TYP", "year", "month"],
    partitioning_flavor="hive",
    ddb=ddb,
)
# %%
writer.write(
    batch_size=1_000_000,
    # sort_by=["time", "symbol"],
    # distinct=True,
    # ascending=True,
    # presort=True,
    row_group_size=150_000,
    # preload_partitions=True
)

# %%
