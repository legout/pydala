# %%
# %load_ext memory_profiler
import sys

sys.path.append("/Volumes/WD_Blue_1TB/coding/libs/pydala/")

import tqdm
from src.pydala.dataset.reader import dataset
from src.pydala.dataset.writer import Writer
import duckdb
from src.pydala.dataset.utils.table import (
    to_relation,
    to_arrow,
    partition_by,
    with_strftime_column,
)
from src.pydala.utils import run_parallel
from joblib import Parallel, delayed

ddb = duckdb.connect()

# %%

ds1 = dataset(
    path="history/daily",
    bucket="yfin-db",
    protocol="s3",
    key="volker",
    secret="s78anwg9",
    endpoint_url="http://localhost:9000",
    partitioning="hive",
    #time_range="2022-01-01",
    ddb=ddb,
    materialize=False,
)
ds2 = dataset(
    path="history2/daily",
    bucket="yfin-db",
    protocol="s3",
    key="volker",
    secret="s78anwg9",
    endpoint_url="http://localhost:9000",
    #partitioning=["exchange", "year"],
    partitioning="hive",
    #time_range="2022-01-01",
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
    table=ds2.arrow_table,
    path="history/daily",
    bucket="yfin-db",
    protocol="s3",
    key="volker",
    secret="s78anwg9",
    endpoint_url="http://localhost:9000",
    format="parquet",
    mode="delta",
    timestamp_column="time",
    partitioning=["exchange", "year"],
    ddb=ddb,
)
# %%
writer.write(
    batch_size=1_000_000,
    sort_by=["time", "symbol"],
    distinct=True,
    ascending=True,
    presort=False,
    row_group_size=150_000,
    partition_flavor="hive",
    preload_partitions=True,
    compression="zstd"
)

# %%
ds3 = dataset(
    path="history/daily",
    bucket="yfin-db",
    protocol="s3",
    key="dac1464c34bd969beee7191d088482af",
    secret="740d61236ac60fe88e07c0b93377e2ea982e1e02e8baf38f01e34dc4a59c4958",
    endpoint_url="https://1dcf4b5bb5a92f314db2e3fef8573553.r2.cloudflarestorage.com",
    #partitioning=["exchange", "year"],
    partitioning="hive",
    #time_range="2022-01-01",
    ddb=ddb,
    materialize=False,
)
#%%
writer = Writer(
    table=ds1.arrow_table,
    path="history/daily",
    bucket="yfin-db",
    protocol="s3",
    key="dac1464c34bd969beee7191d088482af",
    secret="740d61236ac60fe88e07c0b93377e2ea982e1e02e8baf38f01e34dc4a59c4958",
    endpoint_url="https://1dcf4b5bb5a92f314db2e3fef8573553.r2.cloudflarestorage.com",
    format="parquet",
    mode="overwrite",
    timestamp_column="time",
    partitioning=["exchange", "year"],
    ddb=ddb,
    s3_additional_kwargs=dict(ACL="private")
)
#%%
writer.write(
    batch_size=1_000_000,
    sort_by=["time", "symbol"],
    distinct=True,
    ascending=True,
    presort=False,
    row_group_size=150_000,
    partition_flavor="hive",
    preload_partitions=True,
    compression="zstd"
)
# %%
