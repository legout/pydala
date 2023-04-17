#%%
from fsspec import filesystem
import pyarrow.dataset as pds
import duckdb
import polars as pl
from joblib import Parallel, delayed
import tqdm

# from joblib.externals.loky import set_loky_pickler
# from joblib import wrap_non_picklable_objects
# import dill
# import cloudpickle


# set_loky_pickler('cloudpickle')
#%%
fs1 = filesystem(
    "s3", key="volker", secret="s78anwg9", endpoint_url="http://localhost:9000"
)
fs2 = filesystem(
    "s3",
    key="dac1464c34bd969beee7191d088482af",
    secret="740d61236ac60fe88e07c0b93377e2ea982e1e02e8baf38f01e34dc4a59c4958",
    endpoint_url="https://1dcf4b5bb5a92f314db2e3fef8573553.r2.cloudflarestorage.com",
)


con1 = duckdb.connect()
con2 = duckdb.connect()

con1.register_filesystem(fs1)
con2.register_filesystem(fs2)

dataset1 = pds.dataset(
    "yfin-db/history/daily", filesystem=fs1, partitioning=["exchange"]
)
table1 = dataset1.to_table()

con1.register("dataset", dataset1)
dataset2 = pds.dataset(
    "findata/history/daily", filesystem=fs2, partitioning=["exchange"]
)
con2.register("dataset", dataset2)

rel1 = con1.from_arrow(dataset1)
rel11 = con1.from_arrow(table1)
rel2 = con2.from_arrow(dataset2)
# %%

partitions1 = (
    rel1.project("strftime(time, '%Y') as _partition_")
    .distinct()
    .pl()["_partition_"]
    .to_list()
)
#%%
%memit aa=pl.scan_pyarrow_dataset(dataset1).with_columns([pl.col("time").dt.strftime("%Y").alias("_part_")]).collect(streaming=False).partition_by("_part_")

# %%
aa=[rel1.order("time,exchange,symbol").filter(f"strftime(time, '%Y')='{part}'").arrow() for part in partitions1]

# %%
%memit aa=pl.from_arrow(table1).with_columns([pl.col("time").dt.strftime("%Y").alias("_part_")]).partition_by("_part_")

# %%
