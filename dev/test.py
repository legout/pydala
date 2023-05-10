# %%
#
#%load_ext memory_profiler
import sys

sys.path.append("/Volumes/WD_Blue_1TB/coding/libs/pydala/")

# %%
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
from joblib import wrap_non_picklable_objects, Parallel, delayed


def run_test():
    ds = dataset(
        path="history/daily",
        bucket="yfin-db",
        protocol="s3",
        key="volker",
        secret="s78anwg9",
        endpoint_url="http://localhost:9000",
        partitioning=["exchange"],
        #time_range="2022-01-01",
        ddb=ddb,
        materialize=True,
    )

    partitions = partition_by(
        with_strftime_column(
            ds.ddb_rel, timestamp_column="time", strftime=["%Y"], column_names=["year"]
        ),
        columns=["exchange", "year"],
        sort_by=["time", "symbol"],
        distinct=True,
        ascending=True,
        as_dict=True,
        drop=True,
    )
    
    @delayed
    @wrap_non_picklable_objects
    def get_part(part):
        part[0], to_arrow(part[1])

    #_ = [(n, to_arrow(p)) for n, p in tqdm.tqdm(partitions)]
    _ = Parallel(n_jobs=-1, backend="loky")(delayed(get_part)(part) for part in tqdm.tqdm(partitions))

#%%


#%%

# writer = Writer(
#     table=ds.arrow_table,
#     path="history2/daily",
#     bucket="yfin-db",
#     protocol="s3",
#     key="volker",
#     secret="s78anwg9",
#     endpoint_url="http://localhost:9000",
#     format="parquet",
#     mode="delta",
#     timestamp_column="time",
#     partitioning=["exchange", "year"],
#     ddb=ddb,
# )
# # %%
# writer.write(batch_size=1_000_000, preload_partitions=True)

# # %%
