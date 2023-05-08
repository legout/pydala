# %%
#
import sys

sys.path.append("/Volumes/WD_Blue_1TB/coding/libs/pydala/")

#%%
from src.pydala.dataset.reader import dataset
from src.pydala.dataset.writer import Writer
import duckdb
from src.pydala.dataset.utils.table import to_relation

ddb = duckdb.connect()

#%%
ds = dataset(
    path="history/daily",
    bucket="yfin-db",
    protocol="s3",
    key="volker",
    secret="s78anwg9",
    endpoint_url="http://localhost:9000",
    partitioning=["exchange"],
    time_range="2022-01-01",
    ddb=ddb
    materialize=True
)


writer = Writer(
    table=ds.ddb_rel,
    path="history2/daily",
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
#%%
writer.write(batch_size=1_000_000, preload_partitions=True)
# %%

