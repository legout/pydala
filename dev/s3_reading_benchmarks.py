# %%
import s3fs
import pyarrow.parquet as pq
import pyarrow.dataset as pds
import duckdb
import polars as pl

fs = s3fs.S3FileSystem()
con = duckdb.connect()
con.register_filesystem(fs)

# %%

big_file = "s3://dswb-nes-data/PTO/data/final/data.parquet"
some_big_files = "s3://dswb-nes-data/PTO/data/raw/copa/"
many_smaller_files = "s3://dswb-nes-data/EWN/MMS/stage1/AP/current/"
# %%
df_lazy_arrow1 = pds.dataset(many_smaller_files)
#%%
df_lazy_pl1 = pl.concat([pl.scan_parquet("s3://"+f) for f in fs.glob(many_smaller_files)], how="diagonal")
#%%
df_lazy_pl2 = pl.scan_pyarrow_dataset(df_lazy_arrow1)
#%%
df_lazy_duck1a = con.read_parquet(many_smaller_files+"/*")
df_lazy_duck1b = con.from_parquet(many_smaller_files + "/*")
df_lazy_duck2a = con.from_arrow(df_lazy_arrow1)
df_lazy_duck2b = con.from_query("SELECT * FROM df_lazy_arrow1")
#df_lazy_duck3 = con.from_query("SELECT * FROM df_lazy_pl1")
df_lazy_duck4 = con.from_query("SELECT * FROM df_lazy_pl2")

#%%
%time aa=pl.from_arrow(df_lazy_arrow1.to_table())

#%%
%time aa=df_lazy_pl1.collect()

#%%
%time aa=df_lazy_pl2.collect()

#%%
%time aa=df_lazy_pl1.filter(pl.col("CreationDate")>'20220101').collect()

#%%
%time aa=df_lazy_pl2.filter(pl.col("CreationDate")>'20220101').collect()

# %%
%time aa=df_lazy_duck1a.pl()
#%%
%time aa=df_lazy_duck1b.pl()
#%%
%time aa=df_lazy_duck2a.pl()
#%%
%time aa=df_lazy_duck2b.pl()
#%%
%time aa=df_lazy_duck3.pl()
#%%
%time aa=df_lazy_duck4.pl()

# %%
%time aa=df_lazy_duck1a.filter("CreationDate>'20220101'").pl()
#%%
%time aa=df_lazy_duck1b.filter("CreationDate>'20220101'").pl()
#%%
%time aa=df_lazy_duck2a.filter("CreationDate>'20220101'").pl()
#%%
%time aa=df_lazy_duck2b.filter("CreationDate>'20220101'").pl()
#%%
%time aa=df_lazy_duck3.filter("CreationDate>'20220101'").pl()
#%%
%time aa=df_lazy_duck4.filter("CreationDate>'20220101'").pl()



#%%
import polars as pl
import pyarrow as pa

def pl_dtype_to_pa(dtype:pl.DataType)->pa.lib.DataType:
    from polars.utils import convert
    return convert.dtype_to_arrow_type(dtype)
    
    

def pa_dtype_to_pl(dtype:pa.lib.DataType)->pl.DataType:
    pa_dtype_to_pl = {
        pa.int8(): pl.Int8(),
        pa.int16(): pl.Int16(),
        pa.int32(): pl.Int32(),
        pa.int64(): pl.Int64(),
        pa.uint8(): pl.UInt8(),
        pa.uint16(): pl.UInt16(),
        pa.uint32(): pl.UInt32(),
        pa.uint64(): pl.UInt64(),
        pa.float16(): pl.Float32(),
        pa.float32(): pl.Float32(),
        pa.float64(): pl.Float64(),
        pa.bool_(): pl.Boolean(),
        pa.large_utf8(): pl.Utf8(),
        pa.utf8(): pl.Utf8(),
        pa.date32(): pl.Date(),
        pa.timestamp("us"): pl.Datetime("us"),
        pa.timestamp("ms"): pl.Datetime("ms"),
        pa.timestamp("us"): pl.Datetime("us"),
        pa.timestamp("ns"): pl.Datetime("ns"),
        pa.duration("us"): pl.Duration("us"),
        pa.duration("ms"): pl.Duration("ms"),
        pa.duration("us"): pl.Duration("us"),
        pa.duration("ns"): pl.Duration("ns"),
        pa.time64("us"): pl.Time(),
        pa.null(): pl.Null(),
    }
    tz = None
    if isinstance(dtype, pa.lib.TimestampType):
        dtype, tz = pa.timestamp(dtype.unit), dtype.tz
        
    pl_dtype = pa_dtype_to_pl[dtype]
    if tz:
        pl_dtype.tz=tz
        
    return pl_dtype
    
        
        
        
# %%
