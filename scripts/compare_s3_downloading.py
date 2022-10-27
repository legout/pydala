#%%
import os
import subprocess
import time
from tempfile import mkdtemp

import aiobotocore
import boto3
import nest_asyncio
import pyarrow.fs as pafs
import s3fs
from fsspec.implementations.arrow import ArrowFSWrapper
from mindsphere_tools.datalake_filesystem import Client

client = Client()

nest_asyncio.apply()

os.environ["AWS_PROFILE"] = "mdsp"

bucket = "datalake-prod-a-sagewn-1595245204497"


folder_with_many_files = "data/ten=sagewn/PU1/Aumann/results"
big_file = "data/ten=sagewn/EWN/Prediction_Turnover_R2P_2301.parquet"

fs1 = s3fs.S3FileSystem()
fs2 = pafs.S3FileSystem()


def timer_func(func):
    # This function shows the execution time of 
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time.monotonic()
        result = func(*args, **kwargs)
        t2 = time.monotonic()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func


#%%

## 1. S5Cmd

@timer_func
def download_s5cmd(src:str, dest:str):
    if not "." in src.split("/")[-1]:
        src = src + "/*"
    if not src.startswith("s3"):
        src = "s3://"+src
    res = subprocess.run(
        f"s5cmd cp {src}  {dest}/",
        shell=True,
        capture_output=True,
    )
    print(f"Downloaded to {dest}")
    return res

res = download_s5cmd(os.path.join(bucket, folder_with_many_files), mkdtemp())



# RESULT: Takes about 8-9 seconds
#%%

res= download_s5cmd(os.path.join(bucket, big_file),  mkdtemp())


# RESULT: Takes about 1 second
#%%
@timer_func
def download_s3fs(src:str, dest:str):
    if not "." in src.split("/")[-1]:
        recursive=True
    else:
        recursive=False
        dest = os.path.join(dest, src.split("/")[-1])
    fs1.get(src, dest, recursive=recursive)

# RESULT: Took about 1:45 minutes

@timer_func
def download_pafs(src:str, dest:str):
    if not "." in src.split("/")[-1]:
        recursive=True
    else:
        recursive=False
        dest = os.path.join(dest, src.split("/")[-1])
    ArrowFSWrapper(fs2).get(src, dest, recursive=recursive)
# RESULT: Manually stopped after ~ 4 minutes

#%%

import polars as pl
import pyarrow.dataset as ds
import pyarrow.parquet as pq

#from metaflow import s3

@timer_func
def pq_table(src, filesystem=None):
    if filesystem is None:
        src = "s3://" + src
    pq.read_table(src, filesystem=filesystem)

def pa_ds(src, filesystem=None, polars=False):
    if filesystem is None:
        src = "s3://" + src
    dataset = ds.dataset(src, filesystem=filesystem)
    if polars:
        pl.scan_ds(dataset).collect().to_arrow()
    else:
        dataset.to_table()

#def load_parquet(bucket, path):
#    with S3(s3root=bucket) as s3:
#        tmp_data_path = s3.get_many(path)
#        first_path = tmp_data_path[0].path
#        self.table = pq.read_table(first_path)

# RESULTS: SLOOOOWWW

import os
#%%
# 3. Concurrent Download with Mutliprocessing
from concurrent.futures import (ProcessPoolExecutor, ThreadPoolExecutor,
                                as_completed)
from functools import partial

import boto3
import tqdm

AWS_BUCKET = bucket
OUTPUT_DIR = mkdtemp()

def download_one_file(bucket: str, output: str, client: boto3.client, s3_file: str):
    """
    Download a single file from S3
    Args:
        bucket (str): S3 bucket where images are hosted
        output (str): Dir to store the images
        client (boto3.client): S3 client
        s3_file (str): S3 object name
    """
    client.download_file(
        Bucket=bucket, Key=s3_file, Filename=os.path.join(output, s3_file.split("/")[-1])
    )


files_to_download = [f.replace(bucket, "").lstrip("/") for f in fs1.ls(os.path.join(bucket, folder_with_many_files))][:2000]
#files_to_download = [r["Key"] for r in client2.list_objects_v2(Bucket=bucket, Prefix="data/ten=sagewn/PU1/Aumann/results")["Contents"]]
# Creating only one session and one client
session = boto3.Session(profile_name="mdsp")
client = session.client("s3")
# The client is shared between threads
func = partial(download_one_file, AWS_BUCKET, OUTPUT_DIR, client)

# List for storing possible failed downloads to retry later
failed_downloads = []
#%%
print(OUTPUT_DIR)
with tqdm.tqdm(desc="Downloading images from S3", total=len(files_to_download)) as pbar:
    with ProcessPoolExecutor(max_workers=16) as executor:
        # Using a dict for preserving the downloaded file for each future, to store it as a failure if we need that
        futures = {
            executor.submit(func, file_to_download): file_to_download for file_to_download in files_to_download
        }
        for future in as_completed(futures):
            if future.exception():
                failed_downloads.append(futures[future])
            pbar.update(1)

            
#%%            
if len(failed_downloads) > 0:
    print("Some downloads have failed. Saving ids to csv")
    with open(
        os.path.join(OUTPUT_DIR, "failed_downloads.csv"), "w", newline=""
    ) as csvfile:
        wr = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
        wr.writerow(failed_downloads)

#%%
# 4. Concurrent Download with Threading

#%%
# 5. Concurrent Download with Threading (TransferConfig)

from multiprocessing import Pool

import boto3

session = boto3.Session(profile_name="mdsp")

s3r = session.resource('s3')

my_bucket = s3r.Bucket(bucket)

objects_to_download = []
for obj in my_bucket.objects.filter(Prefix=folder_with_many_files):    
        objects_to_download.append((my_bucket.name, obj.key))
#%%
tempdir=mkdtemp()
#print(objects_to_download)
#%%
def s3_downloader(s3_object_tuple):
    my_bucket, my_object = s3_object_tuple
    s3_object = s3r.Object(my_bucket, my_object)
    out_file = os.path.join(tempdir, my_object.split("/")[-1])
    #print(f'Downloading s3://{my_bucket}/{my_object} to {out_file}')
    s3_object.download_file(out_file)
    #print(f'Downloading finished s3://{my_bucket}/{my_object}')
    
with Pool(8) as p:
    p.map(s3_downloader, objects_to_download)




from concurrent import futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

#%%
import boto3.session

KEYS_TO_DOWNLOAD = ["data/ten=sagewn/" + f for f in client.ls("PU1/Aumann/results")][:2500] # all the files that you want to download
BUCKET_NAME = bucket


#%%

def download_object(file_name):
    """Downloads an object from S3 to local."""

    s3_client = boto3.client("s3")
    download_path = Path(LOCAL_DOWNLOAD_PATH) / file_name.split("/")[-1]
    print(f"Downloading {file_name} to {download_path}")
    s3_client.download_file(
        BUCKET_NAME,
        file_name,
        str(download_path)
    )
    return "Success"

def download_parallel_multiprocessing():

    #session = boto3.Session(profile_name="mdsp")
    #s3_client = session.client("s3", region_name="eu-central-1")
    
    with ProcessPoolExecutor() as executor:
        future_to_key = {executor.submit(download_object, key): key for key in KEYS_TO_DOWNLOAD}

        for future in futures.as_completed(future_to_key):
            key = future_to_key[future]
            exception = future.exception()

            if not exception:
                yield key, future.result()
            else:
                yield key, exception

if __name__ == "__main__":
    for key, result in download_parallel_multiprocessing():
        print(f"{key}: {result}")

#%%
LOCAL_DOWNLOAD_PATH = mkdtemp()
def download_object(s3_client, file_name):
    """Downloads an object from S3 to local."""

    #s3_client = boto3.client("s3")
    download_path = Path(LOCAL_DOWNLOAD_PATH) / file_name.split("/")[-1]
    print(f"Downloading {file_name} to {download_path}")
    s3_client.download_file(
        BUCKET_NAME,
        file_name,
        str(download_path)
    )
    return "Success"

def download_parallel_multiprocessing():

    session = boto3.Session(profile_name="mdsp")
    s3_client = session.client("s3", region_name="eu-central-1")
    
    with ThreadPoolExecutor() as executor:
        future_to_key = {executor.submit(download_object, s3_client, key): key for key in KEYS_TO_DOWNLOAD}

        for future in futures.as_completed(future_to_key):
            key = future_to_key[future]
            exception = future.exception()

            if not exception:
                yield key, future.result()
            else:
                yield key, exception

if __name__ == "__main__":
    for key, result in download_parallel_multiprocessing():
        print(f"{key}: {result}")
#%%
# 6. Concurrent Download with Dask Delayed
# Can use processes and threads
# Maybe test with run_on_ray

import os
from functools import partial

import boto3
import botocore
import dask
from dask.diagnostics import ProgressBar

# playing with the number of threads can increase / decrease the throughput
dask.config.set(scheduler='processes', num_workers=8)


def _s3_download(session, path, bucket, key):
    """wrapper to avoid crushing on not found objects
    s3_client: s3 resource service client
    path: path to store the downloaded file
    bucket: bucket in which to find the file
    key: key of the file
    """
    try:
        s3_client = session.client("s3", region_name="eu-central-1")

        s3_client.download_file(bucket,
            key, os.path.join(path, key.split("/")[-1])
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print('The object does not exist')
        else:
            raise


def fetch_multiple(profile_name, bucket, keys,
                   path):
    """Initialise an s3 client Session and download a list of files
    aws_access_key_id: access key
    aws_secret_access_key: secret key
    bucket: s3 bucket where the files are stored
    keys: list of keys to download
    """
    session = boto3.Session(
        #aws_access_key_id=aws_access_key_id,
        #aws_secret_access_key=aws_secret_access_key,
        profile_name=profile_name
    )

    #s3 = session.client("s3", region_name="eu-central-1")

    _download = partial(_s3_download, session, path, bucket)

    delayed_futures = []
    for k in keys:
        delayed_futures.append(dask.delayed(_download)(k))

    with ProgressBar():
        dask.compute(*delayed_futures)




#%%
# 7. Concurrent Download with Joblib
# Can use processes, threads and its own loky


# %%
# 8. Try smart_open
from smart_open import s3

# we use workers=1 for reproducibility; you should use as many workers as you have cores
# bucket = 'silo-open-data'
# prefix = 'Official/annual/monthly_rain/'
dd = []
for key, content in s3.iter_bucket(bucket, prefix=folder_with_many_files,  workers=8):
    #print(key)
    dd.append(content) 

#%%
# 9. aiobotocore

# import asyncio
# from itertools import chain
# import json
# from typing import List
# from json.decoder import WHITESPACE
# import logging
# from functools import partial

# # Third Party
# import asyncpool
# import aiobotocore.session
# import aiobotocore.config

# _NUM_WORKERS = 50


# # def iterload(string_or_fp, cls=json.JSONDecoder, **kwargs):
# #     # helper for parsing individual jsons from string of jsons (stolen from somewhere)
# #     string = str(string_or_fp)

# #     decoder = cls(**kwargs)
# #     idx = WHITESPACE.match(string, 0).end()
# #     while idx < len(string):
# #         obj, end = decoder.raw_decode(string, idx)
# #         yield obj
# #         idx = WHITESPACE.match(string, end).end()


# async def get_object(s3_client, bucket: str, key: str):
#     # Get json content from s3 object

#     # get object from s3
#     response = await s3_client.get_object(Bucket=bucket, Key=key)
#     async with response['Body'] as stream:
#         content = await stream.read()

#     return list(content.decode())


# async def go(bucket: str, prefix: str) -> List[dict]:
#     """
#     Returns list of dicts of object contents

#     :param bucket: s3 bucket
#     :param prefix: s3 bucket prefix
#     :return: list of dicts of object contents
#     """
#     logging.basicConfig(level=logging.INFO)
#     logger = logging.getLogger()

#     session = aiobotocore.session.AioSession(profile="mdsp")
#     config = aiobotocore.config.AioConfig(max_pool_connections=_NUM_WORKERS)
#     contents = []
#     async with session.create_client('s3', config=config) as client:
#         worker_co = partial(get_object, client, bucket)
#         async with asyncpool.AsyncPool(None, _NUM_WORKERS, 's3_work_queue', logger, worker_co,
#                                        return_futures=True, raise_on_join=True, log_every_n=10) as work_pool:
#             # list s3 objects using paginator
#             paginator = client.get_paginator('list_objects')
#             async for result in paginator.paginate(Bucket=bucket, Prefix=prefix):
#                 for c in result.get('Contents', []):
#                     contents.append(await work_pool.push(c['Key']))

#     # retrieve results from futures
#     contents = [c.result() for c in contents]
#     return list(chain.from_iterable(contents))


# _loop = asyncio.get_event_loop()
# _result = _loop.run_until_complete(go(bucket, folder_with_many_files))

# COMMENT: To slow due to the slow paginator (lists only 1000 objects during each iteration)

# %%
# 10. metaflow
