# Compare file downloads
# cancidates:
# - pyarrow.filesystem
# - s3fs
# - boto3
# - aioboto3

#%%

import s3fs
import pyarrow.fs as pafs
from fsspec.implementations.arrow import ArrowFSWrapper
import boto3
import aioboto3
from joblib import Parallel, delayed

from boto3.session import Session

import time
import asyncio
import progressbar
from pathlib import Path

#%%

bucket_name = "dswb-nes-data"
folder = "PU1/Aumann/traces"
local = "/home/ubuntu/data/"

# %%


session = Session()
s3 = session.resource("s3")
bucket = s3.Bucket(bucket_name)
client = boto3.client("s3")


filenames = [x.key for x in bucket.objects.filter(Prefix=folder)]

#%%

async def download_file_async(client, bucket_name, filename):
    local_path = Path(local) / filename
    local_path.parent.mkdir(exist_ok=True, parents=True)
    _ = await client.download_file(
        Bucket=bucket_name,
        Key=filename,
        Filename=local + filename,
    )
    
def download_file_sync(session, bucket_name, filename):
    #session = Session()

    client = session.client("s3")
    local_path = Path(local) / filename
    local_path.parent.mkdir(exist_ok=True, parents=True)
    _ = client.download_file(
        Bucket=bucket_name,
        Key=filename,
        Filename=local + filename,
    )


#%%
async def download_files_async(fns):
    session = aioboto3.Session()
    async with session.client("s3") as s3:
        tasks = [asyncio.create_task(download_file_async(s3, bucket_name, fn)) for fn in fns]

        _ = [
            await task
            for task in progressbar.progressbar(
                asyncio.as_completed(tasks), max_value=len(fns)
            )
        ]


def download_files_sync(fns):
    session = Session()

    _ = Parallel(verbose=10, n_jobs=-1)(delayed(download_file_sync)(session, bucket_name, fn) for fn in fns)


#%%
await download_files_async(filenames[:2500])

#%%
download_files_sync(filenames[:2500])

# %%

fs1 = s3fs.S3FileSystem()
fs2 = ArrowFSWrapper(pafs.S3FileSystem())
# %%

def download_file_sync2(filename):
    local_path = Path(local) / filename
    local_path.parent.mkdir(exist_ok=True, parents=True)
    
    fs1.get_file(bucket_name + "/" + filename, local_path)
    
def download_files_sync2(fns, backend="loky"):
    _ = Parallel(backend=backend, verbose=10, n_jobs=-1)(delayed(download_file_sync2)(fn) for fn in fns)

def download_file_sync3(filename):
    local_path = Path(local) / filename
    local_path.parent.mkdir(exist_ok=True, parents=True)
    
    fs1.get_file(bucket_name + "/" + filename, local_path)
    
def download_files_sync3(fns, backend="loky"):
    _ = Parallel(backend=backend, verbose=10, n_jobs=-1)(delayed(download_file_sync3)(fn) for fn in fns)

# %%


from mindsphere_tools.loginmanager import AwsCredentialsManager, load_credentials
import os
import s3fs

credentials = load_credentials()

aws = AwsCredentialsManager(**credentials)


# %%
url = aws._gateway + "datalake/v3/generateAccessCredentials"
data = {
  "durationSeconds": 43200,
  "accesses": [
    {
      "path": "/",
      "permissions": [
        "READ",
        "WRITE",
        "DELETE"
      ]
    }
  ]
}

resp = aws.client.post(url=url, json=data, headers=aws._headers)
# %%
aws_credentials = resp.json()["credentials"]
bucket = os.path.join(resp.json()["storageAccount"], resp.json()["accesses"][0]["path"])
# %%
fs = s3fs.S3FileSystem(anon=False, key=aws_credentials["accessKeyId"], secret=aws_credentials["secretAccessKey"], token=aws_credentials["sessionToken"])
# %%
