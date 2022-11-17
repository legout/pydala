from fsspec import spec
from fsspec.utils import infer_storage_options
from pyarrow.fs import FileSystem

from ..filesystem.base import fsspec_filesystem, pyarrow_filesystem
from ..filesystem.dirfs import fsspec_dir_filesystem, pyarrow_subtree_filesystem


def get_filesystem(
    bucket: str | None,
    protocol: str,
    profile: str | None,
    endpoint_url: str | None,
    storage_options: dict | None,
    caching: bool,
    cache_bucket: str | None,
    fsspec_fs: spec.AbstractFileSystem | None,
    pyarrow_fs: FileSystem | None,
    use_pyarrow_fs: bool = False,
):

    filesystem = {}

    if fsspec_fs:
        filesystem["fsspec_main"] = fsspec_fs
    else:
        filesystem["fsspec_main"] = fsspec_filesystem(
            protocol=protocol,
            profile=profile,
            endpoint_url=endpoint_url,
            **storage_options,
        )
    if use_pyarrow_fs:
        if pyarrow_fs:
            filesystem["pyarrow_main"] = pyarrow_fs
        else:
            filesystem["pyarrow_main"] = pyarrow_filesystem(
                protocol=protocol,
                endpoint_url=endpoint_url,
                **storage_options,
            )

    if bucket:
        if hasattr(filesystem["fsspec_main"], "path"):
            filesystem["fsspec_main"] = fsspec_dir_filesystem(
                path=bucket, filesystem=filesystem["fsspec_main"].fs
            )
        else:
            filesystem["fsspec_main"] = fsspec_dir_filesystem(
                path=bucket, filesystem=filesystem["fsspec_main"]
            )
        if use_pyarrow_fs:
            if hasattr(filesystem["pyarrow_main"], "base_path"):
                filesystem["pyarrow_main"] = pyarrow_subtree_filesystem(
                    path=bucket, filesystem=filesystem["pyarrow_main"].base_fs
                )
            else:
                filesystem["pyarrow_main"] = pyarrow_subtree_filesystem(
                    path=bucket, filesystem=filesystem["pyarrow_main"]
                )

    if caching:
        cache_bucket = cache_bucket or ""
        filesystem["fsspec_cache"] = fsspec_dir_filesystem(
            path=cache_bucket,
            filesystem=fsspec_filesystem(protocol="file"),
        )
        if use_pyarrow_fs:
            filesystem["pyarrow_cache"] = pyarrow_subtree_filesystem(
                path=cache_bucket,
                filesystem=pyarrow_filesystem(protocol="file"),
            )
    return filesystem


def get_storage_path_options(
    bucket: str | None, path: str | None, protocol: str | None
):
    if bucket:
        protocol = protocol or infer_storage_options(bucket)["protocol"]
        bucket = infer_storage_options(bucket)["path"]
    else:
        bucket = None
        protocol = protocol or infer_storage_options(path)["protocol"]

    path = infer_storage_options(path)["path"]

    return bucket, path, protocol
