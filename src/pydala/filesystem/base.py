import os
from tempfile import mkdtemp

from fsspec import spec
from fsspec.utils import infer_storage_options
from pyarrow.fs import FileSystem

from .dirfs import fsspec_dir_filesystem, pyarrow_subtree_filesystem
from .fs import fsspec_filesystem, pyarrow_filesystem


class BaseFileSystem:
    def __init__(
        self,
        path: str,
        bucket: str | None = None,
        name: str | None = None,
        caching: bool = False,
        cache_storage: str | None = "/tmp/pydala/",
        protocol: str | None = None,
        profile: str | None = None,
        endpoint_url: str | None = None,
        storage_options: dict = {},
        fsspec_fs: spec.AbstractFileSystem | None = None,
        pyarrow_fs: FileSystem | None = None,
        use_pyarrow_fs: bool = False,
    ):
        self._name = name
        self._tables = dict()
        self._cached = False
        self._profile = profile
        self._endpoint_url = endpoint_url
        self._storage_options = storage_options
        self._use_pyarrow_fs = use_pyarrow_fs

        self._set_paths(
            path=path,
            bucket=bucket,
            protocol=protocol,
            caching=caching,
            cache_storage=cache_storage,
        )

        self._filesystem = self._get_filesystems(
            bucket=self._bucket,
            protocol=self._protocol,
            profile=self._profile,
            endpoint_url=self._endpoint_url,
            storage_options=self._storage_options,
            caching=self._caching,
            cache_bucket=self._cache_bucket,
            fsspec_fs=fsspec_fs,
            pyarrow_fs=pyarrow_fs,
            use_pyarrow_fs=self._use_pyarrow_fs,
        )
        self._set_filesystem()

    

    def _get_storage_path_options(self,
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

    def _set_paths(
        self,
        path: str,
        bucket: str | None,
        protocol: str | None,
        caching: bool,
        cache_storage: str | None,
    ):
        self._bucket, self._path, self._protocol = self._get_storage_path_options(
            bucket=bucket, path=path, protocol=protocol
        )

        self._caching = caching
        self._cache_storage = cache_storage

        if self._caching:

            if cache_storage:  # is not None:
                os.makedirs(cache_storage, exist_ok=True)
                self._cache_bucket = os.path.join(cache_storage, "cache")
            else:
                self._cache_bucket = mkdtemp()
        else:
            self._cache_bucket = None
            
    def _get_filesystems(
        self,
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


    def _set_filesystem(self):
        if self._cached:
            self._fs = self._filesystem["fsspec_cache"]
            if self._use_pyarrow_fs:
                self._pafs = self._filesystem["pyarrow_cache"]
            else:
                self._pafs = None
        else:
            self._fs = self._filesystem["fsspec_main"]
            if self._use_pyarrow_fs:
                self._pafs = self._filesystem["pyarrow_main"]
            else:
                self._pafs = None
    