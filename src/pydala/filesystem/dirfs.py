import copy
import os
import subprocess

import pyarrow.fs as pafs
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.spec import AbstractFileSystem
from fsspec.utils import infer_storage_options

from .base import fsspec_filesystem, pyarrow_filesystem


class DirFileSystem(DirFileSystem):
    def __init__(self, path, fs, *args, **storage_options):
        super().__init__(path, fs, *args, **storage_options)

    def ls(self, path, detail=False, **kwargs):
        ret = self.fs.ls(self._join(path), detail=detail, **kwargs)
        if detail:
            ret = copy.deepcopy(ret)
            for entry in ret:
                entry["name"] = self._relpath(entry["name"])
            return ret

        return self._relpath(ret)

    @staticmethod
    def _check_for_s5cmd() -> bool:
        """Check wether s5cmd is available of not"""
        res = subprocess.run("which s5cmd", shell=True, capture_output=True)
        return res.returncode == 0

    @property
    def has_s5cmd(self) -> bool:
        if not hasattr(self, "_has_s5cmd"):
            self._has_s5cmd = self._check_for_s5cmd()

        return self._has_s5cmd

    def sync(self, src: str, dest: str, recursive=True, exclude: str | None = None):
        if self.has_s5cmd:
            src_path = infer_storage_options(src)["path"]

            if self.isfile(src_path):
                dest = os.path.dirname(dest) + "/"
            else:
                dest = dest.lstrip("/") + "/"

            if recursive and not src.endswith("*"):
                src = src.lstrip("/") + "/*"

            if exclude is None:
                res = subprocess.run(
                    f"s5cmd sync {src} {dest}", shell=True, capture_output=True
                )
            else:
                res = subprocess.run(
                    f's5cmd sync --exclude "{exclude}" {src} {dest}',
                    shell=True,
                    capture_output=True,
                )

            #return res

    def s5cp(self, src: str, dest: str, recursive=True, exclude: str | None = None):
        if self.has_s5cmd:
            src_path = infer_storage_options(src)["path"]

            if self.isfile(src_path):
                dest = os.path.dirname(dest) + "/"
            else:
                dest = dest.lstrip("/") + "/"

            if recursive and not src.endswith("*"):
                src = src.lstrip("/") + "/*"

            if exclude is None:
                res = subprocess.run(
                    f"s5cmd cp {src} {dest}", shell=True, capture_output=True
                )
            else:
                res = subprocess.run(
                    f's5cmd cp --exclude "{exclude}" {src} {dest}',
                    shell=True,
                    capture_output=True,
                )
            #return res

    def s5mv(self, src: str, dest: str, recursive=True, exclude: str | None = None):
        if self.has_s5cmd:
            src_path = infer_storage_options(src)["path"]

            if self.isfile(src_path):
                dest = os.path.dirname(dest) + "/"
            else:
                dest = dest.lstrip("/") + "/"

            if recursive and not src.endswith("*"):
                src = src.lstrip("/") + "/*"

            if exclude is None:
                res = subprocess.run(
                    f"s5cmd mv {src} {dest}", shell=True, capture_output=True
                )
            else:
                res = subprocess.run(
                    f's5cmd mv --exclude "{exclude}" {src} {dest}',
                    shell=True,
                    capture_output=True,
                )
            #return res

    def invalidate_cache(self, path=None):
        return self.fs.invalidate_cache(path=path)


def fsspec_dir_filesystem(
    path: str,
    filesystem: AbstractFileSystem | None = None,
    protocol: str | None = None,
    profile: str | None = None,
    endpoint_url: str | None = None,
    **storage_options,
):

    filesystem = filesystem or fsspec_filesystem(
        protocol=protocol, profile=profile, endpoint_url=endpoint_url, **storage_options
    )
    return DirFileSystem(path=path, fs=filesystem)


def pyarrow_subtree_filesystem(
    path: str,
    filesystem: pafs.SubTreeFileSystem | None = None,
    protocol: str | None = None,
    profile: str | None = None,
    endpoint_url: str | None = None,
    **storage_options,
):
    filesystem = filesystem or pyarrow_filesystem(
        protocol=protocol, profile=profile, endpoint_url=endpoint_url, **storage_options
    )
    return pafs.SubTreeFileSystem(base_path=path, base_fs=filesystem)
