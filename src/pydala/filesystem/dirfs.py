import copy
import os
import subprocess

import pyarrow.fs as pafs
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.spec import AbstractFileSystem
from fsspec.utils import infer_storage_options


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

    def sync(self, src: str, dest: str, recursive=True):
        if self.has_s5cmd:
            # src_protocol = infer_storage_options(src)["protocol"]
            src_path = infer_storage_options(src)["path"]
            if src_path == "file":
                src_path = ""

            # dest_protocol = infer_storage_options(src)["protocol"]
            dest_path = infer_storage_options(src)["path"]
            if dest_path == "file":
                dest_path = ""

            if self.isfile(src_path):
                dest_path = os.path.dirname(dest_path)
            else:

                if recursive and not src.endswith("*"):
                    src = src.lstrip("/") + "/*"

            res = subprocess.run(f"s5cmd sync {src} {dest}", shell=True)
            return res

    def s5cp(self, src: str, dest: str, recursive=True):
        if self.has_s5cmd:
            # src_protocol = infer_storage_options(src)["protocol"]
            src_path = infer_storage_options(src)["path"]
            if src_path == "file":
                src_path = ""

            # dest_protocol = infer_storage_options(src)["protocol"]
            dest_path = infer_storage_options(src)["path"]
            if dest_path == "file":
                dest_path = ""

            if self.isfile(src_path):
                dest_path = os.path.dirname(dest_path)
            else:

                if recursive and not src.endswith("*"):
                    src = src.lstrip("/") + "/*"

            res = subprocess.run(f"s5cmd cp {src} {dest}", shell=True)
            return res


def fsspec_dir_filesystem(path: str, filesystem: AbstractFileSystem):
    return DirFileSystem(path=path, fs=filesystem)


def pyarrow_subtree_filesystem(path: str, filesystem: pafs.SubTreeFileSystem):
    return pafs.SubTreeFileSystem(base_path=path, base_fs=filesystem)
