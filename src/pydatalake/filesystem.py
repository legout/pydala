import shutil
from pathlib import Path
import pyarrow.fs as pafs
import s3fs
from joblib import Parallel, delayed

def path_exists(
    path: str, filesystem: pafs.FileSystem | s3fs.S3FileSystem | None = None
) -> bool:
    if filesystem is not None:
        if hasattr(filesystem, "exists"):
            return filesystem.exists(path)
        else:
            return filesystem.get_file_info(path).type > 0
    else:
        return Path(path).exists()


def is_file(
    path: str,
    filesystem: pafs.FileSystem | s3fs.S3FileSystem | None = None,
) -> bool:
    if filesystem is not None:
        if hasattr(filesystem, "isfile"):
            return filesystem.isfile(path)
        else:
            return filesystem.get_file_info(path).type == 2
    else:
        return Path(path).is_file()


def open(path: str, filesystem: pafs.FileSystem | s3fs.S3FileSystem):
    if hasattr(filesystem, "open"):
        return filesystem.open(path)
    else:
        return filesystem.open_input_file(path)


def delete(path: str, filesystem: pafs.FileSystem | s3fs.S3FileSystem | None):
    if filesystem is None:
        shutil.rmtree(path)
    else:
        if hasattr(filesystem, open):
            filesystem.delete(path, recursive=True)
        else:
            filesystem.delete_dir(path)


def copy_to_tmp_directory(
    src: str|list,
    dest: str,
    filesystem: pafs.FileSystem | s3fs.S3FileSystem | None = None,
):
    if filesystem is None:
        src = Path(src)
        dest = Path(dest)
        
        if src.is_file:
            shutil.copyfile(src=src, dst=dest)
        else:
            shutil.copytree(src=src, dst=dest)
    else:
        if isinstance(src, str):
            _ = filesystem.get(src, dest, recursive=True)
        else:
            dest = [f"{dest}/{src_}" for src_ in src]
            _ = Parallel()(
                delayed(filesystem.get)(src_, dest_) for src_, dest_ in zip(src, dest)
            )
