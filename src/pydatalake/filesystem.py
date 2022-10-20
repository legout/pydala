import configparser
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pyarrow.fs as pafs
import s3fs
import yaml
from fsspec.implementations import arrow, local


class AWSCredentialsManager:
    def __init__(
        self,
        profile: str = "default",
        credentials: str | Path | dict[str, str] = "~/.aws/credentials",
    ) -> None:
        self._profile = profile

        if isinstance(credentials, str):
            self._filename = Path(credentials).expanduser()
            self.load_credentials()

        elif isinstance(credentials, dict):
            self._filename = Path("~/.aws/credentials").expanduser()
            self._credentials = credentials

        else:
            self._filename = Path("~/.aws/credentials").expanduser()
            self.load_credentials()

    @staticmethod
    def load_credentials(filename: str | Path, profile: str) -> dict[str, str]:
        config = configparser.ConfigParser()

        if isinstance(filename, (str, Path)):
            config.read(filename)

            if profile in config.sections():
                return dict(config[profile])
            else:
                raise ValueError(f"given profile {profile} not found in {filename}")

        else:
            raise TypeError(
                f"filename must be of type str or Path and not {type(filename)}."
            )

    def load_credentials(self) -> None:
        filename = self._filename or Path("~/.aws/credentials").expanduser()
        self._credentials = self._load_credentials(
            filename=filename, profile=self._profile
        )

    @staticmethod
    def _write_credentials(
        credentials: dict[str, str], filename: str | Path, profile: str
    ) -> None:

        if isinstance(credentials, dict):

            config = configparser.ConfigParser()
            config[profile] = credentials

            if isinstance(filename, str):
                with open(filename, "a") as f:
                    config.write(f)

            else:
                raise TypeError(
                    f"filename must be of type str or Path and not {type(filename)}."
                )

        else:
            raise TypeError(f"credentials must be of type dict not {type(credentials)}")

    def write_credentials(self) -> None:
        filename = self._filename or Path("~/.aws/credentials").expanduser()
        profile = self._profile or "default"

        self._write_credentials(
            credentials=self._credentials, filename=filename, profile=profile
        )

    @staticmethod
    def _export_env(
        profile: str | None = None, credentials: dict | None = None
    ) -> None:

        if profile is not None:
            os.environ["AWS_PROFILE"] = profile

        elif credentials is not None:
            for k in credentials:
                os.environ[k.upper()] = credentials[k]
        else:
            raise ValueError("either profile or credentials must be not None.")

    def export_env(self) -> None:
        self._export_env(profile=self._profile, credentials=self._credentials)

    def swtich_profile(self, profile: str) -> None:
        self._profile = profile
        self.load_credentials()


class S5CMD(AWSCredentialsManager):
    def __init__(
        self,
        bucket: str | None = None,
        profile: str = "default",
        credentials: str | Path | dict[str, str] = "~/.aws/credentials",
    ) -> None:
        super().__init__(profile=profile, credentials=credentials)
        self._bucket = bucket

    def _gen_path(self, path: str | Path) -> str | Path:
        if self._bucket is not None:
            return os.path.join(self._bucket, path)
        else:
            return path

    def _strip_bucket(self, path: str) -> str:
        if self._bucket is not None:
            return path.split(self._bucket)[-1].lstrip("/")
        else:
            return path

    def _add_global_options(self, *args, **kwargs) -> str:
        options = [f"--{arg}" if len(arg) > 1 else f"-{arg}" for arg in args]
        options += [f"--{k} {v}" if len(k) > 1 else f"-{k} {v}" for k, v in kwargs]
        return " ".join(options) + " "

    @staticmethod
    def _format_error(stderr:bytes)->dict[str, str] | str |None:
        stderr:str = stderr.decode("uft-8").strip()

        if len(stderr)>0:
            if stderr[0] == "{":
                return json.loads(stderr)
            else:
                return stderr            
                
            
    @staticmethod
    def _format_out(stdout:bytes, with_stat:bool)-> list[Any]|None:
        stdout:str = stdout.decode("utf-8").strip()
               
        if len(stdout)>0:
            if stdout[0]=="{":
                res = [json.loads(e) for e in stdout.split()]

            if with_stat:
                stat = res[-1]

                if stat["success"] == 1:
                    return res[:-1]
    
    def _run_command(self, command: str, *args, **kwargs) -> list[Any] | str:

        with_stat = kwargs.pop("with_stat", True)

        if not isinstance(with_stat, bool):
            raise TypeError("with_stat must be of type bool.")

        if self.has_s5cmd:

            command_ = "s5cmd --stat " if with_stat else "s5cmd "
            command_ += self._add_global_options(*args, **kwargs)
            command_ += command

            res = subprocess.run(command_, shell=True, capture_output=True)

            stdout = self._format_out(res.stdout)
            strerr = self._format_error(res.stderr)

            if "json" in args:
                res = [json.loads(e) for e in res.split()]

                if with_stat:
                    stat = res[-1]

                    if stat["success"] == 1:
                        return res[:-1]

                    else:
                        raise subprocess.SubprocessError(
                            f"Running command {command_} finished with an error."
                        )

                else:
                    return res
            else:
                return res

        else:
            raise FileNotFoundError(
                "s5cmd not found. See here for more information https://github.com/peak/s5cmd."
            )

    @staticmethod
    def _check_for_s5cmd() -> bool:
        res = subprocess.run("which s5cmd", shell=True, capture_output=True)
        return res.returncode == 0

    @property
    def has_s5cmd(self):
        if not hasattr(self, "_has_s5cmd"):
            self._has_s5cmd = self._check_for_s5cmd()

        return self._has_s5cmd

    def print_help(self, operation: str = "cp"):
        command = f"{operation} -h"
        res = self._run_command(command, with_stat=False)
        print(res)

    def ls(self, path: str) -> list:
        pass

    def cp(self):
        pass

    def rm(self):
        pass

    def mv(self):
        pass

    def mb(self):
        pass

    def rb(self):
        pass

    def select(self):
        pass

    def du(self):
        pass

    def cat(self):
        pass

    def run(self):
        pass

    def version(self):
        pass

    def sync(self):
        pass


# class FileSystem:
#     def __init__(
#         self,
#         type_: str | None = "s3",
#         filesystem: pafs.S3FileSystem
#         | pafs.LocalFileSystem
#         | s3fs.S3FileSystem
#         | None = None,
#         credentials: dict | None = None,
#         bucket: str | None = None,
#     ):
#         self._bucket = bucket
#         self._credentials = credentials

#         if credentials is not None:
#             self.set_env(**credentials)

#         if type_ is not None and filesystem is None:
#             self._type = type_

#             if self._type == "local":
#                 self._fs = pafs.LocalFileSystem()
#                 self._filesystem = local.LocalFileSystem(self._fs)

#             elif self._type == "s3":
#                 self._filesystem = s3fs.S3FileSystem(anon=False)
#                 self._fs = self._filesystem

#             else:
#                 raise ValueError("type_ must be 'local' or 's3'.")

#         elif filesystem is not None:

#             if type(filesystem) in [pafs.S3FileSystem, s3fs.S3FileSystem]:
#                 self._type = "s3"
#                 if isinstance(filesystem, pafs.S3FileSystem):

#                     self._filesystem = arrow.ArrowFSWrapper(filesystem)
#                     self._fs = filesystem

#                 elif isinstance(filesystem, s3fs.S3FileSystem):
#                     self._filesystem = filesystem
#                     self._fs = filesystem

#             elif type(filesystem) == pafs.LocalFileSystem:
#                 self._type = "local"
#                 self._filesystem = local.LocalFileSystem(filesystem)
#                 self._fs = filesystem

#             elif filesystem is None:
#                 self._type = "local"
#                 self._fs = pafs.LocalFileSystem()
#                 self._filesystem = local.LocalFileSystem(self._fs)

#             else:
#                 raise TypeError(
#                     """filesystem must be 's3fs.S3FileSystem', 'pyarrow.fs.S3FileSystem',
#                     'pyarrow.fs.LocalFileSystem' or None."""
#                 )

#         else:
#             raise ValueError("type_ or filesystem must not be None.")

#     @staticmethod
#     def _check_for_s5cmd()->bool:
#         res = subprocess.run("which s5cmd", shell=True, capture_output=True)
#         return res.returncode==0

#     @property
#     def has_s5cmd(self):
#         if not hasattr(self, _has_s5cmd):
#             self._has_s5cmd=self._check_for_s5cmd()

#         return self._has_s5cmd

#     def _gen_path(self, path: str) -> str:
#         if self._bucket is not None:
#             if self._bucket not in path:
#                 return os.path.join(self._bucket, path)
#             else:
#                 return path
#         else:
#             return path

#     def _strip_path(self, path: str) -> str:
#         if self._bucket is not None:
#             return path.split(self._bucket)[-1]
#         else:
#             return path

#     def set_env(self, **kwargs):
#         for k in kwargs:
#             os.environ[k] = kwargs[k]

#     def cat(
#         self,
#         path: str|list,
#         recursive: bool = False,
#         on_error: str = "raise",
#         batch_size: int | None = None,
#         **kwargs
#     ):
#         """Fetch (potentially multiple) paths' contents


#         Args:
#             path (str | list): URL(s) of file on this filesystems
#             recursive (bool, optional): If True, assume the path(s) are directories,
#                 and get all the contained files. Defaults to False.
#             on_error (str, optional): If raise, an underlying exception will be
#                 raised (converted to KeyError   if the type is in self.missing_exceptions);
#                 if omit, keys with exception will simply not be included in the output;
#                 if "return", all keys are included in the output, but the value will be
#                 bytes or an exception instance. Defaults to "raise".
#             batch_size (int | None, optional): Defaults to None.

#         Returns:
#             dict: dict of {path: contents} if there are multiple paths
#                 or the path has been otherwise expanded
#         """
#         path = self._gen_path(path)
#         return self._filesystem.cat(
#             path=path,
#             recursive=recursive,
#             on_error=on_error,
#             batch_size=batch_size,
#             **kwargs
#         )

#     def cat_file(
#         self,
#         path: str,
#         version_id: str | None = None,
#         start: int | None = None,
#         end: int | None = None,
#     )-:
#         """Get the content of a file

#         Args:
#             path (str): URL of file on this filesystems
#             version_id (str | None, optional): Defaults to None.
#             start (int | None, optional): Bytes limits of the read. If negative, backwards
#                 from end, like usual python slices. Either can be None for start or
#                 end of file, respectively. Defaults to None.
#             end (int | None, optional): see start. Defaults to None.

#         Returns:
#             str: file content
#         """
#         path = self._gen_path(path)
#         return self._filesystem.cat_file(
#             path=path, version_id=version_id, start=start, end=end
#         )

#     def checksum(self, path: str, refresh: bool = False):
#         """Unique value for current version of file

#         Args:
#             path (str): path of file to get checksum for.
#             refresh (bool, optional): if False, look in local cache for file
#                 details first. Defaults to False.

#         Returns:
#             str: checksum
#         """
#         path = self._gen_path(path)
#         return self._filesystem.checksum(path=path, refresh=refresh)

#     def copy(self,  path1:str,
#         path2:str,
#         recursive:bool=False,
#         on_error:str|None=None,
#         maxdepth:int|None=None,
#         batch_size:int|None=None,
#         **kwargs,):
#         """Copy within two locations in the filesystem.

#         Args:
#             path1 (str): source path.
#             path2 (str): destination path.
#             recursive (bool, optional): copy recursive. Defaults to False.
#             on_error (str | None, optional): If raise, any not-found exceptions
#                 will be raised; if ignore any not-found exceptions will cause
#                 the path to be skipped; defaults to raise unless recursive is true,
#                 where the default is ignore. Defaults to None.
#             maxdepth (int | None, optional): maximum number of directory
#                 levels to descend, None for unlimited. Defaults to None.
#             batch_size (int | None, optional): Defaults to None.
#         """
#         path1 = self._gen_path(path1)
#         path2 = self._gen_path(path2)

#         self._filesystem.copy(path1=path1, path2=path2, recursive=recursive,
#                               on_error=on_error, maxdepth=maxdepth, batch_size=batch_size, **kwargs)

#     def cp(self, *args, **kwargs):
#         self.copy(*args, **kwargs)

#     def cp_file(self, path1:str, path2:str, preserve_etag:bool|None=None, **kwargs):
#         """Copy file between locations on S3.


#         Args:
#             path1 (str): source path.
#             path2 (str): destination path.
#             preserve_etag (bool | None, optional): Whether to preserve etag while
#                 copying. If the file is uploaded as a single part, then it will
#                 be always equalivent to the md5 hash of the file hence etag will
#                 always be preserved. But if the file is uploaded in multi parts,
#                 then this option will try to reproduce the same multipart upload
#                 while copying and preserve  the generated etag. Defaults to None.
#         """
#         path1 = self._gen_path(path1)
#         path2 = self._gen_path(path2)

#         self._filesystem.cp_file(path1=path1, path2=path2, preserve_etag=preserve_etag, **kwargs)

#     def delete(self, path: str|list, recursive:bool=False, maxdepth:int|None=None):
#         """Delete files.

#         Args:
#             path (str | list): File(s) to delete.
#             recursive (bool, optional): If file(s) are directories, recursively
#                 delete contents and then also remove the directory. Defaults to False.
#             maxdepth (int | None, optional): Depth to pass to walk for finding files
#                 to delete, if recursive. If None, there will be no limit and infinite
#                 recursion may be possible. Defaults to None.
#         """
#         path = self._gen_path(path)
#         self._filesystem.delete(path, recursive=recursive, maxdepth=maxdepth)

#     def du(self, path:str, total:bool=True, maxdepth:int|None=None, **kwargs):
#         """Space used by files within a path

#         Args:
#             path (str):
#             total (bool, optional): whether to sum all the file sizes. Defaults to True.
#             maxdepth (int | None, optional): maximum number of directory
#                 levels to descend, None for unlimited. Defaults to None.
#         """
#         path=self._gen_path(path)
#         self._filesystem.du(path=path, total=total, maxdepth=maxdepth, **kwargs)

#     def disk_usage(self, *args, **kwargs):
#         self.du(*args, **kwargs)

#     def download(self, *args, **kwargs):
#         self.get(*args, **kwargs)


#     def exists(self, path: str) -> bool:
#         """Returns True, if path exists, else returns False"""
#         path = self._gen_path(path)
#         return self._filesystem.exists(path)

#     def get(self, rpath:str, lpath:str, recursive:bool=False, batch_size:int|None=None **kwargs):
#         """Copy file(s) to local.

#         Copies a specific file or tree of files (if recursive=True). If lpath
#         ends with a "/", it will be assumed to be a directory, and target files
#         will go within. Can submit a list of paths, which may be glob-patterns
#         and will be expanded.

#         The get_file method will be called concurrently on a batch of files. The
#         batch_size option can configure the amount of futures that can be executed
#         at the same time. If it is -1, then all the files will be uploaded concurrently.
#         The default can be set for this instance by passing "batch_size" in the
#         constructor, or for all instances by setting the "gather_batch_size" key
#         in ``fsspec.config.conf``, falling back to 1/8th of the system limit.
#         """
#         rpath = self._gen_path(rpath)

#         self._filesystem.get(rpath=rpath, lpath=lpath, recursive=recursive, batch_size=batch_size, **kwargs)


#     def invalidate_cache(self, path:str|None=None):

#         path=self._gen_path(path)
#         self._filesystem.invalidate_cache(path=path)


#     def is_file(
#         self,
#         path: str,
#     ) -> bool:
#         return self._filesystem.isfile(path)

#     def open(self, path: str, mode="r", **kwargs):
#         return self._filesystem.open(path, mode=mode, **kwargs)

#     def rm(self, path: str|list, recursive:bool=False, maxdepth:int|None=None):
#         """Delete files.

#         Args:
#             path (str | list): File(s) to delete.
#             recursive (bool, optional): If file(s) are directories, recursively
#                 delete contents and then also remove the directory. Defaults to False.
#             maxdepth (int | None, optional): Depth to pass to walk for finding files
#                 to delete, if recursive. If None, there will be no limit and infinite
#                 recursion may be possible. Defaults to None.
#         """
#         path = self._gen_path(path)
#         self._filesystem.rm(path, recursive=recursive, maxdepth=maxdepth)
