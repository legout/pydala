import os
from pathlib import Path
from typing import Any, Union

import pyarrow.fs as pafs
import s3fs
from fsspec.implementations import arrow, local

from .utils import S5CMD

fs_type = Union[s3fs.S3FileSystem, pafs.S3FileSystem, pafs.LocalFileSystem, None]


class FileSystem:
    def __init__(
        self,
        type_: str | None = "s3",
        aws_profile: str = "default",
        filesystem: fs_type = None,
        credentials: dict | None = None,
        bucket: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        endpoint_url: str | None = None,
        session_token: str | None = None,
        use_s5cmd: bool = True,
    ) -> None:
        self._bucket = bucket
        self._credentials = credentials
        self._aws_profile = aws_profile

        if credentials is not None:
            self.set_aws_env(**credentials)

        if aws_profile is not None:
            self.set_aws_profile(aws_profile)

        if type_ is not None and filesystem is None:
            self._type = type_

            if self._type == "local":
                self._fs = pafs.LocalFileSystem()
                self._filesystem = local.LocalFileSystem(self._fs)

            elif self._type == "s3":
                self._filesystem = s3fs.S3FileSystem(
                    anon=False,
                    key=access_key,
                    secret=secret_key,
                    token=session_token,
                    client_kwargs=dict(endpoint_url=endpoint_url),
                )
                self._fs = self._filesystem

            else:
                raise ValueError("type_ must be 'local' or 's3'.")

        elif filesystem is not None:

            if type(filesystem) in [pafs.S3FileSystem, s3fs.S3FileSystem]:
                self._type = "s3"
                if isinstance(filesystem, pafs.S3FileSystem):

                    self._filesystem = arrow.ArrowFSWrapper(filesystem)
                    self._fs = filesystem

                elif isinstance(filesystem, s3fs.S3FileSystem):
                    self._filesystem = filesystem
                    self._fs = filesystem

            elif type(filesystem) == pafs.LocalFileSystem:
                self._type = "local"
                self._filesystem = local.LocalFileSystem(filesystem)
                self._fs = filesystem

            elif filesystem is None:
                self._type = "local"
                self._fs = pafs.LocalFileSystem()
                self._filesystem = local.LocalFileSystem(self._fs)

            else:
                raise TypeError(
                    """filesystem must be 's3fs.S3FileSystem', 'pyarrow.fs.S3FileSystem',
                    'pyarrow.fs.LocalFileSystem' or None."""
                )

        else:
            raise ValueError("type_ or filesystem must not be None.")

        if self._type == "s3" and use_s5cmd and S5CMD._check_for_s5cmd():
            self._s5 = S5CMD(
                bucket=bucket, profile=aws_profile, endpoint_url=endpoint_url
            )
            self._has_s5cmd = True

    def _gen_path(self, path: str) -> str:
        if self._bucket is not None:
            if self._bucket not in path:
                return os.path.join(self._bucket, path)

        return path

    def _gen_paths(self, paths: list) -> list:
        return list(map(self._gen_path, paths))

    def _strip_path(self, path: str) -> str:
        if self._bucket is not None:
            return path.split(self._bucket)[-1].lstrip("/")
        else:
            return path

    def _strip_paths(self, paths: list) -> list:
        return list(map(self._strip_path, paths))

    def set_aws_env(self, **kwargs):
        for k in kwargs:
            os.environ[k] = kwargs[k]

    def set_aws_profile(self, name: str | None):
        name = name or "default"
        os.environ["AWS_PROFILE"] = name

    def cat(
        self,
        path: str | list,
        recursive: bool = False,
        on_error: str = "raise",
        **kwargs,
    ):
        """Fetch (potentially multiple) paths' contents


        Args:
            path (str | list): URL(s) of file on this filesystems
            recursive (bool, optional): If True, assume the path(s) are directories,
                and get all the contained files. Defaults to False.
            on_error (str, optional): If raise, an underlying exception will be
                raised (converted to KeyError   if the type is in self.missing_exceptions);
                if omit, keys with exception will simply not be included in the output;
                if "return", all keys are included in the output, but the value will be
                bytes or an exception instance. Defaults to "raise".

        Returns:
            dict: dict of {path: contents} if there are multiple paths
                or the path has been otherwise expanded
        """
        if isinstance(path, str):
            path = [path]
        return self._filesystem.cat(
            path=self._gen_path(path), recursive=recursive, on_error=on_error, **kwargs
        )

    def cat_file(
        self,
        path: str,
        version_id: str | None = None,
        start: int | None = None,
        end: int | None = None,
        **kwargs,
    ):
        """Get the content of a file

        Args:
            path (str): URL of file on this filesystems
            version_id (str | None, optional): Defaults to None.
            start (int | None, optional): Bytes limits of the read. If negative, backwards
                from end, like usual python slices. Either can be None for start or
                end of file, respectively. Defaults to None.
            end (int | None, optional): see start. Defaults to None.

        Returns:
            str: file content
        """
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            return self._filesystem.cat_file(
                path=self._gen_path(path),
                version_id=version_id,
                start=start,
                end=end,
                **kwargs,
            )
        else:
            return self._filesystem.cat_file(
                path=self._gen_path(path),
                start=start,
                end=end,
                **kwargs,
            )

    def checksum(self, path: str, refresh: bool = False):
        """Unique value for current version of file

        Args:
            path (str): path of file to get checksum for.
            refresh (bool, optional): if False, look in local cache for file
                details first. Defaults to False.

        Returns:
            str: checksum
        """
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            return self._filesystem.checksum(path=self._gen_path(path), refresh=refresh)
        else:
            return self._filesystem.checksum(path=self._gen_path(path))

    def copy(
        self,
        path1: str,
        path2: str,
        recursive: bool = False,
        on_error: str | None = None,
        maxdepth: int | None = None,
        batch_size: int | None = None,
        **kwargs,
    ):
        """Copy within two locations in the filesystem.

        Args:
            path1 (str): source path.
            path2 (str): destination path.
            recursive (bool, optional): copy recursive. Defaults to False.
            on_error (str | None, optional): If raise, any not-found exceptions
                will be raised; if ignore any not-found exceptions will cause
                the path to be skipped; defaults to raise unless recursive is true,
        """
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            self._filesystem.copy(
                self._gen_path(path1),
                self._gen_path(path2),
                recursive=recursive,
                on_error=on_error,
                maxdepth=maxdepth,
                batch_size=batch_size,
                **kwargs,
            )
        else:
            self._filesystem.copy(
                self._gen_path(path1),
                self._gen_path(path2),
                recursive=recursive,
                on_error=on_error,
                **kwargs,
            )

    def cp(self, *args, **kwargs):
        self.cp(*args, **kwargs)

    def cp_file(self, path1: str, path2: str, **kwargs):
        """Copy file between locations on S3.


        Args:
            path1 (str): source path.
            path2 (str): destination path.
            preserve_etag (bool | None, optional): Whether to preserve etag while
                copying. If the file is uploaded as a single part, then it will
                be always equalivent to the md5 hash of the file hence etag will
                always be preserved. But if the file is uploaded in multi parts,
                then this option will try to reproduce the same multipart upload
                while copying and preserve  the generated etag. Defaults to None.
        """

        self._filesystem.cp_file(self._gen_path(path1), self._gen_path(path2), **kwargs)

    def delete(
        self, path: str | list, recursive: bool = False, maxdepth: int | None = None
    ):
        """Delete files.

        Args:
            path (str | list): File(s) to delete.
            recursive (bool, optional): If file(s) are directories, recursively
                delete contents and then also remove the directory. Defaults to False.
            maxdepth (int | None, optional): Depth to pass to walk for finding files
                to delete, if recursive. If None, there will be no limit and infinite
                recursion may be possible. Defaults to None.
        """
        if isinstance(path, str):
            path = [path]
        self._filesystem.delete(
            self._gen_paths(path), recursive=recursive, maxdepth=maxdepth
        )

    def du(self, path: str, total: bool = True, maxdepth: int | None = None, **kwargs):
        """Space used by files within a path

        Args:
            path (str):
            total (bool, optional): whether to sum all the file sizes. Defaults to True.
            maxdepth (int | None, optional): maximum number of directory
                levels to descend, None for unlimited. Defaults to None.
            kwargs: passed to ``ls``
        Return:
            Dict of {fn: size} if total=False, or int otherwise, where numbers
                refer to bytes used.
        """
        return self._filesystem.du(
            path=self._gen_path(path), total=total, maxdepth=maxdepth, **kwargs
        )

    def disk_usage(self, *args, **kwargs):
        self.du(*args, **kwargs)

    def download(self, *args, **kwargs):
        self.download(*args, **kwargs)

    def exists(self, path: str) -> bool:
        """Returns True, if path exists, else returns False"""
        return self._filesystem.exists(self._gen_path(path))

    def get(
        self,
        rpath: str,
        lpath: str,
        recursive: bool = False,
        batch_size: int | None = None,
        **kwargs,
    ):
        """Copy file(s) to local.

        Copies a specific file or tree of files (if recursive=True). If lpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within. Can submit a list of paths, which may be glob-patterns
        and will be expanded.

        ---------------------------------
        Only available when using s3fs.S3FileSystem:

        The get_file method will be called concurrently on a batch of files. The
        batch_size option can configure the amount of futures that can be executed
        at the same time. If it is -1, then all the files will be uploaded concurrently.
        The default can be set for this instance by passing "batch_size" in the
        constructor, or for all instances by setting the "gather_batch_size" key
        in ``fsspec.config.conf``, falling back to 1/8th of the system limit.
        """
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            self._filesystem.get(
                self._gen_path(rpath),
                lpath,
                recursive=recursive,
                batch_size=batch_size,
                **kwargs,
            )
        else:
            self._filesystem.get(
                self._gen_path(rpath), lpath, recursive=recursive, **kwargs
            )

    def get_file(
        self,
        rpath: str,
        lpath: str,
        callback: object | None = None,
        outfile: str | None = None,
        **kwargs,
    ):
        """Copy single remote file to local"""
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            self._filesystem.get_file(self._gen_path(rpath), lpath, callback=callback)
        else:
            self._filesystem.get_file(
                self._gen_path(rpath), lpath, callback=callback, outfile=outfile
            )

    def glob(self, path: str, **kwargs) -> list:
        """Find files by glob-matching.

        If the path ends with '/' and does not contain "*", it is essentially
        the same as ``ls(path)``, returning only files.

        We support ``"**"``,
        ``"?"`` and ``"[..]"``. We do not support ^ for pattern negation.

        Search path names that contain embedded characters special to this
        implementation of glob may not produce expected results;
        e.g., 'foo/bar/*starredfilename*'.

        kwargs are passed to ``ls``."""
        return self._strip_paths(self._filesystem.glob(self._gen_path(path), **kwargs))

    def head(self, path: str, size: int = 1024) -> str | bytes:
        """Get the first ``size`` bytes from file"""
        return self._filesystem.head(path=self._gen_path(path), size=size)

    def info(self, path, **kwargs) -> dict:
        """Give details of entry at path

        Returns a single dictionary, with exactly the same information as ``ls``
        would with ``detail=True``.

        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.

        Some file systems might not be able to measure the file's size, in
        which case, the returned dict will include ``'size': None``.

        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), type (file,
        directory, or something else) and other FS-specific keys."""

        return self._filesystem.info(self._gen_path(path), **kwargs)

    def invalidate_cache(self, path: str | None = None) -> None:
        """Discard any cached directory information

        Parameters
        ----------
        path: string or None
            If None, clear all listings cached else listings at or under given
            path."""
        self._filesystem.invalidate_cache(path=self._gen_path(path))

    def isfile(
        self,
        path: str,
    ) -> bool:
        """Is this entry file-like?"""
        return self._filesystem.isfile(self._gen_path(path))

    def isdir(
        self,
        path: str,
    ) -> bool:
        """Is this entry directory-like?"""
        return self._filesystem.isdir(self._gen_path(path))

    def lexists(self, path: str) -> bool:
        """If there is a file at the given path (including
        broken links)"""
        return self._filesystem.lexists(self._gen_path(path))

    def listdir(self, path: str, detail: bool = True) -> list:
        return self._strip_paths(
            self._filesystem.listdir(self._gen_path(path), detail=detail)
        )

    def ls(self, path: str, detail: bool = False, **kwargs) -> list:
        """List objects at path.

        This should include subdirectories and files at that location. The
        difference between a file and a directory must be clear when details
        are requested.

        The specific keys, or perhaps a FileInfo class, or similar, is TBD,
        but must be consistent across implementations.
        Must include:

        - full path to the entry (without protocol)
        - size of the entry, in bytes. If the value cannot be determined, will
        be ``None``.
        - type of entry, "file", "directory" or other

        Additional information
        may be present, aproriate to the file-system, e.g., generation,
        checksum, etc.

        May use refresh=True|False to allow use of self._ls_from_cache to
        check for a saved listing and avoid calling the backend. This would be
        common where listing may be expensive.

        Parameters
        ----------
        path: str
        detail: bool
            if True, gives a list of dictionaries, where each is the same as
            the result of ``info(path)``. If False, gives a list of paths
            (str).
        kwargs: may have additional backend-specific options, such as version
            information

        Returns
        -------
        List of strings if detail is False, or list of directory information
        dicts if detail is True."""

        res = self._filesystem.ls(self._gen_path(path), detail=detail, **kwargs)
        if detail:
            return res
        return self._strip_paths(res)

    def makedir(self, path: str, create_parents: bool = True, **kwargs):
        self._filesystem.makedir(
            self._gen_path(path), create_parents=create_parents, **kwargs
        )

    def makedirs(self, path: str, exist_ok: bool = True):
        """Recursively make directories

        Creates directory at path and any intervening required directories.
        Raises exception if, for instance, the path already exists but is a
        file.

        Parameters
        ----------
        path: str
            leaf directory name
        exist_ok: bool (False)
            If False, will error if the target already exists"""

        self._filesystem.makedirs(self._gen_path(path), exist_ok=exist_ok)

    def merge(self, path: str, filelist: list, **kwargs):
        """Create single S3 file from list of S3 files

        Uses multi-part, no data is downloaded. The original files are
        not deleted.

        Parameters
        ----------
        path : str
            The final file to produce
        filelist : list of str
            The paths, in order, to assemble into the final file."""
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            filelist = [self._gen_path(f) for f in filelist]
            path = self._gen_path(path)
            self._filesystem.merge(path, filelist, **kwargs)

        else:
            raise NotImplemented(
                f"Merge is not implmented for {type(self._fs)} use s3fs.S3FileSystem instead."
            )

    def metadata(self, path: str, refresh: bool = False, **kwargs):
        """Return metadata of path.

        Parameters
        ----------
        path : string/bytes
            filename to get metadata for
        refresh : bool (=False)
            (ignored)"""
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            self._filesystem.metadata(self._gen_path(path), refresh=refresh, **kwargs)
        else:
            raise NotImplemented(
                f"Merge is not implmented for {type(self._fs)} use s3fs.S3FileSystem instead."
            )

    def mkdir(self, path: str, create_parents: bool = True, **kwargs):
        """Create directory entry at path

        For systems that don't have true directories, may create an for
        this instance only and not touch the real filesystem

        Parameters
        ----------
        path: str
            location
        create_parents: bool
            if True, this is equivalent to ``makedirs``
        kwargs:
            may be permissions, etc."""
        self._filesystem.mkdir(
            self._gen_path(path), create_parents=create_parents, **kwargs
        )

    def mkdirs(self, path: str, exist_ok: bool = True):
        self._filesystem.mkdirs(self._gen_path(path), exist_ok=exist_ok)

    def modified(self, path: str):
        """Return the modified timestamp of a file as a datetime.datetime"""
        return self._filesystem.modified(self._gen_path(path))

    def move(self, path1: str, path2: str, **kwargs):
        self._filesystem.move(self._gen_path(path1), self._gen_path(path2), **kwargs)

    def mv(self, path1: str, path2: str, **kwargs):
        """Move file(s) from one location to another"""
        self._filesystem.mv(self._gen_path(path1), self._gen_path(path2), **kwargs)

    def mv_file(self, path1: str, path2: str, **kwargs):
        """Move file(s) from one location to another"""
        if not isinstance(self._filesystem, s3fs.S3FileSystem):
            self._filesystem.mv_file(
                self._gen_path(path1), self._gen_path(path2), **kwargs
            )
        else:
            raise NotImplemented(
                f"Merge is not implmented for {type(self._fs)} use pyarrow.fs.S3FileSystem instead."
            )

    def open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        cache_options: dict | None = None,
        compression: str | None = None,
        **kwargs,
    ):
        """Return a file-like object from the filesystem

        The resultant instance must function correctly in a context ``with``
        block.

        Parameters
        ----------
        path: str
            Target file
        mode: str like 'rb', 'w'
            See builtin ``open()``
        block_size: int
            Some indication of buffering - this is a value in bytes
        cache_options : dict, optional
            Extra arguments to pass through to the cache.
        compression: string or None
            If given, open file using compression codec. Can either be a compression
            name (a key in ``fsspec.compression.compr``) or "infer" to guess the
            compression from the filename suffix.
        encoding, errors, newline: passed on to TextIOWrapper for text mode
        """
        return self._filesystem.open(
            self._gen_path(path),
            mode=mode,
            block_size=block_size,
            cache_options=cache_options,
            compression=compression,
            **kwargs,
        )

    def open_async(self, path: str, mode: str = "rb", **kwargs):
        if isinstance(self._fs, s3fs.S3FileSystem):
            return self._filesystem.open_async(
                self._gen_path(path), mode=mode, **kwargs
            )
        else:
            raise NotImplemented(
                f"Merge is not implmented for {type(self._fs)} use s3fs.S3FileSystem instead."
            )

    def pipe(
        self,
        path: str,
        value: bytes | None = None,
        batch_size: int | None = None,
        **kwargs,
    ):
        """Put value into path

        (counterpart to ``cat``)

        Parameters
        ----------
        path: string or dict(str, bytes)
            If a string, a single remote location to put ``value`` bytes; if a dict,
            a mapping of {path: bytesvalue}.
        value: bytes, optional
            If using a single path, these are the bytes to put there. Ignored if
            ``path`` is a dict"""
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            self._filesystem.pipe(
                self._gen_path(path), value=value, batch_size=batch_size, **kwargs
            )
        else:
            self._filesystem.pipe(self._gen_path(path), value=value, **kwargs)

    def pipe_file(
        self, path: str, data: bytes | None = None, chunksize: int = 52428800, **kwargs
    ):
        """Set the bytes of given file"""
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            self._filesystem.pipe_file(
                self._gen_path(path), data=data, chunksize=chunksize, **kwargs
            )
        else:
            self._filesystem.pipe_file(self._gen_path(path), value=data, **kwargs)

    @property
    def protocol(self):
        return self._filesystem.protocol

    def put(
        self,
        lpath: str,
        rpath: str,
        recursive: bool = False,
        callback: object | None = None,
        batch_size: int | None = None,
        **kwargs,
    ):
        """Copy file(s) from local.

        Copies a specific file or tree of files (if recursive=True). If rpath
        ends with a "/", it will be assumed to be a directory, and target files
        will go within.

        -----------------------------------------------
        Only available when using the s3fs.S3Filesystem:

        The put_file method will be called concurrently on a batch of files. The
        batch_size option can configure the amount of futures that can be executed
        at the same time. If it is -1, then all the files will be uploaded concurrently.
        The default can be set for this instance by passing "batch_size" in the
        constructor, or for all instances by setting the "gather_batch_size" key
        in ``fsspec.config.conf``, falling back to 1/8th of the system limit ."""

        if isinstance(self._fs, s3fs.S3FileSystem):
            self._filesystem.put(
                lpath,
                self._gen_path(rpath),
                recursive=recursive,
                callback=callback,
                batch_size=batch_size,
                **kwargs,
            )
        else:
            self._filesystem.put(
                lpath,
                self._gen_path(rpath),
                recursive=recursive,
                callback=callback,
                **kwargs,
            )

    def put_file(
        self,
        lpath: str,
        rpath: str,
        callback: object | None,
        chunksize: int = 52428800,
        **kwargs,
    ):
        """Copy single file to remote"""
        if isinstance(self._fs, s3fs.S3FileSystem):  #
            self._filesystem.put_file(
                lpath,
                self._gen_path(rpath),
                callback=callback,
                chunksize=chunksize,
                **kwargs,
            )
        else:
            self._filesystem.put_file(
                lpath, self._gen_path(rpath), callback=callback, **kwargs
            )

    def put_tags(self, path: str, tags: dict | str, mode="o"):
        """Set tags for given existing key

        Tags are a str:str mapping that can be attached to any key, see
        https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html

        This is similar to, but distinct from, key metadata, which is usually
        set at key creation time.

        Parameters
        ----------
        path: str
            Existing key to attach tags to
        tags: dict str, str
            Tags to apply.
        mode:
            One of 'o' or 'm'
            'o': Will over-write any existing tags.
            'm': Will merge in new tags with existing tags.  Incurs two remote
            calls."""

        if isinstance(self._filesystem, s3fs.S3FileSystem):
            self._filesystem.put_tags(self._gen_path(path), tags=tags, mode=mode)
        else:
            raise NotImplemented(
                f"Merge is not implmented for {type(self._fs)} use s3fs.S3FileSystem instead."
            )

    def read_block(
        self, fn: str, offset: int, length: int, delimiter: str | None = None
    ):
        """Read a block of bytes from

        Starting at ``offset`` of the file, read ``length`` bytes.  If
        ``delimiter`` is set then we ensure that the read starts and stops at
        delimiter boundaries that follow the locations ``offset`` and ``offset
        + length``.  If ``offset`` is zero then we start at zero.  The
        bytestring returned WILL include the end delimiter string.

        If offset+length is beyond the eof, reads to eof.

        Parameters
        ----------
        fn: string
            Path to filename
        offset: int
            Byte offset to start read
        length: int
            Number of bytes to read
        delimiter: bytes (optional)
            Ensure reading starts and stops at delimiter bytestring

        Examples
        --------
        >>> fs.read_block('data/file.csv', 0, 13)  # doctest: +SKIP
        b'Alice, 100\nBo'
        >>> fs.read_block('data/file.csv', 0, 13, delimiter=b'\n')  # doctest: +SKIP
        b'Alice, 100\nBob, 200\n'

        Use ``length=None`` to read to the end of the file.
        >>> fs.read_block('data/file.csv', 0, None, delimiter=b'\n')  # doctest: +SKIP
        b'Alice, 100\nBob, 200\nCharlie, 300'

        See Also
        --------
        utils.read_block"""

        self._filesystem.read_block(
            self._gen_path(fn), offset=offset, length=length, delimiter=delimiter
        )

    def rename(self, path1: str, path2: str, **kwargs):
        self._filesystem.rename(self._gen_path(path1), self._gen_path(path2), **kwargs)

    def rm(
        self, path: str | list, recursive: bool = False, maxdepth: int | None = None
    ):
        """Delete files.

        Args:
            path (str | list): File(s) to delete.
            recursive (bool, optional): If file(s) are directories, recursively
                delete contents and then also remove the directory. Defaults to False.
            maxdepth (int | None, optional): Depth to pass to walk for finding files
                to delete, if recursive. If None, there will be no limit and infinite
                recursion may be possible. Defaults to None.
        """
        if isinstance(path, str):
            path = [path]
        self._filesystem.rm(
            self._gen_paths(path), recursive=recursive, maxdepth=maxdepth
        )

    def rm_file(self, path: str):
        """Delete a file"""
        self._filesystem.rm_file(self._gen_path(path))

    def rmdir(self, path: str):
        """Remove a directory, if empty"""
        self._filesystem.rmdir(self._gen_path(path))

    def sign(self, path: str, expiration: int = 100, **kwargs):
        """Create a signed URL representing the given path

        Some implementations allow temporary URLs to be generated, as a
        way of delegating credentials.

        Parameters
        ----------
        path : str
            The path on the filesystem
        expiration : int
            Number of seconds to enable the URL for (if supported)

        Returns
        -------
        URL : str
            The signed URL

        Raises
        ------
        NotImplementedError : if method is not implemented for a filesystem"""

        return self._filesystem.sign(
            self._gen_path(path), expiration=expiration, **kwargs
        )

    def size(self, path: str):
        """Size in bytes of file"""
        return self._filesystem.size(self._gen_path(path))

    def sizes(self, paths: str | list):
        """Size in bytes of each file in a list of paths"""
        if isinstance(paths, str):
            paths = [paths]
        return self._filesystem.sizes(self._gen_paths(paths))

    def stat(self, path: str, **kwargs):
        return self._filesystem.stat(self._gen_path(path), **kwargs)

    def tail(self, path: str, size: int = 1024):
        """Get the last ``size`` bytes from file"""
        return self._filesystem.tail(self._gen_path(path), size=size)

    def touch(self, path: str, truncate: bool = True, **kwargs):
        """Create empty file, or update timestamp

        Parameters
        ----------
        path: str
            file location
        truncate: bool
            If True, always set file size to 0; if False, update timestamp and
            leave file unchanged, if backend allows this"""

        self._filesystem.touch(self._gen_path(path), truncate=truncate)

    def ukey(self, path: str):
        """Hash of file properties, to tell if it has changed"""
        return self._filesystem.ukey(self._gen_path(path))

    def upload(self, lpath: str, rpath: str, recursive: bool = False, **kwargs):
        self._filesystem.upload(
            lpath, self._gen_path(rpath), recursive=recursive, **kwargs
        )

    def url(
        self,
        path: str,
        expires: int = 3600,
        client_method: str = "get_object",
        **kwargs,
    ):
        """Generate presigned URL to access path by HTTP

        Parameters
        ----------
        path : string
            the key path we are interested in
        expires : int
            the number of seconds this signature will be good for."""
        if isinstance(self._filesystem, s3fs.S3FileSystem):
            return self._filesystem.url(
                self._gen_path(path),
                expires=expires,
                client_method=client_method,
                **kwargs,
            )
        else:
            raise NotImplemented(
                f"Merge is not implmented for {type(self._fs)} use s3fs.S3FileSystem instead."
            )

    def walk(self, path: str, maxdepth: int | None = None, **kwargs):
        """Return all files belows path

        List all files, recursing into subdirectories; output is iterator-style,
        like ``os.walk()``. For a simple list of files, ``find()`` is available.

        Note that the "files" outputted will include anything that is not
        a directory, such as links.

        Parameters
        ----------
        path: str
            Root to recurse into
        maxdepth: int
            Maximum recursion depth. None means limitless, but not recommended
            on link-based file-systems.
        kwargs: passed to ``ls``"""
        return self._strip_paths(
            self._filesystem.walk(self._gen_path(path), maxdepth=maxdepth, **kwargs)
        )

    def s5ls(
        self,
        path: str,
        detail: bool = False,
        only_objects: bool = True,
        recursive: bool = False,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        only_res: bool = True,
    ) -> tuple[list[str], Any | str, Any | str] | tuple[
        Any | str, Any | str, Any | str
    ] | None:
        """list buckets and objects.

        Args:
            path (str): Objects path.
            detail (bool, optional): Return list of objects or list with
                objects and object information. Defaults to False.
            only_objects (bool, optional): Return only objects or also paths. Defaults to True.
            recursive (bool, optional): Recursive. Defaults to False.
            global_options (str | None, optional): global options. Defaults to "--json --stat".
            operation_options (str | None, optional): operation specific options. Defaults to None.

        Returns:
            tuple[list[str], Any | str, Any | str] | tuple[ Any | str, Any | str, Any | str ] | None: _description_
        """
        if not path.startswith("s3"):
            path = "s3://" + path
        res, stat, err = self._s5.ls(
            path=path,
            detail=detail,
            only_objects=only_objects,
            recursive=recursive,
            global_options=global_options,
            operation_options=operation_options,
        )
        if only_res:
            return res
        return res, stat, err

    def s5cp(
        self,
        src: str | Path,
        dest: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        only_res: bool = True,
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Copy objects.

        Run `print_help("cp")` for more information.

        Args:
            src (str | Path): Source path.
            dest (str | Path): Destination path.
            global_options (str | None, optional): global options. Defaults to "--json --stat".
            operation_options (str | None, optional): operation specific options. Defaults to None.

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output.
        """
        res, stat, err = self._s5.cp(
            src=src,
            dest=dest,
            global_options=global_options,
            operation_options=operation_options,
        )
        if only_res:
            return res
        return res, stat, err

    def s5sync(
        self,
        src: str | Path,
        dest: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        only_res: bool = True,
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Sync objects.

        Run `print_help("sync")` for more information.

        Args:
            src (str | Path): Source path.
            dest (str | Path): Destination path.
            global_options (str | None, optional): global options. Defaults to "--json --stat".
            operation_options (str | None, optional): operation specific options. Defaults to None.

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output.
        """
        res, stat, err = self._s5.sync(
            src=src,
            dest=dest,
            global_options=global_options,
            operation_options=operation_options,
        )
        if only_res:
            return res
        return res, stat, err

    def s5rm(
        self,
        path: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        recursive: bool = False,
        only_res: bool = True,
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Remove objects.

        Run `print_help("rm")` for more information.

        Args:
            path (str | Path): Objects path.
            global_options (str | None, optional): global options. Defaults to "--json --stat".
            operation_options (str | None, optional): operation specific options. Defaults to None.
            recursive (bool, optional): Recursive. Defaults to False.

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output.
        """
        res, stat, err = self._s5.rm(
            path=path,
            global_options=global_options,
            operation_options=operation_options,
            recursive=recursive,
        )
        if only_res:
            return res
        return res, stat, err

    def s5mv(
        self,
        src: str | Path,
        dest: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        only_res: bool = True,
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Move/rename objects.

        Run `print_help("mv")` for more information.

        Args:
            src (str | Path): Source path.
            dest (str | Path): Destination path.
            global_options (str | None, optional): global options. Defaults to "--json --stat".
            operation_options (str | None, optional): operation specific options. Defaults to None.

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output.
        """
        res, stat, err = self._s5.mv(
            src=src,
            dest=dest,
            global_options=global_options,
            operation_options=operation_options,
        )
        if only_res:
            return res
        return res, stat, err

    def s5mb(
        self,
        bucket: str | Path,
        global_options: str | None = "--json --stat",
        only_res: bool = True,
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Make bucket.

        Run `print_help("mb")` for more information.

        Args:
            bucket (str | Path): Bucket name.
            global_options (str | None, optional): global options. Defaults to "--json --stat".

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output.
        """
        res, stat, err = self._s5.mb(bucket=bucket, global_options=global_options)
        if only_res:
            return res
        return res, stat, err

    def s5rb(
        self,
        bucket: str | Path,
        global_options: str | None = "--json --stat",
        only_res: bool = True,
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Remove bucket.

        Run `print_help("rb")` for more information.

        Args:
            bucket (str | Path): Bucket name.
            global_options (str | None, optional): global options. Defaults to "--json --stat".

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output
        """
        res, stat, err = self._s5.rb(bucket=bucket, global_options=global_options)
        if only_res:
            return res
        return res, stat, err

    def s5du(
        self,
        path: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = "-H",
        recursive: bool = False,
        only_res: bool = True,
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Show object size(s) usage.

        Run `print_help("du")` for more information.

        Args:
            path (str | Path): Object path(s).
            global_options (str | None, optional): global options. Defaults to "--json --stat".
            operation_options (str | None, optional): operation specific options. Defaults to "-H".
            recursive (bool, optional): Recursive. Defaults to False.

        Returns:
            tuple[Any | str, Any | str, Any | str]: Object size(s) usage.
        """
        res, stat, err = self._s5.du(
            path=path,
            global_options=global_options,
            operation_options=operation_options,
            recursive=recursive,
        )
        if only_res:
            return res
        return res, stat, err

    def s5cat(
        self, object: str | Path, only_res: bool = True
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Print remote object content

        Run `print_help("cat")` for more information.

        Args:
            object (str | Path): Object path.

        Returns:
            tuple[Any | str, Any | str, Any | str]: Object content.
        """
        res, stat, err = self._s5.cat(object=object)
        if only_res:
            return res
        return res, stat, err

    def s5run(
        self, filename: str | Path, only_res: bool = True
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Run commands in batch

        Run `print_help("run")` for more information.

        Args:
            filename (str | Path): Name of the file with the commands to run in parallel.

        Returns:
            tuple[Any | str, Any | str, Any | str]: response of the commands
        """
        res, stat, err = self._s5.run(filename=filename)
        if only_res:
            return res
        return res, stat, err
