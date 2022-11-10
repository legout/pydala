import json
import os
import subprocess
from pathlib import Path
from typing import Any

from aiobotocore.session import AioSession
from s3fs import S3FileSystem


class S5CmdFileSystem(S3FileSystem):
    def __init__(
        self,
        anon: bool = False,
        key: str | None = None,
        secret: str | None = None,
        token: str | None = None,
        use_ssl: bool = True,
        client_kwargs: dict | None = None,
        requester_pays: bool = False,
        default_block_size: int | None = None,
        default_fill_cache: bool = True,
        default_cache_type: str | None = "bytes",
        version_aware: bool = False,
        config_kwargs: dict | None = None,
        s3_additional_kwargs: dict | None = None,
        session:AioSession|None=None,
        username: str | None = None,
        password: str | None = None,
        cache_regions: bool = False,
        asynchronous: bool = False,
        loop=None,
        endpoint_url: str | None = None,
        profile: str | None = None,
        region: str | None = None,
        **kwargs,
    ):
        if endpoint_url is not None:
            os.environ["S3_ENDPOINT_URL"] = endpoint_url
            if client_kwargs is None:
                client_kwargs = dict(endpoint_url=endpoint_url)
            else:
                client_kwargs["endpoint_url"] = endpoint_url

        if region is not None:
            if client_kwargs is None:
                client_kwargs = dict(region_name=region)
            else:
                client_kwargs["region_name"] = region
        

        if profile is not None:
            os.environ["AWS_PROFILE"] = profile            

        super().__init__(
            anon=anon,
            key=key,
            secret=secret,
            token=token,
            use_ssl=use_ssl,
            client_kwargs=client_kwargs,
            requester_pays=requester_pays,
            default_block_size=default_block_size,
            default_fill_cache=default_fill_cache,
            default_cache_type=default_cache_type,
            version_aware=version_aware,
            config_kwargs=config_kwargs,
            s3_additional_kwargs=s3_additional_kwargs,
            session=session,
            username=username,
            password=password,
            cache_regions=cache_regions,
            asynchronous=asynchronous,
            loop=loop,
            profile=profile,
            **kwargs,
        )

    def _gen_path(self, path: str | Path, recursive: bool = False) -> str | Path:
        """Generates path based on the given parameters and the initial bucket name"""

        if isinstance(path, Path):
            path = path.as_posix()

        if path.endswith("/*"):
            recursive = False

        if "*" in path:
            recursive = False

        if "s3" in path:
            path = "s3://" + path.lstrip("s3").lstrip(":").lstrip("/")

        if recursive:
            if not path.endswith("/*"):
                path = path.rstrip("/") + "/*"

        else:
            if not path.endswith("*") and not "." in path:
                path = path.rstrip("/") + "/"

        return path

    @staticmethod
    def _format_json_error(stderr: bytes) -> Any | str:
        """Formats the json stderr."""
        stderr: str = stderr.decode().strip()
        if len(stderr) > 0:

            return json.loads(stderr)
        else:
            return stderr

    @staticmethod
    def _format_error(stderr: bytes) -> str:
        """Formats the stderr"""

        stderr: str = stderr.decode().strip()
        return stderr

    @staticmethod
    def _format_json_output(stdout: bytes) -> tuple[list[Any] | str, list[Any]]:
        """Formats the json stdout"""
        stdout: str = stdout.decode().strip()

        if len(stdout) > 0:
            stdout_json = [json.loads(e) for e in stdout.split("\n")]
            res = [
                out
                for out in stdout_json
                if sorted(out.keys()) != ["error", "operation", "success"]
            ]
            stat = [
                out
                for out in stdout_json
                if sorted(out.keys()) == ["error", "operation", "success"]
            ]

        else:
            res = stdout
            stat = []

        return res, stat

    def _format_output(stdout: bytes) -> str:
        """Formats the stdout"""
        stdout: str = stdout.decode().strip()

        return stdout

    def _run_command(self, operation_command: str, global_options: str | None = None):

        """Function runs the s5cmd operations and formats the stdout and stderr"""

        if self.has_s5cmd:

            global_options = global_options or ""
            final_command = " ".join(["s5cmd", global_options, operation_command])
            print(final_command)

            response = subprocess.run(final_command, shell=True, capture_output=True)
            if "--json" in final_command:
                # return response, None, None
                res, stat = self._format_json_output(response.stdout)
                err = self._format_json_error(response.stderr)

                return res, stat, err
            else:
                res = self._format_output(response.stdout)
                err = self._format_error(response.stderr)
                return res, None, err

        else:
            raise FileNotFoundError(
                "s5cmd not found. See here for more information https://github.com/peak/s5cmd."
            )

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

    def print_s5cmd_help(self, operation: str = "cp") -> None:
        if self.has_s5cmd:
            resp = subprocess.run(
                f"s5cmd {operation} -h", shell=True, capture_output=True
            )
            if len(resp.stdout) > 0:
                print(resp.stdout.decode().strip())
            else:
                print(resp.stderr.decode().strip())
        else:
            raise FileNotFoundError(
                "s5cmd not found. See here for more information https://github.com/peak/s5cmd."
            )

    def s5ls(
        self,
        path: str | Path,
        detail: bool = False,
        only_objects: bool = False,
        recursive: bool = False,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        full_output: bool = False,
    ):
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
        if not "s3" in path:
            path = "s3://" + path
        path = self._gen_path(path, recursive=recursive)
        operation_options = operation_options or ""
        operation_command = " ".join(["ls", operation_options, path])

        res, stat, err = self._run_command(
            operation_command=operation_command,
            global_options=global_options,
        )
        if len(res) > 0:
            if not detail:
                if only_objects:
                    res = [
                        r["key"].split("s3://")[-1] for r in res if r["type"] == "file"
                    ]

                else:
                    res = [r["key"].split("s3://")[-1] for r in res]

        if full_output:
            return res, stat, err

        return res

    # def s3ls(self, *args, **kwargs):
    #     return super().ls(*args, **kwargs)

    # def ls(self, *args, **kwargs):
    #     if self.has_s5cmd:
    #         return self.s5ls(*args, **kwargs)
    #     else:
    #         return self.s3ls(*args, **kwargs)

    def s5cp(
        self,
        src: str | Path,
        dest: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        recursive: bool = True,
        full_output: bool = False,
    ):
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
        # if not "s3" in src:
        #    src = "s3://" + src
        src = self._gen_path(src, recursive=recursive)
        # if "s3" in dest:
        #    dest = "s3://" + src
        dest = self._gen_path(dest)

        operation_options: str = operation_options or ""
        operation_command = f" cp {operation_options} {src} {dest}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=global_options
        )
        if full_output:
            return res, stat, err
        return res

    # def s3cp(self, *args, **kwargs):
    #     super().cp(*args, **kwargs)

    # def cp(self, *args, **kwargs) -> None:
    #     if self.has_s5cmd:
    #         self.s5cp(*args, **kwargs)
    #     else:
    #         self.s3cp(*args, **kwargs)

    def sync(
        self,
        src: str | Path,
        dest: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        full_output: bool = False,
        recursive: bool = False,
    ):
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
        if self.has_s5cmd:
            # if not "s3" in src:
            #    src = "s3://" + src
            src = self._gen_path(src, recursive=recursive)
            # if "s3" in dest:
            #    dest = "s3://" + src
            dest = self._gen_path(dest)

            operation_options: str = operation_options or ""
            operation_command = f" sync {operation_options} {src} {dest}"
            res, stat, err = self._run_command(
                operation_command=operation_command, global_options=global_options
            )
            if full_output:
                return res, stat, err
            else:
                return res
        else:
            NotImplementedError(
                """Install s5cmd to use this function. 
                See here for more information https://github.com/peak/s5cmd."""
            )

    def s5rm(
        self,
        path: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        recursive: bool = False,
        full_output: bool = False,
    ):
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
        if not "s3" in path:
            path = "s3://" + path
        path = self._gen_path(path, recursive=recursive)
        operation_options: str = operation_options or ""
        operation_command = f" rm {operation_options} {path}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=global_options
        )
        if full_output:
            return res, stat, err
        else:
            return res

    # def s3rm(self, *args, **kwargs):
    #     return super().rm(*args, **kwargs)

    # def rm(self, *args, **kwargs) -> None:
    #     if self.has_s5cmd:
    #         self.s5rm(*args, **kwargs)
    #     else:
    #         self.s3rm(*args, **kwargs)

    def s5mv(
        self,
        src: str | Path,
        dest: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        recursive: bool = True,
        full_output: bool = False,
    ):
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

        # if not "s3" in src:
        #   src = "s3://" + src
        src = self._gen_path(src, recursive=recursive)
        # if "s3" in dest:
        #    dest = "s3://" + src
        dest = self._gen_path(dest)

        operation_options: str = operation_options or ""
        operation_command = f" mv {operation_options} {src} {dest}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=global_options
        )
        if full_output:
            return res, stat, err
        else:
            return res

    # def s3mv(self, *args, **kwargs):
    #     super().mv(*args, **kwargs)

    # def mv(self, *args, **kwargs) -> None:
    #     if self.has_s5cmd:
    #         path1 = kwargs.pop("src", None)
    #         if path1 is None:
    #             path1 = args[0]
    #             args = args[1:]
    #         if not "s3" in path1:
    #             path1 = "s3://" + path1

    #         path2 = kwargs.pop("dest", None)
    #         if path2 is None:
    #             path2 = args[0]
    #             args = args[1:]
    #         if not "s3" in path2:
    #             path2 = "s3://" + path2

    #         self.s5mv(path1, path2, *args, **kwargs)
    #     else:
    #         self.s3mv(*args, **kwargs)

    def mb(
        self,
        bucket: str | Path,
        global_options: str | None = "--json --stat",
        full_output: bool = False,
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Make bucket.

        Run `print_help("mb")` for more information.

        Args:
            bucket (str | Path): Bucket name.
            global_options (str | None, optional): global options. Defaults to "--json --stat".

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output.
        """
        if self.has_s5cmd:
            if isinstance(bucket, Path):
                bucket: str | Path = bucket.as_posix()
            if "s3" in bucket:
                bucket = bucket.lstrip("s3").lstrip(":").lstrip("/")
            bucket = bucket.rstrip("*").rstrip("/")
            bucket = f"s3://{bucket}"

            operation_command = f"mb {bucket}"
            res, stat, err = self._run_command(
                operation_command=operation_command, global_options=global_options
            )
            if full_output:
                return res, stat, err
            return res
        else:
            NotImplementedError(
                """Install s5cmd to use this function. 
                See here for more information https://github.com/peak/s5cmd."""
            )

    def rb(
        self,
        bucket: str | Path,
        global_options: str | None = "--json --stat",
        full_output: bool = False,
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Remove bucket.

        Run `print_help("rb")` for more information.

        Args:
            bucket (str | Path): Bucket name.
            global_options (str | None, optional): global options. Defaults to "--json --stat".

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output
        """
        if self.has_s5cmd:
            if isinstance(bucket, Path):
                bucket: str | Path = bucket.as_posix()
            if "s3" in bucket:
                bucket = bucket.lstrip("s3").lstrip(":").lstrip("/")
            bucket = bucket.rstrip("*").rstrip("/")
            bucket = f"s3://{bucket}"

            operation_command = f"rb {bucket}"
            res, stat, err = self._run_command(
                operation_command=operation_command, global_options=global_options
            )
            if full_output:
                return res, stat, err
            return res

    def select(self):
        pass

    def s5du(
        self,
        path: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = "-H",
        recursive: bool = False,
        full_output: bool = False,
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
        if not "s3" in path:
            path = "s3://" + path
        path = self._gen_path(path, recursive=recursive)
        operation_options: str = operation_options or ""
        operation_command = f" du {operation_options} {path}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=global_options
        )
        if full_output:
            return res, stat, err
        return res

    # def s3du(self, *args, **kwargs):
    #     return super().du(*args, **kwargs)

    # def du(self, *args, **kwargs):
    #     if self.has_s5cmd:
    #         # path = kwargs.get("path", None) or args[0]
    #         # path = "s3://" + path if not "s3" in path else path
    #         return self.s5du(*args, **kwargs)
    #     else:
    #         return self.s3du(*args, **kwargs)

    def s5cat(self, object: str | Path) -> tuple[Any | str, Any | str, Any | str]:
        """Print remote object content

        Run `print_help("cat")` for more information.

        Args:
            object (str | Path): Object path.

        Returns:
            tuple[Any | str, Any | str, Any | str]: Object content.
        """
        if not "s3" in object:
            object = "s3://" + object
        object = self._gen_path(object)

        resp = subprocess.run(f"s5cmd cat {object}", shell=True, capture_output=True)
        if len(resp.stdout) > 0:
            return resp.stdout
        else:
            print(resp.stderr.decode().strip())

    # def s3cat(self, *args, **kwargs):
    #     return super().cat(*args, **kwargs)

    # def cat(self, *args, **kwargs):
    #     if self.has_s5cmd:
    #         return self.s5cat(*args, **kwargs)
    #     else:
    #         return self.s3cat(*args, **kwargs)

    def run(self, filename: str | Path) -> tuple[Any | str, Any | str, Any | str]:
        """Run commands in batch

        Run `print_help("run")` for more information.

        Args:
            filename (str | Path): Name of the file with the commands to run in parallel.

        Returns:
            tuple[Any | str, Any | str, Any | str]: response of the commands
        """
        operation_command = f" run {filename}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=""
        )
        return res, stat, err

    # def s3get(self, *args, **kwargs):
    #     super().get(*args, **kwargs)

    # def get(
    #     self,
    #     rpath: str,
    #     lpath: str,
    #     recursive: bool = False,
    #     callback: ... = None,
    #     **kwargs,
    # ):
    #     if self.has_s5cmd:
    #         if not "s3" in rpath:
    #             rpath = "s3://" + rpath
    #         self.s5cp(rpath, lpath)
    #     else:
    #         self.s3get(rpath, lpath, recursive, callback, **kwargs)

    # def download(self, rpath, lpath, recursive=False, **kwargs):
    #     self.get(rpath, lpath, recursive, **kwargs)

    # def s3put(self, *args, **kwargs):
    #     super().put(*args, **kwargs)

    # def put(self, lpath, rpath, recursive=False, callback=..., **kwargs):
    #     if self.has_s5cmd:
    #         if not "s3" in rpath:
    #             rpath = "s3://" + rpath
    #         self.s5cp(lpath, rpath)
    #     else:
    #         self.s3put(lpath, rpath, recursive, callback, **kwargs)

    # def upload(self, lpath, rpath, recursive=False, **kwargs):
    #     self.put(lpath, rpath, recursive, **kwargs)

    @property
    def version(self) -> None:
        """Prints the s5cmd version."""
        if self.has_s5cmd:
            resp = subprocess.run(f"s5cmd version", shell=True, capture_output=True)
            if len(resp.stdout) > 0:
                print(resp.stdout.decode().strip())
            else:
                print(resp.stderr.decode().strip())
        else:
            raise FileNotFoundError(
                "s5cmd not found. See here for more information https://github.com/peak/s5cmd."
            )
