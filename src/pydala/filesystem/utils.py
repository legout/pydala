from ensurepip import version
from glob import glob
import json
import subprocess
from pathlib import Path
from typing import Any
import os
import sys

from .aws import AwsCredentialsManager


class S5CMD(AwsCredentialsManager):
    def __init__(
        self,
        bucket: str | None = None,
        profile: str = "default",
        credentials: str | Path | dict[str, str] = "~/.aws/credentials",
    ) -> None:
        super().__init__(profile=profile, credentials=credentials)
        if bucket is not None:
            self._bucket = bucket if bucket.startswith("s3://") else f"s3://{bucket}"
        else:
            self._bucket = None

    def _gen_path(self, path: str | Path, recursive: bool = False) -> str | Path:
        """Generates path based on the given parameters and the initial bucket name"""

        if isinstance(path, Path):
            path = path.as_posix()

        if path[-1] == "*":
            recursive = False

        if "s3" in path:
            path = path.lstrip("s3").lstrip(":").lstrip("/")

            if self._bucket is not None:
                path = os.path.join(self._bucket, path)

            if not path.startswith("s3"):
                path = "s3://" + path

        if recursive:
            if not path.endswith("/*"):
                path = path.rstrip("/") + "/*"
        else:
            if not path.endswith("*"):
                path = path.rstrip("/") + "/"

        return path

    def _strip_bucket(self, path: str) -> str:
        "Strips the bucket name from the path"
        if self._bucket is not None:
            return path.split(self._bucket)[-1].lstrip("/")
        else:
            return path

    @staticmethod
    def _format_json_error(stderr: bytes) -> dict[str, str] | str | None:
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
    def _format_json_output(stdout: bytes) -> list[Any] | None:
        """Formats the json stdout"""
        stdout: str = stdout.decode().strip()

        if len(stdout) > 0:
            stdout_json = [json.loads(e) for e in stdout.split()]
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

    def _run_command(
        self,
        operation_command: str,
        global_options: str | None = None,
    ) -> list[Any] | str:
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
    def has_s5cmd(self):
        if not hasattr(self, "_has_s5cmd"):
            self._has_s5cmd = self._check_for_s5cmd()

        return self._has_s5cmd

    def print_help(self, operation: str = "cp"):
        if self._has_s5cmd:
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

    def ls(
        self,
        path: str,
        detail: bool = False,
        only_objects: bool = True,
        recursive: bool = False,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
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
                    return (
                        list(
                            map(
                                self._strip_bucket,
                                [r["key"] for r in res if r["type"] == "file"],
                            )
                        ),
                        stat,
                        err,
                    )
                else:
                    return (
                        list(map(self._strip_bucket, [r["key"] for r in res])),
                        stat,
                        err,
                    )
            else:
                _ = {r.update({"key": self._strip_bucket(r["key"])}) for r in res}
                return res, stat, err
        else:
            return res, stat, err

    def cp(
        self,
        src: str | Path,
        dest: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
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

        if "s3" in src:
            src = self._gen_path(src, recursive=True)
        if "s3" in dest:
            dest = self._gen_path(dest)

        operation_options: str = operation_options or ""
        operation_command = f" cp {operation_options} {src} {dest}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=global_options
        )
        return res, stat, err

    def sync(
        self,
        src: str | Path,
        dest: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
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

        if "s3" in src:
            src = self._gen_path(src, recursive=True)
        if "s3" in dest:
            dest = self._gen_path(dest)
        operation_options: str = operation_options or ""
        operation_command = f" sync {operation_options} {src} {dest}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=global_options
        )
        return res, stat, err

    def rm(
        self,
        path: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
        recursive: bool = False,
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

        path = self._gen_path(path, recursive=recursive)
        operation_options: str = operation_options or ""
        operation_command = f" rm {operation_options} {path}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=global_options
        )
        return res, stat, err

    def mv(
        self,
        src: str | Path,
        dest: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = None,
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

        if "s3" in src:
            src = self._gen_path(src, recursive=True)
        if "s3" in dest:
            dest = self._gen_path(dest)

        operation_options: str = operation_options or ""
        operation_command = f" mv {operation_options} {src} {dest}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=global_options
        )
        return res, stat, err

    def mb(
        self, bucket: str | Path, global_options: str | None = "--json --stat"
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Make bucket.

        Run `print_help("mb")` for more information.

        Args:
            bucket (str | Path): Bucket name.
            global_options (str | None, optional): global options. Defaults to "--json --stat".

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output.
        """
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
        return res, stat, err

    def rb(
        self, bucket: str | Path, global_options: str | None = "--json --stat"
    ) -> tuple[Any | str, Any | str, Any | str]:
        """Remove bucket.

        Run `print_help("rb")` for more information.

        Args:
            bucket (str | Path): Bucket name.
            global_options (str | None, optional): global options. Defaults to "--json --stat".

        Returns:
            tuple[Any | str, Any | str, Any | str]: Operation output
        """
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
        return res, stat, err

    def select(self):
        pass

    def du(
        self,
        path: str | Path,
        global_options: str | None = "--json --stat",
        operation_options: str | None = "-H",
        recursive: bool = False,
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
        path = self._gen_path(path, recursive=recursive)
        operation_options: str = operation_options or ""
        operation_command = f" du {operation_options} {path}"
        res, stat, err = self._run_command(
            operation_command=operation_command, global_options=global_options
        )
        return res, stat, err

    def cat(self, object: str | Path) -> tuple[Any | str, Any | str, Any | str]:
        """Print remote object content

        Run `print_help("cat")` for more information.

        Args:
            object (str | Path): Object path.

        Returns:
            tuple[Any | str, Any | str, Any | str]: Object content.
        """
        object = self._gen_path(object)

        resp = subprocess.run(f"s5cmd cat {object}", shell=True, capture_output=True)
        if len(resp.stdout) > 0:
            return resp.stdout
        else:
            print(resp.stderr.decode().strip())

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

    @property
    def version(self) -> None:
        """Prints the s5cmd version."""
        if self._has_s5cmd:
            resp = subprocess.run(f"s5cmd version", shell=True, capture_output=True)
            if len(resp.stdout) > 0:
                print(resp.stdout.decode().strip())
            else:
                print(resp.stderr.decode().strip())
        else:
            raise FileNotFoundError(
                "s5cmd not found. See here for more information https://github.com/peak/s5cmd."
            )
