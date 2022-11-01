import configparser
import os
from pathlib import Path


class AwsCredentialsManager:
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
    def _load_credentials(filename: str | Path, profile: str) -> dict[str, str]:
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

    def set_profile(self, profile: str) -> None:
        self._export_env(profile=profile)
