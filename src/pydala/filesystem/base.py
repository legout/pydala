import fsspec
import pyarrow.fs as pafs

from .s5cmd import S5CmdFileSystem


def get_fsspec_filesystem(
    protocol: str = "s3",
    profile: str | None = None,
    endpoint_url: str | None = None,
    **storage_options
):
    if protocol.lower() == "local":
        protocol = "file"

    if protocol.lower().startswith("s3"):
        key = storage_options.pop("key", None) or storage_options.pop("username", None)
        secret = storage_options.pop("secret", None) or storage_options.pop(
            "password", None
        )
        token = storage_options.pop("token", None) or storage_options.pop(
            "session_token", None
        )
        region = storage_options.pop("region", None)

        fsspec_filesystem = S5CmdFileSystem(
            key=key,
            secret=secret,
            token=token,
            endpoint_url=endpoint_url,
            profile=profile,
            region=region,
        )

    elif protocol.lower().startswith("gcs") or protocol.lower().startswith("gs"):
        import gcsfs

        endpoint_url = endpoint_url or storage_options.pop("endpoint_url", None)
        token = storage_options.pop("token", None)
        default_location = storage_options.pop("default_location", None)
        fsspec_filesystem = gcsfs.GCSFileSystem(
            token=token,
            endpoint_url=endpoint_url,
            default_location=default_location,
            **storage_options,
        )

    elif protocol.lower().startswith("az") or protocol.lower().startswith("abfs"):
        import adlfs

        fsspec_filesystem = adlfs.AzureBlobFileSystem(
            **storage_options,
        )

    elif protocol.lower().startswith("hdfs") or protocol.lower().startswith(
        "arrow_hdfs"
    ):
        host = storage_options.pop("host", "default")
        port = storage_options.pop("port", 0)
        user = storage_options.pop("user", None)
        kerb_ticket = storage_options.pop("kerb_ticket", None)
        extra_conf = storage_options.pop("extra_cong", None)
        fsspec_filesystem = fsspec.filesystem(
            "hdfs",
            host=host,
            port=port,
            user=user,
            kerb_ticket=kerb_ticket,
            extra_conf=extra_conf,
            **storage_options,
        )

    else:
        fsspec_filesystem = fsspec.filesystem(protocol=protocol, **storage_options)

    return fsspec_filesystem


def get_pyarrow_filesystem(
    protocol: str = "s3", endpoint_url: str | None = None, **storage_options
):
    if protocol.lower() == "local":
        protocol = "file"

    if protocol.lower().startswith("s3"):
        key = storage_options.pop("key", None) or storage_options.pop("username", None)
        secret = storage_options.pop("secret", None) or storage_options.pop(
            "password", None
        )
        token = storage_options.pop("token", None) or storage_options.pop(
            "session_token", None
        )
        region = storage_options.pop("region", None)

        pyarrow_filesystem = pafs.S3FileSystem(
            access_key=key,
            secret_key=secret,
            session_token=token,
            region=region,
            endpoint_override=endpoint_url,
            **storage_options,
        )

    elif protocol.lower().startswith("gcs") or protocol.lower().startswith("gs"):

        endpoint_url = endpoint_url or storage_options.pop("endpoint_url", None)
        token = storage_options.pop("token", None)
        default_location = storage_options.pop("default_location", None)

        pyarrow_filesystem = pafs.GcsFileSystem(
            access_token=token,
            default_bucket_location=default_location,
            endpoint_override=endpoint_url,
            **storage_options,
        )

    elif protocol.lower().startswith("hdfs") or protocol.lower().startswith(
        "arrow_hdfs"
    ):
        host = storage_options.pop("host", "default")
        port = storage_options.pop("port", 0)
        user = storage_options.pop("user", None)
        kerb_ticket = storage_options.pop("kerb_ticket", None)
        extra_conf = storage_options.pop("extra_cong", None)

        pyarrow_filesystem = pafs.HadoopFileSystem(
            host=host,
            port=port,
            user=user,
            kerb_ticket=kerb_ticket,
            extra_conf=extra_conf,
            **storage_options,
        )
    else:
        fsspec_filesystem = get_fsspec_filesystem(protocol=protocol, **storage_options)
        pyarrow_filesystem = pafs.PyFileSystem(
            pafs.FSSpecHandler(fsspec_filesystem),
        )

    return pyarrow_filesystem
