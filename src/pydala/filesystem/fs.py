import fsspec
import pyarrow.fs as pafs
from fsspec.implementations.dirfs import DirFileSystem
from fsspec.spec import AbstractFileSystem
from s3fs import S3FileSystem


def get_fsspec_filesystem(
    protocol: str = "s3",
    profile: str | None = None,
    endpoint_url: str | None = None,
    **storage_options,
):
    if protocol is None:
        protocol="file"
        
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

        client_kwargs = storage_options.pop("client_kwargs", None)

        if client_kwargs is not None:
            if endpoint_url is not None:
                client_kwargs["endpoint_url"] = endpoint_url

            if region is not None:
                client_kwargs["region"] = region

        fsspec_filesystem = S3FileSystem(
            key=key,
            secret=secret,
            token=token,
            client_kwargs=client_kwargs,
            profile=profile,
            **storage_options,
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


def get_fsspec_dir_filesystem(
    path: str,
    filesystem: AbstractFileSystem | None = None,
    protocol: str | None = None,
    profile: str | None = None,
    endpoint_url: str | None = None,
    **storage_options,
):
    filesystem = filesystem or get_fsspec_filesystem(
        protocol=protocol, profile=profile, endpoint_url=endpoint_url, **storage_options
    )
    return DirFileSystem(path=path, fs=filesystem)


def get_pyarrow_filesystem(
    protocol: str = "s3", endpoint_url: str | None = None, **storage_options
):  
    if protocol is None:
        protocol="file"
        
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
        fsspec_filesystem_ = get_fsspec_filesystem(protocol=protocol, **storage_options)
        pyarrow_filesystem = pafs.PyFileSystem(
            pafs.FSSpecHandler(fsspec_filesystem_),
        )

    return pyarrow_filesystem


def get_pyarrow_subtree_filesystem(
    path: str,
    filesystem: pafs.SubTreeFileSystem | None = None,
    protocol: str | None = None,
    profile: str | None = None,
    endpoint_url: str | None = None,
    **storage_options,
):
    filesystem = filesystem or get_pyarrow_filesystem(
        protocol=protocol, profile=profile, endpoint_url=endpoint_url, **storage_options
    )
    return pafs.SubTreeFileSystem(base_path=path, base_fs=filesystem)
