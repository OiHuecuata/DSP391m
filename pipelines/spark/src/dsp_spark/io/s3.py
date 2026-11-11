"""Storage URI validation and S3A configuration; Spark owns all cloud I/O."""

from pathlib import Path
from urllib.parse import urlsplit, urlunsplit


def storage_uri(value: str) -> str:
    parts = urlsplit(value)
    if not parts.scheme:
        return Path(value).expanduser().resolve().as_uri()
    if parts.scheme not in ("file", "s3a") or parts.query or parts.fragment:
        raise ValueError("storage must be a file or s3a URI without query/fragment")
    if parts.scheme == "s3a" and (not parts.netloc or "@" in parts.netloc):
        raise ValueError("s3a URI requires a bucket and must not contain credentials")
    if parts.scheme == "file" and (
        parts.netloc not in ("", "localhost") or not parts.path.startswith("/")
    ):
        raise ValueError("file URI must be local and absolute")
    if any(part in (".", "..") for part in parts.path.split("/")):
        raise ValueError("storage URI must not contain traversal segments")
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def s3a_options(
    *,
    endpoint: str | None = None,
    region: str | None = None,
    path_style: bool = False,
    profile: str | None = None,
) -> dict[str, str]:
    options = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    }
    if endpoint:
        parts = urlsplit(endpoint)
        if (
            parts.scheme not in ("http", "https")
            or not parts.netloc
            or parts.username
            or parts.password
        ):
            raise ValueError("S3 endpoint must be http(s) and contain no credentials")
        options["spark.hadoop.fs.s3a.endpoint"] = endpoint
        options["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(
            parts.scheme == "https"
        ).lower()
    if region:
        options["spark.hadoop.fs.s3a.endpoint.region"] = region
    if profile:
        import re

        if not re.fullmatch(r"[A-Za-z0-9_.-]+", profile):
            raise ValueError("invalid AWS profile name")
        # SDK v2 reads credential_process and refreshes expiring session tokens.
        # Profile files and the process executable must exist on every node.
        options["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
            "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
        )
        options["spark.executorEnv.AWS_PROFILE"] = profile
        options["spark.executor.extraJavaOptions"] = f"-Daws.profile={profile}"
        options["spark.driver.extraJavaOptions"] = f"-Daws.profile={profile}"
    if path_style:
        options["spark.hadoop.fs.s3a.path.style.access"] = "true"
    return options
