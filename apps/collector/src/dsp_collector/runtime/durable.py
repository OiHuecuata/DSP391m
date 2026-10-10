"""Immutable raw/handoff storage. S3 conditional creation is the production adapter."""

import hashlib
import os
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlsplit


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


class ArtifactStore:
    def __init__(self, base_uri, *, s3_client=None):
        self.base = base_uri.rstrip("/")
        if self.base.startswith("s3://"):
            self.base = "s3a://" + self.base.removeprefix("s3://")
        parsed = urlsplit(self.base)
        if (
            parsed.scheme not in ("file", "s3", "s3a")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Use file:// development storage or s3:// operational storage"
            )
        if parsed.scheme == "file" and parsed.netloc not in ("", "localhost"):
            raise ValueError("Remote file authority denied")
        self.s3 = s3_client

    def _client(self):
        if self.s3 is None:
            import boto3

            self.s3 = boto3.client("s3")
        return self.s3

    def uri(self, key):
        if not key or any(p in ("", ".", "..") for p in key.split("/")) or "\\" in key:
            raise ValueError("Unsafe artifact key")
        return self.base + "/" + key

    def read(self, uri):
        if not uri.startswith(self.base + "/"):
            raise ValueError("Artifact outside configured storage prefix")
        p = urlsplit(uri)
        if (
            p.query
            or p.fragment
            or ".." in unquote(p.path).split("/")
            or "\\" in unquote(p.path)
        ):
            raise ValueError("Unsafe artifact URI")
        if p.scheme == "file":
            path = Path(unquote(p.path))
            if not path.resolve().is_relative_to(
                Path(unquote(urlsplit(self.base).path)).resolve()
            ):
                raise ValueError("Artifact resolves outside configured storage")
            return path.read_bytes()
        return (
            self._client()
            .get_object(Bucket=p.netloc, Key=p.path.lstrip("/"))["Body"]
            .read()
        )

    def optional(self, uri):
        try:
            return self.read(uri)
        except FileNotFoundError:
            return None
        except Exception as error:
            if getattr(error, "response", {}).get("Error", {}).get("Code") in (
                "NoSuchKey",
                "404",
            ):
                return None
            raise

    def list(self, prefix):
        location = self.uri(prefix)
        parsed = urlsplit(location)
        if parsed.scheme == "file":
            directory = Path(unquote(parsed.path))
            return sorted(path.as_uri() for path in directory.glob("*.json"))
        paginator = self._client().get_paginator("list_objects_v2")
        return sorted(
            f"s3a://{parsed.netloc}/{item['Key']}"
            for page in paginator.paginate(
                Bucket=parsed.netloc, Prefix=parsed.path.lstrip("/") + "/"
            )
            for item in page.get("Contents", [])
            if item["Key"].endswith(".json")
        )

    def put(self, key, raw):
        if not isinstance(raw, bytes):
            raise TypeError("Exact raw bytes required")
        uri = self.uri(key)
        p = urlsplit(uri)
        if ".." in unquote(p.path).split("/") or "\\" in unquote(p.path):
            raise ValueError("Unsafe artifact key encoding")
        if p.scheme == "file":
            path = Path(unquote(p.path))
            if not path.resolve().is_relative_to(
                Path(unquote(urlsplit(self.base).path)).resolve()
            ):
                raise ValueError("Artifact write resolves outside configured storage")
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".durable-")
            try:
                with os.fdopen(fd, "wb") as output:
                    output.write(raw)
                    output.flush()
                    os.fsync(output.fileno())
                try:
                    os.link(
                        temporary, path
                    )  # Atomic no-replace; retries compare bytes.
                except FileExistsError:
                    pass
                directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                os.unlink(temporary)
        else:
            try:
                self._client().put_object(
                    Bucket=p.netloc,
                    Key=p.path.lstrip("/"),
                    Body=raw,
                    IfNoneMatch="*",
                    Metadata={"sha256": sha256(raw)},
                )
            except Exception as error:
                if getattr(error, "response", {}).get("ResponseMetadata", {}).get(
                    "HTTPStatusCode"
                ) not in (409, 412):
                    raise
        if self.read(uri) != raw:
            raise ValueError("Immutable artifact conflict or readback failure")
        return {
            "artifact_uri": uri,
            "content_sha256": sha256(raw),
            "byte_count": len(raw),
        }
