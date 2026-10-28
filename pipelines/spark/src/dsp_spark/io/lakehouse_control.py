"""Small control plane; cooperative single writer, immutable data generations.

S3 locks never expire or auto-steal. A crashed owner requires supervised recovery
after proving its Spark driver is dead. Every writer must use this same root.
"""

import fcntl
import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from dsp_collector.runtime.outbox import atomic_json

from .s3 import storage_uri


def writable_root(root):
    root = storage_uri(root)
    if urlsplit(root).scheme == "s3a" and not urlsplit(root).path.strip("/"):
        raise ValueError("a dedicated lakehouse prefix is required")
    if "bootstrap" in unquote(urlsplit(root).path).lower().split("/"):
        raise ValueError("bootstrap snapshot is immutable")
    return root


def safe_key(key):
    if (
        not key
        or key.startswith("/")
        or any(p in ("", ".", "..") for p in unquote(key).split("/"))
    ):
        raise ValueError("invalid control key")
    return key


class LocalControl:
    def __init__(self, root):
        self.root = Path(unquote(urlsplit(writable_root(root)).path))

    def get(self, key):
        path = self.root / safe_key(key)
        if not path.is_file():
            return None, None
        raw = path.read_bytes()
        return json.loads(raw), hashlib.sha256(raw).hexdigest()

    def put(self, key, value, *, expected=None, absent=False):
        _, etag = self.get(key)
        if (absent and etag is not None) or (expected is not None and etag != expected):
            raise RuntimeError("control compare-and-set conflict")
        atomic_json(self.root / safe_key(key), value)
        return self.get(key)[1]

    @contextmanager
    def writer(self):
        self.root.mkdir(parents=True, exist_ok=True)
        with (self.root / "writer.lock").open("a") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("lakehouse writer already active") from None
            yield


class S3Control:
    def __init__(self, root, client):
        parts = urlsplit(writable_root(root))
        self.bucket, self.prefix, self.client = (
            parts.netloc,
            parts.path.strip("/"),
            client,
        )

    def key(self, key):
        return self.prefix + "/control/" + safe_key(key)

    def get(self, key):
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self.key(key))
        except Exception as exc:
            code = getattr(exc, "response", {}).get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                return None, None
            raise
        return json.loads(response["Body"].read()), response["ETag"]

    def put(self, key, value, *, expected=None, absent=False):
        conditions = {"IfNoneMatch": "*"} if absent else {}
        if expected is not None:
            conditions["IfMatch"] = expected
        response = self.client.put_object(
            Bucket=self.bucket,
            Key=self.key(key),
            Body=json.dumps(value, sort_keys=True, allow_nan=False).encode(),
            ContentType="application/json",
            **conditions,
        )
        return response["ETag"]

    @contextmanager
    def writer(self):
        owner = uuid4().hex
        # Conditional creation provides cross-process/host exclusion. No TTL:
        # an old Spark driver can never outlive a lease and silently resume.
        etag = self.put("writer.lock", {"owner": owner}, absent=True)
        try:
            yield
        finally:
            self.client.delete_object(
                Bucket=self.bucket, Key=self.key("writer.lock"), IfMatch=etag
            )


def control_store(root, *, profile=None, region=None):
    if storage_uri(root).startswith("file:"):
        return LocalControl(root)
    import boto3

    return S3Control(
        root, boto3.Session(profile_name=profile, region_name=region).client("s3")
    )
