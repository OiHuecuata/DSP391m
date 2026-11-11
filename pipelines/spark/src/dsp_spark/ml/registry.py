"""Immutable run artifacts, manifest committed last. All paths must be staging."""

import hashlib
import json
from importlib.metadata import version
from pathlib import Path
from urllib.parse import unquote, urlsplit

from .config import ARTIFACT_VERSION, staging_uri

RUNTIME_PACKAGES = (
    "pyspark",
    "delta-spark",
    "numpy",
    "scipy",
    "scikit-learn",
    "umap-learn",
    "joblib",
    "numba",
    "pynndescent",
)


def runtime_versions():
    from importlib.metadata import PackageNotFoundError

    result = {}
    for name in (*RUNTIME_PACKAGES, "sentence-transformers", "torch", "transformers"):
        try:
            result[name] = version(name)
        except PackageNotFoundError:
            result[name] = "NOT_INSTALLED"
    return result


class Registry:
    def __init__(self, root, *, profile=None, region=None):
        self.root = staging_uri(root)
        self.profile, self.region = profile, region

    def uri(self, key):
        if not key or any(p in ("", ".", "..") for p in key.split("/")):
            raise ValueError("invalid artifact key")
        return self.root + "/" + key

    def _s3(self):
        import boto3

        return boto3.Session(profile_name=self.profile, region_name=self.region).client(
            "s3"
        )

    def put(self, key, payload):
        uri = urlsplit(self.uri(key))
        if uri.scheme == "file":
            path = Path(unquote(uri.path))
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("xb") as stream:
                stream.write(payload)
        else:
            self._s3().put_object(
                Bucket=uri.netloc,
                Key=uri.path.lstrip("/"),
                Body=payload,
                IfNoneMatch="*",
            )

    def get(self, key, *, max_bytes=256_000_000):
        uri = urlsplit(self.uri(key))
        if uri.scheme == "file":
            path = Path(unquote(uri.path))
            if path.stat().st_size > max_bytes:
                raise ValueError("artifact byte limit exceeded")
            return path.read_bytes()
        result = self._s3().get_object(Bucket=uri.netloc, Key=uri.path.lstrip("/"))
        if result["ContentLength"] > max_bytes:
            result["Body"].close()
            raise ValueError("artifact byte limit exceeded")
        return result["Body"].read()

    def manifest(self, run_id):
        data = json.loads(
            self.get(f"runs/{run_id}/manifest.json", max_bytes=10_000_000)
        )
        if data["artifact_version"] != ARTIFACT_VERSION or data["status"] not in (
            "candidate",
            "validated",
        ):
            raise ValueError(
                "unsupported artifact/state; production promotion requires a separate reviewed process"
            )
        for name in RUNTIME_PACKAGES:
            if data["runtime"][name] != version(name):
                raise ValueError(f"incompatible artifact runtime: {name}")
        for name in ("sentence-transformers", "torch", "transformers"):
            if data["runtime"][name] != runtime_versions()[name]:
                raise ValueError(f"incompatible encoder runtime: {name}")
        return data

    def load_umap(self, run_id, manifest):
        payload = self.get(f"runs/{run_id}/umap.joblib")
        if hashlib.sha256(payload).hexdigest() != manifest["umap_sha256"]:
            raise ValueError("UMAP artifact checksum mismatch")
        # Joblib/pickle is executable: only use an operator-owned, access-controlled registry.
        return payload

    def commit(self, run_id, manifest):
        self.put(
            f"runs/{run_id}/manifest.json",
            json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False).encode(),
        )
