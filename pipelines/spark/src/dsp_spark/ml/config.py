"""Versioned, strict configuration for exchangeable sentence encoders and KMeans."""

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from urllib.parse import urlsplit

from ..io.s3 import storage_uri

TEXT_VERSION = "nfkc-whitespace-v1"
ARTIFACT_VERSION = "demand-clustering-v1"


def fingerprint(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, allow_nan=False).encode()
    ).hexdigest()


@dataclass(frozen=True)
class EncoderConfig:
    model: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    revision: str = ""
    dimension: int = 384
    batch_size: int = 32
    device: str = "cpu"
    normalize: bool = True
    version: str = "sentence-transformers-v1"
    max_seq_length: int = 128

    def __post_init__(self):
        if not self.model or not re.fullmatch(r"[0-9a-f]{40}", self.revision):
            raise ValueError(
                "encoder revision must be an immutable 40-character commit SHA"
            )
        if min(self.dimension, self.batch_size, self.max_seq_length) < 1:
            raise ValueError("encoder dimensions and batch sizes must be positive")
        if not self.normalize:
            raise ValueError("this semantic baseline requires L2 normalization")

    @property
    def key(self):
        from importlib.metadata import PackageNotFoundError, version

        libraries = {}
        for name in ("sentence-transformers", "torch", "transformers"):
            try:
                libraries[name] = version(name)
            except PackageNotFoundError:
                libraries[name] = "NOT_INSTALLED"
        return fingerprint(
            {
                "encoder": asdict(self),
                "text_version": TEXT_VERSION,
                "libraries": libraries,
            }
        )


@dataclass(frozen=True)
class ReductionConfig:
    n_components: int = 15
    n_neighbors: int = 30
    min_dist: float = 0.0
    metric: str = "cosine"
    random_state: int = 42
    transform_seed: int = 42
    fit_max_rows: int = 10000
    fit_max_bytes: int = 256_000_000

    def __post_init__(self):
        if self.n_components < 1 or self.n_neighbors < 2:
            raise ValueError("invalid UMAP dimensions/neighbors")
        if self.metric != "cosine" or self.min_dist < 0:
            raise ValueError("UMAP must use cosine and a nonnegative min_dist")
        if self.fit_max_rows <= self.n_components + 1 or self.fit_max_bytes < 1:
            raise ValueError("invalid bounded fit limits")


@dataclass(frozen=True)
class TrainingConfig:
    encoder: EncoderConfig
    reduction: ReductionConfig = field(default_factory=ReductionConfig)
    ks: tuple[int, ...] = (5, 8, 10)
    primary_candidate: int = 8
    seed: int = 42
    max_iter: int = 100
    tol: float = 1e-4
    partitions: int = 4
    stability_seeds: tuple[int, ...] = (17, 29, 43, 71, 97)
    stability_fraction: float = 0.8
    representative_posts: bool = False

    def __post_init__(self):
        if not self.ks or min(self.ks) < 2 or len(set(self.ks)) != len(self.ks):
            raise ValueError("ks must contain distinct integers >=2")
        if self.primary_candidate not in self.ks:
            raise ValueError("primary_candidate must be in ks")
        if min(self.partitions, self.max_iter) < 1:
            raise ValueError("invalid runtime limits")
        if len(set(self.stability_seeds)) < 3 or not 0 < self.stability_fraction < 1:
            raise ValueError(
                "stability requires >=3 distinct seeds and a fraction in (0,1)"
            )

    @classmethod
    def from_dict(cls, raw):
        raw = dict(raw)
        raw["encoder"] = EncoderConfig(**raw["encoder"])
        raw["reduction"] = ReductionConfig(**raw.get("reduction", {}))
        for key in ("ks", "stability_seeds"):
            if key in raw:
                raw[key] = tuple(raw[key])
        return cls(**raw)


def staging_uri(value):
    uri = storage_uri(value)
    if "staging" not in urlsplit(uri).path.split("/"):
        raise ValueError(
            "writes require a dedicated staging path; production writes are disabled"
        )
    return uri
