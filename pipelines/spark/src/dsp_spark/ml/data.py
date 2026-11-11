"""Read-only pinned Silver loader. Evidence never joins the feature corpus."""

import unicodedata
from dataclasses import asdict, dataclass

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from ..io.s3 import storage_uri
from ..processing.intelligence.catalog import contract
from .config import TEXT_VERSION, fingerprint


def normalize_text(text):
    return " ".join(unicodedata.normalize("NFKC", text or "").split())


@dataclass(frozen=True)
class Snapshot:
    path: str
    version: int
    provenance: str
    dataset_id: str
    generation: str | None = None
    schema_version: str = contract("silver_social_posts")["properties"][
        "schema_version"
    ]["const"]

    def __post_init__(self):
        object.__setattr__(self, "path", storage_uri(self.path))
        if (
            self.schema_version
            != contract("silver_social_posts")["properties"]["schema_version"]["const"]
        ):
            raise ValueError("unsupported Silver schema version")
        if self.version < 0 or self.provenance not in (
            "real",
            "synthetic",
            "mixed",
            "unknown",
        ):
            raise ValueError("snapshot requires explicit Delta version and provenance")
        if not self.dataset_id or "dataset_v1" in self.dataset_id.lower().replace(
            " ", "_"
        ):
            raise ValueError("dataset_id required; Dataset V1 is not supported")


def require_schema(frame, required):
    actual = {f.name: f.dataType.simpleString() for f in frame.schema}
    for name, kind in required.items():
        if actual.get(name) != kind:
            raise ValueError(f"incompatible schema: {name} must be {kind}")


def unique_keys(frame, keys):
    if (
        frame.filter(F.expr(" OR ".join(f"`{k}` IS NULL" for k in keys)))
        .limit(1)
        .count()
    ):
        raise ValueError("null primary key")
    if frame.groupBy(*keys).count().filter("count > 1").limit(1).count():
        raise ValueError("duplicate primary key/fanout")


def load_posts(spark, snapshot, *, after=None):
    raw = (
        spark.read.format("delta")
        .option("versionAsOf", snapshot.version)
        .load(snapshot.path)
    )
    require_schema(
        raw,
        {
            "post_id": "string",
            "content_id": "string",
            "text_normalized": "string",
            "platform": "string",
            "schema_version": "string",
            "lineage_ids": "array<string>",
            "intent_labels": "array<string>",
            "topic_labels": "array<string>",
            "is_duplicate": "boolean",
            "last_seen_at": "string",
        },
    )
    if (
        raw.filter(~F.col("schema_version").eqNullSafe(snapshot.schema_version))
        .limit(1)
        .count()
    ):
        raise ValueError("incompatible Silver schema_version")
    unique_keys(raw, ("post_id",))
    if (
        raw.filter(
            F.col("platform").isNull()
            | (F.trim("platform") == "")
            | F.col("lineage_ids").isNull()
            | (F.size("lineage_ids") == 0)
        )
        .limit(1)
        .count()
    ):
        raise ValueError("missing source/provenance lineage")
    # Reject corrupt keys rather than choosing an arbitrary post/evidence projection.
    dedup = raw
    unique_keys(dedup, ("content_id",))
    if dedup.filter(F.trim("content_id") == "").limit(1).count():
        raise ValueError("empty content_id")
    if after is not None:
        from datetime import datetime

        if datetime.fromisoformat(after).tzinfo is None:
            raise ValueError("watermark requires an explicit timezone")
        if (
            dedup.filter(
                F.col("last_seen_at").isNotNull()
                & F.to_timestamp("last_seen_at").isNull()
            )
            .limit(1)
            .count()
        ):
            raise ValueError("invalid last_seen_at watermark")
        dedup = dedup.filter(
            F.to_timestamp("last_seen_at") > F.to_timestamp(F.lit(after))
        )
    normalized = F.udf(normalize_text, "string")
    cleaned = dedup.withColumn("text", normalized("text_normalized")).withColumn(
        "text_hash_ml", F.sha2("text", 256)
    )
    # Do not silently change the benchmark population by removing cross-content text duplicates.
    total = cleaned.count()
    posts = cleaned.filter(F.length("text") > 0)
    count = posts.count()
    summary = {
        "snapshot": asdict(snapshot),
        "input_rows": total,
        "rows": count,
        "empty_text_excluded": total - count,
        "after": after,
        "flagged_duplicate_posts": posts.filter(F.col("is_duplicate") == True).count(),
        "repeated_text_groups": posts.groupBy("text_hash_ml")
        .count()
        .filter("count > 1")
        .count(),
        "sources": {
            r.platform: r["count"] for r in posts.groupBy("platform").count().collect()
        },
        "text_version": TEXT_VERSION,
        "comparability": "NOT_DIRECTLY_COMPARABLE",
        "comparability_reasons": [
            "V3 corpus/encoder/UMAP/protocol equality not verified",
            "Spark KMeans differs from sklearn",
        ],
    }
    # Distributed ordered-free fingerprint (multiset modular sum of SHA256); no corpus collect.
    digest = posts.select(
        F.sha2(F.concat_ws("\u0000", "content_id", "text_hash_ml"), 256).alias("h")
    )
    chunks = digest.agg(
        *[
            F.sum(F.conv(F.substring("h", i, 8), 16, 10).cast("decimal(38,0)")).alias(
                f"s{i}"
            )
            for i in range(1, 65, 8)
        ]
    ).first()
    summary["dataset_fingerprint"] = fingerprint(
        {
            "snapshot": asdict(snapshot),
            "count": count,
            "hash_sums": [str(v) for v in chunks],
            "text_version": TEXT_VERSION,
        }
    )
    return posts, summary


def audit_table(spark, path, keys):
    table = DeltaTable.forPath(spark, path)
    version = int(table.history(1).first().version)
    frame = spark.read.format("delta").option("versionAsOf", version).load(path)
    detail = table.detail().first().asDict()
    return {
        "path": path,
        "version": version,
        "rows": frame.count(),
        "schema": frame.schema.jsonValue(),
        "partition_columns": detail["partitionColumns"],
        "duplicate_keys": frame.count() - frame.select(*keys).distinct().count(),
        "keys": list(keys),
        "provenance": "unknown until verified from lineage/source policy",
    }
