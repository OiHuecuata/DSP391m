"""Deterministic identity, UTC dates, policy filtering and typed projections."""

import hashlib
import json
import re
import unicodedata
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

from .catalog import GOLD, VERSION, contract

VN = ZoneInfo("Asia/Ho_Chi_Minh")


def encode(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest(value):
    return "sha256:" + hashlib.sha256(encode(value).encode()).hexdigest()


def instant(value):
    if value is None:
        return None
    result = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("timestamp must include an explicit UTC offset")
    return result.astimezone(UTC)


def timestamp(value):
    return (
        instant(value).isoformat(timespec="microseconds").replace("+00:00", "Z")
        if value is not None
        else None
    )


def vn_date(value):
    return instant(value).astimezone(VN).date().isoformat() if value else None


def normalized(value):
    return re.sub(r"\s+", " ", unicodedata.normalize("NFC", value or "")).strip()


def folded(value):
    text = unicodedata.normalize("NFD", normalized(value).casefold().replace("đ", "d"))
    return "".join(c for c in text if not unicodedata.combining(c))


def project(name, row):
    spec = contract(name)["properties"]
    result = {}
    for field, node in spec.items():
        if field in row:
            result[field] = row[field]
        elif field == "schema_version":
            result[field] = "1" if name in GOLD else "3"
        elif field == "processing_version":
            result[field] = VERSION
        elif node["type"] == "array":
            result[field] = []
        elif isinstance(node["type"], list) and "null" in node["type"]:
            result[field] = None
        else:
            raise ValueError(f"missing required {name}.{field}")
    return result


def lineage(rows):
    return sorted(
        {
            key
            for row in rows
            for key in row.get("lineage_ids", [row.get("observation_id")])
            if key
        }
    )


def content_id(platform, native, *, comment=False):
    return f"{platform}:{'comment:' if comment else ''}{native}"


def active_observations(datasets, *, as_of, policies=None, excluded_runs=()):
    """Apply current policies and evidence-based entity tombstones before deriving.

    Absence, failed collection and HTTP 403/404 never imply deletion. A deleted
    source entity cannot resurrect through replay, even from an observation
    collected after deletion. Restoration requires a future explicit adapter.
    Parent deletion suppresses its comments/metrics while unrelated venues and
    other source links survive. Archives are never physically destroyed here.
    """
    as_of = instant(as_of)
    policies = policies or {}
    excluded_runs = set(excluded_runs)
    deleted, unavailable = set(), set()
    for row in datasets.get("bronze_source_changes", []):
        if (
            instant(row["collected_at"]) > as_of
            or row["collection_run_id"] in excluded_runs
        ):
            continue
        key = (
            row["source_platform"],
            row["source_record_id"],
            row["entity_record_type"],
        )
        evidence = json.loads(row["evidence_json"])
        verified = evidence.get("verified_endpoint") or evidence.get("source_url")
        if row["change_type"] == "deleted":
            if not verified or not (
                evidence.get("source_tombstone") is True
                or evidence.get("http_status") == 410
            ):
                raise ValueError("deletion lacks verified tombstone evidence")
            deleted.add(key)
        elif (
            row["change_type"] == "unavailable"
            and policies.get(key[0], {}).get("deletion_required")
            and verified
        ):
            unavailable.add(key)
    suppressed = deleted | unavailable
    # Replies can form deeper trees. Suppress descendants of a deleted comment
    # without deleting comments under unrelated posts or independent sources.
    expanding = True
    while expanding:
        expanding = False
        for row in datasets.get("bronze_social_comments", []):
            source = row["source_platform"]
            key = (source, row["source_record_id"], "Comment")
            if (
                key not in suppressed
                and (source, row.get("parent_comment_id"), "Comment") in suppressed
            ):
                suppressed.add(key)
                expanding = True
    result, quality = {}, {
        "excluded_observations": 0,
        "expired_observations": 0,
        "deleted_entities": len(suppressed),
        "unreviewed_policy_observations": 0,
    }
    for name, rows in datasets.items():
        kept = {}
        for row in rows:
            if row["collection_run_id"] in excluded_runs:
                quality["excluded_observations"] += 1
                continue
            if instant(row["collected_at"]) > as_of:
                continue
            source = row["source_platform"]
            policy = policies.get(source, {})
            if not policy:
                quality["unreviewed_policy_observations"] += 1
            expiry = instant(row.get("retention_expires_at"))
            if policy.get("retention_days"):
                policy_expiry = instant(row["collected_at"]) + timedelta(
                    days=policy["retention_days"]
                )
                expiry = min(expiry, policy_expiry) if expiry else policy_expiry
            if policy.get("storage_permission") is False or (
                expiry is not None and expiry <= as_of
            ):
                quality["expired_observations"] += 1
                continue
            sid, typ = row["source_record_id"], row["record_type"]
            blocked = (source, sid, typ) in suppressed
            if name == "bronze_engagement_snapshots":
                metric_typ = (
                    "Comment"
                    if row.get("content_type") == "comment"
                    else "Video" if row.get("content_type") == "video" else "Post"
                )
                blocked = (
                    blocked
                    or (source, row["native_content_id"], metric_typ) in suppressed
                )
            if name == "bronze_social_comments":
                blocked = blocked or any(
                    (source, row.get("native_post_id"), t) in suppressed
                    for t in ("Video", "Post")
                )
                blocked = (
                    blocked
                    or (source, row.get("parent_comment_id"), "Comment") in suppressed
                )
            if not blocked:
                prior = kept.get(row["observation_id"])
                if prior and prior != row:
                    raise ValueError("conflicting duplicate Bronze observation")
                kept[row["observation_id"]] = row
        result[name] = sorted(
            kept.values(), key=lambda r: (r["collected_at"], r["observation_id"])
        )
    return result, quality
