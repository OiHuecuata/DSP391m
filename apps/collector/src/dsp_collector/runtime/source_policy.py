"""Fail-closed Threads admission. Accessible source data is not permission."""

from datetime import UTC, datetime

from ..normalizers.observation import _secret_check, canonical_json
from .durable import sha256


def admit(policy, *, fixture=False, now=None):
    now = now or datetime.now(UTC)
    if fixture:
        if policy != {
            "status": "SYNTHETIC",
            "reference": "threads-vertical-fixture-v1",
        }:
            raise ValueError("Explicit isolated fixture policy required")
        return "SYNTHETIC"
    required = (
        "reference",
        "reviewer",
        "retention_policy_id",
        "valid_until",
        "retention_days",
        "license_or_terms_ref",
        "policy_reviewed_at",
        "allowed_fields",
    )
    if (
        policy.get("source") != "threads"
        or policy.get("status") != "APPROVED"
        or policy.get("collection_enabled") is not True
        or any(not policy.get(k) for k in required)
        or policy.get("allowed_interfaces") != ["public-search", "public-detail"]
        or policy.get("public_author_metadata_allowed") is not True
    ):
        raise PermissionError(
            "Threads source policy UNKNOWN/disabled or incomplete review"
        )
    if policy.get(
        "storage_permission"
    ) is not True or "research_analytics" not in policy.get("allowed_derived_uses", []):
        raise PermissionError("Storage and research use need an explicit source review")
    expiry = datetime.fromisoformat(policy["valid_until"])
    if expiry.utcoffset() is None or expiry <= now:
        raise PermissionError("Threads source review expired")
    if type(policy["retention_days"]) is not int or policy["retention_days"] < 1:
        raise ValueError("Bounded retention required")
    return "PERMITTED_COLLECTION"


def policy_hash(policy):
    return sha256(canonical_json(policy).encode())


def validate_payload(envelope, policy=None):
    _secret_check(envelope)
    if envelope.get("source") != "threads" or envelope.get("count") != len(
        envelope.get("posts", [])
    ):
        raise ValueError("Invalid Threads acquisition envelope")
    if policy and policy.get("status") != "SYNTHETIC":
        allowed = policy.get("allowed_fields", [])

        def check(value, prefix=""):
            if isinstance(value, dict):
                for key, child in value.items():
                    location = prefix + "." + key if prefix else key
                    if not any(
                        location == item
                        or item.endswith(".*")
                        and (location == item[:-2] or location.startswith(item[:-1]))
                        for item in allowed
                    ):
                        raise PermissionError(
                            "Captured native field is outside the reviewed source allowlist"
                        )
                    check(child, location)
            elif isinstance(value, list):
                for child in value:
                    check(child, prefix)

        def native_posts(post):
            if post.get("raw_payload"):
                check(post["raw_payload"])
            detail = post.get("detail") or {}
            if detail.get("raw_payload"):
                check(detail["raw_payload"])
            for reply in detail.get("visible_replies", []):
                native_posts(reply)

        for post in envelope["posts"]:
            native_posts(post)
