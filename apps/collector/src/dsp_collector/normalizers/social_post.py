"""Pure Threads projection -> social-post.v1 adaptation, with no source I/O.

The outer search ID is authoritative and required. Detail is eligible only
when its reported status is ``ok``, its ID matches, and any supplied code or
username does not conflict with search. Unsafe detail is ignored wholesale;
otherwise each usable detail field wins independently, including 0 and False.
Empty text is valid. Missing/invalid publication time stays null.

Query annotations describe the search observation and are never recomputed
from enriched text. Detail status/error retain the crawler's report, even if
identity checks make that detail ineligible. Unknown raw fields are omitted.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _source_id(value: Any) -> str | None:
    if type(value) is int and value > 0:
        return str(value)
    if isinstance(value, str):
        value = value.strip()
        if value.isascii() and value.isdecimal() and int(value) > 0:
            return value
    return None


def _integer(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _boolean(value: Any) -> bool | None:
    return value if type(value) is bool else None


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _url(value: Any) -> str | None:
    if not isinstance(value, str) or any(c.isspace() for c in value):
        return None
    try:
        parsed = urlsplit(value)
        return value if parsed.scheme in {"http", "https"} and parsed.hostname else None
    except ValueError:
        # Invalid URL syntax is unusable source data, permitting search fallback.
        return None


def _utc_timestamp(value: str | datetime) -> str:
    """Require an explicit timezone; never infer the host's local timezone."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("observation timestamps must be timezone-aware")
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _published_at(value: Any) -> str | None:
    """Threads taken_at is Unix seconds; malformed values are unavailable."""
    if type(value) not in (int, float):
        return None
    try:
        if not math.isfinite(value):
            return None
        return _utc_timestamp(datetime.fromtimestamp(value, UTC))
    except (ValueError, OverflowError, OSError):
        # Out-of-range source seconds are unavailable, never replaced with now.
        return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _prefer(detail_value: Any, search_value: Any, convert: Callable[[Any], Any]) -> Any:
    selected = convert(detail_value)
    return selected if selected is not None else convert(search_value)


def _eligible_detail(post: Mapping[str, Any], source_id: str) -> Mapping[str, Any]:
    detail = _mapping(post.get("detail"))
    if post.get("detail_status") != "ok" or _source_id(detail.get("id")) != source_id:
        return {}
    search_code = _string(post.get("code"))
    if (
        search_code is not None
        and detail.get("code") is not None
        and detail.get("code") != search_code
    ):
        return {}
    search_username = _string(_mapping(post.get("user")).get("username"))
    detail_username = _string(_mapping(detail.get("author")).get("username"))
    if search_username and detail_username and search_username != detail_username:
        return {}
    return detail


def _strings(value: Any, field: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(f"{field} must be a list of strings or null")
    return list(value)


def _query_contexts(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError("query_contexts must be a list or null")
    contexts = []
    for item in value:
        if not isinstance(item, Mapping) or any(
            _string(item.get(key)) is None for key in ("query", "kind")
        ):
            raise ValueError("each query context requires query and kind strings")
        context = {"query": item["query"], "kind": item["kind"]}
        for key in ("geo_key", "geo_name"):
            if item.get(key) is not None and not isinstance(item[key], str):
                raise ValueError(f"query context {key} must be a string or null")
            context[key] = item.get(key)
        contexts.append(context)
    return contexts


def normalize_social_post(
    post: Mapping[str, Any],
    *,
    run_id: str,
    collected_at: str | datetime,
    normalized_at: str | datetime | None = None,
) -> dict[str, Any]:
    """Normalize one Threads search/enriched record without mutating it.

    ``run_id`` and ``collected_at`` come from the caller, never status.json.
    ``normalized_at`` defaults to UTC now, as required by the contract; supplying
    it makes the complete transformation reproducible. Metadata must be valid,
    whereas unusable optional source scalars fall back to search, then null.
    Missing context lists become empty; malformed context raises ValueError or
    TypeError rather than silently losing provenance. Schema validation is external.
    """
    if not isinstance(post, Mapping):
        raise TypeError("post must be a mapping")
    source_id = _source_id(post.get("id"))
    if source_id is None:
        raise ValueError("post.id must be a positive decimal Threads post ID")
    if _string(run_id) is None:
        raise ValueError("run_id must be a nonempty string")
    status = post.get("detail_status")
    if status is not None and status not in ("ok", "error"):
        raise ValueError("detail_status must be ok, error, or null")

    detail = _eligible_detail(post, source_id)
    user = _mapping(post.get("user"))
    author = _mapping(detail.get("author"))
    engagement = _mapping(detail.get("engagement"))
    relevance = _mapping(post.get("relevance"))

    result = {
        "schema_version": "1",
        "record_id": f"threads:{source_id}",
        "source": "threads",
        "source_record_id": source_id,
        "source_url": _prefer(detail.get("url"), post.get("url"), _url),
        "source_code": _prefer(detail.get("code"), post.get("code"), _string),
        "text": _prefer(detail.get("text"), post.get("text"), _text),
        "published_at": _prefer(
            detail.get("taken_at"), post.get("taken_at"), _published_at
        ),
        "author_id": _prefer(author.get("id"), user.get("pk"), _source_id),
        "author_username": _prefer(
            author.get("username"), user.get("username"), _string
        ),
        "author_full_name": _prefer(
            author.get("full_name"), user.get("full_name"), _string
        ),
        "author_is_verified": _prefer(
            author.get("is_verified"), user.get("is_verified"), _boolean
        ),
        "is_reply": _prefer(detail.get("is_reply"), post.get("is_reply"), _boolean),
        "media_type": _prefer(
            detail.get("media_type"), post.get("media_type"), _integer
        ),
        # Detail lists contain extracted visible media only. Empty/malformed
        # lists cannot disprove the search projection's media evidence.
        "has_media": (
            True
            if isinstance(detail.get("media"), list)
            and any(
                isinstance(item, Mapping)
                and item.get("type") in ("image", "video")
                and _url(item.get("url")) is not None
                for item in detail["media"]
            )
            else _boolean(post.get("has_media"))
        ),
        "run_id": run_id,
        "collected_at": _utc_timestamp(collected_at),
        "normalized_at": _utc_timestamp(
            datetime.now(UTC) if normalized_at is None else normalized_at
        ),
        "matched_queries": _strings(post.get("matched_queries"), "matched_queries"),
        "query_contexts": _query_contexts(post.get("query_contexts")),
        "categories": _strings(post.get("categories"), "categories"),
        "detected_locations": _strings(
            post.get("detected_locations"), "detected_locations"
        ),
        "vietnam_score": _integer(relevance.get("vietnam_score")),
        "topic_score": _integer(relevance.get("topic_score")),
        "language_hint": _string(relevance.get("language_hint")),
        "relevance_signals": _strings(relevance.get("signals"), "relevance.signals"),
        "detail_status": status,
        "detail_error": _string(post.get("detail_error")),
    }
    # Preserve provided zeros: the crawler already defaults some missing
    # counters to 0, so normalization cannot distinguish missing from observed 0.
    for field in ("like_count", "reply_count", "repost_count", "quote_count"):
        result[field] = _prefer(engagement.get(field), post.get(field), _integer)
    return result
