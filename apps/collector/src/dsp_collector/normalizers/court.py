"""Pure PBPlus record -> court.v1 adaptation, without crawling or geocoding.

PBPlus currently identifies a listing by its full URL, not a verified native
court ID. Preserve that URL identity without removing query/fragment/path
components: URL-based identity may change if the source changes its URLs.
Optional geography is input enrichment, not an authoritative venue location.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

GEO_STATUSES = frozenset(
    {
        "ok",
        "ok_region_unverified",
        "missing_address",
        "not_found",
        "outside_vietnam",
        "region_mismatch",
        "error",
    }
)
ADDRESS_SOURCES = frozenset({"venue_page", "court_name_fallback"})


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _url(value: Any) -> str | None:
    if not isinstance(value, str) or any(c.isspace() for c in value):
        return None
    try:
        parsed = urlsplit(value)
        return value if parsed.scheme in {"http", "https"} and parsed.hostname else None
    except ValueError:
        # Malformed source URLs are unusable, permitting the identity fallback.
        return None


def _utc_timestamp(value: str | datetime) -> str:
    """Require an explicit timezone and use the social-post UTC output style."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("observation timestamps must be timezone-aware")
    return (
        value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    )


def _court_count(value: Any) -> int | None:
    """Accept nonnegative integers, integral floats, and decimal strings."""
    if type(value) is int:
        return value if value >= 0 else None
    if type(value) is float:
        return (
            int(value)
            if math.isfinite(value) and value >= 0 and value.is_integer()
            else None
        )
    if isinstance(value, str):
        value = value.strip()
        if value.isascii() and value.isdecimal():
            return int(value)
    return None


def _coordinate(value: Any, bound: float) -> float | None:
    """Unavailable, malformed, nonfinite, or out-of-range values become null.

    Each coordinate is handled independently; an incomplete pair stays
    incomplete. Reject booleans rather than treating them as numeric 0 or 1.
    """
    if type(value) not in (int, float, str):
        return None
    try:
        number = float(value)
    except (ValueError, OverflowError):
        # Invalid coordinate enrichment must not invalidate the court entity.
        return None
    return number if math.isfinite(number) and -bound <= number <= bound else None


def _reported_label(value: Any, allowed: frozenset[str], field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{field} must be a known source label or null")
    return value


def normalize_court(
    court: Mapping[str, Any],
    *,
    run_id: str,
    collected_at: str | datetime,
    geocoded_at: str | datetime | None = None,
    normalized_at: str | datetime | None = None,
) -> dict[str, Any]:
    """Normalize one PBPlus record without I/O or input mutation.

    A usable HTTP(S) URL in ``id`` is authoritative, falling back to ``url``.
    Other optional unusable source scalars become null; missing counts never
    become zero. ``address_source`` and ``geocode_status`` retain only explicit
    source reports, with no status inferred from coordinates or timestamps.
    Unknown fields (including HTML and source-specific payloads) are omitted.

    All observation timestamps come from the caller. Only ``normalized_at``
    defaults to UTC now; injecting it makes the full transformation repeatable.
    Schema validation is external, consistent with the social-post adapter.
    """
    if not isinstance(court, Mapping):
        raise TypeError("court must be a mapping")
    source_id = _url(court.get("id")) or _url(court.get("url"))
    if source_id is None:
        raise ValueError("court requires a usable HTTP(S) source identity in id or url")
    if _string(run_id) is None:
        raise ValueError("run_id must be a nonempty string")

    return {
        "schema_version": "1",
        "record_id": f"pbplus:{source_id}",
        "source": "pbplus",
        "source_record_id": source_id,
        "source_url": _url(court.get("url")),
        "source_code": _string(court.get("code")),
        "court_name": _string(court.get("court_name")),
        "number_of_courts": _court_count(court.get("number_of_courts")),
        "has_media": (
            court.get("has_media") if type(court.get("has_media")) is bool else None
        ),
        "address": _string(court.get("address")),
        "latitude": _coordinate(court.get("latitude"), 90),
        "longitude": _coordinate(court.get("longitude"), 180),
        "address_source": _reported_label(
            court.get("address_source"), ADDRESS_SOURCES, "address_source"
        ),
        "geo_status": _reported_label(
            court.get("geocode_status"), GEO_STATUSES, "geocode_status"
        ),
        "run_id": run_id,
        "collected_at": _utc_timestamp(collected_at),
        "geocoded_at": _utc_timestamp(geocoded_at) if geocoded_at is not None else None,
        "normalized_at": _utc_timestamp(
            datetime.now(UTC) if normalized_at is None else normalized_at
        ),
    }
