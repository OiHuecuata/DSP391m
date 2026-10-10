"""Source-supported fields only; unavailable counters never become zero."""

METRIC_KEYS = {
    "like_count": "like_count",
    "view_count": "view_count",
    "reply_count": "direct_reply_count",
    "repost_count": "repost_count",
    "quote_count": "quote_count",
    "share_count": "share_count",
    "comment_count": "comment_count",
}


def metrics(native):
    info = native.get("text_post_app_info") or {}
    values, availability = {}, {}
    for name, key in METRIC_KEYS.items():
        raw = native.get(key) if name in ("like_count", "view_count") else info.get(key)
        if isinstance(raw, str) and raw.isascii() and raw.isdecimal():
            raw = int(raw)
        value = raw if type(raw) is int and 0 <= raw < 2**63 else None
        state = (
            "NOT_PROVIDED"
            if raw is None
            else "INVALID"
            if value is None
            else "AVAILABLE"
        )
        if (
            name in ("like_count", "view_count")
            and native.get("like_and_view_counts_disabled") is True
        ):
            value, state = None, "UNAVAILABLE"
        values[name], availability[name] = value, state
    return values, availability


def additional_fields(native):
    values, availability = metrics(native)
    return {
        "engagement": values,
        "metric_availability": availability,
        "language": native.get("language"),
        "updated_at": native.get("edited_at", native.get("updated_at")),
    }
