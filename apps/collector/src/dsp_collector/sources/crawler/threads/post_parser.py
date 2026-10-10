"""Parser for one public Threads post page.

Responsibilities:
- parse server-rendered Threads post JSON embedded in HTML
- identify the requested main post
- extract text, author, engagement, media, and visible replies
- preserve the original source post as ``raw_payload`` for Bronze storage

This module deliberately has NO Playwright/network/Kafka/Data-Lake logic.
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse
from .rich_fields import metrics as _native_metrics


def _metric_availability(raw):
    return _native_metrics(raw)[1]


class ThreadsPostParserError(RuntimeError):
    """Raised when a Threads post page cannot be parsed."""


THREAD_ITEMS_KEY = '"thread_items":'
POST_OBJECT_KEY = '"post":'


def _string_id(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _iter_json_values_after_key(
    html: str,
    *,
    key: str,
    opening_char: str,
    expected_type: type,
) -> list[Any]:
    """Decode JSON values that immediately follow a known object key.

    Threads SSR pages may expose the requested post in more than one shape.
    Reading both ``thread_items`` arrays and direct ``post`` objects avoids
    assuming that the canonical post always lives in one parent structure.
    """
    decoder = json.JSONDecoder()
    values: list[Any] = []
    cursor = 0

    while True:
        key_index = html.find(key, cursor)
        if key_index < 0:
            break

        value_start = key_index + len(key)
        while value_start < len(html) and html[value_start].isspace():
            value_start += 1

        if value_start >= len(html) or html[value_start] != opening_char:
            cursor = key_index + len(key)
            continue

        try:
            value, consumed = decoder.raw_decode(html[value_start:])
        except json.JSONDecodeError:
            cursor = key_index + len(key)
            continue

        if isinstance(value, expected_type):
            values.append(value)

        cursor = value_start + consumed

    return values


def _iter_thread_item_arrays(html: str) -> list[list[Any]]:
    return _iter_json_values_after_key(
        html,
        key=THREAD_ITEMS_KEY,
        opening_char="[",
        expected_type=list,
    )


def _iter_direct_post_objects(html: str) -> list[dict[str, Any]]:
    return _iter_json_values_after_key(
        html,
        key=POST_OBJECT_KEY,
        opening_char="{",
        expected_type=dict,
    )


def _walk_post_dicts(value: Any):
    """Yield dictionaries that look like raw Threads posts."""
    if isinstance(value, dict):
        post = value.get("post")
        if isinstance(post, dict):
            yield post

        if (
            ("pk" in value or "id" in value)
            and "user" in value
            and ("code" in value or "caption" in value)
        ):
            yield value

        for child in value.values():
            yield from _walk_post_dicts(child)

    elif isinstance(value, list):
        for child in value:
            yield from _walk_post_dicts(child)


def _looks_like_post(value: dict[str, Any]) -> bool:
    return (
        ("pk" in value or "id" in value)
        and isinstance(value.get("user"), dict)
        and ("code" in value or "caption" in value)
    )


def extract_post_candidates(html: str) -> list[dict[str, Any]]:
    """Return unique raw post dictionaries found in Threads SSR data.

    Candidate discovery intentionally scans more than ``thread_items`` because
    a public Threads page can embed the canonical post in a different SSR
    object while also embedding unrelated/recommended thread items.
    """
    if not html:
        raise ValueError("html must not be empty")

    arrays = _iter_thread_item_arrays(html)
    direct_posts = _iter_direct_post_objects(html)

    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None]] = set()

    def add(post: dict[str, Any]) -> None:
        if not _looks_like_post(post):
            return

        post_id = _string_id(post.get("pk") or post.get("id"))
        code = post.get("code")
        key = (post_id, str(code) if code is not None else None)

        if key in seen:
            return

        seen.add(key)
        candidates.append(post)

    for array in arrays:
        for post in _walk_post_dicts(array):
            add(post)

    for post in direct_posts:
        add(post)
        for nested in _walk_post_dicts(post):
            add(nested)

    if not candidates:
        raise ThreadsPostParserError(
            "No Threads post objects could be parsed from the page SSR data"
        )

    return candidates


def _extract_code_from_url(url: str | None) -> str | None:
    if not url:
        return None

    path_parts = [part for part in urlparse(url).path.split("/") if part]

    try:
        post_index = path_parts.index("post")
    except ValueError:
        return None

    if post_index + 1 >= len(path_parts):
        return None

    return path_parts[post_index + 1]


def _find_main_post(
    candidates: list[dict[str, Any]],
    *,
    expected_post_id: str | None,
    expected_code: str | None,
    source_url: str | None,
) -> dict[str, Any]:
    """Return only the explicitly requested post; never guess a fallback."""
    requested_code = expected_code or _extract_code_from_url(source_url)
    requested_id = _string_id(expected_post_id)

    if requested_id is None and not requested_code:
        raise ThreadsPostParserError(
            "Cannot identify the requested post without an expected ID, code, "
            "or canonical post URL"
        )

    for post in candidates:
        post_id = _string_id(post.get("pk") or post.get("id"))
        post_code = post.get("code")

        id_matches = requested_id is None or post_id == requested_id
        code_matches = requested_code is None or post_code == requested_code

        if id_matches and code_matches:
            return post

    available = [
        f"{_string_id(post.get('pk') or post.get('id'))}/{post.get('code')}"
        for post in candidates[:12]
    ]
    raise ThreadsPostParserError(
        "Requested Threads post was not found in page SSR data: "
        f"expected={requested_id}/{requested_code}; "
        f"candidates={available}"
    )


def _author(raw_post: dict[str, Any]) -> dict[str, Any]:
    user = raw_post.get("user") or {}

    return {
        "id": _string_id(user.get("pk") or user.get("id")),
        "username": user.get("username"),
        "full_name": user.get("full_name"),
        "is_verified": user.get("is_verified"),
        "profile_pic_url": user.get("profile_pic_url"),
    }


def _text(raw_post: dict[str, Any]) -> str | None:
    caption = raw_post.get("caption")

    if isinstance(caption, dict):
        value = caption.get("text")
        return str(value) if value is not None else None

    if isinstance(caption, str):
        return caption

    return None


def _canonical_url(
    raw_post: dict[str, Any],
    *,
    source_url: str | None = None,
) -> str | None:
    user = raw_post.get("user") or {}
    username = user.get("username")
    code = raw_post.get("code")

    if username and code:
        return f"https://www.threads.com/@{username}/post/{code}"

    return source_url


def _image_item(
    raw: dict[str, Any],
    *,
    carousel_index: int | None = None,
) -> dict[str, Any] | None:
    candidates = (raw.get("image_versions2") or {}).get("candidates") or []

    if not candidates:
        return None

    candidate = candidates[0]
    if not isinstance(candidate, dict):
        return None

    item: dict[str, Any] = {
        "type": "image",
        "url": candidate.get("url"),
        "width": candidate.get("width"),
        "height": candidate.get("height"),
    }

    if carousel_index is not None:
        item["carousel_index"] = carousel_index

    return item


def _video_item(
    raw: dict[str, Any],
    *,
    carousel_index: int | None = None,
) -> dict[str, Any] | None:
    versions = raw.get("video_versions") or []

    if not versions:
        return None

    version = versions[0]
    if not isinstance(version, dict):
        return None

    item: dict[str, Any] = {
        "type": "video",
        "url": version.get("url"),
        "width": version.get("width"),
        "height": version.get("height"),
    }

    if carousel_index is not None:
        item["carousel_index"] = carousel_index

    return item


def extract_media(raw_post: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract one representative URL for each visible image/video."""
    media: list[dict[str, Any]] = []

    carousel = raw_post.get("carousel_media") or []

    if isinstance(carousel, list) and carousel:
        for index, item in enumerate(carousel):
            if not isinstance(item, dict):
                continue

            video = _video_item(item, carousel_index=index)
            if video and video.get("url"):
                media.append(video)
                continue

            image = _image_item(item, carousel_index=index)
            if image and image.get("url"):
                media.append(image)

        return media

    video = _video_item(raw_post)
    if video and video.get("url"):
        media.append(video)

    image = _image_item(raw_post)
    if image and image.get("url"):
        media.append(image)

    return media


def _engagement(raw_post: dict[str, Any]) -> dict[str, int]:
    from .rich_fields import metrics

    return metrics(raw_post)[0]


def _compact_post(
    raw_post: dict[str, Any],
    *,
    source_url: str | None = None,
) -> dict[str, Any]:
    info = raw_post.get("text_post_app_info") or {}

    return {
        "id": _string_id(raw_post.get("pk") or raw_post.get("id")),
        "code": raw_post.get("code"),
        "url": _canonical_url(raw_post, source_url=source_url),
        "text": _text(raw_post),
        "taken_at": raw_post.get("taken_at"),
        "author": _author(raw_post),
        "engagement": _engagement(raw_post),
        "is_reply": bool(info.get("is_reply")),
        "media_type": raw_post.get("media_type"),
        "media": extract_media(raw_post),
        "raw_payload": raw_post,
        "language": raw_post.get("language"),
        "updated_at": raw_post.get("edited_at", raw_post.get("updated_at")),
        "metric_availability": _metric_availability(raw_post),
    }


def _visible_replies(
    candidates: list[dict[str, Any]],
    *,
    main_post: dict[str, Any],
) -> list[dict[str, Any]]:
    """Return only replies whose parent linkage resolves to the main post.

    Threads pages can embed related/recommended posts. Merely seeing
    ``is_reply=True`` is therefore not enough to attach a candidate to the
    requested thread.
    """
    main_id = _string_id(main_post.get("pk") or main_post.get("id"))
    replies: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    if main_id is None:
        return replies

    pending = list(candidates)
    connected = {main_id}
    while pending:
        next_pending = []
        for post in pending:
            post_id = _string_id(post.get("pk") or post.get("id"))
            if post_id is None or post_id in connected or post_id in seen_ids:
                continue
            info = post.get("text_post_app_info") or {}
            if not info.get("is_reply"):
                continue
            parent = _string_id(
                info.get("reply_to_post_id")
                or info.get("replied_to_post_id")
                or info.get("parent_post_id")
            )
            if parent not in connected:
                next_pending.append(post)
                continue
            compact = _compact_post(post)
            compact.update(
                reply_to_post_id=parent,
                root_post_id=main_id,
                parent_comment_id=parent if parent != main_id else None,
            )
            replies.append(compact)
            seen_ids.add(post_id)
            connected.add(post_id)
        if len(next_pending) == len(pending):
            break
        pending = next_pending

    return replies


def parse_post_detail(
    html: str,
    *,
    expected_post_id: str | None = None,
    expected_code: str | None = None,
    source_url: str | None = None,
) -> dict[str, Any]:
    """Parse one public Threads post page into a Bronze-friendly record."""
    candidates = extract_post_candidates(html)

    main_post = _find_main_post(
        candidates,
        expected_post_id=expected_post_id,
        expected_code=expected_code,
        source_url=source_url,
    )

    detail = _compact_post(
        main_post,
        source_url=source_url,
    )

    info = main_post.get("text_post_app_info") or {}

    detail.update(
        {
            "visible_replies": _visible_replies(
                candidates,
                main_post=main_post,
            ),
            "visible_reply_count": 0,
            "is_quote_post": bool(info.get("is_quote_post")),
            "is_paid_partnership": bool(
                main_post.get("is_paid_partnership") or info.get("is_paid_partnership")
            ),
            "raw_payload": main_post,
        }
    )

    detail["visible_reply_count"] = len(detail["visible_replies"])
    detail["reply_extraction_status"] = "VISIBLE_PARTIAL"

    return detail
