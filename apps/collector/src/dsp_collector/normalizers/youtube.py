"""YouTube videos and public top-level comments, without invented statistics."""

import re

from ..sources.crawler.threads.vietnam_terms import _detect_locations, _detect_topics
from .social_post import _utc_timestamp, normalize_social_post


def counter(value):
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        value = int(value)
    return value if type(value) is int and 0 <= value < 2**63 else None


def normalize_youtube(payload, *, run_id, collected_at, queries=(), video_id=None):
    identity = payload.get("id")
    if not isinstance(identity, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", identity):
        raise ValueError("Invalid YouTube source identity")
    comment = video_id is not None
    snippet = payload.get("snippet", {})
    stats = payload.get("statistics", {})
    text = (
        snippet.get("textOriginal", snippet.get("textDisplay", ""))
        if comment
        else snippet.get("title", "") + "\n\n" + snippet.get("description", "")
    )
    # Reuse the canonical empty SocialPost shape; no Threads values survive.
    result = normalize_social_post(
        {"id": "1"},
        run_id=run_id,
        collected_at=collected_at,
        normalized_at=collected_at,
    )
    result.update(
        source="youtube_comment" if comment else "youtube",
        source_record_id=identity,
        record_id=("youtube-comment:" if comment else "youtube:") + identity,
        source_url=f"https://www.youtube.com/watch?v={video_id or identity}"
        + (f"&lc={identity}" if comment else ""),
        text=text,
        published_at=(
            _utc_timestamp(snippet["publishedAt"])
            if snippet.get("publishedAt")
            else None
        ),
        author_id=(
            snippet.get("authorChannelId", {}).get("value")
            if comment
            else snippet.get("channelId")
        ),
        author_username=None,
        author_full_name=(
            snippet.get("authorDisplayName") if comment else snippet.get("channelTitle")
        ),
        like_count=counter(
            snippet.get("likeCount") if comment else stats.get("likeCount")
        ),
        reply_count=None if comment else counter(stats.get("commentCount")),
        repost_count=None,
        quote_count=None,
        is_reply=False,
        media_type=None,
        has_media=not comment,
        matched_queries=list(queries),
        detected_locations=_detect_locations(text),
        categories=_detect_topics(text)[0],
        view_count=None if comment else counter(stats.get("viewCount")),
        parent_record_id=f"youtube:{video_id}" if comment else None,
        video_duration=(
            None if comment else payload.get("contentDetails", {}).get("duration")
        ),
        tags=None if comment else snippet.get("tags"),
    )
    return result
