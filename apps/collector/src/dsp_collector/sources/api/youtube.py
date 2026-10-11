"""Bounded official YouTube Data API v3 discovery; credentials never enter URLs."""

import re
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from ..crawler.threads.vietnam_terms import build_query_plan


class APIStop(RuntimeError):
    def __init__(self, reason, status=None):
        self.reason, self.status = reason, status
        super().__init__(reason)


@dataclass
class YouTubeConfig:
    keywords: list[str] = field(default_factory=lambda: ["pickleball"])
    include_geography: bool = True
    max_queries: int = 7
    order: str = "date"
    published_after: str | None = None
    published_before: str | None = None
    max_pages: int = 2
    page_size: int = 25
    max_videos: int = 100
    max_requests: int = 40
    max_search_requests: int = 14
    max_comment_requests: int = 20
    max_comment_pages: int = 2
    max_comments_per_video: int = 100
    request_delay: float = 0.5
    retries: int = 1
    timeout_seconds: int = 20

    def __post_init__(self):
        for name in (
            "max_queries",
            "max_pages",
            "page_size",
            "max_videos",
            "max_requests",
            "max_search_requests",
            "timeout_seconds",
        ):
            if (
                type(getattr(self, name)) is not int
                or not 1 <= getattr(self, name) <= 10000
            ):
                raise ValueError(f"Invalid YouTube {name}")
        for name in (
            "max_comment_requests",
            "max_comment_pages",
            "max_comments_per_video",
            "retries",
        ):
            if (
                type(getattr(self, name)) is not int
                or not 0 <= getattr(self, name) <= 10000
            ):
                raise ValueError(f"Invalid YouTube {name}")
        if (
            self.page_size > 50
            or self.order not in ("date", "relevance", "viewCount")
            or not 0 <= self.request_delay <= 60
            or self.retries > 3
        ):
            raise ValueError("Invalid YouTube pagination/order/rate policy")
        if not self.keywords or any(
            not isinstance(k, str) or not k.strip() for k in self.keywords
        ):
            raise ValueError("YouTube keywords must be nonempty strings")
        for value in (self.published_after, self.published_before):
            if value and datetime.fromisoformat(value).utcoffset() is None:
                raise ValueError("YouTube publication window must include timezone")

    def queries(self):
        values = []
        for seed in self.keywords:
            (
                values.extend(q.query for q in build_query_plan(seed))
                if self.include_geography
                else values.append(seed.strip())
            )
        return list(dict.fromkeys(values))[: self.max_queries]


def collect_youtube(
    api_key, configuration=None, *, client=None, sleep=time.sleep, progress=None
):
    if not api_key:
        raise APIStop("youtube_not_configured")
    c = configuration or YouTubeConfig()
    owned = client is None
    client = client or httpx.Client(timeout=c.timeout_seconds, follow_redirects=False)
    used = Counter(search=0, videos=0, commentThreads=0, comments=0)
    envelope = {
        "schema_version": "1",
        "source": "youtube",
        "started_at": datetime.now(UTC).isoformat(),
        "collected_at": None,
        "query_runs": [],
        "responses": [],
        "videos": [],
        "comments": [],
        "video_queries": {},
        "errors": [],
        "requests": {},
        "search_exposures": 0,
        "stopping_reason": "completed",
        "config": dict(c.__dict__),
    }

    def request(endpoint, params):
        for attempt in range(c.retries + 1):
            if (
                sum(used.values()) >= c.max_requests
                or (endpoint == "search" and used[endpoint] >= c.max_search_requests)
                or (
                    endpoint == "commentThreads"
                    and used[endpoint] >= c.max_comment_requests
                )
            ):
                raise APIStop("request_budget")
            sleep(c.request_delay * (2**attempt))
            used[endpoint] += 1
            if progress:
                progress(
                    {
                        "stage": "youtube_api",
                        "requests_used": sum(used.values()),
                        "request_counts": dict(used),
                        "search_exposures": envelope["search_exposures"],
                        "videos": len(envelope["videos"]),
                        "comments": len(envelope["comments"]),
                    }
                )
            try:
                response = client.get(
                    "https://www.googleapis.com/youtube/v3/" + endpoint,
                    params={k: v for k, v in params.items() if v is not None},
                    headers={"X-Goog-Api-Key": api_key},
                )
                data = response.json()
            except (httpx.HTTPError, ValueError):
                # Do not serialize exception text, request headers or error bodies.
                if attempt < c.retries:
                    continue
                raise APIStop("api_unavailable") from None
            if response.status_code == 200:
                envelope["responses"].append(
                    {"endpoint": endpoint, "params": params, "payload": data}
                )
                return data
            reason = next(
                (
                    e.get("reason", "api_error")
                    for e in data.get("error", {}).get("errors", [])
                ),
                "api_error",
            )
            reason = (
                reason if re.fullmatch(r"[A-Za-z0-9_]{1,80}", reason) else "api_error"
            )
            if (
                response.status_code == 429
                or response.status_code >= 500
                or reason in ("rateLimitExceeded", "userRateLimitExceeded")
            ) and attempt < c.retries:
                continue
            raise APIStop(reason, response.status_code)

    def pages(endpoint, params, limit):
        token, seen = None, set()
        for _ in range(limit):
            data = request(endpoint, {**params, "pageToken": token})
            yield data
            token = data.get("nextPageToken")
            if not data.get("items") or not token:
                return
            if token in seen:
                raise APIStop("repeated_page_token")
            seen.add(token)

    try:
        ids = []
        for query in c.queries():
            run = {
                "query": query,
                "pages": 0,
                "exposures": 0,
                "stopping_reason": "page_limit",
            }
            envelope["query_runs"].append(run)
            try:
                for data in pages(
                    "search",
                    {
                        "part": "snippet",
                        "type": "video",
                        "q": query,
                        "regionCode": "VN",
                        "relevanceLanguage": "vi",
                        "order": c.order,
                        "maxResults": c.page_size,
                        "publishedAfter": c.published_after,
                        "publishedBefore": c.published_before,
                    },
                    c.max_pages,
                ):
                    run["pages"] += 1
                    run["exposures"] += len(data.get("items", []))
                    envelope["search_exposures"] += len(data.get("items", []))
                    for item in data.get("items", []):
                        identity = item.get("id", {}).get("videoId")
                        if not identity:
                            continue
                        envelope["video_queries"].setdefault(identity, [])
                        if query not in envelope["video_queries"][identity]:
                            envelope["video_queries"][identity].append(query)
                        if identity not in ids and len(ids) < c.max_videos:
                            ids.append(identity)
                    if len(ids) >= c.max_videos:
                        run["stopping_reason"] = "video_limit"
                        break
                    if not data.get("items") or not data.get("nextPageToken"):
                        run["stopping_reason"] = "exhausted"
                if len(ids) >= c.max_videos:
                    envelope["stopping_reason"] = "video_limit"
                    break
            except APIStop as exc:
                run["stopping_reason"] = exc.reason
                envelope["errors"].append(
                    {"endpoint": "search", "reason": exc.reason, "status": exc.status}
                )
                envelope["stopping_reason"] = exc.reason
                break
        for offset in range(0, len(ids), 50):
            data = request(
                "videos",
                {
                    "part": "snippet,statistics,contentDetails",
                    "id": ",".join(ids[offset : offset + 50]),
                },
            )
            envelope["videos"].extend(data.get("items", []))
        seen_comments = set()
        for video in envelope["videos"]:
            video_id, count = video["id"], 0
            if (
                not c.max_comments_per_video
                or used["commentThreads"] >= c.max_comment_requests
            ):
                continue
            try:
                for data in pages(
                    "commentThreads",
                    {
                        "part": "snippet,replies",
                        "videoId": video_id,
                        "maxResults": min(100, c.max_comments_per_video),
                        "order": "time",
                        "textFormat": "plainText",
                    },
                    c.max_comment_pages,
                ):
                    for thread in data.get("items", []):
                        comment = thread.get("snippet", {}).get("topLevelComment")
                        if (
                            not comment
                            or comment["id"] in seen_comments
                            or count >= c.max_comments_per_video
                        ):
                            continue
                        seen_comments.add(comment["id"])
                        envelope["comments"].append(
                            {"video_id": video_id, "payload": comment}
                        )
                        count += 1
                        observed_replies = 0
                        for reply in thread.get("replies", {}).get("comments", []):
                            parent_id = reply.get("snippet", {}).get("parentId")
                            if parent_id != comment["id"]:
                                envelope["errors"].append(
                                    {
                                        "reason": "reply_parent_conflict",
                                        "video_id": video_id,
                                        "comment_id": reply.get("id"),
                                    }
                                )
                                continue
                            if (
                                reply.get("id") in seen_comments
                                or count >= c.max_comments_per_video
                            ):
                                continue
                            seen_comments.add(reply.get("id"))
                            envelope["comments"].append(
                                {
                                    "video_id": video_id,
                                    "parent_comment_id": parent_id,
                                    "payload": reply,
                                }
                            )
                            count += 1
                            observed_replies += 1
                        total_replies = thread.get("snippet", {}).get("totalReplyCount")
                        if (
                            type(total_replies) is int
                            and total_replies > observed_replies
                        ):
                            envelope["errors"].append(
                                {
                                    "reason": "partial_replies",
                                    "video_id": video_id,
                                    "comment_id": comment["id"],
                                    "expected": total_replies,
                                    "observed": observed_replies,
                                }
                            )
                    if count >= c.max_comments_per_video:
                        break
            except APIStop as exc:
                envelope["errors"].append(
                    {
                        "endpoint": "commentThreads",
                        "video_id": video_id,
                        "reason": exc.reason,
                        "status": exc.status,
                    }
                )
                if exc.reason in (
                    "quotaExceeded",
                    "dailyLimitExceeded",
                    "request_budget",
                    "api_unavailable",
                    "keyInvalid",
                    "accessNotConfigured",
                ):
                    raise
        if used["commentThreads"] >= c.max_comment_requests and c.max_comment_requests:
            envelope["comment_stopping_reason"] = "comment_request_budget"
    except APIStop as exc:
        envelope["stopping_reason"] = exc.reason
        envelope["errors"].append({"reason": exc.reason, "status": exc.status})
    finally:
        envelope["collected_at"] = (
            datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
        )
        envelope["requests"] = {
            "budget": c.max_requests,
            "used": sum(used.values()),
            "by_endpoint": dict(used),
            "remaining_quota": None,
        }
        if owned:
            client.close()
    return envelope
