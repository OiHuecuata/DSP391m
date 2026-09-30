"""Threads smoke-test crawler.

This file owns source mechanics only:
- build/open Threads search URLs
- parse SSR search results
- iterate a domain-provided query plan
- let ``vietnam_terms`` adapt/filter raw posts
- deduplicate and write JSON

Vietnam/domain vocabulary and scoring do not live here.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

try:
    from .vietnam_terms import (
        QuerySpec,
        adapt_post,
        build_query_plan,
        merge_adapted_post,
    )
except ImportError:
    # Direct execution support:
    # python .../threads/crawler.py
    from vietnam_terms import (
        QuerySpec,
        adapt_post,
        build_query_plan,
        merge_adapted_post,
    )


THREADS_SEARCH_URL = "https://www.threads.com/search/"
DEFAULT_QUERY = "pickleball"
DEFAULT_LIMIT = 10
DEFAULT_SORT = "recent"
DEFAULT_TIMEOUT_MS = 60_000
DEFAULT_PER_QUERY_CAP = 3
VALID_SORTS = {"top", "recent"}
SEARCH_RESULTS_MARKER = '"searchResults":{"inform_module"'


class ThreadsCrawlerError(RuntimeError):
    """Raised when Threads search data cannot be fetched or parsed."""


def _collector_root() -> Path:
    return Path(__file__).resolve().parents[5]


def default_output_path() -> Path:
    return _collector_root() / ".local" / "threads" / "pickleball_sample.json"


def build_search_url(query: str, sort: str = DEFAULT_SORT) -> str:
    query = query.strip()

    if not query:
        raise ValueError("query must not be empty")
    if sort not in VALID_SORTS:
        raise ValueError(f"sort must be one of {sorted(VALID_SORTS)}")

    params: dict[str, str] = {
        "q": query,
        "serp_type": "default",
    }

    if sort == "recent":
        params["filter"] = "recent"

    return f"{THREADS_SEARCH_URL}?{urlencode(params)}"


def _extract_search_results_candidates(html: str) -> list[dict[str, Any]]:
    key = '"searchResults":'
    decoder = json.JSONDecoder()
    results: list[dict[str, Any]] = []
    cursor = 0

    while True:
        marker_index = html.find(SEARCH_RESULTS_MARKER, cursor)
        if marker_index < 0:
            break

        object_start = html.find("{", marker_index + len(key))
        if object_start < 0:
            break

        try:
            result, consumed = decoder.raw_decode(html[object_start:])
        except json.JSONDecodeError:
            cursor = marker_index + len(SEARCH_RESULTS_MARKER)
            continue

        if isinstance(result, dict):
            results.append(result)

        cursor = object_start + consumed

    if not results:
        raise ThreadsCrawlerError(
            "No searchResults objects found in Threads page"
        )

    return results


def _post_from_edge(edge: dict[str, Any]) -> dict[str, Any] | None:
    try:
        post = edge["node"]["thread"]["thread_items"][0]["post"]
    except (KeyError, IndexError, TypeError):
        return None

    if not isinstance(post, dict):
        return None

    user = post.get("user") or {}
    info = post.get("text_post_app_info") or {}
    caption = post.get("caption") or {}

    username = user.get("username")
    code = post.get("code")

    url = (
        f"https://www.threads.com/@{username}/post/{code}"
        if username and code
        else None
    )

    has_media = bool(
        ((post.get("image_versions2") or {}).get("candidates") or [])
        or (post.get("video_versions") or [])
        or (post.get("carousel_media") or [])
    )

    return {
        "id": post.get("pk"),
        "code": code,
        "url": url,
        "text": caption.get("text") if isinstance(caption, dict) else None,
        "taken_at": post.get("taken_at"),
        "like_count": post.get("like_count") or 0,
        "reply_count": info.get("direct_reply_count") or 0,
        "repost_count": info.get("repost_count") or 0,
        "quote_count": info.get("quote_count") or 0,
        "is_reply": bool(info.get("is_reply")),
        "media_type": post.get("media_type"),
        "has_media": has_media,
        "user": {
            "pk": user.get("pk"),
            "username": username,
            "full_name": user.get("full_name"),
            "is_verified": bool(user.get("is_verified")),
        },
    }


def _candidate_post_count(search_results: dict[str, Any]) -> int:
    return sum(
        1
        for edge in search_results.get("edges") or []
        if isinstance(edge, dict) and _post_from_edge(edge) is not None
    )


def parse_posts(
    html: str,
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if limit < 1:
        raise ValueError("limit must be >= 1")

    candidates = _extract_search_results_candidates(html)
    search_results = max(candidates, key=_candidate_post_count)

    print(
        f"[threads-crawler] SSR candidates={len(candidates)}, "
        f"selected_posts={_candidate_post_count(search_results)}",
        flush=True,
    )

    posts: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for edge in search_results.get("edges") or []:
        if not isinstance(edge, dict):
            continue

        post = _post_from_edge(edge)
        if post is None:
            continue

        post_id = post.get("id")
        if post_id is not None:
            post_id = str(post_id)
            if post_id in seen_ids:
                continue
            seen_ids.add(post_id)

        posts.append(post)

        if len(posts) >= limit:
            break

    return posts, search_results.get("page_info")


def _fetch_search_html(
    page: Page,
    *,
    query: str,
    sort: str,
    timeout_ms: int,
) -> tuple[str, str]:
    search_url = build_search_url(query, sort)
    print(f"[threads-crawler] opening: {search_url}", flush=True)

    try:
        page.goto(
            search_url,
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        page.wait_for_timeout(2_000)
        html = page.evaluate("document.documentElement.innerHTML")

        if SEARCH_RESULTS_MARKER not in html:
            print(
                "[threads-crawler] searchResults not visible yet; retrying once...",
                flush=True,
            )
            page.reload(
                wait_until="domcontentloaded",
                timeout=timeout_ms,
            )
            page.wait_for_timeout(3_000)
            html = page.evaluate("document.documentElement.innerHTML")
    except PlaywrightTimeoutError as exc:
        raise ThreadsCrawlerError(
            f"Threads page timed out after {timeout_ms} ms for query {query!r}"
        ) from exc

    return html, page.url


def _collect_for_query(
    page: Page,
    *,
    query_spec: QuerySpec,
    sort: str,
    timeout_ms: int,
    raw_limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, str]:
    """Fetch/parse one QuerySpec without applying domain rules."""
    html, landed_url = _fetch_search_html(
        page,
        query=query_spec.query,
        sort=sort,
        timeout_ms=timeout_ms,
    )
    posts, page_info = parse_posts(
        html=html,
        limit=raw_limit,
    )
    return posts, page_info, landed_url


def crawl_threads_posts(
    query: str = DEFAULT_QUERY,
    limit: int = DEFAULT_LIMIT,
    sort: str = DEFAULT_SORT,
    *,
    headless: bool = True,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
    per_query_cap: int = DEFAULT_PER_QUERY_CAP,
) -> dict[str, Any]:
    """Execute the domain query plan and collect adapted posts."""
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if per_query_cap < 1:
        raise ValueError("per_query_cap must be >= 1")

    query_plan = build_query_plan(query)

    collected: dict[str, dict[str, Any]] = {}
    query_runs: list[dict[str, Any]] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        page = browser.new_page(
            viewport={"width": 1440, "height": 1000},
            locale="vi-VN",
        )

        try:
            for query_spec in query_plan:
                if len(collected) >= limit:
                    break

                try:
                    raw_posts, page_info, landed_url = _collect_for_query(
                        page,
                        query_spec=query_spec,
                        sort=sort,
                        timeout_ms=timeout_ms,
                        raw_limit=30,
                    )
                except ThreadsCrawlerError as exc:
                    print(
                        f"[threads-crawler] query failed "
                        f"{query_spec.query!r}: {exc}",
                        flush=True,
                    )
                    query_runs.append(
                        {
                            "query": query_spec.query,
                            "kind": query_spec.kind,
                            "geo_name": query_spec.geo_name,
                            "status": "error",
                            "error": str(exc),
                        }
                    )
                    continue

                accepted_this_query = 0

                for raw_post in raw_posts:
                    adapted = adapt_post(
                        raw_post,
                        query_spec=query_spec,
                    )
                    if adapted is None:
                        continue

                    post_id = adapted.get("id")
                    if post_id is None:
                        continue

                    post_id = str(post_id)

                    if post_id in collected:
                        merge_adapted_post(
                            collected[post_id],
                            adapted,
                        )
                        continue

                    collected[post_id] = adapted
                    accepted_this_query += 1

                    if accepted_this_query >= per_query_cap:
                        break
                    if len(collected) >= limit:
                        break

                query_runs.append(
                    {
                        "query": query_spec.query,
                        "kind": query_spec.kind,
                        "geo_name": query_spec.geo_name,
                        "status": "ok",
                        "landed_url": landed_url,
                        "raw_count": len(raw_posts),
                        "accepted_count": accepted_this_query,
                        "page_info": page_info,
                    }
                )

                print(
                    f"[threads-crawler] query={query_spec.query!r} "
                    f"raw={len(raw_posts)} accepted={accepted_this_query} "
                    f"total={len(collected)}/{limit}",
                    flush=True,
                )

                page.wait_for_timeout(750)
        finally:
            browser.close()

    posts = list(collected.values())[:limit]

    if not posts:
        raise ThreadsCrawlerError(
            "No posts matched the configured Vietnam relevance policy"
        )

    return {
        "source": "threads",
        "seed_query": query,
        "sort": sort,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "count": len(posts),
        "query_runs": query_runs,
        "posts": posts,
    }


def save_json(payload: dict[str, Any], output_path: Path) -> Path:
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Crawl Vietnam-oriented Threads posts to JSON."
    )
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument(
        "--sort",
        choices=sorted(VALID_SORTS),
        default=DEFAULT_SORT,
    )
    parser.add_argument(
        "--per-query-cap",
        type=int,
        default=DEFAULT_PER_QUERY_CAP,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output_path(),
    )
    parser.add_argument("--headed", action="store_true")
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=DEFAULT_TIMEOUT_MS,
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    print(
        f"[threads-crawler] start seed={args.query!r} "
        f"sort={args.sort!r} limit={args.limit}",
        flush=True,
    )

    try:
        payload = crawl_threads_posts(
            query=args.query,
            limit=args.limit,
            sort=args.sort,
            headless=not args.headed,
            timeout_ms=args.timeout_ms,
            per_query_cap=args.per_query_cap,
        )
        output_path = save_json(payload, args.output)
    except (ThreadsCrawlerError, ValueError) as exc:
        print(f"[threads-crawler] ERROR: {exc}", flush=True)
        return 1

    print(
        f"[threads-crawler] saved {payload['count']} posts to {output_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
