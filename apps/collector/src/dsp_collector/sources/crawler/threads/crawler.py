"""Smoke-test crawler for public Threads keyword search results.

Phase 1 only:
    Threads search -> up to N public posts -> local JSON file

This module intentionally does NOT call the normalizer, Kafka, Spark,
Data Lake, or Data Warehouse. It exists only to inspect source data quality.
"""
from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

THREADS_SEARCH_URL = "https://www.threads.com/search/"
DEFAULT_QUERY = "pickleball"
DEFAULT_LIMIT = 10
DEFAULT_SORT = "recent"
DEFAULT_TIMEOUT_MS = 60_000
VALID_SORTS = {"top", "recent"}
SEARCH_RESULTS_MARKER = '"searchResults":{"inform_module"'
class ThreadsCrawlerError(RuntimeError):
    """Raised when Threads search data cannot be fetched or parsed."""

def _collector_root() -> Path:
    """Return ``apps/collector`` from this file's repository location."""
    # crawler.py -> threads -> crawler -> sources -> dsp_collector -> src -> collector
    return Path(__file__).resolve().parents[5]

def default_output_path() -> Path:
    """Return the local smoke-test JSON output path."""
    return _collector_root() / ".local" / "threads" / "pickleball_sample.json"

def build_search_url(query: str, sort: str = DEFAULT_SORT) -> str:
    query = query.strip()
    if not query:
        raise ValueError("query must not be empty")

    if sort not in VALID_SORTS:
        raise ValueError(f"sort must be one of {sorted(VALID_SORTS)}")

    params = {
        "q": query,
        "serp_type": "default",
    }

    if sort == "recent":
        params["filter"] = "recent"

    return f"{THREADS_SEARCH_URL}?{urlencode(params)}"

def _extract_search_results_candidates(html: str,) -> list[dict[str, Any]]:
    """Extract every SSR searchResults object found in the page."""
    key = '"searchResults":'
    decoder = json.JSONDecoder()
    results: list[dict[str, Any]] = []
    cursor = 0

    while True:
        marker_index = html.find(SEARCH_RESULTS_MARKER, cursor)
        if marker_index < 0:
            break

        object_start = html.find("{",marker_index + len(key))
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
        raise ThreadsCrawlerError("No searchResults objects found in Threads page")

    return results

def _post_from_edge(edge: dict[str, Any]) -> dict[str, Any] | None:
    """Convert one Threads search edge into a compact raw smoke-test record."""
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
    permalink = (
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
        "url": permalink,
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

def _query_score(search_results: dict[str, Any],query: str) -> int:
    query = query.casefold()
    score = 0
    for edge in search_results.get("edges") or []:
        post = _post_from_edge(edge)
        if not post:
            continue
        text = post.get("text") or ""
        if query in text.casefold():
            score += 1
    return score

def parse_posts(html: str,query: str,limit: int) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Parse up to ``limit`` search posts from rendered Threads HTML."""
    if limit < 1:
        raise ValueError("limit must be >= 1")

    candidates = _extract_search_results_candidates(html)
    search_results = max(
        candidates,
        key=lambda candidate: _query_score(candidate, query),
    )
    score = _query_score(search_results, query)
    print(
        f"[threads-crawler] SSR candidates={len(candidates)}, "
        f"best_query_matches={score}",
        flush=True,
    )
    if score == 0:
        raise ThreadsCrawlerError(f"searchResults found, but no posts matched query {query!r}")

    edges = search_results.get("edges") or []
    posts: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for edge in edges:
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

def crawl_threads_posts(
    query: str = DEFAULT_QUERY,
    limit: int = DEFAULT_LIMIT,
    sort: str = DEFAULT_SORT,
    *,
    headless: bool = True,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
) -> dict[str, Any]:
    """Fetch one public Threads search page and return parsed smoke-test data."""
    if limit < 1:
        raise ValueError("limit must be >= 1")

    search_url = build_search_url(query, sort)
    print(f"[threads-crawler] opening: {search_url}", flush=True)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        page = browser.new_page(
            viewport={"width": 1440, "height": 1000},
            locale="en-US",
        )

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=timeout_ms)
            print(f"[threads-crawler] landed: {page.url}", flush=True)
            # The search data is SSR-embedded, but Threads can take a short time
            # to settle the rendered document. Do not wait for networkidle because
            # social apps may keep network connections alive indefinitely.
            page.wait_for_timeout(2_000)
            html = page.evaluate("document.documentElement.innerHTML")

            debug_dir = _collector_root() / ".local" / "threads"
            debug_dir.mkdir(parents=True, exist_ok=True)
            (debug_dir / "debug.html").write_text(html,encoding="utf-8")
            page.screenshot(path=str(debug_dir / "debug.png"),full_page=True)

            # One bounded retry if the SSR marker is not present yet.
            if SEARCH_RESULTS_MARKER not in html:
                print(
                    "[threads-crawler] searchResults not visible yet; retrying once...",
                    flush=True,
                )
                page.reload(wait_until="domcontentloaded", timeout=timeout_ms)
                page.wait_for_timeout(3_000)
                html = page.evaluate("document.documentElement.innerHTML")
        except PlaywrightTimeoutError as exc:
            raise ThreadsCrawlerError(
                f"Threads page timed out after {timeout_ms} ms"
            ) from exc
        finally:
            browser.close()

    posts, page_info = parse_posts(html=html, query=query, limit=limit)
    if not posts:
        raise ThreadsCrawlerError("Threads search loaded, but no parsable public posts were found")

    return {
        "source": "threads",
        "query": query,
        "sort": sort,
        "search_url": search_url,
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "count": len(posts),
        "page_info": page_info,
        "posts": posts,
    }

def save_json(payload: dict[str, Any], output_path: Path) -> Path:
    """Write the smoke-test payload as UTF-8 pretty JSON."""
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl a small public Threads keyword-search sample to JSON.")
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--sort", choices=sorted(VALID_SORTS), default=DEFAULT_SORT)
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output_path(),
        help="JSON output path",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Show Chromium window for debugging",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=DEFAULT_TIMEOUT_MS,
        help="Navigation timeout in milliseconds",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    print(
        f"[threads-crawler] start query={args.query!r} "
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