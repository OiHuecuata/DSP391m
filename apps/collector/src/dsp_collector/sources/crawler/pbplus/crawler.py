"""Crawler for pickleball court listings on pickleballplus.vn.

The JSON output uses the same top-level envelope as the Threads crawler so
both sources can be consumed in the same way. Court-specific fields stay on
each record.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


BASE_URL = "https://pickleballplus.vn"
LIST_URL = f"{BASE_URL}/tim-kiem-san"
DEFAULT_LIMIT = 1000
DEFAULT_TIMEOUT = 20
DEFAULT_DELAY = 0.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/26.0 Safari/605.1.15"
    )
}


class PBPlusCrawlerError(RuntimeError):
    """Raised when court listings cannot be collected."""


def _collector_root() -> Path:
    return Path(__file__).resolve().parents[5]


def default_output_path() -> Path:
    return _collector_root() / ".local" / "pbplus" / "pickleball_sample.json"


def get_page(url: str, *, timeout: int = DEFAULT_TIMEOUT) -> BeautifulSoup:
    response = requests.get(url, headers=HEADERS, timeout=timeout)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def find_court_links(soup: BeautifulSoup) -> list[str]:
    """Find unique detail-page links in a court search page."""
    links: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if "/san-tap/" not in href:
            continue

        full_url = urljoin(BASE_URL, href)
        if full_url not in seen:
            links.append(full_url)
            seen.add(full_url)

    return links


def extract_number_of_courts(text: str) -> int | None:
    """Extract a court count from Vietnamese listing text."""
    patterns = (
        r"(?:quy mô|gồm)\s*(\d+)\s*sân\b",
        r"(\d+)\s*sân(?:\s+pickleball)?\b",
    )

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))

    return None


def count_subcourts(soup: BeautifulSoup) -> int | None:
    text = soup.get_text(" ", strip=True)
    matches = re.findall(r"Sân con\s+(\d+)", text, flags=re.IGNORECASE)
    return max((int(value) for value in matches), default=None)


def scrape_court(url: str, *, timeout: int = DEFAULT_TIMEOUT) -> dict[str, Any]:
    soup = get_page(url, timeout=timeout)
    heading = soup.find("h1")
    name = heading.get_text(" ", strip=True) if heading else None
    page_text = soup.get_text(" ", strip=True)

    description_count = extract_number_of_courts(page_text)
    subcourt_count = count_subcourts(soup)
    number_of_courts = subcourt_count or description_count
    code = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1] or None

    return {
        "id": url,
        "code": code,
        "url": url,
        "has_media": bool(soup.select("img[src]")),
        "court_name": name,
        "number_of_courts": number_of_courts,
    }


def crawl_pbplus_courts(
    limit: int = DEFAULT_LIMIT,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    delay: float = DEFAULT_DELAY,
) -> dict[str, Any]:
    """Collect court listings in a Threads-compatible result envelope."""
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if timeout < 1:
        raise ValueError("timeout must be >= 1")
    if delay < 0:
        raise ValueError("delay must be >= 0")

    listing_soup = get_page(LIST_URL, timeout=timeout)
    court_links = find_court_links(listing_soup)[:limit]
    posts: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for url in tqdm(court_links, desc="Crawling PBPlus courts"):
        try:
            posts.append(scrape_court(url, timeout=timeout))
        except requests.RequestException as exc:
            errors.append({"url": url, "error": str(exc)})
            print(f"[pbplus-crawler] failed {url}: {exc}", flush=True)

        if delay:
            time.sleep(delay)

    query_run: dict[str, Any] = {
        "query": LIST_URL,
        "kind": "directory",
        "geo_name": "Vietnam",
        "status": "ok" if not errors else "partial",
        "landed_url": LIST_URL,
        "raw_count": len(court_links),
        "accepted_count": len(posts),
    }
    if errors:
        query_run["errors"] = errors

    return {
        "source": "pbplus",
        "seed_query": "pickleball courts",
        "sort": "directory",
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "count": len(posts),
        "query_runs": [query_run],
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
        description="Crawl pickleball court listings from pickleballplus.vn."
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    parser.add_argument("--output", type=Path, default=default_output_path())
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    print(f"[pbplus-crawler] start limit={args.limit}", flush=True)

    try:
        payload = crawl_pbplus_courts(
            limit=args.limit,
            timeout=args.timeout,
            delay=args.delay,
        )
    except (PBPlusCrawlerError, requests.RequestException, ValueError) as exc:
        print(f"[pbplus-crawler] ERROR: {exc}", flush=True)
        return 1

    json_path = save_json(payload, args.output)
    print(
        f"[pbplus-crawler] saved {payload['count']} courts to {json_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
