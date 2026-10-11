"""Crawler for pickleball court listings on pickleballplus.vn.

The JSON output uses the same top-level envelope as the Threads crawler so
both sources can be consumed in the same way. Court-specific fields stay on
each record.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

BASE_URL = "https://pickleballplus.vn"
LIST_URL = f"{BASE_URL}/tim-kiem-san"
DEFAULT_LIMIT = 1000
DEFAULT_TIMEOUT = 20
DEFAULT_DELAY = 0.5
DEFAULT_RETRIES = 2
RELATED_COURT_MARKERS = ("Các sân liên quan", "Sân liên quan")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/26.0 Safari/605.1.15"
    )
}


class PBPlusCrawlerError(RuntimeError):
    """Raised when court listings cannot be collected."""


class PBPlusAccessBlocked(PBPlusCrawlerError):
    """Stop rather than retry authentication or rate-limit responses."""


def _collector_root() -> Path:
    return Path(__file__).resolve().parents[5]


def default_output_path() -> Path:
    return _collector_root() / ".local" / "pbplus" / "pickleball_sample.json"


def get_page(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> BeautifulSoup:
    """Fetch and parse a page, retrying transient request failures."""
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            if response.status_code in (401, 403, 429):
                raise PBPlusAccessBlocked(
                    f"Public directory access blocked: HTTP {response.status_code}"
                )
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(min(2**attempt, 8) + random.uniform(0, 0.25))

    raise AssertionError("unreachable")


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


def _find_heading(soup: BeautifulSoup, title: str) -> Tag | None:
    expected = " ".join(title.casefold().split())
    for heading in soup.find_all(re.compile(r"^h[1-6]$")):
        actual = " ".join(heading.get_text(" ", strip=True).casefold().split())
        if actual == expected:
            return heading
    return None


def _section_nodes(heading: Tag) -> list[Any]:
    """Return content following a heading until the next peer section."""
    heading_level = int(heading.name[1])
    nodes: list[Any] = []

    for node in heading.find_all_next():
        node_name = getattr(node, "name", None)
        if (
            node_name
            and re.fullmatch(r"h[1-6]", node_name)
            and int(node_name[1]) <= heading_level
        ):
            break
        nodes.append(node)

    return nodes


def _section_text(soup: BeautifulSoup, title: str) -> str:
    heading = _find_heading(soup, title)
    if heading is None:
        return ""

    parts = [
        str(node).strip()
        for node in _section_nodes(heading)
        if getattr(node, "name", None) is None and str(node).strip()
    ]
    return " ".join(parts)


def _section_items(soup: BeautifulSoup, title: str) -> list[str]:
    heading = _find_heading(soup, title)
    if heading is None:
        return []

    items: list[str] = []
    for node in _section_nodes(heading):
        if getattr(node, "name", None) != "li":
            continue
        value = node.get_text(" ", strip=True)
        if value and value not in items:
            items.append(value)
    return items


def _section_has_media(soup: BeautifulSoup, title: str) -> bool:
    heading = _find_heading(soup, title)
    if heading is None:
        return False

    return any(
        getattr(node, "name", None) == "img"
        and any(
            node.get(attribute)
            for attribute in ("src", "data-src", "data-original", "data-lazy-src")
        )
        for node in _section_nodes(heading)
    )


def _main_venue_text(soup: BeautifulSoup) -> str:
    """Exclude recommended venues so their fields cannot bleed into this one."""
    text = soup.get_text(" ", strip=True)
    folded_text = text.casefold()
    cut_points = [
        index
        for marker in RELATED_COURT_MARKERS
        if (index := folded_text.find(marker.casefold())) >= 0
    ]
    return text[: min(cut_points)] if cut_points else text


def _extract_labeled_value(
    text: str,
    label: str,
    *,
    stop_labels: tuple[str, ...],
) -> str | None:
    stops = "|".join(re.escape(value) for value in stop_labels)
    pattern = rf"\b{re.escape(label)}\s*:\s*(.+?)(?=\s+(?:{stops})\s*:|$)"
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None

    value = " ".join(match.group(1).split()).strip(" :-")
    return value or None


def extract_number_of_courts(text: str) -> int | None:
    """Extract a court count from Vietnamese listing text."""
    patterns = (
        r"(?:số lượng\s+)?sân(?:\s+con)?\s*:\s*(\d+)\b",
        r"(?:quy mô|gồm)\s*(?:cụm\s*)?(\d+)\s*sân\b",
        r"(\d+)\s*sân(?:\s+pickleball)?\b",
    )

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return int(match.group(1))

    return None


def count_subcourts(text: str) -> int | None:
    matches = re.findall(
        r"(?:Sân con|Sân mái che|Sân trong nhà|Sân ngoài trời)\s*(\d+)\b",
        text,
        flags=re.IGNORECASE,
    )
    return max((int(value) for value in matches), default=None)


def scrape_court(
    url: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
) -> dict[str, Any]:
    soup = get_page(url, timeout=timeout, retries=retries)
    heading = soup.find("h1")
    name = heading.get_text(" ", strip=True) if heading else None
    page_text = soup.get_text(" ", strip=True)
    main_venue_text = _main_venue_text(soup)

    # Read only the current venue's introduction and subcourt section so
    # recommendations elsewhere on the page cannot inflate its court count.
    introduction = _section_text(soup, "Giới thiệu") or main_venue_text
    subcourt_text = _section_text(soup, "Danh sách sân con") or main_venue_text
    description_count = extract_number_of_courts(introduction)
    subcourt_count = count_subcourts(subcourt_text)
    number_of_courts = subcourt_count or description_count
    code = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1] or None

    address = _extract_labeled_value(
        page_text,
        "Địa chỉ",
        stop_labels=(
            "Phone",
            "Số điện thoại",
            "Sđt liên hệ",
            "SĐT",
            "Chủ sân",
            "Khoảng giá",
            "Giá tiền",
            "Số lượng sân",
            "Số lượng sân con",
            "Quy mô",
            "Thông tin sân",
            "Các dịch vụ",
            "Thời gian mở cửa",
            "Các sân liên quan",
        ),
    )
    phone = _extract_labeled_value(
        page_text,
        "Phone",
        stop_labels=(
            "Chủ sân",
            "Khoảng giá",
            "Giá tiền",
            "Số lượng sân",
            "Quy mô",
            "Các dịch vụ",
            "Thời gian mở cửa",
            "Các sân liên quan",
        ),
    )
    opening_hours = _section_items(soup, "Thời gian mở cửa")
    services = _section_items(soup, "Các dịch vụ")

    record: dict[str, Any] = {
        "id": url,
        "code": code,
        "url": url,
        "has_media": _section_has_media(soup, "Hình ảnh sân"),
        "court_name": name,
        "description": introduction or None,
        "number_of_courts": number_of_courts,
    }
    if address:
        record["address"] = address
    if phone:
        record["phone"] = phone
    if opening_hours:
        record["opening_hours"] = opening_hours
    if services:
        record["services"] = services
    return record


def crawl_pbplus_courts(
    limit: int = DEFAULT_LIMIT,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    delay: float = DEFAULT_DELAY,
    retries: int = DEFAULT_RETRIES,
) -> dict[str, Any]:
    """Collect court listings in a Threads-compatible result envelope."""
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if timeout < 1:
        raise ValueError("timeout must be >= 1")
    if delay < 0:
        raise ValueError("delay must be >= 0")
    if retries < 0:
        raise ValueError("retries must be >= 0")

    started_at = datetime.now(UTC).isoformat()
    listing_soup = get_page(LIST_URL, timeout=timeout, retries=retries)
    all_links = find_court_links(listing_soup)
    raw_links = [
        a["href"] for a in listing_soup.select("a[href]") if "/san-tap/" in a["href"]
    ]
    court_links = all_links[:limit]
    posts: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    blocked = False
    requested = 0

    for url in tqdm(court_links, desc="Crawling PBPlus courts"):
        requested += 1
        try:
            posts.append(scrape_court(url, timeout=timeout, retries=retries))
        except PBPlusAccessBlocked as exc:
            errors.append(
                {"url": url, "error_type": type(exc).__name__, "error": str(exc)}
            )
            blocked = True
            break
        except (requests.RequestException, ValueError) as exc:
            errors.append(
                {"url": url, "error_type": type(exc).__name__, "error": str(exc)}
            )
            print(f"[pbplus-crawler] failed {url}: {exc}", flush=True)

        if delay:
            time.sleep(delay + random.uniform(0, 0.1))

    query_run: dict[str, Any] = {
        "query": LIST_URL,
        "kind": "directory",
        "geo_name": "Vietnam",
        "status": (
            "empty"
            if not court_links
            else "error" if errors and not posts else "partial" if errors else "ok"
        ),
        "landed_url": LIST_URL,
        "raw_count": len(court_links),
        "failed_count": len(errors),
        "accepted_count": len(posts),
        "listing_entries_discovered": len(raw_links),
        "unique_links_discovered": len(all_links),
        "duplicate_links": len(raw_links) - len(all_links),
        "detail_pages_requested": requested,
        "stop_reason": (
            "access_blocked"
            if blocked
            else "directory_exhausted" if len(all_links) <= limit else "limit"
        ),
        "blocked": blocked,
    }
    if errors:
        query_run["errors"] = errors

    return {
        "source": "pbplus",
        "seed_query": "pickleball courts",
        "sort": "directory",
        "started_at": started_at,
        "collected_at": datetime.now(UTC).isoformat(),
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
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
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
            retries=args.retries,
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
