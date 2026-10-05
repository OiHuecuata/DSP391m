"""Geocode PBPlus court records from crawler JSON."""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from time import sleep
from typing import Any

from geopy.exc import GeocoderServiceError
from geopy.geocoders import Nominatim
from tqdm import tqdm


def _collector_root() -> Path:
    return Path(__file__).resolve().parents[5]


def default_input_path() -> Path:
    return _collector_root() / ".local" / "pbplus" / "pickleball_sample.json"


def extract_address(name: str | None) -> str:
    """Legacy fallback for crawler records that do not contain an address."""
    if not name:
        return ""

    name = str(name).strip()
    if " - " in name:
        return name.split(" - ", 1)[1].strip()
    return name


def _normalize_place(value: str) -> str:
    folded = unicodedata.normalize("NFD", value.casefold())
    folded = "".join(
        character for character in folded if unicodedata.category(character) != "Mn"
    )
    folded = re.sub(
        r"\b(tinh|thanh pho|province|city|state|municipality)\b",
        " ",
        folded,
    )
    return " ".join(re.sub(r"[^a-z0-9]+", " ", folded).split())


def _expected_region(address: str) -> str | None:
    """Return the province/city suffix when it is present in the address."""
    region_prefix = re.compile(
        r"^(?:tỉnh|thành phố|province|city|municipality)\b",
        flags=re.IGNORECASE,
    )
    for component in reversed(address.split(",")):
        component = component.strip()
        if region_prefix.search(component):
            normalized = _normalize_place(component)
            return normalized or None
    return None


def geocode_address(
    geolocator: Nominatim,
    address: str,
) -> dict[str, Any]:
    if not address:
        return {"status": "missing_address"}

    try:
        location = geolocator.geocode(f"{address}, Vietnam", timeout=10)
    except GeocoderServiceError as exc:
        print(f"[pbplus-geo] geocoding failed for {address!r}: {exc}")
        return {"status": "error", "error": str(exc)}

    if location is None:
        return {"status": "not_found"}

    raw = location.raw if isinstance(location.raw, dict) else {}
    address_parts = raw.get("address") or {}
    if not isinstance(address_parts, dict):
        address_parts = {}
    country = address_parts.get("country")
    matched_text = str(raw.get("display_name") or location.address or "")
    normalized_country = _normalize_place(str(country or ""))
    matched_place = _normalize_place(matched_text)
    is_vietnam = normalized_country in {"vietnam", "viet nam"} or any(
        candidate in matched_place for candidate in ("vietnam", "viet nam")
    )
    if not is_vietnam:
        return {
            "status": "outside_vietnam",
            "matched_address": matched_text or None,
            "country": country,
        }

    expected_region = _expected_region(address)
    if expected_region:
        result_regions = [
            str(address_parts.get(key) or "")
            for key in (
                "state",
                "province",
                "region",
                "city",
                "municipality",
                "county",
                "district",
                "town",
                "village",
            )
        ]
        result_regions.append(matched_text)
        if not any(
            expected_region in _normalize_place(result_region)
            for result_region in result_regions
            if result_region
        ):
            return {
                "status": "region_mismatch",
                "matched_address": matched_text or None,
                "country": country,
                "expected_region": expected_region,
            }

    return {
        "status": "ok" if expected_region else "ok_region_unverified",
        "latitude": location.latitude,
        "longitude": location.longitude,
        "matched_address": matched_text or None,
        "country": country,
        "expected_region": expected_region,
    }


def geocode_payload(
    payload: dict[str, Any],
    *,
    delay: float = 1.0,
) -> dict[str, Any]:
    """Add address, latitude, and longitude to each crawler record."""
    if delay < 0:
        raise ValueError("delay must be >= 0")

    geolocator = Nominatim(user_agent="pickleball-demand-analysis")
    locations: dict[str, dict[str, Any]] = {}
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise TypeError("input JSON must contain a 'posts' list")

    for post in tqdm(posts, desc="Geocoding PBPlus courts"):
        name = post.get("court_name") or post.get("name")
        address = post.get("address")
        address_source = "venue_page" if address else "court_name_fallback"
        address = address or extract_address(name)
        post["address"] = address
        post["address_source"] = address_source

        if address not in locations:
            locations[address] = geocode_address(geolocator, address)
            if address and delay:
                sleep(delay)

        result = locations[address]
        post["geocode_status"] = result["status"]
        post["latitude"] = result.get("latitude")
        post["longitude"] = result.get("longitude")
        for key in ("matched_address", "country", "expected_region", "error"):
            if result.get(key) is not None:
                post[f"geocode_{key}"] = result[key]

    payload["geocoded_at"] = datetime.now(UTC).isoformat()
    return payload


def save_json(payload: dict[str, Any], output_path: Path) -> Path:
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def _parse_args() -> argparse.Namespace:
    output_dir = _collector_root() / ".local" / "pbplus"
    parser = argparse.ArgumentParser(
        description="Geocode PBPlus court records and save structured JSON."
    )
    parser.add_argument("--input", type=Path, default=default_input_path())
    parser.add_argument(
        "--output",
        type=Path,
        default=output_dir / "pickleball_geocoded.json",
    )
    parser.add_argument("--delay", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    input_path = args.input.expanduser().resolve()
    if not input_path.exists():
        print(f"[pbplus-geo] input file not found: {input_path}")
        return 1

    try:
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise TypeError("input JSON must be an object")
        geocoded_payload = geocode_payload(payload, delay=args.delay)
    except (json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
        print(f"[pbplus-geo] ERROR: {exc}")
        return 1

    successful = sum(
        post.get("latitude") is not None and post.get("longitude") is not None
        for post in geocoded_payload["posts"]
    )
    total = len(geocoded_payload["posts"])
    status_counts: dict[str, int] = {}
    for post in geocoded_payload["posts"]:
        status = post.get("geocode_status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    json_path = save_json(geocoded_payload, args.output)
    print(f"[pbplus-geo] geocoded {successful}/{total} locations")
    print(f"[pbplus-geo] location checks: {status_counts}")
    print(f"[pbplus-geo] saved JSON: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
