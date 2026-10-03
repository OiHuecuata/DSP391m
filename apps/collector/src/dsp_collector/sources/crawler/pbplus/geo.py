"""Geocode PBPlus court records from crawler JSON."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any

from geopy.geocoders import Nominatim
from tqdm import tqdm


def _collector_root() -> Path:
    return Path(__file__).resolve().parents[5]


def default_input_path() -> Path:
    return _collector_root() / ".local" / "pbplus" / "pickleball_sample.json"


def extract_address(name: str | None) -> str:
    """Use the address suffix when a court name includes one."""
    if not name:
        return ""

    name = str(name).strip()
    if " - " in name:
        return name.split(" - ", 1)[1].strip()
    return name


def geocode_address(
    geolocator: Nominatim,
    address: str,
) -> tuple[float | None, float | None]:
    if not address:
        return None, None

    try:
        location = geolocator.geocode(f"{address}, Vietnam", timeout=10)
    except Exception as exc:
        print(f"[pbplus-geo] geocoding failed for {address!r}: {exc}")
        return None, None

    if location is None:
        return None, None
    return location.latitude, location.longitude


def geocode_payload(
    payload: dict[str, Any],
    *,
    delay: float = 1.0,
) -> dict[str, Any]:
    """Add address, latitude, and longitude to each crawler record."""
    if delay < 0:
        raise ValueError("delay must be >= 0")

    geolocator = Nominatim(user_agent="pickleball-demand-analysis")
    locations: dict[str, tuple[float | None, float | None]] = {}
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("input JSON must contain a 'posts' list")

    for post in tqdm(posts, desc="Geocoding PBPlus courts"):
        name = post.get("court_name") or post.get("name")
        address = post.get("address") or extract_address(name)
        post["address"] = address

        if address not in locations:
            locations[address] = geocode_address(geolocator, address)
            if delay:
                sleep(delay)

        post["latitude"], post["longitude"] = locations[address]

    payload["geocoded_at"] = datetime.now(timezone.utc).isoformat()
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
            raise ValueError("input JSON must be an object")
        geocoded_payload = geocode_payload(payload, delay=args.delay)
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        print(f"[pbplus-geo] ERROR: {exc}")
        return 1

    successful = sum(
        post.get("latitude") is not None and post.get("longitude") is not None
        for post in geocoded_payload["posts"]
    )
    total = len(geocoded_payload["posts"])
    json_path = save_json(geocoded_payload, args.output)
    print(f"[pbplus-geo] geocoded {successful}/{total} locations")
    print(f"[pbplus-geo] saved JSON: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
