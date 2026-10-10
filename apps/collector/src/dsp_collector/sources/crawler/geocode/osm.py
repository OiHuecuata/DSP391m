"""Geocode PBPlus court records from crawler JSON."""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from pathlib import Path
from time import sleep
from typing import Any

from geopy.exc import GeocoderServiceError
from geopy.geocoders import Nominatim
from tqdm import tqdm


def _collector_root() -> Path:
    return Path(__file__).resolve().parents[5]


def default_input_path() -> Path:
    return _collector_root() / ".local" / "pbplus" / "pickleball_court_raw.json"


def extract_address(name: str | None) -> str:
    """Legacy fallback for crawler records that do not contain an address."""
    if not name:
        return ""

    name = str(name).strip()
    if " - " in name:
        return name.split(" - ", 1)[1].strip()
    return name


def clean_scraped_address(address: str | None) -> str:
    """Remove court description text accidentally appended to an address."""
    if not address:
        return ""

    value = " ".join(str(address).split()).strip()
    value = re.split(
        r"\s+(?:Điểm nổi bật|Số sân con|Số lượng sân(?: con)?|Giờ mở cửa|Các dịch vụ|Giá tiền chi tiết|Danh sách sân con|Thời gian hoạt động|Giá thuê sân|Số điện thoại(?: liên hệ| liên lạc)?|Link Map|Link bản đồ|Bản đồ Google Maps)\s*:",
        value,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    value = re.split(
        r"\s+-\s+(?:Thời gian hoạt động|Giá thuê sân|Pickleball\s+.+?\s+có quy mô)\s*:",
        value,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    value = re.split(
        r"\s+(?:Với cơ sở vật chất|Nằm ngay tại|Các dịch vụ|Giá tiền chi tiết|Danh sách sân con|Lịch trống|Ưu đãi đặc biệt|Dịch vụ đa dạng|Hotline|Liên hệ ngay|Tập máy)\b|\s*[✅⏰🔥🎾📞🛎🏆]|\s*\(?\s*(?:Link\s+(?:Map|bản\s+đồ)|Bản\s+đồ\s+Google\s+Maps)\b",
        value,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    return value.strip(" .,:;-\t")


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


_ADMIN_AREA_PREFIX = re.compile(
    r"^(?:phường|xã|thị trấn|quận|huyện|thị xã|thành phố|tỉnh)\s+",
    flags=re.IGNORECASE,
)


def _administrative_fallbacks(address: str) -> list[tuple[str, str]]:
    """Build ward/district queries for addresses the geocoder cannot place."""
    parts = [part.strip() for part in address.split(",") if part.strip()]
    admin_indices = [
        index for index, part in enumerate(parts) if _ADMIN_AREA_PREFIX.match(part)
    ]
    if not admin_indices:
        return []

    admin_parts = parts[admin_indices[0] :]
    candidates: list[tuple[str, str]] = []
    full_area = ", ".join(admin_parts)
    if full_area:
        first_prefix = _ADMIN_AREA_PREFIX.match(admin_parts[0])
        prefix = first_prefix.group(0).casefold().strip() if first_prefix else ""
        level = "ward" if prefix in {"phường", "xã", "thị trấn"} else "district"
        candidates.append((full_area, level))

    city_parts = [
        part for part in admin_parts if re.match(r"^(?:thành phố|tỉnh)\s+", part, re.I)
    ]
    city = city_parts[-1] if city_parts else ""
    for prefix, level in (("phường", "ward"), ("xã", "ward"), ("quận", "district"), ("huyện", "district"), ("thị xã", "district")):
        area = next(
            (
                part
                for part in admin_parts
                if re.match(rf"^{prefix}\s+", part, re.IGNORECASE)
            ),
            None,
        )
        if area:
            query = ", ".join(part for part in (area, city) if part)
            if query and all(existing[0].casefold() != query.casefold() for existing in candidates):
                candidates.append((query, level))

    return candidates


def _fallback_area_matches(query: str, address_parts: dict[str, Any], matched_text: str) -> bool:
    """Reject a nearby result unless it actually names the requested area."""
    first_component = query.split(",", 1)[0].strip()
    prefix_match = _ADMIN_AREA_PREFIX.match(first_component)
    prefix = prefix_match.group(0).casefold().strip() if prefix_match else ""
    expected_area = _normalize_place(_ADMIN_AREA_PREFIX.sub("", first_component))
    if not expected_area:
        return False

    if prefix in {"phường", "xã", "thị trấn"}:
        keys = ("suburb", "quarter", "neighbourhood", "village", "hamlet")
        label = "phuong" if prefix == "phường" else "xa"
        normalized_match = f" {_normalize_place(matched_text)} "
        if f" {label} {expected_area} " in normalized_match:
            return True
    else:
        keys = (
            "state",
            "province",
            "region",
            "city",
            "municipality",
            "county",
            "district",
            "city_district",
        )
        normalized_match = f" {_normalize_place(matched_text)} "
        label = "quan" if prefix == "quận" else "huyen"
        if f" {label} {expected_area} " in normalized_match:
            return True

    result_areas = [str(address_parts.get(key) or "") for key in keys]
    expected = f" {expected_area} "
    return any(
        expected in f" {_normalize_place(value)} "
        for value in result_areas
        if value
    )


def geocode_address(
    geolocator: Nominatim,
    address: str,
    *,
    delay: float = 1.0,
) -> dict[str, Any]:
    if not address:
        return {"status": "missing_address"}

    queries = [(address, "street_address"), *_administrative_fallbacks(address)]
    mismatch_result: dict[str, Any] | None = None
    for query_index, (query, match_level) in enumerate(queries):
        if query_index and delay:
            sleep(delay)
        try:
            location = geolocator.geocode(
                query,
                timeout=10,
                country_codes="vn",
                addressdetails=True,
                language="vi,en",
            )
        except GeocoderServiceError as exc:
            print(f"[pbplus-geo] geocoding failed for {query!r}: {exc}")
            return {"status": "error", "error": str(exc)}

        if location is None:
            continue

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

        if match_level != "street_address" and not _fallback_area_matches(
            query, address_parts, matched_text
        ):
            continue

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
                mismatch_result = {
                    "status": "region_mismatch",
                    "matched_address": matched_text or None,
                    "country": country,
                    "expected_region": expected_region,
                }
                continue

        result = {
            "status": "ok" if expected_region else "ok_region_unverified",
            "latitude": location.latitude,
            "longitude": location.longitude,
            "matched_address": matched_text or None,
            "country": country,
            "expected_region": expected_region,
            "match_level": match_level,
        }
        return result

    return mismatch_result or {"status": "not_found"}


def geocode_payload(
    payload: dict[str, Any],
    *,
    delay: float = 1.0,
) -> dict[str, Any]:
    """Add address, latitude, and longitude to each crawler record."""
    if delay < 0:
        raise ValueError("delay must be >= 0")

    geolocator = Nominatim(
        user_agent="dsp391m-pbplus-court-geocoder",
        timeout=10,
    )
    locations: dict[str, dict[str, Any]] = {}
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise TypeError("input JSON must contain a 'posts' list")

    for post in tqdm(posts, desc="Geocoding PBPlus courts"):
        name = post.get("court_name") or post.get("name")
        # Keep the crawler's original fields byte-for-byte in meaning. Clean a
        # separate query value for geocoding instead of replacing its address.
        address = clean_scraped_address(post.get("address"))
        address = address or clean_scraped_address(extract_address(name))

        if address not in locations:
            locations[address] = geocode_address(geolocator, address, delay=delay)
            if address and delay:
                sleep(delay)

        result = locations[address]
        # Remove diagnostic fields left by older output versions. Keep the
        # crawler record and only append coordinates as the geocoding result.
        for key in (
            "address_source",
            "geocode_status",
            "geocode_matched_address",
            "geocode_country",
            "geocode_expected_region",
            "geocode_error",
            "geocode_match_level",
        ):
            post.pop(key, None)
        # Coordinates are deliberately written last so they appear at the end
        # of each JSON record, after every field supplied by the crawler.
        post["latitude"] = result.get("latitude")
        post["longitude"] = result.get("longitude")

    # This timestamp was added by older geocoder output and is not part of the
    # PBPlus crawler payload.
    payload.pop("geocoded_at", None)
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
        default=output_dir / "pbplus_osm_geocoded.json",
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
