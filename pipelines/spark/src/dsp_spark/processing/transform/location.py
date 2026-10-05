"""Explicit named-place aliases shared by text mentions and source addresses.

No network, fuzzy matching, accent stripping, GIS, or geographic inference.
Drivers load the catalog; executor closures receive the validated alias map.
"""

import json
import re
import unicodedata
from functools import lru_cache
from importlib.resources import files
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..clean.contracts import validate


def normalize_match_text(value: str) -> str:
    value = unicodedata.normalize("NFKC", value).casefold()
    return " ".join("".join(c if c.isalnum() else " " for c in value).split())


def validated_alias_map(catalog: dict) -> dict[str, tuple[str, str]]:
    validate(catalog, "location_reference")
    aliases = {}
    ids = set()
    for place in catalog["locations"]:
        key = place["location_id"]
        if key in ids:
            raise ValueError(f"Duplicate location_id: {key}")
        ids.add(key)
        name = place["canonical_name"]
        if not normalize_match_text(name):
            raise ValueError("Canonical name must contain letters or numbers")
        normalized = {normalize_match_text(a) for a in place["aliases"]}
        if not all(normalized):
            raise ValueError("Aliases must contain letters or numbers")
        if normalize_match_text(name) not in normalized:
            raise ValueError(f"Canonical name must be an explicit alias: {name}")
        for alias in normalized:
            if alias in aliases and aliases[alias][0] != key:
                raise ValueError(f"Conflicting normalized alias: {alias}")
            aliases[alias] = (key, name)
    return aliases


@lru_cache(maxsize=1)
def load_alias_map() -> dict[str, tuple[str, str]]:
    relative = "configs/reference/vietnam_locations.v1.json"
    root = next(
        (p for p in Path(__file__).resolve().parents if (p / relative).is_file()), None
    )
    path = (
        root / relative
        if root is not None
        else files("dsp_contracts") / "reference/vietnam_locations.v1.json"
    )
    return validated_alias_map(json.loads(path.read_text(encoding="utf-8")))


def resolve_alias(value: str | None, aliases=None):
    selected = load_alias_map() if aliases is None else aliases
    return selected.get(normalize_match_text(value)) if value else None


def resolve_court_address(
    address: str | None, address_source: str | None, aliases=None
):
    """Resolve explicit comma components, not arbitrary page-body substrings.

    The observed PBPlus address sometimes includes page text before its final
    'Địa chỉ :' marker. Read only that final address section. Municipality and
    province prefixes label source components; they do not imply equivalences
    between nested city/province names. Multiple distinct catalog places are
    ambiguous. Court-name fallbacks never establish physical address evidence.
    """
    selected = load_alias_map() if aliases is None else aliases
    if address_source == "court_name_fallback":
        return None, None, "court_name_fallback"
    if not address or not address.strip():
        return None, None, "missing_address"
    section = re.split(r"địa\s+chỉ\s*:", address, flags=re.IGNORECASE)[-1]
    # Observed address suffixes contain these non-address page annotations.
    section = re.split(
        r"\s+(?:bản đồ|link (?:map|bản đồ)|số điện thoại|điểm nổi bật)\b|\s+-\s+",
        section,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    matches = set()
    for component in section.split(","):
        normalized = normalize_match_text(component)
        normalized = re.sub(r"^(?:thành phố|tp|tỉnh)\s+", "", normalized)
        match = selected.get(normalized)
        if match:
            matches.add(match)
    if len(matches) == 1:
        key, name = next(iter(matches))
        return key, name, "address_alias"
    return None, None, "ambiguous_address" if matches else "unmapped_address"


def location_expression(value, *, address_source=None):
    aliases = load_alias_map()
    schema = T.StructType(
        [
            T.StructField("location_id", T.StringType()),
            T.StructField("location_name", T.StringType()),
            T.StructField("location_resolution", T.StringType()),
        ]
    )
    if address_source is not None:
        return F.udf(
            lambda text, source: resolve_court_address(text, source, aliases), schema
        )(value, address_source)

    def resolve_mention(text):
        match = resolve_alias(text, aliases)
        return (*match, "text_alias") if match else (None, text, "unmapped_mention")

    return F.udf(resolve_mention, schema)(value)


def main():
    aliases = load_alias_map()
    print(
        json.dumps(
            {
                "schema_version": "1",
                "locations": len({v[0] for v in aliases.values()}),
                "normalized_aliases": len(aliases),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
