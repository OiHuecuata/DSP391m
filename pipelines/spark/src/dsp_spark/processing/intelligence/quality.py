"""Fail-closed DQ validation and reproducible population/coverage evidence."""

from collections import Counter

from ..clean.contracts import validate
from .catalog import GOLD, KEYS
from .common import digest, encode, instant
from .geography import POST, PRE


def validate_tables(tables, *, bronze_ids=None):
    for name, rows in tables.items():
        seen = set()
        for row in rows:
            validate(row, name)
            key = tuple(row[k] for k in KEYS[name])
            if any(k is None for k in key) or key in seen:
                raise ValueError(f"duplicate or null {name} grain: {key}")
            seen.add(key)
            if not row["lineage_ids"] and name != "national_location_coverage_v1":
                raise ValueError(f"missing {name} lineage")
            for field, value in row.items():
                if (
                    value is not None
                    and field.endswith(("_at", "_time"))
                    and isinstance(value, str)
                    and "T" in value
                    and (
                        instant(value).utcoffset().total_seconds() != 0
                        or not value.endswith("Z")
                    )
                ):
                    raise ValueError("canonical timestamps must use UTC Z")
            if row.get("latitude") is not None and (
                row.get("longitude") is None or not row.get("coordinate_source")
            ):
                raise ValueError("coordinates require a pair and provenance")
            if name in GOLD and not row["metric_definition_id"].endswith(name):
                raise ValueError("missing metric definition")
            if (
                bronze_ids is not None
                and name
                not in (
                    "dim_admin_unit",
                    "dim_location_alias",
                    "bridge_admin_crosswalk",
                )
                and not set(row["lineage_ids"]).issubset(bronze_ids)
            ):
                raise ValueError(f"{name} references absent or suppressed Bronze")
            if name == "court_supply_by_admin_version_v2":
                if (
                    row["known_count_venue_count"]
                    + row["unknown_count_venue_count"]
                    + row["conflicting_count_venue_count"]
                    != row["venue_count"]
                ):
                    raise ValueError("court-count denominators do not partition venues")
                if (
                    row["known_count_venue_count"] == 0
                    and row["known_court_count"] is not None
                ):
                    raise ValueError("unknown court supply must remain NULL")
            if (
                name == "national_location_coverage_v1"
                and row["resolved_records"] is not None
                and row["records_accepted"] is not None
                and row["resolved_records"] > row["records_accepted"]
            ):
                raise ValueError("coverage numerator exceeds eligible observations")
    if "dim_admin_unit" in tables:
        units = {r["admin_unit_id"]: r for r in tables["dim_admin_unit"]}
        for row in tables.get("bridge_admin_crosswalk", []):
            old, new = units.get(row["old_admin_unit_id"]), units.get(
                row["new_admin_unit_id"]
            )
            if (
                not old
                or not new
                or old["admin_version"] != PRE
                or new["admin_version"] != POST
            ):
                raise ValueError("invalid administrative crosswalk")
        for row in tables.get("dim_location_alias", []):
            if row["admin_unit_id"] not in units:
                raise ValueError("alias references missing administrative unit")
        for name in ("silver_court_venues", "silver_social_geo_mentions"):
            for row in tables.get(name, []):
                for field, version in (
                    ("old_province_id", PRE),
                    ("old_ward_id", PRE),
                    ("new_province_id", POST),
                    ("new_ward_id", POST),
                ):
                    key = row.get(field)
                    if key and (
                        key not in units or units[key]["admin_version"] != version
                    ):
                        raise ValueError(
                            "geographic projection has wrong administrative version"
                        )
    if "silver_court_venues" in tables:
        venues = {r["venue_id"] for r in tables["silver_court_venues"]}
        evidence = {r["evidence_id"] for r in tables["silver_venue_evidence"]}
        candidates = {r["candidate_id"] for r in tables["silver_venue_candidates"]}
        for row in tables["silver_venue_candidates"]:
            if (
                row["evidence_id"] not in evidence
                or row["venue_id"]
                and row["venue_id"] not in venues
            ):
                raise ValueError("invalid venue candidate reference")
        for row in tables["silver_venue_source_links"]:
            if row["venue_id"] not in venues or not set(row["evidence_ids"]).issubset(
                evidence
            ):
                raise ValueError("invalid source link")
        for row in tables["silver_geocoding_attempts"]:
            if row["candidate_id"] not in candidates:
                raise ValueError("geocoding references missing candidate")
    if "silver_social_posts" in tables:
        contents = {
            r["content_id"]
            for name in ("silver_social_posts", "silver_social_comments")
            for r in tables[name]
        }
        mentions = {r["mention_id"] for r in tables["silver_social_geo_mentions"]}
        for name in (
            "silver_social_geo_mentions",
            "silver_social_intent_evidence",
            "silver_social_engagement_snapshots",
        ):
            for row in tables[name]:
                if row["content_id"] not in contents:
                    raise ValueError("orphan content evidence")
                if (
                    row.get("intent_location_id")
                    and row["intent_location_id"] not in mentions
                ):
                    raise ValueError("intent references missing mention")
    return {
        "DQ-01": "PASS",
        "DQ-02": "PASS",
        "DQ-03": "PASS",
        "DQ-04": "PASS",
        "DQ-05": "PASS",
        "DQ-06": "PASS",
        "DQ-07": "PASS",
        "DQ-08": "PASS",
        "DQ-09": "PASS",
        "DQ-10": "REPLAY_CHECK_REQUIRED",
        "DQ-11": "MEASURED_SAMPLE_ONLY",
        "DQ-12": "ACTIVE_DERIVATIONS_FILTERED_ARCHIVE_ERASURE_EXTERNAL",
    }


def table_digest(rows):
    return digest(sorted(encode(r) for r in rows))


def profile(name, rows):
    keys = KEYS.get(name, ("observation_id",))
    result = {
        "rows": len(rows),
        "duplicate_keys": len(rows) - len({tuple(r[k] for k in keys) for r in rows}),
        "null_fields": dict(
            Counter(k for r in rows for k, v in r.items() if v is None)
        ),
        "sources": dict(
            Counter(
                r.get("platform", r.get("source_platform", "MULTI_SOURCE"))
                for r in rows
            )
        ),
        "digest": table_digest(rows),
    }
    for field in ("content_id", "venue_id", "source_record_id"):
        if rows and field in rows[0]:
            result["unique_" + field] = len({r[field] for r in rows if r[field]})
    if rows and "resolution_status" in rows[0]:
        result["resolution_status"] = dict(
            Counter(r["resolution_status"] for r in rows)
        )
    for field in (
        "published_at",
        "observed_at",
        "collected_at",
        "first_seen_at",
        "activity_date",
    ):
        values = [r.get(field) for r in rows if r.get(field)]
        if values:
            result[field + "_range"] = {"min": min(values), "max": max(values)}
    return result
