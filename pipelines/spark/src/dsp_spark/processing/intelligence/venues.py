"""Evidence-preserving entity resolution with stable source links.

Automatic matching requires an exact normalized name AND a numbered address,
or explicitly provenance-tagged POI coordinates. Similar names alone never
merge. Contradictions survive in evidence and canonical conflict fields.
"""

import json
import re
from collections import defaultdict

from .common import digest, encode, folded, lineage, normalized, project, timestamp


def court_count(value, *, text=False):
    if value is None:
        return None
    scan = folded(str(value))
    match = (
        re.search(r"\b(\d+)\s+san\b", scan)
        if text
        else re.fullmatch(r"\s*(\d+)(?:\.0+)?\s*(?:san)?\s*", scan)
    )
    return int(match[1]) if match else None


def price(value):
    scan = folded(value)
    # Units such as monthly memberships and per-person fees cannot be compared
    # to court-hour fees. Bare prices remain unresolved in the original evidence.
    if not re.search(r"(?:/|moi\s+)(?:gio|h)\b", scan) or re.search(
        r"nguoi|thang|buoi", scan
    ):
        return None, None, None
    matches = re.findall(r"\b(\d+(?:[.,]\d+)?)\s*(k|nghin|ngan|trieu|vnd|d)\b", scan)
    values = [
        (
            float(v.replace(".", "").replace(",", ""))
            if u in ("vnd", "d") and re.fullmatch(r"\d{1,3}(?:[.,]\d{3})+", v)
            else float(v.replace(",", "."))
        )
        * {"k": 1000, "nghin": 1000, "ngan": 1000, "trieu": 1000000, "vnd": 1, "d": 1}[
            u
        ]
        for v, u in matches
    ]
    return (
        (min(values), max(values), "VND_PER_COURT_HOUR")
        if values
        else (None, None, None)
    )


def amenity(value, positive, negative):
    try:
        parsed = json.loads(value or "")
    except (ValueError, TypeError):
        parsed = None
    if isinstance(parsed, dict):
        values = {
            v
            for k, v in parsed.items()
            if isinstance(v, bool) and re.search(positive, folded(k))
        }
        return next(iter(values)) if len(values) == 1 else None
    if isinstance(parsed, list):
        if not all(isinstance(item, str) for item in parsed):
            return None
        value = " ".join(parsed)
    scan = folded(value)
    yes, no = bool(re.search(positive, scan)), bool(re.search(negative, scan))
    return True if yes and not no else False if no and not yes else None


def social_attributes(content):
    text = content.get("text_normalized", content.get("comment_text_normalized", ""))
    # Require an explicit named venue cue. A count of courts alone is not an
    # identifiable physical venue. Preserve the original text for review.
    match = re.search(
        r"(?:sân|Sân)\s+([A-ZĐ][^,.;!?\n]{1,70}?Pickleball)\b", text, re.IGNORECASE
    )
    if not match:
        return None
    name = normalized(match[1])
    if folded(name).startswith(("nao ", "pickleball")):
        return None
    address_match = re.search(
        r"(?:địa chỉ\s*:\s*|tại\s+)([^.;!?\n]{5,150})", text, re.IGNORECASE
    )
    if not address_match:
        address_match = re.search(
            r"(?:\b\d+\s+)?(?:đường|phố)\s+[^.;!?\n]{4,130}", text, re.IGNORECASE
        )
    low, high, unit = price(text)
    return {
        "venue_name": name,
        "address_text": (
            normalized(
                address_match[1]
                if address_match and address_match.lastindex
                else address_match[0]
            )
            if address_match
            else None
        ),
        "court_count": court_count(text, text=True),
        "price_min_vnd": low,
        "price_max_vnd": high,
        "price_unit": unit,
        "venue_status": (
            "CLOSED" if "SUPPLY_CLOSURE" in content["intent_labels"] else "UNKNOWN"
        ),
        "is_indoor": amenity(text, r"trong nha", r"ngoai troi"),
        "has_lighting": amenity(text, r"co den|den sang", r"khong co den"),
        "has_parking": amenity(text, r"co bai (?:xe|do)", r"khong co bai (?:xe|do)"),
        "latitude": None,
        "longitude": None,
        "geocoding_precision": "UNRESOLVED",
        "coordinate_source": None,
        "verification_status": "UNVERIFIED",
    }


def source_attributes(row):
    payload = json.loads(row["raw_payload"])
    low, high, unit = price(row.get("price_raw") or "")
    lat, lon = row.get("latitude_raw"), row.get("longitude_raw")
    coordinate_source = payload.get("coordinate_source")
    precision = payload.get("geocoding_precision", "UNRESOLVED")
    if precision not in ("POI", "STREET", "WARD", "PROVINCE", "UNRESOLVED"):
        precision = "UNRESOLVED"
    # A numeric pair without precision/provenance stays raw Bronze evidence.
    if lat is None or lon is None or not coordinate_source or precision == "UNRESOLVED":
        lat = lon = coordinate_source = None
        precision = "UNRESOLVED"
    raw_status = folded(row.get("venue_status_raw"))
    status = (
        "CLOSED"
        if raw_status in ("closed", "dong cua", "ngung hoat dong")
        else "ACTIVE" if raw_status in ("active", "open", "hoat dong") else "UNKNOWN"
    )
    verified = payload.get("verification_status") == "VERIFIED" and bool(
        payload.get("verification_evidence")
    )
    amenities = row.get("amenities_raw") or ""
    return {
        "venue_name": normalized(row.get("venue_name_raw")) or None,
        "address_text": normalized(row.get("address_raw")) or None,
        "court_count": court_count(row.get("court_count_raw")),
        "price_min_vnd": low,
        "price_max_vnd": high,
        "price_unit": unit,
        "venue_status": status,
        "is_indoor": amenity(amenities, r"trong nha|indoor", r"ngoai troi|outdoor"),
        "has_lighting": amenity(amenities, r"co den|lighting", r"khong co den"),
        "has_parking": amenity(amenities, r"bai (?:xe|do)|parking", r"khong co bai"),
        "latitude": lat,
        "longitude": lon,
        "geocoding_precision": precision,
        "coordinate_source": coordinate_source,
        "verification_status": (
            "VERIFIED"
            if verified
            else (
                "PROBABLE"
                if row["source_platform"] in ("pbplus", "osm", "overture")
                and row.get("venue_name_raw")
                and row.get("address_raw")
                else "UNVERIFIED"
            )
        ),
    }


def match_key(attributes):
    name, address = folded(attributes.get("venue_name")), folded(
        attributes.get("address_text")
    )
    if not name:
        return None
    if address and re.search(r"\b\d+[a-z]?\b", address) and len(address) >= 12:
        return ("NAME_NUMBERED_ADDRESS", name, address)
    if attributes.get("geocoding_precision") == "POI" and attributes.get(
        "coordinate_source"
    ):
        return (
            "NAME_PROVENANCE_POI",
            name,
            round(attributes["latitude"], 6),
            round(attributes["longitude"], 6),
        )
    return None


def build_venues(
    courts, contents, geography, *, previous_links=(), previous_candidates=()
):
    candidates, evidence, attempts = [], [], []
    inputs = []
    for row in courts:
        attrs = source_attributes(row)
        inputs.append(
            (row, attrs, None, "SOURCE_VENUE_OBSERVATION", row.get("description_raw"))
        )
    for content in contents:
        attrs = social_attributes(content)
        if attrs:
            # Preserve actual social platform/record type; never fake PBPlus.
            row = {
                "observation_id": content.get("source_observation_id")
                or digest([content["content_id"], content["text_hash"]]),
                "source_platform": content["platform"],
                "source_record_id": content["source_record_id"],
                "record_type": content["content_type"],
                "source_url": content["source_url"],
                "collected_at": content["last_seen_at"],
                "lineage_ids": content["lineage_ids"],
                "first_seen_at": content["first_seen_at"],
            }
            inputs.append(
                (
                    row,
                    attrs,
                    content["content_id"],
                    next(
                        (
                            l
                            for l in content["intent_labels"]
                            if l.startswith("SUPPLY_")
                        ),
                        "SOCIAL_VENUE_MENTION",
                    ),
                    content.get(
                        "text_normalized", content.get("comment_text_normalized")
                    ),
                )
            )
    persisted = {
        (r["source_platform"], r["source_record_id"], r["record_type"]): r["venue_id"]
        for r in previous_links
    }
    previous_by_id = {r["candidate_id"]: r for r in previous_candidates}
    key_index = defaultdict(set)
    for r in previous_candidates:
        key = match_key(r)
        if key and r["venue_id"]:
            key_index[key].add(r["venue_id"])
    # Chronological ordering makes backfill and replay choose the same identity.
    # On incremental runs the persistent source link always takes precedence.
    inputs.sort(
        key=lambda x: (
            timestamp(x[0]["collected_at"]),
            x[0]["source_platform"],
            x[0]["source_record_id"],
            x[0]["observation_id"],
        )
    )
    link_evidence = defaultdict(list)
    attrs_by_candidate = {}
    for row, attrs, cid, typ, span in inputs:
        source_key = (
            row["source_platform"],
            row["source_record_id"],
            row["record_type"],
        )
        key = match_key(attrs)
        hits = key_index[key] if key else set()
        existing = persisted.get(source_key)
        ambiguous = len(hits) > 1 or bool(existing and hits and existing not in hits)
        matched = bool(existing or hits)
        venue_id = (
            None
            if ambiguous
            else existing
            or next(iter(hits), None)
            or digest(["venue", key if key else source_key])
        )
        if venue_id:
            persisted[source_key] = venue_id
            if key:
                key_index[key].add(venue_id)
        evidence_id = digest(["venue_evidence", row["observation_id"], cid])
        candidate_id = digest(["venue_candidate", evidence_id])
        prior = previous_by_id.get(candidate_id)
        match_status = (
            "AMBIGUOUS"
            if ambiguous
            else (
                prior["match_status"]
                if prior and prior["venue_id"] == venue_id
                else "MATCHED" if matched else "NEW"
            )
        )
        candidate = project(
            "silver_venue_candidates",
            dict(
                **attrs,
                candidate_id=candidate_id,
                venue_id=venue_id,
                source_platform=row["source_platform"],
                source_record_id=row["source_record_id"],
                evidence_id=evidence_id,
                discovered_at=timestamp(
                    row.get("first_seen_at") or row["collected_at"]
                ),
                match_status=match_status,
                lineage_ids=lineage([row]),
            ),
        )
        candidates.append(candidate)
        attrs_by_candidate[candidate_id] = attrs
        evidence.append(
            project(
                "silver_venue_evidence",
                {
                    "evidence_id": evidence_id,
                    "candidate_id": candidate_id,
                    "venue_id": venue_id,
                    "content_id": cid,
                    "source_platform": row["source_platform"],
                    "source_record_id": row["source_record_id"],
                    "source_url": row.get("source_url"),
                    "observed_at": timestamp(row["collected_at"]),
                    "evidence_type": typ,
                    "evidence_span": span,
                    "extraction_method": (
                        "SOURCE_FIELDS" if cid is None else "SOCIAL_RULE_UNCALIBRATED"
                    ),
                    "attributes_json": encode(attrs),
                    "lineage_ids": lineage([row]),
                },
            )
        )
        if venue_id:
            link_evidence[source_key].append((candidate, evidence[-1]))
        fake_content = {
            "content_id": candidate_id,
            "platform": row["source_platform"],
            "content_type": "Venue",
            "text_hash": digest(attrs.get("address_text")),
            "published_at": timestamp(row["collected_at"]),
            "lineage_ids": lineage([row]),
        }
        mentions = geography.mentions(fake_content, attrs.get("address_text") or "")
        resolved_ids = sorted({i for m in mentions for i in m["raw_admin_unit_ids"]})
        attempts.append(
            project(
                "silver_geocoding_attempts",
                {
                    "attempt_id": digest(
                        [
                            candidate_id,
                            attrs.get("address_text"),
                            geography.reference_hash,
                        ]
                    ),
                    "candidate_id": candidate_id,
                    "query": attrs.get("address_text"),
                    "address_hash": digest(attrs.get("address_text")),
                    "provider": "REPOSITORY_GAZETTEER",
                    "geocoding_provider": "REPOSITORY_GAZETTEER",
                    "candidate_admin_unit_ids": resolved_ids,
                    "precision": attrs["geocoding_precision"],
                    "geocoding_precision": attrs["geocoding_precision"],
                    "resolution_status": (
                        "RESOLVED"
                        if attrs["latitude"] is not None
                        else "ADMIN_ONLY" if resolved_ids else "UNRESOLVED"
                    ),
                    "coordinate_source": attrs["coordinate_source"],
                    "latitude": attrs["latitude"],
                    "longitude": attrs["longitude"],
                    "geocoded_at": timestamp(row["collected_at"]),
                    "source_reference": "; ".join(
                        sorted(
                            {
                                m["source_reference"]
                                for m in mentions
                                if m["source_reference"]
                            }
                        )
                    )
                    or None,
                    "dependency_status": (
                        "SOURCE_COORDINATES"
                        if attrs["latitude"] is not None
                        else "POI_GEOCODER_UNAVAILABLE"
                    ),
                    "lineage_ids": lineage([row]),
                },
            )
        )
    links, canonical_groups = [], defaultdict(list)
    for source_key, records in sorted(link_evidence.items()):
        records.sort(key=lambda pair: (pair[1]["observed_at"], pair[1]["evidence_id"]))
        latest_candidate, latest_evidence = records[-1]
        venue_id = latest_candidate["venue_id"]
        link = project(
            "silver_venue_source_links",
            {
                "source_platform": source_key[0],
                "source_record_id": source_key[1],
                "record_type": source_key[2],
                "venue_id": venue_id,
                "first_seen_at": min(r[0]["discovered_at"] for r in records),
                "last_seen_at": latest_evidence["observed_at"],
                "match_method": (
                    match_key(attrs_by_candidate[latest_candidate["candidate_id"]])[0]
                    if match_key(attrs_by_candidate[latest_candidate["candidate_id"]])
                    else "PERSISTENT_SOURCE_ID"
                ),
                "evidence_ids": sorted(r[1]["evidence_id"] for r in records),
                "lineage_ids": lineage([r[1] for r in records]),
            },
        )
        links.append(link)
        canonical_groups[venue_id].append((latest_candidate, latest_evidence, link))
    venues = []
    for venue_id, records in sorted(canonical_groups.items()):
        conflicts = []

        def agreed(field, records=records, conflicts=conflicts):
            values = {
                encode(
                    attrs_by_candidate[c["candidate_id"]].get(field)
                ): attrs_by_candidate[c["candidate_id"]].get(field)
                for c, _, _ in records
                if attrs_by_candidate[c["candidate_id"]].get(field) is not None
            }
            if len(values) > 1:
                conflicts.append(field)
                return None
            return next(iter(values.values()), None)

        fields = {
            field: agreed(field)
            for field in (
                "venue_name",
                "address_text",
                "court_count",
                "price_min_vnd",
                "price_max_vnd",
                "price_unit",
                "is_indoor",
                "has_lighting",
                "has_parking",
                "latitude",
                "longitude",
                "coordinate_source",
            )
        }
        statuses = {
            c["venue_status"] for c, _, _ in records if c["venue_status"] != "UNKNOWN"
        }
        if len(statuses) > 1:
            conflicts.append("venue_status")
        status = next(iter(statuses)) if len(statuses) == 1 else "UNKNOWN"
        verification = (
            "VERIFIED"
            if any(c["verification_status"] == "VERIFIED" for c, _, _ in records)
            else (
                "PROBABLE"
                if any(c["verification_status"] == "PROBABLE" for c, _, _ in records)
                else "UNVERIFIED"
            )
        )
        last = max(e["observed_at"] for _, e, _ in records)
        fake = {
            "content_id": venue_id,
            "platform": "venue",
            "content_type": "Venue",
            "text_hash": digest(fields["address_text"]),
            "published_at": last,
            "lineage_ids": lineage([r[2] for r in records]),
        }
        mentions = geography.mentions(fake, fields["address_text"] or "")
        old = {m["old_province_id"] for m in mentions if m["old_province_id"]}
        new = {m["new_province_id"] for m in mentions if m["new_province_id"]}
        old_wards = {m["old_ward_id"] for m in mentions if m["old_ward_id"]}
        new_wards = {m["new_ward_id"] for m in mentions if m["new_ward_id"]}
        fields["address_normalized"] = fields.pop("address_text")
        precision = {
            c["geocoding_precision"] for c, _, _ in records if c["latitude"] is not None
        }
        venues.append(
            project(
                "silver_court_venues",
                dict(
                    **fields,
                    venue_id=venue_id,
                    old_province_id=next(iter(old)) if len(old) == 1 else None,
                    new_province_id=next(iter(new)) if len(new) == 1 else None,
                    old_ward_id=next(iter(old_wards)) if len(old_wards) == 1 else None,
                    new_ward_id=next(iter(new_wards)) if len(new_wards) == 1 else None,
                    geocoding_precision=(
                        next(iter(precision))
                        if len(precision) == 1 and fields["latitude"] is not None
                        else "UNRESOLVED"
                    ),
                    court_count_status=(
                        "CONFLICTING"
                        if "court_count" in conflicts
                        else "KNOWN" if fields["court_count"] is not None else "UNKNOWN"
                    ),
                    venue_status=status,
                    verification_status=verification,
                    first_seen_at=min(l["first_seen_at"] for _, _, l in records),
                    last_seen_at=last,
                    source_count=len({c["source_platform"] for c, _, _ in records}),
                    conflicting_fields=sorted(conflicts),
                    evidence_ids=sorted(
                        {i for _, _, l in records for i in l["evidence_ids"]}
                    ),
                    source_platforms=sorted(
                        {c["source_platform"] for c, _, _ in records}
                    ),
                    lineage_ids=lineage([r[2] for r in records]),
                ),
            )
        )
        for c, _, _ in records:
            c["conflicting_fields"] = sorted(conflicts)
    return {
        "silver_venue_candidates": candidates,
        "silver_court_venues": venues,
        "silver_venue_evidence": evidence,
        "silver_venue_source_links": links,
        "silver_geocoding_attempts": attempts,
    }


def link_content_venues(contents, venues):
    by_name = defaultdict(list)
    for venue in venues:
        if venue["venue_name"]:
            by_name[folded(venue["venue_name"])].append(venue["venue_id"])
    for content in contents:
        text = folded(
            content.get("text_normalized", content.get("comment_text_normalized", ""))
        )
        content["mentioned_venue_ids"] = sorted(
            {
                ids[0]
                for name, ids in by_name.items()
                if len(ids) == 1
                and re.search(r"(?<!\w)" + re.escape(name) + r"(?!\w)", text)
            }
        )
