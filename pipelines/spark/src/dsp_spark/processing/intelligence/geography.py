"""Versioned interpretation from the repository's provenance-tagged gazetteer.

No reverse allocation from a merged province. No invented codes or centroids.
Optional custom references can supply validated many-to-many crosswalks.
"""

import json
import re
from collections import Counter
from importlib.resources import files
from pathlib import Path

from .common import digest, folded, project, vn_date

PRE = "VN_ADMIN_PRE_2025"
POST = "VN_ADMIN_POST_2025"
CUTOVER = "2025-07-01"


def load_reference(path=None):
    if path:
        return json.loads(Path(path).read_text())
    root = next(
        (
            p
            for p in Path(__file__).resolve().parents
            if (p / "configs/reference/threads_geography.v1.json").exists()
        ),
        None,
    )
    target = (
        root / "configs/reference/threads_geography.v1.json"
        if root
        else files("dsp_contracts") / "reference/threads_geography.v1.json"
    )
    return json.loads(target.read_text())


class Geography:
    def __init__(self, reference=None):
        reference = reference or load_reference()
        self.reference_hash = digest(reference)
        self.units, self.crosswalk, self.aliases = [], [], []
        locations = [
            r
            for r in reference["locations"]
            if r["admin_version"] in ("pre_2025_07", "2025_07")
        ]
        lookup = {
            (r["admin_version"], r["canonical_name"]): r
            for r in locations
            if r["level"] == "province"
        }
        counts = Counter(
            r.get("current_name")
            for r in locations
            if r["level"] == "province" and r["admin_version"] == "pre_2025_07"
        )
        for row in locations:
            pre = row["admin_version"] == "pre_2025_07"
            parent = lookup.get((row["admin_version"], row.get("parent_name")))
            # This repository reference is explicitly the immediately pre-July
            # snapshot; earlier validity and historical renamed units are unknown.
            unit = project(
                "dim_admin_unit",
                {
                    "admin_unit_id": row["geo_id"],
                    "admin_code": row.get("official_code"),
                    "admin_name": row["canonical_name"],
                    "admin_level": row["level"].upper(),
                    "admin_version": PRE if pre else POST,
                    "parent_admin_unit_id": parent["geo_id"] if parent else None,
                    "valid_from": None if pre else CUTOVER,
                    "valid_to": "2025-06-30" if pre else None,
                    "source_reference": row["source_url"],
                    "reference_status": "CURATED_INTERNAL_IDS_NO_BOUNDARIES",
                    "lineage_ids": [row["source_url"]],
                },
            )
            self.units.append(unit)
            if pre and row["level"] == "province":
                target = lookup.get(("2025_07", row.get("current_name")))
                if target:
                    self.crosswalk.append(
                        project(
                            "bridge_admin_crosswalk",
                            {
                                "old_admin_unit_id": row["geo_id"],
                                "new_admin_unit_id": target["geo_id"],
                                "relation_type": (
                                    "MERGED"
                                    if counts[row.get("current_name")] > 1
                                    else "UNCHANGED"
                                ),
                                "mapping_method": "REPOSITORY_PROVINCE_REFERENCE",
                                "source_reference": row["source_url"],
                                "lineage_ids": [row["source_url"]],
                            },
                        )
                    )
            for alias in sorted(set(row.get("aliases", []) + [row["canonical_name"]])):
                alias_type = (
                    "OFFICIAL"
                    if alias == row["canonical_name"]
                    else (
                        "HISTORICAL"
                        if alias in row.get("alias_provenance", {})
                        else "COLLOQUIAL"
                    )
                )
                self.aliases.append(
                    project(
                        "dim_location_alias",
                        {
                            "alias_id": digest([row["geo_id"], alias]),
                            "alias_text": alias,
                            "alias_type": alias_type,
                            "admin_unit_id": row["geo_id"],
                            "source_reference": row.get("alias_provenance", {}).get(
                                alias, row["source_url"]
                            ),
                            "lineage_ids": [row["source_url"]],
                        },
                    )
                )
        # Caller-provided crosswalks are evidence-bearing explicit relationships,
        # never inferred by spelling or allocating overlap percentages.
        for mapping in reference.get("crosswalks", []):
            self.crosswalk.append(
                project(
                    "bridge_admin_crosswalk",
                    {**mapping, "lineage_ids": [mapping["source_reference"]]},
                )
            )
        self.by_id = {u["admin_unit_id"]: u for u in self.units}
        self.forward = {}
        for mapping in self.crosswalk:
            if (
                mapping["old_admin_unit_id"] not in self.by_id
                or mapping["new_admin_unit_id"] not in self.by_id
            ):
                raise ValueError("crosswalk references unknown unit")
            if (
                self.by_id[mapping["old_admin_unit_id"]]["admin_version"] != PRE
                or self.by_id[mapping["new_admin_unit_id"]]["admin_version"] != POST
            ):
                raise ValueError("crosswalk has incorrect administrative versions")
            self.forward.setdefault(mapping["old_admin_unit_id"], []).append(
                mapping["new_admin_unit_id"]
            )
        aliases = {}
        for alias in self.aliases:
            aliases.setdefault(folded(alias["alias_text"]), set()).add(
                alias["admin_unit_id"]
            )
        self.alias_lookup = aliases
        self.unversioned_refs = {}
        for row in reference["locations"]:
            if row["admin_version"] not in ("pre_2025_07", "2025_07"):
                for alias in row.get("aliases", [row["canonical_name"]]):
                    self.unversioned_refs[folded(alias)] = row["source_url"]
        self.pattern = re.compile(
            r"(?<!\w)(?:"
            + "|".join(
                re.escape(a)
                for a in sorted(
                    set(aliases) | set(self.unversioned_refs),
                    key=lambda a: (-len(a), a),
                )
            )
            + r")(?!\w)",
            re.IGNORECASE,
        )

    def resolve(self, raw, published_at=None):
        ids = sorted(self.alias_lookup.get(folded(raw), []))
        old = [i for i in ids if self.by_id[i]["admin_version"] == PRE]
        new = [i for i in ids if self.by_id[i]["admin_version"] == POST]
        day = vn_date(published_at)
        modern = day is not None and day >= CUTOVER
        # A raw name shared across versions is contextual; after the cutover it
        # represents the modern province, never a precise old merged territory.
        written = new if modern and new else old if day and not modern and old else ids
        historic = new if modern and new else old if day and not modern else []
        if modern and not new and len(old) == 1:
            historic = self.forward.get(old[0], [])
        harmonized = (
            new
            if new and (modern or not old)
            else sorted({n for o in old for n in self.forward.get(o, [])})
        )
        # Multiple modern targets from SPLIT/PARTIAL mappings remain ambiguous.
        old_id = (
            old[0]
            if len(old) == 1 and ((day is not None and not modern) or not new)
            else None
        )
        new_id = harmonized[0] if len(harmonized) == 1 else None
        ambiguous = len(written) > 1 or len(harmonized) > 1
        status = "AMBIGUOUS" if ambiguous else "RESOLVED" if written else "UNRESOLVED"
        return {
            "old_id": old_id,
            "new_id": new_id,
            "ids": ids,
            "status": status,
            "modes": {
                "AS_WRITTEN": written,
                "HISTORICAL": historic,
                "HARMONIZED": harmonized,
            },
        }

    def mentions(self, content, text, *, role="UNSPECIFIED"):
        # Fold Vietnamese diacritics while maintaining offsets in the NFC text.
        scan = folded(text)
        result = []
        matches = list(self.pattern.finditer(scan))
        # Explicit administrative cues outside the limited gazetteer remain
        # actual unresolved evidence instead of disappearing from coverage.
        cue = re.compile(
            r"(?i:phường|xã|quận|huyện|tỉnh|thành phố|TP\.)\s+(?:[A-ZÀ-ỸĐ][\wÀ-ỹđ]*|\d+)(?:\s+[A-ZÀ-ỸĐ][\wÀ-ỹđ]*){0,3}"
        )
        for match in cue.finditer(text):
            if not any(
                match.start() < m.end() and m.start() < match.end() for m in matches
            ):
                matches.append(match)
        for match in sorted(matches, key=lambda m: (m.start(), m.end())):
            raw = text[match.start() : match.end()]
            resolution = self.resolve(raw, content.get("published_at"))
            old_unit = self.by_id.get(resolution["old_id"], {})
            new_unit = self.by_id.get(resolution["new_id"], {})
            old_province = (
                resolution["old_id"]
                if old_unit.get("admin_level") == "PROVINCE"
                else old_unit.get("parent_admin_unit_id")
            )
            new_province = (
                resolution["new_id"]
                if new_unit.get("admin_level") == "PROVINCE"
                else new_unit.get("parent_admin_unit_id")
            )
            refs = sorted(
                {self.by_id[i]["source_reference"] for i in resolution["ids"]}
            )
            if folded(raw) in self.unversioned_refs:
                refs.append(self.unversioned_refs[folded(raw)])
            result.append(
                project(
                    "silver_social_geo_mentions",
                    {
                        "mention_id": digest(
                            [
                                content["content_id"],
                                content["text_hash"],
                                match.start(),
                                match.end(),
                                role,
                                self.reference_hash,
                            ]
                        ),
                        "content_id": content["content_id"],
                        "content_type": content["content_type"],
                        "platform": content["platform"],
                        "location_text_raw": raw,
                        "location_role": role,
                        "old_province_id": old_province,
                        "old_ward_id": (
                            resolution["old_id"]
                            if old_unit.get("admin_level") == "WARD"
                            else None
                        ),
                        "new_province_id": new_province,
                        "new_ward_id": (
                            resolution["new_id"]
                            if new_unit.get("admin_level") == "WARD"
                            else None
                        ),
                        "resolution_precision": (
                            "WARD"
                            if any(
                                self.by_id[i]["admin_level"] == "WARD"
                                for i in resolution["ids"]
                            )
                            else "PROVINCE" if resolution["ids"] else "UNRESOLVED"
                        ),
                        "resolution_status": resolution["status"],
                        "evidence_span": raw,
                        "span_start": match.start(),
                        "span_end": match.end(),
                        "extraction_method": "GAZETTEER_RULE_UNCALIBRATED",
                        "raw_admin_unit_ids": resolution["ids"],
                        "interpretation_json": json.dumps(
                            resolution["modes"], sort_keys=True
                        ),
                        "source_reference": "; ".join(refs) or None,
                        "lineage_ids": content["lineage_ids"],
                    },
                )
            )
        return result


def assignments(mentions):
    """Distinct provable province projections; never sum them nationally."""
    values = {(PRE, r["old_province_id"]) for r in mentions if r.get("old_province_id")}
    values |= {
        (POST, r["new_province_id"]) for r in mentions if r.get("new_province_id")
    }
    return sorted(values) or [("UNRESOLVED", "UNRESOLVED")]
