"""P0 and P1 analytics over validated Silver entities and metric snapshots.

Location projections and NATIONAL unique-content groups are separate scopes.
Publication dates always use Vietnam time; snapshot_as_of is measurement time.
Missing measurements remain NULL; no model confidence or demand is invented.
"""

import json
from collections import defaultdict
from datetime import date, timedelta
from statistics import median

from .catalog import GOLD
from .common import instant, lineage, project, timestamp, vn_date
from .geography import assignments


def total(values):
    known = [v for v in values if v is not None]
    return sum(known) if known else None


def rate(numerator, denominator):
    return numerator / denominator if numerator is not None and denominator else None


def month(day):
    return day[:7] + "-01"


def dimensions(row, mentions):
    return assignments(mentions.get(row["content_id"], [])) + [("NATIONAL", "NATIONAL")]


def finish(name, row, source_rows, generated_at, *, status="OBSERVED_SAMPLE_PARTIAL"):
    platforms = sorted(
        {
            s
            for r in source_rows
            for s in r.get(
                "source_platforms", [r.get("platform", r.get("source_platform"))]
            )
            if s
        }
    )
    return project(
        name,
        dict(
            **row,
            lineage_ids=lineage(source_rows),
            source_platforms=platforms,
            generated_at=generated_at,
            metric_definition_id="requirements-v1.0:" + name,
            completeness_status=status,
        ),
    )


def supply(venues, candidates, *, snapshot_date, generated_at):
    groups = defaultdict(dict)
    for venue in venues:
        if (
            venue["verification_status"] == "UNVERIFIED"
            or venue["venue_status"] == "CLOSED"
        ):
            continue
        for version, location in assignments([venue]) + [("NATIONAL", "NATIONAL")]:
            groups[(version, location)][venue["venue_id"]] = venue
    # Unknown geographies are explicitly scoped, never distributed over regions.
    result = []
    for (version, location), entities in sorted(groups.items()):
        rows = list(entities.values())
        known = [
            r
            for r in rows
            if r["court_count_status"] == "KNOWN" and r["court_count"] is not None
        ]
        priced = [
            r["price_min_vnd"]
            for r in rows
            if r["price_min_vnd"] is not None
            and r["price_unit"] == "VND_PER_COURT_HOUR"
        ]
        result.append(
            finish(
                "court_supply_by_admin_version_v2",
                {
                    "snapshot_date": snapshot_date,
                    "admin_version": version,
                    "admin_unit_id": location,
                    "venue_count": len(rows),
                    "known_court_count": total(r["court_count"] for r in known),
                    "known_count_venue_count": len(known),
                    "unknown_count_venue_count": sum(
                        r["court_count_status"] == "UNKNOWN" for r in rows
                    ),
                    "conflicting_count_venue_count": sum(
                        r["court_count_status"] == "CONFLICTING" for r in rows
                    ),
                    "court_count_coverage_rate": rate(len(known), len(rows)),
                    "verified_venue_count": sum(
                        r["verification_status"] == "VERIFIED" for r in rows
                    ),
                    "probable_venue_count": sum(
                        r["verification_status"] == "PROBABLE" for r in rows
                    ),
                    "price_min_vnd": min(priced) if priced else None,
                    "price_median_vnd": float(median(priced)) if priced else None,
                    "priced_venue_count": len(priced),
                    "price_unit": "VND_PER_COURT_HOUR" if priced else None,
                    "venue_last_observed_at": max(r["last_seen_at"] for r in rows),
                    "unverified_candidate_count": sum(
                        c["verification_status"] == "UNVERIFIED"
                        and c["venue_id"] in entities
                        for c in candidates
                    ),
                },
                rows,
                generated_at,
            )
        )
    return result


def social_daily(silver, *, generated_at, as_of, thresholds=None):
    thresholds = thresholds or {}
    contents = silver["silver_social_posts"] + silver["silver_social_comments"]
    mentions = defaultdict(list)
    for row in silver["silver_social_geo_mentions"]:
        mentions[row["content_id"]].append(row)
    evidence = defaultdict(list)
    for row in silver["silver_social_intent_evidence"]:
        evidence[(row["content_id"], row["intent_type"])].append(row)
    latest = {}
    for row in silver["silver_social_engagement_snapshots"]:
        if row["snapshot_status"] != "AVAILABLE" or instant(
            row["observed_at"]
        ) > instant(as_of):
            continue
        if (
            row["content_id"] not in latest
            or row["observed_at"] > latest[row["content_id"]]["observed_at"]
        ):
            latest[row["content_id"]] = row
    activity_groups, engagement_groups, intent_groups, comment_groups = (
        defaultdict(dict) for _ in range(4)
    )
    for row in contents:
        if (
            not row["published_date_vn"]
            or row["is_duplicate"]
            or instant(row["published_at"]) > instant(as_of)
        ):
            continue
        day, cid = row["published_date_vn"], row["content_id"]
        for version, location in dimensions(row, mentions):
            activity_groups[(day, version, location)][cid] = row
            if (
                "post_id" in row
                and "comment_id" not in row
                and row["is_pickleball_related"] is True
            ):
                engagement_groups[(day, version, location, row["platform"])][cid] = row
            if "comment_id" in row:
                comment_groups[(day, row["platform"], location, version)][cid] = row
            # Independently classified intent is retained, with uncertain topic
            # visible in Silver; explicitly irrelevant text cannot become demand.
            if row["is_pickleball_related"] is not False:
                for label in row["intent_labels"]:
                    intent_groups[(day, location, version, row["platform"], label)][
                        cid
                    ] = row
    result = {
        name: []
        for name in (
            "social_activity_by_admin_version_daily",
            "social_engagement_daily_v1",
            "social_intent_daily_v2",
            "social_comment_analytics_daily_v1",
        )
    }
    for (day, version, location), entities in sorted(activity_groups.items()):
        rows = list(entities.values())
        posts = [r for r in rows if "comment_id" not in r]
        comments = [r for r in rows if "comment_id" in r]
        classified = sum(r["is_pickleball_related"] is not None for r in rows)
        result["social_activity_by_admin_version_daily"].append(
            finish(
                "social_activity_by_admin_version_daily",
                {
                    "activity_date": day,
                    "admin_version": version,
                    "admin_unit_id": location,
                    "post_count": len(posts),
                    "comment_count": sum(not r["is_reply"] for r in comments),
                    "reply_count": sum(r["is_reply"] for r in comments),
                    "unique_content_count": len(rows),
                    "relevant_post_count": sum(
                        r["is_pickleball_related"] is True for r in posts
                    ),
                    "relevant_comment_count": sum(
                        r["is_pickleball_related"] is True for r in comments
                    ),
                    "classified_content_count": classified,
                    "classification_coverage_rate": rate(classified, len(rows)),
                    "geographic_interpretation": (
                        "RAW_EVIDENCE_PRE_AND_FORWARD_HARMONIZED_POST"
                        if version != "NATIONAL"
                        else "NATIONAL_DISTINCT_CONTENT"
                    ),
                },
                rows + mentions_for(rows, mentions),
                generated_at,
            )
        )
    for (day, version, location, platform), entities in sorted(
        engagement_groups.items()
    ):
        rows = list(entities.values())
        snapshots = [latest[r["content_id"]] for r in rows if r["content_id"] in latest]
        counts = {
            k: sum(s[k] is not None for s in snapshots)
            for k in ("views", "likes", "comments", "shares")
        }
        totals = {k: total(s[k] for s in snapshots) for k in counts}
        totals_engagement = [
            s["engagement_total"]
            for s in snapshots
            if s["engagement_total"] is not None
        ]
        threshold = thresholds.get(platform)
        result["social_engagement_daily_v1"].append(
            finish(
                "social_engagement_daily_v1",
                {
                    "activity_date": day,
                    "admin_version": version,
                    "admin_unit_id": location,
                    "platform": platform,
                    "post_count": len(rows),
                    "total_views": totals["views"],
                    "total_likes": totals["likes"],
                    "total_comments_reported": totals["comments"],
                    "total_shares": totals["shares"],
                    "avg_likes_per_post": rate(totals["likes"], counts["likes"]),
                    "avg_comments_per_post": rate(
                        totals["comments"], counts["comments"]
                    ),
                    "median_engagement": (
                        float(median(totals_engagement)) if totals_engagement else None
                    ),
                    "high_engagement_post_count": (
                        sum(v >= threshold for v in totals_engagement)
                        if threshold is not None and totals_engagement
                        else None
                    ),
                    "posts_with_metrics_count": len(snapshots),
                    "posts_with_views": counts["views"],
                    "posts_with_likes": counts["likes"],
                    "posts_with_comments": counts["comments"],
                    "posts_with_shares": counts["shares"],
                    "snapshot_as_of": (
                        max(s["observed_at"] for s in snapshots) if snapshots else None
                    ),
                    "metric_window": "LatestValidAtOrBeforeAsOf",
                    "high_engagement_threshold": threshold,
                },
                rows + snapshots + mentions_for(rows, mentions),
                generated_at,
                status="METRIC_DEFINITION_UNCONFIRMED_PARTIAL",
            )
        )
    for (day, location, version, platform, label), entities in sorted(
        intent_groups.items()
    ):
        rows = list(entities.values())
        ev = [e for r in rows for e in evidence[(r["content_id"], label)]]
        result["social_intent_daily_v2"].append(
            finish(
                "social_intent_daily_v2",
                {
                    "activity_date": day,
                    "admin_unit_id": location,
                    "admin_version": version,
                    "platform": platform,
                    "intent_type": label,
                    "post_count": sum("comment_id" not in r for r in rows),
                    "comment_count": sum(
                        "comment_id" in r and not r["is_reply"] for r in rows
                    ),
                    "reply_count": sum(
                        "comment_id" in r and r["is_reply"] for r in rows
                    ),
                    "evidence_count": len({e["evidence_id"] for e in ev}),
                    "resolved_content_count": (
                        len(rows)
                        if location not in ("UNRESOLVED", "NATIONAL")
                        else sum(
                            bool(mentions.get(r["content_id"]))
                            and any(
                                m["resolution_status"] == "RESOLVED"
                                for m in mentions[r["content_id"]]
                            )
                            for r in rows
                        )
                    ),
                    "high_confidence_count": None,
                    "unique_content_count": len(rows),
                },
                rows + ev + mentions_for(rows, mentions),
                generated_at,
                status="RULE_UNCALIBRATED_PARTIAL",
            )
        )
    for (day, platform, location, version), entities in sorted(comment_groups.items()):
        rows = list(entities.values())
        relevant = [r for r in rows if r["is_pickleball_related"] is True]
        sentiments = [r["sentiment"] for r in relevant if r["sentiment"] is not None]
        seeking = sum("SEEK_COURT" in r["intent_labels"] for r in relevant)
        result["social_comment_analytics_daily_v1"].append(
            finish(
                "social_comment_analytics_daily_v1",
                {
                    "activity_date": day,
                    "platform": platform,
                    "admin_unit_id": location,
                    "admin_version": version,
                    "collected_comment_count": sum(not r["is_reply"] for r in rows),
                    "collected_reply_count": sum(r["is_reply"] for r in rows),
                    "reported_comment_count": None,
                    "relevant_comment_count": len(relevant),
                    "positive_comment_count": (
                        sentiments.count("POSITIVE") if sentiments else None
                    ),
                    "negative_comment_count": (
                        sentiments.count("NEGATIVE") if sentiments else None
                    ),
                    "neutral_comment_count": (
                        sentiments.count("NEUTRAL") if sentiments else None
                    ),
                    "sentiment_classified_count": len(sentiments),
                    "court_seeking_comment_count": seeking,
                    "booking_intent_comment_count": sum(
                        "BOOKING_INTENT" in r["intent_labels"] for r in relevant
                    ),
                    "price_inquiry_comment_count": sum(
                        "PRICE_INQUIRY" in r["intent_labels"] for r in relevant
                    ),
                    "capacity_complaint_count": sum(
                        "CAPACITY_COMPLAINT" in r["intent_labels"] for r in relevant
                    ),
                    "comments_with_resolved_geo": sum(
                        any(
                            m["resolution_status"] == "RESOLVED"
                            for m in mentions.get(r["content_id"], [])
                        )
                        for r in rows
                    ),
                    "court_seeking_comment_share": rate(seeking, len(relevant)),
                    "source_collection_status": "COLLECTED_SAMPLE_REPORTED_GEOGRAPHY_UNALLOCATABLE",
                },
                rows + mentions_for(rows, mentions),
                generated_at,
            )
        )
    return result, mentions, latest


def mentions_for(rows, mentions):
    return [m for r in rows for m in mentions.get(r["content_id"], [])]


def coverage(runs, silver, *, generated_at, geography):
    groups = defaultdict(list)
    resolved_ids = {
        key
        for r in silver["silver_social_geo_mentions"]
        if r["resolution_status"] == "RESOLVED"
        for key in r["lineage_ids"]
    }
    resolved_ids |= {
        key
        for r in silver["silver_court_venues"]
        if r["old_province_id"] or r["new_province_id"]
        for key in r["lineage_ids"]
    }
    source_days = set()
    for run in runs:
        day = vn_date(run["collected_at"])
        source = run["source_platform"]
        source_days.add((source, day))
        raw = json.loads(run["raw_payload"])
        query_rows = (
            json.loads(run["query_metadata"]) if run.get("query_metadata") else []
        )
        query_rows = query_rows if isinstance(query_rows, list) else []
        national = {
            "run": run,
            "queries": query_rows,
            "planned": raw.get("planned_queries"),
            "returned": run["records_returned"],
            "accepted": run["records_accepted"],
            "resolved": len(
                resolved_ids.intersection(run.get("accepted_observation_ids", []))
            ),
        }
        # Accepted IDs may be injected by the pipeline from the actual Bronze
        # observations for this source/run; absent IDs mean unknown, not zero.
        if "accepted_observation_ids" not in run:
            national["resolved"] = None
        groups[("NATIONAL", "NATIONAL", source, day)].append(national)
        targeted = defaultdict(list)
        for query in query_rows:
            if not isinstance(query, dict):
                continue
            hints = query.get("geo_provenance", [])
            targets = {
                (geography.by_id[h["geo_id"]]["admin_version"], h["geo_id"])
                for h in hints
                if h.get("geo_id") in geography.by_id
            }
            if not targets and query.get("target_admin_unit_id") in geography.by_id:
                target = query["target_admin_unit_id"]
                targets = {(geography.by_id[target]["admin_version"], target)}
            if not targets and query.get("geo_name"):
                resolved = geography.resolve(query["geo_name"], run["collected_at"])
                targets = {
                    (geography.by_id[i]["admin_version"], i)
                    for i in resolved["modes"]["AS_WRITTEN"]
                }
            for target in targets:
                targeted[target].append(query)
        for (version, location), queries in targeted.items():
            # Query-level counters can include duplicates from overlapping
            # requests. Retain only documented counters, with their sample scope.
            groups[(version, location, source, day)].append(
                {
                    "run": run,
                    "queries": queries,
                    "planned": None,
                    "returned": total(q.get("raw_count") for q in queries),
                    "accepted": total(q.get("accepted_count") for q in queries),
                    "resolved": None,
                }
            )
    # MISSING means no query evidence for that source/day, not absent market.
    for source, day in source_days:
        for unit in geography.units:
            if unit["admin_level"] == "PROVINCE":
                groups.setdefault(
                    (unit["admin_version"], unit["admin_unit_id"], source, day), []
                )
    result = []
    for (version, location, source, day), entries in sorted(groups.items()):
        qs = [q for e in entries for q in e["queries"] if isinstance(q, dict)]
        # A query without a recorded status is neither success nor failure.
        successes = [
            q
            for q in qs
            if q.get("status") in ("ok", "success") and not q.get("blocked")
        ]
        failures = [
            q
            for q in qs
            if q.get("status") in ("error", "failed", "partial") or q.get("blocked")
        ]
        planned = (
            total(e["planned"] for e in entries)
            if entries and all(e["planned"] is not None for e in entries)
            else None
        )
        accepted = total(e["accepted"] for e in entries)
        resolved = (
            total(e["resolved"] for e in entries)
            if entries and all(e["resolved"] is not None for e in entries)
            else None
        )
        status = (
            "MISSING"
            if not entries
            else (
                "FAILED"
                if all(e["run"]["execution_status"] == "failed" for e in entries)
                else "PARTIAL" if failures or not qs or planned is None else "COMPLETE"
            )
        )
        result.append(
            finish(
                "national_location_coverage_v1",
                {
                    "admin_unit_id": location,
                    "admin_version": version,
                    "source_platform": source,
                    "period_start": day,
                    "period_end": day,
                    "planned_queries": planned,
                    "successful_queries": len(successes) if qs else None,
                    "failed_queries": len(failures) if qs else None,
                    "records_returned": total(e["returned"] for e in entries),
                    "records_accepted": accepted,
                    "resolved_records": resolved,
                    "last_success_at": max(
                        (
                            e["run"]["ended_at"] or e["run"]["collected_at"]
                            for e in entries
                            if e["run"]["execution_status"] == "success"
                        ),
                        default=None,
                    ),
                    "collection_status": status,
                    "query_success_rate": rate(len(successes), planned) if qs else None,
                    "geo_resolution_rate": rate(resolved, accepted),
                    "query_evidence_status": (
                        "QUERY_OBSERVATIONS_PLAN_UNKNOWN"
                        if qs and planned is None
                        else "QUERY_PLAN_RECORDED" if qs else "NO_QUERY_EVIDENCE"
                    ),
                    "collection_run_ids": sorted(
                        {e["run"]["collection_run_id"] for e in entries}
                    ),
                },
                [e["run"] for e in entries],
                generated_at,
                status="PIPELINE_COVERAGE_NOT_MARKET_COVERAGE",
            )
        )
    return result


def research(silver, gold, mentions, latest, *, generated_at):
    admin_versions = {
        r["admin_unit_id"]: r["admin_version"] for r in silver["dim_admin_unit"]
    }
    result = {
        name: []
        for name in (
            "venue_community_feedback_weekly_v1",
            "location_term_usage_monthly_v1",
            "venue_discovery_trends_v1",
            "location_market_research_monthly_v1",
        )
    }
    contents = silver["silver_social_posts"] + silver["silver_social_comments"]
    content_lookup = {r["content_id"]: r for r in contents}
    feedback, terms, discoveries, monthly = (defaultdict(list) for _ in range(4))
    for content in contents:
        if not content["published_date_vn"] or content["is_duplicate"]:
            continue
        day = content["published_date_vn"]
        start = date.fromisoformat(day)
        week = (start - timedelta(days=start.weekday())).isoformat()
        for venue in content["mentioned_venue_ids"]:
            feedback[(venue, week)].append(content)
        for version, location in dimensions(content, mentions):
            monthly[(month(day), location, version)].append(content)
    for (venue, week), rows in sorted(feedback.items()):
        classified = [r for r in rows if "VENUE_FEEDBACK" in r["intent_labels"]]
        sentiments = [r["sentiment"] for r in classified if r["sentiment"] is not None]
        result["venue_community_feedback_weekly_v1"].append(
            finish(
                "venue_community_feedback_weekly_v1",
                {
                    "venue_id": venue,
                    "week_start": week,
                    "mentioning_post_count": len(
                        {r["content_id"] for r in rows if "comment_id" not in r}
                    ),
                    "mentioning_comment_count": len(
                        {r["content_id"] for r in rows if "comment_id" in r}
                    ),
                    "positive_feedback_count": (
                        sentiments.count("POSITIVE") if sentiments else None
                    ),
                    "negative_feedback_count": (
                        sentiments.count("NEGATIVE") if sentiments else None
                    ),
                    "price_complaint_count": None,
                    "facility_complaint_count": (
                        sum(
                            "FACILITIES" in r["topic_labels"]
                            and r["sentiment"] == "NEGATIVE"
                            for r in classified
                        )
                        if classified
                        else None
                    ),
                    "booking_difficulty_count": sum(
                        "CAPACITY_COMPLAINT" in r["intent_labels"] for r in rows
                    ),
                    "service_complaint_count": None,
                    "source_count": len({r["platform"] for r in rows}),
                    "classified_evidence_count": len(classified),
                    "sentiment_coverage_rate": rate(len(sentiments), len(rows)),
                },
                rows,
                generated_at,
                status="RULE_SAMPLE_NOT_OBJECTIVE_VENUE_SCORE",
            )
        )
    for mention in silver["silver_social_geo_mentions"]:
        content = content_lookup.get(mention["content_id"])
        if not content or not content["published_date_vn"] or content["is_duplicate"]:
            continue
        mention["raw_admin_unit_ids"]
        modes = json.loads(mention["interpretation_json"])
        written = modes["AS_WRITTEN"]
        term_version = (
            "AMBIGUOUS"
            if len(written) != 1
            else admin_versions.get(written[0], "UNKNOWN")
        )
        # Harmonized canonical location used once, so raw aliases do not double
        # count in a pre+post concatenation. Unknown term geography is explicit.
        location = (
            mention["new_province_id"] or mention["old_province_id"] or "UNRESOLVED"
        )
        terms[
            (
                month(content["published_date_vn"]),
                mention["location_text_raw"].casefold(),
                location,
                term_version,
                content["platform"],
            )
        ].append((mention, content))
    denominators = defaultdict(int)
    for key, rows in terms.items():
        denominators[(key[0], key[2], key[4])] += len(rows)
    for (mo, term, location, version, platform), rows in sorted(terms.items()):
        result["location_term_usage_monthly_v1"].append(
            finish(
                "location_term_usage_monthly_v1",
                {
                    "month": mo,
                    "location_term_raw_normalized": term,
                    "canonical_location_id": location,
                    "term_admin_version": version,
                    "platform": platform,
                    "post_mention_count": sum("comment_id" not in c for _, c in rows),
                    "comment_mention_count": sum("comment_id" in c for _, c in rows),
                    "unique_content_count": len({c["content_id"] for _, c in rows}),
                    "term_usage_share": rate(
                        len(rows), denominators[(mo, location, platform)]
                    ),
                    "resolution_coverage": rate(
                        sum(m["resolution_status"] == "RESOLVED" for m, _ in rows),
                        len(rows),
                    ),
                    "mention_count": len(rows),
                },
                [r for pair in rows for r in pair],
                generated_at,
            )
        )
    for venue in silver["silver_court_venues"]:
        for version, location in assignments([venue]) + [("NATIONAL", "NATIONAL")]:
            discoveries[
                (location, version, month(vn_date(venue["first_seen_at"])))
            ].append(venue)
    for (location, version, mo), rows in sorted(discoveries.items()):
        eligible = [
            r for r in rows if r["verification_status"] in ("VERIFIED", "PROBABLE")
        ]
        result["venue_discovery_trends_v1"].append(
            finish(
                "venue_discovery_trends_v1",
                {
                    "month": mo,
                    "admin_unit_id": location,
                    "admin_version": version,
                    "newly_discovered_venue_count": len(
                        {r["venue_id"] for r in eligible}
                    ),
                    "verified_discovery_count": sum(
                        r["verification_status"] == "VERIFIED" for r in rows
                    ),
                    "probable_discovery_count": sum(
                        r["verification_status"] == "PROBABLE" for r in rows
                    ),
                    "unverified_candidate_count": sum(
                        r["verification_status"] == "UNVERIFIED" for r in rows
                    ),
                    "first_seen_min": min(r["first_seen_at"] for r in rows),
                    "first_seen_max": max(r["first_seen_at"] for r in rows),
                    "discovery_basis": "FIRST_OBSERVED_IN_LAKE_NOT_OPENING_DATE",
                },
                rows,
                generated_at,
            )
        )
    supply_rows = gold["court_supply_by_admin_version_v2"]
    discovery_lookup = {
        (r["month"], r["admin_unit_id"], r["admin_version"]): r
        for r in result["venue_discovery_trends_v1"]
    }
    for key in discovery_lookup:
        monthly.setdefault(key, [])
    for (mo, location, version), source_rows in sorted(monthly.items()):
        rows = list({r["content_id"]: r for r in source_rows}.values())
        supply_options = [
            r
            for r in supply_rows
            if r["admin_unit_id"] == location
            and r["admin_version"] == version
            and r["snapshot_date"][:7] <= mo[:7]
        ]
        # Do not project today's supply backward into a historical social month.
        selected = max(supply_options, key=lambda r: r["snapshot_date"], default=None)
        next_month = (
            date.fromisoformat(mo).replace(day=28) + timedelta(days=4)
        ).replace(day=1)
        month_end = (next_month - timedelta(days=1)).isoformat()
        selected = (
            selected if selected and selected["snapshot_date"] <= month_end else None
        )
        posts = [
            r
            for r in rows
            if "comment_id" not in r and r["is_pickleball_related"] is True
        ]
        comments = [
            r for r in rows if "comment_id" in r and r["is_pickleball_related"] is True
        ]
        snapshots = [
            latest[r["content_id"]] for r in posts if r["content_id"] in latest
        ]
        discovery = discovery_lookup.get((mo, location, version))
        result["location_market_research_monthly_v1"].append(
            finish(
                "location_market_research_monthly_v1",
                {
                    "month": mo,
                    "admin_unit_id": location,
                    "admin_version": version,
                    "venue_count": selected["venue_count"] if selected else None,
                    "known_court_count": (
                        selected["known_court_count"] if selected else None
                    ),
                    "court_count_coverage_rate": (
                        selected["court_count_coverage_rate"] if selected else None
                    ),
                    "relevant_post_count": len(posts),
                    "relevant_comment_count": len(comments),
                    "total_views": total(s["views"] for s in snapshots),
                    "total_likes": total(s["likes"] for s in snapshots),
                    "total_shares": total(s["shares"] for s in snapshots),
                    "court_seeking_post_count": sum(
                        "SEEK_COURT" in r["intent_labels"] for r in posts
                    ),
                    "court_seeking_comment_count": sum(
                        "SEEK_COURT" in r["intent_labels"] for r in comments
                    ),
                    "booking_intent_content_count": sum(
                        "BOOKING_INTENT" in r["intent_labels"] for r in posts + comments
                    ),
                    "capacity_complaint_count": sum(
                        "CAPACITY_COMPLAINT" in r["intent_labels"]
                        for r in posts + comments
                    ),
                    "newly_discovered_venue_count": (
                        discovery["newly_discovered_venue_count"] if discovery else None
                    ),
                    "source_count": len(
                        {r["platform"] for r in rows}
                        | set(selected["source_platforms"] if selected else [])
                        | set(discovery["source_platforms"] if discovery else [])
                    ),
                    "social_coverage_status": (
                        "OBSERVED_SAMPLE_PARTIAL" if rows else "NO_CONTENT_OBSERVED"
                    ),
                    "supply_coverage_status": (
                        "OBSERVED_AS_OF_SNAPSHOT"
                        if selected
                        else "HISTORICAL_SUPPLY_SNAPSHOT_MISSING"
                    ),
                    "supply_as_of": selected["snapshot_date"] if selected else None,
                    "classification_coverage_rate": rate(
                        sum(r["is_pickleball_related"] is not None for r in rows),
                        len(rows),
                    ),
                },
                rows
                + snapshots
                + ([selected] if selected else [])
                + ([discovery] if discovery else []),
                generated_at,
            )
        )
    return result


def build_gold(
    silver, runs, geography, *, as_of, generated_at=None, historical_supply=()
):
    generated_at = timestamp(generated_at or as_of)
    result = {name: [] for name in GOLD}
    result["court_supply_by_admin_version_v2"] = supply(
        silver["silver_court_venues"],
        silver["silver_venue_candidates"],
        snapshot_date=vn_date(as_of),
        generated_at=generated_at,
    )
    daily, mentions, latest = social_daily(
        silver, generated_at=generated_at, as_of=as_of
    )
    result.update(daily)
    result["national_location_coverage_v1"] = coverage(
        runs, silver, generated_at=generated_at, geography=geography
    )
    research_gold = {
        **result,
        "court_supply_by_admin_version_v2": list(historical_supply)
        + result["court_supply_by_admin_version_v2"],
    }
    result.update(
        research(silver, research_gold, mentions, latest, generated_at=generated_at)
    )
    return result
