"""Read an immutable S3 Silver snapshot into an atomic, ignored website cache.

No S3 writes. Delta transaction logs determine active files; latest collected
observation wins per source record. Credentials stay with the AWS CLI profile.
"""
import argparse
import json
import os
import re
import subprocess
import tempfile
import unicodedata
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[2]
DATASETS = ("courts", "social_posts", "social_comments")

def unique_latest(rows):
    latest = {}
    for row in sorted(rows, key=lambda r: (str(r.get("collected_at") or ""), str(r.get("lakehouse_ingested_at") or ""), str(r.get("run_id") or ""))):
        key = (row.get("source"), row.get("source_record_id") or row.get("record_id"))
        if key[1]:
            latest[key] = row
    return list(latest.values())

def collection_history(tables):
    """Count active Silver observations by collection day in ICT, before deduplication."""
    days, skipped = {}, 0
    sources = {"courts": {"pbplus": "pbplus"}, "social_posts": {"threads": "threads", "youtube": "youtube"},
               "social_comments": {"youtube_comment": "youtube"}}
    for table, mapping in sources.items():
        for row in tables.get(table, []):
            source = mapping.get(row.get("source"))
            if source is None:
                continue
            try:
                value = datetime.fromisoformat(str(row.get("collected_at") or "").replace("Z", "+00:00"))
                if value.tzinfo is None:
                    raise ValueError("Timestamp has no timezone")
                day = value.astimezone(timezone(timedelta(hours=7))).date().isoformat()
            except (ValueError, TypeError):
                skipped += 1
                continue
            counts = days.setdefault(day, {"threads": 0, "youtube": 0, "pbplus": 0})
            counts[source] += 1
    return {"timezone": "Asia/Ho_Chi_Minh", "basis": "silver_observations", "skippedUndated": skipped,
            "days": [{"date": day, "counts": counts} for day, counts in sorted(days.items())]}

def engagement_history(rows):
    groups = {}
    for row in rows:
        source = row.get("source")
        if source not in ("threads", "youtube"):
            continue
        try:
            stamp = datetime.fromisoformat(str(row.get("collected_at") or "").replace("Z", "+00:00"))
            if stamp.tzinfo is None:
                continue
            day = stamp.astimezone(timezone(timedelta(hours=7))).date().isoformat()
        except (ValueError, TypeError):
            continue
        groups.setdefault((source, day), []).append(row)
    result = {"threads": [], "youtube": []}
    for (source, day), observations in sorted(groups.items()):
        items = unique_latest(observations)
        point = {"date": day, "items": len(items)}
        for key, column in [("likes", "like_count"), ("replies", "reply_count"), ("reposts", "repost_count"), ("quotes", "quote_count"), ("views", "view_count")]:
            values = [row[column] for row in items if isinstance(row.get(column), (int, float)) and not isinstance(row[column], bool) and row[column] >= 0]
            point[key] = sum(values) if values else None
            point[key + "Reported"] = len(values)
        result[source].append(point)
    return result

def clean_address(value):
    text = unicodedata.normalize("NFKD", value or "").casefold().replace("đ", "d")
    return re.sub(r"[^a-z0-9]+", " ", "".join(c for c in text if not unicodedata.combining(c))).strip()

def court_posts(rows, geocoded):
    refs = {r.get("id"): r for r in geocoded}
    result, enriched = [], 0
    for row in unique_latest(rows):
        if row.get("source") != "pbplus":
            continue
        record = {"id": row.get("source_record_id") or row["record_id"], "url": row.get("source_url"),
                  "collectedAt": row.get("collected_at"), "court_name": row.get("court_name"), "services": row.get("services") or [], "opening_hours": row.get("opening_hours") or [], "number_of_courts": row.get("number_of_courts"),
                  "address": row.get("address"), "latitude": row.get("latitude"), "longitude": row.get("longitude")}
        ref = refs.get(record["id"], {})
        if record["latitude"] is None and record["longitude"] is None and clean_address(record["address"]) and clean_address(record["address"]) == clean_address(ref.get("address")) and ref.get("latitude") is not None and ref.get("longitude") is not None:
            record.update(latitude=ref["latitude"], longitude=ref["longitude"], coordinate_source="local_geocoding")
            enriched += 1
        result.append(record)
    return result, enriched

def social_records(rows, sources):
    records = []
    for row in unique_latest(rows):
        if row.get("source") not in sources:
            continue
        url = row.get("source_url") or ""
        parsed = urlparse(url)
        allowed = ("threads.net", "threads.com") if row.get("source") == "threads" else ("youtube.com", "youtu.be")
        if parsed.scheme not in ("http", "https") or not any(parsed.hostname == host or (parsed.hostname or "").endswith("." + host) for host in allowed):
            url = None
        records.append({"id": row.get("record_id"), "source": row.get("source"), "url": url,
                        "text": str(row.get("text") or "")[:20000], "publishedAt": row.get("published_at"),
                        "collectedAt": row.get("collected_at"), "likes": row.get("like_count"),
                        "views": row.get("view_count"), "replies": row.get("reply_count"),
                        "reposts": row.get("repost_count"), "quotes": row.get("quote_count"),
                        "parentRecordId": row.get("parent_record_id"), "hasMedia": row.get("has_media"), "videoDuration": row.get("video_duration"),
                        "tags": [str(t)[:150] for t in (row.get("tags") or []) if isinstance(t, str)][:100]})
    return sorted(records, key=lambda r: str(r["publishedAt"] or r["collectedAt"] or ""), reverse=True)

def snapshot_changes(previous, current):
    if not previous:
        return None
    def datasets(snapshot):
        return {"courts": snapshot["posts"], "threads": snapshot["social"]["threads"],
                "youtube": snapshot["social"]["youtube"], "comments": snapshot["social"]["youtube_comments"]}
    def content(row):
        # Re-observing unchanged content is not a content update.
        return {k: v for k, v in row.items() if k not in ("collectedAt", "collected_at")}
    old, new = datasets(previous), datasets(current)
    sources = []
    for name, rows in new.items():
        before = {r["id"]: content(r) for r in old[name]}
        after = {r["id"]: content(r) for r in rows}
        sources.append({"id": name, "added": len(after.keys() - before.keys()),
                        "removed": len(before.keys() - after.keys()),
                        "updated": sum(before[k] != after[k] for k in before.keys() & after.keys())})
    return {"comparedAt": previous["synced_at"], "sources": sources}


def run(args):
    from deltalake import DeltaTable
    from resolve_source import resolve
    base, generation = resolve(args)
    parsed = urlparse(base)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("Use an immutable s3://.../bootstrap/.../lakehouse snapshot. Managed generations need a pinned manifest reader.")
    if ".." in parsed.path.split("/"):
        raise ValueError("Invalid snapshot path")
    # A new directory prevents a failed sync from mixing old and new exports.
    cache_root = ROOT / ".local" / "website-s3-cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="read-", dir=cache_root) as temporary:
        tables, versions = {}, {}
        for name in DATASETS:
            target = Path(temporary) / name
            subprocess.run([args.aws, "s3", "sync", (generation[0][name] if generation else f"{base}/silver/{name}") + "/", str(target), "--profile", args.profile, "--region", args.region, "--only-show-errors"], check=True, timeout=300)
            delta = DeltaTable(str(target), version=generation[1][name] if generation else None)
            if delta.count() > 100000:
                raise ValueError(f"{name} exceeds the website snapshot row limit")
            versions[name] = delta.version()
            tables[name] = delta.to_pyarrow_table().to_pylist()
        geo_file = ROOT / "apps/collector/.local/pbplus/pbplus_osm_geocoded.json"
        geocoded = json.loads(geo_file.read_text()).get("posts", []) if geo_file.exists() else []
        courts, enriched = court_posts(tables["courts"], geocoded)
        posts = social_records(tables["social_posts"], {"threads", "youtube"})
        comments = social_records(tables["social_comments"], {"youtube_comment"})
        collected = max((str(r.get("collected_at") or "") for r in tables["courts"]), default="") or None
        now = datetime.now(timezone.utc).isoformat()
        payload = {"mode": "s3_snapshot", "source": "pbplus", "collected_at": collected, "synced_at": now,
                   "s3": {"uri": base, "region": args.region, "versions": versions, "localCoordinateMatches": enriched},
                   "engagement": engagement_history(tables["social_posts"]), "history": collection_history(tables), "posts": courts, "social": {"threads": [r for r in posts if r["source"] == "threads"],
                                               "youtube": [r for r in posts if r["source"] == "youtube"], "youtube_comments": comments}}
        output = ROOT / ".local" / "website-data" / "snapshot.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        previous = None
        if output.exists():
            try:
                previous = json.loads(output.read_text())
                payload["changes"] = snapshot_changes(previous, payload)
            except (ValueError, KeyError, TypeError):
                payload["changes"] = None
        if previous is not None:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent, delete=False) as backup:
                json.dump(previous, backup, ensure_ascii=False, default=str, allow_nan=False)
                backup.flush()
                os.fsync(backup.fileno())
                backup_name = backup.name
            os.replace(backup_name, output.parent / "previous-snapshot.json")
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent, delete=False) as stream:
            json.dump(payload, stream, ensure_ascii=False, default=str, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
            staged = stream.name
        os.replace(staged, output)
        print(json.dumps({"courts": len(courts), "local_coordinate_matches": enriched, "threads_posts": len(payload["social"]["threads"]), "youtube_videos": len(payload["social"]["youtube"]), "youtube_comments": len(comments), "versions": versions, "output": str(output)}, indent=2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="s3://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse")
    parser.add_argument("--profile", default="tracker")
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--aws", default="aws")
    run(parser.parse_args())
