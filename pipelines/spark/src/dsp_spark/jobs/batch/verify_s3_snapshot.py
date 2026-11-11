"""Read-only, pinned Delta snapshot reconciliation. Never writes cloud objects."""

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from ...runtime.config import LakehousePaths, SparkSettings
from ...runtime.spark import create_spark


def local_tables(root):
    result = {}
    for log in sorted(Path(root).rglob("_delta_log")):
        commits = sorted(log.glob("[0-9]" * 20 + ".json"))
        if not commits:
            raise ValueError(f"no JSON history: {log.parent.name}")
        versions = [int(p.stem) for p in commits]
        if versions != list(range(versions[-1] + 1)):
            raise ValueError("local history requires checkpoint-aware inspection")
        active, metadata = {}, None
        for commit in commits:
            for line in commit.read_text().splitlines():
                action = json.loads(line)
                if "add" in action:
                    active[action["add"]["path"]] = action["add"]
                if "remove" in action:
                    active.pop(action["remove"]["path"], None)
                metadata = action.get("metaData", metadata)
        stats = [json.loads(a.get("stats", "{}")) for a in active.values()]
        if any("numRecords" not in s for s in stats) or any(
            a.get("deletionVector") for a in active.values()
        ):
            raise ValueError("cannot infer snapshot counts from local statistics")
        name = log.parent.relative_to(root).as_posix()
        result[name] = {
            "version": versions[-1],
            "rows": sum(s["numRecords"] for s in stats),
            "schema": json.loads(metadata["schemaString"]),
            "path": str(log.parent.resolve()),
        }
    return result


def frame_digest(frame, limit=500_000):
    """Bounded exact multiset of canonical row SHA256s, preserving duplicates."""
    encoded = F.to_json(
        F.struct(*(F.col(n) for n in frame.columns)),
        {"ignoreNullFields": "false", "timeZone": "UTC"},
    )
    hashes = [
        row[0] for row in frame.select(F.sha2(encoded, 256)).limit(limit + 1).collect()
    ]
    if len(hashes) > limit:
        raise ValueError("verification row limit exceeded; use distributed comparator")
    digest = hashlib.sha256("\n".join(sorted(hashes)).encode()).hexdigest()
    return len(hashes), "sha256:" + digest


def verify_snapshot(spark, local_root, remote_root, *, content=True, only=None):
    catalog = local_tables(local_root)
    if only:
        catalog = {n: catalog[n] for n in only}
    report = {
        "started_at": datetime.now(UTC).isoformat(),
        "read_only": True,
        "local_root": str(Path(local_root).resolve()),
        "remote_root": remote_root,
        "tables": {},
    }
    for name, reference in catalog.items():
        path = remote_root.rstrip("/") + "/" + name
        row = {"s3_path": path, "local_reference": reference}
        try:
            version = int(DeltaTable.forPath(spark, path).history(1).first().version)
            remote = (
                spark.read.format("delta").option("versionAsOf", version).load(path)
            )
            local = (
                spark.read.format("delta")
                .option("versionAsOf", reference["version"])
                .load(reference["path"])
            )
            row.update(delta_version=version, schema=remote.schema.jsonValue())
            row["schema_match"] = remote.schema == local.schema
            if content:
                count, digest = frame_digest(remote)
                local_count, local_digest = frame_digest(local)
                row.update(
                    rows=count,
                    local_rows=local_count,
                    digest=digest,
                    local_digest=local_digest,
                    content_match=digest == local_digest,
                )
            else:
                row["rows"] = remote.count()
            row["count_match"] = row["rows"] == reference["rows"]
            row["version_match"] = version == reference["version"]
            row["status"] = (
                "PASS"
                if all(row[k] for k in ("schema_match", "count_match", "version_match"))
                and row.get("content_match", True)
                else "FAIL"
            )
        except Exception as exc:  # noqa: BLE001 — persist safe failure state for retry
            # Full SDK/process exceptions can contain sensitive transport data.
            row.update(status="FAIL", error_type=type(exc).__name__)
        report["tables"][name] = row
        print(
            json.dumps(
                {"table": name, "status": row["status"], "rows": row.get("rows")}
            ),
            flush=True,
        )
    report["status"] = (
        "PASS"
        if report["tables"]
        and all(r["status"] == "PASS" for r in report["tables"].values())
        else "FAIL"
    )
    report["finished_at"] = datetime.now(UTC).isoformat()
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-root", type=Path, required=True)
    parser.add_argument("--remote-root", required=True)
    parser.add_argument("--profile", default="dsp391m-spark")
    parser.add_argument("--region", default="ap-southeast-2")
    parser.add_argument("--only", action="append")
    parser.add_argument("--metadata-only", action="store_true")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    settings = SparkSettings(
        LakehousePaths(args.remote_root), s3_region=args.region, s3_profile=args.profile
    )
    spark = create_spark(settings)
    try:
        report = verify_snapshot(
            spark,
            args.local_root,
            args.remote_root,
            content=not args.metadata_only,
            only=args.only,
        )
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        if report["status"] != "PASS":
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
