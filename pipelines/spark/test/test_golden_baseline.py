"""Frozen legacy Gold capture using synthetic inputs and temporary Delta only.

Capture once with --capture PATH; normal unittest runs compare against that
reviewed fixture without rewriting it. No broker, warehouse or live API is used.
"""

import hashlib
import json
import os
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [
    str(ROOT / "pipelines/spark/src"),
    str(ROOT / "apps/collector/src"),
    str(ROOT / "apps/collector/test"),
]

from delta.tables import DeltaTable
from dsp_collector.normalizers.court import normalize_court
from dsp_collector.normalizers.social_post import normalize_social_post
from dsp_collector.normalizers.youtube import normalize_youtube
from dsp_collector.publishers.kafka import build_social_post_event
from dsp_spark.io.delta import read_delta
from dsp_spark.jobs.batch import build_court_supply_by_location as court_job
from dsp_spark.jobs.batch import build_location_market_signal_snapshot as market_job
from dsp_spark.jobs.batch import build_social_activity_daily as social_job
from dsp_spark.jobs.batch.ingest_pbplus import ingest_pbplus
from dsp_spark.jobs.streaming.social_kafka_to_delta import persist_social_batch
from dsp_spark.processing.clean.contracts import contract_documents, validate
from dsp_spark.runtime.config import LakehousePaths, MarketSignalSettings, SparkSettings
from dsp_spark.runtime.spark import create_spark
from pyspark.sql import types as T
from test_youtube import comment, video

AT = "2026-10-05T12:00:00.000000Z"
END_DATE = "2026-10-05"
EXPECTED = ROOT / "tests/fixtures/pipeline/gold-baseline.v1.json"
TABLES = (
    "social_activity_daily",
    "social_source_activity_daily",
    "court_supply_by_location",
    "location_market_signal_snapshot",
)
KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("key", T.BinaryType()),
        T.StructField("value", T.BinaryType()),
        T.StructField("topic", T.StringType()),
        T.StructField("partition", T.IntegerType()),
        T.StructField("offset", T.LongType()),
        T.StructField("timestamp", T.TimestampType()),
    ]
)


class FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        fixed = cls.fromisoformat(AT)
        return fixed.astimezone(tz) if tz is not None else fixed.replace(tzinfo=None)


def canonical(value):
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def pins():
    files = (
        "tests/fixtures/pipeline/threads.json",
        "tests/fixtures/pipeline/pbplus.json",
        "apps/collector/test/test_youtube.py",
        "configs/reference/vietnam_locations.v1.json",
    )
    return {
        "generation_time": AT,
        "normalization_time": AT,
        "event_emission_time": AT,
        "analysis_end_date": END_DATE,
        "window_days": 30,
        "session_timezone": "UTC",
        "business_fingerprint_excluded_fields": ["gold_generated_at"],
        "hash_encoding": "UTF-8 sorted-key compact JSON, rows sorted by same encoding",
        "input_sha256": {
            name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
            for name in files
        },
        "contract_sha256": {
            name: fingerprint(document)
            for name, document in json.loads(contract_documents()).items()
        },
    }


def input_records():
    threads = json.loads((ROOT / "tests/fixtures/pipeline/threads.json").read_text())
    pbplus = json.loads((ROOT / "tests/fixtures/pipeline/pbplus.json").read_text())
    social = [
        normalize_social_post(
            raw,
            run_id=threads["run_id"],
            collected_at=threads["collected_at"],
            normalized_at=AT,
        )
        for raw in threads["posts"]
    ]
    social += [
        normalize_youtube(video(), run_id="golden-youtube-v1", collected_at=AT),
        normalize_youtube(
            comment()["snippet"]["topLevelComment"],
            run_id="golden-youtube-v1",
            collected_at=AT,
            video_id="video_00001",
        ),
    ]
    courts = [
        normalize_court(
            raw,
            run_id=pbplus["run_id"],
            collected_at=pbplus["collected_at"],
            normalized_at=AT,
        )
        for raw in pbplus["posts"]
    ]
    return social, {**pbplus, "records": courts, "count": len(courts)}


def verify_legacy_semantics(result):
    tables = result["tables"]
    market = tables["location_market_signal_snapshot"]["rows"]
    fields = json.loads(contract_documents())["location_market_signal_snapshot"][
        "properties"
    ]
    assert len(fields) == 23
    assert MarketSignalSettings().window_days == 30
    assert all(set(row) == set(fields) for row in market)
    assert all(
        (row["window_start_date"], row["window_end_date"], row["window_days"])
        == ("2026-09-06", END_DATE, 30)
        for row in market
    )
    by_location = {row["location_id"]: row for row in market}
    assert set(by_location) == {"vn-hanoi", "vn-da-nang", "vn-hue"}
    assert by_location["vn-hanoi"]["known_court_count"] is None
    assert by_location["vn-hue"]["known_court_count"] == 0
    assert by_location["vn-da-nang"]["venue_count"] is None
    assert by_location["vn-hanoi"]["social_like_count"] == 3  # Threads only.
    assert by_location["vn-hanoi"]["social_post_count"] == 1
    daily = tables["social_activity_daily"]["rows"]
    assert any(row["location_id"] is None for row in daily)
    assert {row["activity_date"] for row in daily} == {"2026-10-05"}  # UTC.
    native = tables["social_source_activity_daily"]["rows"]
    assert {row["source"] for row in native} == {
        "threads",
        "youtube",
        "youtube_comment",
    }
    assert all(
        row["youtube_views"] == 123 for row in native if row["source"] == "youtube"
    )
    assert all(
        row["location_id"] == "vn-hanoi" and row["youtube_views"] is None
        for row in native
        if row["source"] == "youtube_comment"
    )


def capture(spark):
    records, courts = input_records()
    events = [
        build_social_post_event(row, emitted_at=datetime.fromisoformat(AT))
        for row in records
    ]
    rows = [
        (
            event["record_id"].encode(),
            canonical(event).encode("utf-8"),
            "golden-fixture-no-broker",
            0,
            index,
            datetime.fromisoformat(AT),
        )
        for index, event in enumerate(events + [events[0]])
    ]
    frame = spark.createDataFrame(rows, KAFKA_SCHEMA)
    documents = json.loads(contract_documents())
    results, metadata = [], []
    with tempfile.TemporaryDirectory(prefix="dsp-m0-golden-") as directory:
        root = Path(directory)
        settings = SparkSettings(LakehousePaths((root / "lakehouse").as_uri()))
        artifact = root / "court-input.json"
        artifact.write_text(canonical(courts), encoding="utf-8")
        for run in (1, 2):
            persist_social_batch(frame, run, settings=settings)
            ingest_pbplus(spark, artifact, settings)
            assert read_delta(spark, settings.paths.social_posts).count() == len(
                records
            )
            assert read_delta(spark, settings.paths.courts).count() == len(
                courts["records"]
            )
            with ExitStack() as clocks:
                for module in (social_job, court_job, market_job):
                    clocks.enter_context(
                        patch.object(module, "datetime", FrozenDateTime)
                    )
                social_job.build_social_activity_daily(spark, settings)
                court_job.build_court_supply_by_location(spark, settings)
                market_job.build_location_market_signal_snapshot(
                    spark,
                    settings,
                    market_settings=MarketSignalSettings(end_date=END_DATE),
                )
            tables, versions = {}, {}
            for name in TABLES:
                path = getattr(settings.paths, name)
                values = sorted(
                    (
                        row.asDict(recursive=True)
                        for row in read_delta(spark, path).collect()
                    ),
                    key=canonical,
                )
                for value in values:
                    validate(value, name)
                    assert value["gold_generated_at"] == AT
                business = [
                    {k: v for k, v in row.items() if k != "gold_generated_at"}
                    for row in values
                ]
                tables[name] = {
                    "rows": values,
                    "row_count": len(values),
                    "schema_fields": list(documents[name]["properties"]),
                    "business_sha256": fingerprint(sorted(business, key=canonical)),
                    "full_rows_sha256": fingerprint(values),
                }
                versions[name] = (
                    DeltaTable.forPath(spark, path).history(1).first().version
                )
            result = {"fixture_version": "1", "pins": pins(), "tables": tables}
            verify_legacy_semantics(result)
            results.append(result)
            metadata.append(
                {
                    "run": run,
                    "delta_versions": versions,
                    "temporary_root": str(root),
                    "captured_at": datetime.now(UTC).isoformat(),
                }
            )
    assert results[0] == results[1], "Repeated legacy builds changed pinned output"
    evidence = os.getenv("DSP_M0_GOLDEN_EVIDENCE")
    if evidence:
        with Path(evidence).open("x", encoding="utf-8") as output:
            json.dump(
                {"repeat_equal": True, "runs": metadata, "result": results[0]},
                output,
                ensure_ascii=False,
                indent=2,
                allow_nan=False,
            )
            output.write("\n")
    return results


class GoldenBaselineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-m0-runtime-only"))
        )
        try:
            cls.results = capture(cls.spark)
        except BaseException:
            cls.spark.stop()
            raise

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_repeated_delta_rebuild_and_business_fingerprints(self):
        self.assertEqual(self.results[0], self.results[1])

    def test_reviewed_golden_fixture_matches(self):
        self.assertEqual(
            json.loads(EXPECTED.read_text(encoding="utf-8")), self.results[0]
        )

    def test_legacy_contract_window_nulls_and_source_scope(self):
        verify_legacy_semantics(self.results[0])


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--capture":
        destination = Path(sys.argv[2])
        if destination.exists():
            raise FileExistsError("Capture never overwrites a reviewed golden fixture")
        spark = create_spark(
            SparkSettings(LakehousePaths("file:///tmp/dsp-m0-runtime-only"))
        )
        try:
            results = capture(spark)
        finally:
            spark.stop()
        with destination.open("x", encoding="utf-8") as output:
            json.dump(results[0], output, ensure_ascii=False, indent=2, allow_nan=False)
            output.write("\n")
        print(
            canonical(
                {
                    "repeat_equal": True,
                    "fixture": str(destination),
                    "business_sha256": {
                        k: v["business_sha256"] for k, v in results[0]["tables"].items()
                    },
                }
            )
        )
    else:
        unittest.main()
