"""Go-owned acquisition/ingestion handoff; Airflow-owned finite processing.

No source calls occur on processing or retry. All control artifacts are immutable.
"""

import argparse
import json
import os
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from dsp_collector.normalizers.observation import canonical_json, digest, parse_json
from dsp_collector.publishers.observation import TOPIC, publish
from dsp_collector.runtime.durable import ArtifactStore, sha256
from dsp_collector.runtime.source_policy import admit, policy_hash
from dsp_spark.processing.threads.contracts import PREVIEW, RESEARCH, SILVER, path
from dsp_spark.runtime.clickhouse import ClickHouseSettings
from dsp_spark.runtime.config import LakehousePaths, SparkSettings
from dsp_spark.runtime.spark import create_spark

from . import threads_manifest


def settings(config):
    return SparkSettings(
        paths=LakehousePaths(config["lakehouse_base_uri"]),
        kafka_bootstrap_servers=config["kafka_bootstrap_servers"],
    )


def target_config(config):
    spark = settings(config)
    warehouse = ClickHouseSettings(**config["clickhouse"])
    resources = [
        "delta:" + path(spark, n)
        for n in SILVER
        + RESEARCH
        + PREVIEW
        + ["admitted_threads_observations", "rejected_threads_events"]
    ]
    resources.append(
        "clickhouse:"
        + canonical_json([warehouse.host, warehouse.port, warehouse.database])
    )
    return {
        "base_uri": spark.paths.base,
        "resources": sorted(resources),
        "include_group": False,
    }


@contextmanager
def ownership(config, *, native=False):
    spark = settings(config)
    if spark.paths.base.startswith("file://"):
        from .analytical_refresh import analytical_ownership

        fields = {"base": spark.paths.base}
        if native:
            fields.update(
                record_observations=spark.paths.record_observations,
                observation_artifacts=spark.paths.observation_artifacts,
            )
            fields.update(
                {
                    n: spark.paths.base + "/bronze/" + n
                    for n in (
                        "bronze_social_posts",
                        "bronze_social_comments",
                        "bronze_engagement_snapshots",
                        "bronze_collection_runs",
                    )
                }
            )
        else:
            fields.update(
                {
                    n: path(spark, n)
                    for n in SILVER
                    + RESEARCH
                    + PREVIEW
                    + ["admitted_threads_observations", "rejected_threads_events"]
                }
            )
        cfg = SimpleNamespace(
            spark=SimpleNamespace(paths=SimpleNamespace(**fields)),
            warehouse=None if native else ClickHouseSettings(**config["clickhouse"]),
        )
        with analytical_ownership(cfg, include_group=False):
            yield
    else:
        # No expiring lease is stolen while an old Spark/warehouse writer can run.
        # A crashed cloud owner needs confirmed termination and explicit recovery.
        store = ArtifactStore(config["control_base_uri"])
        token = canonical_json(
            {
                "owner": uuid4().hex,
                "scope": "native" if native else "research",
                "targets": target_config(config),
            }
        ).encode()
        from urllib.parse import urlsplit

        uri = store.uri(
            "leases/"
            + digest([spark.paths.base, "native" if native else target_config(config)])
            + ".json"
        )
        p = urlsplit(uri)
        client = store._client()
        client.put_object(
            Bucket=p.netloc, Key=p.path.lstrip("/"), Body=token, IfNoneMatch="*"
        )
        try:
            yield
        finally:
            current = client.get_object(Bucket=p.netloc, Key=p.path.lstrip("/"))
            if current["Body"].read() != token:
                raise RuntimeError("Cloud ownership fence lost")
            client.delete_object(
                Bucket=p.netloc, Key=p.path.lstrip("/"), IfMatch=current["ETag"]
            )


def _spark(config):
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        if not spark.version.startswith("4.0."):
            raise RuntimeError("Managed Threads processing requires Spark 4.0.x")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        return spark
    return create_spark(settings(config), kafka=True)


def acquire(config, *, run_id, directory, progress=None, retry=False):
    from dsp_collector.sources.crawler.threads.incremental import collect_incremental
    from dsp_spark.jobs.batch.ingest_threads_observations import ingest, resolve_all

    fixture = config.get("mode") == "fixture"
    policy = config["source_policy"]
    origin = admit(policy, fixture=fixture)
    fetcher = None
    if fixture:
        # This module is shipped only with the source checkout, never live inference.
        from tests.threads_vertical.fixtures import FixtureFetcher

        fetcher = FixtureFetcher(config.get("fixture_revision", 0))
    directory = Path(directory)
    if retry and not (directory / "fetch-checkpoint.json").is_file():
        raise ValueError("Retry has no retained acquisition checkpoint")
    if retry:
        state = json.loads((directory / "fetch-checkpoint.json").read_text())
    else:
        state = collect_incremental(
            config["acquisition"],
            run_id=run_id,
            directory=directory,
            policy=policy,
            fixture=fixture,
            fetcher=fetcher,
            progress=progress,
        )
    raw = ArtifactStore(config["acquisition"]["raw_base_uri"])
    control = ArtifactStore(config["control_base_uri"])
    cfg = settings(config)
    with ownership(config, native=True):
        spark = _spark(config)
        try:
            events = []
            for reference in state["artifacts"]:
                envelope = parse_json(raw.read(reference["artifact_uri"]))
                outbox_key = (
                    "outbox/" + digest([run_id, reference["content_sha256"]]) + ".json"
                )
                prior = control.optional(control.uri(outbox_key))
                current = (
                    parse_json(prior)
                    if prior
                    else ingest(
                        spark,
                        store=raw,
                        reference=reference,
                        settings=cfg,
                        policy=policy,
                        fixture=fixture,
                    )
                )
                resolve_all(
                    spark,
                    current,
                    settings=cfg,
                    store=raw,
                    policy=policy,
                    fixture=fixture,
                )
                control.put(outbox_key, canonical_json(current).encode())
                events.extend(current)
            if not events:
                raise ValueError("No accepted native observations to hand off")
            collection, coverage = _collection_record(
                config, state, events, run_id, origin, fixture
            )
            from dsp_spark.jobs.batch.ingest_threads_observations import _insert_exact
            from dsp_spark.processing.clean.contracts import spark_schema

            _insert_exact(
                spark,
                [collection],
                cfg.paths.base + "/bronze/bronze_collection_runs",
                spark_schema("threads_collection_eda_v1"),
                "collection_run_id",
            )
            receipts = publish(events, bootstrap=cfg.kafka_bootstrap_servers)
        finally:
            if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
                spark.stop()
    if not events:
        raise ValueError("No accepted native observations to hand off")
    inputs = [
        {
            "kind": "artifact",
            "source_platform": "threads",
            "collection_run_id": run_id,
            "artifact_uri": r["artifact_uri"],
            "content_sha256": r["content_sha256"],
            "artifact_schema_version": "threads-fetch.v2",
            "record_count": None,
            "policy_reference": policy["reference"],
        }
        for r in state["artifacts"]
    ]
    kafka_refs = []
    for partition in sorted({r["partition"] for r in receipts.values()}):
        offsets = [
            r["offset"] for r in receipts.values() if r["partition"] == partition
        ]
        kafka_refs.append(
            {
                "kind": "kafka_ack",
                "source_platform": "threads",
                "collection_run_id": run_id,
                "broker_namespace": cfg.kafka_bootstrap_servers,
                "topic": TOPIC,
                "partition": partition,
                "first_offset": min(offsets),
                "last_offset": max(offsets),
                "acknowledged_count": len(offsets),
            }
        )
    acquisition = {
        "outcome": "partial",
        "accepted_records": len(events),
        "coverage": coverage,
        "runs": [
            {
                "source_platform": "threads",
                "collection_run_id": run_id,
                "outcome": "partial",
                "accepted_records": len(events),
                "coverage": coverage,
            }
        ],
    }
    created = max(e["emitted_at"] for e in events)
    spec = {
        "analysis_end_date": created[:10],
        "window_days": 30,
        "processing_contract_version": "threads-observation.v2",
        "code_version": "threads-vertical.v1",
        "configuration_hash": digest(config),
        "reference_version": config.get("admin_reference", {}).get("content_sha256"),
        "feature_version": "threads-own-text-rules-v1",
        "policy_version": policy_hash(policy),
        "tombstone_version": None,
    }
    handoff = threads_manifest.build(
        inputs=inputs,
        ingestion_refs=kafka_refs,
        acquisition=acquisition,
        spec=spec,
        targets=target_config(config),
        created_at=created,
        metadata={
            "event_ids": sorted(e["event_id"] for e in events),
            "collection": collection,
        },
    )
    reference = control.put(
        "manifests/" + handoff["manifest_id"].removeprefix("sha256:") + ".json",
        canonical_json(handoff).encode(),
    )
    control.put(
        "pending/" + handoff["manifest_id"].removeprefix("sha256:") + ".json",
        canonical_json(reference).encode(),
    )
    return {
        "status": "PARTIAL",
        "handoff": reference,
        "manifest_id": handoff["manifest_id"],
        "metrics": {
            "unique_posts": sum(e["record_type"] == "Post" for e in events),
            "observations": len(events),
            "replies": sum(e["record_type"] == "Comment" for e in events),
            "queries_processed": len(state["queries"]),
            "successful_queries": coverage["successful_queries"],
            "failed_queries": coverage["failed_queries"],
        },
    }


def _collection_record(config, state, events, run_id, origin, fixture):
    from dsp_collector.sources.crawler.threads.crawler import expanded_query_plan

    successful = sum(q["status"] == "OK" for q in state["queries"].values())
    failed = sum(q["status"] == "FAILED" for q in state["queries"].values())
    coverage = {
        "status": "PARTIALLY_COLLECTED",
        "planned_queries": len(
            expanded_query_plan(config["acquisition"].get("queries", ["pickleball"]))
        ),
        "successful_queries": successful,
        "failed_queries": failed,
        "records_returned": None,
        "pagination_complete": False,
    }
    collection = {
        "collection_run_id": run_id,
        "origin": origin,
        "collection_status": "PARTIAL",
        "historical_window_status": (
            "PUBLIC_RESULTS_FILTER_ONLY_NO_BACKFILL_GUARANTEE"
            if not fixture
            else "SYNTHETIC_FIXTURE"
        ),
        "unique_content_count": len(state["seen"]),
        "planned_queries": coverage["planned_queries"],
        "successful_queries": successful,
        "failed_queries": failed,
        "requests_used": state["requests"],
        "artifact_count": len(state["artifacts"]),
        "last_success_at": max(e["emitted_at"] for e in events),
        "query_terms": sorted(v["query"] for v in state["queries"].values()),
    }
    return collection, coverage


def process(config, *, manifest_uri):
    from dsp_spark.io.clickhouse import ClickHouseClient
    from dsp_spark.io.delta import read_delta
    from dsp_spark.io.research_clickhouse import publish as publish_gold
    from dsp_spark.jobs.streaming.threads_observations_to_delta import consume_batch
    from dsp_spark.processing.threads.gold import build as build_gold
    from dsp_spark.processing.threads.silver import build as build_silver
    from pyspark.sql import functions as F

    control = ArtifactStore(config["control_base_uri"])
    manifest = threads_manifest.validate(
        parse_json(control.read(manifest_uri)), targets=target_config(config)
    )
    if manifest["processing_spec"]["configuration_hash"] != digest(config):
        raise ValueError("Processing config changed since acquisition admission")
    fixture = config.get("mode") == "fixture"
    admit(config["source_policy"], fixture=fixture)
    attempt = "attempt:" + uuid4().hex

    def record(stage, outcome, error=None, generation=None):
        value = threads_manifest.ack(
            manifest,
            attempt=attempt,
            stage=stage,
            outcome=outcome,
            error=error,
            generation=generation,
        )
        control.put(
            "acks/"
            + manifest["manifest_id"].removeprefix("sha256:")
            + "/"
            + value["ack_id"].removeprefix("sha256:")
            + ".json",
            canonical_json(value).encode(),
        )

    stage = "kafka_admission"
    with ownership(config):
        spark = _spark(config)
        try:
            cfg = settings(config)
            raw = ArtifactStore(config["acquisition"]["raw_base_uri"])
            starting, ending = {TOPIC: {}}, {TOPIC: {}}
            for reference in manifest["ingestion_refs"]:
                partition = str(reference["partition"])
                starting[TOPIC][partition] = reference["first_offset"]
                ending[TOPIC][partition] = reference["last_offset"] + 1
            batch = (
                spark.read.format("kafka")
                .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
                .option("subscribe", TOPIC)
                .option("startingOffsets", canonical_json(starting))
                .option("endingOffsets", canonical_json(ending))
                .option("failOnDataLoss", "true")
                .load()
            )
            wanted = manifest["metadata"]["event_ids"]
            batch = batch.filter(
                F.get_json_object(F.col("value").cast("string"), "$.event_id").isin(
                    wanted
                )
            ).cache()
            found = {
                r[0]
                for r in batch.select(
                    F.get_json_object(F.col("value").cast("string"), "$.event_id")
                )
                .distinct()
                .collect()
            }
            if found != set(wanted):
                raise ValueError("Acknowledged Kafka handoff is incomplete")
            consume_batch(
                spark,
                batch,
                settings=cfg,
                store=raw,
                policy=config["source_policy"],
                fixture=fixture,
            )
            admitted = (
                read_delta(spark, path(cfg, "admitted_threads_observations"))
                .filter(F.col("observation_id").isin(wanted))
                .count()
            )
            if admitted != len(wanted):
                raise ValueError("Not all handed-off native references were admitted")
            record(stage, "SUCCESS")
            # Query coverage is retained at run grain, rather than inflated by replay.
            collection_path = cfg.paths.base + "/bronze/bronze_collection_runs"
            from dsp_spark.processing.threads.references import (
                load_aliases,
                load_annotations,
            )

            stage = "silver_features"
            silver = build_silver(
                spark,
                cfg,
                aliases=load_aliases(config, fixture=fixture),
                annotations=load_annotations(config, fixture=fixture),
            )
            record(stage, "SUCCESS")
            runs = (
                read_delta(spark, path(cfg, "admitted_threads_observations"))
                .select("collection_run_id")
                .distinct()
            )
            collections = [
                r.asDict(recursive=True)
                for r in read_delta(spark, collection_path)
                .join(runs, "collection_run_id")
                .collect()
            ]
            stage = "research_gold"
            gold = build_gold(spark, cfg, collections=collections)
            record(stage, "PARTIAL")
            stage = "research_publication"
            with ClickHouseClient(ClickHouseSettings(**config["clickhouse"])) as client:
                generation = publish_gold(spark, cfg, client, gold)
            record(stage, "PARTIAL", generation=generation["generation_id"])
            control.put(
                "completed/"
                + manifest["manifest_id"].removeprefix("sha256:")
                + ".json",
                canonical_json(
                    {
                        "manifest_id": manifest["manifest_id"],
                        "generation_id": generation["generation_id"],
                        "status": "INACTIVE_PARTIAL",
                    }
                ).encode(),
            )
            return {
                "verification_level": (
                    "LOCAL_INTEGRATION_VERIFIED"
                    if cfg.paths.base.startswith("file://")
                    else "CLOUD_PROCESSING_VERIFIED"
                ),
                "manifest_id": manifest["manifest_id"],
                "silver_counts": silver,
                "gold": generation,
            }
        except Exception:
            record(
                stage, "FAILED", error={"code": "PROCESSING_FAILED", "retryable": True}
            )
            raise
        finally:
            if not os.getenv("DATABRICKS_RUNTIME_VERSION"):
                spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("process", "pending"))
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--manifest-uri")
    args = parser.parse_args(argv)
    config = parse_json(args.config.read_bytes())
    if args.action == "pending":
        admit(config["source_policy"], fixture=config.get("mode") == "fixture")
        control = ArtifactStore(config["control_base_uri"])
        result = []
        for uri in control.list("pending"):
            reference = parse_json(control.read(uri))
            raw = control.read(reference["artifact_uri"])
            if sha256(raw) != reference["content_sha256"]:
                raise ValueError("Pending handoff checksum mismatch")
            manifest = threads_manifest.validate(
                parse_json(raw), targets=target_config(config)
            )
            completed = control.uri(
                "completed/" + manifest["manifest_id"].removeprefix("sha256:") + ".json"
            )
            if control.optional(completed) is None:
                result.append(process(config, manifest_uri=reference["artifact_uri"]))
    else:
        if not args.manifest_uri:
            parser.error("process requires --manifest-uri")
        result = process(config, manifest_uri=args.manifest_uri)
    print(canonical_json(result))
    return result


if __name__ == "__main__":
    main()
