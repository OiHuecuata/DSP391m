"""Finite local E2E control plane composing the existing collector and data jobs."""

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from importlib.metadata import version
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "apps/collector/src"), str(ROOT / "pipelines/spark/src")]


@contextmanager
def lakehouse_lock(config):
    """One writer across backend cycles and finite local orchestration commands."""
    uri = urlsplit(config.spark.paths.base)
    if uri.scheme != "file" or uri.netloc:
        raise ValueError("Local pipeline writer lock requires a local file URI")
    directory = Path(unquote(uri.path))
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".pipeline-writer.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another local pipeline writer is active") from None
        yield


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_report(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".report-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(payload, output, ensure_ascii=False, allow_nan=False, indent=2)
            output.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def positive_integer(name, default):
    raw = os.getenv(name, str(default))
    if not re.fullmatch(r"[0-9]+", raw) or int(raw) < 1:
        raise ValueError(f"{name} must be a positive integer")
    return int(raw)


def shared_setting(producer, consumer, default):
    values = [os.environ[k] for k in (producer, consumer) if k in os.environ]
    if len(set(values)) > 1:
        raise ValueError(f"Conflicting {producer} and {consumer}; configure one value")
    return values[0] if values else default


@dataclass(frozen=True)
class Config:
    mode: str
    state: Path
    replay: dict
    kafka: object
    spark: object
    warehouse: object
    market: object
    stream_timeout: int
    collector_timeout: int
    limits: dict


def resolve_config():
    from dsp_collector.runtime.config import KAFKA_SOCIAL_POST_TOPIC, KafkaSettings
    from dsp_spark.runtime.clickhouse import ClickHouseSettings
    from dsp_spark.runtime.config import MarketSignalSettings, SparkSettings

    mode = os.getenv("PIPELINE_MODE", "replay")
    if mode not in ("replay", "live"):
        raise ValueError("PIPELINE_MODE must be replay or live")
    kafka = KafkaSettings.from_env()
    kafka = KafkaSettings(
        **{
            **asdict(kafka),
            "enabled": True,
            "bootstrap_servers": shared_setting(
                "KAFKA_BOOTSTRAP_SERVERS",
                "DSP_KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092",
            ),
            "social_post_topic": shared_setting(
                "KAFKA_SOCIAL_POST_TOPIC",
                "DSP_KAFKA_SOCIAL_POST_TOPIC",
                KAFKA_SOCIAL_POST_TOPIC,
            ),
        }
    )
    if kafka.bootstrap_servers not in ("localhost:9092", "127.0.0.1:9092"):
        raise ValueError(
            "Host-side local E2E uses localhost:9092 or 127.0.0.1:9092, not kafka:29092"
        )
    # All subprocesses and job settings get exactly the same transport values.
    os.environ.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": kafka.bootstrap_servers,
            "DSP_KAFKA_BOOTSTRAP_SERVERS": kafka.bootstrap_servers,
            "KAFKA_SOCIAL_POST_TOPIC": kafka.social_post_topic,
            "DSP_KAFKA_SOCIAL_POST_TOPIC": kafka.social_post_topic,
            "KAFKA_ENABLED": "false",  # Publication is its own acknowledged stage.
            "PYSPARK_PYTHON": sys.executable,
            "PYSPARK_DRIVER_PYTHON": sys.executable,
        }
    )
    spark = SparkSettings.from_env()
    if not re.fullmatch(r"local\[(?:[1-9][0-9]*|\*)\]", spark.master):
        raise ValueError("Local E2E requires a local Spark master")
    for field in spark.paths.__dataclass_fields__:
        if not getattr(spark.paths, field).startswith("file://"):
            raise ValueError("Local E2E requires local file lakehouse/checkpoint paths")
    warehouse = ClickHouseSettings.from_env()
    if warehouse.host not in ("localhost", "127.0.0.1"):
        raise ValueError("Host-side local E2E requires a loopback ClickHouse host")
    replay = {}
    for source, variable in (
        ("threads", "THREADS_REPLAY_PATH"),
        ("pbplus", "PBPLUS_REPLAY_PATH"),
    ):
        supplied = os.getenv(variable, "")
        if mode == "replay":
            if not supplied:
                raise ValueError(
                    f"Replay requires {variable}; see orchestration/local/README.md"
                )
            path = Path(supplied).expanduser().resolve()
            if not path.is_file():
                raise FileNotFoundError(f"Replay input missing: {path}")
            replay[source] = {"path": str(path), "sha256": digest(path)}
        elif supplied:
            raise ValueError(f"Live mode cannot use {variable}; unset replay paths")
    return Config(
        mode,
        Path(os.getenv("PIPELINE_STATE_DIR", ROOT / ".local/pipeline"))
        .expanduser()
        .resolve(),
        replay,
        kafka,
        spark,
        warehouse,
        MarketSignalSettings.from_env(),
        positive_integer("PIPELINE_STREAM_TIMEOUT_SECONDS", 300),
        positive_integer("PIPELINE_COLLECTOR_TIMEOUT_SECONDS", 1200),
        {
            "threads": positive_integer("PIPELINE_THREADS_LIMIT", 10),
            "pbplus": positive_integer("PIPELINE_PBPLUS_LIMIT", 1000),
        },
    )


def writable_directory(path):
    path.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(dir=path):
        pass


def preflight(config):
    from dsp_spark.processing.clean.contracts import contract_documents, parse_json
    from dsp_spark.processing.transform.location import load_alias_map
    from dsp_spark.runtime.config import DELTA_VERSION, SPARK_VERSION

    if sys.version_info[:2] != (3, 12):
        raise ValueError("Local E2E is verified with Python 3.12; select it in Poetry")
    for package, expected in (
        ("pyspark", SPARK_VERSION),
        ("delta-spark", DELTA_VERSION),
    ):
        if version(package) != expected:
            raise ValueError(f"Install the locked {package} runtime with make install")
    java = Path(os.environ.get("JAVA_HOME", "")) / "bin/java"
    if not java.is_file():
        raise ValueError(
            "Set JAVA_HOME or DSP_JAVA_HOME to an installed Java 17/21 JDK"
        )
    result = subprocess.run(
        [str(java), "-version"], capture_output=True, text=True, check=True
    )
    if not re.search(r'version "(?:17|21)\.', result.stderr + result.stdout):
        raise ValueError("Local E2E requires Java 17 or 21")
    if not shutil.which("docker"):
        raise ValueError("Install Docker Engine and Compose 2.24+ before local E2E")
    result = subprocess.run(
        ["docker", "compose", "version", "--short"],
        capture_output=True,
        text=True,
        check=True,
    )
    parts = re.search(r"(\d+)\.(\d+)", result.stdout)
    if not parts or tuple(map(int, parts.groups())) < (2, 24):
        raise ValueError("Docker Compose 2.24+ is required")
    writable_directory(config.state)
    if not os.getenv("DSP_SPARK_JARS"):
        writable_directory(
            Path(
                os.getenv("DSP_SPARK_IVY_DIR", ROOT / "pipelines/spark/.local/ivy")
            ).expanduser()
        )
    for field in config.spark.paths.__dataclass_fields__:
        path = Path(unquote(urlsplit(getattr(config.spark.paths, field)).path))
        writable_directory(path if field == "base" or path.is_dir() else path.parent)
        if (path / "_delta_log").is_dir():
            writable_directory(path / "_delta_log")
        if field == "social_checkpoint":
            for name in ("commits", "offsets", "sources"):
                if (path / name).is_dir():
                    writable_directory(path / name)
    load_alias_map()  # Validate the shared reference before collection/publication.
    for source, identity in config.replay.items():
        envelope = parse_json(Path(identity["path"]).read_text())
        if envelope.get("source") != source:
            raise ValueError(f"Wrong source in {source} replay input")
    if config.mode == "live":
        from playwright.sync_api import sync_playwright

        with sync_playwright() as browser:
            if not Path(browser.chromium.executable_path).is_file():
                raise ValueError(
                    "Install Chromium: poetry run playwright install --with-deps chromium"
                )
    return {
        "mode": config.mode,
        "python": sys.executable,
        "spark_master": config.spark.master,
        "kafka_bootstrap_servers": config.kafka.bootstrap_servers,
        "kafka_topic": config.kafka.social_post_topic,
        "paths": asdict(config.spark.paths),
        "clickhouse": {
            "host": config.warehouse.host,
            "port": config.warehouse.port,
            "database": config.warehouse.database,
            "user": config.warehouse.user,
        },
        "analysis": asdict(config.market),
        "replay_inputs": config.replay,
        "catalog_sha256": digest(ROOT / "configs/reference/vietnam_locations.v1.json"),
        "contracts_sha256": hashlib.sha256(contract_documents().encode()).hexdigest(),
    }


def ensure_infrastructure(config):
    from confluent_kafka.admin import AdminClient
    from dsp_spark.io.clickhouse import ClickHouseClient, migrate

    # Healthy containers are left running; only unavailable services go through up.
    result = subprocess.run(
        ["docker", "compose", "ps", "--all", "--format", "json"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    output = result.stdout.strip()
    services = (
        json.loads(output)
        if output.startswith("[")
        else [json.loads(line) for line in output.splitlines()]
    )
    healthy = {
        r["Service"]
        for r in services
        if r.get("State") == "running" and r.get("Health") == "healthy"
    }
    missing = [name for name in ("kafka", "clickhouse") if name not in healthy]
    if missing:
        subprocess.run(
            [
                "docker",
                "compose",
                "up",
                "-d",
                "--wait",
                "--wait-timeout",
                "120",
                *missing,
            ],
            cwd=ROOT,
            check=True,
            stdout=sys.stderr,
        )
    subprocess.run(
        ["sh", "infra/kafka/create-topics.sh", config.kafka.social_post_topic],
        cwd=ROOT,
        check=True,
        stdout=sys.stderr,
    )
    admin = AdminClient({"bootstrap.servers": config.kafka.bootstrap_servers})
    topic = admin.list_topics(config.kafka.social_post_topic, timeout=15).topics.get(
        config.kafka.social_post_topic
    )
    if topic is None or topic.error is not None:
        raise RuntimeError("Configured host Kafka topic is unavailable")
    with ClickHouseClient(config.warehouse) as client:
        if client.http.get("/ping").text.strip() != "Ok.":
            raise RuntimeError("ClickHouse readiness failed")
        migrate(client)
    return {
        "topic_exists": True,
        "partitions": len(topic.partitions),
        "started_services": missing,
    }


def collection_status(result, exit_code):
    status = result.get("status")
    if {"success": 0, "partial": 2, "failed": 1}.get(status) != exit_code:
        raise ValueError("Collector status and exit code disagree")
    if status == "failed" or (status == "partial" and result.get("count", 0) <= 0):
        raise ValueError("Collector failed or partial collection has no usable records")


def check_artifacts(source, manifest, directory):
    from dsp_spark.processing.clean.contracts import parse_json, validate

    result = manifest["result"]
    collection_status(result, manifest["exit_code"])
    envelopes = {}
    for name in ("bronze_path", "silver_path"):
        path = Path(result[name]).resolve()
        if not path.is_relative_to(directory.resolve()) or not path.is_file():
            raise ValueError(
                "Collector artifact missing or outside its collection directory"
            )
        if digest(path) != manifest["hashes"][name]:
            raise ValueError("Collector artifact hash changed")
        envelopes[name] = parse_json(path.read_text())
        if (
            envelopes[name].get("source") != source
            or envelopes[name].get("run_id") != result["run_id"]
        ):
            raise ValueError("Collector artifact source/run lineage mismatch")
    bronze, silver = envelopes["bronze_path"], envelopes["silver_path"]
    validate(bronze, source + "_bronze")
    if (
        silver.get("schema_version") != "1"
        or silver.get("status") != result["status"]
        or silver.get("count") != result["count"]
        or len(silver.get("records", [])) != result["count"]
        or silver.get("rejected_count") != result["rejected_count"]
        or silver.get("input_count") != bronze["count"]
        or result["count"] + result["rejected_count"] != bronze["count"]
    ):
        raise ValueError(
            "Collector artifact counts/status disagree with reported result"
        )
    identities = set()
    for record in silver["records"]:
        validate(record, "social" if source == "threads" else "court")
        if record["run_id"] != result["run_id"] or record["source"] != source:
            raise ValueError("Normalized record lineage mismatch")
        if record["record_id"] in identities:
            raise ValueError("Duplicate normalized record identity in collection")
        identities.add(record["record_id"])
    return result


def collect(config, source, run_directory):
    identity = config.replay.get(source)
    if identity and digest(identity["path"]) != identity["sha256"]:
        raise ValueError("Replay input changed after preflight")
    directory = (
        config.state / "replay" / source / identity["sha256"]
        if identity
        else run_directory / source
    )
    path = directory / "manifest.json"
    if path.exists():
        manifest = json.loads(path.read_text())
        if manifest["input"]["sha256"] != identity["sha256"]:
            raise ValueError("Replay manifest/input mismatch")
        return {
            **check_artifacts(source, manifest, directory),
            "manifest_path": str(path),
        }
    directory.mkdir(parents=True, exist_ok=True)
    attempt = Path(tempfile.mkdtemp(prefix="collection-", dir=directory))
    arguments = ["--artifact-root", str(attempt), "--no-status"]
    if identity:
        arguments += ["--replay", identity["path"]]
        supplied = json.loads(Path(identity["path"]).read_text()).get("run_id")
        if supplied is None:
            arguments += ["--run-id", "replay-" + identity["sha256"][:32]]
    else:
        arguments += ["--limit", str(config.limits[source])]
    module = "dsp_collector.jobs." + (
        "collect_social" if source == "threads" else "collect_courts"
    )
    environment = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            [str(ROOT / "apps/collector/src"), str(ROOT / "pipelines/spark/src")]
        ),
    }
    completed = subprocess.run(
        [sys.executable, "-m", module, *arguments],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=config.collector_timeout,
        check=False,
    )
    (attempt / "stdout.log").write_text(completed.stdout)
    (attempt / "stderr.log").write_text(completed.stderr)
    if completed.returncode not in (0, 2):
        raise RuntimeError(
            f"{source} collector exited {completed.returncode}; see {attempt}/stderr.log"
        )
    result = json.loads(completed.stdout.strip().splitlines()[-1])
    manifest = {
        "input": identity,
        "result": result,
        "exit_code": completed.returncode,
        "hashes": {
            name: digest(result[name]) for name in ("bronze_path", "silver_path")
        },
    }
    check_artifacts(source, manifest, directory)
    write_report(path, manifest)
    return {**result, "manifest_path": str(path)}


def fingerprint(frame):
    # Generation metadata varies; every analytical value participates.
    rows = [
        {k: v for k, v in r.asDict().items() if k != "gold_generated_at"}
        for r in frame.collect()
    ]
    serialized = sorted(
        json.dumps(r, sort_keys=True, ensure_ascii=False, allow_nan=False) for r in rows
    )
    return hashlib.sha256(
        json.dumps(serialized, ensure_ascii=False).encode()
    ).hexdigest()


def verify(spark, config, collections):
    from delta.tables import DeltaTable
    from dsp_collector.publishers.kafka import build_social_post_event
    from dsp_spark.io.clickhouse import ClickHouseClient, read_gold_rows
    from dsp_spark.io.delta import read_delta
    from pyspark.sql import functions as F

    bronze_counts = {}
    for source, result in collections.items():
        manifest_path = Path(result["manifest_path"])
        check_artifacts(
            source, json.loads(manifest_path.read_text()), manifest_path.parent
        )
        frame = read_delta(spark, getattr(config.spark.paths, source + "_bronze"))
        rows = frame.filter(
            (F.col("source") == source) & (F.col("run_id") == result["run_id"])
        ).collect()
        if len(rows) != 1 or rows[0].payload_json != Path(
            result["bronze_path"]
        ).read_bytes().decode("utf-8"):
            raise ValueError(f"{source} Bronze run/envelope verification failed")
        bronze_counts[source] = 1
        table = "social_posts" if source == "threads" else "courts"
        silver = read_delta(spark, getattr(config.spark.paths, table))
        if source == "threads" and DeltaTable.isDeltaTable(
            spark, config.spark.paths.social_comments
        ):
            silver = silver.unionByName(
                read_delta(spark, config.spark.paths.social_comments)
            )
        stored = silver.filter(F.col("run_id") == result["run_id"]).collect()
        expected = json.loads(Path(result["silver_path"]).read_text())["records"]
        actual = {r.record_id: r.asDict(recursive=True) for r in stored}
        if len(stored) != len(expected) or len(actual) != len(stored):
            raise ValueError(f"{source} Silver observation count/identity mismatch")
        for record in expected:
            persisted = actual.get(record["record_id"], {})
            if any(persisted.get(k) != v for k, v in record.items()):
                raise ValueError(f"{source} Silver payload differs from collection")
            if (
                source == "threads"
                and persisted.get("event_id")
                != build_social_post_event(record)["event_id"]
            ):
                raise ValueError("Social event identity mismatch")
    tables = (
        "social_posts",
        "courts",
        "social_activity_daily",
        "court_supply_by_location",
        "location_market_signal_snapshot",
    )
    frames = {
        name: read_delta(spark, getattr(config.spark.paths, name)) for name in tables
    }
    counts = {name: frame.count() for name, frame in frames.items()}
    from dsp_spark.processing.clean.gold import validated_gold

    for name in tables[2:]:
        validated_gold(frames[name], name)
    fingerprints = {name: fingerprint(frames[name]) for name in tables[2:]}
    from delta.tables import DeltaTable

    rejected = (
        read_delta(spark, config.spark.paths.rejected_social)
        .filter(F.col("kafka_topic") == config.kafka.social_post_topic)
        .count()
        if DeltaTable.isDeltaTable(spark, config.spark.paths.rejected_social)
        else 0
    )
    source = read_gold_rows(spark, config.spark.paths.location_market_signal_snapshot)
    with ClickHouseClient(config.warehouse) as client:
        client.check_schema("location_market_signal_snapshot")
        served = client.read_rows()
    if served != source or len(served) != counts["location_market_signal_snapshot"]:
        raise ValueError("Final ClickHouse/Delta verification mismatch")
    return {
        "bronze": bronze_counts,
        "delta": counts,
        "fingerprints": fingerprints,
        "rejected_social": {
            "topic_rows": rejected,
            "path": config.spark.paths.rejected_social,
        },
        "clickhouse": {"rows": len(served), "all_fields_match": True},
    }


def execute(config, summary, directory):
    with lakehouse_lock(config):
        return _execute_locked(config, summary, directory)


def _execute_locked(config, summary, directory):
    from dsp_collector.publishers.kafka import (
        publish_bronze_batch,
        publish_social_batch,
    )
    from dsp_spark.io.clickhouse import (
        ClickHouseClient,
        read_gold_rows,
        replace_serving,
    )
    from dsp_spark.jobs.batch.build_court_supply_by_location import (
        build_court_supply_by_location,
    )
    from dsp_spark.jobs.batch.build_location_market_signal_snapshot import (
        build_location_market_signal_snapshot,
    )
    from dsp_spark.jobs.batch.build_social_activity_daily import (
        build_social_activity_daily,
    )
    from dsp_spark.jobs.batch.ingest_bronze import ingest_bronze
    from dsp_spark.jobs.batch.ingest_pbplus import ingest_pbplus
    from dsp_spark.jobs.streaming.social_kafka_to_delta import start_social_stream
    from dsp_spark.runtime.spark import create_spark

    def stage(name, operation):
        summary["stage"] = name
        print(f"[pipeline-e2e] {name}", file=sys.stderr, flush=True)
        return operation()

    summary["infrastructure"] = stage(
        "infrastructure", lambda: ensure_infrastructure(config)
    )
    collections = summary["collection"] = {}
    for source in ("threads", "pbplus"):
        collections[source] = stage(
            "collect_" + source,
            lambda source=source: collect(config, source, directory),
        )
    summary["threads_run_id"] = collections["threads"]["run_id"]
    summary["pbplus_run_id"] = collections["pbplus"]["run_id"]
    raw_publication = stage(
        "bronze_kafka_publish",
        lambda: publish_bronze_batch(
            collections["threads"]["bronze_path"], settings=config.kafka
        ),
    )
    summary["bronze_publication"] = asdict(raw_publication)
    if raw_publication.flush_error or any(
        e.get("event_id") or e.get("observation_id") for e in raw_publication.errors
    ):
        raise ValueError("Bronze observations could not be acknowledged")
    publication = stage(
        "kafka_publish",
        lambda: publish_social_batch(
            collections["threads"]["silver_path"], settings=config.kafka
        ),
    )
    summary["publication"] = asdict(publication)
    if (
        publication.status != "success"
        or publication.published_count != collections["threads"]["count"]
    ):
        raise ValueError("Not all expected social messages were acknowledged")
    spark = stage("spark_runtime", lambda: create_spark(config.spark, kafka=True))
    query = None
    try:
        for source in collections:
            stage(
                "bronze_" + source,
                lambda source=source: ingest_bronze(
                    spark,
                    collections[source]["bronze_path"],
                    config.spark,
                    source=source,
                ),
            )
        stage(
            "court_silver",
            lambda: ingest_pbplus(
                spark, collections["pbplus"]["silver_path"], config.spark
            ),
        )
        query = stage(
            "social_stream",
            lambda: start_social_stream(spark, config.spark, available_now=True),
        )
        if not query.awaitTermination(config.stream_timeout):
            raise TimeoutError("AvailableNow streaming timed out")
        progress = query.recentProgress
        consumed = sum(p["numInputRows"] for p in progress)
        completed = [p["batchId"] for p in progress if p["numInputRows"] > 0]
        checkpoint = Path(unquote(urlsplit(config.spark.paths.social_checkpoint).path))
        if any(
            not (checkpoint / "commits" / str(batch)).is_file() for batch in completed
        ):
            raise ValueError("Consumed streaming batch has no checkpoint commit")
        summary["streaming"] = {
            "completed": True,
            "input_rows": consumed,
            "checkpoint": config.spark.paths.social_checkpoint,
            "committed_batches": completed,
        }
        summary["social_gold"] = stage(
            "social_gold", lambda: build_social_activity_daily(spark, config.spark)
        )
        summary["court_gold"] = stage(
            "court_gold", lambda: build_court_supply_by_location(spark, config.spark)
        )
        summary["market_snapshot"] = stage(
            "market_snapshot",
            lambda: build_location_market_signal_snapshot(
                spark, config.spark, market_settings=config.market
            ),
        )
        rows = read_gold_rows(spark, config.spark.paths.location_market_signal_snapshot)
        with ClickHouseClient(config.warehouse) as client:
            summary["load"] = stage(
                "clickhouse_load", lambda: replace_serving(client, rows)
            )
        summary.update(
            stage("verification", lambda: verify(spark, config, collections))
        )
    finally:
        try:
            if query is not None and query.isActive:
                query.stop()
        finally:
            spark.stop()
    summary["status"] = (
        "partial"
        if any(r["status"] == "partial" for r in collections.values())
        else "success"
    )
    summary["stage"] = "complete"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Check runtime/configuration without starting services or collecting",
    )
    args = parser.parse_args(argv)

    def interrupted(_signum, _frame):
        raise KeyboardInterrupt("Pipeline interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    summary = {
        "status": "failed",
        "mode": os.getenv("PIPELINE_MODE", "replay"),
        "pipeline_run_id": uuid4().hex,
        "stage": "configuration",
    }
    directory = None
    try:
        from dotenv import dotenv_values

        env_file = (
            Path(os.getenv("PIPELINE_ENV_FILE", ROOT / ".env")).expanduser().resolve()
        )
        if "PIPELINE_ENV_FILE" in os.environ and not env_file.is_file():
            raise FileNotFoundError(f"PIPELINE_ENV_FILE missing: {env_file}")
        file_values = {
            k: v
            for k, v in dotenv_values(env_file, interpolate=False).items()
            if v is not None
        }
        os.environ.update({**file_values, **os.environ})
        summary["mode"] = os.getenv("PIPELINE_MODE", "replay")
        config = resolve_config()
        summary["mode"] = config.mode
        summary["stage"] = "preflight"
        summary["configuration"] = preflight(config)
        directory = config.state / "runs" / summary["pipeline_run_id"]
        directory.mkdir(parents=True, exist_ok=True)
        summary["summary_path"] = str(directory / "summary.json")
        if args.preflight:
            summary.update(status="success", stage="preflight_complete")
        else:
            execute(config, summary, directory)
    except (Exception, KeyboardInterrupt) as exc:  # noqa: BLE001
        message = str(exc) or type(exc).__name__
        if secret := os.getenv("CLICKHOUSE_PASSWORD"):
            message = message.replace(secret, "[redacted]")
        summary.update(status="failed", error=message)
    finally:
        if directory is not None:
            try:
                write_report(directory / "summary.json", summary)
            except OSError as exc:
                summary.update(
                    status="failed",
                    error=f"Cannot persist run summary: {type(exc).__name__}",
                )
        terminal = {
            key: summary[key]
            for key in (
                "status",
                "mode",
                "pipeline_run_id",
                "stage",
                "summary_path",
                "threads_run_id",
                "pbplus_run_id",
                "delta",
                "clickhouse",
                "fingerprints",
                "streaming",
                "error",
            )
            if key in summary
        }
        if "collection" in summary:
            terminal["collection"] = {
                source: {
                    "run_id": result["run_id"],
                    "status": result["status"],
                    "accepted": result["count"],
                    "rejected": result["rejected_count"],
                }
                for source, result in summary["collection"].items()
            }
        if "publication" in summary:
            terminal["publication"] = {
                key: summary["publication"][key]
                for key in ("topic", "status", "published_count", "failed_count")
            }
        if args.preflight and "configuration" in summary:
            terminal["configuration"] = summary["configuration"]
        print(json.dumps(terminal, ensure_ascii=False, allow_nan=False), flush=True)
    return 1 if summary["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
