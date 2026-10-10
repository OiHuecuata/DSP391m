"""Source observation and canonical Silver event transport; no crawling or storage.

Acknowledgements, not enqueue success, determine publication results. Idempotence
protects retries within a producer session; later Silver replay can duplicate
events. Downstream consumers must deduplicate stable event IDs. No exactly-once
end-to-end guarantee or atomic commit spanning local files and Kafka exists.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from importlib.resources import files
from pathlib import Path
from typing import Any, Literal

from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry, Resource

from ..runtime.config import KafkaSettings


class PublicationError(RuntimeError):
    """Producer initialization, enqueue, delivery or flush failed."""


@dataclass(slots=True)
class DeliveryReceipt:
    event_id: str
    record_id: str
    status: Literal["pending", "delivered", "failed"] = "pending"
    error: str | None = None


@dataclass(frozen=True, slots=True)
class PublicationResult:
    run_id: str
    topic: str
    status: Literal["success", "partial", "failed"]
    input_count: int
    published_count: int
    failed_count: int
    errors: list[dict[str, Any]]
    flush_error: str | None = None


@lru_cache(maxsize=1)
def _validators() -> tuple[Draft202012Validator, Draft202012Validator]:
    """Resolve both contracts locally; never retrieve schemas over HTTP."""
    root = next(
        (
            p / "contracts"
            for p in Path(__file__).resolve().parents
            if (p / "contracts/data/silver/social-post.v1.schema.json").is_file()
        ),
        None,
    )
    root = root or files("dsp_contracts")
    silver_path = root / "data/silver/social-post.v1.schema.json"
    event_path = root / "kafka/social-post-event.v1.schema.json"
    silver = json.loads(silver_path.read_text(encoding="utf-8"))
    event = json.loads(event_path.read_text(encoding="utf-8"))
    silver["$id"] = "file:///dsp-contracts/data/silver/social-post.v1.schema.json"
    event["$id"] = "file:///dsp-contracts/kafka/social-post-event.v1.schema.json"
    registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema)) for schema in (silver, event)
    )
    formats = FormatChecker()

    @formats.checks("date-time", raises=(ValueError, TypeError))
    def timezone_datetime(value: Any) -> bool:
        return not isinstance(value, str) or (
            re.fullmatch(
                r"\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})",
                value,
            )
            is not None
            and datetime.fromisoformat(value.upper()).utcoffset() is not None
        )

    return tuple(
        Draft202012Validator(schema, registry=registry, format_checker=formats)
        for schema in (silver, event)
    )


def _validate(value: Mapping[str, Any], validator: Draft202012Validator) -> None:
    try:
        validator.validate(value)
    except ValidationError as exc:
        raise ValueError(
            f"contract violation at {exc.json_path}: {exc.message}"
        ) from exc


def _event_id(run_id: str, record_id: str) -> str:
    framed = json.dumps([run_id, record_id], ensure_ascii=False, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(framed.encode("utf-8")).hexdigest()


def validate_social_post_event(event: Mapping[str, Any]) -> None:
    _validate(event, _validators()[1])
    for field in ("source", "run_id", "record_id"):
        if event[field] != event["payload"][field]:
            raise ValueError(f"event {field} conflicts with Silver payload")
    if event["source"] in ("youtube", "youtube_comment"):
        prefix = (
            "youtube-comment:" if event["source"] == "youtube_comment" else "youtube:"
        )
        if event["record_id"] != prefix + event["payload"]["source_record_id"]:
            raise ValueError("YouTube identity conflicts with source_record_id")
    if event["event_id"] != _event_id(event["run_id"], event["record_id"]):
        raise ValueError("event_id conflicts with observation identity")


def build_social_post_event(
    record: Mapping[str, Any], *, emitted_at: datetime | None = None
) -> dict[str, Any]:
    """Wrap an already-normalized record, preserving its entire payload."""
    _validate(record, _validators()[0])
    if record["source"] not in ("threads", "youtube", "youtube_comment"):
        raise ValueError("Unsupported SocialPost source")
    when = datetime.now(UTC) if emitted_at is None else emitted_at
    if when.utcoffset() is None:
        raise ValueError("emitted_at must be timezone-aware")
    event = {
        "schema_version": "1",
        "event_id": _event_id(record["run_id"], record["record_id"]),
        "event_type": "social_post.normalized",
        "source": record["source"],
        "run_id": record["run_id"],
        "record_id": record["record_id"],
        "emitted_at": when.astimezone(UTC)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z"),
        "payload": copy.deepcopy(dict(record)),
    }
    validate_social_post_event(event)
    return event


def _producer(config: dict[str, Any]) -> Any:
    from confluent_kafka import Producer

    return Producer(config)


class KafkaPublisher:
    """Small injectable producer wrapper. close flushes and releases the client."""

    def __init__(
        self,
        settings: KafkaSettings | None = None,
        *,
        producer_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        self.settings = settings if settings is not None else KafkaSettings.from_env()
        self._closed = False
        self._receipts: list[DeliveryReceipt] = []
        config = {
            "bootstrap.servers": self.settings.bootstrap_servers,
            "client.id": self.settings.client_id,
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 5,
            "retries": self.settings.retries,
            "retry.backoff.ms": 250,
            "delivery.timeout.ms": self.settings.delivery_timeout_ms,
            "request.timeout.ms": self.settings.request_timeout_ms,
            "socket.timeout.ms": self.settings.request_timeout_ms,
            "allow.auto.create.topics": False,
            "partitioner": "murmur2_random",
        }
        try:
            self._producer = (producer_factory or _producer)(config)
        except Exception as exc:
            raise PublicationError(
                f"Kafka producer initialization failed: {exc}"
            ) from exc

    def publish_bronze_observation(self, event):
        validate_bronze_event(event)
        return self._publish_event(event, event["event_id"])

    def publish_social_post(self, event: Mapping[str, Any]) -> DeliveryReceipt:
        validate_social_post_event(event)
        return self._publish_event(event, event["record_id"])

    def _publish_event(self, event, record_key) -> DeliveryReceipt:
        if self._closed:
            raise PublicationError("Kafka publisher is closed")
        serialized = json.dumps(event, ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
        receipt = DeliveryReceipt(event["event_id"], record_key)

        def delivered(error: Any, _message: Any) -> None:
            receipt.status = "failed" if error is not None else "delivered"
            receipt.error = str(error) if error is not None else None

        try:
            self._producer.produce(
                self.settings.social_post_topic,
                key=record_key.encode("utf-8"),
                value=serialized,
                on_delivery=delivered,
            )
        except Exception as exc:
            raise PublicationError(f"Kafka enqueue failed: {exc}") from exc
        self._receipts.append(receipt)
        return receipt

    def flush(self) -> None:
        if self._closed:
            raise PublicationError("Kafka publisher is closed")
        try:
            remaining = self._producer.flush(self.settings.delivery_timeout_ms / 1000)
        except Exception as exc:
            for receipt in self._receipts:
                if receipt.status == "pending":
                    receipt.status, receipt.error = (
                        "failed",
                        f"delivery unconfirmed: {exc}",
                    )
            raise PublicationError(f"Kafka flush failed: {exc}") from exc
        pending = [r for r in self._receipts if r.status == "pending"]
        for receipt in pending:
            receipt.status, receipt.error = (
                "failed",
                "delivery acknowledgement not observed before flush completed",
            )
        failures = [r for r in self._receipts if r.status == "failed"]
        if remaining or pending or failures:
            raise PublicationError(
                f"Kafka delivery failed: {len(failures)} failed/unconfirmed, {remaining} queued"
            )

    def close(self) -> None:
        if self._closed:
            return
        try:
            self.flush()
        finally:
            self._closed = True
            self._producer = None


def publish_social_batch(
    batch: Mapping[str, Any] | str | Path,
    *,
    settings: KafkaSettings | None = None,
    publisher_factory: Callable[[KafkaSettings], KafkaPublisher] | None = None,
) -> PublicationResult:
    """Publish accepted records from saved Silver, without crawling or rewriting it.

    Empty batches succeed without creating a producer. Invalid records are
    reported alongside delivery failures; valid records may still publish.
    Unconfirmed delivery can have reached Kafka, so replay may create duplicates.
    """
    if isinstance(batch, (str, Path)):
        batch = json.loads(Path(batch).read_text(encoding="utf-8"))
    if not isinstance(batch, Mapping):
        raise TypeError("Silver batch must be an object")
    if (
        batch.get("source") not in ("threads", "youtube")
        or batch.get("schema_version") != "1"
    ):
        raise ValueError("expected a supported SocialPost Silver v1 batch")
    records = batch.get("records")
    run_id = batch.get("run_id")
    if (
        not isinstance(run_id, str)
        or not run_id.strip()
        or not isinstance(records, list)
    ):
        raise ValueError("Silver batch requires run_id and records")
    if type(batch.get("count")) is not int or batch["count"] != len(records):
        raise ValueError("Silver batch count must equal len(records)")
    selected = settings if settings is not None else KafkaSettings.from_env()
    errors = []
    events = []
    for index, record in enumerate(records):
        try:
            event = build_social_post_event(record)
            if event["run_id"] != run_id:
                raise ValueError("record run_id conflicts with Silver batch")
            events.append((index, event))
        except (ValueError, TypeError) as exc:
            errors.append({"record_index": index, "error": str(exc)})
    receipts = []
    flush_error = None
    if events:
        publisher = (publisher_factory or KafkaPublisher)(selected)
        try:
            for index, event in events:
                try:
                    receipts.append((index, publisher.publish_social_post(event)))
                except (ValueError, TypeError, PublicationError) as exc:
                    errors.append(
                        {
                            "record_index": index,
                            "record_id": event["record_id"],
                            "error": str(exc),
                        }
                    )
        finally:
            try:
                publisher.close()
            except PublicationError as exc:
                flush_error = str(exc)
    published = 0
    for index, receipt in receipts:
        if receipt.status == "delivered":
            published += 1
        else:
            errors.append(
                {
                    "record_index": index,
                    "record_id": receipt.record_id,
                    "event_id": receipt.event_id,
                    "error": receipt.error or "delivery not acknowledged",
                }
            )
    failed = len(records) - published
    status: Literal["success", "partial", "failed"] = (
        "partial"
        if (failed or flush_error) and published
        else "failed" if failed or flush_error else "success"
    )
    return PublicationResult(
        run_id,
        selected.social_post_topic,
        status,
        len(records),
        published,
        failed,
        errors,
        flush_error,
    )


def validate_bronze_event(event):
    from ..runtime.bronze import DATASETS, decode, validate_observation

    if (
        set(event)
        != {"schema_version", "event_type", "event_id", "dataset", "record_json"}
        or event["schema_version"] != "1"
        or event["event_type"] != "bronze.observed"
        or event["dataset"] not in DATASETS
    ):
        raise ValueError("invalid Bronze event envelope")
    row = decode(event["record_json"])
    validate_observation(event["dataset"], row)
    if row["observation_id"] != event["event_id"]:
        raise ValueError("Bronze event identity conflicts with observation")
    return row


def publish_bronze_batch(artifact, *, settings=None, publisher_factory=None):
    """Publish source observations independently of Silver acceptance, with acks.

    Reuses the existing topic. Malformed source rows are returned as failures;
    successful observations and collection-run errors remain replayable.
    """
    import os

    from ..runtime.bronze import (
        encode,
        extract_collection_status,
        extract_observations,
        load_policies,
    )

    envelope = (
        json.loads(Path(artifact).read_text())
        if isinstance(artifact, (str, Path))
        else artifact
    )
    extractor = (
        extract_collection_status
        if "posts" not in envelope
        and "videos" not in envelope
        and ("finished_at" in envelope or "completed_at" in envelope)
        else extract_observations
    )
    datasets, errors = extractor(
        envelope,
        artifact_uri=(
            Path(artifact).resolve().as_uri()
            if isinstance(artifact, (str, Path))
            else None
        ),
        policies=load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY")),
    )
    selected = settings or KafkaSettings.from_env()
    receipts, flush_error = [], None
    publisher = (publisher_factory or KafkaPublisher)(selected)
    try:
        for dataset, rows in datasets.items():
            for row in rows:
                event = {
                    "schema_version": "1",
                    "event_type": "bronze.observed",
                    "event_id": row["observation_id"],
                    "dataset": dataset,
                    "record_json": encode(row),
                }
                try:
                    receipts.append(publisher.publish_bronze_observation(event))
                except (ValueError, TypeError, PublicationError) as exc:
                    errors.append(
                        {"observation_id": row["observation_id"], "error": str(exc)}
                    )
    finally:
        try:
            publisher.close()
        except PublicationError as exc:
            flush_error = str(exc)
    for receipt in receipts:
        if receipt.status != "delivered":
            errors.append(
                {
                    "event_id": receipt.event_id,
                    "error": receipt.error or "delivery not acknowledged",
                }
            )
    published = sum(r.status == "delivered" for r in receipts)
    failed = len(errors)
    return PublicationResult(
        envelope["run_id"],
        selected.social_post_topic,
        (
            "partial"
            if published and (failed or flush_error)
            else "failed" if failed or flush_error else "success"
        ),
        published + failed,
        published,
        failed,
        errors,
        flush_error,
    )
