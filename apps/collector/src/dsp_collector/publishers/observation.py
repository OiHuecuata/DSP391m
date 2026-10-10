"""v2 verified-native references; enqueue and uncertain acknowledgement never mark handoff complete."""

from functools import lru_cache
from importlib.resources import files
from pathlib import Path

from ..normalizers.observation import _validator, canonical_json, parse_json

TOPIC = "dsp.social.observations.v2"


@lru_cache(maxsize=1)
def _schema():
    name = "kafka/social-observation-event.v2.schema.json"
    root = next(
        (
            p / "contracts"
            for p in Path(__file__).resolve().parents
            if (p / "contracts" / name).is_file()
        ),
        None,
    )
    return parse_json(
        (root / name if root else files("dsp_contracts") / name).read_bytes()
    )


def validate(event):
    _validator(_schema()).validate(event)
    if event["event_id"] != event["observation_id"]:
        raise ValueError("Transport ID must preserve observation identity")
    return event


def build(row, receipt):
    event = {
        k: row[k]
        for k in (
            "observation_id",
            "source_platform",
            "source_record_id",
            "record_type",
            "collection_run_id",
            "query_id",
            "fetch_batch_id",
            "artifact_uri",
            "artifact_sha256",
            "payload_hash",
        )
    }
    event.update(
        schema_version="2",
        event_id=row["observation_id"],
        event_type="social.observation.verified",
        emitted_at=row["observed_at"],
        **receipt,
    )
    return validate(event)


def publish(events, *, bootstrap, producer_factory=None):
    from confluent_kafka import Producer

    events = [validate(e) for e in events]
    producer = (producer_factory or Producer)(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": 30000,
            "allow.auto.create.topics": False,
        }
    )
    receipts = {}

    def callback(event_id):
        def delivered(error, message):
            receipts[event_id] = (
                None
                if error
                else {
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                }
            )

        return delivered

    for event in events:
        producer.produce(
            TOPIC,
            key=event["observation_id"].encode(),
            value=canonical_json(event).encode(),
            on_delivery=callback(event["event_id"]),
        )
        producer.poll(0)
    if (
        producer.flush(35)
        or len(receipts) != len(events)
        or any(v is None for v in receipts.values())
    ):
        raise RuntimeError(
            "Kafka acknowledgement uncertain: retry the same persisted observation IDs"
        )
    return receipts
