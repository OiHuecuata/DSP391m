# Local Threads Silver Kafka transport

From the repository root:

```sh
poetry install --no-root
make kafka-up
make kafka-topic
make collect-threads-kafka
make kafka-consume-social
```

`make collect-threads` stays local by default. Kafka requires `--publish-kafka`
or explicit `KAFKA_ENABLED=true`. PBPlus is batch only and has no Kafka path.
Bronze and Silver files are committed before the first Kafka producer is created.

Replay already-saved Silver without contacting Threads or changing artifacts:

```sh
PYTHONPATH=apps/collector/src poetry run python -m dsp_collector.jobs.collect_social \
  --replay-silver "apps/collector/.local/silver/social_posts/<run_id>.json"
```

The topic default is defined once in `runtime/config.py`; override with
`KAFKA_SOCIAL_POST_TOPIC`. Settings also accept `KAFKA_BOOTSTRAP_SERVERS`
(default `localhost:9092`), `KAFKA_CLIENT_ID`, `KAFKA_DELIVERY_TIMEOUT_MS`
(30000), `KAFKA_REQUEST_TIMEOUT_MS` (5000) and `KAFKA_RETRIES` (10).
Container clients use `kafka:29092` on the Compose network. The development
broker uses PLAINTEXT and binds the host port to loopback. Authentication/TLS
and production broker sizing are outside this local implementation.

The pinned Apache Kafka broker runs KRaft with a persistent named volume.
A one-time initializer sets that volume to the image broker user (UID 1000).
`make kafka-topic` explicitly creates three partitions, replication factor one,
and describes the topic; automatic topic creation is disabled.
`make kafka-down` stops Kafka and retains its volume and topic data.

Each event references the existing Silver schema. The payload is unchanged.
The message key is UTF-8 `record_id`. Event ID is `sha256:` plus the SHA-256
hex digest of compact UTF-8 JSON `[run_id, record_id]`. Event creation time is
UTC and does not participate in identity. Framing avoids delimiter ambiguity.

Delivery uses `acks=all`, producer idempotence, bounded retries and timeout.
Only successful delivery callbacks count as published. Enqueue failures,
failed callbacks, incomplete flushes and missing acknowledgements are visible.
Publication reports success, partial or failed and CLI exits 0, 2 or 1.
Collection-level rejection status is retained when combined with publication.
Empty Silver batches succeed without creating a producer.

This targets at-least-once delivery with operator replay, not exactly-once
end-to-end delivery. There is no transaction spanning local Silver and Kafka,
nor an automatic outbox scheduler. A timeout may follow a successful broker
write; full-batch replay may duplicate acknowledged records too. Future
consumers must deduplicate by event ID. Producer idempotence does not deduplicate
independent producer sessions. Local artifacts remain available after failure.

Schema validation uses `jsonschema` and a registry containing only repository
contracts; it does not fetch remote schemas. Run commands from this repository
checkout, which supplies those contract files.
