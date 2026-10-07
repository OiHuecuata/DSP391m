# One-command local E2E

`make pipeline-e2e` runs the existing supported Threads/PBPlus pipeline, including
real Kafka and AvailableNow Structured Streaming. It verifies collection handoffs,
Delta Bronze/Silver/Gold, and every ClickHouse serving value before reporting success.
Replay is the default; live collection is explicit. Social activity remains a
text-mentioned activity signal, not measured demand. Unsupported geography stays
in daily social Gold with null identity and is reported/excluded from the snapshot.

## Prerequisites

Use Python **3.12**, Poetry 2.x, Java **17 or 21**, Docker Engine and Compose
**2.24+**. Run from the repository root with Docker daemon access. Linux/WSL and
macOS host-side execution use loopback ports 9092 (Kafka) and 8123 (ClickHouse).

```bash
poetry env use python3.12
make install
export JAVA_HOME=/absolute/path/to/your/java17-or-java21
cp .env.example .env
```

Edit `.env` as needed; do not replace an existing configuration. Live mode also
requires browser binaries and OS dependencies:

```bash
poetry run playwright install --with-deps chromium
```

The first infrastructure startup needs Docker image downloads; the first Spark
startup needs Maven access. A compatible existing Ivy cache or `DSP_SPARK_JARS`
bundle can support offline Spark startup. No AWS, Databricks, Go backend, frontend,
or Airflow service is required by this command.

## Replay

Use saved **raw crawler/Bronze envelopes**, not normalized Silver files:

```bash
make pipeline-e2e PIPELINE_MODE=replay \
  THREADS_REPLAY_PATH=path/to/threads-bronze.json \
  PBPLUS_REPLAY_PATH=path/to/pbplus-bronze.json
```

To try the committed, explicitly synthetic development fixtures:

```bash
make pipeline-e2e PIPELINE_MODE=replay \
  THREADS_REPLAY_PATH=tests/fixtures/pipeline/threads.json \
  PBPLUS_REPLAY_PATH=tests/fixtures/pipeline/pbplus.json
```

This publishes fixture data to the configured local targets. For automatically
isolated acceptance verification, including identical replay and empty input:

```bash
make pipeline-e2e-test
```

The acceptance script invokes `make pipeline-e2e` itself. It uses separate Kafka
topics, ClickHouse databases, Delta roots, checkpoints, and an empty configuration
file. It retains evidence under `/tmp/dsp-pipeline-acceptance-*/acceptance.json`.
It also requires nonzero exits for absent analysis dates, conflicting topics, and
schema-invalid handoff. It performs no external crawling and deletes no normal data.

Replay preserves an input's `run_id`. If none is present, identity derives from
the input SHA-256. Immutable normalized artifacts and their hashes are cached under
`.local/pipeline/replay/<source>/<input-sha256>/`; exact replay reuses them so
normalization timestamps and observation payloads do not drift. A changed or
missing cached artifact fails explicitly. An interrupted collection gets a fresh
attempt directory. Reusing the same source run ID for different content is an
error detected by verification; use new source identities for new observations.

## Live and preflight

```bash
make pipeline-e2e PIPELINE_MODE=live
```

Replay paths must be unset in live mode. The existing collectors run with limits
10 Threads posts and 1000 PBPlus venues by default. Adjust explicitly:

```bash
make pipeline-e2e PIPELINE_MODE=live \
  PIPELINE_THREADS_LIMIT=3 PIPELINE_PBPLUS_LIMIT=20
```

Runtime/configuration checks can run without service startup or collection:

```bash
make pipeline-preflight PIPELINE_MODE=replay \
  THREADS_REPLAY_PATH=tests/fixtures/pipeline/threads.json \
  PBPLUS_REPLAY_PATH=tests/fixtures/pipeline/pbplus.json
```

## Execution and failure policy

```text
resolve configuration + runtime/path/catalog preflight
  → inspect/start unavailable Kafka and ClickHouse; wait for health
  → create/describe topic; validate host connection; migrate ClickHouse
  → collect/replay Threads and PBPlus through existing CLIs
  → validate exact artifact paths, hashes, contracts, counts and run IDs
  → publish saved SocialPost records with delivery acknowledgements
  → archive both raw envelopes to Delta Bronze; ingest Court Silver
  → drain Kafka with AvailableNow and verify completed checkpoint commits
  → build social daily Gold, court supply/quality, then market snapshot
  → existing validated staging + atomic ClickHouse replacement
  → verify per-run Bronze/Silver payloads, Gold contracts/fingerprints,
     table counts and all ClickHouse values
  → persist run summary and emit final JSON
```

Healthy services are not recreated. Missing/unhealthy services go through the
existing Compose health-gated startup. No normal checkpoint is reset. The Spark
session and any active query are stopped on success/failure/interruption.

Collector exit 0 continues, including valid zero-record collection. Exit 2
continues only for usable partial output whose accepted records pass contracts;
the final summary remains `partial` and shell exit is 0 after successful verification.
Collector exit 1, unusable partial output, status/exit disagreement, incomplete
Kafka acknowledgements, runtime/schema/Gold/load failures, and verification
mismatches stop the run with a failed JSON summary and nonzero shell exit.

A pipeline run is not a cross-service transaction. Earlier immutable artifacts,
Kafka messages, and Delta commits can remain after later failure. Retry the same
replay inputs; insert-only merges preserve observations. The current ClickHouse
loader continues to publish atomically after its staging validation. Use one
coordinated writer per lakehouse/serving target; do not overlap this command with
manual writers or an Airflow refresh of the same datasets.

Airflow remains the existing finite refresh control plane. It is not started here
because its DAG expects social Delta to exist already. After initial E2E ingestion,
the existing `airflow-up`/refresh targets remain available. No message-level Airflow
processing or replacement of the Kafka path is introduced.

## Empty input

Both Silver tables exist with zero rows after valid fresh empty ingestion. Gold
and ClickHouse can then publish an empty state. Supply-only data is also supported.
If the entire daily social Gold dataset is empty, supply an analysis date:

```bash
make pipeline-e2e PIPELINE_MODE=replay \
  THREADS_REPLAY_PATH=tests/fixtures/pipeline/threads-empty.json \
  PBPLUS_REPLAY_PATH=tests/fixtures/pipeline/pbplus-empty.json \
  DSP_MARKET_SIGNAL_END_DATE=2026-10-05
```

No date defaults to today. The existing maximum input activity-date rule otherwise
remains unchanged. Empty later batches retain prior Silver observations; a shared
lakehouse containing historical observations will not become empty just because
the current input is empty. Use a fresh isolated root/topic/database to verify an
empty entire pipeline.

## Configuration contract

The runner loads `.env` without evaluating shell code or interpolating secrets.
Exported environment and explicitly passed Make variables override file values.
`PIPELINE_ENV_FILE` selects a different dotenv file (default: repository `.env`).

| Setting | Default | Required / consumer |
|---|---|---|
| `PIPELINE_MODE` | `replay` | Runner; `replay` or `live` |
| `THREADS_REPLAY_PATH`, `PBPLUS_REPLAY_PATH` | Unset | Both required for replay; raw collection envelopes |
| `PIPELINE_STATE_DIR` | `.local/pipeline` | Runner artifacts, cache, manifests and summaries |
| `PIPELINE_ENV_FILE` | `.env` | Optional explicit dotenv file; must exist if overridden |
| `PIPELINE_THREADS_LIMIT` | `10` | Positive integer; live Threads CLI |
| `PIPELINE_PBPLUS_LIMIT` | `1000` | Positive integer; live PBPlus CLI |
| `PIPELINE_COLLECTOR_TIMEOUT_SECONDS` | `1200` | Per collector subprocess |
| `PIPELINE_STREAM_TIMEOUT_SECONDS` | `300` | AvailableNow completion deadline |
| `JAVA_HOME` / Make `DSP_JAVA_HOME` | Existing Make Java selection | Installed Java 17/21; Spark and preflight |
| `KAFKA_BOOTSTRAP_SERVERS`, `DSP_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Producer/consumer; either namespace resolves both |
| `KAFKA_SOCIAL_POST_TOPIC`, `DSP_KAFKA_SOCIAL_POST_TOPIC` | `dsp391.social-posts.v1` | Topic creation, producer and consumer |
| `KAFKA_CLIENT_ID` | `dsp391-collector` | Existing producer settings |
| `KAFKA_DELIVERY_TIMEOUT_MS` | `30000` | Existing producer settings |
| `KAFKA_REQUEST_TIMEOUT_MS` | `5000` | Existing producer settings |
| `KAFKA_RETRIES` | `10` | Existing producer settings |
| `KAFKA_ENABLED` | `false`; runner forces collection-time `false` | Existing collector setting, not an E2E switch |
| `DSP_SPARK_MASTER` | `local[2]` | Runner permits local Spark masters only |
| `DSP_SPARK_APP_NAME` | `dsp391-lakehouse` | Spark |
| `DSP_KAFKA_STARTING_OFFSETS` | `earliest` | Used only for a new streaming checkpoint |
| `DSP_SPARK_TRIGGER_INTERVAL` | `10 seconds` | Existing settings; finite command uses AvailableNow |
| `DSP_SPARK_JARS` | Unset | Optional compatible offline JAR bundle |
| `DSP_SPARK_IVY_DIR` | `pipelines/spark/.local/ivy` | Spark dependency cache |
| `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON` | Runner's Poetry interpreter | Runner sets these explicitly; overrides are not used |
| `DSP_LAKEHOUSE_BASE_URI` | `pipelines/spark/.local/lakehouse` as a file URI | All data jobs; local file URI only |
| `DSP_DELTA_THREADS_BRONZE_PATH` | `<base>/bronze/threads` | Threads raw archive |
| `DSP_DELTA_PBPLUS_BRONZE_PATH` | `<base>/bronze/pbplus` | PBPlus raw archive |
| `DSP_DELTA_SOCIAL_POSTS_PATH` | `<base>/silver/social_posts` | Stream and social Gold |
| `DSP_DELTA_COURTS_PATH` | `<base>/silver/courts` | Court ingestion and supply Gold |
| `DSP_DELTA_REJECTED_SOCIAL_PATH` | `<base>/rejected/social_posts` | Existing malformed-event handling |
| `DSP_SPARK_SOCIAL_CHECKPOINT_PATH` | `<base>/checkpoints/social_posts_kafka_to_delta` | Persistent streaming offsets/commits |
| `DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH` | `<base>/gold/social_activity_daily` | Social Gold and snapshot |
| `DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH` | `<base>/gold/court_supply_by_location` | Supply Gold and snapshot |
| `DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH` | `<base>/quality/unresolved_court_locations` | Supply quality |
| `DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH` | `<base>/gold/location_market_signal_snapshot` | Snapshot, loader and verification |
| `DSP_MARKET_SIGNAL_WINDOW_DAYS` | `30` | Existing inclusive-window contract |
| `DSP_MARKET_SIGNAL_END_DATE` | Unset / maximum social activity date | Required when daily social Gold is empty |
| `CLICKHOUSE_HOST` | `localhost` | Host loader; loopback only |
| `CLICKHOUSE_PORT` | `8123` | Host loader and Compose published HTTP port |
| `CLICKHOUSE_DATABASE` | `dsp391m` | Migration/load/verification target |
| `CLICKHOUSE_USER` | `dsp391m` | HTTP authentication |
| `CLICKHOUSE_PASSWORD` | Empty | Local authentication; omitted from reports |

Producer and consumer topic/bootstrap conflicts fail before collection. Host jobs
use loopback Kafka; `kafka:29092` remains the Docker-network address used by the
topic helper. All jobs use one resolved Spark path configuration, catalog/contracts
hashes, analysis window and ClickHouse target. Catalog override/hierarchy changes
are not supported. The runner sets both PySpark interpreter variables to its
Poetry interpreter and disables collection-time auto-publication; Kafka publishing
is a separately acknowledged stage. `KAFKA_ENABLED` is not a pipeline-mode switch.

## Evidence, inspection and shutdown

Final stdout contains JSON with status, mode, run IDs, collection counts, Delta
counts, fingerprints, ClickHouse equality, and the exact `summary_path`.
The persisted summary also includes resolved non-secret configuration, artifact
manifest paths/hashes, per-stage reports and checkpoint evidence. Failure returns
nonzero; consult `stage`, `error`, and the recorded collector log paths.

```bash
make delta-inspect-social
make delta-inspect-courts
make delta-inspect-gold-market-signal
make kafka-status
make clickhouse-status
make kafka-down
make clickhouse-down
```

Inspection uses the same exported path overrides when targets are isolated.
Shutdown retains volumes and Delta/checkpoints. Runtime data/cache/logs stay in
ignored `.local` directories or the acceptance `/tmp` root. The committed JSON
fixtures are synthetic test inputs, not collected/generated runtime output.
