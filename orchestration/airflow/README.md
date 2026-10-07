# Local batch orchestration

Airflow schedules dependencies, records task state/logs, and handles bounded
readiness retries. It invokes the existing DSP391m collector and Spark job CLIs;
there are no data transformations in the DAG.

```text
Airflow = orchestration
Spark = data processing
Delta Lake = analytical storage / source of truth
ClickHouse = serving/query layer
Kafka = streaming transport
Spark Structured Streaming = streaming processing
```

Social activity is an interest/activity proxy associated with text-mentioned
places, not measured demand or verified user/post locations. PBPlus is known
source supply, not a complete national inventory. Gold and the existing
ClickHouse loader retain their null, window, deduplication, and replacement rules.

## Runtime and startup

Docker Compose 2.24+ and the existing project Python/Poetry/Java setup are needed.
The `airflow` profile adds PostgreSQL metadata, Airflow API/UI, scheduler with
LocalExecutor, and DAG processor. Images are pinned: Airflow **3.3.2/Python 3.12**
and PostgreSQL **16.13-bookworm**. There is no Celery, Redis, triggerer, Docker
socket inside Airflow, Kubernetes, or cloud prerequisite. PostgreSQL is only
Airflow metadata; analytical data remains in Delta and ClickHouse.

```sh
make airflow-init
make airflow-up
make airflow-status
make airflow-dag-list
make airflow-test
```

`airflow-up` includes initialization, so running it alone also works. Initialization
builds the image from the repository's `pyproject.toml` and `poetry.lock`, prepares
local signing/login files, starts the metadata DB, and runs `airflow db migrate`.
The image has an isolated `/opt/dsp-python` job environment and Java 17, while
Airflow retains its own constrained environment. Business source is mounted
from this checkout; no package copy or dependency list is maintained in DAG code.
The build context includes only dependency manifests; credentials/data are excluded.
Poetry/build tools are absent from the final job environment.

The API/UI is at **http://localhost:8081**, configurable with `AIRFLOW_PORT`.
The default SimpleAuthManager development username is `admin`. Initial password
is generated in ignored `.local/airflow/config/passwords.json`; read it locally
to log in. `AIRFLOW_LOCAL_USERNAME` and optional `AIRFLOW_LOCAL_PASSWORD` customize
the first initialization. Existing password/JWT/Fernet files are retained on
restart. Keep the username consistent with the existing password file; changing
credentials requires deliberately updating that local file. The metadata password
defaults to `airflow-local-only`, a development-only value configurable through
`AIRFLOW_METADATA_PASSWORD` (URL-encode reserved URI characters). No real credentials
are committed. Services publish only the UI on loopback; metadata has no host port.

Airflow readiness uses metadata health, scheduler heartbeat HTTP, API HTTP,
and DAG processor heartbeat checks. `make airflow-logs` shows recent service logs.
Task stdout/stderr and underlying job results are available in the UI and in
`.local/airflow/logs/`. Complete per-run artifact handoff and validation reports
live in `.local/airflow/runs/<hash-of-Airflow-run-ID>/`.

## DAG and task graph

DAG ID: **`dsp391m_market_signal_refresh`**. Default schedule is **manual only**.
Set `DSP_AIRFLOW_SCHEDULE=@daily` in `.env` and recreate through `airflow-up` to opt
into daily UTC refreshes. `catchup=False`; DAGs are initially paused. Manual trigger
unpauses this DAG. No historical crawls are triggered by default. Airflow run IDs
and logical dates never override the analytical date/window.

```text
preflight
  ├─ collect_pbplus
  │    → ingest_pbplus_bronze
  │    → ingest_pbplus_delta
  │    → build_court_supply_gold ─────────────┐
  └─ check_social_delta                     │
       → build_social_activity_gold ────────┤
                                            ↓
                              build_market_signal_snapshot
                                            ↓
                              ensure_clickhouse_ready
                                            ↓
                              migrate_clickhouse
                                            ↓
                              load_market_signals_clickhouse
                                            ↓
                                        validate
```

Every task uses `all_success`. A failed branch blocks market-signal publication.
Only ClickHouse `/ping` readiness retries: **two retries, 15 seconds apart**.
Collection, contract validation, ingestion, Gold, migration, load, and equality
checks have zero Airflow retries; fix deterministic failures and rerun/clear tasks.
The existing PBPlus crawler retains its own transport retry behavior. There is
one active DAG run and one active task at a time, preventing concurrent Spark
jobs and analytical writers within this DAG. Coordinate any independent standalone
writers yourself. Task timeout is 20 minutes; DAG run timeout is one hour.

| Stage | Existing entry point |
| --- | --- |
| Collect/replay | `dsp_collector.jobs.collect_courts` |
| Bronze | `dsp_spark.jobs.batch.ingest_bronze --source pbplus --artifact ...` |
| Court Silver | `dsp_spark.jobs.batch.ingest_pbplus --artifact ...` |
| Check social input | `dsp_spark.jobs.batch.inspect_delta --table social_posts` |
| Social Gold | `dsp_spark.jobs.batch.build_social_activity_daily` |
| Supply Gold + quality | `dsp_spark.jobs.batch.build_court_supply_by_location` |
| Market Gold | `dsp_spark.jobs.batch.build_location_market_signal_snapshot` |
| ClickHouse DDL | `dsp_spark.jobs.batch.load_market_signals_clickhouse --migrate-only` |
| ClickHouse load | `dsp_spark.jobs.batch.load_market_signals_clickhouse` |

Preflight checks local paths, required inputs, writable mounts, and existing
window configuration. `validate` reuses existing Gold reader/ClickHouse client to
compare all served fields to canonical Delta; it neither loads nor calculates metrics.

## Replay, live collection, and manual runs

Default PBPlus mode is **replay**. Set an explicit saved Bronze/crawler envelope
with `DSP_AIRFLOW_PBPLUS_REPLAY`, or provide a per-run `replay_path`. Optional
already enriched envelope: `DSP_AIRFLOW_PBPLUS_GEOGRAPHY` / `geography_path`.
No fixture filename or expected row count is hardcoded in production DAG logic.

Example using the existing saved real artifact:

```sh
make airflow-trigger-market-signals \
  AIRFLOW_CONF='{"pbplus_mode":"replay","replay_path":"apps/collector/.local/bronze/pbplus/c332d92238334ccdb349ab92aab14f2a.json"}'
```

`AIRFLOW_RUN_ID` optionally selects the manual run ID. JSON passes through an
environment value and structured API request, never shell interpolation. In the
UI, unpause the DAG and choose Trigger with the same configuration JSON.

Live PBPlus collection is an explicit alternative:

```sh
make airflow-trigger-market-signals \
  AIRFLOW_CONF='{"pbplus_mode":"live","replay_path":"","geography_path":""}'
```

This calls the existing finite crawler without live geocoding. It writes Bronze/
Silver into a run-specific directory, leaving the collector's standalone status
file alone. A completed collection manifest is reused when that task is cleared;
input/artifact changes fail clearly. Interrupted attempts use fresh directories
to respect immutable collector artifacts. Replay preserves the original source
`run_id`; Delta retains `(run_id, record_id)` idempotency. Downstream tasks use
the manifest's exact paths, never a guessed newest artifact.

Threads is not collected by this DAG. Run Kafka and the existing continuous
Structured Streaming job independently using the existing Make commands. The DAG
reads the currently available social Delta state and does not schedule messages,
manage streaming checkpoints, or change streaming lifecycle behavior.

## Paths and existing configuration

Make passes the repository's absolute path and current host UID to Compose.
The whole checkout is mounted **read-only at that same absolute path**. Two
writable submounts expose the existing `pipelines/spark/.local` (lakehouse/Ivy)
and `.local/airflow/runs`. Logs/config have explicit mounts. Thus saved artifact
paths and Delta URIs retain their host meanings; there is no private container
lakehouse. On Linux, use the owning UID to read existing mode-600 collector inputs.
Direct Compose commands must run from the repository root with appropriate
`AIRFLOW_UID`; Make sets it automatically.

Compose reads optional `.env` and passes existing DSP configuration to jobs.
`DSP_MARKET_SIGNAL_WINDOW_DAYS` defaults to 30. `DSP_MARKET_SIGNAL_END_DATE` remains
optional and is forwarded only when supplied; Airflow does not synthesize it.
Existing lakehouse/table overrides work if the configured paths are mounted.
Paths outside this checkout need an explicit Compose volume override; preflight
fails missing/unwritable paths instead of creating a hidden replacement lakehouse.
This local phase rejects S3A paths used by this DAG. ClickHouse uses the Compose service hostname
`clickhouse` and internal HTTP port 8123, with existing configured database/user/
password. Host standalone commands retain their existing localhost/published port.

## Smoke and shutdown

Use the saved artifact; no Threads or PBPlus recrawl is required:

```sh
DSP_AIRFLOW_PBPLUS_REPLAY=apps/collector/.local/bronze/pbplus/c332d92238334ccdb349ab92aab14f2a.json make airflow-smoke
make airflow-down
```

Smoke starts/init-tests the real stack, checks API/metadata/imports/DAG registration,
triggers two real scheduler runs, validates all task dependency timestamps and
Delta-to-ClickHouse equality, and compares analytical content across reruns
excluding generation metadata. It then triggers a missing-input failure and
checks all downstream tasks are `upstream_failed` and serving is unchanged.
Evidence is saved in ignored `.local/airflow/smoke.json`. This refreshes local
Gold through existing jobs and the real serving table; it does not alter Silver
business rules or crawl during replay. `airflow-down` stops only Airflow/metadata;
it retains metadata/logs/artifacts, Kafka, ClickHouse, and lakehouse data.

See [executed local evidence](local-smoke.md) once verification is recorded.
Live collection, distributed executors, cloud deployment, S3 writes/reads, and
remote Databricks execution are not claimed verified. S3A configuration and
offline Databricks validation remain prepared capabilities. Airflow adds no
demand/gap/ranking/recommendation model or frontend/backend redesign.
