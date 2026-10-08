# Live accumulation and EDA

The canonical data remains the existing local Delta lakehouse. Live collection
creates immutable run-scoped raw and normalized artifacts, archives each raw
envelope in Delta Bronze, publishes Threads through Kafka/AvailableNow streaming,
and merges source observations into Silver. Gold and ClickHouse are refreshed
through the existing jobs and validated loader. No S3 or Databricks is required.

```bash
make collect-live-eda LIVE_EDA_CYCLES=2
make report-live-eda
```

Prerequisites are the same as `pipeline-e2e`: Poetry/Python 3.12, Java 17/21,
Chromium, Docker Kafka and ClickHouse. Set `JAVA_HOME` or Make `DSP_JAVA_HOME`.
Infrastructure startup and topic/migration readiness reuse the existing helpers.
Settings live in `configs/collection/live_eda.v1.json`; override its path with
`LIVE_EDA_CONFIG`. Output defaults to ignored `.local/live-eda`, configurable with
`LIVE_EDA_OUTPUT`. Use a **new output directory** for a new scheduled campaign;
completed cycles in an existing directory are reused rather than recrawled.

Threads query seeds expand through the existing canonical country/city plan.
The scale configuration removes the former three-post/query sample cap, retains
native search payloads, globally deduplicates source IDs within a run, and caps
detail enrichment. Public SSR page-info and stopping reasons are recorded. No
private API pagination or authentication bypass is used. Current public search
exposes null cursors/no next page on inspected results; requested recent sorting
can be removed by the source redirect. Extra query seeds do not guarantee extra
results. Detail skipping is recorded as unrequested, not a failed enrichment.

PBPlus traverses unique detail URLs from the public listing. No pagination links
were found in the inspected listing. This measures the discoverable directory,
not complete national inventory. Sources use serial requests, configurable delay,
timeouts and bounded retry/backoff. HTTP 401/403/429 stop collection without
retries/bypass. If usable records exist, partial output is preserved; source
failures remain in the cycle report. A source blocked in one cycle is not retried
in the next cycle of that campaign.

Each source result has exact Bronze/Silver paths, run ID, status, counts and hashes
in a manifest. Processing records source-identity changes, business-field changes,
new observations, stage durations and Kafka/stream metrics. Repeated identity with
a **new run ID** is valid history. Replaying the **same run ID/event ID** inserts no
duplicate observations. Gold selects latest logical entities; it does not delete
Silver history. Social activity remains a text-mentioned proxy, not measured demand.

For recovery, reprocess saved live artifacts without crawling:

```bash
PYTHONPATH=pipelines/spark/src:apps/collector/src poetry run python \
  scripts/collect_live_dataset.py --cycles 2 --ingest-only
```

Use the normal Java/PySpark environment for that direct command. Reports and raw
files remain durable under `.local/live-eda`; `/tmp` is not the dataset's only copy.
The disk guard checks available space before new collection and between cycles,
with a configurable minimum (2 GiB default). Source limits bound each collection.

## EDA access

Read Delta directly in the repository's Spark environment:

```python
from dsp_spark.runtime.config import SparkSettings
from dsp_spark.runtime.spark import create_spark
from dsp_spark.io.delta import read_delta

settings = SparkSettings.from_env()
spark = create_spark(settings)
social = read_delta(spark, settings.paths.social_posts)
courts = read_delta(spark, settings.paths.courts)
activity = read_delta(spark, settings.paths.social_activity_daily)
supply = read_delta(spark, settings.paths.court_supply_by_location)
snapshot = read_delta(spark, settings.paths.location_market_signal_snapshot)
```

`make report-live-eda` measures Delta rows, active Parquet bytes, all retained
filesystem bytes/files, duplicate keys, latest-entity null rates, publication-date
coverage and explicit >90-day staleness. It writes `dataset-report.json` and
versioned Parquet snapshots under `.local/live-eda/eda-parquet/`. Silver exports
select this campaign's live run IDs; Gold exports reflect the complete canonical
lakehouse. Parquet exports are notebook conveniences, not canonical storage.

## Scheduling and backup

Use the existing Airflow refresh DAG for finite transformations where configured.
Adding a separate recurring live-collection DAG is deferred. A scheduler can run
the exact Make command above in a fresh timestamped output directory. Initially
prefer two Threads scans/day and one PBPlus scan/day or less; review observed
new-ID/duplicate rates before increasing frequency. The short verification campaign
does not establish daily growth or source update frequency.

Run one coordinated writer at a time. After all queries/jobs finish, copy these
directories together to a new ignored local backup directory:

- `apps/collector/.local` and the campaign's raw/normalized artifacts
- `.local/pipeline` and campaign manifests/reports
- `pipelines/spark/.local/lakehouse`, including `_delta_log` and checkpoints

Do not copy only current Parquet files; Delta requires its log and referenced
history. This campaign's before/after backups are under `.local/backups/live-eda/`.
Local copies protect against accidental processing changes; an external device is
needed for protection against failure of the same disk. Backups/data stay out of Git.
