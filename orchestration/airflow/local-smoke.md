# Local Airflow verification

Executed October 7, 2026. The real Airflow scheduler ran existing DSP391m batch
jobs against the existing local lakehouse and ClickHouse. PBPlus used saved real
Bronze `apps/collector/.local/bronze/pbplus/c332d92238334ccdb349ab92aab14f2a.json`
(337 observations). Neither PBPlus nor Threads was recrawled. Kafka and continuous
Structured Streaming were not started/stopped or managed by this phase.

## Runtime and actual runs

Airflow 3.3.2/Python 3.12 with LocalExecutor, PostgreSQL 16.13 metadata, API/UI,
scheduler, and DAG processor started successfully. `airflow db migrate` exited 0.
API health reported metadata, scheduler, and DAG processor healthy; the triggerer
is absent by design. `airflow dags list` returned
`dsp391m_market_signal_refresh`. Import errors were empty. Eight focused tests
passed using the actual pinned Airflow runtime.

Actual manually triggered scheduler runs:

- `smoke__20261007T082601109351__first`: success.
- `smoke__20261007T082601109351__rerun`: success.
- `smoke__20261007T082601109351__missing_input`: intentionally failed; failure gate passed.

| Task | First | Rerun | Missing input |
| --- | --- | --- | --- |
| preflight | success | success | failed |
| collect_pbplus | success | success | upstream_failed |
| ingest_pbplus_bronze | success | success | upstream_failed |
| ingest_pbplus_delta | success | success | upstream_failed |
| build_court_supply_gold | success | success | upstream_failed |
| check_social_delta | success | success | upstream_failed |
| build_social_activity_gold | success | success | upstream_failed |
| build_market_signal_snapshot | success | success | upstream_failed |
| ensure_clickhouse_ready | success | success | upstream_failed |
| migrate_clickhouse | success | success | upstream_failed |
| load_market_signals_clickhouse | success | success | upstream_failed |
| validate | success | success | upstream_failed |

Every successful dependency's parent end time preceded its child start time.
Both branches completed before the market snapshot. The failed preflight used
an absent saved artifact; all eleven downstream tasks became `upstream_failed`.
Full SQL rows, including generation timestamps, remained unchanged across this
intentional failure. Validation errors were not retried.

## Gold and serving evidence

Both successful runs produced/read **8 Delta Gold rows**, loaded **8 ClickHouse
rows**, and compared **all 23 fields** exactly. Source:
`pipelines/spark/.local/lakehouse/gold/location_market_signal_snapshot`.
Snapshot stayed `2026-08-13`; window stayed `2026-07-15`–`2026-08-13`, 30 days.
Airflow's October execution time did not substitute a business date.

Both actual loader logs reported:

```json
{"status":"success","loaded_rows":8,"table":"dsp391m.location_market_signal_snapshot","reload":"atomic_full_replacement"}
```

Executed serving SQL:

```sql
SELECT count() AS rows,
       uniqExact(tuple(snapshot_date, location_id)) AS identities,
       countIf(has_social_signal AND has_supply_signal) AS both_signal,
       countIf(NOT has_social_signal AND has_supply_signal) AS supply_only,
       countIf(has_social_signal AND NOT has_supply_signal) AS social_only
FROM dsp391m.location_market_signal_snapshot
SETTINGS output_format_json_quote_64bit_integers=0
FORMAT JSONEachRow
```

Result: `rows=8, identities=8, both_signal=2, supply_only=6, social_only=0`.
The analytical content hash, excluding only Gold generation metadata, is identical
for both runs: `3fd48d02811f48bedc3ac700eda3874013ba74da903d466ddb680557265ce8d0`.
No duplicate serving identity was introduced. Replay retained the original source
run identity and reused existing Delta observation merge semantics.

| Location | Posts | Engagement | Venues | Known courts |
| --- | --- | --- | --- | --- |
| Hà Nội | 0 | 0 | 126 | 1524 |
| TP. Hồ Chí Minh | 1 | 7 | 104 | 548 |
| Nha Trang | 1 | 18 | 10 | 45 |

Hà Nội retains 107 known-count and 19 unknown-count venues, coverage
`0.8492063492063492`. Nha Trang retains activity/venue `0.1` and engagement/venue
`1.8`. Generation timestamps change on rebuild; analytical values do not.
These are source observations and mentioned-place activity, not demand or a ranking.

## Commands and regression checks

```sh
DSP_AIRFLOW_PBPLUS_REPLAY=apps/collector/.local/bronze/pbplus/c332d92238334ccdb349ab92aab14f2a.json make airflow-smoke
make airflow-status
make airflow-dag-list
make airflow-test
BLACK_NUM_WORKERS=1 make format lint
make backend-test backend-vet
make spark-test
PYTHONPATH=apps/collector/src poetry run python -m unittest discover -s apps/collector/test -p 'test_*.py'
make spark-gold-market-signal
make load-market-signals-clickhouse
make smoke-clickhouse
git diff --check
```

Results: eight Airflow tests passed; 106 Spark tests passed; collector suite ran
203 tests with one skipped and no failures; backend tests/vet passed; formatting,
Ruff, and whitespace checks passed. Existing host Gold and loader commands passed
after the scheduler runs, producing the same eight-row analytical state. The
existing host ClickHouse smoke, including actual Go/API reads and isolated null
fixtures, also passed. Host rebuilds update generation metadata after the recorded
Airflow runs. Existing Spark tests emitted socket resource warnings but completed.

Full scheduler-run/task-state/source/SQL evidence is in ignored
`.local/airflow/smoke.json`. Per-run collection/validation reports and task logs
remain in `.local/airflow/runs/` and `.local/airflow/logs/`. Standalone serving/API
evidence is in `.local/clickhouse-smoke.json`. Temporary execution logs are under
`/tmp/dsp391m-airflow-*.log`.

## Scope

Airflow = orchestration; Spark = processing; Delta = analytical source of truth;
ClickHouse = serving/query layer. Kafka + Structured Streaming remains an
independent streaming path, with no per-message tasks or streaming lifecycle
conversion. No processing/normalization/Gold/loader/backend business logic was
changed. Live PBPlus collection under Airflow was not exercised; only offline
replay was verified. Distributed execution, cross-writer coordination, production
authentication, AWS S3 write/read, and remote Databricks execution remain unverified.
S3A configuration and offline Databricks preparation remain as before.

## Git state

Initial worktree and staged diff were clean at `a317575` (`Init Clickhouse Serving
Layer`). The final index entry listing and staged diff are byte-identical to the
initial snapshots. No staging/restore/reset/checkout/stash/commit command was run,
and no concurrent external commit appeared. Changes are unstaged source/docs
files; generated data, keys, logs, and smoke artifacts remain ignored.
