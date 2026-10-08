# DSP391m

For paced live accumulation, dataset metrics, Parquet EDA access and backups, see
[live collection instructions](orchestration/local/live_collection.md).

Run the supported local data pipeline with `make pipeline-e2e` in replay or live
mode. See [one-command setup, configuration, and verification](orchestration/local/README.md).
`make pipeline-e2e-test` exercises real Kafka/Spark/Delta/ClickHouse with isolated
synthetic replay fixtures, including rerun, empty input, and failure cases.

For local Delta Gold → ClickHouse → Go analytics API serving, see
[ClickHouse instructions](infra/clickhouse/README.md) and
[executed smoke evidence](infra/clickhouse/local-smoke.md).

For local batch orchestration of the existing jobs, see
[Airflow setup and DAG instructions](orchestration/airflow/README.md).

### Local Data Operations

Control recurring Threads/YouTube collection and observe Delta/ClickHouse at `/data-operations`.
See [operation, replay, and bounded live verification](orchestration/local/data_operations.md).
