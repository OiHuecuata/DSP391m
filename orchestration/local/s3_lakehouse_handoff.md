# HANDOFF CONTEXT — DSP391m S3 LAKEHOUSE IMPLEMENTATION

Cập nhật UTC 2026-10-28T10:53:02.897133+00:00. Repository /home/tuancui/DSP391m; branch `model`; HEAD `3c961cfc66dba3cdee4507fb246007b6f30f56a9`; worktree modified/untracked, chưa commit/push. Không sửa main, không ML, không sửa lỗi ClickHouse staging, không xóa local/S3 bootstrap/checkpoints.

Implementation thực tế: SDK v2 profile S3A; fsync durable outbox tự enqueue cả ba collectors; worker opt-in báo PARTIAL/pending trước Spark; coordinated managed Delta writer + verified committed receipts; durable downstream JSON notifications/polling; historical/catch-up pinned bootstrap với overlapping-ID reconciliation và restart plan; pinned full rebuild 13 Silver + 10 Gold; immutable generation manifests, read-back/quality/reconciliation gates, CAS watermark, rollback/paused/resume; finite CLI/watch/guard Root; scripts/tests/runbook. Không claim exactly-once hay incremental computation.

S3 bootstrap chỉ đọc: `s3a://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse`.
Synthetic approved prefix: `s3a://dsp391m-data-lake/staging/s3-lakehouse-e2e-20261028`.
Real authoritative writable namespace CHƯA tạo/chưa bootstrap/chưa cutover.
Runtime Spark 4.0.2, Delta 4.0.1, Hadoop/connector 3.4.1, SDK bundle 2.24.6, Java17; dsp391m-spark credential_process temporary credentials, region ap-southeast-2. Root chỉ finite supervised, watch Root bị từ chối.

State/evidence:

- [44/44 S3 tables PASS](../../.local/s3-lakehouse/snapshot-verification.json): direct Spark read, pinned versions, full schema/count/content digests khớp immutable local snapshot.
- Historical local full rebuild `20261028T090000Z-5f56358d0efa` VALIDATED, 7.731 input rows/six grains, zero unexplained. [Manifest](../../.local/s3-lakehouse/historical-seeded-20261028/report.json); 13 Silver +10 Gold schemas/counts/versions/digests đầy đủ. Historical seed preserves dated supply/caches bằng pinned previous_versions; không export real data lên cloud.
- Local three-collector synthetic E2E PASS, hai generations, replay no duplicates/commits, new data trigger, read-back, rollback paused. [Report](../../.local/s3-lakehouse/local-smoke-20261028/smoke-report.json).
- S3 synthetic E2E: **PASS**. Current sau rollback `20261028T090235Z-5f472f1a9d62`, paused=true. Generation2 `20261028T094921Z-ae06443330eb`. [Report](../../.local/s3-lakehouse/cloud-smoke-report.json); sau test current pointer rollback về generation1 và paused.
- Tests: collector 305 (304 PASS/1 SKIP), new managed lakehouse10 PASS, worker8 PASS, orchestration17 PASS, intelligence21 PASS, contracts14 PASS; lint/format/poetry lock checks PASS. Logs copied vào .local/s3-lakehouse/logs. Source policy test clock fixed, không nới expiration validation.

Modified files:

```text
 M Makefile
 M apps/collector/src/dsp_collector/runtime/artifacts.py
 M apps/collector/test/test_bronze_observations.py
 M orchestration/local/collection_worker.py
 M pipelines/spark/src/dsp_spark/io/s3.py
 M pipelines/spark/src/dsp_spark/jobs/batch/build_intelligence.py
 M pipelines/spark/src/dsp_spark/jobs/batch/ingest_bronze.py
 M pipelines/spark/src/dsp_spark/processing/clean/bronze.py
 M pipelines/spark/src/dsp_spark/runtime/config.py
 M pipelines/spark/src/dsp_spark/runtime/spark.py
 M poetry.lock
 M pyproject.toml
 M tests/integration/test_collection_worker.py
?? apps/collector/src/dsp_collector/runtime/outbox.py
?? apps/collector/test/test_s3_outbox.py
?? configs/collection/s3_runtime.env.example
?? configs/collection/s3_synthetic_policy.example.json
?? orchestration/local/s3_lakehouse.md
?? orchestration/local/s3_lakehouse_handoff.md
?? orchestration/local/s3_lakehouse_implementation_report.md
?? pipelines/spark/src/dsp_spark/io/lakehouse_control.py
?? pipelines/spark/src/dsp_spark/jobs/batch/lakehouse_generation.py
?? pipelines/spark/src/dsp_spark/jobs/batch/s3_lakehouse.py
?? pipelines/spark/src/dsp_spark/jobs/batch/verify_s3_snapshot.py
?? pipelines/spark/test/test_s3_lakehouse.py
?? pipelines/spark/test/test_s3_lakehouse_delta.py
?? scripts/smoke_s3_lakehouse.py
```

Production blockers: reviewed real permissions/retention registry; approved real-data migration/cutover with quiesced local writers/gap run reconciliation; non-Root runtime IAM/profile + durable host/supervisor. Phê duyệt hiện có chỉ bao phủ synthetic prefix ở trên, không phải real migration/production writer changes. Không xin lại phê duyệt đã cấp cho synthetic.

Operational limits: local filesystem durable queue (chưa multi-host); cooperative non-expiring S3 conditional lock; bounded 250k Bronze rows/table; full rebuild fallback; backend receipt terminal sync chưa có; receipt Kafka producer chưa wire; root watch forbidden. ClickHouse serving mismatch còn nguyên ngoài scope. No production services started. Mọi consumer resolve control/current.json → control/generations/<id>.json, không lấy latest từng table; staged quality manifest không phải con trỏ promoted.

Commands tiếp theo nằm trong [runbook](s3_lakehouse.md); [báo cáo A–J](s3_lakehouse_implementation_report.md) chứa đủ 44 bảng và 23 output counts/paths. Read-only monitor:

```bash
export PYTHONPATH=apps/collector/src:pipelines/spark/src
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse status --base s3a://dsp391m-data-lake/staging/s3-lakehouse-e2e-20261028 --profile dsp391m-spark
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse verify --base s3a://dsp391m-data-lake/staging/s3-lakehouse-e2e-20261028 --profile dsp391m-spark
```

Chưa chạy mutation real data cho tới policy/approval/runtime gates được giải quyết. Giữ các synthetic generations để review, không tự cleanup hay VACUUM. Không chạy production ClickHouse publication hoặc ML ở bước kế tiếp nếu chưa có yêu cầu mới.
