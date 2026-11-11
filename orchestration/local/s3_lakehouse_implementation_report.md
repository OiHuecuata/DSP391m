# DSP391m — báo cáo triển khai S3 lakehouse

Cập nhật UTC: 2026-10-28T10:53:02.897133+00:00. Đây là implementation trên branch model, không triển khai ML. Các kết quả local và cloud tách rõ theo evidence thực tế.

## A. Branch status và source changes

Branch `model`, HEAD `3c961cfc66dba3cdee4507fb246007b6f30f56a9`. Không commit/push/merge, không đổi main. Worktree ban đầu sạch. Trạng thái cuối (M = sửa; ?? = file mới):

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

Nhóm sửa chính: AWS profile S3A; durable collector outbox + worker opt-in; S3 conditional single writer; pinned historical/catch-up bootstrap; immutable generation/full rebuild/rollback; CLI + smoke + runbook + tests; dependency boto3/lockfile. Không sửa `ml/`, không cài MLflow và không sửa lỗi ClickHouse staging mismatch.

## B. S3 integration và 44-table verification

Spark 4.0.2, Delta 4.0.1, Hadoop 3.4.1, hadoop-aws 3.4.1, AWS SDK v2 2.24.6, Scala 2.13, Java 17. Maven/Ivy resolve đúng dependencies; S3 read/write chạy thực tế, không tự đưa JAR sai version. Profile dsp391m-spark dùng credential_process lấy temporary credentials từ bootstrap login; SDK v2 ProfileCredentialsProvider trên driver/executor. Region ap-southeast-2. Không hardcode hoặc ghi secrets vào repository. Boto3 control plane được khóa cùng Poetry.

Snapshot chỉ đọc: `s3a://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse`.
Reference local: `.local/s3-snapshots/20261028T064025Z/lakehouse`.
Lượt toàn bộ bắt đầu 2026-10-28T08:25:38.298754+00:00, kết thúc 2026-10-28T08:53:36.855240+00:00: **PASS, 44/44**. Schema đầy đủ, S3 paths, count local/remote, versions, duplicate-aware SHA256 digests và error types nằm trong [JSON đầy đủ](../../.local/s3-lakehouse/snapshot-verification.json). Không ghi vào bootstrap. S3 GET/LIST requests có phát sinh phí đọc thông thường.

| Table trong snapshot | Version | Rows S3 = local | Content/schema/count/version |
| --- | --- | --- | --- |
| bronze/collection_runs | 175 | 90 | PASS |
| bronze/court_observations | 5 | 1687 | PASS |
| bronze/engagement_snapshots | 35 | 1932 | PASS |
| bronze/pbplus | 5 | 6 | PASS |
| bronze/social_comments | 11 | 85 | PASS |
| bronze/social_posts | 35 | 1847 | PASS |
| bronze/source_changes | 36 | 2090 | PASS |
| bronze/threads | 64 | 65 | PASS |
| bronze/youtube | 18 | 19 | PASS |
| gold/court_supply_by_admin_version_v2 | 35 | 88 | PASS |
| gold/court_supply_by_location | 92 | 8 | PASS |
| gold/geocoded_csv_locations | 1 | 333 | PASS |
| gold/location_market_research_monthly_v1 | 35 | 100 | PASS |
| gold/location_market_signal_snapshot | 95 | 8 | PASS |
| gold/location_term_usage_monthly_v1 | 35 | 108 | PASS |
| gold/national_location_coverage_v1 | 35 | 1676 | PASS |
| gold/pbplus_map_link_evidence | 0 | 5 | PASS |
| gold/pbplus_venue_locations | 2 | 337 | PASS |
| gold/social_activity_by_admin_version_daily | 35 | 215 | PASS |
| gold/social_activity_daily | 92 | 47 | PASS |
| gold/social_comment_analytics_daily_v1 | 35 | 12 | PASS |
| gold/social_engagement_daily_v1 | 35 | 219 | PASS |
| gold/social_intent_daily_v2 | 35 | 367 | PASS |
| gold/social_source_activity_daily | 86 | 119 | PASS |
| gold/venue_community_feedback_weekly_v1 | 35 | 26 | PASS |
| gold/venue_discovery_trends_v1 | 35 | 24 | PASS |
| quality/intelligence_manifest | 35 | 1 | PASS |
| quality/unresolved_court_locations | 92 | 52 | PASS |
| silver/courts | 5 | 1687 | PASS |
| silver/intelligence/bridge_admin_crosswalk | 35 | 63 | PASS |
| silver/intelligence/court_venues | 35 | 373 | PASS |
| silver/intelligence/dim_admin_unit | 35 | 100 | PASS |
| silver/intelligence/dim_location_alias | 35 | 170 | PASS |
| silver/intelligence/geocoding_attempts | 35 | 1781 | PASS |
| silver/intelligence/social_comments | 35 | 75 | PASS |
| silver/intelligence/social_engagement_snapshots | 35 | 1932 | PASS |
| silver/intelligence/social_geo_mentions | 35 | 450 | PASS |
| silver/intelligence/social_intent_evidence | 35 | 949 | PASS |
| silver/intelligence/social_posts | 35 | 649 | PASS |
| silver/intelligence/venue_candidates | 35 | 1781 | PASS |
| silver/intelligence/venue_evidence | 35 | 1781 | PASS |
| silver/intelligence/venue_source_links | 35 | 373 | PASS |
| silver/social_comments | 6 | 83 | PASS |
| silver/social_posts | 33 | 1554 | PASS |

Giới hạn chứng minh credentials: Spark local[2] dùng cùng máy/profile; chưa kiểm thử executor nhiều host hay refresh qua một session đã thực sự hết hạn. Không tuyên bố vận hành unattended bằng Root.

## C. Continuous ingestion và bootstrap

Hook artifacts của PBPlus/Threads/YouTube tự đưa source envelope vào durable queue khi bật DSP_S3_OUTBOX_DIR. Worker opt-in `DSP_S3_INGESTION_MODE=outbox` ngừng trước legacy Spark/Kafka/ClickHouse và báo PARTIAL/pending. Giữ source artifacts, copy immutable envelope và fsync trạng thái. Managed writer kiểm tra schema/permissions/retention, MERGE bằng stable IDs, read-back các records, rồi mới ghi receipt committed. Retry/backoff, failed state và notifications retry độc lập; notification failure không đảo Bronze đã commit. Downstream notifications mặc định là durable S3 JSON, scheduler polling versions trực tiếp; Kafka producer cho receipt chưa bật.

S3 writes phối hợp bằng conditional writer.lock không TTL/không auto-steal. Mọi writer vào namespace phải tuân cùng control root; không cho legacy independent Spark writers ghi vào đó. CAS current pointer và immutable generation manifests. Không tuyên bố exactly-once end-to-end.

Bootstrap được triển khai cho sáu Bronze record tables + ba source-envelope tables: pin versions từ Phase 1, pin catch-up vector upfront trong bootstrap-plan.json, kiểm tra overlap identity/business values, reconcile count/digest từng bảng, chỉ publish bootstrap marker sau cả chín bảng PASS. Retry interrupted bootstrap giữ pinned plan, không trôi sang catch-up version mới. Marker chứa pinned versions của 23 snapshot outputs để seed dated supply history/caches qua business rules cũ. Không copy live Delta files.

**Chưa bootstrap dữ liệu thật hoặc cutover production.** Synthetic cloud chỉ ở prefix đã được người dùng phê duyệt: `s3a://dsp391m-data-lake/staging/s3-lakehouse-e2e-20261028`. Real migration còn cần policy/approval/runtime phù hợp.

## D. Bronze → 13 Silver

Full historical rebuild local generation `20261028T090000Z-5f56358d0efa`, status `VALIDATED`. Đọc toàn bộ Bronze pinned vector `{"bronze_collection_runs": 175, "bronze_court_observations": 5, "bronze_engagement_snapshots": 35, "bronze_social_comments": 11, "bronze_social_posts": 35, "bronze_source_changes": 36}`. Source input 7.731 rows thuộc sáu grains, không phải unique market entities. Không bỏ business validations. Dùng lại existing eligibility/retention/tombstone/canonical identity/venue/evidence/geography/intent/reference transforms. Manifest đầy đủ [historical local](../../.local/s3-lakehouse/historical-seeded-20261028/report.json).

| Dataset | Historical rebuild local: rows | Synthetic S3 generation 2: rows | Relative output path |
| --- | --- | --- | --- |
| silver_venue_candidates | 1781 | 2 | silver/intelligence/venue_candidates |
| silver_court_venues | 373 | 2 | silver/intelligence/court_venues |
| silver_venue_evidence | 1781 | 2 | silver/intelligence/venue_evidence |
| silver_venue_source_links | 373 | 2 | silver/intelligence/venue_source_links |
| silver_social_posts | 649 | 4 | silver/intelligence/social_posts |
| silver_social_comments | 75 | 2 | silver/intelligence/social_comments |
| silver_social_engagement_snapshots | 1932 | 6 | silver/intelligence/social_engagement_snapshots |
| silver_social_geo_mentions | 450 | 7 | silver/intelligence/social_geo_mentions |
| silver_geocoding_attempts | 1781 | 2 | silver/intelligence/geocoding_attempts |
| silver_social_intent_evidence | 949 | 6 | silver/intelligence/social_intent_evidence |
| dim_admin_unit | 100 | 100 | silver/intelligence/dim_admin_unit |
| bridge_admin_crosswalk | 63 | 63 | silver/intelligence/bridge_admin_crosswalk |
| dim_location_alias | 170 | 170 | silver/intelligence/dim_location_alias |

Schemas từng bảng, paths tuyệt đối, output versions, profiles và digests nằm trong các JSON manifests. Hai Gold tables comment_analytics/community_feedback có 0 rows trong synthetic staging do fixture không có published_at/venue link đủ điều kiện; schema-correct Delta outputs vẫn được tạo/đọc lại, không thêm dummy rows. Không dùng synthetic counts để đại diện thị trường. Seed historical chỉ kiểm chứng local; dữ liệu thật chưa được đưa vào writable S3 namespace.

| Dataset | Input | Eligible | Excluded | Consumed/handled | Unexplained |
| --- | --- | --- | --- | --- | --- |
| bronze_collection_runs | 90 | 90 | 0 | 90 | 0 |
| bronze_court_observations | 1687 | 1687 | 0 | 1687 | 0 |
| bronze_engagement_snapshots | 1932 | 1932 | 0 | 1932 | 0 |
| bronze_social_comments | 85 | 85 | 0 | 85 | 0 |
| bronze_social_posts | 1847 | 1847 | 0 | 1847 | 0 |
| bronze_source_changes | 2090 | 2090 | 0 | 2090 | 0 |

Handling: court/posts/comments được trace qua lineage; engagement snapshots trace qua lineage hoặc documented orphan-retained-in-Bronze; run metadata dùng coverage-control; source_changes dùng lifecycle-control ngay cả khi không có surviving metric cite trực tiếp. Không ép số Silver rows bằng Bronze rows; zero unexplained eligible loss là gate bắt buộc.

## E. Silver → 10 Gold và generations

Gold dùng đúng bộ Silver đã validated trong cùng processing generation của existing bounded adapter. Tất cả Silver được đọc lại xác minh trước ghi Gold; toàn bộ 23 outputs được đọc lại và validate trước pointer publication. Gold giữ temporal metrics, national distinct totals, multi-label overlap, NULL semantics, negative metric corrections và Decimal money schemas theo contracts hiện có.

| Dataset | Historical rebuild local: rows | Synthetic S3 generation 2: rows | Relative output path |
| --- | --- | --- | --- |
| court_supply_by_admin_version_v2 | 88 | 3 | gold/court_supply_by_admin_version_v2 |
| social_engagement_daily_v1 | 219 | 5 | gold/social_engagement_daily_v1 |
| social_intent_daily_v2 | 367 | 5 | gold/social_intent_daily_v2 |
| social_comment_analytics_daily_v1 | 12 | 0 | gold/social_comment_analytics_daily_v1 |
| social_activity_by_admin_version_daily | 215 | 5 | gold/social_activity_by_admin_version_daily |
| national_location_coverage_v1 | 1676 | 294 | gold/national_location_coverage_v1 |
| venue_community_feedback_weekly_v1 | 26 | 0 | gold/venue_community_feedback_weekly_v1 |
| location_term_usage_monthly_v1 | 108 | 4 | gold/location_term_usage_monthly_v1 |
| venue_discovery_trends_v1 | 24 | 3 | gold/venue_discovery_trends_v1 |
| location_market_research_monthly_v1 | 100 | 4 | gold/location_market_research_monthly_v1 |

S3 E2E: **PASS**. Current pointer sau rollback: `20261028T090235Z-5f472f1a9d62`, paused=true. Latest synthetic generation 2: `20261028T094921Z-ae06443330eb`. Base: `s3a://dsp391m-data-lake/staging/s3-lakehouse-e2e-20261028`; mỗi path relative trong bảng nằm dưới `/generations/<generation_id>/`. [S3 report đầy đủ](../../.local/s3-lakehouse/cloud-smoke-report.json) chứa both generations, paths, schemas, counts, digests, quality, reconciliation, watermark và rollback pointer đã hoàn tất.

Generation ID không phải Delta version. Metadata gồm six-table Bronze version vector, processing version/code digest, reference/policy/contract fingerprints, 23 schemas/versions/counts/digests, quality/reconciliation, as-of/timestamps/paths và rollback target. Stage chưa complete không được current.json chọn. Failed generation giữ current cũ. Rollback verify retained data rồi CAS pointer, đặt paused để watcher không promote ngược ngay.

## F. Automation và recovery

`s3_lakehouse watch --interval 300` là micro-batch implementation; min 30s, default 300s. Poll Bronze versions, capture pinned vector dưới writer lock, **FULL_REBUILD** khi đổi inputs/code/reference/contracts/policies/eligibility/retention boundaries; no-op khi pinned input và outputs vẫn intact. Không gọi đây là incremental computation. Processing watermark chỉ advance sau manifest validated và CAS current pointer. Pending outbox không bị mất khi S3 lỗi; transient processing exceptions ghi safe failure report và retry ở poll sau. JVM crash cần supervisor restart; S3 orphan lock cần supervised recovery sau khi xác minh owner đã chết.

Không bật production watch/supervisor hay đổi collector environment. Root bị CLI chặn chạy watch; hiện chỉ dùng finite supervised development được phê duyệt. Chưa có IAM runtime identity nên không tuyên bố crawler chạy unattended production an toàn. Backend dashboard chưa đồng bộ terminal committed từ receipt; xem outbox/report/control markers để biết final ingestion status. Không publication ClickHouse; adapter failure đã mô phỏng local không làm đổi pointer hoặc mất Gold.

## G. Tests và bằng chứng

| Test | Actual status | Phạm vi | Evidence |
| --- | --- | --- | --- |
| Historical S3 read | 44/44 PASS | Spark trực tiếp S3; schema/version/count và toàn bộ SHA256 multiset khớp | [snapshot-verification.json](../../.local/s3-lakehouse/snapshot-verification.json) |
| Managed writer / generation / recovery | 10 tests PASS | 8 control tests + 2 real Delta integration: pinned bootstrap interrupted/retry, invalid contract, duplicate, failed staging, mocked ClickHouse outage, rollback | [logs/dsp-s3-local-final.log](../../.local/s3-lakehouse/logs/dsp-s3-local-final.log) |
| Collector outbox | 5 tests PASS (nằm trong collector suite) | Cả ba source hooks; S3-like outage/backoff; notification failure; stable identities; false receipt rejection | [logs/dsp-s3-outbox-tests.log](../../.local/s3-lakehouse/logs/dsp-s3-outbox-tests.log) |
| Collector regression | 305 tests: 304 PASS, 1 SKIP | Một test optional PBPlus local sample không có fixture trên máy nên skip; clock của retention fixture được cố định, giữ nguyên production validation | [logs/dsp-s3-collector-regression.log](../../.local/s3-lakehouse/logs/dsp-s3-collector-regression.log) |
| Worker integration | 8 tests PASS | Outbox mode báo PARTIAL/pending, không khởi chạy Spark/crawler khi replay; default behavior vẫn pass | [logs/dsp-s3-worker-regression.log](../../.local/s3-lakehouse/logs/dsp-s3-worker-regression.log) |
| Local orchestration | 17 tests PASS | Existing scheduler/runtime boundaries | [logs/dsp-s3-orchestration-regression.log](../../.local/s3-lakehouse/logs/dsp-s3-orchestration-regression.log) |
| Business intelligence | 21 tests PASS | Existing business rules: expiry/tombstones/corrections/geo/null/multilabel semantics | [logs/dsp-s3-intelligence-regression.log](../../.local/s3-lakehouse/logs/dsp-s3-intelligence-regression.log) |
| Contracts/runtime | 14 tests PASS | Schemas, package compatibility and serialized validation | [logs/dsp-s3-contract-regression.log](../../.local/s3-lakehouse/logs/dsp-s3-contract-regression.log) |
| Full historical rebuild | VALIDATED: 13 Silver + 10 Gold | 7.731 observations thuộc 6 grains; zero unexplained; local outputs only | [historical-seeded-20261028/report.json](../../.local/s3-lakehouse/historical-seeded-20261028/report.json) |
| Three collectors local E2E | PASS | Auto enqueue/commit, retry no extra commits, new generation/read-back, rollback/paused | [local-smoke-20261028/smoke-report.json](../../.local/s3-lakehouse/local-smoke-20261028/smoke-report.json) |
| Synthetic S3 E2E | PASS | Approved isolated staging; Bronze + two generations + replay/read-back/rollback | [cloud-smoke-report.json](../../.local/s3-lakehouse/cloud-smoke-report.json) |
| Formatting/dependencies | PASS | Ruff scoped; Black scoped; poetry check --lock; git diff --check | [logs](../../.local/s3-lakehouse/logs) |

Commands đã chạy:

```bash
export PYTHONPATH=apps/collector/src:pipelines/spark/src
export SPARK_LOCAL_IP=127.0.0.1
.venv/bin/python -B -m dsp_spark.jobs.batch.verify_s3_snapshot --local-root .local/s3-snapshots/20261028T064025Z/lakehouse --remote-root s3a://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse --report .local/s3-lakehouse/snapshot-verification.json
.venv/bin/python -B -m unittest discover -v -s apps/collector/test
.venv/bin/python -B -m unittest discover -v -s pipelines/spark/test -p 'test_s3_lakehouse*.py'
.venv/bin/python -B -m unittest discover -v -s pipelines/spark/test -p test_intelligence.py
.venv/bin/python -B -m unittest discover -v -s pipelines/spark/test -p test_contracts.py
.venv/bin/python -B -m unittest discover -v -s tests/integration -p test_collection_worker.py
.venv/bin/python -B -m unittest discover -v -s tests/integration -p test_pipeline_orchestration.py
.venv/bin/python -B scripts/smoke_s3_lakehouse.py --workdir .local/s3-lakehouse/local-smoke-20261028
.venv/bin/python -B .local/s3-lakehouse/rebuild_historical_local.py
.venv/bin/python -B .local/s3-lakehouse/rebuild_historical_seeded_local.py
.venv/bin/python -B scripts/smoke_s3_lakehouse.py --workdir .local/s3-lakehouse/cloud-smoke-20261028 --base s3a://dsp391m-data-lake/staging/s3-lakehouse-e2e-20261028 --profile dsp391m-spark --allow-cloud-write --supervised-root --report .local/s3-lakehouse/cloud-smoke-report.json
poetry check --lock
git diff --check
```

Không kiểm thử: production real-data migration/cutover, broad crawl, distributed multi-host executors, credentials thực sự hết hạn, real Kafka outage trong S3 notification path, unattended cloud service. S3-like outage / Kafka-like notification failure / invalid contract / interrupted staging đều fault-inject local bằng synthetic fixtures. Không fault-inject production, không VACUUM hoặc xóa checkpoint/data.

## H. Deployment runbook

[Runbook thực thi 11 bước](s3_lakehouse.md): connectivity, 44-table verification, bootstrap và nine-table validation, catch-up gap reconciliation, enable outbox, finite approved crawler run, full rebuild, verify, watch, monitor, rollback/resume. Có commands cụ thể và giới hạn approval/Root/policies. [Runtime env example](../../configs/collection/s3_runtime.env.example) không tự load; [synthetic policy](../../configs/collection/s3_synthetic_policy.example.json) không cấp quyền dữ liệu thật.

## I. Remaining issues

| Mức | Nhóm | Vấn đề / trạng thái |
| --- | --- | --- |
| BLOCKER production | Data policy | Chưa có reviewed real storage/derived permissions + retention registry cho historical/catch-up/new data; không tự cấp quyền. |
| BLOCKER production | Credentials/cloud runtime | Phiên hiện tại là Root; cần runtime identity phù hợp, executor credentials và supervisor được duyệt trước unattended. |
| BLOCKER production | Migration/orchestration | Chưa có phê duyệt writable real-data namespace/cutover; cần quiesce writers, pin catch-up, đối chiếu tất cả gap run IDs và replay saved envelopes trước resume. |
| HIGH operational | Delta concurrency | Lock hợp tác: mọi writer phải vào cùng entrypoint/control root. Crashed lock cần xác minh driver chết + supervised conditional recovery; chưa có auto recovery. |
| HIGH serving ngoài scope | Data quality/ClickHouse | Lỗi staged ClickHouse rows differ from validated Gold giữ nguyên, không promote serving; S3 Gold độc lập. |
| MEDIUM | Scale | Existing business adapter bounded 250.000 rows/table; verifier 500.000. Incremental computation/distributed transform chưa có; full rebuild là fallback đã kiểm chứng. |
| MEDIUM | Orchestration | Backend terminal receipt sync và distributed outbox chưa có; local durable disk + status/report là nguồn vận hành hiện tại. |
| MEDIUM | Kafka/source availability | Receipt notification dùng durable control event/polling; chưa wire producer Kafka cho receipts; không chạy broad source crawl trong lượt này. |
| LOW | Cost/storage | Giữ synthetic generations và logs để review; chưa cleanup/VACUUM. S3 request latency đáng kể; cadence micro-batch cần đo trước production. |

## J. Handoff

[HANDOFF CONTEXT — DSP391m S3 LAKEHOUSE IMPLEMENTATION](s3_lakehouse_handoff.md) là context độc lập. Raw reports/logs nằm dưới .local/s3-lakehouse, không chứa credentials và không commit. Current HEAD vẫn như đầu lượt; code changes còn trong working tree.
