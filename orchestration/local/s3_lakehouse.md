# DSP391m — S3 lakehouse runbook

Phạm vi: branch `model`, Bronze → 13 Silver → 10 Gold; không ML, không publication ClickHouse. Snapshot `s3a://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse` bất biến. Mọi lệnh ghi cloud bên dưới chỉ được chạy **sau phê duyệt riêng cho prefix và loại dữ liệu**. Flags CLI ghi nhận phê duyệt; chúng không thay thế phê duyệt của người vận hành.

## Runtime và kiểm tra chỉ đọc

Spark 4.0.2 / Scala 2.13, Delta 4.0.1, Hadoop client và `hadoop-aws` 3.4.1, AWS SDK v2 bundle 2.24.6. Để Ivy resolve dependencies được khóa; không trộn JAR AWS SDK v1 hoặc connector Hadoop khác version. Java 17. Dependency boto3 được khóa trong Poetry cho control plane S3.

Profile `dsp391m-spark` dùng `credential_process` của AWS CLI lấy temporary credentials từ `dsp391m-bootstrap`. S3A dùng `software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider`; tên profile truyền tới driver qua `aws.profile`, tới executor qua cả Java option và `AWS_PROFILE`. Không truyền access key/token qua SparkConf. Thử nghiệm hiện tại là local Spark driver với hai execution threads; chưa chứng minh credential distribution trên cluster nhiều host. Runtime nhiều host phải cung cấp credential process/config tương ứng trên mọi executor hoặc dùng IAM role phù hợp.

```bash
git branch --show-current
git rev-parse HEAD
git status --short
aws s3api head-bucket --bucket dsp391m-data-lake --profile dsp391m-spark --region ap-southeast-2
export PYTHONPATH=apps/collector/src:pipelines/spark/src
export SPARK_LOCAL_IP=127.0.0.1
poetry run python -m dsp_spark.jobs.batch.verify_s3_snapshot \
  --local-root .local/s3-snapshots/20261028T064025Z/lakehouse \
  --remote-root s3a://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse \
  --report .local/s3-lakehouse/snapshot-verification.json
```

Verifier đọc trực tiếp từng Delta table từ S3, ghim version, so schema, count và multiset SHA256 của toàn bộ row với snapshot local cùng version. Giới hạn verifier 500.000 rows/table; vượt ngưỡng sẽ fail, không sampling. Report có đủ 44 paths/schemas/versions/counts/digests/error types. Không chạy production ingestion khi chưa PASS đủ 44 và `content_match=true`.

## Kiến trúc và quyền writer

Collectors hiện có PBPlus/Threads/YouTube → source envelope local → outbox durable → một Spark writer → Bronze Delta → receipt xác nhận đọc lại → durable downstream notification → polling micro-batch → generation Silver/Gold.

Chỉ bật outbox khi đã sẵn sàng cutover và được phê duyệt. `DSP_S3_OUTBOX_DIR` phải nằm trên storage bền vững dùng chung cho các worker trên máy hiện tại; bản MVP này không phải distributed queue. Hook ghi artifact của cả ba collector enqueue tự động. Backend giữ environment khi gọi collection worker. Khi `DSP_S3_INGESTION_MODE=outbox`, collection worker trả `PARTIAL`, `bronze_ingestion_status=pending`, `processing_status=pending`; monitor receipt/report riêng để biết trạng thái cuối. Chưa có đồng bộ ngược trạng thái committed vào dashboard backend.

Outbox copy envelope và job JSON bằng atomic replace + fsync, khóa flock, giữ artifacts; trạng thái pending/failed/committed, bounded exponential retry 2–3600 giây. Cùng source/run nhưng payload khác bị từ chối. Không có xác nhận giả khi S3 chưa commit. Commit receipt được ghi sau read-back six record tables; replay sau crash dùng receipt hoặc insert-only MERGE theo stable observation IDs. Không tuyên bố exactly-once end-to-end.

Mọi writer phải dùng cùng lakehouse root và `control.writer()`; **không chạy legacy ingestion/streaming jobs trực tiếp vào namespace này**. S3 lock dùng conditional PutObject `If-None-Match: *`, release conditional DeleteObject `If-Match`. Lock không hết hạn/không auto-steal: driver chậm không thể sống quá lease rồi ghi đè owner mới. Crash để lại lock; cần chứng minh driver/executors đã dừng rồi recovery có giám sát. Object lock release chỉ xóa marker tạm do chính phiên tạo, không xóa dữ liệu Delta. IAM runtime phải có quyền conditional operations trên control prefix; bucket policy nên giới hạn writer duy nhất. Lock là hợp tác giữa các entrypoint quản lý, không bảo vệ khỏi writer ngoài giao thức.

Notifications mặc định là JSON durable ở `control/notifications/`; scheduler polling trực tiếp Bronze versions nên Kafka không nằm trên critical path. Có callback notification độc lập và kiểm thử callback failure; chưa bật Kafka topic/producer cho S3 receipts. Kafka outage không đảo trạng thái Bronze committed. Không đổi checkpoint Kafka hiện tại.

## Policy gate

Trước ghi dữ liệu thật cần registry riêng do chủ nguồn/người vận hành duyệt cho **tất cả nguồn thực sự có trong historical + catch-up + new input**: `storage_permission=true`, `policy_reviewed_at`, `retention_policy_id`, explicit `retention_days` (positive integer hoặc reviewed null), `derived_processing_permission=true`. Các allowed fields có hạn chế chưa được adapter hỗ trợ sẽ fail theo registry loader. `s3_synthetic_policy.example.json` chỉ cho fixture tổng hợp; `synthetic_only=true` không được dùng cho dữ liệu thật. Không tự cấp quyền từ availability công khai của nguồn.

Root session chỉ được chạy finite development command đã phê duyệt, thêm `--supervised-root`. CLI **từ chối Root chạy watch**. Cần IAM runtime role/profile phù hợp và môi trường luôn chạy trước khi bật unattended operation. Không tạo long-lived Root key.

## Smoke test tổng hợp, không crawl

Local (thư mục mới mỗi lượt):

```bash
poetry run python scripts/smoke_s3_lakehouse.py --workdir .local/s3-lakehouse/local-smoke-unique
make s3-lakehouse-test
```

Cloud chỉ sau phê duyệt prefix sau; không bootstrap dữ liệu thật trong smoke:

```bash
poetry run python scripts/smoke_s3_lakehouse.py \
  --workdir .local/s3-lakehouse/cloud-smoke-20261028 \
  --base s3a://dsp391m-data-lake/staging/s3-lakehouse-e2e-20261028 \
  --profile dsp391m-spark --allow-cloud-write --supervised-root \
  --report .local/s3-lakehouse/cloud-smoke-report.json
```

Script dùng processors offline thật của cả ba collector với fixture synthetic, tự enqueue, commit Bronze, build 23 outputs, replay không commit/duplicate, thêm fixture mới tự tạo generation sau, read-back tất cả outputs và rollback về generation đầu với `paused=true`. Không tạo compute/service cloud mới; có S3 request/storage charges. Không cleanup prefix hay VACUUM sau test. Fault injection chỉ trong local synthetic tests, không ở production.

## Bootstrap lịch sử và catch-up — chỉ sau phê duyệt dữ liệu thật

Chọn **namespace mới, rỗng**, ví dụ `s3a://dsp391m-data-lake/authoritative/lakehouse-v1`. Chưa tạo namespace này trong implementation local. Đặt biến cho phiên được duyệt:

```bash
export LAKE_BASE=s3a://dsp391m-data-lake/authoritative/lakehouse-v1
export REAL_POLICIES=/absolute/path/to/reviewed-real-source-policies.json
export RUNTIME_PROFILE=dsp391m-runtime
```

1. Dừng/pause acquisition bằng control hiện có, đợi tất cả collection runs cũ và local Delta writer hoàn tất; ghi lại local version vector và run IDs. Không xóa local data/checkpoints. Bootstrap dùng snapshot S3 đã verifier xác minh; catch-up dùng local lake sau khi writer được quiesce. Envelope phát sinh trong cutover phải được enqueue trước khi acquisition được resume.
2. `bootstrap-plan.json` ghim upfront historical versions lấy từ Phase 1 report và catch-up versions từng bảng. Retry bắt buộc cùng roots/policies/plan. Records append sau pinned catch-up phải vào outbox/replay; không tự giả định đã nằm trong bootstrap.

```bash
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse bootstrap \
  --base "$LAKE_BASE" --profile "$RUNTIME_PROFILE" --allow-cloud-write \
  --policies "$REAL_POLICIES" \
  --catchup-root file:///home/tuancui/DSP391m/pipelines/spark/.local/lakehouse \
  --report .local/s3-lakehouse/bootstrap-report.json
```

Bootstrap merge six Bronze record datasets và ba source-envelope tables, theo observation ID hoặc source/run; overlap phải cùng business values, chỉ bỏ khác biệt transport metadata. Collection runs chọn trạng thái terminal mới nhất. Mỗi bảng được đọc lại so count/digest; `control/bootstrap.json` chỉ xuất hiện khi tất cả chín bảng validated. Fail giữa chừng giữ stage và plan để retry; chưa cho process dữ liệu thật nếu marker chưa có. Không promote serving và không đổi collector environment tự động.

Marker bootstrap lưu thêm pinned versions của 23 outputs trong snapshot đã verifier xác minh. Generation đầu dùng seed này chỉ để tái dùng classification caches và giữ dated supply snapshots qua eligibility/lifecycle logic hiện có. Silver hiện tại vẫn rebuild toàn bộ từ pinned authoritative Bronze. Các ngày supply trước as-of được giữ, ngày hiện tại được tính lại; không cộng thêm một ngày đã có sẵn. Các generations tiếp theo kế thừa historical supply từ previous validated generation.

3. Đối chiếu `bootstrap-report.json`, kiểm tra nine table versions/counts/digests và identity coverage của catch-up. Replay **toàn bộ envelope thuộc runs sau snapshot, đang dở hoặc phát sinh sau pinned catch-up**, bằng saved envelope ban đầu; dư overlap an toàn nhờ stable IDs, bỏ sót run không an toàn. Cần đối chiếu manifest acquisition/run IDs trước khi resume.

```bash
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse enqueue \
  --base "$LAKE_BASE" --artifact /absolute/path/to/saved-bronze-envelope.json \
  --outbox .local/s3-lakehouse/outbox
```

4. Chỉ sau chấp thuận cutover: áp dụng các biến outbox từ `configs/collection/s3_runtime.env.example` cho backend/collector; giữ paths của legacy writers local. Có thể chạy **một finite collection run giới hạn đã duyệt** qua controller hiện có. Không chạy broad crawl chỉ để test S3.

Các endpoint điều khiển thực tế (chỉ chạy sau phê duyệt, không được script tự gọi):

```bash
curl --fail --silent --show-error http://localhost:8080/api/v1/collection/status
curl --fail --silent --show-error -X POST http://localhost:8080/api/v1/collection/stop
# Sau khi finite scope/limits trong configs/collection/data_ops.v1.json được duyệt:
curl --fail --silent --show-error -X POST http://localhost:8080/api/v1/collection/run \
  -H 'Content-Type: application/json' --data '{"sources":["youtube"]}'
```

`collection/stop` dừng scheduling; vẫn phải đợi run hiện tại kết thúc và xác minh status trước pinned catch-up. Thay port nếu backend cấu hình khác. Không gọi `collection/start` cho đến khi runtime identity và vận hành continuous đã được duyệt.

## Full rebuild, verify và cập nhật tự động

```bash
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse process \
  --base "$LAKE_BASE" --profile "$RUNTIME_PROFILE" --allow-cloud-write \
  --policies "$REAL_POLICIES" --outbox .local/s3-lakehouse/outbox \
  --report .local/s3-lakehouse/processing-report.json
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse verify \
  --base "$LAKE_BASE" --profile "$RUNTIME_PROFILE" \
  --report .local/s3-lakehouse/generation-verified.json
```

Writer capture six-table Bronze version vector dưới lock; cả rebuild đọc `versionAsOf`. Tái dùng business transforms cũ, eligibility/retention/tombstones/lineage/contracts, decimal money schemas và geographies. Mode **FULL_REBUILD**; incremental computation chưa triển khai. Version detection/no-op dùng vector, code/reference/contract/policy digests, ngày VN và retention/future-content boundaries. Giới hạn adapter 250.000 rows/Bronze table, fail rõ nếu vượt; chưa phải distributed business transform cho quy mô lớn.

Mỗi output nằm dưới `<base>/generations/<id>/<catalog-relative-path>`; manifest chứa 13 Silver + 10 Gold paths, versions, schemas, counts/digests, quality report, six Bronze reconciliation rows, code/reference/policy/contract digests, as-of, timestamp, rollback target. Consumer chỉ resolve `control/current.json` → immutable `control/generations/<id>.json`. Không đọc “latest” từng bảng độc lập; quality Delta manifest bên trong stage không phải con trỏ promote. Read-back 23 tables + validate + reconciliation phải thành công trước compare-and-set pointer. Gold xây từ cùng bộ Silver trong generation bằng existing adapter rồi được validated/read-back; không trộn Silver namespaces.

Reconciliation phân biệt observation/canonical entity, eligibility exclusion, lineage, run coverage-control, lifecycle-control và orphan metrics giữ lại Bronze. Không cộng six grains thành unique entity count. `unexplained=0` bắt buộc; không ép Silver count bằng Bronze count.

Sau phê duyệt vận hành, danh tính không phải Root, registry thật và supervised finite checks PASS:

```bash
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse watch \
  --base "$LAKE_BASE" --profile "$RUNTIME_PROFILE" --allow-cloud-write \
  --policies "$REAL_POLICIES" --outbox .local/s3-lakehouse/outbox \
  --interval 300 --report .local/s3-lakehouse/processing-report.json
```

Đây là managed local micro-batch service, default 300s (min 30s). Chạy dưới supervisor của môi trường đã duyệt để restart khi JVM/process chết; source hiện tại không tạo/bật service unattended tự động. Lỗi pending giữ retry state; processing exception ghi safe `error_type` vào report, watch retry ở lượt sau. Existing current generation không đổi khi build fail. Không chạy đồng thời một watch và independent writer khác. Receipt ghi per-run; con trỏ generation là processing watermark đã validated. Chưa có runtime IAM/supervisor thì automation chỉ mới là implementation đã kiểm thử finite, chưa phải dịch vụ production.

Dừng writer/watch trước khi deploy source mới, rồi restart process để loaded Python modules khớp code digest; không hot-edit code trên một driver đang chạy. Giữ phiên bản code tương ứng manifest khi cần tái lập generation.

## Monitor, failure và rollback

```bash
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse status --base "$LAKE_BASE" --profile "$RUNTIME_PROFILE"
poetry run python -m json.tool .local/s3-lakehouse/processing-report.json
```

Kiểm tra local `outbox/jobs/*.json`: failed/retry_at/attempts/error_type/notification_status. Cloud `control/receipts/`, `control/notifications/`, `control/bootstrap-plan.json`, `control/bootstrap.json`, `control/current.json`, `control/generations/`. Không in config/credential-process output hoặc temporary tokens. AWS auth failure: refresh approved login session/role, giữ pending, retry; không thay bằng static Root keys.

Rollback chỉ đổi con trỏ sau verify retained generation, không xóa data/history:

```bash
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse rollback \
  --base "$LAKE_BASE" --profile "$RUNTIME_PROFILE" --allow-cloud-write \
  --generation-id RETAINED_VALIDATED_ID
poetry run python -m dsp_spark.jobs.batch.s3_lakehouse resume \
  --base "$LAKE_BASE" --profile "$RUNTIME_PROFILE" --allow-cloud-write
```

Rollback đặt paused để watcher không tự lập tức promote lại dữ liệu mới. `resume` là hành động riêng cần phê duyệt tương ứng. Bootstrap gốc luôn là recovery source chỉ đọc; quay lại consumer path snapshot cũng là cutover có phê duyệt, không reverse-write/xóa authoritative Bronze. Lock recovery: xác minh owner process đã chết, dừng scheduling, lấy marker owner/ETag, thao tác conditional marker cleanup có giám sát sau phê duyệt; không remove lock mù hoặc tự steal. CLI không cung cấp force-unlock.

ClickHouse đứng ngoài job này. Downstream serving failure không xóa Gold, không invalidate manifest, không chạy production publication. Lỗi `staged ClickHouse rows differ from validated Gold` giữ nguyên phạm vi ngoài nhiệm vụ.
