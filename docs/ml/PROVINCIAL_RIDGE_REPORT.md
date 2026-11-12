# Báo cáo triển khai Provincial Ridge — 11-11-2026

## Kiểm chứng blocker bổ sung — 11-11-2026

**Blocker chưa được gỡ:** hai profiles AWS đã hết login session, người vận hành
chưa thể đăng nhập lại; local data vẫn chỉ có **2 tỉnh eligible**. Phần đã hoàn
tất là audit tái lập, so sánh các bản local/export và kiểm thử. Không đổi model,
thresholds 48/5/2 hoặc folds; không crawl, restore, overwrite nguồn hay promote.

**Local ↔ S3:** `sts get-caller-identity` và `s3api head-bucket` với cả hai profiles
đều thất bại vì expired login. Không thể xác định row/count/version differences
trên S3 hiện tại. Cách đăng nhập đúng là `aws login --profile dsp391m-bootstrap
--region ap-southeast-2` (thêm `--remote` nếu cần), sau đó kiểm tra lại profile
Spark. Không dùng `aws sso login` cho chain `login_session` đang cấu hình và không
thay IAM/credentials. Không yêu cầu người dùng gửi token/code vào chat.

**Local ↔ các bản đã có:** exact schema-type/order và row-multiset comparison
trên cả bốn bảng cho 0 missing/extra rows khi so reference v35 với current local
v35, historical seed v0 và full Silver Parquet export v35; cả bốn export checksums
khớp. Các nguồn đều có posts 649, geo 450, intents 949, admin 100. V35 có đủ JSON
history 0–35 và một active Parquet file mỗi bảng, không missing/wrong-size files;
toàn bộ row columns đã đọc thành công, latest versions sau audit vẫn 35. Không
phát hiện bản Silver giàu history hơn trong những nguồn đã kiểm tra.

**Nguyên nhân chỉ hai tỉnh eligible:** data tập trung quá muộn so với cutoff
selection, không phải lỗi join làm mất direct-demand history. 34 tỉnh reference,
16 có completed-range observations; chỉ ba tỉnh có geo history trước 12-01-2026.

| Tỉnh | Panel weeks | History weeks có geo | Geo history | Direct history | Eligible |
|---|---:|---:|---:|---:|---|
| Hà Nội | 116 | 5 | 6 | 2 | yes |
| Hồ Chí Minh | 116 | 8 | 10 | 3 | yes |
| Đà Nẵng | 116 | 2 | 2 | 0 | no |
| 31 tỉnh khác | 116 mỗi tỉnh | 0 | 0 | 0 | no |

`panel_weeks` là zero-filled calendar, không chứng minh 116 tuần có collection
coverage. Full table đúng các cột yêu cầu và deficit từng tỉnh:
[`provincial_readiness_audit.csv`](provincial_readiness_audit.csv).

| Bước/grain | Giữ lại | Loại so bước trước | Tỷ lệ loại |
|---|---:|---:|---:|
| Geo mentions → valid resolution | 379 / 450 | 71 UNRESOLVED | 15.78% |
| Valid mentions → unique post–province pairs | 283 / 379 | 96 pair duplicates | 25.33% |
| Pairs → matching new-admin province | 283 | 0 | 0% |
| Pairs → matching social post | 283 | 0 | 0% |
| Pairs → completed publication range | 240 | 43 | 15.19% |
| Completed pairs → selection history | 18 | 222 | 92.50% |
| Historical geo pairs → direct demand | 5 | 13 | 72.22% |

Post grain: 649 có valid nonnull publication date; 552 thuộc completed weeks;
chỉ 24 trước selection cutoff, trong đó 5 direct-demand posts. **Cả 5 direct posts
đều có valid geo/admin link**, không có direct history bị mất do geo filter/join.
Không có unmatched `new_province_id`; `province_id` là alias của ID này và được
join vào `VN_ADMIN_POST_2025`. Intent rows 949 được aggregate về 724 content IDs
trước join. Có 75 evidence rows ngoài post scope; chúng không phải đối tượng
forecast, không được dùng để nhân bản/tăng post target. Không suy diễn chúng là
broken keys khi chưa xét Silver comment scope.

Đà Nẵng có completed-range 18 geo/4 direct, nhưng 16 geo và cả 4 direct ở sau
cutoff; không được dùng để chọn tỉnh. Thiếu tối thiểu **3 bài Đà Nẵng trước cutoff,
trong đó ít nhất 2 direct**, đều distinct và geo-linked. Các tỉnh zero-history cần
ít nhất 5 geo posts, trong đó 2 direct. Không có năm direct-history posts chưa map
để sửa join rồi đạt tỉnh thứ ba.

**Benchmark/inference/Gold:** actual-source benchmark, final refit, next-week
forecast và real-data Gold staging vẫn NOT_RUN do gate `<3`. Audit artifacts
Delta/CSV đã ghi riêng dưới `.local/ml/provincial-ridge/recheck/verified-audit/`;
không có model mới hoặc market metric. Synthetic results trong báo cáo ban đầu
chỉ là tests, không thay benchmark nguồn thực tế.

**Bổ sung dữ liệu, không sửa điều kiện:**

1. Sau re-login, discover/verify current S3 generations và pinned versions; so
   exact content với local trước khi kết luận cloud có thêm history. Chỉ restore
   Silver snapshot có provenance và integrity xác minh được, vào namespace mới.
2. Ưu tiên archived Silver/source snapshots trước cutoff cho tỉnh thứ ba; ba bản
   mirror/export local đã audit hiện không giúp tăng coverage.
3. Nếu cần crawler, đề xuất historical discovery theo địa phương và direct intents
   (`tìm sân`, `tìm người chơi`, `hỏi giá`) với publication dates trước 12-01-2026,
   valid extracted geo, stable content IDs và source permissions. Đà Nẵng thiếu
   3 geo/2 direct là lựa chọn ít thiếu nhất, không phải hạn ngạch được phép bịa.
4. Historical posts thu thập hôm nay không tự chứng minh availability ở forecast
   origin cũ; phải ghi retrospective protocol hoặc có snapshot/first-seen evidence.
   Prospective crawling cần tiếp tục tích lũy history thật; không kéo watermark
   hoặc đổi cutoff chỉ để pass gate. Không có crawler nào được tự động chạy ở đây.
5. Trước promotion: actual-source benchmark/acceptance, final refit, frozen-artifact
   inference, S3 staging readback/replay/rollback và non-Root runtime được review.

Xem [aggregate evidence mới](provincial_readiness_evidence.json) và
[filter/join CSV](provincial_filter_join_audit.csv). Trạng thái tests của lần
kiểm chứng được cập nhật riêng ở cuối báo cáo; số liệu ở mục E là lượt ban đầu.

### Tests của lượt kiểm chứng blocker

| Final run | Tổng | PASSED | FAILED/ERROR | SKIPPED |
|---|---:|---:|---:|---:|
| Toàn bộ Spark suite, bao gồm Ridge/Delta/ETL/audit | 202 | 201 | 0 | 1 |
| Toàn bộ collector suite | 305 | 304 | 0 | 1 |
| Local orchestration integration | 28 | 28 | 0 | 0 |
| Airflow DAG + runtime trong cached image 3.3.2 | 8 | 8 | 0 | 0 |
| Tổng các final suites, không tính lặp targeted rerun | **543** | **541** | **0** | **2** |

Spark suite chạy 857.581 giây. Data-readiness test được chạy lại sau khi bổ sung
physical-file checks, **1 PASS / 37.982 giây**, không cộng lặp vào tổng trên.
Assertions kiểm tra file inventory, deficit/eligibility history-only và exact
snapshot equality/difference. Synthetic Ridge tests tiếp tục kiểm tra train,
evaluate dry-run, predict, save/load parity, correct province/week và Gold replay.
Các output đó chỉ là fixtures, không phải final inference trên source hiện tại.

SKIPPED:

- `test_provincial_ridge_delta.ProvincialSparkTests.test_s3_pinned_silver_reads`:
  không có credentials/test descriptor hợp lệ sau khi AWS checks báo expired;
  operator chưa thể login. Không đổi SKIP thành PASS.
- `test_court_normalizer.CourtNormalizerTests.test_local_pbplus_samples_read_only_when_available`:
  không có local PBPlus sample; synthetic cases trong cùng suite vẫn chạy.

Host Python không có Airflow nên lần import ban đầu lỗi dependency. Cached image
có sẵn được dùng offline, repo read-only; lần thử container đầu gặp writer-lock
trên mount read-only. Đã chuyển riêng lock path sang ephemeral `/tmp`, không sửa
source/tests/IAM hay mở quyền mount; final Airflow run **8 PASS**. Các lỗi setup
ban đầu được giữ trong logs, không báo chúng là code regressions hoặc SKIPPED.
Không có assertion failure hoặc environment error còn tồn tại trong final suites.

Logs bền trong `.local/ml/provincial-ridge/recheck/logs/`; JSON evidence có full
Spark passed/skipped IDs. Không chạy Go/frontend suites ngoài phạm vi data pipeline.

Files bổ sung riêng cho lượt gỡ blocker: `ml/provincial/audit.py`, batch entrypoint
`audit_provincial_data.py`, data-readiness integration test, hai audit CSVs và
`provincial_readiness_evidence.json`; hai tài liệu yêu cầu đã cập nhật. Model fit,
feature engineering, cutoffs/config và thresholds của lần triển khai trước giữ
nguyên. Source Delta versions trước/sau vẫn 35; không commit/push/promotion.

Code đã triển khai trên branch `model`, chưa commit/push. **Chưa có model cuối
cho dữ liệu Silver hiện tại:** S3 access bị chặn bởi expired AWS session và local
reference chỉ có 2 tỉnh đạt eligibility, dưới minimum 3. Giữ nguyên benchmark
defaults; không hạ ngưỡng, thêm dữ liệu mô phỏng vào nguồn thật hoặc tạo metric giả.

| Scope | Status |
|---|---|
| Silver loader, panel, lag features, pooled scaling, Ridge, CV, final refit, predict | IMPLEMENTED |
| Local Spark/Delta tests, all three CLI operations, save/load and Gold replay | TESTED với fixtures synthetic riêng |
| Eligibility trên Silver local reference | TESTED: 116 tuần, 34 province candidates, chỉ 2 eligible |
| Training/evaluation/final prediction trên snapshot local đó | BLOCKED trước fit |
| Current S3 Silver verification | BLOCKED: expired credential-process session |
| S3 integration automated test | SKIPPED; không ghi nhận PASSED |
| Actual-source benchmark, cloud publication, multi-host execution, promotion | NOT_RUN |

## A. File đã thay đổi

| File | Vai trò |
|---|---|
| `pipelines/spark/src/dsp_spark/ml/provincial/__init__.py` | Package trong ML architecture sẵn có |
| `pipelines/spark/src/dsp_spark/ml/provincial/config.py` | Protocol defaults, completed week cutoff, exact fold planning |
| `pipelines/spark/src/dsp_spark/ml/provincial/data.py` | Pinned Silver reads, schema audit, geo/intent de-dup counts, panel, eligibility |
| `pipelines/spark/src/dsp_spark/ml/provincial/features.py` | Calendar validation, ordered 48 lags, six pooled scalers, full one-hot, assembler |
| `pipelines/spark/src/dsp_spark/ml/provincial/ridge.py` | Spark-only Ridge fit, per-fold regularization conversion, clipped predictions |
| `pipelines/spark/src/dsp_spark/ml/provincial/evaluation.py` | Spark metrics, walk-forward, coefficients/group/lag/province effects and occlusion |
| `pipelines/spark/src/dsp_spark/ml/provincial/pipeline.py` | Evaluation, final refit, immutable metadata/model I/O and next-week Gold MERGE |
| `pipelines/spark/src/dsp_spark/jobs/batch/provincial_demand.py` | `train`, `evaluate`, `predict` CLI, dry-run/staging guards |
| `configs/ml/provincial_ridge.json` | Default model/protocol config, explicit as-of and audited example watermark |
| `configs/ml/provincial_inputs.example.json` | Four explicit source path/version descriptors, provenance |
| `contracts/data/gold/provincial_demand_forecast_v1.schema.json` | Forecast output contract |
| `pipelines/spark/requirements-ridge.txt` | Production numerical dependency; no sklearn/Pandas/Torch training |
| `pipelines/spark/requirements-ridge-test.txt` | Test-only sklearn reference dependencies |
| `pipelines/spark/test/test_provincial_ridge.py` | Pure calendar/fold/config/order tests |
| `pipelines/spark/test/test_provincial_ridge_delta.py` | Spark/Delta, CLI, parity, forecasting, idempotency and optional S3 tests |
| `Makefile` | Four opt-in Ridge targets, no automatic collection-cycle training |
| `pipelines/spark/README.md` | Job discovery/runbook link |
| `docs/ml/README.md` | Link mới, giữ nguyên historical clustering report |
| `docs/ml/PROVINCIAL_RIDGE.md` | Full design, semantics, artifacts, commands, operational limits |
| `docs/ml/PROVINCIAL_RIDGE_REPORT.md` | Báo cáo A–G này |
| `docs/ml/provincial_ridge_evidence.json` | Aggregate evidence từ execution, không chứa raw post text |

## B. Data lineage và eligibility

Root S3 xác định từ manifest cũ:
`s3a://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse`;
region `ap-southeast-2`, current profile `dsp391m-spark`.

| Silver suffix | Local reference version/rows |
|---|---|
| `/silver/intelligence/social_posts` | 35 / 649 |
| `/silver/intelligence/social_geo_mentions` | 35 / 450 |
| `/silver/intelligence/social_intent_evidence` | 35 / 949 |
| `/silver/intelligence/dim_admin_unit` | 35 / 100 |

Fresh S3 reads thất bại; không suy ra S3 hiện tại còn bằng local reference. Đã chỉ
định explicit local snapshots trong `.local/ml/provincial-ridge/local-inputs.json`
để audit độc lập. Dữ liệu/schema được đọc bằng Spark, không sử dụng Bronze/Gold.
Provenance `unknown`, new admin version `VN_ADMIN_POST_2025`.

Panel local: **116 tuần**, 05-08-2024 đến week start 19-10-2026;
observed watermark 28-10-2026, completed cutoff 25-10-2026. Eligibility chỉ dùng
75 tuần trước **12-01-2026**. Hà Nội: 6 geo / 2 direct posts; Hồ Chí Minh: 10 / 3;
Đà Nẵng: 2 / 0; 31 tỉnh khác không có historical eligible observations.
Chỉ hai tỉnh selected nên không tạo train/test metrics hoặc next-week forecast.

Audit/error artifact thực tế:
`.local/ml/provincial-ridge/staging/registry/provincial_ridge/runs/20261111T105210Z-0ae6cf5d7c6a/`.
`province_eligibility_audit` có đủ 34 tỉnh; `ridge_model_errors` giải thích gate.
Không có final model trong run BLOCKED này.

## C. Cấu hình và tính tương đương

Mặc định alpha 10, 8-week lookback, horizon 1, three folds, validation 12 weeks,
seed 42; Spark `LinearRegression` giữ các tham số được yêu cầu, gồm
`standardization=False`, `solver=auto`, `maxIter=1000`, `tol=1e-6`.
Sáu population scalers gộp toàn bộ lag positions, train-only, province one-hot
không scale. Formula được đối chiếu source Spark 4.0.2 và kiểm thử:

`regParam = 10 × population_std(label_train) / n_train`.

Với local source run BLOCKED: chưa có fitted feature dimension, regParam hoặc
training-sample count. Không báo hệ số của mô hình chưa huấn luyện.

Với **synthetic fixture riêng**: P=3, dimension **51**, calendar 64 tuần,
final train 56 sample weeks/tỉnh = **168 samples**, cutoff **29-03-2026**;
next-week test forecast đúng **30-03-2026**, 3 rows. Final regParam
**0.06654964218749375**. Fold train rows 84/102/117, regParam tương ứng
0.1330992843749875 / 0.10874808181556772 / 0.09523050414480806.
Final model/scaler được refit riêng, khác final CV fold.

## D. Metrics đã thực sự đo — SYNTHETIC ONLY

Các số dưới dùng fixture tuần hoàn dễ dự đoán để kiểm tra code, **không phải**
market benchmark hoặc kết quả của Silver S3/local hiện có. Notebook không tìm
thấy; đánh dấu `NOT_DIRECTLY_COMPARABLE`.

48 test province-weeks, mỗi tỉnh 16 tuần, zero-target ratio 0.0:

| Model | RMSE | MAE | Bias | Macro MAE |
|---|---:|---:|---:|---:|
| Ridge | 0.01491678 | 0.01058298 | -0.0000006801 | 0.01058298 |
| Moving Average 4 | 1.11803399 | 1.00000000 | 0.00000000 | 1.00000000 |

RMSE skill 98.6658%, MAE skill 98.9417%, chỉ mô tả fixture.

| Fold | Test rows | Ridge RMSE | Ridge MAE | MA4 RMSE | MA4 MAE |
|---|---:|---:|---:|---:|---:|
| 1 | 18 | 0.01623343 | 0.01090438 | 1.06718737 | 0.94444444 |
| 2 | 15 | 0.01504253 | 0.01116191 | 1.14746097 | 1.03333333 |
| 3 | 15 | 0.01302025 | 0.00961836 | 1.14746097 | 1.03333333 |

| Synthetic province | Ridge RMSE | Ridge MAE | MA4 RMSE | MA4 MAE |
|---|---:|---:|---:|---:|
| p0 | 0.01495853 | 0.01064813 | 1.11803399 | 1.00000000 |
| p1 | 0.01459353 | 0.01033837 | 1.11803399 | 1.00000000 |
| p2 | 0.01519219 | 0.01076243 | 1.11803399 | 1.00000000 |

Grouped occlusion trên last test fold: target/seek_court/seek_players có
delta RMSE khoảng 0.37779136; các channel constant trong fixture có delta 0.
Đây là sensitivity của fixture, không suy ra nhu cầu địa phương hay quan hệ nhân quả.
Top positive/negative coefficients và province-effect tables được lưu cùng run.

Persistent evaluation review artifacts:
`.local/ml/provincial-ridge/synthetic-review-af10c8af/`.
Run `synthetic-evaluation` có status EVALUATED và **không có final model**, đúng
contract của evaluate. Training/final-forecast/save-load paths được kiểm tra riêng
trong integration suite bằng temporary synthetic artifacts.

## E. Kiểm thử

New suite: **10 PASSED, 0 FAILED, 1 SKIPPED**, 178.998 giây.
Regression: **14 contract/config + 5 clustering tests PASSED**.
Thêm targeted finite-output validation rerun: PASSED (17.673 giây).

PASSED protocol tests:

- `test_completed_week_asof_and_watermark`
- `test_folds_match_array_split_and_expanding_train`
- `test_strict_config_and_order`
- `test_no_implicit_source_fallback`

PASSED Spark/Delta tests:

- `test_geo_dedup_multi_intent_union_timezone_and_incomplete_week`
- `test_lag_order_no_target_leakage_scaler_train_only_and_stable_onehot`
- `test_missing_duplicate_weeks_and_unknown_province_fail`
- `test_spark_regularization_equivalence_and_constant_label`
- `test_metrics_bias_macro_and_zero_baseline`
- `test_full_silver_benchmark_refit_saved_forecast_and_idempotency`

SKIPPED: `test_s3_pinned_silver_reads`, không có explicit test descriptor/credentials.
Separate read-only S3 audit đã thử và BLOCKED do session hết hạn.
Coverage bao gồm 17 yêu cầu kiểm thử: geo/multi-intent dedup, VN date/week cutoffs,
history-only eligibility, lag alignment/no leakage, train-only pooled scaler,
feature order/encoding, model fit/parity, folds/metrics/clip, save/load,
forecast-week/count correctness và replay. Gold JSON contract được validate.
CLI train/predict executed và evaluate dry-run verified không đổi staging files;
separate evaluate CLI execution/readback cũng hoàn tất.

Ruff, synchronous Black formatting, compileall, JSON Schema validation và
`git diff --check` đều đã chạy. Logs tại `/tmp/dsp-ridge-*.log`; evidence aggregate
track tại [`provincial_ridge_evidence.json`](provincial_ridge_evidence.json).

## F. Lệnh chạy

Xem đầy đủ environment setup và flags trong [runbook](PROVINCIAL_RIDGE.md).
Sau khi có bốn Silver snapshots và ≥3 tỉnh đủ điều kiện:

```bash
export PYTHONPATH="$PWD/pipelines/spark/src:$PWD/apps/collector/src"

.venv/bin/python -m dsp_spark.jobs.batch.provincial_demand train \
  --inputs .local/ml/provincial-inputs.json --config configs/ml/provincial_ridge.json \
  --registry .local/ml/provincial-ridge/staging/registry \
  --execute --report .local/ml/ridge-training.json

.venv/bin/python -m dsp_spark.jobs.batch.provincial_demand evaluate \
  --inputs .local/ml/provincial-inputs.json --config configs/ml/provincial_ridge.json \
  --registry .local/ml/provincial-ridge/staging/registry \
  --execute --report .local/ml/ridge-evaluation.json

.venv/bin/python -m dsp_spark.jobs.batch.provincial_demand predict \
  --inputs .local/ml/provincial-inputs.json \
  --registry .local/ml/provincial-ridge/staging/registry --run-id "$RUN_ID" \
  --execute --publish --output-root .local/ml/provincial-ridge/staging/output \
  --report .local/ml/ridge-forecast.json
```

`RUN_ID` phải lấy từ TRAINED manifest với final refit. Thay paths/config bằng
audited inputs; dùng local reference hiện tại sẽ exit 2 ở eligibility gate.
Bỏ `--execute` để dry-run. Tests: `make ridge-test` hoặc unittest commands trong
runbook; S3 test chỉ bật khi explicit `DSP_RIDGE_S3_INPUTS` và profile được cấp.

## G. Giới hạn và công việc còn thiếu

- Khôi phục AWS session, xác minh S3 snapshot mới, source permissions/provenance
  và collection watermark. S3 write/readback/rollback chưa chạy.
- Cần ít nhất ba tỉnh đạt geo/direct history trước first validation cutoff;
  không dùng dữ liệu tương lai để sửa selection. Silver hiện tại chưa đủ.
- Notebook chưa có; cần đối chiếu corpus, intent union policy, calendar và folds
  trước tuyên bố tương đương benchmark. Publication-date backtest chưa chứng minh
  point-in-time availability của classifier/geo snapshots.
- Numerical parity: regular fixture đạt sai lệch ~1e-14; fixture có constant
  column đạt max coefficient diff 4.77e-9, max prediction diff 1.78e-8, trong
  tolerance 1e-7. Không tuyên bố giống bitwise.
- Spark phát Cholesky fallback và line-search warnings/errors trên periodic,
  collinear fixture. Spark vẫn trả model, finite output và metrics đã kiểm tra;
  không khẳng định optimizer hội tụ chỉ dựa vào trạng thái EVALUATED/TRAINED.
  Saved-fold numerical diagnostics được lưu trong aggregate evidence. Production
  cần solver/acceptance review trên dữ liệu phù hợp.

  Đối chiếu bổ sung ba saved folds với sklearn trên chính synthetic train/test
  inputs cho max test-prediction differences lần lượt **8.88e-16**, **1.29e-9**,
  **3.47e-8**, vẫn trong tolerance 1e-7. Đây là đo numerical parity, không chứng
  nhận hội tụ từ log hay model readiness trên dữ liệu thị trường.
- Chỉ local[2] được chạy; multi-host packaging, memory/partition sizing,
  shared storage, IAM non-Root và concurrent-writer operation chưa được xác minh.
- Forecast là proxy quan sát mạng xã hội. Zero/missing coverage và source bias
  có thể chi phối kết quả. Candidate artifacts không tự promote hoặc phục vụ API.

Không đổi hoạt động Bronze/Silver/Gold đang có; không triển khai MLflow, service
API, PyTorch hoặc thuật toán bổ sung ngoài Ridge và Moving Average baseline.
