# Provincial Demand Forecasting — Spark Ridge

## Kiểm chứng blocker dữ liệu bổ sung — 11-11-2026

Đã kiểm tra lại STS và `head-bucket` bằng cả `dsp391m-bootstrap` và
`dsp391m-spark`: cả bốn request bị chặn bởi **expired login session**, trước khi
có thể kiểm chứng nội dung/quyền S3. Người vận hành xác nhận chưa thể login lúc
này. Không đổi IAM, profile hay credentials. Vì vậy **chênh lệch local ↔ S3 hiện
tại chưa xác định được**, không được báo hai bên giống nhau từ báo cáo cũ.

Credential chain hiện có: `dsp391m-spark` →
`aws configure export-credentials --profile dsp391m-bootstrap --format process` →
bootstrap `login_session`. Đăng nhập trong terminal của người vận hành:

```bash
aws login --profile dsp391m-bootstrap --region ap-southeast-2
# Remote/WSL không có browser callback: thêm --remote, xác thực trực tiếp với AWS.
aws sts get-caller-identity --profile dsp391m-bootstrap --region ap-southeast-2
aws sts get-caller-identity --profile dsp391m-spark --region ap-southeast-2
aws s3api head-bucket --bucket dsp391m-data-lake --profile dsp391m-spark --region ap-southeast-2
```

Không in `export-credentials` ra terminal/log/chat. Cấu hình bootstrap
`login_session` hiện tham chiếu Root; identity phiên mới vẫn cần xác minh bằng STS.
Quyền đọc hữu hạn không đồng nghĩa đủ điều kiện ghi cloud bằng ML runtime;
cloud-write guard non-Root và production approval vẫn được giữ nguyên.

Audit local đã chạy bằng code mới, không thay model/scaler/folds/thresholds:

- 34 tỉnh trong dimension, 16 tỉnh có geo-linked observation trong completed
  range, chỉ 3 tỉnh có geo posts trong history trước validation đầu tiên.
- Calendar panel 116 tuần; history cutoff exclusive **12-01-2026**, 75 tuần lịch.
  Số tuần có observation không bằng số tuần zero-filled: Hà Nội 5 tuần history,
  Hồ Chí Minh 8, Đà Nẵng 2. `MIN_PANEL_WEEKS=48` giữ nguyên ý nghĩa calendar chung.
- Hà Nội 6 geo/2 direct; Hồ Chí Minh 10/3; Đà Nẵng 2/0. Vẫn **2 eligible**.
- Đà Nẵng có 18 geo/4 direct trong toàn completed range nhưng 16 geo và cả 4 direct
  nằm sau history cutoff; dùng chúng để select tỉnh sẽ leak validation/test.
- Tối thiểu cần thêm **3 bài distinct geo-linked Đà Nẵng**, ít nhất **2 bài có
  direct intent**, có publication week trước cutoff và provenance/quyền sử dụng
  phù hợp. Direct posts cũng count vào geo threshold; không cộng nhầm thành 5 bài.
  Tỉnh khác hiện thiếu tối thiểu 5 bài geo, trong đó ít nhất 2 direct.

Đã đọc toàn bộ active row columns, kiểm tra đủ 36 JSON commits và active Parquet
file của version 35: không thiếu file, không sai kích thước; source version sau
audit vẫn 35. Exact distributed `exceptAll` trên bốn bảng xác nhận reference khớp
**Silver local hiện hành v35**, **historical seed v0** và **full unfiltered Silver
Parquet export v35** đã có; checksum export cũng khớp. Không bản nào cung cấp thêm
history. Các đối chiếu này là local ↔ local/export, **không phải fresh S3 proof**.

Review artifacts đã track:
[audit 34 tỉnh](provincial_readiness_audit.csv),
[filter/join counts và tỷ lệ loại](provincial_filter_join_audit.csv),
[evidence lần kiểm chứng](provincial_readiness_evidence.json).
Private Delta audit tables và report:
`.local/ml/provincial-ridge/recheck/verified-audit/`.

CLI audit có thể chạy lại độc lập, chỉ ghi metadata vào output directory mới:

```bash
.venv/bin/python -m dsp_spark.jobs.batch.audit_provincial_data \
  --inputs .local/ml/provincial-ridge/local-inputs.json \
  --config .local/ml/provincial-ridge/local-config.json \
  --compare-inputs .local/ml/provincial-ridge/recheck/current_local-inputs.json \
  --compare-inputs .local/ml/provincial-ridge/recheck/historical_seed-inputs.json \
  --export-manifest .local/modeling/research_exports/full_silver_v35_a831c81_20261028T185656Z/manifest.json \
  --output-dir .local/ml/provincial-ridge/recheck/audit-next
```

`--compare-inputs` nhận descriptor S3 đã xác minh khi credentials hoạt động;
comparison dùng exact row multiset, không sampling. `--export-manifest` chỉ audit
export full Silver đã chỉ định, không restore hay fallback training bằng Parquet.
Với checkpointed historical snapshots thiếu complete JSON history, physical
inventory báo `NOT_VERIFIED` thay vì suy diễn đủ file; Spark row read vẫn độc lập.

Kết luận hiện tại: benchmark nguồn thực tế, final refit/inference và real-data Gold
staging **BLOCKED/NOT_RUN**. Không tạo market metrics, không promote production.
Kết quả test cuối và kế hoạch bổ sung dữ liệu nằm trong
[báo cáo kiểm chứng](PROVINCIAL_RIDGE_REPORT.md).

Triển khai ngày 11-11-2026 trên branch `model`. Mô hình dự báo **số bài đăng có
geo link tới tỉnh và intent nhu cầu trực tiếp**, không phải booking hoặc nhu cầu
thị trường đã xác minh. Tỉnh được nhắc tới không xác định vị trí của người đăng.

**Kết quả dữ liệu hiện có: BLOCKED.** S3 credential-process session hết hạn.
Lần thử độc lập trên Silver local giữ nguyên default tìm được **2 tỉnh đủ điều
kiện**, dưới mức tối thiểu 3; pipeline dừng trước fit/scaling/evaluation/final
forecast. Không có metric benchmark hoặc dự báo thị trường được tạo cho snapshot
này. Fixture synthetic được dùng riêng để kiểm thử code, không thay dữ liệu thật.

## Audit và data lineage

Notebook `Provincial Demand Forecasting(2).ipynb` không tìm thấy khi tìm cả repo
và `.local` (bỏ cache model/dependencies). Dùng đặc tả được cung cấp trong yêu cầu;
chưa thể tuyên bố cùng snapshot hoặc tái lập notebook.

Đường dẫn S3 được khôi phục từ verification manifest hiện có, **không đoán path**:
`s3a://dsp391m-data-lake/bootstrap/20261028T064025Z/lakehouse/`.
Region `ap-southeast-2`, profile hiện có `dsp391m-spark`. Đọc lại trực tiếp bốn bảng
đã thử và bị chặn do expired AWS session; kết quả xác minh cũ không được coi là
chứng cứ hiện tại. Không có S3 write trong lần triển khai này.

| Logical source | Path tương đối dưới root trên | Local reference version | Local rows |
|---|---|---:|---:|
| social posts | `silver/intelligence/social_posts` | 35 | 649 |
| geo mentions | `silver/intelligence/social_geo_mentions` | 35 | 450 |
| intent evidence | `silver/intelligence/social_intent_evidence` | 35 | 949 |
| admin dimension | `silver/intelligence/dim_admin_unit` | 35 | 100 |

Local reference đọc bằng Spark tại
`.local/s3-snapshots/20261028T064025Z/lakehouse`. Đây là nguồn local được chỉ định
**riêng** khi chạy, không phải fallback tự động từ S3. Provenance giữ `unknown`.
Schema thực tế: `content_id:string`, `published_date_vn:string`,
`published_at:string`, `intent_labels:array<string>`, `is_commercial:boolean`;
geo có `new_province_id:string`, `resolution_status:string`; evidence có
`intent_type:string`; dimension có `admin_unit_id`, `admin_name`, `admin_level`,
`admin_version` kiểu string. Cả bốn schema version `3` được đối chiếu contract repo.
Dimension có 34 provinces + 3 wards ở `VN_ADMIN_POST_2025` và 63 provinces ở
`VN_ADMIN_PRE_2025`; config dùng đúng `VN_ADMIN_POST_2025`.

Local social publication dates từ 05-08-2024 đến 28-10-2026. Geo có 379
`RESOLVED`, 71 `UNRESOLVED`. Các snapshot/row counts/schema đầy đủ được giữ trong
`.local/ml/provincial-ridge/audit.json`; aggregate evidence track trong
[`provincial_ridge_evidence.json`](provincial_ridge_evidence.json).

## Counts, panel và chống leakage

Inputs bắt buộc là bốn Silver Delta snapshots pin `path` + `version`, dataset ID,
provenance và optional generation. Không có nhánh đọc Bronze/Gold thay thế. Tái
sử dụng `create_spark`, `LakehousePaths`, data contracts, `unique_keys`, Delta
MERGE và registry I/O của repo. Không đổi crawler, collection worker hoặc ETL.

Intent được xác định bằng union `social_posts.intent_labels` và
`social_intent_evidence.intent_type`. Evidence được aggregate thành ba boolean
flags cho mỗi `content_id` trước join; duplicate evidence không tăng counts.
Chỉ `SEEK_COURT`, `SEEK_PLAYERS`, `PRICE_INQUIRY` góp vào target; target là OR,
không phải tổng ba cột. Geo loại null/empty và năm status không hợp lệ trong
đặc tả, deduplicate `(content_id, province_id)` trước join, rồi kiểm tra province
ID thuộc dimension/version đã chọn. Cùng bài có thể count một lần ở mỗi tỉnh.
Posts phải unique theo content ID; schema/key lỗi fail fast.

`published_date_vn` được ưu tiên. Fallback timestamp string phải có timezone,
được parse về UTC và đổi sang `Asia/Ho_Chi_Minh` để lấy ngày. Spark session giữ
UTC theo runtime repo. Tuần bắt đầu Monday, kết thúc Sunday.

`as_of_date` là **exclusive VN midnight**, `observed_through` là watermark ngày
inclusive. Chỉ dùng tuần có Sunday ≤ watermark và < as-of date. Config example
dùng watermark audited 28-10-2026; operator phải thay bằng collection watermark
đã xác minh khi chọn snapshot khác. Không tự kéo zero padding tới hôm nay.
Calendar start lấy ngày xuất bản sớm nhất trong completed range hoặc Monday
`panel_start` explicit. Calendar cross provinces từ reference dimension; lấp sáu
count bằng 0 trong phạm vi đó. **0 nghĩa là không quan sát được bài phù hợp trong
dữ liệu thu thập, không có nghĩa nhu cầu thực tế bằng 0.**

`MIN_PANEL_WEEKS=48` là điều kiện độ dài calendar chung. Ngưỡng 5 geo posts và
2 direct posts của từng tỉnh được tính **chỉ trước validation đầu tiên**. Không
đòi 48 tuần trước validation cho từng tỉnh vì điều đó không khớp fold formula ở
panel 48 tuần. Reference province universe cố định; tỉnh chỉ xuất hiện trong
validation/test vẫn có lịch sử 0 và bị loại. Eligibility audit lưu tất cả selected
và excluded, totals/cutoff/reason. Không giảm mức ≥3 tỉnh để ép chạy.

Một pooled model dùng tất cả eligible provinces. Lag window theo tỉnh và
`week_start`, kiểm tra duplicate/missing tuần trước lag. Feature order:
`lag8 × 6 channels, lag7 × 6, ..., lag1 × 6, full province one-hot`.
Mã hóa tỉnh sorted theo ID, không drop province dummy nào; one-hot không scale.
`VectorAssembler` chỉ nhận 48 lag inputs và province indicators, không nhận
target/geo/commercial của tuần đang dự báo. Không dùng random split.

Scaler explode tám lag rows trong train, tính sáu `avg`/`stddev_pop`, dùng chung
tham số cho từng channel ở mọi lag. Variance 0 → scale 1. Validation/test không
fit scaler. Không thay bằng 48 scalers độc lập. Số chiều `48 + P`.

## Ridge objective và numerical parity

scikit-learn Ridge tối thiểu `SSE + alpha*||w||²`, intercept không penalize.
Spark 4.0.2 `auto` với ≤4096 features đi qua WeightedLeastSquares, trong đó label
standardization vẫn bật khi `standardization=False`. Objective trên original
feature/label scale tương ứng:

```text
SSE/(2*n_train) + regParam/(2*std_pop(y_train))*||w||²
regParam = alpha * std_pop(y_train) / n_train
alpha = 10.0
```

Conversion dùng label std và n thực tế của mỗi fold/final refit. Constant label
trả intercept bằng mean, coefficients 0; regParam 0 trong case này không đổi
nghiệm. Chỉ hỗ trợ runtime 4.0.2 và bounded dimensions để tránh silently chuyển
sang nhánh optimizer có cách scale khác. `standardization=False` bảo toàn tác
động regularization trên one-hot và numeric inputs đã scale bên ngoài.

Nguồn kiểm tra: [Spark LinearRegression 4.0.2](https://github.com/apache/spark/blob/v4.0.2/mllib/src/main/scala/org/apache/spark/ml/regression/LinearRegression.scala),
[WeightedLeastSquares](https://github.com/apache/spark/blob/v4.0.2/mllib/src/main/scala/org/apache/spark/ml/optim/WeightedLeastSquares.scala),
[scikit-learn Ridge 1.6](https://scikit-learn.org/1.6/modules/generated/sklearn.linear_model.Ridge.html).
Các tham số còn lại giữ `elasticNetParam=0`, `fitIntercept=True`,
`loss=squaredError`, `solver=auto`, `maxIter=1000`, `tol=1e-6`.
`seed=42` được lưu; normal-equation Ridge không có random-seed fit parameter.

Kiểm thử so coefficient/prediction trên data xác định và có negative control
`alpha/n` để phát hiện thiếu label-scale factor. Test có thêm constant column
đạt tolerance 1e-7; số sai lệch thực tế nằm trong evidence. Spark có thể fallback
Cholesky → Quasi-Newton khi có constant/collinear columns; không tuyên bố exact
bitwise equivalence với sklearn. Không sử dụng sklearn/pandas/Torch để train ở
production; sklearn chỉ import trong test.

## Walk-forward và model cuối

`first_test=max(8+12+20, int(total_weeks*0.75))`; phần còn lại chia ba block
liên tiếp như `numpy.array_split` (remainder cấp cho block đầu). Train expanding
trước validation, validation 12 tuần trước test, cùng boundaries cho mọi tỉnh.
Hyperparameters fixed; validation metrics được lưu riêng, không dùng test để
tune/select. Dự báo test là **rolling one-step with observed history**: các tuần
test sau dùng quan sát thật tuần trước làm lags, không refit model/scaler và
không gọi đây là recursive multi-week forecast.

Sau clip `max(0, raw_prediction)`, tính Spark aggregates cho RMSE, MAE, bias
`mean(prediction-label)`, macro MAE theo tỉnh, per-fold/per-province metrics,
zero-target ratio và moving average bốn target lags. Skill `1 - ridge/baseline`
chỉ có giá trị khi denominator khác 0, còn lại null. Giữ continuous prediction.
Kiểm tra đủ rectangular province×test-week grid, unique keys, finite outputs và
đúng province/week trước ghi kết quả thành công.

`train` luôn chạy benchmark trước, rồi **refit model/scaler trên toàn bộ valid
completed samples** với eligible provinces đã freeze. Final metadata có training
weeks/samples/cutoff riêng. `evaluate` không tạo final model. `predict` từ chối
artifact EVALUATED/BLOCKED/thiếu final refit; không lấy model cuối của CV làm final.
Next-week features lấy đúng tám completed weeks mới nhất, không tạo dummy future
target row. Optional inference watermark/as-of override được ghi trong report.

## Artifact và Gold output

Tái sử dụng `Registry` I/O nhưng namespace và manifest version riêng, không áp
clustering runtime/UMAP dependencies lên Ridge:

```text
<registry>/provincial_ridge/runs/<run_id>/
  reservation.json
  folds/1|2|3/spark_model/
  folds/1|2|3/metadata.json         # scaler + regularization + order + mapping
  final/spark_model/               # train operation only
  final/metadata.json
  tables/<table_name>/             # immutable per-run Delta tables
  manifest.json                   # committed last; full lineage/config/state
```

Run ID exclusive; không overwrite historical models/outputs. State giữ
`candidate`, không tự promote production. Incomplete model run không có success
manifest; errors table ghi failure. Business eligibility failure có BLOCKED
manifest, eligibility audit và model-errors table, không fake prediction/metrics.

Tables gồm đủ `walk_forward_folds`, `province_eligibility_audit`,
`ridge_predictions_by_province_week`, `ridge_metrics_by_fold`,
`ridge_metrics_by_province`, `ridge_benchmark_summary`, `ridge_model_errors`;
thêm validation metrics, coefficients, top positive/negative coefficients,
province effects, coefficient magnitudes theo group/lag, và grouped occlusion.
Occlusion chỉ trên final evaluation fold, thay cả tám raw channel lags bằng
train channel mean (scaled coordinate 0), dự báo lại và đo delta RMSE/MAE.
Coefficients mô tả linear associations; predictive occlusion importance khác
coefficient magnitude; cả hai không chứng minh causality. Province effects là
penalized deviations quanh shared intercept trong full one-hot coding.

Gold riêng: `<output-root>/gold/provincial_demand_forecast_v1`.
Key `(province_id, forecast_week, model_version, data_version)`. Insert-only MERGE
không tạo duplicate khi rerun; raw prediction/name/cutoff mâu thuẫn cùng key bị
reject. Dùng source-data version và model version để chọn forecast history.
Giữ `generated_at` lần publish đầu, không overwrite Gold khác. JSON contract:
[`provincial_demand_forecast_v1.schema.json`](../../contracts/data/gold/provincial_demand_forecast_v1.schema.json).
CLI report có bounded forecast preview, timestamps export bằng Spark JSON UTC.

## Cài đặt, CLI và orchestration

```bash
.venv/bin/python -m pip install -r pipelines/spark/requirements-ridge.txt
# Only needed for parity tests:
.venv/bin/python -m pip install -r pipelines/spark/requirements-ridge-test.txt
export PYTHONPATH="$PWD/pipelines/spark/src:$PWD/apps/collector/src"
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1
export AWS_REGION=ap-southeast-2
# Set DSP_S3_PROFILE to the reviewed existing runtime profile when reading S3.
# Local only; do not advertise loopback on a multi-host cluster.
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_SUBMIT_ARGS='--driver-memory 2g pyspark-shell'
```

Copy [`provincial_inputs.example.json`](../../configs/ml/provincial_inputs.example.json)
để điền bốn verified paths/versions. Với managed generations, resolve
`control/current.json` một lần, lấy `paths`/`output_versions` từ validated
generation manifest; không lấy latest từng bảng giữa run. Config:
[`provincial_ridge.json`](../../configs/ml/provincial_ridge.json).
Các lệnh mặc định dry-run; `--execute` mới fit/ghi artifacts.

```bash
# Read-only panel/eligibility plan:
.venv/bin/python -m dsp_spark.jobs.batch.provincial_demand \
  --profile "$DSP_S3_PROFILE" evaluate \
  --inputs .local/ml/provincial-inputs.json --config configs/ml/provincial_ridge.json \
  --registry .local/ml/provincial-ridge/staging/registry --report .local/ml/ridge-plan.json

# Evaluation artifacts without final model:
.venv/bin/python -m dsp_spark.jobs.batch.provincial_demand evaluate \
  --inputs .local/ml/provincial-inputs.json --config configs/ml/provincial_ridge.json \
  --registry .local/ml/provincial-ridge/staging/registry \
  --execute --report .local/ml/ridge-evaluation.json

# Benchmark followed by separate final refit:
.venv/bin/python -m dsp_spark.jobs.batch.provincial_demand train \
  --inputs .local/ml/provincial-inputs.json --config configs/ml/provincial_ridge.json \
  --registry .local/ml/provincial-ridge/staging/registry \
  --execute --report .local/ml/ridge-training.json

# RUN_ID from successful training; --publish explicitly selects Gold staging.
.venv/bin/python -m dsp_spark.jobs.batch.provincial_demand predict \
  --inputs .local/ml/provincial-inputs.json \
  --registry .local/ml/provincial-ridge/staging/registry --run-id "$RUN_ID" \
  --execute --publish --output-root .local/ml/provincial-ridge/staging/output \
  --report .local/ml/ridge-forecast.json
```

Nếu không `--publish`, predict vẫn trả giá trị forecast trong local JSON report.
Forecast với Silver mới có thể truyền `--as-of-date`/`--observed-through`, giữ
nguyên model/mapping/scaler đã fit. CLI exit 2 cho insufficient history/eligibility;
không coi exit đó là successful benchmark.

`make ridge-train`, `ridge-evaluate`, `ridge-predict` dùng cùng CLI qua
`RIDGE_ARGS`, và `make ridge-test` chạy tests. Đây là downstream step opt-in sau
Silver generation đã validated; không tự gắn training vào mọi crawler cycle.
Collection worker/local ETL lifecycle giữ nguyên. Không có MLflow/service mới.

S3 read/write dùng runtime và profile chain sẵn có. Cloud write còn cần explicit
`--allow-cloud-write` và non-Root identity; output roots phải là dedicated staging
và disjoint khỏi source. Production publication/promotion cần approval riêng và
chưa thực hiện. On cluster, cài cùng package/dependencies/Java/connector/profile
trên driver/executors và dùng S3 hoặc shared filesystem cho artifacts, tránh local
filesystem riêng từng host. Test hiện tại chỉ local[2], không claim multi-host.
Covariance dùng normal equations có dimension bound; collect chỉ O(P), 6 scaler
stats, fold metrics/coefficients và bounded previews; không collect corpus/panel.

## Kết quả thực tế và phần còn thiếu

Local attempt: panel 116 tuần từ 05-08-2024 tới week 19-10-2026, training cutoff
25-10-2026. Eligibility chỉ xem 75 tuần trước **12-01-2026**:

| Province | Historical geo posts | Historical direct posts | Decision |
|---|---:|---:|---|
| Hà Nội | 6 | 2 | SELECTED |
| Hồ Chí Minh | 10 | 3 | SELECTED |
| Đà Nẵng | 2 | 0 | EXCLUDED |
| 31 tỉnh còn lại | 0 | 0 | EXCLUDED |

Không fit Ridge với hai tỉnh, không xuất final forecast, không tính regParam/metric
cho run bị chặn. Toàn bộ audit 34 tỉnh lưu tại run artifact/report. Width 51,
regParam/metric và cutoff của synthetic tests là dữ liệu **kiểm thử riêng**, xem
[báo cáo kết quả](PROVINCIAL_RIDGE_REPORT.md), không phải benchmark thị trường.

Để chạy benchmark nguồn thật: khôi phục AWS session và audit S3 mới; xác minh
source policies/provenance/coverage watermark; cung cấp ≥3 provinces đạt history
threshold trước validation đầu tiên theo timeline đã pin. Không dùng tương lai
hoặc hạ default thresholds để đạt gate. Khi có notebook, đối chiếu source snapshot,
intent policy, panel range và fold boundaries trước tuyên bố comparable.

Retrospective publication-date backtest giả định observation history đã có sẵn;
Silver hiện tại không chứng minh point-in-time availability của text/labels/geo
classification ở từng forecast origin. Zero fill còn bị ảnh hưởng missing coverage
và source bias. Cần lịch sử version/availability và coverage audit trước production.
Production còn cần acceptance review, source permission/IAM non-Root, multi-host
resource/packaging tests và S3 staging readback/replay/rollback khi có quyền.
