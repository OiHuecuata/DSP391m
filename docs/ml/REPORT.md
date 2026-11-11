# Báo cáo triển khai — 29-10-2026

Đã triển khai trực tiếp trên branch `model`, chưa commit/push. Pipeline chạy được
end-to-end bằng MiniLM thật → UMAP 15D → Spark ML KMeans trên **Silver local đã
pin**. Chưa chạy được từ S3 vì phiên AWS hết hạn. Không thay đổi Bronze/Silver,
không publish production, không promote model.

## Trạng thái

| Thành phần | Implementation | Validation thực tế |
|---|---|---|
| Pinned Silver loader, schema/PK/provenance checks | IMPLEMENTED | TESTED local Delta version 35 và synthetic failures |
| Distributed sentence encoding và Delta cache | IMPLEMENTED | TESTED MiniLM 384D trên 649 posts; changed-text cache test |
| Bounded full-corpus UMAP fit, saved transform | IMPLEMENTED | TESTED 15D, save/load và unseen synthetic post inference |
| Spark KMeans K=5/8/10 | IMPLEMENTED | TESTED cả 3 candidates, không chốt model cuối |
| Cosine silhouette, CH, sizes, coverage, ARI stability | IMPLEMENTED | TESTED; parity với scikit-learn trên fixture nhỏ |
| c-TF-IDF, keyword ranking, label lift | IMPLEMENTED | TESTED trong training run; labels chỉ hậu kiểm |
| Versioned candidate registry | IMPLEMENTED | TESTED immutability, checksum, incompatible runtime rejection |
| Inference và dedicated Gold MERGE | IMPLEMENTED | TESTED 649 assignments; real replay không đổi count/Delta version |
| Incremental timestamp selection | IMPLEMENTED | TESTED synthetic new post và real empty watermark batch |
| Gold rollback | IMPLEMENTED | TESTED isolated synthetic local Delta, không rollback dữ liệu thị trường |
| Separate spatial count job | IMPLEMENTED | TESTED CLI local, 13 địa phương; evidence fanout protection |
| Trend extension interface | IMPLEMENTED | NOT_RUN: chưa có algorithm/benchmark được chọn |
| Current S3 schema/content verification | IMPLEMENTED audit | BLOCKED: expired AWS credential session |
| Training/inference từ S3, S3 staging write/rollback | IMPLEMENTED code paths | NOT_RUN: cloud access blocker; không có cloud write |
| Colab assignment ARI, lexical ablation | IMPLEMENTED optional paths | NOT_RUN: chưa có baseline export; ablation chưa chạy |
| Multi-host distributed runtime | IMPLEMENTED Spark interfaces | NOT_RUN: chỉ local[2] được đo |
| KDE/Moran/LISA/Gi*/Spearman | Extension boundary | NOT_RUN: không có spatial statistical outputs giả |
| Production validation/promotion/deployment | Disabled | NOT_RUN: cần phê duyệt riêng |

## Dữ liệu và giới hạn đối chiếu

Local immutable bootstrap reference có social posts 649, intent evidence 949,
geo mentions 450, canonical venues 373; cả bốn Delta version 35, schema version
`3`, không partition, unique primary keys. Social nguồn YouTube 598 và Threads 51;
không empty text, 9 nhóm text trùng sau NFKC. Provenance giữ **unknown**: không thấy
marker synthetic không đủ để xác nhận dữ liệu thật. Source policies chưa được
xác nhận cho production derived storage.

Benchmark được cung cấp có 6.490 posts và 229 venues có tọa độ. Snapshot audited
có 649 posts và **0 venues có tọa độ hợp lệ**. Chưa có Dataset V3/notebook/Colab
assignment export để kiểm chứng equality. Không sử dụng Dataset V1. Mọi metric
dưới đây là **NOT_DIRECTLY_COMPARABLE**, không khẳng định tái lập kết quả Colab.

## Metrics đo được

Run `20261029T072901Z-c247e766e9fb`, status `candidate`.
MiniLM revision `e8f8c211226b894fcb81acc59f3b34ba3efd5f42`, L2 normalized 384D,
max sequence length 128. UMAP 15D, neighbors 30, min_dist 0, cosine, seed 42.
Spark KMeans Euclidean, k-means||, seed 42, maxIter 100, tol 1e-4.

| K | Cosine silhouette / original embedding | CH / UMAP Euclidean | Mean stability ARI | Coverage | Noise |
|---:|---:|---:|---:|---:|---:|
| 5 | 0.117981 | 756.626670 | 0.739927 | 100% | 0% |
| 8 | 0.139307 | 1206.047460 | 0.805104 | 100% | 0% |
| 10 | 0.156088 | 859.693321 | 0.752032 | 100% | 0% |

Coverage tính trên eligible nonempty posts, không phải chứng cứ chất lượng cụm.
Stability: 5 seeds 17/29/43/71/97, target 80% hash-selected subsamples,
fixed encoder và fitted UMAP; refit KMeans và so với full-data fit trên overlap.
Số dòng và ARI từng lần nằm trong [evidence](evidence.json).
K=8 cluster sizes: 85, 8, 13, 113, 15, 110, 182, 123 (cluster ID 0→7),
max/min ratio 22,75. Các cụm còn nhỏ/lệch; chưa đặt semantic names.
Không coi K=8 là lựa chọn cuối chỉ vì đang là primary candidate.

Training pipeline mất **109,03 giây** (không gồm cài dependency, tải weights và
Spark startup), local[2], 4 encoder partitions, batch size 32, driver JVM 2 GiB.
Python driver peak RSS khoảng 355 MiB; không bao gồm JVM hoặc executors.
UMAP estimate 16.572.864 bytes array working set; không phải total RSS.

Inference CLI load artifact đã lưu và ghi 649 assignments vào staging local.
Replay readback xác nhận trước/sau đều 649, unique key, Gold Delta vẫn version 0,
cache vẫn 649 rows, watermark tương lai trả 0. Source latest Delta vẫn 35.
Không biến distance tới centroid thành confidence score.

Spatial CLI đọc pinned local inputs, tạo 13 geographies. 355/373 venues và
433/450 geo mentions thiếu `old_province_id`; locality counts chỉ là proxy,
không được diễn giải thành nhu cầu/booking thực tế hoặc KDE đã kiểm định.

## Kiểm thử đã chạy

- **11/11** unit + real Spark/Delta integration pass, 125,77 giây. Bao phủ schema,
  null/unique IDs, evidence fanout, empty text, snapshot pinning, vector shape và
  finite/L2 checks, cache invalidation, save/load parity, deterministic known/new
  inference, metric parity, contract validation, incremental rerun và rollback.
- **14/14** existing contract/config regression tests pass.
- Ruff, Black formatting check qua synchronous Python API, compileall, JSON parse,
  dependency `pip check`, `git diff --check`: pass.
- ResourceWarnings từ PySpark socket cleanup xuất hiện trong test log; tests không
  có failure. Không claim đã chạy toàn bộ repository test suite.

Chi tiết private runtime artifacts giữ tại `.local/ml/`, logs `/tmp/dsp-ml-*.log`.
[Evidence đã track](evidence.json) chỉ chứa aggregate metadata/metrics, không chứa
post text, keywords, representative snippets, weights hoặc centroids. Full run
manifest/artifacts giữ trong `.local/ml/staging/registry/runs/<run_id>/`.

## Cần hoàn thành trước production

1. Reauthenticate AWS credential-process session; chạy read-only audit S3 mới,
   pin generation/version và đối chiếu schema/content với snapshot dùng train.
2. Cấp runtime IAM non-Root giới hạn prefix, xác nhận source permissions,
   retention, provenance và real/synthetic policy cho derived data/model artifacts.
3. Cung cấp V3 snapshot, notebook protocol, encoder/UMAP identity và versioned
   assignment export; chạy baseline ARI/parity và vòng benchmark encoder mới.
   Không suy ra V3 equality từ count venues trùng nhau.
4. Review duplicate/near-duplicate weighting, nguồn YouTube chiếm đa số, cluster
   imbalance và c-TF-IDF/label enrichment. Semantic names cần human review/version.
5. Kiểm thử S3 staging write/readback/rollback bằng identity được phép; kiểm thử
   executor packaging/cache, resource limits, concurrency, telemetry và multi-host.
6. Xác định acceptance thresholds, validation/release record, monitored deployment
   và phê duyệt promotion riêng. Hiện registry chỉ có candidate, production disabled.
7. Spatial cần sửa/hoàn thiện geography/coordinates và review CRS/grid/weights
   trước KDE/statistical tests. Trend tiếp tục đợi benchmark, chưa có model.

Kiến trúc, file inventory, CLI training/inference, configuration, registry và output
contract nằm trong [runbook](README.md). Đây là bàn giao code và kết quả local đã
kiểm thử; cloud/production blockers được giữ rõ, không thay bằng metric mô phỏng.
