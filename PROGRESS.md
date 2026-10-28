# NHẬT KÝ TIẾN ĐỘ THỰC HIỆN (PROGRESS)

Tài liệu này cập nhật kết quả đạt được và các vấn đề còn tồn đọng sau khi hoàn thành từng giai đoạn của đề tài.

---

## Giai đoạn 0: Lập kế hoạch — [HOÀN THÀNH]
- **Thời gian hoàn thành:** 2026-10-26
- **Kết quả đạt được:**
  - Khảo sát môi trường thực tế trên máy chủ: Windows 10/11 (Build 26200), 12 luồng xử lý (logical CPUs), tổng RAM 13.86 GB, dung lượng trống ổ `E:\` ~51.3 GB; đã cài đặt Docker 29.2.1, Docker Compose v5.1.0 và Python 3.12.5.
  - Tra cứu tài liệu chính thức của Apache IoTDB nhánh 1.3.x (`1.3.3-standalone`) về cấu hình Prometheus metric (`dn_metric_reporter_list`, `dn_metric_level`, `dn_metric_prometheus_reporter_port`), giao thức ghi `Session` (`insert_record`, `insert_tablet` / `NumpyTablet`) và cơ chế `Sequence`/`Unsequence` + `Compaction`.
  - Hoàn thành [`PLAN.md`](PLAN.md) định nghĩa rõ 6 giai đoạn, cấu trúc thư mục, 7 giả định thiết kế cốt lõi và bảng quản trị rủi ro.
- **Vấn đề tồn đọng / Lưu ý cho Giai đoạn 1:**
  - Docker Desktop daemon hiện chưa được bật sẵn (`docker desktop start` cần được kích hoạt khi bắt đầu Giai đoạn 1).
  - Cần kiểm tra trực tiếp tệp cấu hình `/iotdb/conf/iotdb-system.properties` và đầu ra thực tế của endpoint `http://localhost:9091/metrics` trên bản `apache/iotdb:1.3.3-standalone` để đối chiếu chính xác 100% tên từng tham số và tên metric Prometheus trước khi chốt cấu hình thu thập.

---

## Giai đoạn 1: Hạ tầng bằng Docker — [HOÀN THÀNH]
- **Thời gian hoàn thành:** 2026-10-26
- **Kết quả đạt được:**
  - Dựng thành công toàn bộ hạ tầng trong [`docker-compose.yml`](docker-compose.yml) với phiên bản image ghim cứng: `apache/iotdb:1.3.3-standalone`, `prom/prometheus:v2.54.1`, `grafana/grafana:11.2.2`, `gcr.io/cadvisor/cadvisor:v0.49.1`, và `iotdb-write-cost-loadgen:1.0.0` (`python:3.12.5-slim-bookworm`).
  - Trích xuất trực tiếp `iotdb-system.properties.template`, `datanode-env.sh` và `confignode-env.sh` từ image chính thức để thiết lập bộ nhớ (`DataNode: 1536M On-Heap + 512M Off-Heap`, `ConfigNode: 384M On-Heap + 128M Off-Heap` trong giới hạn container `3072 MiB` / `4 vCPU`), lưu trữ trên Docker Named Volume (`ext4` trên NVMe SSD) và bật Prometheus exporter (`9091` DataNode, `9092` ConfigNode).
  - Xác minh thực tế tên metric trên `http://localhost:9091/metrics` (`file_global_count`, `file_global_size`, `flush_memtable_count_total`, `cost_task_seconds_*`, `data_read_total`, `data_written_total`, `wal_buffer_*`, `process_cpu_time`, `process_used_mem`) và cAdvisor / Cgroup v2 (`/sys/fs/cgroup/io.stat`), cập nhật đầy đủ vào [`docs/environment.md`](docs/environment.md) và dashboard Grafana [`docker/grafana/dashboards/iotdb_write_cost.json`](docker/grafana/dashboards/iotdb_write_cost.json).
  - Chạy qua [`scripts/smoke_test.py`](scripts/smoke_test.py) từ cả máy chủ và bên trong container `loadgen`: xác nhận ghi đơn lẻ (`insert_record`), ghi lô (`NumpyTablet`), `FLUSH`, đọc lại khớp dữ liệu, xóa sạch database, tất cả Prometheus targets `UP` và Grafana dashboard được nạp tự động.
- **Phát hiện kỹ thuật & Xử lý:**
  - Nhân Linux WSL2 (`6.18.54.1-microsoft-standard-WSL2`) không bật `/proc/<pid>/io` nên `process_io_size` của IoTDB báo `0`; tuy nhiên Cgroup v2 `/sys/fs/cgroup/io.stat` (`rbytes`, `wbytes`, `rios`, `wios`) và cAdvisor `container_fs_writes_bytes_total{device="/dev/sdd"}` hoạt động chính xác tuyệt đối ở mức container `iotdb-service`. Bộ thu thập ở Giai đoạn 3 sẽ đọc kết hợp cả hai nguồn này.

---

## Giai đoạn 2: Bộ sinh tải (`loadgen/`) — [HOÀN THÀNH]
- **Thời gian hoàn thành:** 2026-10-26
- **Kết quả đạt được:**
  - Xây dựng mô hình sinh tải xác định theo seed trong [`loadgen/workload.py`](loadgen/workload.py) và đặc tả toán học đầy đủ trong [`docs/workload_model.md`](docs/workload_model.md).
  - Tách biệt 4 nhánh bộ sinh ngẫu nhiên độc lập (`PCG64` + `SeedSequence`) cho dữ liệu cảm biến gốc, lựa chọn vị trí bị trễ (`ooo_ratio` từ `0%` đến `100%`), mẫu độ trễ mạng (`exponential`, `lognormal` $\sigma=1.0$, và phân bố đuôi nặng `pareto`/Lomax $\alpha=2.5$ cùng kỳ vọng `delay_mean_ms`), và hoán vị toàn cục ở mức `100%`, đảm bảo **bất biến tập dữ liệu logic** khi giữ nguyên `seed`.
  - Xây dựng trình thực thi đa luồng đồng bộ qua `threading.Barrier` trong [`loadgen/client_runner.py`](loadgen/client_runner.py) dùng `apache-iotdb==1.3.3` (`insert_record` cho `batch_size=1` và `NumpyTablet` cho `batch_size > 1`), kèm bước `run_warmup`, làm sạch database (`DELETE DATABASE` + đợi giải phóng TsFile cũ), đăng ký trước schema (`GORILLA` + `LZ4`) và bước ổn định nền (`FLUSH` + đợi hàng đợi compaction tĩnh).
  - Phát hiện cơ chế quan trọng từ mã nguồn client `iotdb/utils/NumpyTablet.py`: hàm khởi tạo `NumpyTablet` tự động kiểm tra và sắp xếp tăng dần mảng `timestamps` nội bộ trong phạm vi từng lô kích thước `B`, đóng vai trò bộ đệm sắp xếp phía biên trước khi gửi xuống DataNode.
  - Viết và chạy qua toàn bộ bộ kiểm thử đơn vị trong [`tests/test_workload.py`](tests/test_workload.py) (kiểm định K-S test cho 3 phân bố độ trễ, độ chính xác `ooo_ratio`, kích thước batch và tính tái lập theo seed).
- **Vấn đề tồn đọng:** Không có.

---

## Giai đoạn 3: Đo lường & Trích xuất Nhật ký — [HOÀN THÀNH]
- **Thời gian hoàn thành:** 2026-10-26
- **Kết quả đạt được:**
  - Hoàn thiện bộ thu thập [`loadgen/metrics.py`](loadgen/metrics.py) kết hợp Prometheus exporter của DataNode (`:9091/metrics`), Linux Cgroup v2 (`/sys/fs/cgroup/cpu.stat`, `memory.current`, `io.stat` kèm lệnh `sync` đồng bộ page cache trước khi chốt số đo) và API dự phòng cAdvisor (`:8080`).
  - Xây dựng công cụ trích xuất log DataNode theo offset byte từng lần chạy [`scripts/parse_iotdb_logs.py`](scripts/parse_iotdb_logs.py), bóc tách chính xác từng tệp `sequence`/`unsequence` sinh ra khi Flush (`TsFileProcessor`) và chi tiết `CrossSpaceCompactionTask` (`CHUNK_OVERLAP_OR_MODIFIED`, `PAGE_OVERLAP_OR_MODIFIED`) lẫn `InnerSpaceCompactionTask`.
  - Định nghĩa schema dữ liệu chuẩn 62 cột trong [`docs/data_schema.md`](docs/data_schema.md) (có unit test đối chiếu tự động 1-1 giữa tài liệu và code trong [`tests/test_metrics_and_logs.py`](tests/test_metrics_and_logs.py) — tổng cộng **19/19 unit tests PASSED**).
  - Kiểm thử tích hợp trực tiếp trên IoTDB đang chạy (`30.000` bản ghi, `batch_size=500`, `num_clients=2`): ở `ooo_ratio=0%` sinh `12` tệp `seq` và `0` tệp `unseq` (`comp_cross=0`, `wa_storage=2.482`); ở `ooo_ratio=50%` sinh `10` tệp `seq` + `4` tệp `unseq`, kích hoạt `2` tác vụ `CrossSpaceCompaction` với `72` chunk chồng lấp, làm `wa_storage` tăng lên `3.250` (+30.9%) và `wa_disk` tăng từ `2.901` lên `3.820` (+31.7%).
- **Lưu ý kỹ thuật cho Giai đoạn 4:**
  - Bộ lập lịch `SizeTieredCompactionSelector` của IoTDB có thể kích hoạt gộp các tệp `seq` nhỏ sau 1–2 chu kỳ lập lịch (`2.0s`/chu kỳ) ngay sau khi `CrossSpaceCompaction` nhả khóa tệp; đã nâng ngưỡng chờ tĩnh trong `flush_and_quiesce` lên 4 nhịp liên tiếp (~`4.4s` không phát sinh tác vụ mới) để bắt trọn mọi hoạt động nền của lần chạy.

---

## Giai đoạn 4: Ma trận thí nghiệm — [HOÀN THÀNH]
- **Thời gian hoàn thành:** 2026-10-27
- **Kết quả đạt được:**
  - Chạy xong đợt **Pilot** (`data/processed/pilot_runs.csv`, 28 lần chạy trải khắp `batch_size` từ `1` đến `50.000`, `num_clients` ∈ `{1, 4, 16}`, `ooo_ratio` ∈ `{0%, 50%, 100%}`), đo thực tế thời gian nạp và hệ số biến thiên (`wa_storage` CV `< 4.4%`, thông lượng CV `6.4%–69%` khi khối lượng quá nhỏ).
  - Hiệu chỉnh thiết kế ma trận chính thức từ số liệu Pilot trong [`scripts/run_matrix.py`](scripts/run_matrix.py):
    1. **Ma trận chính 2 chiều Full-Factorial (`main_matrix`):** Đủ `6 × 6 = 36` tổ hợp (`ooo_ratio` ∈ `{0, 10, 25, 50, 75, 100}%` × `batch_size` ∈ `{1, 10, 100, 1.000, 10.000, 50.000}` tại `num_clients=4`, `delay_dist="exponential"`, `delay_mean=25.000 ms`). Khối lượng dữ liệu cố định `100.000` bản ghi cho `batch_size >= 100`; giảm có kiểm soát và chuẩn hóa theo khối lượng cho hai ô bị nghẽn RPC (`25.000` bản ghi cho `batch_size=10` và `12.000` bản ghi cho `batch_size=1` ở `c=4` để vẫn vượt ngưỡng tự động flush `2.500` điểm/chuỗi).
    2. **Ma trận đồng thời 3 chiều Full-Factorial (`concurrency_study`):** Tổ hợp cân bằng `3 × 3 × 3 = 27` ô (`ooo_ratio` ∈ `{0, 50, 100}%` × `batch_size` ∈ `{100, 1.000, 50.000}` × `num_clients` ∈ `{1, 4, 16}`, cố định `100.000` bản ghi) cộng thêm dải co giãn RPC đơn lẻ (`batch_size=1` qua `num_clients` ∈ `{1, 4, 16}`).
    3. **Thí nghiệm phụ phân bố độ trễ (`delay_dist_study`):** `3 × 3 = 9` ô (`exponential`, `lognormal`, `pareto` × `ooo_ratio` ∈ `{25, 50, 75}%` ở cùng kỳ vọng `25.000 ms`).
    4. **Thí nghiệm phụ độ trễ trung bình (`delay_mean_study`):** 6 mức `{200, 1.000, 5.000, 25.000, 75.000, 200.000} ms` quét qua ngưỡng cửa sổ lô client (`10.000 ms`) và ngưỡng Flush MemTable (`25.000 ms`).
  - Thực thi trọn vẹn **67 tổ hợp duy nhất × 10 lần lặp độc lập (10 seed khác nhau `101..1010`) = 670 lần chạy** bằng lệnh duy nhất `make experiment`, thứ tự thực thi được ngẫu nhiên hóa toàn cục (`data/processed/matrix_manifest.json`).
  - Kiểm chứng thực tế cơ chế **Resume** chịu lỗi: lần chạy thứ `623` (`conc_o050_b100_c16_exp_d25000_r05_s606`) gặp ngắt kết nối socket tạm thời (`TSocket read 0 bytes`), được ghi nhận tự động vào `data/raw/failed_runs.jsonl` mà không mất dữ liệu của `669` lần chạy còn lại; khi gọi lại `make experiment`, hệ thống khôi phục và chạy bù đúng 1 lần chạy còn thiếu để đạt **670/670 lần chạy hoàn chỉnh** (`0` lỗi RPC trong tập kết quả cuối `data/processed/runs.csv` và `data/processed/runs.parquet`).
- **Vấn đề tồn đọng:** Không có.

---

## Giai đoạn 5: Phân tích thống kê & Trực quan hóa — [HOÀN THÀNH]
- **Thời gian hoàn thành:** 2026-10-27
- **Kết quả đạt được:**
  - Xây dựng và thực thi hoàn chỉnh từ đầu [`notebooks/analysis.ipynb`](notebooks/analysis.ipynb) qua `jupyter nbconvert --execute` không phát sinh lỗi, xuất bảng tổng hợp 67 tổ hợp (`data/processed/summary_by_cell.csv`) kèm cả hai khoảng tin cậy 95% theo **Student-$t$** ($df=9$) và **Percentile Bootstrap** ($B=4.000$), cùng kết quả kiểm định mô hình (`data/processed/statistical_models.json`).
  - Kết xuất trọn bộ **7 biểu đồ khoa học 300 DPI** có thanh sai số 95% CI tại `report/figures/`:
    1. `fig1_cost_vs_ooo_ratio.png`: WA Storage, WA Disk, thông lượng và độ trễ `p95` theo `ooo_ratio`.
    2. `fig2_cost_vs_batch_size.png`: Chi phí WA, CPU chuẩn hóa, thông lượng và độ trễ trên 5 bậc độ lớn `batch_size` (`1` đến `50.000`).
    3. `fig3_cost_vs_num_clients.png`: Co giãn đồng thời `num_clients` ∈ `{1, 4, 16}` và tương tác phân mảnh lô.
    4. `fig4_heatmap_ooo_x_batch.png`: Heatmap tương tác 2 chiều đủ `36` ô `ooo_ratio × batch_size`.
    5. `fig5_lsm_internals_decomposition.png`: Phân rã thành phần `WAL + Seq Flush + Unseq Flush + Compaction` và đối chiếu `CHUNK_OVERLAP_OR_MODIFIED` từ log DataNode.
    6. `fig6_delay_dist_comparison.png`: So sánh 3 phân bố `exponential`, `lognormal`, `pareto` ở cùng kỳ vọng trễ.
    7. `fig7_delay_mean_threshold.png`: Bước nhảy phi tuyến khi `delay_mean` vượt cửa sổ lô `NumpyTablet` (`10s`) và cửa sổ Flush `MemTable` (`25s`).
  - Định lượng đầy đủ bằng hồi quy tuyến tính/bậc hai, hồi quy thang $\log_2-\log_2$ (hệ số nhân khi nhân đôi tham số kèm 95% CI) và kiểm định ANOVA đa nhân tố Type-II (xác nhận tương tác rất mạnh `ooo_ratio × batch_size` với $F(25, 324) = 15.05, p = 1.47 \times 10^{-40}$ và tương tác bậc ba `ooo_ratio × batch_size × num_clients` với $F(8, 243) = 3.44, p = 9.08 \times 10^{-4}$).
- **Vấn đề tồn đọng:** Không có.

---

## Giai đoạn 6: Báo cáo và Đóng gói — [HOÀN THÀNH]
- **Thời gian hoàn thành:** 2026-10-27
- **Kết quả đạt được:**
  - Hoàn thành báo cáo khoa học tiếng Việt [`report/report.md`](report/report.md) đầy đủ các phần: Tóm tắt, Cơ sở lý thuyết (LSM-tree, `sequence`/`unsequence`, `TVList`, `Inner`/`Cross` Compaction, `NumpyTablet` kèm trích dẫn bài báo PVLDB, tài liệu chính thức 1.3.3 và dòng mã nguồn), Thiết kế thí nghiệm, Kết quả thực nghiệm (mọi số liệu lấy trực tiếp từ 670 lần chạy thật kèm 95% CI), Thảo luận cơ chế nội tại, Hạn chế, Kết luận & Khuyến nghị kỹ thuật, Tài liệu tham khảo.
  - Kết xuất bản báo cáo đóng gói Microsoft Word [`report/report.docx`](report/report.docx) (`2,28 MB`, nhúng trọn bộ 7 biểu đồ 300 DPI và các bảng số liệu định dạng chuẩn) bằng [`scripts/build_report_docx.py`](scripts/build_report_docx.py).
  - Hoàn thiện hướng dẫn cài đặt và tái lập thí nghiệm từ đầu trong [`README.md`](README.md), kiểm tra lại toàn bộ 6 tiêu chí hoàn thành (`22/22` unit tests qua, `smoke_test` qua, notebook chạy sạch từ đầu, không rò rỉ khóa/mật khẩu, cấu hình IoTDB bất biến xuyên suốt ma trận).
- **Vấn đề tồn đọng:** Không có — toàn bộ đề tài đã hoàn thành trọn vẹn từ Giai đoạn 0 đến Giai đoạn 6.
