# Đo Lường Thực Nghiệm Ảnh Hưởng Của Thứ Tự Và Kích Thước Dữ Liệu Đến Chi Phí Ghi Vào Apache IoTDB

Kho mã nguồn này cung cấp toàn bộ hạ tầng thực nghiệm đóng gói bằng Docker, bộ sinh tải chuỗi thời gian đến sai thứ tự (out-of-order) có tái lập theo hạt giống (`seed`), công cụ thu thập số đo đa tầng (Prometheus + Linux Cgroup v2 + trình phân tích nhật ký DataNode), tập dữ liệu kết quả thực nghiệm, Jupyter Notebook phân tích thống kê và báo cáo khoa học đầy đủ cho hệ quản trị cơ sở dữ liệu chuỗi thời gian **Apache IoTDB 1.3.3**.

---

## 1. Cấu Trúc Thư Mục

```text
├── docker-compose.yml              # Dựng trọn bộ IoTDB 1.3.3, Prometheus, Grafana, cAdvisor, Loadgen
├── Makefile                        # Các lệnh tự động hóa toàn trình (up, smoke, test, pilot, experiment, analyze, report)
├── requirements.txt                # Danh sách thư viện Python ghim cứng phiên bản
├── requirements.lock               # Tệp khóa toàn bộ cây phụ thuộc (pip freeze)
├── PLAN.md                         # Kế hoạch thực hiện, giả định thiết kế và quản trị rủi ro
├── PROGRESS.md                     # Nhật ký kết quả kiểm chứng qua từng giai đoạn (Giai đoạn 0 -> 6)
├── docker/
│   ├── iotdb/                      # Cấu hình bộ nhớ JVM, MemTable, Flush, Compaction & Prometheus Exporter
│   ├── prometheus/                 # Cấu hình thu thập metric 5 giây/lần
│   ├── grafana/                    # Provisioning tự động Datasource & Dashboard giám sát chi phí ghi
│   └── loadgen/                    # Dockerfile đóng gói bộ sinh tải Python 3.12.5
├── loadgen/
│   ├── workload.py                 # Mô hình sinh chuỗi thời gian sai thứ tự xác định theo seed (PCG64)
│   ├── metrics.py                  # Bộ thu thập metric Prometheus (:9091), Cgroup v2 & cAdvisor (:8080)
│   └── client_runner.py            # Trình nạp đa luồng (Session/NumpyTablet), warm-up, cleanup & quiesce
├── scripts/
│   ├── smoke_test.py               # Kiểm tra thông suốt đọc/ghi IoTDB + Prometheus + Grafana
│   ├── parse_iotdb_logs.py         # Trích xuất chi tiết tệp TsFile Flush & Chunk Overlap từ log DataNode
│   ├── run_matrix.py               # Điều phối ma trận thí nghiệm ngẫu nhiên hóa thứ tự, hỗ trợ resume
│   └── build_report_docx.py        # Kết xuất báo cáo Markdown + biểu đồ sang định dạng Word (.docx)
├── docs/
│   ├── environment.md              # Đặc tả chi tiết phần cứng, giới hạn container, JVM và tên metric
│   ├── workload_model.md           # Định nghĩa toán học của ooo_ratio, 3 phân bố độ trễ và bất biến dữ liệu
│   └── data_schema.md              # Đặc tả 62 cột dữ liệu của tập kết quả chuẩn hóa
├── tests/                          # Bộ kiểm thử đơn vị (pytest) cho bộ sinh tải, parser và cơ chế resume
├── data/
│   ├── raw/                        # Nhật ký từng RPC (.parquet), snapshot Prometheus/Cgroup & log theo run_id
│   └── processed/                  # Tập dữ liệu tổng hợp (runs.csv, runs.parquet, summary_by_cell.csv)
├── notebooks/
│   └── analysis.ipynb              # Phân tích thống kê (95% CI, hồi quy OLS/log-log, ANOVA) & vẽ biểu đồ
└── report/
    ├── figures/                    # Toàn bộ biểu đồ độ phân giải cao (300 DPI)
    ├── report.md                   # Báo cáo khoa học chi tiết bằng tiếng Việt
    └── report.docx                 # Bản báo cáo đóng gói định dạng Microsoft Word
```

---

## 2. Yêu Cầu Hệ Thống

- **Docker Engine** ($\ge 24.0$) và **Docker Compose v2** (`docker compose`), cấp tối thiểu **4 vCPU** và **5 GB RAM** cho Docker VM.
- **Python 3.12+** (nếu chạy bộ điều phối và phân tích trực tiếp từ máy chủ để đọc trực tiếp Cgroup v2 qua `docker exec`; hoặc có thể chạy qua container `loadgen`).
- **GNU Make** (tùy chọn; mọi lệnh `make` đều có lệnh `python` tương đương bên dưới).

> **Lưu ý về tính tương đối của số liệu tuyệt đối:** Các kết quả thông lượng và độ trễ trong kho mã được đo trên cấu hình máy thực tế mô tả trong [`docs/environment.md`](docs/environment.md) (`iotdb-service` giới hạn `4 vCPU`, `3072 MiB RAM`, DataNode `1536 MiB` On-Heap + `512 MiB` Off-Heap trên ổ NVMe SSD). Khi chạy lại trên phần cứng khác, giá trị tuyệt đối sẽ thay đổi theo năng lực CPU/I/O nhưng các quy luật tỷ lệ về Write Amplification, phân tách `sequence`/`unsequence` và ngưỡng cửa sổ thời gian vẫn giữ nguyên bản chất.

---

## 3. Hướng Dẫn Cài Đặt Và Chạy Lại Từ Đầu (Trong Vài Lệnh)

### Bước 1: Tạo môi trường Python & cài đặt phụ thuộc đã ghim phiên bản

```bash
python -m venv .venv
# Trên Linux / macOS:
source .venv/bin/activate
# Trên Windows PowerShell:
.\.venv\Scripts\Activate.ps1

pip install -r requirements.lock
```

### Bước 2: Khởi động hạ tầng Docker & kiểm tra Smoke Test

```bash
# Dựng IoTDB 1.3.3, Prometheus (cổng 9090), Grafana (cổng 3000) và cAdvisor (cổng 8080)
docker compose up -d iotdb-service cadvisor prometheus grafana

# Xác nhận ghi/đọc IoTDB, Prometheus targets UP và Grafana Dashboard hoạt động
python -m scripts.smoke_test
```

*(Hoặc chạy kiểm tra hoàn toàn từ bên trong mạng Docker bằng container `loadgen`: `docker compose run --rm loadgen`)*

- Giao diện **Prometheus**: `http://localhost:9090`
- Giao diện **Grafana**: `http://localhost:3000` (đăng nhập xem chế độ Anonymous Viewer hoặc `admin` / biến `GF_SECURITY_ADMIN_PASSWORD`, mở sẵn dashboard **Apache IoTDB Write Amplification & LSM-Tree Cost**).

### Bước 3: Chạy bộ kiểm thử đơn vị (Unit Tests)

```bash
python -m pytest tests/ -v
# Hoặc: make test
```

### Bước 4: Chạy thí nghiệm thăm dò (Pilot) và Ma trận thí nghiệm đầy đủ

```bash
# (Tùy chọn) Chạy ma trận Pilot (28 lần chạy) để ước lượng thời gian và hệ số biến thiên
python -m scripts.run_matrix --mode pilot

# Chạy toàn bộ ma trận thực nghiệm chính thức (67 tổ hợp x 10 lần lặp = 670 lần chạy, tự động ngẫu nhiên hóa thứ tự)
make experiment
# Hoặc tương đương:
python -m scripts.run_matrix --mode full
```

- **Cơ chế Resume an toàn:** Kết quả mỗi lần chạy (`run_id`) được ghi bền vững tức thời vào `data/processed/runs.csv` và `data/processed/runs.parquet`. Nếu quá trình chạy bị ngắt quãng giữa chừng, chỉ cần gõ lại `make experiment` — hệ thống sẽ tự động bỏ qua các `run_id` đã hoàn tất và chỉ thực thi tiếp phần còn lại theo đúng thứ tự ngẫu nhiên hóa ban đầu trong `data/processed/matrix_manifest.json`.

### Bước 5: Tái tạo phân tích thống kê, biểu đồ và báo cáo Word

```bash
# Chạy lại từ đầu toàn bộ Jupyter Notebook, xuất bảng thống kê và 7 biểu đồ vào report/figures/
python -m jupyter nbconvert --to notebook --execute --inplace notebooks/analysis.ipynb

# Xuất báo cáo report/report.md (kèm biểu đồ và bảng biểu) sang report/report.docx
python -m scripts.build_report_docx
```

### Bước 6: Dọn dẹp hạ tầng sau khi hoàn tất

```bash
docker compose down
# Nếu muốn xóa luôn cả Docker Named Volumes chứa dữ liệu và log của IoTDB/Prometheus/Grafana:
docker compose down -v
```
