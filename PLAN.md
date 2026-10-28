# KẾ HOẠCH THỰC HIỆN ĐỀ TÀI (PLAN)

**Đề tài:** Đo lường thực nghiệm ảnh hưởng của thứ tự và kích thước dữ liệu đến chi phí ghi vào Apache IoTDB  
**Ngày lập kế hoạch:** 2026-10-26  

---

## 1. Mục tiêu và Phạm vi
Xây dựng hệ thống đo lường thực nghiệm đầu-cuối (end-to-end), tái lập được bằng Docker Compose, nhằm định lượng ảnh hưởng của:
1. Tỷ lệ dữ liệu sai thứ tự (`ooo_ratio`),
2. Phân bố và độ trễ trung bình của dữ liệu đến muộn (`delay_dist`, `delay_mean`),
3. Kích thước lô ghi (`batch_size`),
4. Số luồng máy biên gửi đồng thời (`num_clients`),

lên **chi phí ghi thực tế** của Apache IoTDB (thông lượng, độ trễ `p50`/`p95`/`p99`, số lượng/kích thước `TsFile` sequence & unsequence, số lần `flush`, hoạt động `compaction`, hệ số khuếch đại ghi — *Write Amplification*, CPU, RAM và Disk I/O).

---

## 2. Cấu trúc thư mục dự kiến
```text
.
├── PLAN.md                     # Kế hoạch thực hiện, giả định, rủi ro
├── PROGRESS.md                 # Nhật ký kết quả và tồn đọng qua từng giai đoạn
├── README.md                   # Hướng dẫn dựng hạ tầng và chạy lại toàn bộ từ đầu
├── Makefile                    # Lệnh điều phối tự động (up, smoke, test, pilot, experiment, analyze, report)
├── requirements.txt            # Ghim phiên bản toàn bộ thư viện Python
├── docker-compose.yml          # Điều phối IoTDB, Prometheus, Grafana, cAdvisor, Loadgen
├── docker/                     # Cấu hình IoTDB, Prometheus, Grafana provisioning + dashboard, Dockerfile loadgen
├── docs/
│   ├── environment.md          # Cấu hình phần cứng thực tế, container limits, tham số bộ nhớ/lưu trữ IoTDB
│   ├── workload_model.md       # Định nghĩa toán học của ooo_ratio, delay_dist, delay_mean, batching, seed
│   └── data_schema.md          # Đặc tả schema tập dữ liệu thực nghiệm (CSV/Parquet)
├── loadgen/                    # Package Python sinh tải, thu thập metric, quản lý vòng đời thí nghiệm
├── scripts/                    # Smoke test, log parser IoTDB, runner ma trận thí nghiệm, xuất báo cáo PDF/DOCX
├── tests/                      # Unit tests kiểm chứng bộ sinh tải (pytest)
├── data/
│   ├── raw/                    # Log từng request, snapshot metric, log IoTDB trích xuất theo run_id
│   └── processed/              # Bảng tổng hợp kết quả thực nghiệm (runs.csv, runs.parquet)
├── notebooks/
│   └── analysis.ipynb          # Phân tích thống kê, kiểm định/hồi quy, sinh biểu đồ và bảng số liệu
└── report/
    ├── report.md               # Báo cáo khoa học đầy đủ bằng tiếng Việt
    ├── report.docx             # Bản đóng gói Word xuất tự động từ số liệu thật
    └── figures/                # Biểu đồ chất lượng cao (PNG/PDF) sinh từ notebook
```

---

## 3. Lộ trình các giai đoạn

| Giai đoạn | Nội dung chính | Tiêu chí kiểm tra trước khi sang giai đoạn kế tiếp |
| :--- | :--- | :--- |
| **GĐ 0. Lập kế hoạch** | Khảo sát tài nguyên máy chủ, tra cứu cấu hình IoTDB 1.3.x, lập `PLAN.md` và `PROGRESS.md`. | `PLAN.md` nêu rõ lộ trình, giả định và rủi ro. |
| **GĐ 1. Hạ tầng Docker** | Dựng `docker-compose.yml` với phiên bản ghim cứng (`apache/iotdb:1.3.3-standalone`, Prometheus, Grafana, cAdvisor, Loadgen); bật Prometheus reporter trên IoTDB; viết `docs/environment.md` và `scripts/smoke_test.py`. | `docker compose up -d` chạy ổn định; Prometheus scrape được metric IoTDB & cAdvisor; Grafana có sẵn datasource + dashboard; smoke test ghi/đọc thành công. |
| **GĐ 2. Bộ sinh tải** | Hiện thực `loadgen/` dùng `apache-iotdb==1.3.3` (`Session`); hỗ trợ `ooo_ratio`, `delay_dist` (`exponential`, `lognormal`, `pareto`), `delay_mean`, `batch_size`, `num_clients`, warm-up, cleanup, settle/quiesce; viết `docs/workload_model.md` và `tests/`. | Toàn bộ unit test (`pytest`) đạt 100%: đúng tỷ lệ OOO, đúng kỳ vọng/hình dạng phân bố trễ, đúng `batch_size`, tái lập tuyệt đối theo `seed`. |
| **GĐ 3. Đo lường** | Thu thập throughput, latency (`p50`, `p95`, `p99`), TsFile `seq`/`unseq` count & size, flush count/time, compaction count/time/read/write bytes, Write Amplification, CPU/RAM/Disk I/O container, parser đọc log IoTDB theo `run_id`; viết `docs/data_schema.md`. | Chạy thử 1 run đầu-cuối tạo ra đúng 1 dòng hợp lệ theo schema CSV/Parquet + tệp chi tiết theo `run_id`, không thiếu cột trọng yếu. |
| **GĐ 4. Ma trận thí nghiệm** | Chạy Pilot để đo thời gian và hệ số biến thiên (CV); chốt thiết kế thực nghiệm (ma trận chính + thí nghiệm riêng cho `delay_dist` và `delay_mean`, lặp $\ge 10$ lần/tổ hợp với ngẫu nhiên hóa thứ tự chạy, hỗ trợ resume); tích hợp vào 1 lệnh chạy duy nhất (`make experiment`). | Tập dữ liệu `data/processed/runs.csv` và `runs.parquet` thu đủ 100% số lần chạy cam kết, không lỗi hỏng hoặc thiếu dữ liệu. |
| **GĐ 5. Phân tích** | Xây dựng `notebooks/analysis.ipynb`: tính mean, SD, 95% CI; vẽ biểu đồ error bar & heatmap tương tác; hồi quy tuyến tính/log-log + ANOVA tương tác; xác định ngưỡng phi tuyến; đối chiếu cơ chế LSM-tree/IoTDB với metric thu được. | Chạy lại notebook từ đầu (`nbconvert --execute`) thành công, xuất đầy đủ bảng và hình vào `report/figures/`. |
| **GĐ 6. Báo cáo & Đóng gói** | Viết `report/report.md` kèm xuất bản `.docx`/`.pdf`, hoàn thiện `README.md`, rà soát toàn bộ tiêu chí nghiệm thu và bảo mật. | Báo cáo khớp 100% số liệu thực chạy; hướng dẫn `README.md` rõ ràng, tái lập được trong vài lệnh. |

---

## 4. Các giả định thiết kế (Assumptions)

1. **Phiên bản phần mềm ghim cứng:**
   - Apache IoTDB: `apache/iotdb:1.3.3-standalone` (chế độ 1 ConfigNode + 1 DataNode tích hợp chuẩn single-node của nhánh ổn định 1.3.x) và thư viện Python `apache-iotdb==1.3.3`.
   - Prometheus: `prom/prometheus:v2.54.1`; Grafana: `grafana/grafana:11.2.2`; cAdvisor: `gcr.io/cadvisor/cadvisor:v0.49.1`.
2. **Schema dữ liệu chuỗi thời gian mô phỏng máy biên:**
   - Database: `root.bench`. Mỗi luồng client $c \in \{0, \dots, \text{num\_clients}-1\}$ đại diện cho 1 cổng biên độc lập quản lý thiết bị `root.bench.edge_{c}` gồm $M = 6$ cảm biến kiểu `DOUBLE` (`s0`..`s5`), mô phỏng gói đo công nghiệp điển hình (1 timestamp `INT64` 8 byte + $6 \times 8 = 48$ byte giá trị = **56 byte dữ liệu logic thuần / bản ghi**).
3. **Định nghĩa dữ liệu logic cố định & chuẩn hóa:**
   - Trong ma trận chính, tổng số bản ghi dữ liệu logic mỗi lần chạy được cố định là $N_{\text{total}}$ chia đều cho `num_clients` ($N_{\text{client}} = N_{\text{total}} / \text{num\_clients}$), giúp tổng dung lượng logic $B_{\text{logical}} = N_{\text{total}} \times 56\text{ bytes}$ không đổi khi so sánh `ooo_ratio`, `batch_size`, `num_clients`.
   - Riêng với `batch_size = 1` (gửi từng bản ghi đơn lẻ qua RPC tuần tự tốn chi phí mạng khổng lồ), nếu Pilot cho thấy thời gian chạy toàn tải quá dài, sẽ áp dụng thiết kế giảm khối lượng có kiểm soát cho `batch_size = 1` (hoặc tách ma trận con full-factorial + OFAT theo hướng dẫn đề bài) và chuẩn hóa chi phí trên mỗi MB / mỗi 1.000 bản ghi logic, nêu rõ trong báo cáo.
4. **Định nghĩa `ooo_ratio` và mô hình độ trễ đến muộn:**
   - Chuỗi thời gian gốc sinh với bước mẫu đều $\Delta t = 10\text{ ms}$. Với tỷ lệ $p = \text{ooo\_ratio} \in [0, 1)$, đúng $K = \text{round}(p \cdot N_{\text{client}})$ bản ghi được chọn ngẫu nhiên (theo `seed`) bị trễ mạng $D_i > 0$ sinh từ `delay_dist` có kỳ vọng lý thuyết $\mathbb{E}[D] = \text{delay\_mean}$, và thời điểm đến trung tâm là $a_i = t_i + D_i$. Các bản ghi được gửi theo thứ tự tăng dần của $a_i$. Vì bản ghi không bị trễ ($D_j = 0$) không bao giờ vi phạm tính đơn điệu trước các bản ghi đến sớm hơn nó, tỷ lệ bản ghi thực tế vi phạm thứ tự thời gian ($\tau_k < \max_{j<k} \tau_j$) xấp xỉ sát $p$. Tại biên $p = 1.0$ (`100%`), luồng timestamp được hoán vị ngẫu nhiên đồng đều (Fisher–Yates theo `seed`) đúng nghĩa "xáo trộn hoàn toàn".
5. **Phương thức ghi theo `batch_size`:**
   - Khi `batch_size = 1`: sử dụng `Session.insert_record()` (ghi 1 bản ghi đơn lẻ mỗi RPC).
   - Khi `batch_size > 1`: sử dụng `Session.insert_tablet()` với `NumpyTablet` kích thước đúng bằng `batch_size` bản ghi mỗi RPC. Lưu ý trong `insert_tablet`, yêu cầu của API IoTDB `Tablet` là mảng timestamp bên trong cùng 1 tablet phải được sắp xếp tăng dần; do đó mỗi lô `batch_size` bản ghi liên tiếp tại đầu thu sẽ được sắp xếp cục bộ trong phạm vi lô trước khi đóng gói `NumpyTablet` (phản ánh đúng hành vi gom lô thực tế của agent biên và yêu cầu giao thức `Tablet` của IoTDB).
6. **Cấu hình ngưỡng Flush của IoTDB để quan sát rõ hiệu ứng LSM-tree:**
   - Với tập dữ liệu quy mô vừa trong môi trường thực nghiệm cục bộ, nếu giữ mặc định `avg_series_point_number_threshold = 100000` và MemTable hàng trăm MB, toàn bộ dữ liệu của 1 lần chạy ngắn có thể nằm trọn trong RAM (`TVList`) mà chưa kịp flush xuống đĩa trong lúc ghi. Do đó, cấu hình IoTDB sẽ được thiết lập ngưỡng flush hợp lý (cố định tuyệt đối xuyên suốt mọi lần chạy trong ma trận) để quá trình ghi thực sự sinh ra nhiều `TsFile` và kích hoạt cơ chế phân tách `sequence`/`unsequence` cùng `compaction` giống hệ thống vận hành dài hạn.
7. **Định nghĩa Write Amplification (WA):**
   - Thu thập đồng thời 2 chỉ số bổ trợ nhau:
     - $\text{WA}_{\text{disk}} = \frac{\Delta \text{DiskWriteBytes}}{\text{LogicalPayloadBytes}}$: tổng số byte thực tế tiến trình IoTDB ghi xuống tầng lưu trữ (đo từ Linux kernel `/proc/<pid>/io` `write_bytes` và cAdvisor/Docker block I/O, bao gồm cả WAL + Flush TsFile + Compaction) chia cho tổng byte dữ liệu logic thuần.
     - $\text{WA}_{\text{storage}} = \frac{\Delta \text{FlushWrittenBytes} + \Delta \text{CompactionWrittenBytes}}{\text{LogicalPayloadBytes}}$ (tính từ metric nội bộ IoTDB và log).

---

## 5. Rủi ro và Biện pháp giảm thiểu

| Rủi ro | Mức độ | Biện pháp giảm thiểu |
| :--- | :---: | :--- |
| **Giới hạn RAM máy chủ thực tế** (Tổng RAM 13.86 GB, trống ~3–5 GB khi chạy Windows + WSL2) | Cao | Giới hạn cứng tài nguyên container trong `docker-compose.yml` (ví dụ IoTDB tối đa 3 GB RAM / 4 vCPU, Heap DataNode 1.5 GB + Direct 512 MB; Prometheus/Grafana nhẹ) để tránh OOM hoặc swap làm nhiễu độ trễ. |
| **Compaction chạy bất đồng bộ sau khi client ngừng gửi** làm thiếu hụt chi phí ghi nếu chốt số đo quá sớm | Cao | Thiết kế pha **Settle / Quiesce** sau mỗi lần chạy: gửi lệnh `FLUSH` tường minh (hoặc chờ tự nhiên + kiểm tra hàng đợi flush/compaction qua Prometheus metric về 0 trong khoảng ổn định) trước khi thu hoạch metric cuối, đồng thời tách rõ thời gian ghi phía client (ingestion throughput) và tổng chi phí ghi toàn cục (gồm cả compaction nền). |
| **Nhiễu hiệu năng giữa các lần chạy liên tiếp** (page cache, JVM JIT, nhiệt CPU, rác dữ liệu cũ) | Trung bình | (1) Có bước **Warm-up** kích hoạt JIT trước khi đo; (2) Xóa sạch `DELETE DATABASE root.**` và xác nhận số file `seq`/`unseq` về 0 trước mỗi run; (3) **Ngẫu nhiên hóa hoàn toàn thứ tự chạy** (randomized run order) trên toàn ma trận. |
| **Sai lệch metric Disk I/O của cAdvisor trên Docker Desktop (WSL2)** | Trung bình | Kết hợp cAdvisor với việc đọc trực tiếp `/proc/<datanode_pid>/io` (`read_bytes`, `write_bytes`, `wchar`) bên trong container IoTDB trước và sau mỗi run để đảm bảo độ chính xác tuyệt đối ở mức tiến trình nhân Linux. |
| **Thời gian chạy ma trận bùng nổ tại `batch_size = 1`** (hàng trăm nghìn lệnh RPC tuần tự) | Cao | Chạy **Pilot** ở đầu Giai đoạn 4 để đo tốc độ thực tế (records/s tại `batch_size=1`); nếu vượt ngân sách thời gian hợp lý, dùng thiết kế kết hợp (Full Factorial cho `batch_size >= 100` + chuỗi chuyên biệt cho `batch_size ∈ {1, 10}` hoặc chuẩn hóa quy mô tải theo ô) kèm cơ chế checkpoint/resume từng `run_id`. |
