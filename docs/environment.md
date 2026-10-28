# Đặc Tả Môi Trường Thực Nghiệm (`environment.md`)

Tài liệu này ghi nhận đầy đủ thông số phần cứng vật lý, môi trường ảo hóa Linux container, phiên bản phần mềm ghim cứng, cấu hình bộ nhớ JVM/MemTable, lưu trữ và hệ thống giám sát Prometheus/Grafana dùng trong toàn bộ nghiên cứu thực nghiệm.

> **Lưu ý về tính khái quát hóa:** Mọi số liệu đo được trong báo cáo gắn liền với cấu hình phần cứng, nhân hệ điều hành và giới hạn tài nguyên container nêu dưới đây. Khi tái lập trên phần cứng khác, giá trị tuyệt đối của thông lượng/độ trễ có thể thay đổi nhưng xu hướng tỷ lệ (scaling trend) và cơ chế Write Amplification của LSM-tree có thể đối chiếu trực tiếp.

---

## 1. Cấu hình máy chủ vật lý & Nền tảng Container

| Thành phần | Thông số thực tế |
| :--- | :--- |
| **CPU vật lý** | AMD Ryzen 5 5600H with Radeon Graphics @ 3294 MHz (6 nhân vật lý / 12 luồng logic, kiến trúc Zen 3) |
| **RAM vật lý máy chủ** | 13.86 GiB tổng (Windows 10/11 Build 26200, x86_64) |
| **Ổ cứng vật lý** | NVMe PCIe SSD (`HFM256GDJTNI-82A0A` 256 GB NVMe & `SAMSUNG MZALQ256HBJD-00BL2` 256 GB NVMe); phân vùng làm việc `E:\` còn trống ~51.3 GiB |
| **Môi trường Docker Engine** | Docker Engine `29.2.1` (build `a5c7197`), Docker Compose `v5.1.0` |
| **Nhân Linux (WSL2 VM)** | `6.18.54.1-microsoft-standard-WSL2` (`x86_64`), Cgroup v2 (`cgroupfs`), Storage Driver `overlayfs` |
| **Tài nguyên cấp cho máy ảo Docker** | 12 vCPU, 6.7 GiB RAM |
| **Python môi trường phân tích** | Python `3.12.5` (kèm `requirements.txt` và `requirements.lock` ghim toàn bộ gói phụ thuộc) |

---

## 2. Danh mục Container và Giới hạn Tài nguyên (`docker-compose.yml`)

Tất cả image đều được ghim phiên bản cụ thể (không dùng `latest`), kết nối qua mạng cầu nối riêng `iotdb-write-cost-net`:

| Service | Container Name | Image ghim cứng | Giới hạn CPU | Giới hạn RAM (`mem_limit`) | Cổng ánh xạ |
| :--- | :--- | :--- | :---: | :---: | :--- |
| `iotdb-service` | `iotdb-service` | `apache/iotdb:1.3.3-standalone` (`sha256:a62e4a2d...`) | `4.0` vCPU | `3072 MiB` (không swap: `memswap_limit: 3072m`) | `6667` (RPC), `9091` (DN Metrics), `9092` (CN Metrics) |
| `cadvisor` | `iotdb-cadvisor` | `gcr.io/cadvisor/cadvisor:v0.49.1` | `0.5` vCPU | `256 MiB` | `8080` |
| `prometheus` | `iotdb-prometheus` | `prom/prometheus:v2.54.1` | `1.0` vCPU | `512 MiB` | `9090` |
| `grafana` | `iotdb-grafana` | `grafana/grafana:11.2.2` | `0.5` vCPU | `256 MiB` | `3000` |
| `loadgen` | `iotdb-loadgen` | `python:3.12.5-slim-bookworm` (`iotdb-write-cost-loadgen:1.0.0`) | `4.0` vCPU | `1536 MiB` | Nội bộ |

### Kiến trúc Volume Lưu trữ
- Dữ liệu và nhật ký của IoTDB được gắn vào **Docker Named Volumes** (`iotdb_exp_data -> /iotdb/data`, `iotdb_exp_logs -> /iotdb/logs`) nằm trực tiếp trên hệ thống tệp **`ext4` của nhân Linux WSL2 đặt trên ổ cứng vật lý NVMe SSD**.
- Thiết kế này tránh hoàn toàn tầng chuyển tiếp tệp mạng `drvfs`/`9p` giữa Windows và Linux, đảm bảo:
  1. Thao tác `fsync` của WAL, ghi tuần tự `TsFile` khi Flush và đọc/ghi khi Compaction diễn ra đúng ngữ nghĩa hệ thống tệp `ext4` gốc trên NVMe SSD;
  2. Bộ đếm I/O mức nhân Linux (`/proc/<datanode_pid>/io`: `read_bytes`, `write_bytes`, `rchar`, `wchar`) và cAdvisor phản ánh chính xác lượng byte đọc/ghi khối vật lý của tiến trình DataNode.

---

## 3. Cấu hình Bộ nhớ và Lưu trữ của Apache IoTDB 1.3.3

Toàn bộ cấu hình được quản lý mã nguồn tại [`docker/iotdb/iotdb-system.properties`](../docker/iotdb/iotdb-system.properties), [`docker/iotdb/datanode-env.sh`](../docker/iotdb/datanode-env.sh) và [`docker/iotdb/confignode-env.sh`](../docker/iotdb/confignode-env.sh), giữ **cố định tuyệt đối** xuyên suốt mọi lần chạy thí nghiệm:

### 3.1. Bộ nhớ JVM (`datanode-env.sh` & `confignode-env.sh`)
Trong nhánh IoTDB 1.3.x, biến `MEMORY_SIZE` tự động phân bổ `ON_HEAP_MEMORY` (3/4 khi `< 4 GiB`) và `OFF_HEAP_MEMORY` (1/4):

| Tiến trình | `MEMORY_SIZE` | `ON_HEAP_MEMORY` (`-Xms`/`-Xmx`) | `OFF_HEAP_MEMORY` (`MaxDirectMemorySize`) | Ghi chú |
| :--- | :---: | :---: | :---: | :--- |
| **DataNode** | `2048M` | `1536M` | `512M` | Xử lý toàn bộ RPC ghi, MemTable, Flush và Compaction |
| **ConfigNode** | `512M` | `384M` | `128M` | Quản lý metadata phân vùng và schema vùng |
| **Tổng cộng** | `2560M` | `1920M` | `640M` | Dự phòng `512 MiB` cho bộ đệm OS & native threads trong giới hạn `3072 MiB` |

### 3.2. Phân bổ vùng nhớ bên trong DataNode (`iotdb-system.properties`)
- `datanode_memory_proportion = 3:3:1:1:1:1`:
  - `StorageEngine` chiếm $3/10 \times 1536\text{ MiB} = 460.8\text{ MiB}$.
  - `QueryEngine` chiếm $460.8\text{ MiB}$; `SchemaEngine`, `Consensus`, `Streaming`, `Free` mỗi phần $153.6\text{ MiB}$.
- `storage_engine_memory_proportion = 8:2`:
  - Vùng ghi (`Write`): $0.8 \times 460.8 = 368.64\text{ MiB}$; Vùng `Compaction`: $92.16\text{ MiB}$.
- `write_memory_proportion = 19:1`:
  - Tổng quỹ `MemTable`: $350.2\text{ MiB}$; `flush_proportion = 0.4` ($\approx 140.1\text{ MiB}$), `reject_proportion = 0.8` ($\approx 280.2\text{ MiB}$).

### 3.3. Cấu hình MemTable, TsFile, WAL và Compaction

| Nhóm | Tên tham số chính thức | Giá trị đặt | Ý nghĩa kỹ thuật |
| :--- | :--- | :--- | :--- |
| **Phân tách dữ liệu** | `enable_separate_data` | `true` | Tách luồng ghi thành không gian `sequence` và `unsequence` |
| **Sắp xếp bộ nhớ** | `tvlist_sort_algorithm` | `TIM` | Thuật toán TimSort cho mảng `TVList` trong MemTable |
| **Ngưỡng Flush** | `avg_series_point_number_threshold` | `2500` | Kích hoạt flush MemTable xuống TsFile khi trung bình đạt 2.500 điểm/chuỗi, đảm bảo mỗi lần chạy sinh nhiều TsFile thực tế |
| **Luồng Flush** | `flush_thread_count` | `4` | Số luồng ghi MemTable xuống đĩa song song |
| **WAL** | `wal_mode` | `ASYNC` | Ghi nhật ký trước bất đồng bộ (`wal_buffer_size_in_byte = 32 MiB`, `wal_async_mode_fsync_delay_in_ms = 1000`, `enable_wal_compression = false`) |
| **TsFile & Nén** | `target_chunk_size` / `page_size_in_byte` / `compressor` | `1 MiB` / `64 KiB` / `LZ4` | Kích thước chunk mục tiêu, trang dữ liệu và bộ nén mặc định LZ4 |
| **Compaction bật** | `enable_seq_space_compaction`, `enable_unseq_space_compaction`, `enable_cross_space_compaction` | `true` | Bật đầy đủ cả 3 cơ chế: Seq Inner, Unseq Inner và Cross-Space (Seq + Unseq) |
| **Ưu tiên & Lập lịch** | `compaction_priority` / `compaction_schedule_interval_in_ms` | `BALANCE` / `2000` | Cân bằng giữa Inner & Cross compaction; quét lập lịch mỗi 2 giây |
| **Ngưỡng chọn file** | `inner_compaction_candidate_file_num` / `min_cross_compaction_unseq_file_level` | `10` / `0` | Gom Inner khi đủ 10 TsFile; cho phép Cross-Space gom ngay các tệp `unseq` mức 0 vừa flush |
| **Luồng Compaction** | `compaction_thread_count` / `sub_compaction_thread_count` | `4` / `2` | 4 luồng thực thi compaction chính, 2 luồng con mỗi tác vụ; không bóp băng thông (`compaction_write_throughput_mb_per_sec = 0`) |

---

## 4. Cấu hình Giám sát Metric (IoTDB Prometheus Exporter & cAdvisor)

### 4.1. Tham số cấu hình trong `iotdb-system.properties`
```properties
cn_metric_reporter_list=PROMETHEUS
cn_metric_level=IMPORTANT
cn_metric_async_collect_period=2
cn_metric_prometheus_reporter_port=9092

dn_metric_reporter_list=PROMETHEUS
dn_metric_level=NORMAL
dn_metric_async_collect_period=2
dn_metric_prometheus_reporter_port=9091
dn_metric_internal_reporter_type=MEMORY
```

### 4.2. Các Endpoint và Chu kỳ thu thập (`prometheus.yml`)
- **IoTDB DataNode Metrics:** `http://iotdb-service:9091/metrics` (`scrape_interval: 2s`)
- **IoTDB ConfigNode Metrics:** `http://iotdb-service:9092/metrics` (`scrape_interval: 5s`)
- **cAdvisor Container Metrics:** `http://cadvisor:8080/metrics` (`scrape_interval: 2s`)
- **Grafana Provisioning:** Datasource `Prometheus` (`uid: prometheus_iotdb`) và Dashboard `Apache IoTDB Write Amplification & LSM-Tree Cost` được nạp tự động tại `http://localhost:3000`.

### 4.3. Danh mục tên Metric chính xác đã xác minh trên `apache/iotdb:1.3.3-standalone`
Các tên metric dưới đây đã được đối chiếu trực tiếp từ đầu ra thực tế của `http://localhost:9091/metrics` và `http://localhost:8080/metrics`:

- **Số lượng & kích thước TsFile / WAL hiện hành:**
  - `file_global_count{name="seq"|"unseq", database="...", region="..."}`
  - `file_global_size{name="seq"|"unseq", database="...", region="..."}`
  - `file_count{name="wal"|"mods"|"cross-temp"|"inner-seq-temp"|"inner-unseq-temp"}`
  - `file_size{name="wal"|"mods"|"cross-temp"|"inner-seq-temp"|"inner-unseq-temp"}`
- **Hoạt động Flush:**
  - `flush_memtable_count_total{type="series_full_flush_memtable"|"mem_control_flush_memtable_count"|"manual_flush_memtable_count"|"timed_flush_memtable_count"|"wal_flush_memtable_count"}`
  - `cost_task_seconds_count{name="flush"}` và `cost_task_seconds_sum{name="flush"}`
  - `queue{name="flush", status="running"|"waiting"}`
- **Hoạt động Compaction (Inner Seq, Inner Unseq, Cross Space):**
  - `cost_task_seconds_count{name="inner_seq_compaction"|"inner_unseq_compaction"|"cross_compaction"}`
  - `cost_task_seconds_sum{name="inner_seq_compaction"|"inner_unseq_compaction"|"cross_compaction"}`
  - `data_read_total{type="inner_seq_compaction"|"inner_unseq_compaction"|"cross_compaction", name="not_aligned"|"aligned"|"metadata"}`
  - `data_written_total{type="inner_seq_compaction"|"inner_unseq_compaction"|"cross_compaction", name="not_aligned"|"aligned"|"metadata"}`
  - `queue{name="compaction_inner_seq"|"compaction_inner_unseq"|"compaction_cross", status="running"|"waiting"}`
- **Nhật ký WAL & Tài nguyên tiến trình / Container:**
  - `wal_buffer_sum{name="wrote_wal_buffer_size"|"serialized_wal_buffer_size"}` và `wal_cost_seconds_sum{stage="sync_wal_buffer", type="fsync"}`
  - `process_cpu_time{name="process"}` (nanoseconds), `process_used_mem{name="process"}` (bytes)
  - cAdvisor (Cgroup v2): `container_cpu_usage_seconds_total{id="/docker/<cid>"}`, `container_memory_working_set_bytes{id="/docker/<cid>"}`, `container_fs_reads_bytes_total{device="/dev/sdd", id="/docker/<cid>"}`, `container_fs_writes_bytes_total{device="/dev/sdd", id="/docker/<cid>"}`
  - Cgroup v2 trực tiếp trong container `iotdb-service`: `/sys/fs/cgroup/io.stat` (`rbytes`, `wbytes`, `rios`, `wios`), `/sys/fs/cgroup/cpu.stat` (`usage_usec`), `/sys/fs/cgroup/memory.current`.
