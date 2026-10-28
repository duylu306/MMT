# Đặc Tả Schema Tập Dữ Liệu Thực Nghiệm (`data_schema.md`)

Mọi lần chạy thí nghiệm đều xuất dữ liệu theo hai tầng:
1. **Tầng tổng hợp (`data/processed/runs.csv` và `data/processed/runs.parquet`):** Mỗi dòng tương ứng chính xác **một lần chạy (`run_id`)**, chứa đầy đủ tham số đầu vào, định danh phiên bản, mốc thời gian và các chỉ số chi phí ghi đầu-cuối.
2. **Tầng nguyên bản chi tiết theo `run_id` (`data/raw/`):**
   - `data/raw/requests/{run_id}_requests.parquet`: Nhật ký từng lệnh RPC gửi từ các client.
   - `data/raw/logs/{run_id}_iotdb_log_summary.json`: Kết quả trích xuất chi tiết từ nhật ký DataNode (`log_datanode_all.log` & `log_datanode_compaction.log`) của riêng lần chạy đó.
   - `data/raw/metrics/{run_id}_snapshots.json`: Snapshot bộ đếm Prometheus & Linux Cgroup v2 đầu/cuối và chuỗi mẫu theo giây.

---

## 1. Schema Bảng Tổng Hợp Kết Quả (`runs.csv` / `runs.parquet`)

### 1.1. Định danh, Môi trường & Tham số Đầu vào
| Tên cột | Kiểu dữ liệu | Đơn vị | Mô tả |
| :--- | :---: | :---: | :--- |
| `run_id` | `string` | — | Mã định danh duy nhất của lần chạy (ví dụ: `main_r0042_s10042`) |
| `experiment_group` | `string` | — | Nhóm thí nghiệm: `pilot`, `main_matrix`, `concurrency_study`, `delay_dist_study`, `delay_mean_study` |
| `run_order` | `int64` | — | Thứ tự thực thi thực tế sau khi ngẫu nhiên hóa (1, 2, ...) |
| `rep_id` | `int64` | — | Chỉ số lần lặp của cùng một tổ hợp tham số (`0..9`) |
| `seed` | `int64` | — | Hạt giống sinh số ngẫu nhiên cố định của lần chạy |
| `iotdb_version` | `string` | — | Phiên bản Apache IoTDB Server (`1.3.3-standalone`) |
| `client_version` | `string` | — | Phiên bản thư viện Python `apache-iotdb` (`1.3.3`) |
| `python_version` | `string` | — | Phiên bản trình thông dịch Python chạy bộ sinh tải |
| `start_time_utc` | `string` | ISO-8601 | Thời điểm bắt đầu pha ghi (UTC) |
| `ingest_end_time_utc` | `string` | ISO-8601 | Thời điểm hoàn tất toàn bộ RPC ghi từ phía client (UTC) |
| `end_time_utc` | `string` | ISO-8601 | Thời điểm kết thúc sau khi `FLUSH` và ổn định compaction nền (UTC) |
| `total_records` | `int64` | bản ghi | Tổng số bản ghi dữ liệu logic nạp vào hệ thống |
| `num_measurements` | `int64` | chuỗi | Số cảm biến mỗi thiết bị (`6`) |
| `logical_bytes` | `int64` | byte | Tổng dung lượng dữ liệu logic thuần (`total_records * 56`) |
| `ooo_ratio` | `float64` | `[0..1]` | Tỷ lệ dữ liệu sai thứ tự mục tiêu (`0.0` đến `1.0`) |
| `empirical_ooo_ratio` | `float64` | `[0..1]` | Tỷ lệ bản ghi vi phạm đơn điệu thực tế đo trên luồng sinh ra |
| `delay_dist` | `string` | — | Phân bố độ trễ đến muộn (`exponential`, `lognormal`, `pareto`) |
| `delay_mean_ms` | `float64` | ms | Kỳ vọng lý thuyết của độ trễ đến muộn |
| `batch_size` | `int64` | bản ghi | Kích thước lô mỗi lệnh RPC (`1` dùng `insert_record`; `>1` dùng `NumpyTablet`) |
| `num_clients` | `int64` | luồng | Số luồng biên gửi đồng thời (mỗi luồng 1 thiết bị độc lập) |

### 1.2. Hiệu năng Phía Client (Thông lượng & Độ trễ)
| Tên cột | Kiểu dữ liệu | Đơn vị | Mô tả |
| :--- | :---: | :---: | :--- |
| `ingest_duration_s` | `float64` | giây | Thời gian thực hiện pha nạp dữ liệu tích cực từ client |
| `settle_duration_s` | `float64` | giây | Thời gian chờ `FLUSH` cuối + hoàn tất compaction nền về trạng thái tĩnh |
| `total_duration_s` | `float64` | giây | Tổng thời gian toàn trình (`ingest_duration_s + settle_duration_s`) |
| `total_requests` | `int64` | request | Tổng số lệnh RPC đã gửi |
| `error_requests` | `int64` | request | Số lệnh RPC gặp lỗi |
| `throughput_rps` | `float64` | records/s | Thông lượng nạp phía client (`total_records / ingest_duration_s`) |
| `effective_throughput_rps` | `float64` | records/s | Thông lượng hiệu dụng toàn cục tính cả thời gian ổn định nền (`total_records / total_duration_s`) |
| `throughput_logical_mbps` | `float64` | MiB/s | Băng thông dữ liệu logic nạp vào (`logical_bytes / 2^20 / ingest_duration_s`) |
| `lat_mean_ms` | `float64` | ms | Độ trễ phản hồi trung bình mỗi request RPC |
| `lat_p50_ms` | `float64` | ms | Phân vị thứ 50 (trung vị) độ trễ ghi mỗi request |
| `lat_p95_ms` | `float64` | ms | Phân vị thứ 95 độ trễ ghi mỗi request |
| `lat_p99_ms` | `float64` | ms | Phân vị thứ 99 độ trễ ghi mỗi request |
| `lat_max_ms` | `float64` | ms | Độ trễ cực đại mỗi request |
| `lat_per_1k_rows_p50_ms` | `float64` | ms/1k rows | Độ trễ chuẩn hóa trên 1.000 bản ghi tại phân vị p50 |

### 1.3. Chỉ số Lưu trữ TsFile, Flush, Compaction & Write Amplification
| Tên cột | Kiểu dữ liệu | Đơn vị | Mô tả |
| :--- | :---: | :---: | :--- |
| `flushed_seq_files` | `int64` | tệp | Tổng số `TsFile` không gian `sequence` được tạo bởi tiến trình Flush trong lần chạy |
| `flushed_unseq_files` | `int64` | tệp | Tổng số `TsFile` không gian `unsequence` được tạo bởi tiến trình Flush trong lần chạy |
| `flushed_seq_bytes` | `int64` | byte | Tổng dung lượng các tệp `sequence` sinh ra ngay khi Flush |
| `flushed_unseq_bytes` | `int64` | byte | Tổng dung lượng các tệp `unsequence` sinh ra ngay khi Flush |
| `peak_seq_files` | `int64` | tệp | Số tệp `seq` hoạt động cực đại quan sát qua Prometheus trong lần chạy |
| `peak_unseq_files` | `int64` | tệp | Số tệp `unseq` hoạt động cực đại quan sát qua Prometheus trong lần chạy |
| `final_seq_files` | `int64` | tệp | Số tệp `seq` còn lại trên đĩa sau khi kết thúc ổn định |
| `final_unseq_files` | `int64` | tệp | Số tệp `unseq` còn lại trên đĩa sau khi kết thúc ổn định |
| `final_seq_bytes` | `int64` | byte | Tổng kích thước tệp `seq` cuối cùng từ `file_global_size{name="seq"}` |
| `final_unseq_bytes` | `int64` | byte | Tổng kích thước tệp `unseq` cuối cùng từ `file_global_size{name="unseq"}` |
| `flush_tasks_delta` | `int64` | lần | Số tác vụ Flush thực thi (`cost_task_seconds_count{name="flush"}` $\Delta$) |
| `flush_auto_count_delta` | `int64` | lần | Số lần MemTable tự động flush do đầy ngưỡng điểm/bộ nhớ (không tính lệnh `FLUSH` cuối) |
| `flush_time_s_delta` | `float64` | giây | Tổng thời gian thực thi các tác vụ Flush |
| `compaction_inner_seq_count` | `int64` | lần | Số tác vụ Inner Sequence Compaction đã chạy |
| `compaction_inner_unseq_count` | `int64` | lần | Số tác vụ Inner Unsequence Compaction đã chạy |
| `compaction_cross_count` | `int64` | lần | Số tác vụ Cross-Space Compaction (`seq` + `unseq`) đã chạy |
| `compaction_total_count` | `int64` | lần | Tổng số tác vụ Compaction mọi loại |
| `compaction_total_time_s` | `float64` | giây | Tổng thời gian CPU/I/O thực thi Compaction (`cost_task_seconds_sum` $\Delta$) |
| `compaction_cross_time_s` | `float64` | giây | Tổng thời gian thực thi riêng của Cross-Space Compaction |
| `compaction_read_bytes` | `int64` | byte | Tổng byte đọc bởi Compaction (`sum(data_read_total)` $\Delta$) |
| `compaction_written_bytes` | `int64` | byte | Tổng byte ghi ra bởi Compaction (`sum(data_written_total)` $\Delta$) |
| `compaction_overlap_chunks` | `int64` | chunk | Tổng số `CHUNK_OVERLAP_OR_MODIFIED` phải giải nén/hợp nhất (trích từ log compaction) |
| `compaction_overlap_pages` | `int64` | page | Tổng số `PAGE_OVERLAP_OR_MODIFIED` phải giải mã lại (trích từ log compaction) |
| `wal_written_bytes` | `int64` | byte | Tổng byte ghi vào bộ đệm WAL (`wal_buffer_sum{name="wrote_wal_buffer_size"}` $\Delta$) |
| `wal_fsync_count` | `int64` | lần | Số lần gọi `fsync` của WAL xuống đĩa |
| `wal_fsync_time_s` | `float64` | giây | Tổng thời gian chờ `fsync` của WAL |
| `wa_disk` | `float64` | tỷ số | **Write Amplification mức đĩa vật lý:** `cgroup_disk_write_bytes / logical_bytes` |
| `wa_storage` | `float64` | tỷ số | **Write Amplification mức Storage Engine:** `(wal_written_bytes + flushed_seq_bytes + flushed_unseq_bytes + compaction_written_bytes) / logical_bytes` |
| `wa_tsfile_rewrite` | `float64` | tỷ số | **Hệ số ghi đè TsFile do Compaction:** `(flushed_seq_bytes + flushed_unseq_bytes + compaction_written_bytes) / max(1, flushed_seq_bytes + flushed_unseq_bytes)` |

### 1.4. Tài Nguyên Container (`iotdb-service`)
| Tên cột | Kiểu dữ liệu | Đơn vị | Mô tả |
| :--- | :---: | :---: | :--- |
| `datanode_cpu_seconds` | `float64` | core·s | Thời gian CPU tiêu thụ bởi JVM DataNode (`process_cpu_time / 1e9` $\Delta$) |
| `container_cpu_seconds` | `float64` | core·s | Tổng thời gian CPU của container `iotdb-service` từ Cgroup v2 (`cpu.stat usage_usec / 1e6` $\Delta$) |
| `container_cpu_avg_cores` | `float64` | cores | Số nhân CPU sử dụng trung bình trong suốt lần chạy (`container_cpu_seconds / total_duration_s`) |
| `cpu_ms_per_1k_records` | `float64` | ms/1k rows | Chi phí CPU chuẩn hóa trên mỗi 1.000 bản ghi logic |
| `datanode_heap_avg_mb` | `float64` | MiB | Bộ nhớ Heap trung bình đang sử dụng của DataNode trong lần chạy |
| `datanode_heap_peak_mb` | `float64` | MiB | Bộ nhớ Heap đỉnh của DataNode trong lần chạy |
| `container_ram_avg_mb` | `float64` | MiB | Working Set RAM trung bình của container `iotdb-service` |
| `container_ram_peak_mb` | `float64` | MiB | Working Set RAM cực đại của container `iotdb-service` |
| `cgroup_disk_read_bytes` | `int64` | byte | Tổng byte đọc khối thực tế từ đĩa của container (`io.stat rbytes` $\Delta$) |
| `cgroup_disk_write_bytes` | `int64` | byte | Tổng byte ghi khối thực tế xuống đĩa của container (`io.stat wbytes` $\Delta$) |
| `cgroup_disk_read_iops_total` | `int64` | ops | Tổng số thao tác đọc khối (`io.stat rios` $\Delta$) |
| `cgroup_disk_write_iops_total` | `int64` | ops | Tổng số thao tác ghi khối (`io.stat wios` $\Delta$) |

---

## 2. Schema Tệp Nhật Ký Từng Request (`{run_id}_requests.parquet`)

| Tên cột | Kiểu dữ liệu | Mô tả |
| :--- | :---: | :--- |
| `run_id` | `string` | Mã lần chạy |
| `client_id` | `int32` | Chỉ số luồng biên (`0 .. num_clients-1`) |
| `request_idx` | `int32` | Thứ tự request trong phạm vi luồng client |
| `send_unix_ms` | `int64` | Thời điểm bắt đầu gửi request (epoch ms) |
| `batch_rows` | `int32` | Số bản ghi gửi trong request này |
| `logical_bytes` | `int32` | Số byte dữ liệu logic trong request này (`batch_rows * 56`) |
| `latency_ms` | `float64` | Thời gian phản hồi đầu-cuối của lệnh ghi (bao gồm đóng gói `NumpyTablet` + RPC) |
| `is_error` | `bool` | `True` nếu lệnh gặp ngoại lệ / mã lỗi từ server |
| `error_msg` | `string` | Nội dung lỗi (chuỗi rỗng `""` nếu thành công) |
