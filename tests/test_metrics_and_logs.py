"""Unit tests for Prometheus metric parser and IoTDB DataNode log parser."""

from __future__ import annotations

from pathlib import Path
import re

from loadgen.metrics import parse_iotdb_datanode_metrics
from scripts.parse_iotdb_logs import parse_iotdb_log_texts

EXPECTED_RUN_COLUMNS = [
    "run_id",
    "experiment_group",
    "run_order",
    "rep_id",
    "seed",
    "iotdb_version",
    "client_version",
    "python_version",
    "start_time_utc",
    "ingest_end_time_utc",
    "end_time_utc",
    "total_records",
    "num_measurements",
    "logical_bytes",
    "ooo_ratio",
    "empirical_ooo_ratio",
    "delay_dist",
    "delay_mean_ms",
    "batch_size",
    "num_clients",
    "ingest_duration_s",
    "settle_duration_s",
    "total_duration_s",
    "total_requests",
    "error_requests",
    "throughput_rps",
    "effective_throughput_rps",
    "throughput_logical_mbps",
    "lat_mean_ms",
    "lat_p50_ms",
    "lat_p95_ms",
    "lat_p99_ms",
    "lat_max_ms",
    "lat_per_1k_rows_p50_ms",
    "flushed_seq_files",
    "flushed_unseq_files",
    "flushed_seq_bytes",
    "flushed_unseq_bytes",
    "peak_seq_files",
    "peak_unseq_files",
    "final_seq_files",
    "final_unseq_files",
    "final_seq_bytes",
    "final_unseq_bytes",
    "flush_tasks_delta",
    "flush_auto_count_delta",
    "flush_time_s_delta",
    "compaction_inner_seq_count",
    "compaction_inner_unseq_count",
    "compaction_cross_count",
    "compaction_total_count",
    "compaction_total_time_s",
    "compaction_cross_time_s",
    "compaction_read_bytes",
    "compaction_written_bytes",
    "compaction_overlap_chunks",
    "compaction_overlap_pages",
    "wal_written_bytes",
    "wal_fsync_count",
    "wal_fsync_time_s",
    "wa_disk",
    "wa_storage",
    "wa_tsfile_rewrite",
    "datanode_cpu_seconds",
    "container_cpu_seconds",
    "container_cpu_avg_cores",
    "cpu_ms_per_1k_records",
    "datanode_heap_avg_mb",
    "datanode_heap_peak_mb",
    "container_ram_avg_mb",
    "container_ram_peak_mb",
    "cgroup_disk_read_bytes",
    "cgroup_disk_write_bytes",
    "cgroup_disk_read_iops_total",
    "cgroup_disk_write_iops_total",
]


def test_data_schema_doc_matches_expected_columns() -> None:
    doc_path = Path(__file__).resolve().parents[1] / "docs" / "data_schema.md"
    sec1_text = doc_path.read_text(encoding="utf-8").split("## 2. Schema")[0]
    doc_cols = re.findall(r"^\|\s*`([a-z0-9_]+)`\s*\|", sec1_text, flags=re.MULTILINE)
    assert doc_cols == EXPECTED_RUN_COLUMNS


def test_parse_iotdb_datanode_metrics_exact_1_3_3_format() -> None:
    sample_metrics = """
# TYPE file_global_count gauge
file_global_count{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="seq",database="root.bench",region="1",} 5
file_global_count{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="unseq",database="root.bench",region="1",} 2
file_global_count{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="seq",database="root.other",region="2",} 99
file_global_size{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="seq",database="root.bench",region="1",} 1048576
file_global_size{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="unseq",database="root.bench",region="1",} 262144
cost_task_seconds_count{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="flush",} 12
cost_task_seconds_sum{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="flush",} 0.45
cost_task_seconds_count{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="cross_compaction",} 3
cost_task_seconds_sum{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="cross_compaction",} 1.25
data_read_total{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",type="cross_compaction",name="not_aligned",} 500000
data_written_total{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",type="cross_compaction",name="not_aligned",} 520000
data_written_total{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",type="cross_compaction",name="metadata",} 15000
wal_buffer_sum{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="wrote_wal_buffer_size",} 2000000
queue{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="compaction_cross",status="running",} 1.0
process_cpu_time{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="process",} 2.5E10
process_used_mem{cluster="defaultCluster",nodeType="DATANODE",nodeId="1",name="process",} 3.145728E8
"""
    snap = parse_iotdb_datanode_metrics(sample_metrics, database="root.bench")
    assert snap.seq_files == 5
    assert snap.unseq_files == 2
    assert snap.seq_bytes == 1048576
    assert snap.unseq_bytes == 262144
    assert snap.flush_tasks_total == 12
    assert snap.flush_time_seconds_total == 0.45
    assert snap.compaction_cross_count == 3
    assert snap.compaction_cross_time_s == 1.25
    assert snap.compaction_read_bytes_total == 500000
    assert snap.compaction_written_bytes_total == 535000
    assert snap.wal_written_bytes_total == 2000000
    assert snap.active_or_waiting_bg_tasks == 1
    assert snap.datanode_cpu_ns_total == 2.5e10
    assert snap.datanode_used_heap_bytes == 314572800.0


def test_parse_iotdb_logs_flush_and_compaction() -> None:
    dn_all = (
        "2026-10-26 16:17:58,084 [pool-6-IoTDB-Flush-1] INFO  o.a.i.d.s.d.m.TsFileProcessor:1588 - "
        "The compression ratio of tsfile /iotdb/data/datanode/data/sequence/root.bench/1/2810/179303-1-0-0.tsfile "
        "is 2.47, totalMemTableSize: 120000, the file size: 48582 \n"
        "2026-10-26 16:17:58,235 [pool-6-IoTDB-Flush-2] INFO  o.a.i.d.s.d.m.TsFileProcessor:1588 - "
        "The compression ratio of tsfile /iotdb/data/datanode/data/unsequence/root.bench/1/2810/179304-2-0-0.tsfile "
        "is 1.12, totalMemTableSize: 60000, the file size: 53571 \n"
        "2026-10-26 16:17:58,500 [pool-6-IoTDB-Flush-3] INFO  o.a.i.d.s.d.m.TsFileProcessor:1588 - "
        "The compression ratio of tsfile /iotdb/data/datanode/data/sequence/root.other/1/2810/179305-1-0-0.tsfile "
        "is 2.00, totalMemTableSize: 99999, the file size: 49999 \n"
    )
    dn_comp = (
        "2026-10-26 16:17:59,207 [pool-19-IoTDB-Compaction-Worker-1] INFO  o.a.i.d.s.d.c.e.t.CrossSpaceCompactionTask:248 - "
        "root.bench-1 [Compaction] CrossSpaceCompaction task finishes successfully, time cost is 0.42 s, "
        "compaction speed is 12.50 MB/s, CHUNK_NONE_OVERLAP num is 2, CHUNK_NONE_OVERLAP_BUT_DESERIALIZE num is 0, "
        "CHUNK_OVERLAP_OR_MODIFIED num is 18, PAGE_NONE_OVERLAP num is 4, PAGE_NONE_OVERLAP_BUT_DESERIALIZE num is 3, "
        "PAGE_OVERLAP_OR_MODIFIED num is 25, PAGE_FAKE_OVERLAP num is 1. \n"
        "2026-10-26 16:40:47,232 [pool-19-IoTDB-Compaction-Worker-1] INFO  o.a.i.d.s.d.c.e.t.InnerSpaceCompactionTask:261 - "
        "root.bench-1 [Compaction] Sequence InnerSpaceCompaction task finishes successfully, target files are "
        "[{file: /iotdb/data/datanode/data/sequence/root.bench/1/2810/179303-1-1-1.tsfile, status: COMPACTING}],time cost is 0.11 s, "
        "compaction speed is 5.64 MB/s, Task start time: 10/26/26, 4:40 PM, total process chunk num: 30, rewrite point num: 90000 \n"
    )
    summary = parse_iotdb_log_texts("test_run_01", "root.bench", dn_all, dn_comp)
    assert summary.flushed_seq_files == 1
    assert summary.flushed_seq_bytes == 48582
    assert summary.flushed_unseq_files == 1
    assert summary.flushed_unseq_bytes == 53571
    assert summary.log_cross_compaction_count == 1
    assert summary.log_inner_compaction_count == 1
    assert abs(summary.log_compaction_time_s - 0.53) < 1e-6
    assert summary.compaction_overlap_chunks == 18
    assert summary.compaction_overlap_pages == 25
    assert summary.compaction_non_overlap_chunks == 2
