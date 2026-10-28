"""Multi-threaded Apache IoTDB benchmark runner with warm-up, cleanup, and quiesce."""

from __future__ import annotations

import importlib.metadata
import json
import os
import platform
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import Compressor, TSDataType, TSEncoding
from iotdb.utils.NumpyTablet import NumpyTablet

from loadgen.metrics import MetricsCollector, SystemSnapshot
from loadgen.workload import (
    ClientStream,
    WorkloadConfig,
    generate_client_stream,
    iter_batches,
)
from scripts.parse_iotdb_logs import extract_and_save_run_logs, get_container_log_offsets

IOTDB_SERVER_VERSION = "1.3.3-standalone"


def _get_client_version() -> str:
    try:
        return importlib.metadata.version("apache-iotdb")
    except Exception:  # noqa: BLE001
        return "1.3.3"


def run_warmup(
    host: str,
    port: int,
    user: str,
    password: str,
    warmup_records: int = 8_000,
) -> None:
    """Warm up DataNode JVM JIT, Thrift pools, MemTable TVList sorting, and Cross-Compaction."""
    warmup_db = "root.warmup"
    cfg = WorkloadConfig(
        total_records=warmup_records,
        ooo_ratio=0.50,
        delay_dist="exponential",
        delay_mean_ms=5_000.0,
        batch_size=500,
        num_clients=2,
        seed=7,
        database=warmup_db,
    )
    session = Session(host, str(port), user, password)
    session.open(False)
    try:
        try:
            session.delete_storage_group(warmup_db)
        except Exception:  # noqa: BLE001
            pass
        session.set_storage_group(warmup_db)

        data_types = [TSDataType.DOUBLE] * cfg.num_measurements
        for c in range(cfg.num_clients):
            stream = generate_client_stream(cfg, c)
            # Also exercise insert_record path once
            session.insert_record(
                stream.device_id,
                int(stream.timestamps[0]),
                stream.measurements,
                data_types,
                [float(col[0]) for col in stream.values],
            )
            for _, b_ts, b_vals in iter_batches(stream, cfg.batch_size):
                ts_arr = np.array(b_ts, dtype=TSDataType.INT64.np_dtype(), copy=True)
                val_arrs = [
                    np.array(col, dtype=TSDataType.DOUBLE.np_dtype(), copy=True)
                    for col in b_vals
                ]
                tablet = NumpyTablet(
                    stream.device_id, stream.measurements, data_types, val_arrs, ts_arr
                )
                session.insert_tablet(tablet)

        session.execute_non_query_statement(f"FLUSH {warmup_db}")
        time.sleep(2.5)
        ds = session.execute_query_statement(f"SELECT count(s0) FROM {warmup_db}.edge_0")
        while ds.has_next():
            ds.next()
        ds.close_operation_handle()
        session.delete_storage_group(warmup_db)
        time.sleep(1.5)
    finally:
        session.close()


def reset_and_prepare_database(
    host: str,
    port: int,
    user: str,
    password: str,
    config: WorkloadConfig,
    collector: MetricsCollector,
) -> None:
    """Delete previous database, wait for clean file state, recreate database and pre-register timeseries."""
    session = Session(host, str(port), user, password)
    session.open(False)
    try:
        try:
            session.delete_storage_group(config.database)
        except Exception:  # noqa: BLE001
            pass

        # Wait until IoTDB finishes deleting old region TsFiles and background queues drain
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline:
            snap = collector.take_iotdb_snapshot()
            if (
                snap.seq_files == 0
                and snap.unseq_files == 0
                and snap.active_or_waiting_bg_tasks == 0
            ):
                break
            time.sleep(0.4)

        session.set_storage_group(config.database)

        ts_paths: List[str] = []
        for c in range(config.num_clients):
            for m in range(config.num_measurements):
                ts_paths.append(f"{config.database}.edge_{c}.s{m}")
        n_ts = len(ts_paths)
        session.create_multi_time_series(
            ts_paths,
            [TSDataType.DOUBLE] * n_ts,
            [TSEncoding.GORILLA] * n_ts,
            [Compressor.LZ4] * n_ts,
        )
        # Brief pause so SchemaRegion Raft metadata write settles before baseline snapshot
        time.sleep(0.25)
    finally:
        session.close()


def flush_and_quiesce(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    collector: MetricsCollector,
    timeout_s: float = 60.0,
) -> None:
    """Explicitly flush remaining MemTables and wait until background compactions stabilize."""
    session = Session(host, str(port), user, password)
    session.open(False)
    try:
        session.execute_non_query_statement(f"FLUSH {database}")
    finally:
        session.close()

    # Require >= 4.3s (> 2 full 2.0s scheduler cycles) of unbroken zero active/waiting
    # tasks and unchanging cumulative task counts so both Cross-Space and subsequent
    # Inner-Space compactions finish before taking the final snapshot.
    init_snap = collector.take_iotdb_snapshot()
    prev_total_tasks = (
        init_snap.flush_tasks_total
        + init_snap.compaction_inner_seq_count
        + init_snap.compaction_inner_unseq_count
        + init_snap.compaction_cross_count
    )
    stable_since = time.monotonic() if init_snap.active_or_waiting_bg_tasks == 0 else None
    deadline = time.monotonic() + timeout_s

    while time.monotonic() < deadline:
        time.sleep(0.5)
        snap = collector.take_iotdb_snapshot()
        cur_total_tasks = (
            snap.flush_tasks_total
            + snap.compaction_inner_seq_count
            + snap.compaction_inner_unseq_count
            + snap.compaction_cross_count
        )
        if snap.active_or_waiting_bg_tasks == 0 and cur_total_tasks == prev_total_tasks:
            if stable_since is None:
                stable_since = time.monotonic()
            elif (time.monotonic() - stable_since) >= 4.3:
                return
        else:
            prev_total_tasks = cur_total_tasks
            stable_since = (
                time.monotonic() if snap.active_or_waiting_bg_tasks == 0 else None
            )


def _client_worker(
    stream: ClientStream,
    batch_size: int,
    host: str,
    port: int,
    user: str,
    password: str,
    start_barrier: threading.Barrier,
    out_records: List[Tuple[int, int, int, int, int, float, bool, str]],
) -> None:
    """Worker thread sending batches for one edge device and recording per-RPC telemetry."""
    session = Session(host, str(port), user, password)
    session.open(False)
    data_types = [TSDataType.DOUBLE] * len(stream.measurements)
    ts_dtype = TSDataType.INT64.np_dtype()
    val_dtype = TSDataType.DOUBLE.np_dtype()
    row_bytes = 8 + 8 * len(stream.measurements)

    try:
        batches = list(iter_batches(stream, batch_size))
        start_barrier.wait()

        for req_idx, b_ts, b_vals in batches:
            n_rows = int(b_ts.shape[0])
            send_ms = int(time.time() * 1000)
            t0 = time.perf_counter_ns()
            is_err = False
            err_msg = ""
            try:
                if batch_size == 1:
                    session.insert_record(
                        stream.device_id,
                        int(b_ts[0]),
                        stream.measurements,
                        data_types,
                        [float(col[0]) for col in b_vals],
                    )
                else:
                    # Pass copies because NumpyTablet sorts arrays if out-of-order
                    ts_arr = np.array(b_ts, dtype=ts_dtype, copy=True)
                    val_arrs = [np.array(col, dtype=val_dtype, copy=True) for col in b_vals]
                    tablet = NumpyTablet(
                        stream.device_id,
                        stream.measurements,
                        data_types,
                        val_arrs,
                        ts_arr,
                    )
                    session.insert_tablet(tablet)
            except Exception as exc:  # noqa: BLE001
                is_err = True
                err_msg = str(exc)[:200]
            lat_ms = (time.perf_counter_ns() - t0) / 1_000_000.0
            out_records.append(
                (
                    stream.client_id,
                    req_idx,
                    send_ms,
                    n_rows,
                    n_rows * row_bytes,
                    lat_ms,
                    is_err,
                    err_msg,
                )
            )
    finally:
        session.close()


def execute_single_run(
    run_id: str,
    experiment_group: str,
    run_order: int,
    rep_id: int,
    config: WorkloadConfig,
    data_root: Path = Path("data"),
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    dn_metrics_url: str | None = None,
    cadvisor_url: str | None = None,
    container_name: str = "iotdb-service",
) -> Dict[str, Any]:
    """Execute one complete benchmark run and persist raw + summary artifacts."""
    host = host or os.getenv("IOTDB_HOST", "127.0.0.1")
    port = port or int(os.getenv("IOTDB_PORT", "6667"))
    user = user or os.getenv("IOTDB_USER", "root")
    password = password or os.getenv("IOTDB_PASSWORD", "root")
    dn_metrics_url = dn_metrics_url or os.getenv(
        "IOTDB_DN_METRICS_URL", "http://127.0.0.1:9091/metrics"
    )
    cadvisor_url = cadvisor_url or os.getenv("CADVISOR_URL", "http://127.0.0.1:8080")

    collector = MetricsCollector(
        dn_metrics_url=dn_metrics_url,
        cadvisor_url=cadvisor_url,
        database=config.database,
        container_name=container_name,
    )

    # 1. Pre-generate all client streams in memory before touching timers
    streams = [generate_client_stream(config, c) for c in range(config.num_clients)]
    weighted_emp_ooo = float(
        sum(s.empirical_ooo_ratio * s.num_records for s in streams) / config.total_records
    )

    # 2. Clean database and pre-register schema
    reset_and_prepare_database(host, port, user, password, config, collector)

    # 3. Record log offsets & baseline snapshot (with OS sync)
    log_offsets = get_container_log_offsets(container_name)
    before_snap: SystemSnapshot = collector.take_snapshot(sync_fs=True)
    collector.start_polling(interval_s=1.0)

    # 4. Launch synchronized worker threads
    barrier = threading.Barrier(config.num_clients + 1)
    per_client_logs: List[List[Tuple[int, int, int, int, int, float, bool, str]]] = [
        [] for _ in range(config.num_clients)
    ]
    threads: List[threading.Thread] = []
    for c in range(config.num_clients):
        t = threading.Thread(
            target=_client_worker,
            args=(
                streams[c],
                config.batch_size,
                host,
                port,
                user,
                password,
                barrier,
                per_client_logs[c],
            ),
            name=f"iotdb-client-{c}",
        )
        t.start()
        threads.append(t)

    barrier.wait()
    start_utc = datetime.now(timezone.utc).isoformat()
    t_ingest_start = time.perf_counter()

    for t in threads:
        t.join()

    t_ingest_end = time.perf_counter()
    ingest_end_utc = datetime.now(timezone.utc).isoformat()
    ingest_duration_s = max(t_ingest_end - t_ingest_start, 1e-6)

    # 5. Explicit final FLUSH + background compaction quiesce
    flush_and_quiesce(host, port, user, password, config.database, collector)
    t_total_end = time.perf_counter()
    end_utc = datetime.now(timezone.utc).isoformat()

    collector.stop_polling()
    after_snap: SystemSnapshot = collector.take_snapshot(sync_fs=True)

    settle_duration_s = max(t_total_end - t_ingest_end, 0.0)
    total_duration_s = max(t_total_end - t_ingest_start, 1e-6)

    # 6. Extract IoTDB logs for this run_id
    raw_logs_dir = data_root / "raw" / "logs"
    log_summary = extract_and_save_run_logs(
        run_id=run_id,
        database=config.database,
        start_offsets=log_offsets,
        output_dir=raw_logs_dir,
        container_name=container_name,
    )

    # 7. Persist raw request log & raw snapshots
    flat_reqs = [row for cl in per_client_logs for row in cl]
    req_df = pd.DataFrame(
        flat_reqs,
        columns=[
            "client_id",
            "request_idx",
            "send_unix_ms",
            "batch_rows",
            "logical_bytes",
            "latency_ms",
            "is_error",
            "error_msg",
        ],
    )
    req_df.insert(0, "run_id", run_id)
    req_df["client_id"] = req_df["client_id"].astype("int32")
    req_df["request_idx"] = req_df["request_idx"].astype("int32")
    req_df["batch_rows"] = req_df["batch_rows"].astype("int32")
    req_df["logical_bytes"] = req_df["logical_bytes"].astype("int32")

    raw_req_dir = data_root / "raw" / "requests"
    raw_req_dir.mkdir(parents=True, exist_ok=True)
    req_df.to_parquet(raw_req_dir / f"{run_id}_requests.parquet", index=False)

    raw_metrics_dir = data_root / "raw" / "metrics"
    raw_metrics_dir.mkdir(parents=True, exist_ok=True)
    (raw_metrics_dir / f"{run_id}_snapshots.json").write_text(
        json.dumps(collector.export_raw_dict(before_snap, after_snap), indent=2),
        encoding="utf-8",
    )

    # 8. Compute aggregated deltas & derived metrics
    total_requests = int(len(req_df))
    error_requests = int(req_df["is_error"].sum())
    if error_requests > 0:
        sample_err = req_df.loc[req_df["is_error"], "error_msg"].iloc[0]
        raise RuntimeError(
            f"Run {run_id} encountered {error_requests}/{total_requests} failed RPCs: {sample_err}"
        )

    lat_arr = req_df["latency_ms"].to_numpy(dtype=np.float64)
    rows_arr = req_df["batch_rows"].to_numpy(dtype=np.float64)
    lat_mean_ms = float(np.mean(lat_arr))
    lat_p50_ms = float(np.percentile(lat_arr, 50))
    lat_p95_ms = float(np.percentile(lat_arr, 95))
    lat_p99_ms = float(np.percentile(lat_arr, 99))
    lat_max_ms = float(np.max(lat_arr))
    lat_per_1k_rows_p50_ms = float(np.percentile(lat_arr / np.maximum(rows_arr, 1.0) * 1000.0, 50))

    sample_stats = collector.summarize_samples(before_snap, after_snap)

    flush_tasks_delta = max(0, after_snap.flush_tasks_total - before_snap.flush_tasks_total)
    flush_auto_delta = max(
        0, after_snap.flush_auto_memtable_total - before_snap.flush_auto_memtable_total
    )
    flush_time_s_delta = max(
        0.0, after_snap.flush_time_seconds_total - before_snap.flush_time_seconds_total
    )

    comp_in_seq = max(
        0, after_snap.compaction_inner_seq_count - before_snap.compaction_inner_seq_count
    )
    comp_in_unseq = max(
        0, after_snap.compaction_inner_unseq_count - before_snap.compaction_inner_unseq_count
    )
    comp_cross = max(
        0, after_snap.compaction_cross_count - before_snap.compaction_cross_count
    )
    comp_total_count = comp_in_seq + comp_in_unseq + comp_cross

    comp_cross_time_s = max(
        0.0, after_snap.compaction_cross_time_s - before_snap.compaction_cross_time_s
    )
    comp_total_time_s = (
        max(0.0, after_snap.compaction_inner_seq_time_s - before_snap.compaction_inner_seq_time_s)
        + max(
            0.0,
            after_snap.compaction_inner_unseq_time_s - before_snap.compaction_inner_unseq_time_s,
        )
        + comp_cross_time_s
    )
    comp_read_bytes = max(
        0, after_snap.compaction_read_bytes_total - before_snap.compaction_read_bytes_total
    )
    comp_written_bytes = max(
        0, after_snap.compaction_written_bytes_total - before_snap.compaction_written_bytes_total
    )

    wal_written_bytes = max(
        0, after_snap.wal_written_bytes_total - before_snap.wal_written_bytes_total
    )
    wal_fsync_count = max(
        0, after_snap.wal_fsync_count_total - before_snap.wal_fsync_count_total
    )
    wal_fsync_time_s = max(
        0.0, after_snap.wal_fsync_time_s_total - before_snap.wal_fsync_time_s_total
    )

    datanode_cpu_seconds = max(
        0.0, (after_snap.datanode_cpu_ns_total - before_snap.datanode_cpu_ns_total) / 1e9
    )
    container_cpu_seconds = max(
        0.0, (after_snap.cgroup_cpu_usec_total - before_snap.cgroup_cpu_usec_total) / 1e6
    )
    cgroup_disk_read_bytes = max(
        0, after_snap.cgroup_rbytes_total - before_snap.cgroup_rbytes_total
    )
    cgroup_disk_write_bytes = max(
        0, after_snap.cgroup_wbytes_total - before_snap.cgroup_wbytes_total
    )
    cgroup_disk_read_iops = max(
        0, after_snap.cgroup_rios_total - before_snap.cgroup_rios_total
    )
    cgroup_disk_write_iops = max(
        0, after_snap.cgroup_wios_total - before_snap.cgroup_wios_total
    )

    logical_bytes = config.logical_bytes
    flushed_total_bytes = log_summary.flushed_seq_bytes + log_summary.flushed_unseq_bytes

    wa_disk = float(cgroup_disk_write_bytes / logical_bytes)
    wa_storage = float(
        (wal_written_bytes + flushed_total_bytes + comp_written_bytes) / logical_bytes
    )
    wa_tsfile_rewrite = float(
        (flushed_total_bytes + comp_written_bytes) / max(1, flushed_total_bytes)
    )

    return {
        "run_id": run_id,
        "experiment_group": experiment_group,
        "run_order": int(run_order),
        "rep_id": int(rep_id),
        "seed": int(config.seed),
        "iotdb_version": IOTDB_SERVER_VERSION,
        "client_version": _get_client_version(),
        "python_version": platform.python_version(),
        "start_time_utc": start_utc,
        "ingest_end_time_utc": ingest_end_utc,
        "end_time_utc": end_utc,
        "total_records": int(config.total_records),
        "num_measurements": int(config.num_measurements),
        "logical_bytes": int(logical_bytes),
        "ooo_ratio": float(config.ooo_ratio),
        "empirical_ooo_ratio": weighted_emp_ooo,
        "delay_dist": config.delay_dist,
        "delay_mean_ms": float(config.delay_mean_ms),
        "batch_size": int(config.batch_size),
        "num_clients": int(config.num_clients),
        "ingest_duration_s": float(ingest_duration_s),
        "settle_duration_s": float(settle_duration_s),
        "total_duration_s": float(total_duration_s),
        "total_requests": total_requests,
        "error_requests": error_requests,
        "throughput_rps": float(config.total_records / ingest_duration_s),
        "effective_throughput_rps": float(config.total_records / total_duration_s),
        "throughput_logical_mbps": float(logical_bytes / (1024**2) / ingest_duration_s),
        "lat_mean_ms": lat_mean_ms,
        "lat_p50_ms": lat_p50_ms,
        "lat_p95_ms": lat_p95_ms,
        "lat_p99_ms": lat_p99_ms,
        "lat_max_ms": lat_max_ms,
        "lat_per_1k_rows_p50_ms": lat_per_1k_rows_p50_ms,
        "flushed_seq_files": int(log_summary.flushed_seq_files),
        "flushed_unseq_files": int(log_summary.flushed_unseq_files),
        "flushed_seq_bytes": int(log_summary.flushed_seq_bytes),
        "flushed_unseq_bytes": int(log_summary.flushed_unseq_bytes),
        "peak_seq_files": int(sample_stats["peak_seq_files"]),
        "peak_unseq_files": int(sample_stats["peak_unseq_files"]),
        "final_seq_files": int(after_snap.seq_files),
        "final_unseq_files": int(after_snap.unseq_files),
        "final_seq_bytes": int(after_snap.seq_bytes),
        "final_unseq_bytes": int(after_snap.unseq_bytes),
        "flush_tasks_delta": int(flush_tasks_delta),
        "flush_auto_count_delta": int(flush_auto_delta),
        "flush_time_s_delta": float(flush_time_s_delta),
        "compaction_inner_seq_count": int(comp_in_seq),
        "compaction_inner_unseq_count": int(comp_in_unseq),
        "compaction_cross_count": int(comp_cross),
        "compaction_total_count": int(comp_total_count),
        "compaction_total_time_s": float(comp_total_time_s),
        "compaction_cross_time_s": float(comp_cross_time_s),
        "compaction_read_bytes": int(comp_read_bytes),
        "compaction_written_bytes": int(comp_written_bytes),
        "compaction_overlap_chunks": int(log_summary.compaction_overlap_chunks),
        "compaction_overlap_pages": int(log_summary.compaction_overlap_pages),
        "wal_written_bytes": int(wal_written_bytes),
        "wal_fsync_count": int(wal_fsync_count),
        "wal_fsync_time_s": float(wal_fsync_time_s),
        "wa_disk": wa_disk,
        "wa_storage": wa_storage,
        "wa_tsfile_rewrite": wa_tsfile_rewrite,
        "datanode_cpu_seconds": float(datanode_cpu_seconds),
        "container_cpu_seconds": float(container_cpu_seconds),
        "container_cpu_avg_cores": float(container_cpu_seconds / total_duration_s),
        "cpu_ms_per_1k_records": float(
            (container_cpu_seconds * 1000.0) / (config.total_records / 1000.0)
        ),
        "datanode_heap_avg_mb": float(sample_stats["datanode_heap_avg_mb"]),
        "datanode_heap_peak_mb": float(sample_stats["datanode_heap_peak_mb"]),
        "container_ram_avg_mb": float(sample_stats["container_ram_avg_mb"]),
        "container_ram_peak_mb": float(sample_stats["container_ram_peak_mb"]),
        "cgroup_disk_read_bytes": int(cgroup_disk_read_bytes),
        "cgroup_disk_write_bytes": int(cgroup_disk_write_bytes),
        "cgroup_disk_read_iops_total": int(cgroup_disk_read_iops),
        "cgroup_disk_write_iops_total": int(cgroup_disk_write_iops),
    }
