"""Prometheus, Linux Cgroup v2, and cAdvisor telemetry collector for Apache IoTDB 1.3.3."""

from __future__ import annotations

import re
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Tuple

import requests

PROM_LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<val>[-+0-9.eE]+|NaN|(?:\+|-)?Inf)"
)
LABEL_PAIR_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')


def parse_prometheus_text(text: str) -> List[Tuple[str, Dict[str, str], float]]:
    """Parse Prometheus exposition format into (metric_name, labels_dict, float_value) tuples."""
    parsed: List[Tuple[str, Dict[str, str], float]] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = PROM_LINE_RE.match(line)
        if not m:
            continue
        val_str = m.group("val")
        if val_str in ("NaN", "+Inf", "-Inf", "Inf"):
            continue
        try:
            val = float(val_str)
        except ValueError:
            continue
        labels_raw = m.group("labels") or ""
        labels = dict(LABEL_PAIR_RE.findall(labels_raw))
        parsed.append((m.group("name"), labels, val))
    return parsed


@dataclass
class SystemSnapshot:
    """Point-in-time snapshot of IoTDB DataNode Prometheus metrics and OS container counters."""

    timestamp_unix: float
    # Active TsFile counts & sizes for target database
    seq_files: int = 0
    unseq_files: int = 0
    seq_bytes: int = 0
    unseq_bytes: int = 0
    wal_files: int = 0
    wal_bytes: int = 0
    # Cumulative Flush counters
    flush_tasks_total: int = 0
    flush_time_seconds_total: float = 0.0
    flush_auto_memtable_total: int = 0
    flush_manual_memtable_total: int = 0
    # Cumulative Compaction counters
    compaction_inner_seq_count: int = 0
    compaction_inner_unseq_count: int = 0
    compaction_cross_count: int = 0
    compaction_inner_seq_time_s: float = 0.0
    compaction_inner_unseq_time_s: float = 0.0
    compaction_cross_time_s: float = 0.0
    compaction_read_bytes_total: int = 0
    compaction_written_bytes_total: int = 0
    # Active/waiting background queues
    active_or_waiting_bg_tasks: int = 0
    # Cumulative WAL counters
    wal_written_bytes_total: int = 0
    wal_fsync_count_total: int = 0
    wal_fsync_time_s_total: float = 0.0
    # DataNode JVM process metrics
    datanode_cpu_ns_total: float = 0.0
    datanode_used_heap_bytes: float = 0.0
    # Container OS cgroup v2 / cAdvisor counters
    cgroup_cpu_usec_total: int = 0
    cgroup_memory_bytes: int = 0
    cgroup_rbytes_total: int = 0
    cgroup_wbytes_total: int = 0
    cgroup_rios_total: int = 0
    cgroup_wios_total: int = 0


def parse_iotdb_datanode_metrics(text: str, database: str) -> SystemSnapshot:
    """Populate IoTDB-specific fields of SystemSnapshot from DataNode /metrics text."""
    snap = SystemSnapshot(timestamp_unix=time.time())
    rows = parse_prometheus_text(text)

    for name, labels, val in rows:
        if name == "file_global_count" and labels.get("database") == database:
            space = labels.get("name")
            if space == "seq":
                snap.seq_files += int(round(val))
            elif space == "unseq":
                snap.unseq_files += int(round(val))
        elif name == "file_global_size" and labels.get("database") == database:
            space = labels.get("name")
            if space == "seq":
                snap.seq_bytes += int(round(val))
            elif space == "unseq":
                snap.unseq_bytes += int(round(val))
        elif name == "file_count" and labels.get("name") == "wal":
            snap.wal_files += int(round(val))
        elif name == "file_size" and labels.get("name") == "wal":
            snap.wal_bytes += int(round(val))
        elif name == "cost_task_seconds_count":
            tname = labels.get("name", "")
            ival = int(round(val))
            if tname == "flush":
                snap.flush_tasks_total += ival
            elif tname == "inner_seq_compaction":
                snap.compaction_inner_seq_count += ival
            elif tname == "inner_unseq_compaction":
                snap.compaction_inner_unseq_count += ival
            elif tname == "cross_compaction":
                snap.compaction_cross_count += ival
        elif name == "cost_task_seconds_sum":
            tname = labels.get("name", "")
            if tname == "flush":
                snap.flush_time_seconds_total += val
            elif tname == "inner_seq_compaction":
                snap.compaction_inner_seq_time_s += val
            elif tname == "inner_unseq_compaction":
                snap.compaction_inner_unseq_time_s += val
            elif tname == "cross_compaction":
                snap.compaction_cross_time_s += val
        elif name == "flush_memtable_count_total":
            ftype = labels.get("type", "")
            if ftype == "manual_flush_memtable_count":
                snap.flush_manual_memtable_total += int(round(val))
            else:
                snap.flush_auto_memtable_total += int(round(val))
        elif name == "data_read_total":
            ctype = labels.get("type", "")
            if ctype in ("inner_seq_compaction", "inner_unseq_compaction", "cross_compaction"):
                snap.compaction_read_bytes_total += int(round(val))
        elif name == "data_written_total":
            ctype = labels.get("type", "")
            if ctype in ("inner_seq_compaction", "inner_unseq_compaction", "cross_compaction"):
                snap.compaction_written_bytes_total += int(round(val))
        elif name == "queue":
            qname = labels.get("name", "")
            qstatus = labels.get("status", "")
            if qstatus in ("running", "waiting") and (
                qname == "flush" or qname.startswith("compaction_")
            ):
                snap.active_or_waiting_bg_tasks += int(round(val))
        elif name == "wal_buffer_sum" and labels.get("name") == "wrote_wal_buffer_size":
            snap.wal_written_bytes_total += int(round(val))
        elif (
            name == "wal_cost_seconds_count"
            and labels.get("stage") == "sync_wal_buffer"
            and labels.get("type") == "fsync"
        ):
            snap.wal_fsync_count_total += int(round(val))
        elif (
            name == "wal_cost_seconds_sum"
            and labels.get("stage") == "sync_wal_buffer"
            and labels.get("type") == "fsync"
        ):
            snap.wal_fsync_time_s_total += val
        elif name == "process_cpu_time" and labels.get("name") == "process":
            snap.datanode_cpu_ns_total = val
        elif name == "process_used_mem" and labels.get("name") == "process":
            snap.datanode_used_heap_bytes = val

    return snap


def read_cgroup_stats_docker_exec(
    container_name: str = "iotdb-service", sync_fs: bool = False
) -> Tuple[int, int, int, int, int, int]:
    """Read (cpu_usec, mem_bytes, rbytes, wbytes, rios, wios) from container cgroup v2."""
    sync_prefix = "sync && " if sync_fs else ""
    script = (
        f"{sync_prefix}"
        "cat /sys/fs/cgroup/cpu.stat; "
        "echo '---MEM---'; "
        "cat /sys/fs/cgroup/memory.current; "
        "echo '---IO---'; "
        "cat /sys/fs/cgroup/io.stat"
    )
    proc = subprocess.run(
        ["docker", "exec", container_name, "sh", "-c", script],
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    sections = proc.stdout.split("---MEM---")
    cpu_part = sections[0]
    mem_part, io_part = sections[1].split("---IO---")

    cpu_usec = 0
    for line in cpu_part.splitlines():
        if line.startswith("usage_usec "):
            cpu_usec = int(line.split()[1])
            break

    mem_bytes = int(mem_part.strip().splitlines()[0])

    rbytes = wbytes = rios = wios = 0
    for line in io_part.splitlines():
        for tok in line.split():
            if tok.startswith("rbytes="):
                rbytes += int(tok.split("=", 1)[1])
            elif tok.startswith("wbytes="):
                wbytes += int(tok.split("=", 1)[1])
            elif tok.startswith("rios="):
                rios += int(tok.split("=", 1)[1])
            elif tok.startswith("wios="):
                wios += int(tok.split("=", 1)[1])

    return cpu_usec, mem_bytes, rbytes, wbytes, rios, wios


def read_cgroup_stats_cadvisor(cadvisor_url: str, container_name: str = "iotdb-service") -> Tuple[int, int, int, int, int, int]:
    """Fallback reader via cAdvisor HTTP API when running inside loadgen container without docker CLI."""
    resp = requests.get(f"{cadvisor_url.rstrip('/')}/api/v1.3/docker", timeout=5)
    resp.raise_for_status()
    containers = resp.json()
    target = None
    for info in containers.values():
        if container_name in info.get("aliases", []):
            target = info
            break
    if not target or not target.get("stats"):
        raise RuntimeError(f"Container {container_name} not found in cAdvisor /api/v1.3/docker")
    latest = target["stats"][-1]
    cpu_usec = int(latest["cpu"]["usage"]["total"] // 1_000)
    mem_bytes = int(latest["memory"]["working_set"])
    rbytes = wbytes = rios = wios = 0
    for entry in latest.get("diskio", {}).get("io_service_bytes", []) or []:
        stats_map = entry.get("stats", {})
        rbytes += int(stats_map.get("Read", 0))
        wbytes += int(stats_map.get("Write", 0))
    for entry in latest.get("diskio", {}).get("io_serviced", []) or []:
        stats_map = entry.get("stats", {})
        rios += int(stats_map.get("Read", 0))
        wios += int(stats_map.get("Write", 0))
    return cpu_usec, mem_bytes, rbytes, wbytes, rios, wios


class MetricsCollector:
    """Captures baseline/final snapshots and 1 Hz time-series samples during a benchmark run."""

    def __init__(
        self,
        dn_metrics_url: str = "http://127.0.0.1:9091/metrics",
        cadvisor_url: str = "http://127.0.0.1:8080",
        database: str = "root.bench",
        container_name: str = "iotdb-service",
    ) -> None:
        self.dn_metrics_url = dn_metrics_url
        self.cadvisor_url = cadvisor_url
        self.database = database
        self.container_name = container_name
        self._has_docker_cli = shutil.which("docker") is not None

        self.samples: List[SystemSnapshot] = []
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._http = requests.Session()

    def take_iotdb_snapshot(self) -> SystemSnapshot:
        """Fast (~4ms) HTTP-only snapshot of DataNode Prometheus metrics without docker exec."""
        resp = self._http.get(self.dn_metrics_url, timeout=10)
        resp.raise_for_status()
        return parse_iotdb_datanode_metrics(resp.text, self.database)

    def take_snapshot(self, sync_fs: bool = False) -> SystemSnapshot:
        snap = self.take_iotdb_snapshot()

        if self._has_docker_cli:
            cpu_u, mem_b, rb, wb, rio, wio = read_cgroup_stats_docker_exec(
                self.container_name, sync_fs=sync_fs
            )
        else:
            cpu_u, mem_b, rb, wb, rio, wio = read_cgroup_stats_cadvisor(
                self.cadvisor_url, self.container_name
            )
        snap.cgroup_cpu_usec_total = cpu_u
        snap.cgroup_memory_bytes = mem_b
        snap.cgroup_rbytes_total = rb
        snap.cgroup_wbytes_total = wb
        snap.cgroup_rios_total = rio
        snap.cgroup_wios_total = wio
        return snap

    def start_polling(self, interval_s: float = 1.0) -> None:
        self.samples.clear()
        self._stop_evt.clear()

        def _loop() -> None:
            while not self._stop_evt.is_set():
                try:
                    self.samples.append(self.take_snapshot(sync_fs=False))
                except Exception:  # noqa: BLE001
                    pass
                self._stop_evt.wait(interval_s)

        self._thread = threading.Thread(target=_loop, name="metrics-poller", daemon=True)
        self._thread.start()

    def stop_polling(self) -> None:
        self._stop_evt.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None

    def summarize_samples(
        self, before: SystemSnapshot, after: SystemSnapshot
    ) -> Dict[str, float | int]:
        """Compute peak/average resource metrics across [before, *samples, after]."""
        all_snaps = [before, *self.samples, after]
        return {
            "peak_seq_files": max(s.seq_files for s in all_snaps),
            "peak_unseq_files": max(s.unseq_files for s in all_snaps),
            "datanode_heap_avg_mb": float(
                sum(s.datanode_used_heap_bytes for s in all_snaps) / len(all_snaps) / (1024**2)
            ),
            "datanode_heap_peak_mb": float(
                max(s.datanode_used_heap_bytes for s in all_snaps) / (1024**2)
            ),
            "container_ram_avg_mb": float(
                sum(s.cgroup_memory_bytes for s in all_snaps) / len(all_snaps) / (1024**2)
            ),
            "container_ram_peak_mb": float(
                max(s.cgroup_memory_bytes for s in all_snaps) / (1024**2)
            ),
        }

    def export_raw_dict(
        self, before: SystemSnapshot, after: SystemSnapshot
    ) -> Dict[str, object]:
        return {
            "before": asdict(before),
            "after": asdict(after),
            "samples": [asdict(s) for s in self.samples],
        }
