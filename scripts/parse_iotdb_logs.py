"""Extract per-run Flush and Compaction details from Apache IoTDB 1.3.3 DataNode logs."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple

FLUSH_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*TsFileProcessor:\d+\s+-\s+"
    r"The compression ratio of tsfile (?P<path>\S+) is (?P<ratio>[-\d.Ee+NaNInfinity]+),\s*"
    r"totalMemTableSize:\s*(?P<mem_size>\d+),\s*the file size:\s*(?P<file_size>\d+)"
)

COMPACTION_FINISH_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}).*?\s-\s+"
    r"(?P<region>\S+)\s+\[Compaction\]\s+(?P<task_type>(?:Sequence\s+|Unsequence\s+)?\w+Compaction)\s+task finishes successfully,"
    r"\s*(?:target files are \[.*?\],\s*)?time cost is (?P<time_s>[-\d.Ee+]+)\s*s,\s*compaction speed is (?P<speed_mbps>[-\d.Ee+]+)\s*MB/s(?P<rest>.*)$"
)

CHUNK_OVERLAP_RE = re.compile(r"CHUNK_OVERLAP_OR_MODIFIED num is (\d+)")
PAGE_OVERLAP_RE = re.compile(r"PAGE_OVERLAP_OR_MODIFIED num is (\d+)")
CHUNK_NONE_RE = re.compile(r"CHUNK_NONE_OVERLAP num is (\d+)")
PAGE_NONE_RE = re.compile(r"PAGE_NONE_OVERLAP num is (\d+)")

LOG_LEVEL_WARN_ERR_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+\[[^\]]+\]\s+(?P<level>WARN|ERROR)\s+(?P<msg>.*)$"
)


@dataclass
class LogRunSummary:
    """Structured per-run telemetry parsed from IoTDB DataNode logs."""

    run_id: str
    database: str
    flushed_seq_files: int = 0
    flushed_unseq_files: int = 0
    flushed_seq_bytes: int = 0
    flushed_unseq_bytes: int = 0
    flushed_seq_memtable_bytes: int = 0
    flushed_unseq_memtable_bytes: int = 0
    log_cross_compaction_count: int = 0
    log_inner_compaction_count: int = 0
    log_compaction_time_s: float = 0.0
    compaction_overlap_chunks: int = 0
    compaction_overlap_pages: int = 0
    compaction_non_overlap_chunks: int = 0
    compaction_non_overlap_pages: int = 0
    warn_count: int = 0
    error_count: int = 0
    flush_events: List[Dict[str, Any]] = field(default_factory=list)
    compaction_events: List[Dict[str, Any]] = field(default_factory=list)
    sample_warnings_errors: List[str] = field(default_factory=list)


def get_container_log_offsets(container_name: str = "iotdb-service") -> Tuple[int, int]:
    """Return current byte sizes of (log_datanode_all.log, log_datanode_compaction.log)."""
    local_dir = Path("/iotdb-logs")
    if local_dir.is_dir():
        f_all = local_dir / "log_datanode_all.log"
        f_comp = local_dir / "log_datanode_compaction.log"
        return (
            f_all.stat().st_size if f_all.exists() else 0,
            f_comp.stat().st_size if f_comp.exists() else 0,
        )
    cmd = [
        "docker",
        "exec",
        container_name,
        "sh",
        "-c",
        "wc -c < /iotdb/logs/log_datanode_all.log 2>/dev/null || echo 0; "
        "wc -c < /iotdb/logs/log_datanode_compaction.log 2>/dev/null || echo 0",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=15)
    parts = [p.strip() for p in proc.stdout.strip().splitlines() if p.strip()]
    all_off = int(parts[0]) if len(parts) >= 1 else 0
    comp_off = int(parts[1]) if len(parts) >= 2 else 0
    return all_off, comp_off


def read_container_log_slice(
    container_name: str, log_path: str, start_byte: int
) -> str:
    """Read log content appended since start_byte (1-based tail -c +N in POSIX)."""
    local_dir = Path("/iotdb-logs")
    if local_dir.is_dir():
        local_file = local_dir / Path(log_path).name
        if not local_file.exists():
            return ""
        sz = local_file.stat().st_size
        offset = 0 if sz < start_byte else start_byte
        with local_file.open("rb") as fh:
            fh.seek(offset)
            return fh.read().decode("utf-8", errors="replace")
    # If log rotated and shrunk below start_byte, read from byte 1
    script = (
        f"sz=$(wc -c < {log_path} 2>/dev/null || echo 0); "
        f"off={start_byte + 1}; "
        f"if [ \"$sz\" -lt \"{start_byte}\" ]; then off=1; fi; "
        f"tail -c +\"$off\" {log_path} 2>/dev/null || true"
    )
    cmd = ["docker", "exec", container_name, "sh", "-c", script]
    proc = subprocess.run(cmd, capture_output=True, text=True, errors="replace", check=True, timeout=30)
    return proc.stdout


def parse_iotdb_log_texts(
    run_id: str,
    database: str,
    datanode_all_text: str,
    compaction_text: str,
) -> LogRunSummary:
    """Parse Flush and Compaction events belonging to `database` from raw log slices."""
    summary = LogRunSummary(run_id=run_id, database=database)
    db_seq_marker = f"/sequence/{database}/"
    db_unseq_marker = f"/unsequence/{database}/"

    for line in datanode_all_text.splitlines():
        m_flush = FLUSH_LINE_RE.match(line)
        if m_flush:
            path = m_flush.group("path")
            mem_sz = int(m_flush.group("mem_size"))
            file_sz = int(m_flush.group("file_size"))
            try:
                ratio = float(m_flush.group("ratio"))
            except ValueError:
                ratio = 0.0

            if db_seq_marker in path:
                summary.flushed_seq_files += 1
                summary.flushed_seq_bytes += file_sz
                summary.flushed_seq_memtable_bytes += mem_sz
                summary.flush_events.append(
                    {
                        "ts": m_flush.group("ts"),
                        "space": "seq",
                        "path": path,
                        "memtable_bytes": mem_sz,
                        "file_bytes": file_sz,
                        "compression_ratio": ratio,
                    }
                )
            elif db_unseq_marker in path:
                summary.flushed_unseq_files += 1
                summary.flushed_unseq_bytes += file_sz
                summary.flushed_unseq_memtable_bytes += mem_sz
                summary.flush_events.append(
                    {
                        "ts": m_flush.group("ts"),
                        "space": "unseq",
                        "path": path,
                        "memtable_bytes": mem_sz,
                        "file_bytes": file_sz,
                        "compression_ratio": ratio,
                    }
                )
            continue

        m_lvl = LOG_LEVEL_WARN_ERR_RE.match(line)
        if m_lvl:
            lvl = m_lvl.group("level")
            if lvl == "WARN":
                summary.warn_count += 1
            else:
                summary.error_count += 1
            if len(summary.sample_warnings_errors) < 20:
                summary.sample_warnings_errors.append(line[:240])

    for line in compaction_text.splitlines():
        m_comp = COMPACTION_FINISH_RE.match(line)
        if not m_comp:
            continue
        region = m_comp.group("region")
        if not region.startswith(database):
            continue
        task_type = m_comp.group("task_type")
        time_s = float(m_comp.group("time_s"))
        speed = float(m_comp.group("speed_mbps"))
        rest = m_comp.group("rest")

        c_ov = int(m.group(1)) if (m := CHUNK_OVERLAP_RE.search(rest)) else 0
        p_ov = int(m.group(1)) if (m := PAGE_OVERLAP_RE.search(rest)) else 0
        c_no = int(m.group(1)) if (m := CHUNK_NONE_RE.search(rest)) else 0
        p_no = int(m.group(1)) if (m := PAGE_NONE_RE.search(rest)) else 0

        if "Cross" in task_type:
            summary.log_cross_compaction_count += 1
        else:
            summary.log_inner_compaction_count += 1
        summary.log_compaction_time_s += time_s
        summary.compaction_overlap_chunks += c_ov
        summary.compaction_overlap_pages += p_ov
        summary.compaction_non_overlap_chunks += c_no
        summary.compaction_non_overlap_pages += p_no

        summary.compaction_events.append(
            {
                "ts": m_comp.group("ts"),
                "region": region,
                "task_type": task_type,
                "time_s": time_s,
                "speed_mbps": speed,
                "chunk_overlap": c_ov,
                "page_overlap": p_ov,
                "chunk_none_overlap": c_no,
                "page_none_overlap": p_no,
            }
        )

    return summary


def read_both_container_log_slices(
    container_name: str, start_offsets: Tuple[int, int]
) -> Tuple[str, str]:
    """Read appended slices of (log_datanode_all.log, log_datanode_compaction.log) in one call."""
    local_dir = Path("/iotdb-logs")
    if local_dir.is_dir():
        return (
            read_container_log_slice(container_name, "/iotdb/logs/log_datanode_all.log", start_offsets[0]),
            read_container_log_slice(container_name, "/iotdb/logs/log_datanode_compaction.log", start_offsets[1]),
        )
    delim = "===IOTDB_COMPACTION_LOG_SPLIT_9f8a7b==="
    all_path = "/iotdb/logs/log_datanode_all.log"
    comp_path = "/iotdb/logs/log_datanode_compaction.log"
    s0, s1 = start_offsets
    script = (
        f"sz0=$(wc -c < {all_path} 2>/dev/null || echo 0); "
        f"off0={s0 + 1}; if [ \"$sz0\" -lt \"{s0}\" ]; then off0=1; fi; "
        f"tail -c +\"$off0\" {all_path} 2>/dev/null || true; "
        f"printf '\\n{delim}\\n'; "
        f"sz1=$(wc -c < {comp_path} 2>/dev/null || echo 0); "
        f"off1={s1 + 1}; if [ \"$sz1\" -lt \"{s1}\" ]; then off1=1; fi; "
        f"tail -c +\"$off1\" {comp_path} 2>/dev/null || true"
    )
    proc = subprocess.run(
        ["docker", "exec", container_name, "sh", "-c", script],
        capture_output=True,
        text=True,
        errors="replace",
        check=True,
        timeout=30,
    )
    parts = proc.stdout.split(f"\n{delim}\n", 1)
    all_text = parts[0]
    comp_text = parts[1] if len(parts) > 1 else ""
    return all_text, comp_text


def extract_and_save_run_logs(
    run_id: str,
    database: str,
    start_offsets: Tuple[int, int],
    output_dir: Path,
    container_name: str = "iotdb-service",
) -> LogRunSummary:
    """Slice IoTDB container logs from `start_offsets`, parse, and save JSON report."""
    output_dir.mkdir(parents=True, exist_ok=True)
    all_slice, comp_slice = read_both_container_log_slices(container_name, start_offsets)
    summary = parse_iotdb_log_texts(run_id, database, all_slice, comp_slice)
    out_file = output_dir / f"{run_id}_iotdb_log_summary.json"
    out_file.write_text(json.dumps(asdict(summary), indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract IoTDB log details for a run_id.")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--database", default="root.bench")
    parser.add_argument("--container", default="iotdb-service")
    parser.add_argument("--all-offset", type=int, default=0)
    parser.add_argument("--comp-offset", type=int, default=0)
    parser.add_argument("--output-dir", default="data/raw/logs")
    args = parser.parse_args()

    summary = extract_and_save_run_logs(
        run_id=args.run_id,
        database=args.database,
        start_offsets=(args.all_offset, args.comp_offset),
        output_dir=Path(args.output_dir),
        container_name=args.container,
    )
    print(
        f"[{summary.run_id}] seq_flushed={summary.flushed_seq_files} ({summary.flushed_seq_bytes} B), "
        f"unseq_flushed={summary.flushed_unseq_files} ({summary.flushed_unseq_bytes} B), "
        f"cross_comp={summary.log_cross_compaction_count}, inner_comp={summary.log_inner_compaction_count}"
    )


if __name__ == "__main__":
    main()
