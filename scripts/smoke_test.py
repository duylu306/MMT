"""End-to-end smoke test for Apache IoTDB, Prometheus, and Grafana infrastructure."""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import List

import numpy as np
import requests
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType
from iotdb.utils.NumpyTablet import NumpyTablet


def wait_for_iotdb(host: str, port: int, user: str, password: str, timeout_s: float = 90.0) -> None:
    """Wait until IoTDB DataNode Thrift RPC accepts client sessions."""
    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        session = Session(host, str(port), user, password)
        try:
            session.open(False)
            session.close()
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(2.0)
    raise RuntimeError(f"IoTDB at {host}:{port} not ready within {timeout_s}s: {last_err}")


def verify_iotdb_read_write(host: str, port: int, user: str, password: str) -> None:
    """Verify database creation, single-record write, NumpyTablet batch write, FLUSH, and query readback."""
    db_name = "root.smoke_test"
    device_id = f"{db_name}.edge_0"
    measurements = ["s0", "s1", "s2"]
    data_types = [TSDataType.DOUBLE, TSDataType.DOUBLE, TSDataType.DOUBLE]

    session = Session(host, str(port), user, password)
    session.open(False)
    try:
        try:
            session.delete_storage_group(db_name)
        except Exception:  # noqa: BLE001
            pass

        session.set_storage_group(db_name)

        # 1. Single-record write (insert_record)
        t0 = 1_700_000_000_000
        session.insert_record(device_id, t0, measurements, data_types, [10.5, 20.5, 30.5])

        # 2. Batch write via NumpyTablet (10 rows, including 2 out-of-order timestamps < max)
        batch_ts = np.array([t0 + i * 100 for i in range(1, 11)], dtype=TSDataType.INT64.np_dtype())
        batch_vals = [
            np.array([float(i) for i in range(1, 11)], dtype=TSDataType.DOUBLE.np_dtype()),
            np.array([float(i * 2) for i in range(1, 11)], dtype=TSDataType.DOUBLE.np_dtype()),
            np.array([float(i * 3) for i in range(1, 11)], dtype=TSDataType.DOUBLE.np_dtype()),
        ]
        tablet = NumpyTablet(device_id, measurements, data_types, batch_vals, batch_ts)
        session.insert_tablet(tablet)

        # Force flush and insert one retroactive unsequence point
        session.execute_non_query_statement("FLUSH")
        unseq_ts = t0 + 150
        session.insert_record(device_id, unseq_ts, measurements, data_types, [99.0, 198.0, 297.0])
        session.execute_non_query_statement("FLUSH")

        # 3. Query read-back and verify count + values
        dataset = session.execute_query_statement(f"SELECT s0, s1, s2 FROM {device_id} ORDER BY TIME ASC")
        rows: List[tuple[int, float, float, float]] = []
        while dataset.has_next():
            rec = dataset.next()
            fields = rec.get_fields()
            rows.append(
                (
                    rec.get_timestamp(),
                    fields[0].get_double_value(),
                    fields[1].get_double_value(),
                    fields[2].get_double_value(),
                )
            )
        dataset.close_operation_handle()

        expected_count = 12
        if len(rows) != expected_count:
            raise AssertionError(f"Expected {expected_count} rows, got {len(rows)}")
        if rows[0] != (t0, 10.5, 20.5, 30.5):
            raise AssertionError(f"Unexpected first row: {rows[0]}")
        unseq_matches = [r for r in rows if r[0] == unseq_ts]
        if len(unseq_matches) != 1 or unseq_matches[0][1] != 99.0:
            raise AssertionError(f"Out-of-order point missing or corrupted: {unseq_matches}")

        # Clean up smoke database
        session.delete_storage_group(db_name)
        print(f"[OK] IoTDB R/W verified ({len(rows)} rows written, flushed, queried, and cleaned up).")
    finally:
        session.close()


def _get_with_retry(url: str, timeout_s: float = 30.0) -> requests.Response:
    deadline = time.monotonic() + timeout_s
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(1.5)
    raise RuntimeError(f"Endpoint {url} not ready within {timeout_s}s: {last_err}")


def verify_monitoring(dn_metrics_url: str, prom_url: str, grafana_url: str) -> None:
    """Verify IoTDB Prometheus exporter, Prometheus scrape targets, and Grafana health."""
    resp = _get_with_retry(dn_metrics_url, timeout_s=30.0)
    metrics_text = resp.text
    if "file_count" not in metrics_text and "jvm_" not in metrics_text:
        raise AssertionError("IoTDB DataNode /metrics response did not contain expected IoTDB/JVM metrics")
    print(f"[OK] IoTDB DataNode Prometheus endpoint active ({len(metrics_text)} bytes exposed).")

    # Wait up to 25s for Prometheus scrape targets to turn 'up'
    deadline = time.monotonic() + 25.0
    active_jobs: set[str] = set()
    while time.monotonic() < deadline:
        t_resp = _get_with_retry(f"{prom_url.rstrip('/')}/api/v1/targets", timeout_s=15.0)
        targets = t_resp.json().get("data", {}).get("activeTargets", [])
        active_jobs = {t["labels"].get("job", "") for t in targets if t.get("health") == "up"}
        if {"iotdb-datanode", "cadvisor"}.issubset(active_jobs):
            break
        time.sleep(2.0)

    if "iotdb-datanode" not in active_jobs:
        raise AssertionError(f"Prometheus 'iotdb-datanode' target is not UP. Active UP jobs: {sorted(active_jobs)}")
    print(f"[OK] Prometheus targets UP: {sorted(active_jobs)}")

    g_resp = _get_with_retry(f"{grafana_url.rstrip('/')}/api/health", timeout_s=30.0)
    print(f"[OK] Grafana health verified: {g_resp.json()}")
    d_resp = _get_with_retry(f"{grafana_url.rstrip('/')}/api/search?type=dash-db", timeout_s=15.0)
    dash_titles = [d.get("title") for d in d_resp.json()]
    if not dash_titles:
        raise AssertionError("Grafana provisioned dashboard not found via /api/search")
    print(f"[OK] Grafana provisioned dashboards: {dash_titles}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test IoTDB + observability stack.")
    parser.add_argument("--host", default=os.getenv("IOTDB_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("IOTDB_PORT", "6667")))
    parser.add_argument("--user", default=os.getenv("IOTDB_USER", "root"))
    parser.add_argument("--password", default=os.getenv("IOTDB_PASSWORD", "root"))
    parser.add_argument(
        "--dn-metrics-url",
        default=os.getenv("IOTDB_DN_METRICS_URL", "http://127.0.0.1:9091/metrics"),
    )
    parser.add_argument("--prom-url", default=os.getenv("PROMETHEUS_URL", "http://127.0.0.1:9090"))
    parser.add_argument("--grafana-url", default=os.getenv("GRAFANA_URL", "http://127.0.0.1:3000"))
    parser.add_argument("--skip-monitoring", action="store_true", help="Skip Prometheus/Grafana checks")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    print(f"Connecting to IoTDB at {args.host}:{args.port} ...")
    wait_for_iotdb(args.host, args.port, args.user, args.password)
    verify_iotdb_read_write(args.host, args.port, args.user, args.password)
    if not args.skip_monitoring:
        verify_monitoring(args.dn_metrics_url, args.prom_url, args.grafana_url)
    print("SMOKE TEST PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
