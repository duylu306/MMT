"""Randomized, resumable experimental matrix runner for Apache IoTDB write-cost study."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

import numpy as np
import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from loadgen.client_runner import execute_single_run, run_warmup
from loadgen.workload import DelayDistName, WorkloadConfig

REP_SEEDS: Tuple[int, ...] = (101, 202, 303, 404, 505, 606, 707, 808, 909, 1010)
MATRIX_SHUFFLE_SEED: int = 20261026


@dataclass(frozen=True)
class RunSpec:
    """Specification of a single scheduled benchmark run in the randomized matrix."""

    run_id: str
    experiment_group: str
    run_order: int
    rep_id: int
    seed: int
    total_records: int
    ooo_ratio: float
    delay_dist: DelayDistName
    delay_mean_ms: float
    batch_size: int
    num_clients: int

    def to_workload_config(self) -> WorkloadConfig:
        return WorkloadConfig(
            total_records=self.total_records,
            ooo_ratio=self.ooo_ratio,
            delay_dist=self.delay_dist,
            delay_mean_ms=self.delay_mean_ms,
            batch_size=self.batch_size,
            num_clients=self.num_clients,
            seed=self.seed,
        )


def _format_ooo_tag(ooo: float) -> str:
    return f"o{int(round(ooo * 100)):03d}"


def build_pilot_specs(shuffle_seed: int = MATRIX_SHUFFLE_SEED) -> List[RunSpec]:
    """Build small pilot grid covering all batch sizes, client counts, and OOO regimes."""
    # Candidate cells (ooo_ratio, batch_size, num_clients, total_records)
    cells = [
        (0.50, 1, 1, 2_000),
        (0.50, 1, 4, 4_000),
        (0.50, 1, 16, 8_000),
        (0.50, 10, 1, 15_000),
        (0.50, 10, 4, 24_000),
        (0.00, 100, 4, 80_000),
        (0.50, 100, 4, 80_000),
        (0.00, 1_000, 4, 80_000),
        (0.50, 1_000, 4, 80_000),
        (1.00, 1_000, 4, 80_000),
        (0.50, 1_000, 1, 80_000),
        (0.50, 1_000, 16, 80_000),
        (0.50, 10_000, 4, 80_000),
        (0.50, 50_000, 1, 100_000),
    ]
    unshuffled: List[Dict[str, Any]] = []
    for ooo, bsz, ncli, nrec in cells:
        for rep_id in range(2):
            seed = REP_SEEDS[rep_id]
            rid = (
                f"pilot_{_format_ooo_tag(ooo)}_b{bsz}_c{ncli}_n{nrec}_r{rep_id:02d}_s{seed}"
            )
            unshuffled.append(
                {
                    "run_id": rid,
                    "experiment_group": "pilot",
                    "rep_id": rep_id,
                    "seed": seed,
                    "total_records": nrec,
                    "ooo_ratio": ooo,
                    "delay_dist": "exponential",
                    "delay_mean_ms": 25_000.0,
                    "batch_size": bsz,
                    "num_clients": ncli,
                }
            )
    rng = np.random.default_rng(shuffle_seed)
    perm = rng.permutation(len(unshuffled))
    specs: List[RunSpec] = []
    for order_idx, idx in enumerate(perm, start=1):
        item = unshuffled[int(idx)]
        specs.append(RunSpec(run_order=order_idx, **item))
    return specs


def _records_for_batch(batch_size: int, num_clients: int) -> int:
    """Return logical record count per run calibrated from Pilot runtime & MemTable threshold."""
    if batch_size == 1:
        # Reduced volume per prompt guidance for single-row RPCs, while keeping >= 3,000 rows/client
        # at c=1,4 so MemTables exceed avg_series_point_number_threshold=2500 and auto-flush.
        return 6_000 if num_clients == 1 else 12_000
    if batch_size == 10:
        return 25_000
    return 100_000


def build_full_specs(
    n_reps: int = 10,
    shuffle_seed: int = MATRIX_SHUFFLE_SEED,
) -> List[RunSpec]:
    """Build the 67-cell x 10-rep (670 runs) hybrid Full-Factorial + Sub-Experiment matrix."""
    if n_reps < 1 or n_reps > len(REP_SEEDS):
        raise ValueError(f"n_reps must be in [1, {len(REP_SEEDS)}], got {n_reps}")

    # Deduplicate by (ooo_ratio, batch_size, num_clients, delay_dist, delay_mean_ms)
    # so shared baseline cells are executed once (10 reps) and tagged with primary group,
    # while slicing in analysis filters directly by parameter columns.
    cell_Specs: Dict[Tuple[float, int, int, str, float], str] = {}

    # 1. Block A (main_matrix): Full 6 x 6 Factorial of ooo_ratio x batch_size at num_clients=4
    for ooo in (0.0, 0.10, 0.25, 0.50, 0.75, 1.00):
        for bsz in (1, 10, 100, 1_000, 10_000, 50_000):
            key = (ooo, bsz, 4, "exponential", 25_000.0)
            cell_Specs[key] = "main_matrix"

    # 2. Block B (concurrency_study): Balanced 3 x 3 x 3 Full Factorial across
    #    ooo_ratio in {0.0, 0.50, 1.00} x batch_size in {100, 1000, 50000} x num_clients in {1, 4, 16}
    #    plus single-row concurrency scaling (batch_size=1, ooo_ratio=0.50, num_clients in {1, 4, 16})
    for ooo in (0.0, 0.50, 1.00):
        for bsz in (100, 1_000, 50_000):
            for ncli in (1, 4, 16):
                key = (ooo, bsz, ncli, "exponential", 25_000.0)
                cell_Specs.setdefault(key, "concurrency_study")
    for ncli in (1, 4, 16):
        key = (0.50, 1, ncli, "exponential", 25_000.0)
        cell_Specs.setdefault(key, "concurrency_study")

    # 3. Block C (delay_dist_study): {exponential, lognormal, pareto} x ooo_ratio in {0.25, 0.50, 0.75}
    #    at batch_size=1000, num_clients=4, delay_mean_ms=25_000.0
    for dist in ("exponential", "lognormal", "pareto"):
        for ooo in (0.25, 0.50, 0.75):
            key = (ooo, 1_000, 4, dist, 25_000.0)
            cell_Specs.setdefault(key, "delay_dist_study")

    # 4. Block D (delay_mean_study): delay_mean_ms sweep across client-batch (10s) and MemTable (25s)
    #    windows: {200, 1000, 5000, 25000, 75000, 200000} ms at ooo_ratio=0.50, batch_size=1000, c=4
    for dmean in (200.0, 1_000.0, 5_000.0, 25_000.0, 75_000.0, 200_000.0):
        key = (0.50, 1_000, 4, "exponential", dmean)
        cell_Specs.setdefault(key, "delay_mean_study")

    short_grp = {
        "main_matrix": "main",
        "concurrency_study": "conc",
        "delay_dist_study": "ddist",
        "delay_mean_study": "dmean",
    }
    unshuffled: List[Dict[str, Any]] = []
    for (ooo, bsz, ncli, dist, dmean), group in cell_Specs.items():
        nrec = _records_for_batch(bsz, ncli)
        gtag = short_grp[group]
        for rep_id in range(n_reps):
            seed = REP_SEEDS[rep_id]
            rid = (
                f"{gtag}_{_format_ooo_tag(ooo)}_b{bsz}_c{ncli}_"
                f"{dist[:3]}_d{int(dmean)}_r{rep_id:02d}_s{seed}"
            )
            unshuffled.append(
                {
                    "run_id": rid,
                    "experiment_group": group,
                    "rep_id": rep_id,
                    "seed": seed,
                    "total_records": nrec,
                    "ooo_ratio": ooo,
                    "delay_dist": dist,  # type: ignore[arg-type]
                    "delay_mean_ms": dmean,
                    "batch_size": bsz,
                    "num_clients": ncli,
                }
            )

    rng = np.random.default_rng(shuffle_seed)
    perm = rng.permutation(len(unshuffled))
    specs: List[RunSpec] = []
    for order_idx, idx in enumerate(perm, start=1):
        item = unshuffled[int(idx)]
        specs.append(RunSpec(run_order=order_idx, **item))
    return specs


def save_checkpoint(rows_df: pd.DataFrame, csv_path: Path, parquet_path: Path) -> None:
    """Atomically write updated dataset to CSV and Parquet."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_csv = csv_path.with_suffix(".csv.tmp")
    tmp_pq = parquet_path.with_suffix(".parquet.tmp")
    rows_df.to_csv(tmp_csv, index=False)
    tmp_csv.replace(csv_path)
    rows_df.to_parquet(tmp_pq, index=False)
    tmp_pq.replace(parquet_path)


def execute_matrix(
    specs: List[RunSpec],
    csv_path: Path,
    parquet_path: Path,
    manifest_path: Path,
    failed_log_path: Path,
    data_root: Path = Path("data"),
    runner_fn: Callable[..., Dict[str, Any]] = execute_single_run,
    warmup_fn: Callable[..., None] | None = run_warmup,
    max_retries_per_run: int = 2,
) -> pd.DataFrame:
    """Execute a list of RunSpecs with resume checkpointing and failure logging."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    if not manifest_path.exists():
        manifest_path.write_text(
            json.dumps([asdict(s) for s in specs], indent=2),
            encoding="utf-8",
        )

    existing_rows: List[Dict[str, Any]] = []
    completed_ids: set[str] = set()
    if csv_path.exists():
        existing_df = pd.read_csv(csv_path)
        existing_rows = existing_df.to_dict(orient="records")
        completed_ids = set(existing_df["run_id"].astype(str))

    pending = [s for s in specs if s.run_id not in completed_ids]
    total_count = len(specs)
    print(
        f"[Matrix] Total={total_count}, Completed={len(completed_ids)}, Pending={len(pending)}"
    )

    if not pending:
        return pd.DataFrame(existing_rows)

    host = os.getenv("IOTDB_HOST", "127.0.0.1")
    port = int(os.getenv("IOTDB_PORT", "6667"))
    user = os.getenv("IOTDB_USER", "root")
    password = os.getenv("IOTDB_PASSWORD", "root")

    if warmup_fn is not None:
        print("[Matrix] Running JVM/IoTDB warm-up before starting pending runs...")
        warmup_fn(host, port, user, password)

    t_session_start = time.perf_counter()
    done_in_session = 0
    still_failed: List[str] = []

    for spec in pending:
        attempt = 0
        succeeded = False
        while attempt <= max_retries_per_run and not succeeded:
            attempt += 1
            t0 = time.perf_counter()
            try:
                row = runner_fn(
                    run_id=spec.run_id,
                    experiment_group=spec.experiment_group,
                    run_order=spec.run_order,
                    rep_id=spec.rep_id,
                    config=spec.to_workload_config(),
                    data_root=data_root,
                )
                existing_rows.append(row)
                completed_ids.add(spec.run_id)
                df_now = pd.DataFrame(existing_rows)
                save_checkpoint(df_now, csv_path, parquet_path)

                done_in_session += 1
                elapsed_session = time.perf_counter() - t_session_start
                avg_s = elapsed_session / done_in_session
                rem_runs = len(pending) - done_in_session
                eta_min = (rem_runs * avg_s) / 60.0
                dt = time.perf_counter() - t0
                print(
                    f"[{len(completed_ids):4d}/{total_count:4d}] OK {spec.run_id} "
                    f"({dt:.1f}s | rps={row['throughput_rps']:.0f} | "
                    f"unseq={row['flushed_unseq_files']} | comp={row['compaction_total_count']} | "
                    f"wa_st={row['wa_storage']:.2f} | ETA={eta_min:.1f}m)",
                    flush=True,
                )
                succeeded = True
            except Exception as exc:  # noqa: BLE001
                failed_log_path.parent.mkdir(parents=True, exist_ok=True)
                err_entry = {
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "run_id": spec.run_id,
                    "attempt": attempt,
                    "spec": asdict(spec),
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                }
                with failed_log_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(err_entry) + "\n")
                print(
                    f"[WARN] Run {spec.run_id} failed on attempt {attempt}/{max_retries_per_run + 1}: {exc}",
                    flush=True,
                )
                if attempt <= max_retries_per_run:
                    time.sleep(3.0)

        if not succeeded:
            still_failed.append(spec.run_id)

    if still_failed:
        raise RuntimeError(
            f"{len(still_failed)} run(s) failed after retries ({still_failed[:5]}). "
            f"Re-run the same command to resume."
        )

    return pd.DataFrame(existing_rows)


def print_pilot_summary(df: pd.DataFrame) -> None:
    """Print runtime and coefficient-of-variation (CV) summary from pilot runs."""
    print("\n================ PILOT SUMMARY ================")
    grp = (
        df.groupby(["batch_size", "num_clients", "ooo_ratio", "total_records"], as_index=False)
        .agg(
            n=("run_id", "count"),
            ingest_s_mean=("ingest_duration_s", "mean"),
            total_s_mean=("total_duration_s", "mean"),
            rps_mean=("throughput_rps", "mean"),
            rps_std=("throughput_rps", "std"),
            wa_st_mean=("wa_storage", "mean"),
            wa_st_std=("wa_storage", "std"),
            unseq_files_mean=("flushed_unseq_files", "mean"),
            comp_cross_mean=("compaction_cross_count", "mean"),
        )
        .sort_values(["batch_size", "num_clients", "ooo_ratio"])
    )
    grp["rps_cv_pct"] = (grp["rps_std"] / grp["rps_mean"] * 100.0).fillna(0.0)
    grp["wa_cv_pct"] = (grp["wa_st_std"] / grp["wa_st_mean"] * 100.0).fillna(0.0)
    print(grp.to_string(index=False))
    print(f"Total pilot wall-clock sum: {df['total_duration_s'].sum():.1f} s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Apache IoTDB write-cost experiment matrix.")
    parser.add_argument("--mode", choices=["pilot", "full"], default="full")
    parser.add_argument("--reps", type=int, default=10, help="Repetitions per cell in full mode")
    parser.add_argument("--data-root", default="data")
    args = parser.parse_args()

    data_root = Path(args.data_root)
    if args.mode == "pilot":
        specs = build_pilot_specs()
        df = execute_matrix(
            specs=specs,
            csv_path=data_root / "processed" / "pilot_runs.csv",
            parquet_path=data_root / "processed" / "pilot_runs.parquet",
            manifest_path=data_root / "processed" / "pilot_manifest.json",
            failed_log_path=data_root / "raw" / "pilot_failed_runs.jsonl",
            data_root=data_root,
        )
        print_pilot_summary(df)
    else:
        specs = build_full_specs(n_reps=args.reps)
        df = execute_matrix(
            specs=specs,
            csv_path=data_root / "processed" / "runs.csv",
            parquet_path=data_root / "processed" / "runs.parquet",
            manifest_path=data_root / "processed" / "matrix_manifest.json",
            failed_log_path=data_root / "raw" / "failed_runs.jsonl",
            data_root=data_root,
        )
        print(
            f"\n[Matrix] FULL MATRIX COMPLETE: {len(df)} runs saved to {data_root / 'processed' / 'runs.csv'}"
        )


if __name__ == "__main__":
    main()
