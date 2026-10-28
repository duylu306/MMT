"""Live progress-report demo runner comparing in-order (0% OOO) vs out-of-order writes."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from loadgen.client_runner import execute_single_run, run_warmup
from loadgen.workload import DelayDistName, WorkloadConfig
from scripts.run_matrix import print_pilot_summary


def _print_single_result(label: str, row: Dict[str, Any], data_root: Path) -> None:
    """Print key metrics of one demo run in a clean, readable block."""
    print(f"\n--- {label} ({row['run_id']}) ---")
    print(
        f"  Input      : records={row['total_records']:,} | ooo_ratio={row['ooo_ratio']*100:.0f}% "
        f"(empirical={row['empirical_ooo_ratio']*100:.2f}%) | batch_size={row['batch_size']} | "
        f"clients={row['num_clients']} | seed={row['seed']}"
    )
    print(
        f"  Throughput : foreground={row['throughput_rps']:,.1f} rps | "
        f"effective={row['effective_throughput_rps']:,.1f} rps | "
        f"p50={row['lat_p50_ms']:.2f} ms | p95={row['lat_p95_ms']:.2f} ms"
    )
    print(
        f"  Flush      : seq_files={row['flushed_seq_files']} ({row['flushed_seq_bytes']/1e6:.2f} MB) | "
        f"unseq_files={row['flushed_unseq_files']} ({row['flushed_unseq_bytes']/1e6:.2f} MB)"
    )
    print(
        f"  Compaction : inner_seq={row['compaction_inner_seq_count']} | "
        f"inner_unseq={row['compaction_inner_unseq_count']} | "
        f"cross={row['compaction_cross_count']} | "
        f"overlap_chunks={row['compaction_overlap_chunks']}"
    )
    print(
        f"  Write Cost : WA_storage={row['wa_storage']:.3f}x | "
        f"WA_disk={row['wa_disk']:.3f}x | "
        f"CPU={row['cpu_ms_per_1k_records']:.2f} ms/1k_rows"
    )
    print(f"  Raw Log    : {data_root.as_posix()}/raw/logs/{row['run_id']}_summary.json")


def _print_comparison_table(rows: List[Dict[str, Any]]) -> None:
    """Print a compact side-by-side comparison table across executed demo runs."""
    summary_rows = []
    for r in rows:
        summary_rows.append(
            {
                "ooo_pct": f"{r['ooo_ratio'] * 100:.0f}%",
                "batch_size": r["batch_size"],
                "clients": r["num_clients"],
                "fg_rps": round(r["throughput_rps"], 1),
                "eff_rps": round(r["effective_throughput_rps"], 1),
                "seq_files": r["flushed_seq_files"],
                "unseq_files": r["flushed_unseq_files"],
                "cross_comp": r["compaction_cross_count"],
                "overlap_chunks": r["compaction_overlap_chunks"],
                "wa_storage": round(r["wa_storage"], 3),
                "wa_disk": round(r["wa_disk"], 3),
            }
        )
    df = pd.DataFrame(summary_rows)
    print("\n================ SIDE-BY-SIDE DEMO COMPARISON ================")
    print(df.to_string(index=False))
    print("==============================================================")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run live demo cases (0% OOO vs out-of-order) or print pilot summary."
    )
    parser.add_argument(
        "--case",
        choices=["both", "seq", "ooo", "pilot-summary"],
        default="both",
        help="Which demo action to run (default: both = 0%% OOO followed by OOO case)",
    )
    parser.add_argument("--records", type=int, default=80_000, help="Total logical records per run")
    parser.add_argument("--ooo-ratio", type=float, default=0.50, help="OOO ratio for the OOO case (0..1)")
    parser.add_argument(
        "--delay-dist",
        choices=["exponential", "lognormal", "pareto"],
        default="exponential",
        help="Arrival delay distribution for the OOO case",
    )
    parser.add_argument("--delay-mean-ms", type=float, default=25_000.0, help="Mean arrival delay in ms")
    parser.add_argument("--batch-size", type=int, default=1_000, help="Records per RPC batch")
    parser.add_argument("--clients", type=int, default=4, help="Concurrent writer threads")
    parser.add_argument("--seed", type=int, default=202, help="Deterministic RNG seed")
    parser.add_argument("--data-root", default="data_demo", help="Output folder for demo artifacts")
    parser.add_argument("--skip-warmup", action="store_true", help="Skip initial JVM warm-up")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.case == "pilot-summary":
        pilot_csv = PROJECT_ROOT / "data" / "processed" / "pilot_runs.csv"
        if not pilot_csv.exists():
            print(f"[ERROR] Pilot dataset not found at {pilot_csv}", file=sys.stderr)
            return 1
        print_pilot_summary(pd.read_csv(pilot_csv))
        return 0

    host = os.getenv("IOTDB_HOST", "127.0.0.1")
    port = int(os.getenv("IOTDB_PORT", "6667"))
    user = os.getenv("IOTDB_USER", "root")
    password = os.getenv("IOTDB_PASSWORD", "root")
    raw_data_root = Path(args.data_root)
    data_root = raw_data_root if raw_data_root.is_absolute() else (PROJECT_ROOT / raw_data_root)

    if not args.skip_warmup:
        print("[Demo] Warming up IoTDB JVM before measuring...")
        run_warmup(host, port, user, password)

    executed_rows: List[Dict[str, Any]] = []
    delay_dist: DelayDistName = args.delay_dist

    if args.case in ("both", "seq"):
        print("\n[Demo] Running Case 1: Strictly In-Order Stream (ooo_ratio = 0%)...")
        cfg_seq = WorkloadConfig(
            total_records=args.records,
            ooo_ratio=0.0,
            delay_dist=delay_dist,
            delay_mean_ms=args.delay_mean_ms,
            batch_size=args.batch_size,
            num_clients=args.clients,
            seed=args.seed,
        )
        row_seq = execute_single_run(
            run_id=f"demo_seq_b{args.batch_size}_c{args.clients}",
            experiment_group="demo",
            run_order=1,
            rep_id=0,
            config=cfg_seq,
            data_root=data_root,
        )
        _print_single_result("CASE 1: IN-ORDER (0% OOO)", row_seq, data_root)
        executed_rows.append(row_seq)

    if args.case in ("both", "ooo"):
        ooo_pct = int(round(args.ooo_ratio * 100))
        print(f"\n[Demo] Running Case 2: Out-Of-Order Stream (ooo_ratio = {ooo_pct}%)...")
        cfg_ooo = WorkloadConfig(
            total_records=args.records,
            ooo_ratio=args.ooo_ratio,
            delay_dist=delay_dist,
            delay_mean_ms=args.delay_mean_ms,
            batch_size=args.batch_size,
            num_clients=args.clients,
            seed=args.seed,
        )
        row_ooo = execute_single_run(
            run_id=f"demo_ooo{ooo_pct:03d}_b{args.batch_size}_c{args.clients}",
            experiment_group="demo",
            run_order=2,
            rep_id=0,
            config=cfg_ooo,
            data_root=data_root,
        )
        _print_single_result(f"CASE 2: OUT-OF-ORDER ({ooo_pct}% OOO)", row_ooo, data_root)
        executed_rows.append(row_ooo)

    if len(executed_rows) > 1:
        _print_comparison_table(executed_rows)

    return 0


if __name__ == "__main__":
    sys.exit(main())
