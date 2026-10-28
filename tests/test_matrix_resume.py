"""Unit tests for experimental matrix randomization, distinct seeds, and resume logic."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pytest

from loadgen.workload import WorkloadConfig
from scripts.run_matrix import RunSpec, build_full_specs, execute_matrix


def test_full_specs_randomization_and_distinct_seeds() -> None:
    specs = build_full_specs(n_reps=10, shuffle_seed=12345)
    assert len(specs) == 670
    assert len({s.run_id for s in specs}) == 670
    # Run orders must be 1..N strictly
    orders = [s.run_order for s in specs]
    assert orders == list(range(1, len(specs) + 1))

    # Check every (experiment_group, ooo_ratio, batch_size, num_clients, delay_dist, delay_mean_ms) cell has 10 distinct seeds
    cell_seeds: Dict[tuple, set[int]] = {}
    for s in specs:
        key = (
            s.experiment_group,
            s.ooo_ratio,
            s.batch_size,
            s.num_clients,
            s.delay_dist,
            s.delay_mean_ms,
        )
        cell_seeds.setdefault(key, set()).add(s.seed)

    for key, seeds in cell_seeds.items():
        assert len(seeds) == 10, f"Cell {key} does not have 10 distinct seeds: {seeds}"

    # Verify order is actually randomized (first 15 runs shouldn't all share the same ooo_ratio and batch_size)
    first_cells = {(s.ooo_ratio, s.batch_size, s.num_clients) for s in specs[:15]}
    assert len(first_cells) >= 8


def test_execute_matrix_failure_logging_and_resume(tmp_path: Path) -> None:
    specs = [
        RunSpec(
            run_id=f"unit_r{i}",
            experiment_group="unit",
            run_order=i + 1,
            rep_id=i,
            seed=100 + i,
            total_records=100,
            ooo_ratio=0.0,
            delay_dist="exponential",
            delay_mean_ms=1000.0,
            batch_size=10,
            num_clients=1,
        )
        for i in range(3)
    ]

    csv_path = tmp_path / "runs.csv"
    pq_path = tmp_path / "runs.parquet"
    manifest_path = tmp_path / "manifest.json"
    failed_path = tmp_path / "failed.jsonl"

    fail_r1 = True

    def fake_runner(
        run_id: str,
        experiment_group: str,
        run_order: int,
        rep_id: int,
        config: WorkloadConfig,
        data_root: Path,
    ) -> Dict[str, Any]:
        if run_id == "unit_r1" and fail_r1:
            raise RuntimeError("Simulated transient container hiccup")
        return {
            "run_id": run_id,
            "experiment_group": experiment_group,
            "run_order": run_order,
            "rep_id": rep_id,
            "throughput_rps": 5000.0,
            "flushed_unseq_files": 0,
            "compaction_total_count": 0,
            "wa_storage": 2.0,
        }

    # First pass: unit_r1 fails, unit_r0 and unit_r2 succeed and are checkpointed
    with pytest.raises(RuntimeError, match="1 run\\(s\\) failed"):
        execute_matrix(
            specs=specs,
            csv_path=csv_path,
            parquet_path=pq_path,
            manifest_path=manifest_path,
            failed_log_path=failed_path,
            data_root=tmp_path,
            runner_fn=fake_runner,
            warmup_fn=None,
            max_retries_per_run=0,
        )

    assert csv_path.exists()
    assert failed_path.exists()
    # Resume pass: unit_r0 and unit_r2 are preserved, only unit_r1 is re-executed
    fail_r1 = False
    calls: list[str] = []

    def tracking_runner(**kwargs: Any) -> Dict[str, Any]:
        calls.append(kwargs["run_id"])
        return fake_runner(**kwargs)

    df = execute_matrix(
        specs=specs,
        csv_path=csv_path,
        parquet_path=pq_path,
        manifest_path=manifest_path,
        failed_log_path=failed_path,
        data_root=tmp_path,
        runner_fn=tracking_runner,
        warmup_fn=None,
        max_retries_per_run=0,
    )
    assert calls == ["unit_r1"]
    assert set(df["run_id"]) == {"unit_r0", "unit_r1", "unit_r2"}
