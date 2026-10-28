"""Unit tests for the controlled IoTDB workload generator (Phase 2 verification)."""

from __future__ import annotations

import numpy as np
import pytest
from scipy import stats

from loadgen.workload import (
    LOGNORMAL_SIGMA,
    PARETO_ALPHA,
    WorkloadConfig,
    compute_empirical_ooo_ratio,
    generate_client_stream,
    generate_delays_ms,
    iter_batches,
    partition_records,
)


@pytest.mark.parametrize("target_ooo", [0.0, 0.10, 0.25, 0.50, 0.75, 1.0])
def test_ooo_ratio_accuracy(target_ooo: float) -> None:
    """Verify both generator delayed fraction and empirical running-max OOO ratio."""
    cfg = WorkloadConfig(
        total_records=20_000,
        ooo_ratio=target_ooo,
        delay_dist="exponential",
        delay_mean_ms=10_000.0,
        batch_size=500,
        num_clients=1,
        seed=101,
    )
    stream = generate_client_stream(cfg, client_id=0)
    assert stream.num_records == 20_000

    if target_ooo == 0.0:
        assert stream.delayed_count == 0
        assert stream.empirical_ooo_ratio == 0.0
        assert np.all(np.diff(stream.timestamps) > 0)
    elif target_ooo == 1.0:
        assert stream.delayed_count == 20_000
        # For uniform random permutation of N=20,000, E[running maxima] = H_20000 ~ 10.48,
        # so empirical OOO ratio > 0.998.
        assert stream.empirical_ooo_ratio > 0.998
    else:
        expected_delayed = round(target_ooo * 20_000)
        assert stream.delayed_count == expected_delayed
        # Empirical running-max OOO ratio should be within +/- 0.008 of target_ooo
        assert abs(stream.empirical_ooo_ratio - target_ooo) < 0.008


def test_delay_distributions_mean_and_ks_goodness_of_fit() -> None:
    """Verify theoretical mean and exact CDF shape via Kolmogorov-Smirnov tests."""
    n_samples = 50_000
    target_mean = 8_000.0
    rng = np.random.Generator(np.random.PCG64(2026))

    # 1. Exponential
    d_exp = generate_delays_ms(rng, n_samples, "exponential", target_mean)
    assert np.all(d_exp > 0)
    assert abs(np.mean(d_exp) - target_mean) / target_mean < 0.02
    ks_exp = stats.kstest(d_exp, stats.expon(scale=target_mean).cdf)
    assert ks_exp.pvalue > 0.01

    # 2. Lognormal
    d_log = generate_delays_ms(rng, n_samples, "lognormal", target_mean)
    assert np.all(d_log > 0)
    assert abs(np.mean(d_log) - target_mean) / target_mean < 0.03
    mu_log = np.log(target_mean) - 0.5 * (LOGNORMAL_SIGMA**2)
    ks_log = stats.kstest(
        d_log, stats.lognorm(s=LOGNORMAL_SIGMA, scale=np.exp(mu_log)).cdf
    )
    assert ks_log.pvalue > 0.01

    # 3. Heavy-tailed Pareto Type II (Lomax)
    d_par = generate_delays_ms(rng, n_samples, "pareto", target_mean)
    assert np.all(d_par > 0)
    assert abs(np.mean(d_par) - target_mean) / target_mean < 0.05
    scale_par = target_mean * (PARETO_ALPHA - 1.0)
    ks_par = stats.kstest(d_par, stats.lomax(c=PARETO_ALPHA, scale=scale_par).cdf)
    assert ks_par.pvalue > 0.01

    # Verify tail-heaviness ordering by Coefficient of Variation (CV) and p99.9
    cv_exp = np.std(d_exp) / np.mean(d_exp)
    cv_log = np.std(d_log) / np.mean(d_log)
    cv_par = np.std(d_par) / np.mean(d_par)
    assert cv_exp < cv_log < cv_par
    assert np.percentile(d_exp, 99.9) < np.percentile(d_log, 99.9) < np.percentile(d_par, 99.9)


@pytest.mark.parametrize("batch_size", [1, 10, 100, 1_000, 10_000, 50_000, 7_777])
def test_batch_sizes_and_completeness(batch_size: int) -> None:
    """Verify iter_batches produces exact batch sizes and lossless concatenation."""
    total = 50_000
    cfg = WorkloadConfig(
        total_records=total,
        ooo_ratio=0.25,
        batch_size=batch_size,
        num_clients=1,
        seed=77,
    )
    stream = generate_client_stream(cfg, client_id=0)
    batches = list(iter_batches(stream, batch_size))

    expected_num_batches = (total + batch_size - 1) // batch_size
    assert len(batches) == expected_num_batches

    # Every full batch must match batch_size; last batch holds remainder
    for idx, (b_id, b_ts, b_vals) in enumerate(batches):
        assert b_id == idx
        if idx < len(batches) - 1:
            assert len(b_ts) == batch_size
        else:
            rem = total % batch_size
            assert len(b_ts) == (batch_size if rem == 0 else rem)
        for col in b_vals:
            assert len(col) == len(b_ts)

    reconstructed_ts = np.concatenate([b[1] for b in batches])
    assert np.array_equal(reconstructed_ts, stream.timestamps)


def test_seed_reproducibility_and_logical_invariance() -> None:
    """Verify exact determinism under same seed and identical logical tuples across OOO settings."""
    cfg_a = WorkloadConfig(
        total_records=12_000,
        ooo_ratio=0.50,
        delay_dist="pareto",
        delay_mean_ms=5_000.0,
        batch_size=1_000,
        num_clients=4,
        seed=999,
    )
    cfg_b = WorkloadConfig(
        total_records=12_000,
        ooo_ratio=0.50,
        delay_dist="pareto",
        delay_mean_ms=5_000.0,
        batch_size=1_000,
        num_clients=4,
        seed=999,
    )
    cfg_diff_seed = WorkloadConfig(
        total_records=12_000,
        ooo_ratio=0.50,
        delay_dist="pareto",
        delay_mean_ms=5_000.0,
        batch_size=1_000,
        num_clients=4,
        seed=1000,
    )
    cfg_in_order_same_seed = WorkloadConfig(
        total_records=12_000,
        ooo_ratio=0.0,
        delay_dist="exponential",
        delay_mean_ms=1_000.0,
        batch_size=10,
        num_clients=4,
        seed=999,
    )

    counts = partition_records(12_000, 4)
    assert sum(counts) == 12_000

    for c in range(4):
        s_a = generate_client_stream(cfg_a, c)
        s_b = generate_client_stream(cfg_b, c)
        s_diff = generate_client_stream(cfg_diff_seed, c)
        s_inorder = generate_client_stream(cfg_in_order_same_seed, c)

        # 1. Exact reproducibility for identical seed
        assert np.array_equal(s_a.timestamps, s_b.timestamps)
        for col_a, col_b in zip(s_a.values, s_b.values):
            assert np.array_equal(col_a, col_b)

        # 2. Different seed produces distinct arrival permutation
        assert not np.array_equal(s_a.timestamps, s_diff.timestamps)

        # 3. Same seed produces the EXACT same set of logical (timestamp, values) rows
        #    even when ooo_ratio, delay_dist, delay_mean, and batch_size change!
        order_a = np.argsort(s_a.timestamps)
        assert np.array_equal(s_a.timestamps[order_a], s_inorder.timestamps)
        for col_a, col_in in zip(s_a.values, s_inorder.values):
            assert np.allclose(col_a[order_a], col_in, rtol=0.0, atol=0.0)


def test_compute_empirical_ooo_edge_cases() -> None:
    assert compute_empirical_ooo_ratio(np.array([], dtype=np.int64)) == 0.0
    assert compute_empirical_ooo_ratio(np.array([100], dtype=np.int64)) == 0.0
    assert compute_empirical_ooo_ratio(np.array([10, 20, 15, 30, 5], dtype=np.int64)) == pytest.approx(2 / 5)
