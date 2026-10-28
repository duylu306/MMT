"""Deterministic, seed-reproducible out-of-order time-series workload generator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, List, Literal, Tuple

import numpy as np

DelayDistName = Literal["exponential", "lognormal", "pareto"]

DEFAULT_DATABASE = "root.bench"
DEFAULT_MEASUREMENTS: Tuple[str, ...] = ("s0", "s1", "s2", "s3", "s4", "s5")
BASE_EPOCH_MS: int = 1_700_000_000_000  # 2023-11-14T22:13:20Z in ms
DEFAULT_SAMPLING_STEP_MS: int = 10
# 1x INT64 timestamp (8B) + 6x DOUBLE measurements (48B) = 56 logical bytes/record
BYTES_PER_LOGICAL_ROW: int = 8 + 8 * len(DEFAULT_MEASUREMENTS)

# Independent RNG sub-stream IDs so base dataset and network delay remain orthogonal
_STREAM_BASE_DATA = 0
_STREAM_OOO_SELECT = 1
_STREAM_DELAY_DRAW = 2
_STREAM_FULL_SHUFFLE = 3

# Fixed shape parameters documented in docs/workload_model.md
LOGNORMAL_SIGMA: float = 1.0
PARETO_ALPHA: float = 2.5


@dataclass(frozen=True)
class WorkloadConfig:
    """Configuration for a single controlled workload run."""

    total_records: int
    ooo_ratio: float
    delay_dist: DelayDistName = "exponential"
    delay_mean_ms: float = 10_000.0
    batch_size: int = 1_000
    num_clients: int = 1
    seed: int = 42
    sampling_step_ms: int = DEFAULT_SAMPLING_STEP_MS
    base_epoch_ms: int = BASE_EPOCH_MS
    database: str = DEFAULT_DATABASE
    num_measurements: int = len(DEFAULT_MEASUREMENTS)

    def __post_init__(self) -> None:
        if self.total_records <= 0:
            raise ValueError(f"total_records must be > 0, got {self.total_records}")
        if not (0.0 <= self.ooo_ratio <= 1.0):
            raise ValueError(f"ooo_ratio must be in [0.0, 1.0], got {self.ooo_ratio}")
        if self.delay_dist not in ("exponential", "lognormal", "pareto"):
            raise ValueError(f"Unsupported delay_dist: {self.delay_dist}")
        if self.delay_mean_ms <= 0.0:
            raise ValueError(f"delay_mean_ms must be > 0, got {self.delay_mean_ms}")
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be > 0, got {self.batch_size}")
        if self.num_clients <= 0:
            raise ValueError(f"num_clients must be > 0, got {self.num_clients}")
        if self.sampling_step_ms <= 0:
            raise ValueError(f"sampling_step_ms must be > 0, got {self.sampling_step_ms}")

    @property
    def logical_bytes(self) -> int:
        return self.total_records * (8 + 8 * self.num_measurements)


@dataclass
class ClientStream:
    """Generated arrival-ordered stream for a single edge client/device."""

    client_id: int
    device_id: str
    measurements: List[str]
    timestamps: np.ndarray  # shape (N,), int64, ordered by arrival time at server
    values: List[np.ndarray]  # list of M arrays each shape (N,), float64
    delays_ms: np.ndarray  # shape (N,), float64 delays in original generation order
    delayed_count: int
    empirical_ooo_ratio: float

    @property
    def num_records(self) -> int:
        return int(self.timestamps.shape[0])

    @property
    def logical_bytes(self) -> int:
        return self.num_records * (8 + 8 * len(self.measurements))


def partition_records(total_records: int, num_clients: int) -> List[int]:
    """Split total_records across num_clients so sum(counts) == total_records."""
    if total_records <= 0 or num_clients <= 0:
        raise ValueError("total_records and num_clients must be positive")
    base = total_records // num_clients
    rem = total_records % num_clients
    return [base + (1 if c < rem else 0) for c in range(num_clients)]


def compute_empirical_ooo_ratio(timestamps: np.ndarray) -> float:
    """Compute fraction of arriving records with timestamp < max previous timestamp."""
    n = int(timestamps.shape[0])
    if n <= 1:
        return 0.0
    running_max_before = np.maximum.accumulate(timestamps[:-1])
    violations = timestamps[1:] < running_max_before
    return float(np.count_nonzero(violations) / n)


def generate_delays_ms(
    rng: np.random.Generator,
    size: int,
    delay_dist: DelayDistName,
    delay_mean_ms: float,
) -> np.ndarray:
    """Sample positive arrival delays (in ms) with exact theoretical mean = delay_mean_ms."""
    if size <= 0:
        return np.empty(0, dtype=np.float64)

    if delay_dist == "exponential":
        return rng.exponential(scale=delay_mean_ms, size=size).astype(np.float64)

    if delay_dist == "lognormal":
        mu_log = np.log(delay_mean_ms) - 0.5 * (LOGNORMAL_SIGMA**2)
        return rng.lognormal(mean=mu_log, sigma=LOGNORMAL_SIGMA, size=size).astype(np.float64)

    if delay_dist == "pareto":
        # Lomax (shifted Pareto Type II on (0, inf)) with tail index alpha = 2.5:
        # E[D] = scale / (alpha - 1) => scale = delay_mean_ms * (alpha - 1)
        scale = delay_mean_ms * (PARETO_ALPHA - 1.0)
        # numpy.Generator.pareto(a) draws from Lomax((1+x)^-(a+1)) = U^(-1/a) - 1
        return (scale * rng.pareto(PARETO_ALPHA, size=size)).astype(np.float64)

    raise ValueError(f"Unsupported delay_dist: {delay_dist}")


def _make_rng(seed: int, client_id: int, stream_id: int) -> np.random.Generator:
    seq = np.random.SeedSequence([int(seed), int(client_id), int(stream_id)])
    return np.random.Generator(np.random.PCG64(seq))


def generate_client_stream(config: WorkloadConfig, client_id: int) -> ClientStream:
    """Generate the reproducible sensor stream and arrival permutation for one client."""
    counts = partition_records(config.total_records, config.num_clients)
    if not (0 <= client_id < config.num_clients):
        raise ValueError(f"client_id {client_id} out of range [0, {config.num_clients})")
    n = counts[client_id]
    if n == 0:
        raise ValueError(f"Client {client_id} received 0 records; increase total_records")

    measurements = [f"s{m}" for m in range(config.num_measurements)]
    device_id = f"{config.database}.edge_{client_id}"

    # 1. Canonical generation timestamps & realistic smooth+noisy sensor values
    rng_base = _make_rng(config.seed, client_id, _STREAM_BASE_DATA)
    idx = np.arange(n, dtype=np.int64)
    base_ts = config.base_epoch_ms + idx * np.int64(config.sampling_step_ms)

    phase = idx.astype(np.float64) * 0.01
    base_cols: List[np.ndarray] = []
    for m in range(config.num_measurements):
        offset = 20.0 + 10.0 * m
        signal = offset + 5.0 * np.sin(phase + 0.5 * m) + rng_base.normal(0.0, 0.25, size=n)
        base_cols.append(signal.astype(np.float64))

    # 2. Apply controlled out-of-order model
    delays_ms = np.zeros(n, dtype=np.float64)
    p = config.ooo_ratio

    if p <= 0.0 or n <= 1:
        perm = idx
        delayed_count = 0
    elif p >= 1.0:
        # 100% out-of-order: full Fisher-Yates shuffle across the entire buffer
        rng_shuf = _make_rng(config.seed, client_id, _STREAM_FULL_SHUFFLE)
        perm = rng_shuf.permutation(n).astype(np.int64)
        delayed_count = n
        # Also draw reference delays so metadata/inspections remain well-defined
        rng_delay = _make_rng(config.seed, client_id, _STREAM_DELAY_DRAW)
        delays_ms = generate_delays_ms(rng_delay, n, config.delay_dist, config.delay_mean_ms)
    else:
        # Select exact K = round(p * n) indices in [0, n - 2] (keep n - 1 as undelayed anchor)
        k_delayed = min(int(round(p * n)), n - 1)
        rng_sel = _make_rng(config.seed, client_id, _STREAM_OOO_SELECT)
        rng_delay = _make_rng(config.seed, client_id, _STREAM_DELAY_DRAW)

        selected_idx = rng_sel.choice(n - 1, size=k_delayed, replace=False)
        drawn_delays = generate_delays_ms(
            rng_delay, k_delayed, config.delay_dist, config.delay_mean_ms
        )
        delays_ms[selected_idx] = drawn_delays
        delayed_count = k_delayed

        arrival_time = base_ts.astype(np.float64) + delays_ms
        # Stable sort by arrival_time (ties broken by original generation index)
        perm = np.argsort(arrival_time, kind="mergesort").astype(np.int64)

    arr_ts = np.ascontiguousarray(base_ts[perm], dtype=np.int64)
    arr_vals = [np.ascontiguousarray(col[perm], dtype=np.float64) for col in base_cols]
    emp_ooo = compute_empirical_ooo_ratio(arr_ts)

    return ClientStream(
        client_id=client_id,
        device_id=device_id,
        measurements=measurements,
        timestamps=arr_ts,
        values=arr_vals,
        delays_ms=delays_ms,
        delayed_count=delayed_count,
        empirical_ooo_ratio=emp_ooo,
    )


def iter_batches(
    stream: ClientStream, batch_size: int
) -> Iterator[Tuple[int, np.ndarray, List[np.ndarray]]]:
    """Yield (batch_index, batch_timestamps, batch_values) slices from arrival stream."""
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")
    n = stream.num_records
    batch_idx = 0
    for start in range(0, n, batch_size):
        end = min(start + batch_size, n)
        b_ts = stream.timestamps[start:end]
        b_vals = [col[start:end] for col in stream.values]
        yield batch_idx, b_ts, b_vals
        batch_idx += 1
