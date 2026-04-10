"""
Pandlas Synchro Performance Test
================================

Benchmarks writing variable-rate (synchro) data to an ATLAS session at
different scales.  All test points are written into a **single session**
so they can be viewed together in ATLAS.

The data simulates engine-crank-synchronous signals where the sample
interval is driven by RPM.  RPM sweeps from idle (800 RPM) up to a
configurable max (default 21 000 RPM), producing sample intervals that
range from ~2.3 ms at idle down to ~0.079 ms at peak RPM — matching
real powertrain telemetry with a 36-tooth crank trigger wheel.

Default test points: 100K, 1M, 10M samples.
Adjust SAMPLE_COUNTS to test larger scales (100M+).

Requirements:
  - ATLAS 10 installed with SQL Race API available
  - pandlas installed

Usage:
    python synchro_performance_test.py
"""

import sys
import time
import logging

import numpy as np
import pandas as pd

from pandlas import SQLiteConnection, SQLRaceDBConnection
from pandlas import add_synchro_data
import pandlas.SqlRace as sr

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ========================= CONFIGURATION =========================
# Backend: "sqlite" or "sqlserver"
BACKEND = "sqlserver"

# SQLite settings
SQLITE_DB_DIR = r"C:\McLaren Applied\pandlas\SynchroTest.ssndb"

# SQL Server settings (only used when BACKEND = "sqlserver")
SERVER = r"MCLA-525Q374\LOCAL"
DATABASE = "SQLRACE02"

# Test points — all written into one session, each as a separate parameter
SAMPLE_COUNTS = [100_000, 1_000_000, 10_000_000]  # adjust as needed

# Engine RPM profile
RPM_IDLE = 800          # idle RPM
RPM_MAX = 21_000        # peak RPM
RPM_CYCLES = 5          # number of idle-max-idle sweeps in the session
TEETH_PER_REV = 36      # crank trigger teeth per revolution (typ. 36-1 or 60-2)
PACKET_SIZE = 16_000     # samples per synchro packet (optimal for throughput)
# =================================================================


def rpm_to_interval_ns(rpm: np.ndarray, teeth_per_rev: int = TEETH_PER_REV) -> np.ndarray:
    """Convert RPM profile to inter-sample intervals in nanoseconds.

    At a given RPM, the time between crank teeth is::

        interval = 60 / (RPM * teeth_per_rev)  [seconds]

    800 RPM / 36 teeth  ->  ~2.31 ms  (433 Hz)
    21 000 RPM / 36 teeth  ->  ~0.079 ms  (12 600 Hz)
    """
    rps = rpm / 60.0
    interval_s = 1.0 / (rps * teeth_per_rev)
    return (interval_s * 1e9).astype(np.int64)


def generate_engine_synchro_data(
    n_samples: int,
    rpm_idle: float = RPM_IDLE,
    rpm_max: float = RPM_MAX,
    n_cycles: int = RPM_CYCLES,
) -> tuple[np.ndarray, np.ndarray]:
    """Generate synchro data with a realistic engine RPM sweep.

    RPM sweeps sinusoidally between ``rpm_idle`` and ``rpm_max`` over
    ``n_cycles`` full oscillations, producing sample intervals that range
    from ~2.3 ms (idle) to ~0.079 ms (peak RPM).

    Returns:
        (samples, timestamps_ns) — both 1-D arrays.
    """
    # RPM profile: cosine sweep idle -> max -> idle
    t = np.arange(n_samples, dtype=np.float64)
    rpm_profile = (
        rpm_idle + (rpm_max - rpm_idle)
        * 0.5 * (1.0 - np.cos(2.0 * np.pi * t / max(n_samples, 1) * n_cycles))
    )

    # Inter-sample intervals from RPM (N-1 intervals for N samples)
    intervals_ns = rpm_to_interval_ns(rpm_profile[:-1])
    intervals_ns = np.maximum(intervals_ns, 1000)  # floor at 1 us

    # Timestamps (ns from midnight, starting at 1 h)
    timestamps_ns = np.empty(n_samples, dtype=np.int64)
    timestamps_ns[0] = 3_600_000_000_000  # 1 hour
    timestamps_ns[1:] = timestamps_ns[0] + np.cumsum(intervals_ns)

    # Signal: crank-angle-synchronous combustion pressure (sinusoidal approx)
    angle = np.cumsum(
        np.concatenate([[0.0], 360.0 / TEETH_PER_REV * np.ones(n_samples - 1)])
    )
    samples = np.sin(np.radians(angle)) * 50.0 + 50.0  # 0-100 bar range

    return samples, timestamps_ns


def open_connection(mode, identifier="", key=None):
    """Return the appropriate session connection for the configured backend."""
    if BACKEND == "sqlite":
        return SQLiteConnection(
            SQLITE_DB_DIR, identifier, session_key=key, mode=mode,
        )
    return SQLRaceDBConnection(
        SERVER, DATABASE, identifier, session_key=key, mode=mode,
    )


def main():
    freq_idle = RPM_IDLE / 60 * TEETH_PER_REV
    freq_max = RPM_MAX / 60 * TEETH_PER_REV
    int_idle_ms = 1e9 / freq_idle / 1e6
    int_max_ms = 1e9 / freq_max / 1e6

    print(f"\n{'=' * 65}")
    print("  Pandlas Synchro Performance Test")
    print(f"{'=' * 65}")
    print(f"  Backend:       {BACKEND}")
    print(f"  RPM range:     {RPM_IDLE:,} - {RPM_MAX:,} RPM  ({RPM_CYCLES} sweeps)")
    print(f"  Crank teeth:   {TEETH_PER_REV} per rev")
    print(f"  Sample rate:   {freq_idle:,.0f} Hz (idle) - {freq_max:,.0f} Hz (peak)")
    print(f"  Interval:      {int_idle_ms:.3f} ms (idle) - {int_max_ms:.4f} ms (peak)")
    print(f"  Packet size:   {PACKET_SIZE:,}")
    print(f"  Test points:   {[f'{n:,}' for n in SAMPLE_COUNTS]}")
    print(f"  All signals written to a single session")
    print(f"{'=' * 65}\n")

    results = []

    session_id = (
        f"Synchro Perf - {pd.Timestamp.now():%y/%m/%d %H:%M:%S}"
    )

    with open_connection("w", session_id) as session:
        for i, n_samples in enumerate(SAMPLE_COUNTS):
            param_name = f"Synchro_{n_samples // 1000}K"

            print(f"\n{'~' * 55}")
            print(f"  {param_name}  ({n_samples:,} samples)")
            print(f"{'~' * 55}")

            # ---- Generate ----
            t0 = time.perf_counter()
            samples, timestamps_ns = generate_engine_synchro_data(n_samples)
            t_gen = time.perf_counter() - t0

            duration_s = (timestamps_ns[-1] - timestamps_ns[0]) / 1e9
            print(f"  Data gen:     {t_gen:.3f} s  "
                  f"(simulated duration {duration_s:.2f} s)")

            # ---- Write ----
            t1 = time.perf_counter()
            add_synchro_data(
                session,
                samples,
                timestamps_ns,
                parameter_name=param_name,
                app_group="EngineSync",
                param_group="CrankSync",
                unit="bar",
                description=(
                    f"Crank-sync combustion pressure, {n_samples:,} samples, "
                    f"{RPM_IDLE}-{RPM_MAX} RPM"
                ),
                packet_size=PACKET_SIZE,
                show_progress_bar=True,
            )
            t_write = time.perf_counter() - t1

            rate = n_samples / max(t_write, 1e-9)
            print(f"  Write time:   {t_write:.3f} s")
            print(f"  Throughput:   {rate:,.0f} samples/s")
            print(f"  Data rate:    {rate * 8 / 1e6:,.1f} MB/s")

            results.append({
                "param": param_name,
                "n_samples": n_samples,
                "t_gen": t_gen,
                "t_write": t_write,
                "rate": rate,
                "duration_s": duration_s,
            })

        session_key = session.Key.ToString()

    # ---- Validate smallest parameter ----
    smallest = SAMPLE_COUNTS[0]
    smallest_param = f"Synchro_{smallest // 1000}K"
    print(f"\nValidating {smallest_param} ({smallest:,} samples) ...")

    with open_connection("r", key=session_key) as session:
        read_samples, read_ts = sr.get_samples(
            session, f"{smallest_param}:EngineSync",
        )
        ref_samples, _ = generate_engine_synchro_data(smallest)
        count_ok = len(read_samples) == smallest
        data_ok = (
            np.allclose(read_samples.astype(np.float64), ref_samples, atol=1e-10)
            if count_ok else False
        )
        if count_ok and data_ok:
            print(f"  \u2713 Data integrity OK "
                  f"({len(read_samples):,} / {smallest:,})")
        else:
            print(f"  \u2717 Data integrity FAILED "
                  f"(count {len(read_samples):,}/{smallest:,}, "
                  f"values_ok={data_ok})")

    # ---- Summary ----
    print(f"\n{'=' * 65}")
    print("  RESULTS SUMMARY")
    print(f"{'=' * 65}")
    print(f"  {'Parameter':<20}  {'Samples':>12}  {'Sim (s)':>8}  "
          f"{'Write (s)':>10}  {'Rate':>14}")
    print(f"  {'-' * 20}  {'-' * 12}  {'-' * 8}  {'-' * 10}  {'-' * 14}")
    for r in results:
        print(
            f"  {r['param']:<20}  {r['n_samples']:>12,}  "
            f"{r['duration_s']:>8.2f}  "
            f"{r['t_write']:>10.3f}  {r['rate']:>12,.0f} /s"
        )
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    main()
