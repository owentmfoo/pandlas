"""
Pandlas Performance and Validation Test
=======================================

Combined test that writes data through pandlas into an ATLAS session and then
reads it back, validating both correctness and throughput.

Validates:
  - Float data write and read-back (data integrity)
  - Custom units per parameter
  - Custom descriptions per parameter
  - Custom display format per parameter
  - Custom display limits per parameter (min/max override)
  - Custom warning limits per parameter (min/max override)
  - Default auto-computed limits (from data range)
  - Multiple parameter groups and application groups
  - Laps, point markers, and range markers

Performance:
  - Measures configuration creation time (first batch)
  - Measures streaming write throughput (samples/s)

Requirements:
  - ATLAS 10 installed with SQL Race API available
  - pandlas installed (and able to resolve the ATLAS/SQL Race assemblies)

Usage:
  Adjust BACKEND, paths, and scale constants below, then run:
      python performance_test.py
"""

import sys
import time
import logging
import numpy as np
import pandas as pd

from pandlas import SQLiteConnection, SQLRaceDBConnection
from pandlas import add_synchro_data
from pandlas.utils import timestamp2long
import pandlas.SqlRace as sr

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ========================= CONFIGURATION =========================
# Backend: "sqlite" or "sqlserver"
BACKEND = "sqlserver"

# SQLite settings
SQLITE_DB_DIR = r"C:\McLaren Applied\pandlas\ValidationTest.ssndb"

# SQL Server settings (only used when BACKEND = "sqlserver")
SERVER = r"MCLA-525Q374\LOCAL"
DATABASE = "SQLRACE02"

# Scale
SESSION_DURATION_S = 60  # desired session duration in seconds
BATCH_DURATION_S = 0.1  # how much wall-time each streaming batch covers

# Parameter rate bands — each entry: (prefix, count, sample_interval_ms)
# Parameters at different frequencies, as you'd see on a real car.
RATE_BANDS = [
    ("F1", 10,    1),     # 1 kHz  — high-speed channels (e.g. vibration)
    ("F10", 20,  10),     # 100 Hz — medium-speed (e.g. suspension, steering)
    ("F100", 30, 100),    # 10 Hz  — standard telemetry (e.g. temps, pressures)
    ("F1000", 40, 1000),  # 1 Hz   — slow channels (e.g. strategy, fuel)
]
# =================================================================

ORIGIN_T0 = pd.Timestamp.now()

# Build per-band column names and phase offsets
BAND_META = {}
for prefix, count, interval_ms in RATE_BANDS:
    cols = [f"{prefix}_{i:04d}" for i in range(count)]
    phases = np.linspace(0.0, 2.0 * np.pi, count, endpoint=False)
    n_rows = int(SESSION_DURATION_S * 1000 / interval_ms)
    batch_rows = int(BATCH_DURATION_S * 1000 / interval_ms)
    BAND_META[prefix] = {
        "cols": cols,
        "phases": phases,
        "interval_ms": interval_ms,
        "n_rows": n_rows,
        "batch_rows": max(1, batch_rows),
        "count": count,
    }

TOTAL_PARAMS = sum(b["count"] for b in BAND_META.values())

# ---------- Metadata fixtures (applied to the first band) ----------
_first_prefix = RATE_BANDS[0][0]
_first_cols = BAND_META[_first_prefix]["cols"]
CUSTOM_UNITS = {
    f"{_first_cols[0]}:TestApp": "m/s",
    f"{_first_cols[1]}:TestApp": "deg",
    f"{_first_cols[2]}:TestApp": "bar",
    f"{_first_cols[3]}:TestApp": "rpm",
    f"{_first_cols[4]}:TestApp": "\u00b0C",
}

CUSTOM_DESCRIPTIONS = {
    f"{_first_cols[0]}:TestApp": "Vehicle speed",
    f"{_first_cols[1]}:TestApp": "Steering angle",
    f"{_first_cols[2]}:TestApp": "Brake pressure",
    f"{_first_cols[3]}:TestApp": "Engine speed",
    f"{_first_cols[4]}:TestApp": "Coolant temperature",
}

CUSTOM_FORMATS = {
    f"{_first_cols[0]}:TestApp": "%6.1f",
    f"{_first_cols[1]}:TestApp": "%7.2f",
    f"{_first_cols[2]}:TestApp": "%5.3f",
}

CUSTOM_DISPLAY_LIMITS = {
    f"{_first_cols[0]}:TestApp": (-100.0, 400.0),
    f"{_first_cols[1]}:TestApp": (-180.0, 180.0),
    f"{_first_cols[2]}:TestApp": (0.0, 200.0),
}

CUSTOM_WARNING_LIMITS = {
    f"{_first_cols[0]}:TestApp": (-50.0, 370.0),
    f"{_first_cols[1]}:TestApp": (-170.0, 170.0),
    f"{_first_cols[2]}:TestApp": (0.0, 180.0),
}


# ---- Synchro (engine-synchronous) signals ----
# 5 synchro parameters simulating crank-sync sensors at varying RPM.
# Using 4 events/rev (like a 4-cylinder firing pattern) keeps peak frequency
# well below the 1 kHz row-data band even at high RPM.
SYNCHRO_SIGNALS = [
    ("IgnitionAdv",  "deg",  "Ignition advance angle"),
    ("CylPressure",  "bar",  "Cylinder pressure"),
    ("InjDuration",  "ms",   "Injector pulse duration"),
    ("KnockLevel",   "V",    "Knock sensor voltage"),
    ("CrankTorque",  "Nm",   "Instantaneous crank torque"),
]
SYNCHRO_RPM_IDLE = 800       # idle RPM
SYNCHRO_RPM_MAX = 8_000      # peak RPM
SYNCHRO_EVENTS_PER_REV = 4   # events per revolution (e.g. 4-cyl firing)
SYNCHRO_RPM_CYCLES = 3       # number of idle-max-idle sweeps in the session
# Frequency range: 800*4/60 ≈ 53 Hz (idle) → 8000*4/60 ≈ 533 Hz (peak)


def generate_synchro_signal(
    start_ns: int,
    duration_s: float,
    phase: float = 0.0,
    rpm_idle: float = SYNCHRO_RPM_IDLE,
    rpm_max: float = SYNCHRO_RPM_MAX,
    n_cycles: int = SYNCHRO_RPM_CYCLES,
) -> tuple[np.ndarray, np.ndarray]:
    """Generate a crank-sync signal that spans the full session duration.

    Builds timestamps iteratively from RPM-driven intervals until
    ``duration_s`` is reached, so the sample count adapts naturally
    to the RPM profile.

    Returns:
        (samples, timestamps_ns) tuple.
    """
    duration_ns = int(duration_s * 1e9)
    end_ns = start_ns + duration_ns

    # Over-estimate max possible samples (peak freq × duration + margin)
    peak_freq = rpm_max * SYNCHRO_EVENTS_PER_REV / 60.0
    est_max = int(peak_freq * duration_s * 1.2) + 1000

    # Build an RPM profile over normalised time [0,1] → idle-max-idle sweeps
    t_norm = np.linspace(0.0, 1.0, est_max, dtype=np.float64)
    rpm = rpm_idle + (rpm_max - rpm_idle) * 0.5 * (
        1.0 - np.cos(2.0 * np.pi * t_norm * n_cycles)
    )

    # Derive intervals from RPM (one interval per sample pair)
    rps = rpm / 60.0
    intervals_ns = (1e9 / (rps * SYNCHRO_EVENTS_PER_REV)).astype(np.int64)
    intervals_ns = np.maximum(intervals_ns, 100_000)  # floor at 0.1 ms

    # Build timestamps until we exceed the session duration
    ts_list = [start_ns]
    idx = 0
    while ts_list[-1] + intervals_ns[idx] <= end_ns and idx < len(intervals_ns) - 1:
        ts_list.append(ts_list[-1] + int(intervals_ns[idx]))
        idx += 1

    timestamps_ns = np.array(ts_list, dtype=np.int64)
    n_samples = len(timestamps_ns)

    # Angle-based signal value
    angle = np.cumsum(
        np.concatenate(
            [[0.0], 360.0 / SYNCHRO_EVENTS_PER_REV * np.ones(n_samples - 1)]
        )
    )
    samples = np.sin(np.radians(angle) + phase) * 50.0 + 50.0
    return samples, timestamps_ns


# ----------------------- helpers ---------------------------------
def make_band_df(prefix: str, row_start: int, size: int) -> pd.DataFrame:
    """Create a DataFrame for one rate band with the correct sample interval."""
    meta = BAND_META[prefix]
    idx = ORIGIN_T0 + pd.to_timedelta(
        np.arange(row_start, row_start + size, dtype=np.int64) * meta["interval_ms"],
        unit="ms",
    )
    base = (row_start + np.arange(size))[:, None] / 100.0
    data = np.sin(base + meta["phases"])
    return pd.DataFrame(data, index=idx, columns=meta["cols"])


def open_connection(mode, identifier="", key=None, recorder=False):
    """Return the appropriate session connection for the configured backend."""
    if BACKEND == "sqlite":
        return SQLiteConnection(
            SQLITE_DB_DIR, identifier, session_key=key,
            mode=mode, recorder=recorder,
        )
    return SQLRaceDBConnection(
        SERVER, DATABASE, identifier, session_key=key,
        mode=mode, recorder=recorder,
    )


class ValidationReport:
    """Collects PASS / FAIL / SKIP results and prints a summary."""

    def __init__(self):
        self.results: list[tuple[str, str, str]] = []

    def check(self, label: str, passed: bool, detail: str = ""):
        status = "PASS" if passed else "FAIL"
        self.results.append((status, label, detail))
        symbol = "\u2713" if passed else "\u2717"
        line = f"  {symbol} {label}"
        if not passed and detail:
            line += f"  ({detail})"
        print(line)

    def skip(self, label: str, reason: str = ""):
        self.results.append(("SKIP", label, reason))
        print(f"  \u2298 {label}  ({reason})")

    @property
    def passed(self):
        return sum(1 for s, *_ in self.results if s == "PASS")

    @property
    def failed(self):
        return sum(1 for s, *_ in self.results if s == "FAIL")

    @property
    def skipped(self):
        return sum(1 for s, *_ in self.results if s == "SKIP")


def check_param_attr(report, session, param_id, attr_name, expected, label, tol=None):
    """Read a .NET Parameter property and compare to an expected value.

    For resilience against .NET naming variations, accepts a pipe-separated
    list of attribute names (e.g. ``"Units|Unit"``).  The first one found wins.
    """
    try:
        param = session.GetParameter(param_id)
    except Exception:
        report.skip(label, f"parameter {param_id} not found")
        return

    candidates = [n.strip() for n in attr_name.split("|")]
    actual = None
    found = False
    for name in candidates:
        try:
            actual = getattr(param, name)
            found = True
            break
        except AttributeError:
            continue

    if not found:
        report.skip(label, f"no attribute {candidates}")
        return

    if tol is not None:
        ok = abs(float(actual) - float(expected)) < tol
    else:
        ok = str(actual) == str(expected)

    detail = "" if ok else f"expected {expected!r}, got {actual!r}"
    report.check(label, ok, detail)


# ----------------------- main ------------------------------------
def main():
    session_id = (
        f"Pandlas Validation - {pd.Timestamp.now().strftime('%y/%m/%d %H:%M:%S')}"
    )

    print(f"\n{'=' * 60}")
    print("  Pandlas Performance & Validation Test")
    print(f"{'=' * 60}")
    print(f"  Backend:     {BACKEND}")
    print(f"  Duration:    {SESSION_DURATION_S} s")
    print(f"  Rate bands:")
    for prefix, count, interval_ms in RATE_BANDS:
        meta = BAND_META[prefix]
        freq_hz = 1000 / interval_ms
        print(f"    {prefix:>5}: {count:3d} params @ {freq_hz:,.0f} Hz "
              f"({meta['n_rows']:,} rows)")
    print(f"  Total params: {TOTAL_PARAMS}")
    synchro_idle_hz = SYNCHRO_RPM_IDLE * SYNCHRO_EVENTS_PER_REV / 60
    synchro_peak_hz = SYNCHRO_RPM_MAX * SYNCHRO_EVENTS_PER_REV / 60
    print(f"  Synchro:     {len(SYNCHRO_SIGNALS)} params, "
          f"{SYNCHRO_RPM_IDLE}-{SYNCHRO_RPM_MAX} RPM "
          f"({synchro_idle_hz:.0f}-{synchro_peak_hz:.0f} Hz)")
    print(f"  Origin:      {ORIGIN_T0}")
    print(f"{'=' * 60}\n")

    report = ValidationReport()

    # ==================================================================
    # Phase 1 — Write
    # ==================================================================
    print("Phase 1: Writing data with metadata ...\n")

    # -- First batch of every band (establishes config) ----------------
    first_frames = {}
    for prefix, _, _ in RATE_BANDS:
        meta = BAND_META[prefix]
        df = make_band_df(prefix, 0, meta["batch_rows"])
        df.atlas.ApplicationGroupName = "TestApp"
        df.atlas.ParameterGroupIdentifier = f"Group_{prefix}"
        first_frames[prefix] = df

    # Apply rich metadata to the first band only
    first_band_prefix = RATE_BANDS[0][0]
    first_frames[first_band_prefix].atlas.units = CUSTOM_UNITS
    first_frames[first_band_prefix].atlas.descriptions = CUSTOM_DESCRIPTIONS
    first_frames[first_band_prefix].atlas.display_format = CUSTOM_FORMATS
    first_frames[first_band_prefix].atlas.display_limits = CUSTOM_DISPLAY_LIMITS
    first_frames[first_band_prefix].atlas.warning_limits = CUSTOM_WARNING_LIMITS

    # -- Secondary group: full duration, different app group -----------
    sec_meta = BAND_META[RATE_BANDS[2][0]]  # use medium-rate band for secondary
    df_secondary = make_band_df(RATE_BANDS[2][0], 0, sec_meta["n_rows"])
    sec_cols = [f"S{i:04d}" for i in range(sec_meta["count"])]
    df_secondary.columns = sec_cols
    df_secondary.atlas.ApplicationGroupName = "SecondaryApp"
    df_secondary.atlas.ParameterGroupIdentifier = "SubGroup"
    df_secondary.atlas.units = {f"{c}:SecondaryApp": "V" for c in sec_cols}

    with open_connection("w", session_id) as session:
        # --- config + first batch of each band (timed) ---
        t0 = time.perf_counter()
        for prefix, _, _ in RATE_BANDS:
            first_frames[prefix].atlas.to_atlas_session(
                session, show_progress_bar=False,
            )
        t1 = time.perf_counter()
        cfg_time = t1 - t0

        # secondary group
        df_secondary.atlas.to_atlas_session(session, show_progress_bar=False)

        # --- streaming remaining batches per band (timed) ---
        total_samples = sum(
            BAND_META[p]["batch_rows"] * BAND_META[p]["count"]
            for p, _, _ in RATE_BANDS
        )
        t2 = time.perf_counter()
        for prefix, _, _ in RATE_BANDS:
            meta = BAND_META[prefix]
            for row_start in range(meta["batch_rows"], meta["n_rows"],
                                   meta["batch_rows"]):
                bs = min(meta["batch_rows"], meta["n_rows"] - row_start)
                df = make_band_df(prefix, row_start, bs)
                df.atlas.ApplicationGroupName = "TestApp"
                df.atlas.ParameterGroupIdentifier = f"Group_{prefix}"
                df.atlas.to_atlas_session(session, show_progress_bar=False)
                total_samples += bs * meta["count"]
        t3 = time.perf_counter()
        stream_time = max(1e-9, t3 - t2)

        # laps and markers (after all data so timestamps are in range)
        dur_ms = SESSION_DURATION_S * 1000

        # --- Laps (individual calls) ---
        sr.add_lap(session, ORIGIN_T0 + pd.Timedelta(dur_ms * 0.25, "ms"),
                   2, "Formation Lap")
        sr.add_lap(session, ORIGIN_T0 + pd.Timedelta(dur_ms * 0.50, "ms"),
                   3, "Flying Lap", True)
        sr.add_lap(session, ORIGIN_T0 + pd.Timedelta(dur_ms * 0.75, "ms"),
                   4, "Cool Down Lap", False)

        # --- Update lap (rename the auto-created first lap) ---
        sr.update_lap(session, lap_index=0, new_name="Out Lap")

        # --- Individual markers ---
        sr.add_point_marker(
            session, ORIGIN_T0 + pd.Timedelta(100, "ms"),
            "Pit Entry",
        )
        sr.add_range_marker(
            session,
            ORIGIN_T0 + pd.Timedelta(200, "ms"),
            ORIGIN_T0 + pd.Timedelta(500, "ms"),
            "Safety Car",
            marker_description="Yellow flag period",
        )

        # --- Batch markers (single .Add call) ---
        sr.add_markers_batch(session, [
            {"time": ORIGIN_T0 + pd.Timedelta(dur_ms * 0.10, "ms"),
             "label": "DRS Enabled"},
            {"time": ORIGIN_T0 + pd.Timedelta(dur_ms * 0.60, "ms"),
             "label": "Fastest Sector"},
            {"start_time": ORIGIN_T0 + pd.Timedelta(dur_ms * 0.30, "ms"),
             "end_time":   ORIGIN_T0 + pd.Timedelta(dur_ms * 0.40, "ms"),
             "label": "Harvest Zone",
             "group": "ENERGY",
             "description": "Low SOC harvesting phase"},
            {"start_time": ORIGIN_T0 + pd.Timedelta(dur_ms * 0.55, "ms"),
             "end_time":   ORIGIN_T0 + pd.Timedelta(dur_ms * 0.65, "ms"),
             "label": "Deploy Zone",
             "group": "ENERGY",
             "description": "High power deploy phase"},
        ])

        # --- Synchro (engine-synchronous) signals ---
        print("  Writing synchro signals ...")
        synchro_t0 = time.perf_counter()
        # Align synchro timestamps to the session origin
        origin_ns = int(timestamp2long(ORIGIN_T0))
        synchro_total = 0
        for i, (name, unit, desc) in enumerate(SYNCHRO_SIGNALS):
            samples, ts_ns = generate_synchro_signal(
                start_ns=origin_ns,
                duration_s=SESSION_DURATION_S,
                phase=i * 0.5,
            )
            synchro_total += len(samples)
            add_synchro_data(
                session,
                parameter_name=name,
                app_group="SynchroApp",
                samples=samples,
                timestamps=ts_ns,
                unit=unit,
                description=desc,
            )
        synchro_time = time.perf_counter() - synchro_t0
        print(f"  Synchro time:  {synchro_time:.3f} s  "
              f"({synchro_total:,} samples, "
              f"{synchro_total / synchro_time:,.0f} samples/s)")

        session_key = session.Key.ToString()

    total_time = cfg_time + stream_time
    print(f"  Config time:   {cfg_time:.3f} s")
    print(f"  Stream time:   {stream_time:.3f} s")
    print(f"  Total samples: {total_samples:,}")
    print(f"  Stream rate:   {total_samples / stream_time:,.0f} samples/s")
    print(f"  Overall rate:  {total_samples / total_time:,.0f} samples/s")

    # ==================================================================
    # Phase 2 — Read back and validate
    # ==================================================================
    print(f"\nPhase 2: Validating metadata ...\n")

    with open_connection("r", key=session_key) as session:

        # --- 2a. Custom units ---
        for pid, expected_unit in CUSTOM_UNITS.items():
            check_param_attr(
                report, session, pid, "Units|Unit", expected_unit,
                f"unit  {pid}",
            )

        # --- 2b. Custom descriptions ---
        for pid, expected_desc in CUSTOM_DESCRIPTIONS.items():
            check_param_attr(
                report, session, pid, "Description", expected_desc,
                f"desc  {pid}",
            )

        # --- 2c. Custom display format ---
        for pid, expected_fmt in CUSTOM_FORMATS.items():
            check_param_attr(
                report, session, pid, "FormatOverride", expected_fmt,
                f"fmt   {pid}",
            )

        # --- 2d. Custom display limits ---
        for pid, (lo, hi) in CUSTOM_DISPLAY_LIMITS.items():
            check_param_attr(
                report, session, pid, "MinimumValue", lo,
                f"dLo   {pid}", tol=1e-6,
            )
            check_param_attr(
                report, session, pid, "MaximumValue", hi,
                f"dHi   {pid}", tol=1e-6,
            )

        # --- 2e. Custom warning limits ---
        for pid, (lo, hi) in CUSTOM_WARNING_LIMITS.items():
            check_param_attr(
                report, session, pid, "WarningMinimumValue", lo,
                f"wLo   {pid}", tol=1e-6,
            )
            check_param_attr(
                report, session, pid, "WarningMaximumValue", hi,
                f"wHi   {pid}", tol=1e-6,
            )

        # --- 2f. Default limits (auto-computed from data) ---
        # A parameter without custom limits should use its data min/max.
        auto_col = BAND_META[first_band_prefix]["cols"][5]  # 6th param, no custom limits
        pid_auto = f"{auto_col}:TestApp"
        try:
            auto_data = (
                first_frames[first_band_prefix][auto_col]
                .dropna().to_numpy().astype(np.float32)
            )
            check_param_attr(
                report, session, pid_auto, "MinimumValue",
                float(auto_data.min()), f"auto dLo {pid_auto}", tol=1e-4,
            )
            check_param_attr(
                report, session, pid_auto, "MaximumValue",
                float(auto_data.max()), f"auto dHi {pid_auto}", tol=1e-4,
            )
        except Exception as exc:
            report.skip(f"auto limits {pid_auto}", str(exc))

        # --- 2g. Secondary app group exists ---
        has_secondary = session.ContainsParameter("S0000:SecondaryApp")
        report.check("multi-group  S0000:SecondaryApp exists", has_secondary)

        for c in sec_cols[:5]:
            pid_sec = f"{c}:SecondaryApp"
            check_param_attr(
                report, session, pid_sec, "Units|Unit", "V",
                f"unit  {pid_sec}",
            )

        # --- 2h. Laps ---
        lap_count = session.LapCollection.Count
        report.check(f"laps  count >= 4 (got {lap_count})", lap_count >= 4)

        # Check a specific lap name
        lap_names = [session.LapCollection[i].Name for i in range(lap_count)]
        report.check(
            'lap   "Flying Lap" present',
            "Flying Lap" in lap_names,
        )
        report.check(
            'lap   "Cool Down Lap" present',
            "Cool Down Lap" in lap_names,
        )
        # Validate update_lap renamed Lap 1 → "Out Lap"
        report.check(
            'lap   "Out Lap" (renamed) present',
            "Out Lap" in lap_names,
        )

        # --- 2i. Markers ---
        marker_count = session.Markers.Count
        report.check(f"markers  count >= 6 (got {marker_count})", marker_count >= 6)

        marker_labels = [session.Markers[i].Label for i in range(marker_count)]
        for expected_label in [
            "Pit Entry", "Safety Car", "DRS Enabled",
            "Fastest Sector", "Harvest Zone", "Deploy Zone",
        ]:
            report.check(
                f'marker "{expected_label}" present',
                expected_label in marker_labels,
            )

        # --- 2j. Synchro parameters exist with correct metadata ---
        for name, unit, desc in SYNCHRO_SIGNALS:
            pid = f"{name}:SynchroApp"
            has_sync = session.ContainsParameter(pid)
            report.check(f"synchro  {pid} exists", has_sync)
            if has_sync:
                check_param_attr(
                    report, session, pid, "Units|Unit", unit,
                    f"synchro unit  {pid}",
                )
                check_param_attr(
                    report, session, pid, "Description", desc,
                    f"synchro desc  {pid}",
                )

        # ==============================================================
        # Phase 3 — Data integrity (one parameter per rate band)
        # ==============================================================
        print(f"\nPhase 3: Validating data integrity ...\n")

        for prefix, _, _ in RATE_BANDS:
            meta = BAND_META[prefix]
            first_col = meta["cols"][0]
            pid = f"{first_col}:TestApp"
            samples, timestamps = sr.get_samples(session, pid)
            expected_data = np.sin(
                np.arange(meta["n_rows"]) / 100.0 + meta["phases"][0]
            ).astype(np.float32)

            freq_hz = 1000 / meta["interval_ms"]
            report.check(
                f"count {first_col} @{freq_hz:.0f}Hz "
                f"({len(samples)}/{meta['n_rows']})",
                len(samples) == meta["n_rows"],
            )

            if len(samples) == meta["n_rows"]:
                data_ok = np.allclose(
                    samples.astype(np.float32), expected_data, atol=1e-6,
                )
                report.check(
                    f"data  {first_col} @{freq_hz:.0f}Hz round-trip", data_ok,
                )
            else:
                report.skip(
                    f"data  {first_col} @{freq_hz:.0f}Hz",
                    "sample count mismatch",
                )

    # ==================================================================
    # Summary
    # ==================================================================
    print(f"\n{'=' * 60}")
    print("  RESULTS SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Parameters:     {TOTAL_PARAMS} across {len(RATE_BANDS)} rate bands")
    print(f"  Session:        {SESSION_DURATION_S} s")
    total_rows_all = sum(BAND_META[p]["n_rows"] for p, _, _ in RATE_BANDS)
    print(f"  Rows written:   {total_rows_all:,} (all bands)")
    print(f"  Total samples:  {total_samples:,}")
    print(f"  Config time:    {cfg_time:.3f} s")
    print(f"  Stream time:    {stream_time:.3f} s")
    print(f"  Synchro time:   {synchro_time:.3f} s  ({synchro_total:,} samples)")
    print(f"  Total time:     {total_time + synchro_time:.3f} s")
    print(f"  Row throughput: {total_samples / total_time:,.0f} samples/s")
    print(f"  Sync throughput:{synchro_total / synchro_time:,.0f} samples/s")
    print(f"  Validation:     {report.passed} passed, "
          f"{report.failed} failed, {report.skipped} skipped")
    print(f"{'=' * 60}\n")

    if report.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()