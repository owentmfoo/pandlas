"""Pandlas Complete Showcase
===========================

A single session demonstrating every Pandlas feature:

  1. Multi-rate row data        — 3 frequency bands with sub-grouped params
  2. Session details            — Driver, Circuit, Event metadata
  3. Parameter grouping         — hierarchical auto-grouping via separator
  4. Laps                       — 10 named laps, one renamed via update_lap
  5. Point & range markers      — pit events, DRS zones, safety car, sectors
  6. Synchro (variable-rate)    — engine-synchronous channel
  7. Text channels              — enumerated string parameters
  8. Events                     — low / medium / high severity events

Usage:
    python complete_showcase.py
"""

import time
import logging
import numpy as np
import pandas as pd

from pandlas import (
    SQLiteConnection,
    SQLRaceDBConnection,
    add_lap,
    update_lap,
    add_point_marker,
    add_range_marker,
    add_markers_batch,
    set_session_details,
    add_synchro_data,
    add_text_channel,
    add_events,
)
from pandlas.utils import timestamp2long

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ═══════════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════════
BACKEND = "sqlserver"

SQLITE_DB_DIR = r"C:\McLaren Applied\pandlas\Showcase.ssndb"

SERVER = r"MCLA-525Q374\LOCAL"
DATABASE = "SQLRACE02"

SESSION_DURATION_S = 120  # 2 minutes — enough for 10 laps
ORIGIN_T0 = pd.Timestamp.now()

SESSION_DETAILS = {
    "Driver": "Lando Norris",
    "Team": "McLaren Racing",
    "Circuit": "Silverstone",
    "Vehicle": "MCL60",
    "Event": "British GP 2026",
    "Session": "FP1",
    "Configuration": "High Downforce",
    "Comment": "Pandlas complete showcase — all features",
}

# Lap timing: 10 laps of ~12 s each
LAP_TIMES_S = [0, 12, 23, 34, 45, 56, 67, 78, 89, 100, 111]
LAP_NAMES = [
    "Out Lap", "Lap 1", "Lap 2", "Lap 3", "Lap 4",
    "Lap 5", "Lap 6", "Lap 7", "Lap 8", "Lap 9",
]


def open_connection(mode, identifier="", key=None):
    if BACKEND == "sqlite":
        return SQLiteConnection(
            SQLITE_DB_DIR, identifier, session_key=key, mode=mode,
        )
    return SQLRaceDBConnection(
        SERVER, DATABASE, identifier, session_key=key, mode=mode,
    )


# ═══════════════════════════════════════════════════════════════════════
#  Data generators
# ═══════════════════════════════════════════════════════════════════════

def generate_high_freq_data(origin, duration_s):
    """100 Hz — fast-changing chassis signals under Chassis/ group."""
    rng = np.random.default_rng(10)
    n = duration_s * 100
    times = pd.date_range(origin, periods=n, freq="10ms")
    t = np.linspace(0, duration_s, n)
    return pd.DataFrame({
        "Chassis/DamperFL":    rng.normal(25, 3, n).astype(np.float32),
        "Chassis/DamperFR":    rng.normal(25, 3, n).astype(np.float32),
        "Chassis/DamperRL":    rng.normal(22, 2.5, n).astype(np.float32),
        "Chassis/DamperRR":    rng.normal(22, 2.5, n).astype(np.float32),
        "Chassis/RideHeightF": (30 + 5 * np.sin(2 * np.pi * t / 6)
                                + rng.normal(0, 0.5, n)).astype(np.float32),
        "Chassis/RideHeightR": (35 + 4 * np.sin(2 * np.pi * t / 7)
                                + rng.normal(0, 0.4, n)).astype(np.float32),
    }, index=times)


def generate_mid_freq_data(origin, duration_s):
    """10 Hz — engine & vehicle dynamics under Engine/ and Vehicle/ groups."""
    rng = np.random.default_rng(20)
    n = duration_s * 10
    times = pd.date_range(origin, periods=n, freq="100ms")
    t = np.linspace(0, duration_s, n)

    # RPM follows a repeating accel/brake pattern per ~12 s lap
    lap_phase = (t % 12) / 12
    rpm = 5000 + 13000 * np.where(
        lap_phase < 0.7, lap_phase / 0.7, (1 - lap_phase) / 0.3,
    )

    return pd.DataFrame({
        "Engine/RPM":            rpm.astype(np.float32),
        "Engine/OilTemp":        (95 + 8 * np.sin(2 * np.pi * t / 80)
                                  + rng.normal(0, 0.5, n)).astype(np.float32),
        "Engine/WaterTemp":      (88 + 5 * np.sin(2 * np.pi * t / 60)
                                  + rng.normal(0, 0.3, n)).astype(np.float32),
        "Engine/OilPressure":    (4.5 + 0.5 * np.sin(2 * np.pi * t / 40)
                                  + rng.normal(0, 0.05, n)).astype(np.float32),
        "Vehicle/Speed":         np.clip(rpm / 60, 50, 340).astype(np.float32),
        "Vehicle/SteerAng":      (18 * np.sin(2 * np.pi * t / 5)
                                  * np.sin(2 * np.pi * t / 30)).astype(np.float32),
        "Vehicle/ThrottlePos":   np.clip(
            np.where(lap_phase < 0.7, lap_phase / 0.7, 0) * 100
            + rng.normal(0, 2, n), 0, 100,
        ).astype(np.float32),
        "Vehicle/BrakePressure": np.clip(
            np.where(lap_phase > 0.7, (lap_phase - 0.7) / 0.3, 0) * 35,
            0, 35,
        ).astype(np.float32),
    }, index=times)


def generate_low_freq_data(origin, duration_s):
    """1 Hz — strategy & aero under Strategy/ and Aero/ groups."""
    rng = np.random.default_rng(30)
    n = duration_s
    times = pd.date_range(origin, periods=n, freq="1s")
    t = np.linspace(0, duration_s, n)
    return pd.DataFrame({
        "Strategy/FuelKg":    np.linspace(110, 85, n).astype(np.float32),
        "Strategy/TyreDeg":   np.clip(
            np.linspace(0, 15, n) + rng.normal(0, 0.5, n), 0, 100,
        ).astype(np.float32),
        "Strategy/ERSEnergy": (4000 - 1500 * np.abs(
            np.sin(2 * np.pi * t / 24),
        )).astype(np.float32),
        "Aero/DragCoeff":     (0.85 + 0.02 * np.sin(2 * np.pi * t / 40)
                               + rng.normal(0, 0.002, n)).astype(np.float32),
        "Aero/DownforceN":    (12000 + 2000 * np.sin(
            2 * np.pi * t / 30,
        )).astype(np.float32),
    }, index=times)


def generate_synchro_data(origin, duration_s):
    """Engine-synchronous crank pressure varying with RPM."""
    rng = np.random.default_rng(1)
    events_per_rev = 4
    base_rpm, peak_rpm = 6000, 14000

    samples, timestamps = [], []
    t_ns = 0
    origin_ns = int(timestamp2long(origin))
    end_ns = int(duration_s * 1e9)

    while t_ns < end_ns:
        phase = t_ns / end_ns
        lap_local = (t_ns / 1e9 % 12) / 12
        rpm = base_rpm + (peak_rpm - base_rpm) * (
            lap_local / 0.7 if lap_local < 0.7
            else (1 - lap_local) / 0.3
        )
        rps = rpm / 60.0
        interval_ns = int(1e9 / (rps * events_per_rev))
        pressure = 30.0 + 25.0 * np.sin(2 * np.pi * phase * 10) + rng.normal(0, 2.0)
        samples.append(pressure)
        timestamps.append(origin_ns + t_ns)
        t_ns += interval_ns

    return np.array(samples, dtype=np.float64), np.array(timestamps, dtype=np.int64)


def generate_text_channels(origin, n):
    """Gear, DRS, TyreCompound, PU Mode at 10 Hz."""
    times = pd.date_range(origin, periods=n, freq="100ms")
    rng = np.random.default_rng(2)

    lap_phase = np.array([(i / 10 % 12) / 12 for i in range(n)])
    gear_map = ["N", "1", "2", "3", "4", "5", "6", "7", "8"]
    gears = [gear_map[min(int(p * 8) + 1, 8)] if p < 0.7
             else gear_map[max(int((1 - p) / 0.3 * 8), 1)] for p in lap_phase]

    drs = ["OPEN" if 0.3 < p < 0.6 else "CLOSED" for p in lap_phase]

    compounds = ["SOFT", "MEDIUM", "HARD"]
    tyre_idx = np.clip(np.arange(n) // (n // 3), 0, 2).astype(int)
    tyres = [compounds[i] for i in tyre_idx]

    mode_options = ["Deploy", "Harvest", "Balanced", "Qualifying", "Overtake"]
    modes = [mode_options[rng.integers(0, len(mode_options))] for _ in range(n)]

    return times, gears, drs, tyres, modes


# ── Event generators (one per severity) ──────────────────────────────

def generate_lockup_events(origin):
    """HIGH severity — brake lockups."""
    rng = np.random.default_rng(3)
    n = 8
    offsets = np.sort(rng.uniform(5, SESSION_DURATION_S - 5, size=n))
    return pd.DataFrame({
        "timestamp": [origin + pd.Timedelta(seconds=float(t)) for t in offsets],
        "Status": [f"LOCKUP-{i+1:02d}" for i in range(n)],
        "BrakePressure": rng.uniform(25.0, 40.0, size=n).round(1),
        "WheelSlip": rng.uniform(0.05, 0.25, size=n).round(3),
        "Speed": rng.uniform(150.0, 310.0, size=n).round(1),
    })


def generate_flag_events(origin):
    """MEDIUM severity — track flags."""
    flags = [
        (15.0, "YELLOW-S1"),  (18.0, "GREEN"),
        (52.0, "YELLOW-S3"),  (57.0, "GREEN"),
        (85.0, "VSC-DEPLOYED"), (95.0, "VSC-ENDING"), (97.0, "GREEN"),
    ]
    return pd.DataFrame({
        "timestamp": [origin + pd.Timedelta(seconds=t) for t, _ in flags],
        "Status": [s for _, s in flags],
        "Sector": [1.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0],
    })


def generate_telemetry_events(origin):
    """LOW severity — informational telemetry notices."""
    events = [
        (10.0,  "TYRE-PRES-LOW",  21.5),
        (30.0,  "ERS-LIMIT",      3950.0),
        (48.0,  "FUEL-SAVE-ON",   2.1),
        (60.0,  "FUEL-TARGET",    98.2),
        (75.0,  "BRAKE-WEAR",     18.3),
        (80.0,  "TEMP-WARNING",   105.3),
        (100.0, "WIND-CHANGE",    12.5),
        (115.0, "FUEL-SAVE-OFF",  0.0),
    ]
    return pd.DataFrame({
        "timestamp": [origin + pd.Timedelta(seconds=t) for t, _, _ in events],
        "Status": [s for _, s, _ in events],
        "Value": [v for _, _, v in events],
    })


# ── Marker builder ───────────────────────────────────────────────────

def build_markers(origin):
    """Build a rich set of point and range markers."""
    # Point markers — pit lane events
    pit_points = [
        (2.0,   "Pit Exit"),
        (55.0,  "Pit Entry"),
        (58.0,  "Pit Stop — Tyre Change"),
        (61.0,  "Pit Exit"),
        (110.0, "Pit Entry"),
        (113.0, "Pit Stop — Front Wing Adj"),
        (116.0, "Pit Exit"),
    ]

    range_markers = []

    # DRS zones for each timed lap
    for lap_start in LAP_TIMES_S[1:-1]:
        range_markers.append({
            "start_time": origin + pd.Timedelta(seconds=lap_start + 3),
            "end_time":   origin + pd.Timedelta(seconds=lap_start + 6),
            "label": "DRS Zone 1",
            "group": "DRS",
            "description": "Main straight DRS activation",
        })
        range_markers.append({
            "start_time": origin + pd.Timedelta(seconds=lap_start + 8),
            "end_time":   origin + pd.Timedelta(seconds=lap_start + 10),
            "label": "DRS Zone 2",
            "group": "DRS",
            "description": "Back straight DRS activation",
        })

    # Virtual Safety Car period
    range_markers.append({
        "start_time": origin + pd.Timedelta(seconds=85),
        "end_time":   origin + pd.Timedelta(seconds=97),
        "label": "Virtual Safety Car",
        "group": "RACE_CONTROL",
        "description": "VSC period — speed delta enforced",
    })

    # Sector splits for laps 1-4
    for lap_idx in range(4):
        lap_start = LAP_TIMES_S[lap_idx + 1]
        lap_dur = LAP_TIMES_S[lap_idx + 2] - lap_start
        s1_end = lap_start + lap_dur * 0.30
        s2_end = lap_start + lap_dur * 0.65
        s3_end = lap_start + lap_dur
        for s_num, (s_start, s_end) in enumerate([
            (lap_start, s1_end), (s1_end, s2_end), (s2_end, s3_end),
        ], 1):
            range_markers.append({
                "start_time": origin + pd.Timedelta(seconds=s_start),
                "end_time":   origin + pd.Timedelta(seconds=s_end),
                "label": f"L{lap_idx+1} Sector {s_num}",
                "group": "SECTORS",
            })

    return pit_points, range_markers


# ═══════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════

def main():
    origin = ORIGIN_T0
    n_10hz = SESSION_DURATION_S * 10

    # Generate all data up front
    hi_df  = generate_high_freq_data(origin, SESSION_DURATION_S)
    mid_df = generate_mid_freq_data(origin, SESSION_DURATION_S)
    low_df = generate_low_freq_data(origin, SESSION_DURATION_S)
    sync_samples, sync_timestamps = generate_synchro_data(origin, SESSION_DURATION_S)
    text_times, gears, drs, tyres, modes = generate_text_channels(origin, n_10hz)
    lockup_df    = generate_lockup_events(origin)
    flag_df      = generate_flag_events(origin)
    telemetry_df = generate_telemetry_events(origin)
    pit_points, range_markers = build_markers(origin)

    total_params  = len(hi_df.columns) + len(mid_df.columns) + len(low_df.columns)
    total_events  = len(lockup_df) + len(flag_df) + len(telemetry_df)
    total_markers = len(pit_points) + len(range_markers)
    groups = sorted(set(
        c.split("/")[0] for df in [hi_df, mid_df, low_df] for c in df.columns
    ))

    print(f"\n{'=' * 68}")
    print("  Pandlas Complete Showcase")
    print(f"{'=' * 68}")
    print(f"  Backend:      {BACKEND}")
    print(f"  Duration:     {SESSION_DURATION_S} s ({SESSION_DURATION_S / 60:.0f} min)")
    print(f"  Row data:     {total_params} params in {len(groups)} groups "
          f"({', '.join(groups)})")
    print(f"    100 Hz:     {len(hi_df.columns)} params · "
          f"{len(hi_df):,} rows")
    print(f"     10 Hz:     {len(mid_df.columns)} params · "
          f"{len(mid_df):,} rows")
    print(f"      1 Hz:     {len(low_df.columns)} params · "
          f"{len(low_df):,} rows")
    print(f"  Synchro:      {len(sync_samples):,} samples (crank pressure)")
    print(f"  Text:         4 channels (Gear, DRS, Tyre, PU Mode)")
    print(f"  Events:       {total_events} ({len(lockup_df)} high + "
          f"{len(flag_df)} medium + {len(telemetry_df)} low)")
    print(f"  Laps:         {len(LAP_NAMES)}")
    print(f"  Markers:      {total_markers} ({len(pit_points)} point + "
          f"{len(range_markers)} range)")
    print(f"  Details:      {len(SESSION_DETAILS)} keys")
    print(f"  Origin:       {ORIGIN_T0}")
    print(f"{'=' * 68}\n")

    t0 = time.perf_counter()

    with open_connection("w", "Pandlas Showcase") as session:

        # ── 1. Session details ────────────────────────────────────────
        set_session_details(session, SESSION_DETAILS)
        print("  ✓ Session details set")

        # ── 2. Multi-rate grouped row data ────────────────────────────
        for df_band, label in [
            (hi_df, "100 Hz"), (mid_df, "10 Hz"), (low_df, "1 Hz"),
        ]:
            df_band.atlas.parameter_group_separator = "/"
            df_band.atlas.to_atlas_session(session)
            print(f"  ✓ Row data: {label} — "
                  f"{len(df_band.columns)} params, {len(df_band):,} rows")

        # ── 3. Laps (10 laps, rename first) ──────────────────────────
        for ts, name in zip(LAP_TIMES_S[:-1], LAP_NAMES):
            add_lap(session, origin + pd.Timedelta(seconds=ts), lap_name=name)
        update_lap(session, 0, new_name="Installation Lap")
        print(f"  ✓ {len(LAP_NAMES)} laps (Out Lap → Installation Lap)")

        # ── 4. Markers ───────────────────────────────────────────────
        for ts, label in pit_points:
            add_point_marker(session, origin + pd.Timedelta(seconds=ts), label)
        add_markers_batch(session, range_markers)
        print(f"  ✓ {total_markers} markers "
              f"({len(pit_points)} point + {len(range_markers)} range)")

        # ── 5. Synchro data ──────────────────────────────────────────
        add_synchro_data(
            session,
            parameter_name="CrankPressure",
            app_group="EngineSync",
            samples=sync_samples,
            timestamps=sync_timestamps,
            unit="bar",
            description="Crank angle pressure",
        )
        print(f"  ✓ Synchro: {len(sync_samples):,} samples")

        # ── 6. Text channels ─────────────────────────────────────────
        text_defs = [
            ("Gear",         gears, "DriverInputs", "Current gear"),
            ("DRS",          drs,   "DriverInputs", "DRS flap state"),
            ("TyreCompound", tyres, "Strategy",     "Current tyre compound"),
            ("PUMode",       modes, "Powertrain",   "Power unit mode"),
        ]
        for name, vals, grp, desc in text_defs:
            add_text_channel(
                session, parameter_name=name, values=vals,
                timestamps=text_times, application_group=grp,
                description=desc,
            )
        print(f"  ✓ Text channels: {len(text_defs)} "
              f"({', '.join(n for n, *_ in text_defs)})")

        # ── 7. Events — all severity levels ──────────────────────────
        n_hi = add_events(
            session, lockup_df, status_column="Status",
            description="Brake Lockup", priority="high",
            application_group="BrakeApp",
        )
        n_med = add_events(
            session, flag_df, status_column="Status",
            description="Track Flags", priority="medium",
            application_group="RaceControl",
        )
        n_lo = add_events(
            session, telemetry_df, status_column="Status",
            description="Telemetry Info", priority="low",
            application_group="Telemetry",
        )
        print(f"  ✓ Events: {n_hi} high + {n_med} medium + {n_lo} low")

    elapsed = time.perf_counter() - t0

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'=' * 68}")
    print("  RESULTS")
    print(f"{'=' * 68}")
    print(f"  Total write time:  {elapsed:.3f} s")
    print(f"  Features used:     7 / 7")
    print(f"    • Session details      {len(SESSION_DETAILS)} keys")
    print(f"    • Grouped row data     {total_params} params, "
          f"{len(groups)} groups, 3 rates")
    print(f"    • Laps                 {len(LAP_NAMES)} laps (1 renamed)")
    print(f"    • Markers              {total_markers} "
          f"({len(pit_points)} point + {len(range_markers)} range)")
    print(f"    • Synchro data         {len(sync_samples):,} samples")
    print(f"    • Text channels        {len(text_defs)}")
    print(f"    • Events               {total_events} "
          f"(high / medium / low)")
    print(f"{'=' * 68}")


if __name__ == "__main__":
    main()
