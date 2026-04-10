"""Event logging example for Pandlas.

Demonstrates how to write discrete events (lockups, flags, pit stops, etc.)
to an ATLAS session from a Pandas DataFrame using ``add_events()``.

Each row in the DataFrame becomes one ATLAS event.  Every numeric column
(besides the timestamp) is stored as an event value visible in ATLAS.

Usage:
    python event_logging.py
"""

import logging
import numpy as np
import pandas as pd

from pandlas import SQLiteConnection, SQLRaceDBConnection
from pandlas import add_events, add_lap, set_session_details

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ── Configuration ────────────────────────────────────────────────────────
BACKEND = "sqlserver"

SQLITE_DB_DIR = r"C:\McLaren Applied\pandlas\EventLogging.ssndb"

SERVER = r"MCLA-525Q374\LOCAL"
DATABASE = "SQLRACE02"

SESSION_DURATION_S = 60
SAMPLE_RATE_MS = 100  # 10 Hz
TOTAL_ROWS = int(SESSION_DURATION_S * 1000 / SAMPLE_RATE_MS)

ORIGIN_T0 = pd.Timestamp.now()


def open_connection(mode, identifier="", key=None):
    if BACKEND == "sqlite":
        return SQLiteConnection(
            SQLITE_DB_DIR, identifier, session_key=key, mode=mode,
        )
    return SQLRaceDBConnection(
        SERVER, DATABASE, identifier, session_key=key, mode=mode,
    )


def build_lockup_events(origin: pd.Timestamp, n: int = 8) -> pd.DataFrame:
    """Simulate brake lockup events at random times."""
    rng = np.random.default_rng(42)
    offsets = np.sort(rng.uniform(2, SESSION_DURATION_S - 2, size=n))
    timestamps = [origin + pd.Timedelta(seconds=float(t)) for t in offsets]
    return pd.DataFrame({
        "timestamp": timestamps,
        "Status": [f"LK-{i + 1:03d}" for i in range(n)],
        "BrakePressure": rng.uniform(20.0, 40.0, size=n).round(1),
        "WheelSlip": rng.uniform(0.05, 0.25, size=n).round(3),
        "Speed": rng.uniform(120.0, 310.0, size=n).round(1),
    })

def build_flag_events(origin: pd.Timestamp) -> pd.DataFrame:
    """Simulate track flag events."""
    flags = [
        (5.0,  "YEL-S1",  1.0),   # yellow flag at 5 s, sector 1
        (12.0, "YEL-S3",  3.0),   # yellow flag at 12 s, sector 3
        (25.0, "GREEN",    0.0),   # green flag at 25 s (all clear)
        (40.0, "YEL-S2",  2.0),   # yellow flag at 40 s, sector 2
        (55.0, "GREEN",    0.0),   # green flag at 55 s
    ]
    return pd.DataFrame({
        "timestamp": [origin + pd.Timedelta(seconds=t) for t, _, _ in flags],
        "Status": [s for _, s, _ in flags],
        "FlagType": [float(f) for _, _, f in flags],
    })


def build_pitstop_events(origin: pd.Timestamp) -> pd.DataFrame:
    """Simulate pit stop events."""
    return pd.DataFrame({
        "timestamp": [
            origin + pd.Timedelta(seconds=28.0),
            origin + pd.Timedelta(seconds=50.0),
        ],
        "Status": ["PIT-01", "PIT-02"],
        "StopDuration": [2.4, 2.1],
        "TyreSet": [2.0, 3.0],
        "FuelAdded": [0.0, 0.0],
    })


def main():
    origin = ORIGIN_T0

    lockups = build_lockup_events(origin)
    flags = build_flag_events(origin)
    pitstops = build_pitstop_events(origin)

    print(f"\n{'=' * 60}")
    print("  Pandlas Event Logging Example")
    print(f"{'=' * 60}")
    print(f"  Backend:    {BACKEND}")
    print(f"  Duration:   {SESSION_DURATION_S} s")
    print(f"  Lockups:    {len(lockups)} events (3 values each)")
    print(f"  Flags:      {len(flags)} events (1 value each)")
    print(f"  Pit stops:  {len(pitstops)} events (3 values each)")
    print(f"  Origin:     {ORIGIN_T0}")
    print(f"{'=' * 60}\n")

    with open_connection("w", "Event Logging Example") as session:

        # Optional: add session metadata
        set_session_details(session, {
            "Driver": "Lando Norris",
            "Circuit": "Silverstone",
            "Event": "British GP 2026",
        })

        # Write a simple row-data signal so the session has a time range
        times = pd.date_range(
            origin, periods=TOTAL_ROWS,
            freq=f"{SAMPLE_RATE_MS}ms",
        )
        speed = np.linspace(0, 300, TOTAL_ROWS).astype(np.float32)
        df = pd.DataFrame({"vCar": speed}, index=times)
        df.atlas.to_atlas_session(session)

        # Add a lap so events are contextualised
        add_lap(session, origin + pd.Timedelta(seconds=30), lap_name="Lap 1")

        # ── Write events ─────────────────────────────────────────────
        print("Phase 1: Writing events ...\n")

        n1 = add_events(
            session, lockups,
            status_column="Status",
            description="Brake Lockup",
            priority="high",
            application_group="BrakeApp",
        )
        print(f"  ✓ Lockup events:   {n1}")

        n2 = add_events(
            session, flags,
            status_column="Status",
            description="Track Flags",
            priority="medium",
            application_group="RaceControl",
        )
        print(f"  ✓ Flag events:     {n2}")

        n3 = add_events(
            session, pitstops,
            status_column="Status",
            description="Pit Stops",
            priority="low",
            application_group="Strategy",
        )
        print(f"  ✓ Pit stop events: {n3}")

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  Total events written: {n1 + n2 + n3}")
    print(f"  Event groups: BrakeApp, RaceControl, Strategy")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
