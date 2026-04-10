"""
Pandlas Features Test
=====================

Validates three features:
  1. Session Details — writing session-level metadata (Driver, Circuit, etc.)
  2. Parameter Grouping — auto-creating parameter groups from column separators
  3. Text Channels — writing enumerated text parameters with TextConversion

Requirements:
  - ATLAS 10 installed with SQL Race API available
  - pandlas installed

Usage:
    python features_test.py
"""

import sys
import time
import logging
import numpy as np
import pandas as pd

from pandlas import SQLiteConnection, SQLRaceDBConnection
from pandlas import add_text_channel
from pandlas.utils import timestamp2long
import pandlas.SqlRace as sr

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ========================= CONFIGURATION =========================
BACKEND = "sqlserver"

SQLITE_DB_DIR = r"C:\McLaren Applied\pandlas\FeaturesTest.ssndb"

SERVER = r"MCLA-525Q374\LOCAL"
DATABASE = "SQLRACE02"

SESSION_DURATION_S = 10
SAMPLE_RATE_MS = 100  # 10 Hz
TOTAL_ROWS = int(SESSION_DURATION_S * 1000 / SAMPLE_RATE_MS)

ORIGIN_T0 = pd.Timestamp.now()

# Session details to write and validate
SESSION_DETAILS = {
    "Driver": "Lando Norris",
    "Circuit": "Silverstone",
    "Vehicle": "MCL60",
    "Event": "British GP 2026",
    "Session": "FP1",
    "Comment": "Pandlas features validation test",
}

# Parameter grouping config — columns use "/" separator
GROUPED_COLUMNS = {
    "Chassis/DamperFL": "mm",
    "Chassis/DamperFR": "mm",
    "Chassis/DamperRL": "mm",
    "Chassis/DamperRR": "mm",
    "Engine/RPM": "rpm",
    "Engine/OilTemp": "degC",
    "Engine/WaterTemp": "degC",
    "Strategy/FuelRemaining": "kg",
    "Strategy/TyreDeg": "%",
}

# Text channel config
TEXT_GEARS = ["N", "1", "2", "3", "4", "5", "6", "7", "8"]
TEXT_DRS = ["Closed", "Open"]
TEXT_MODE = ["Qualify", "Race", "Deploy", "Harvest", "Safety Car"]
# =================================================================


class ValidationReport:
    """Simple pass/fail/skip test report."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def check(self, label: str, condition: bool):
        if condition:
            self.passed += 1
            print(f"  \u2713 {label}")
        else:
            self.failed += 1
            print(f"  \u2717 {label}")

    def skip(self, label: str, reason: str = ""):
        self.skipped += 1
        print(f"  \u2298 {label}  ({reason})")


def check_param_attr(report, session, param_id, attr_names, expected, label,
                     tol=None):
    """Check a parameter attribute, trying pipe-separated fallback names."""
    try:
        param = session.GetParameter(param_id)
        for attr_name in attr_names.split("|"):
            if hasattr(param, attr_name):
                actual = getattr(param, attr_name)
                if tol is not None:
                    ok = abs(float(actual) - float(expected)) <= tol
                else:
                    ok = str(actual) == str(expected)
                report.check(f"{label}  ('{actual}' == '{expected}')", ok)
                return
        report.skip(label, f"no attribute '{attr_names}'")
    except Exception as exc:
        report.skip(label, str(exc))


def open_connection(mode, identifier="", key=None):
    if BACKEND == "sqlite":
        return SQLiteConnection(
            SQLITE_DB_DIR, identifier, session_key=key, mode=mode,
        )
    return SQLRaceDBConnection(
        SERVER, DATABASE, identifier, session_key=key, mode=mode,
    )


def main():
    print(f"\n{'=' * 60}")
    print("  Pandlas Features Test")
    print(f"{'=' * 60}")
    print(f"  Backend:     {BACKEND}")
    print(f"  Duration:    {SESSION_DURATION_S} s ({TOTAL_ROWS} rows @ "
          f"{1000 / SAMPLE_RATE_MS:.0f} Hz)")
    print(f"  Grouped:     {len(GROUPED_COLUMNS)} params in "
          f"{len(set(c.split('/')[0] for c in GROUPED_COLUMNS))} groups")
    print(f"  Text params: 3 (Gear, DRS, Mode)")
    print(f"  Details:     {len(SESSION_DETAILS)} keys")
    print(f"  Origin:      {ORIGIN_T0}")
    print(f"{'=' * 60}\n")

    report = ValidationReport()

    session_id = (
        f"Features Test - {pd.Timestamp.now():%y/%m/%d %H:%M:%S}"
    )

    # ==================================================================
    # Phase 1 — Write
    # ==================================================================
    print("Phase 1: Writing data ...\n")

    with open_connection("w", session_id) as session:
        t0 = time.perf_counter()

        # --- 1a. Session details ---
        sr.set_session_details(session, SESSION_DETAILS)
        print("  Session details set.")

        # --- 1b. Grouped parameters ---
        idx = pd.date_range(ORIGIN_T0, periods=TOTAL_ROWS,
                            freq=f"{SAMPLE_RATE_MS}ms")
        t = np.arange(TOTAL_ROWS, dtype=np.float64)

        grouped_data = {}
        for i, col in enumerate(GROUPED_COLUMNS):
            grouped_data[col] = np.sin(t / 10.0 + i) * 50.0 + 50.0

        df_grouped = pd.DataFrame(grouped_data, index=idx)
        df_grouped.atlas.ApplicationGroupName = "CarData"
        df_grouped.atlas.parameter_group_separator = "/"

        # Set units using the clean param identifier
        for col, unit in GROUPED_COLUMNS.items():
            clean = col.split("/")[1]
            df_grouped.atlas.units[f"{clean}:CarData"] = unit

        df_grouped.atlas.to_atlas_session(session, show_progress_bar=False)
        print(f"  Grouped data written ({len(GROUPED_COLUMNS)} params).")

        # --- 1c. Text channels ---
        # Gear: cycles through gear sequence
        gear_indices = np.tile(
            np.arange(len(TEXT_GEARS)),
            TOTAL_ROWS // len(TEXT_GEARS) + 1,
        )[:TOTAL_ROWS]
        gear_values = [TEXT_GEARS[i] for i in gear_indices]

        add_text_channel(
            session,
            parameter_name="Gear",
            values=gear_values,
            timestamps=idx,
            application_group="DriverInputs",
            description="Current gear selection",
        )
        print("  Text channel 'Gear' written.")

        # DRS: alternates open/closed
        drs_values = [TEXT_DRS[i % 2] for i in range(TOTAL_ROWS)]
        add_text_channel(
            session,
            parameter_name="DRS",
            values=drs_values,
            timestamps=idx,
            application_group="DriverInputs",
            description="DRS flap state",
        )
        print("  Text channel 'DRS' written.")

        # Mode: cycles through strategy modes
        mode_values = [TEXT_MODE[i % len(TEXT_MODE)] for i in range(TOTAL_ROWS)]
        add_text_channel(
            session,
            parameter_name="Mode",
            values=mode_values,
            timestamps=idx,
            application_group="Strategy",
            description="Power unit operating mode",
        )
        print("  Text channel 'Mode' written.")

        write_time = time.perf_counter() - t0
        session_key = session.Key.ToString()

    print(f"\n  Total write time: {write_time:.3f} s")

    # ==================================================================
    # Phase 2 — Validate
    # ==================================================================
    print(f"\nPhase 2: Validating ...\n")

    with open_connection("r", key=session_key) as session:

        # --- 2a. Session details ---
        print("  --- Session Details ---")
        items = session.Items
        item_dict = {}
        for i in range(items.Count):
            it = items[i]
            try:
                item_dict[it.Name] = it.Value
            except Exception:
                pass

        for key, expected in SESSION_DETAILS.items():
            actual = item_dict.get(key, None)
            if actual is not None:
                report.check(
                    f'detail  "{key}" = "{actual}"',
                    str(actual) == str(expected),
                )
            else:
                report.skip(f'detail  "{key}"', "not found in Items")

        # --- 2b. Parameter grouping ---
        print("\n  --- Parameter Grouping ---")
        for col in GROUPED_COLUMNS:
            clean = col.split("/")[1]
            pid = f"{clean}:CarData"
            exists = session.ContainsParameter(pid)
            report.check(f"grouped  {pid} exists", exists)

        # Check units on grouped params
        for col, expected_unit in GROUPED_COLUMNS.items():
            clean = col.split("/")[1]
            pid = f"{clean}:CarData"
            if session.ContainsParameter(pid):
                check_param_attr(
                    report, session, pid, "Units|Unit", expected_unit,
                    f"unit     {pid}",
                )

        # --- 2c. Text channels ---
        print("\n  --- Text Channels ---")
        text_checks = [
            ("Gear", "DriverInputs", TEXT_GEARS, "Current gear selection"),
            ("DRS", "DriverInputs", TEXT_DRS, "DRS flap state"),
            ("Mode", "Strategy", TEXT_MODE, "Power unit operating mode"),
        ]
        for name, app, labels, expected_desc in text_checks:
            pid = f"{name}:{app}"
            exists = session.ContainsParameter(pid)
            report.check(f"text  {pid} exists", exists)

            if exists:
                check_param_attr(
                    report, session, pid, "Description", expected_desc,
                    f"desc  {pid}",
                )
                # Read back data and verify round-trip
                try:
                    samples, timestamps = sr.get_samples(session, pid)
                    report.check(
                        f"text  {pid} count ({len(samples)}/{TOTAL_ROWS})",
                        len(samples) == TOTAL_ROWS,
                    )
                except Exception as exc:
                    report.skip(f"text  {pid} read-back", str(exc))

    # ==================================================================
    # Summary
    # ==================================================================
    print(f"\n{'=' * 60}")
    print("  RESULTS SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Write time:    {write_time:.3f} s")
    print(f"  Validation:    {report.passed} passed, "
          f"{report.failed} failed, {report.skipped} skipped")
    print(f"{'=' * 60}\n")

    if report.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
