"""
parquet_to_atlas.py
===================

Load a Parquet file into an ATLAS session using Pandlas.

Requirements:
    pip install pandlas pandas pyarrow
"""

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from pandlas import SQLiteConnection


def parquet_to_atlas(
    parquet_path: str,
    output_path: str,
    session_name: Optional[str] = None,
    timestamp_column: Optional[str] = None,
    application_group: str = "ParquetImport",
    parameter_group: str = "Data",
    units: Optional[Dict[str, str]] = None,
    text_columns: Optional[List[str]] = None,
) -> Path:
    """
    Import a Parquet file into an ATLAS session.

    Args:
        parquet_path:
            Path to input .parquet file.
        output_path:
            Path to output .ssndb file.
        session_name:
            ATLAS session name (defaults to parquet filename).
        timestamp_column:
            Column to use as timestamp (None = existing index).
        application_group:
            ATLAS application group name.
        parameter_group:
            Default parameter group.
        units:
            Optional dict mapping "Channel:AppGroup" -> unit.
        text_columns:
            Columns containing text/enums (converted to numeric codes).

    Returns:
        Path to the created .ssndb file.
    """
    parquet_path = Path(parquet_path)
    output_path = Path(output_path)
    session_name = session_name or parquet_path.stem

    print(f"Loading {parquet_path}...")
    df = pd.read_parquet(parquet_path)

    # Timestamp handling
    if timestamp_column:
        if timestamp_column not in df.columns:
            raise KeyError(f"Timestamp column '{timestamp_column}' not found.")
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        df.set_index(timestamp_column, inplace=True)

    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(
            "DataFrame must have a DatetimeIndex. "
            "Specify timestamp_column or fix the parquet index."
        )

    # Convert text / enum columns to numeric codes
    text_columns = text_columns or []
    text_mappings: Dict[str, Dict[int, str]] = {}

    for col in text_columns:
        if col not in df.columns:
            continue

        df[col] = df[col].astype("category")
        text_mappings[col] = dict(enumerate(df[col].cat.categories))
        df[col] = df[col].cat.codes.astype(float)

        print(f"  Converted text column '{col}' → numeric codes")

    print(f"  Loaded {len(df):,} rows × {len(df.columns)} columns")
    print(f"  Time range: {df.index[0]} → {df.index[-1]}")

    # ATLAS metadata
    df.atlas.ApplicationGroupName = application_group
    df.atlas.ParameterGroupIdentifier = parameter_group
    df.atlas.parameter_group_separator = "/"

    # Units (auto-generate if not provided)
    if units is None:
        units = {}
        for col in df.columns:
            name = col.lower()

            if any(x in name for x in ("pressure", "bar")):
                unit = "bar"
            elif any(x in name for x in ("temp", "deg", "°c")):
                unit = "°C"
            elif any(x in name for x in ("speed", "velocity")):
                unit = "km/h"
            elif any(x in name for x in ("rpm", "revs")):
                unit = "rpm"
            elif any(x in name for x in ("angle", "steer")):
                unit = "deg"
            elif any(x in name for x in ("pos", "mm", "damper", "suspension")):
                unit = "mm"
            elif col in text_columns:
                unit = "code"
            else:
                unit = ""

            units[f"{col}:{application_group}"] = unit

    df.atlas.units = units

    # Write session
    with SQLiteConnection(str(output_path), session_name, mode="w") as session:
        df.atlas.to_atlas_session(session)
        print(f"  ✓ Wrote {len(df.columns)} channels to ATLAS")

    print(f"\n✓ Session created: {output_path}")

    if text_mappings:
        print("  Text column mappings:")
        for col, mapping in text_mappings.items():
            print(f"    {col}: {mapping}")

    print("  Open in ATLAS to view data.")
    return output_path


# ============== USAGE EXAMPLE ==============
if __name__ == "__main__":
    parquet_to_atlas(
        parquet_path="sample_vehicle_data.parquet",
        output_path=r"C:\ATLAS\Sessions\vehicle_import.ssndb",
        session_name="Vehicle Data Import",
        application_group="Vehicle",
        text_columns=["State", "Mode"],
    )
