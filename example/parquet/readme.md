# Parquet to ATLAS Import Utility

This utility provides a simple way to import **Parquet files** into an **ATLAS session (`.ssndb`)** using **Pandlas** as the underlying integration layer.

It is intended for users who already have time‑series data in Parquet format and want to visualise and analyse it in ATLAS with minimal setup.

---

## What Pandlas Provides

Most of the heavy lifting in this workflow is handled by **Pandlas**, which is the our Python interface for writing data into ATLAS sessions.

Pandlas is responsible for:

- Creating and managing the ATLAS session database (`.ssndb`)
- Writing time‑series channels from Pandas DataFrames into ATLAS
- Handling timestamp alignment using a `DatetimeIndex`
- Applying ATLAS metadata such as:
  - Application Group
  - Parameter Group
  - Units
- Ensuring the data is stored in a format ATLAS can read efficiently

In short, **Pandlas is the engine** that performs the actual data ingestion into ATLAS.

---

## What This Script Adds

The `Parquet_Atlas.py` script sits on top of Pandlas and adds **convenience, validation, and automation** around a few common tasks.

---

### 1. Parquet Loading and Validation

The script:

- Loads a Parquet file into a Pandas DataFrame
- Optionally converts a chosen column into a timestamp index
- Validates that the final DataFrame uses a `DatetimeIndex`
- Fails early with clear errors if timestamps are missing or invalid

This avoids subtle issues later during the ATLAS import.

---

### 2. Automatic ATLAS Metadata Assignment

Before handing the data to Pandlas, the script configures ATLAS metadata consistently:

#### Application Group
Used to group all imported channels under a common ATLAS namespace.

#### Parameter Group
Applied uniformly so channels appear neatly grouped in ATLAS.

#### Parameter Group Separator
Ensures consistent hierarchy inside ATLAS.

This removes the need for users to manually configure metadata channel‑by‑channel.

---

### 3. Automatic Unit Inference

If units are not explicitly provided, the script assigns units automatically based on column names.

Examples:

- Columns containing `pressure`, `bar` → `bar`
- Columns containing `temp`, `deg` → `°C`
- Columns containing `speed`, `velocity` → `km/h`
- Columns containing `rpm`, `revs` → `rpm`
- Position‑related columns → `mm`

Users can still override this completely by supplying their own unit mapping if needed.

---

### 4. Text / Enum Column Handling

ATLAS channels must be numeric. To support Parquet files containing text or enumerated states, the script can:

- Convert specified text columns into categorical codes
- Store each unique string as a numeric value
- Preserve a printed mapping between codes and original values

**Example:**

```
State:
  0 → "Idle"
  1 → "Running"
  2 → "Error"
```

This allows state‑based data to be visualised and analysed in ATLAS while retaining traceability back to the original values.

---

### 5. Safe, Minimal API Usage

To maximise compatibility across different Pandlas versions and forks, the script intentionally:

- Avoids optional helper APIs (e.g. lap markers, point markers)
- Uses only the core, stable Pandlas session and DataFrame interfaces

This makes the script reliable in:

- Customer environments
- Controlled IT setups
- Different Pandlas builds

---

## Typical Use Case

1. Start with a Parquet file containing time‑series data
2. Run the script, optionally specifying:
   - Timestamp column
   - Application / parameter group
   - Text columns
3. Open the generated `.ssndb` file in ATLAS
4. Explore, plot, and analyse the data exactly like native ATLAS sessions

---

## Summary

| Component | Responsibility |
|---------|----------------|
| **Pandlas** | Core ATLAS session creation and data ingestion |
| **This script** | Data preparation, validation, metadata setup, unit inference, enum handling |

Together, they provide a **lightweight but robust path from Parquet → ATLAS**, with minimal configuration required from the user.
