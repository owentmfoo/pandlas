# Pandlas

A Pandas extension for [ATLAS 10](https://www.motionapplied.com/), bridging Python
DataFrames and the SQLRace API through
[pythonnet](https://github.com/pythonnet/pythonnet).

> **Note — this package is not maintained nor officially supported by Motion Applied.**

[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)

---

## Features

| Category | Capability |
|----------|-----------|
| **Data writing** | Write Pandas DataFrames to ATLAS sessions (SQLite, SSN2, SQL Server) |
| **Multi-rate** | Mix parameters at different sample rates (1 kHz, 100 Hz, 10 Hz, 1 Hz, …) in the same session |
| **Synchro data** | Write engine-synchronous (variable-rate) channels with automatic packet management |
| **Text channels** | Write enumerated/string parameters via TextConversion lookup tables |
| **Live sessions** | Stream data into live sessions visible in ATLAS via the Server Listener / Recorder |
| **Parameter metadata** | Custom units, descriptions, display format, display limits, and warning limits per parameter |
| **Parameter grouping** | Auto-create parameter groups from column naming (e.g. `"Chassis/DamperFL"`) |
| **Session details** | Set session-level metadata: Driver, Circuit, Vehicle, Event, etc. |
| **Laps & markers** | Add, update laps, point markers, and range markers (individual or batch) |
| **Events** | Write discrete events with priority levels and status labels from DataFrames |
| **Data reading** | Read parameter samples back from historic sessions |
| **Automation** | Helpers for the ATLAS Automation API (open ATLAS, load sessions) |

## Requirements

- **ATLAS 10** installed with a valid licence (SQLRace option enabled)
- **Python 3.9 – 3.11**

## Installation

```bash
pip install "git+https://github.com/Software-Products/pandlas.git"
```

## Quick Start

### Write a session

```python
import pandas as pd
import numpy as np
from pandlas import SQLiteConnection

start = pd.Timestamp("now")
df = pd.DataFrame(
    {"speed": np.sin(np.linspace(0, 10 * np.pi, 1000))},
    index=pd.date_range(start, periods=1000, freq="s"),
)

with SQLiteConnection(r"C:\data\demo.ssndb", "DemoSession", mode="w") as session:
    df.atlas.to_atlas_session(session)
```

### Parameter metadata

All metadata is set via dictionaries keyed by `"{column}:{ApplicationGroupName}"`:

```python
df.atlas.ApplicationGroupName = "MyApp"
df.atlas.ParameterGroupIdentifier = "Chassis"

df.atlas.units            = {"speed:MyApp": "m/s"}
df.atlas.descriptions     = {"speed:MyApp": "Vehicle speed"}
df.atlas.display_format   = {"speed:MyApp": "%6.1f"}
df.atlas.display_limits   = {"speed:MyApp": (0.0, 400.0)}
df.atlas.warning_limits   = {"speed:MyApp": (0.0, 370.0)}
```

### Multi-rate parameters

Write DataFrames at different frequencies into the same session — each gets its
own channel and sample interval:

```python
with SQLiteConnection(db_path, "MultiRate", mode="w") as session:
    # 100 Hz suspension data
    df_fast = pd.DataFrame(
        {"damper_fl": data_100hz},
        index=pd.date_range(start, periods=len(data_100hz), freq="10ms"),
    )
    df_fast.atlas.to_atlas_session(session)

    # 1 Hz strategy data
    df_slow = pd.DataFrame(
        {"fuel_remaining": data_1hz},
        index=pd.date_range(start, periods=len(data_1hz), freq="s"),
    )
    df_slow.atlas.to_atlas_session(session)
```

### Laps and markers

```python
from pandlas import add_lap, add_point_marker, add_range_marker

add_lap(session, start + pd.Timedelta(60, "s"), lap_name="Lap 2")
add_point_marker(session, start + pd.Timedelta(30, "s"), "Pit Entry")
add_range_marker(session, start + pd.Timedelta(10, "s"),
                 start + pd.Timedelta(20, "s"), "Safety Car")
```

### Synchro (variable-rate) data

Engine-synchronous channels where the sample rate varies with RPM:

```python
from pandlas import add_synchro_data
import numpy as np

# samples: float64 array, timestamps_ns: int64 array (nanosecond offsets)
samples = np.random.randn(50_000)
timestamps_ns = np.cumsum(np.full(50_000, 80_000, dtype=np.int64))  # ~12.5 kHz

add_synchro_data(
    session,
    parameter_name="CylPressure",
    app_group="EngineSync",
    samples=samples,
    timestamps=timestamps_ns,
    unit="bar",
    description="Cylinder pressure (crank-synchronous)",
)
```

The module handles byte packing, GCD-based delta scaling, automatic packet
splitting (≤ 65 535 samples per packet), config creation, and sequence-number
management. See `example/synchro_benchmark.py` for a full benchmark.

### Session details

Set session-level metadata visible in ATLAS session properties:

```python
from pandlas import set_session_details

set_session_details(session, {
    "Driver": "Lando Norris",
    "Circuit": "Silverstone",
    "Vehicle": "MCL60",
    "Event": "British GP 2026",
})
```

### Events

Write discrete events from a DataFrame — each row becomes an ATLAS event with
associated numeric values:

```python
import pandas as pd
from pandlas import add_events

# Each row = one event; every numeric column is an event value
events = pd.DataFrame({
    "timestamp": pd.date_range("2026-04-01 14:00", periods=5, freq="30s"),
    "Status":         ["LK-001", "LK-002", "LK-003", "LK-004", "LK-005"],
    "BrakePressure": [32.1, 28.5, 35.0, 30.2, 29.8],
    "WheelSlip":     [0.12, 0.08, 0.15, 0.10, 0.09],
})

add_events(
    session, events,
    status_column="Status",          # shown in ATLAS Status column
    description="Brake Lockup",
    priority="high",                 # "low", "medium", or "high"
    application_group="BrakeApp",
)
```

Events support multiple groups in the same session — just call `add_events()`
once per event type (e.g. lockups, flags, pit stops).

### Parameter grouping

Use a separator in column names to auto-create parameter groups:

```python
df = pd.DataFrame({
    "Chassis/DamperFL": damper_fl_data,
    "Chassis/DamperFR": damper_fr_data,
    "Engine/RPM":       rpm_data,
    "Engine/OilTemp":   oil_data,
}, index=timestamps)

df.atlas.ApplicationGroupName = "CarData"
df.atlas.parameter_group_separator = "/"
df.atlas.to_atlas_session(session)
# Creates groups "Chassis" and "Engine" automatically
```

### Text channels

Write enumerated parameters that display as string labels in ATLAS:

```python
from pandlas import add_text_channel

gears = ["N", "1", "2", "3", "4", "5", "6", "7", "8"]
gear_values = [gears[i % len(gears)] for i in range(len(timestamps))]

add_text_channel(
    session,
    parameter_name="Gear",
    values=gear_values,
    timestamps=timestamps,
    application_group="DriverInputs",
)
```

### Live session

```python
from pandlas import SQLiteConnection

with SQLiteConnection(db_path, "LiveDemo", mode="w", recorder=True) as session:
    # The session is now visible in ATLAS as a live session.
    # Write data in a loop and it updates in real time.
    ...
```

### Read data back

```python
from pandlas import SQLiteConnection, get_samples

with SQLiteConnection(db_path, session_key="<key>", mode="r") as session:
    samples, timestamps = get_samples(session, "speed:MyApp")
```

## Session Backends

| Backend | Class | Use case |
|---------|-------|----------|
| SQLite / SSN2 | `SQLiteConnection`, `Ssn2Session` | Local / file-based sessions |
| SQL Server | `SQLRaceDBConnection` | Shared database sessions |

## Performance

Data serialisation uses vectorised NumPy operations for both sample bytes and
timestamp arrays, avoiding Python-level loops on the hot path. Typical
throughput when writing to SQL Server:

| Data type | Metric | Value |
|-----------|--------|-------|
| **Row data** | Config creation (first batch) | ~2–3 s |
| **Row data** | Streaming write rate | ~40 000–50 000 samples/s |
| **Synchro** | Write rate (variable-interval) | ~50 000–200 000 samples/s |

Run `example/multirate_session.py` to benchmark your setup. The script writes
multi-rate row data and synchro signals, then reads back and validates metadata
and data integrity. For large-scale synchro benchmarks, use
`example/synchro_benchmark.py`.

## Dependencies

| Package | Purpose |
|---------|---------|
| `pandas` | DataFrame core |
| `numpy` | Vectorised data serialisation |
| `pythonnet` | .NET CLR interop |
| `tqdm` | Progress bars |
| `requests` | HTTP client (live examples only) |

## Known Limitations

- The DataFrame index must be a `DatetimeIndex`
- Events and text channels require timestamps within the session time range

## Examples

See the [`example/`](example/) directory:

| Script | Description |
|--------|-------------|
| `complete_showcase.py` | **All 7 features** in one session — the best starting point |
| `create_session.py` | Historic session with multiple rates, metadata, laps, and markers |
| `event_logging.py` | Discrete events (lockups, flags, pit stops) from DataFrames |
| `live_crypto.py` | 📈 Live Binance ticker — 20 crypto assets streaming at 100 Hz with spike alerts |
| `live_sessions.py` | Live streaming to SQLite and SQL Server |
| `live_weather.py` | ☁️ Live Silverstone weather from Open-Meteo with gust/rain events |
| `multirate_session.py` | Multi-rate performance benchmark with full validation (includes synchro signals) |
| `session_features.py` | Session details, parameter grouping, and text channels |
| `synchro_benchmark.py` | Synchro data benchmark — RPM-driven variable-rate channels at 100 K / 1 M / 10 M scale |
| `where_the_iss_at.py` | Live session fed from the ISS position API |