"""Live Weather Station — Silverstone Circuit
==============================================

Streams real-time weather data from the Open-Meteo API into an ATLAS live
session for 2 minutes.  No API key required.

Features demonstrated:
  - Live session (recorder mode)
  - Real-time row data streaming
  - Automatic event generation (wind gusts, rain, temperature alerts)
  - Point markers on weather transitions
  - Session details

Data source: https://open-meteo.com (free, no auth)

Usage:
    python live_weather.py
"""

import time
import logging
import numpy as np
import pandas as pd
import requests

from pandlas import (
    SQLiteConnection,
    SQLRaceDBConnection,
    add_point_marker,
    add_events,
    set_session_details,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════
#  Configuration
# ═══════════════════════════════════════════════════════════════════════
BACKEND = "sqlserver"

SQLITE_DB_DIR = r"C:\McLaren Applied\pandlas\LiveWeather.ssndb"

SERVER = r"MCLA-525Q374\LOCAL"
DATABASE = "SQLRACE02"

# Silverstone Circuit coordinates
LATITUDE = 52.0705
LONGITUDE = -1.0165
LOCATION_NAME = "Silverstone"

# Open-Meteo current weather endpoint (free, no key)
API_URL = (
    f"https://api.open-meteo.com/v1/forecast"
    f"?latitude={LATITUDE}&longitude={LONGITUDE}"
    f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,"
    f"precipitation,rain,wind_speed_10m,wind_direction_10m,"
    f"wind_gusts_10m,surface_pressure,cloud_cover"
    f"&timezone=Europe%2FLondon"
)

POLL_INTERVAL_S = 10       # seconds between API calls
STREAM_INTERVAL_S = 0.01  # seconds between ATLAS writes (100 Hz)
SESSION_DURATION_S = 120  # 2 minutes

# Thresholds for automatic events and markers
WIND_GUST_THRESHOLD_KMH = 15.0
RAIN_THRESHOLD_MM = 0.1
TEMP_DROP_THRESHOLD_C = 2.0

SESSION_DETAILS = {
    "Location": LOCATION_NAME,
    "Latitude": str(LATITUDE),
    "Longitude": str(LONGITUDE),
    "Source": "Open-Meteo API",
    "Comment": "Live weather streaming demo",
}

# Columns we care about from the API
WEATHER_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "precipitation",
    "rain",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "surface_pressure",
    "cloud_cover",
]

WEATHER_UNITS = {
    "temperature_2m":       "degC",
    "relative_humidity_2m": "%",
    "apparent_temperature": "degC",
    "precipitation":        "mm",
    "rain":                 "mm",
    "wind_speed_10m":       "km/h",
    "wind_direction_10m":   "deg",
    "wind_gusts_10m":       "km/h",
    "surface_pressure":     "hPa",
    "cloud_cover":          "%",
}


def open_connection(identifier):
    if BACKEND == "sqlite":
        return SQLiteConnection(
            SQLITE_DB_DIR, identifier, mode="w", recorder=True,
        )
    return SQLRaceDBConnection(
        SERVER, DATABASE, identifier, mode="w", recorder=True,
    )


def fetch_weather() -> dict | None:
    """Fetch current weather from Open-Meteo. Returns None on failure."""
    try:
        resp = requests.get(API_URL, timeout=5)
        if resp.status_code == 200:
            return resp.json()["current"]
    except Exception as exc:
        logger.warning("API request failed: %s", exc)
    return None


def main():
    print(f"\n{'=' * 64}")
    print("  ☁️  Pandlas Live Weather Station")
    print(f"{'=' * 64}")
    print(f"  Location:     {LOCATION_NAME} ({LATITUDE}, {LONGITUDE})")
    print(f"  Backend:      {BACKEND}")
    print(f"  Poll rate:    every {POLL_INTERVAL_S} s (API)")
    print(f"  Stream rate:  {1/STREAM_INTERVAL_S:.0f} Hz to ATLAS")
    print(f"  Duration:     {SESSION_DURATION_S} s")
    print(f"  API:          Open-Meteo (free, no key)")
    print(f"  Events on:    gusts > {WIND_GUST_THRESHOLD_KMH} km/h, "
          f"rain > {RAIN_THRESHOLD_MM} mm")
    print(f"{'=' * 64}\n")

    # Collect events for batch write at end
    gust_events = []
    rain_events = []
    info_events = []
    marker_log = []

    prev_rain = 0.0
    prev_temp = None
    prev_gust_above = False
    sample_count = 0
    last_row = None  # last known weather values

    with open_connection(f"Weather {LOCATION_NAME}") as session:

        set_session_details(session, SESSION_DETAILS)
        print("  Session is LIVE — open in ATLAS to watch data stream in.\n")

        t_start = time.time()
        next_poll = t_start  # poll immediately on first iteration

        while (time.time() - t_start) < SESSION_DURATION_S:
            now_wall = time.time()

            # ── Poll API at POLL_INTERVAL_S cadence ───────────────────
            if now_wall >= next_poll:
                weather = fetch_weather()
                if weather is not None:
                    last_row = {
                        field: float(weather.get(field, 0))
                        for field in WEATHER_FIELDS
                    }

                    # Event / marker detection only on fresh API data
                    temp = last_row["temperature_2m"]
                    gusts = last_row["wind_gusts_10m"]
                    rain = last_row["precipitation"]
                    cloud = last_row["cloud_cover"]
                    now_ts = pd.Timestamp.now()

                    elapsed = now_wall - t_start
                    print(f"  [{elapsed:5.0f}s] {temp:.1f}°C  "
                          f"💨 {last_row['wind_speed_10m']:.1f} km/h "
                          f"(gusts {gusts:.1f})  "
                          f"🌧️ {rain:.1f} mm  ☁️ {cloud:.0f}%")

                    # ── Wind gust event (HIGH) — edge-triggered ───────
                    gust_above = gusts > WIND_GUST_THRESHOLD_KMH
                    if gust_above and not prev_gust_above:
                        gust_events.append({
                            "timestamp": now_ts,
                            "Status": f"GUST-{gusts:.0f}KMH",
                            "WindGust": gusts,
                            "WindDir": last_row["wind_direction_10m"],
                        })
                        add_point_marker(session, now_ts,
                                         f"⚠ Gust {gusts:.0f} km/h")
                        marker_log.append(f"Gust {gusts:.0f} km/h")
                    elif not gust_above and prev_gust_above:
                        gust_events.append({
                            "timestamp": now_ts,
                            "Status": "GUST-CLEAR",
                            "WindGust": gusts,
                            "WindDir": last_row["wind_direction_10m"],
                        })
                        add_point_marker(session, now_ts, "✓ Gusts subsided")
                        marker_log.append("Gusts subsided")
                    prev_gust_above = gust_above

                    # ── Rain onset/stop (MEDIUM) ──────────────────────
                    if rain > RAIN_THRESHOLD_MM and prev_rain <= RAIN_THRESHOLD_MM:
                        rain_events.append({
                            "timestamp": now_ts,
                            "Status": "RAIN-START",
                            "Precipitation": rain,
                        })
                        add_point_marker(session, now_ts, "🌧 Rain Started")
                        marker_log.append("Rain started")
                    elif rain <= RAIN_THRESHOLD_MM and prev_rain > RAIN_THRESHOLD_MM:
                        rain_events.append({
                            "timestamp": now_ts,
                            "Status": "RAIN-STOP",
                            "Precipitation": rain,
                        })
                        add_point_marker(session, now_ts, "☀ Rain Stopped")
                        marker_log.append("Rain stopped")
                    prev_rain = rain

                    # ── Temperature shift (LOW) ───────────────────────
                    if prev_temp is not None:
                        delta = abs(temp - prev_temp)
                        if delta >= TEMP_DROP_THRESHOLD_C:
                            direction = "RISE" if temp > prev_temp else "DROP"
                            info_events.append({
                                "timestamp": now_ts,
                                "Status": f"TEMP-{direction}-{delta:.1f}C",
                                "Value": temp,
                            })
                            add_point_marker(
                                session, now_ts,
                                f"🌡 Temp {direction.lower()} {delta:.1f}°C",
                            )
                            marker_log.append(
                                f"Temp {direction.lower()} {delta:.1f}°C"
                            )
                    prev_temp = temp

                next_poll = now_wall + POLL_INTERVAL_S

            # ── Stream last known values at 100 Hz ────────────────────
            if last_row is not None:
                now_ts = pd.Timestamp.now()
                df = pd.DataFrame(
                    [last_row],
                    index=pd.DatetimeIndex([now_ts]),
                )
                df.atlas.units = WEATHER_UNITS
                df.atlas.to_atlas_session(session, show_progress_bar=False)
                sample_count += 1

            time.sleep(STREAM_INTERVAL_S)

        # ── Write collected events as batch ───────────────────────────
        total_events = 0
        if gust_events:
            n = add_events(
                session, pd.DataFrame(gust_events),
                status_column="Status",
                description="Wind Gusts",
                priority="high",
                application_group="Weather",
            )
            total_events += n

        if rain_events:
            n = add_events(
                session, pd.DataFrame(rain_events),
                status_column="Status",
                description="Precipitation",
                priority="medium",
                application_group="Weather",
            )
            total_events += n

        if info_events:
            n = add_events(
                session, pd.DataFrame(info_events),
                status_column="Status",
                description="Temperature Shift",
                priority="low",
                application_group="Weather",
            )
            total_events += n

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'=' * 64}")
    print("  SESSION COMPLETE")
    print(f"{'=' * 64}")
    print(f"  Samples:    {sample_count} weather readings")
    print(f"  Markers:    {len(marker_log)}")
    for m in marker_log:
        print(f"    • {m}")
    print(f"  Events:     {total_events} total")
    if gust_events:
        print(f"    HIGH:     {len(gust_events)} wind gust alerts")
    if rain_events:
        print(f"    MEDIUM:   {len(rain_events)} rain transitions")
    if info_events:
        print(f"    LOW:      {len(info_events)} temperature shifts")
    print(f"{'=' * 64}")


if __name__ == "__main__":
    main()
