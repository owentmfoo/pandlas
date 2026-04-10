"""Live Crypto Ticker — Binance Public API
==========================================

Streams real-time prices for 20 crypto assets into an ATLAS live session.
Uses the Binance public REST API (no key, no auth required).

Features demonstrated:
  - Live session (recorder mode) with 100 Hz streaming
  - 20 parameters grouped by market cap tier
  - Events: price spike alerts (high), new 24h high/low (medium),
            volume surge (low)
  - Point markers on significant price moves
  - Session details with market metadata

Data source: https://api.binance.com (public, no auth)

Usage:
    python live_crypto.py
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

SQLITE_DB_DIR = r"C:\McLaren Applied\pandlas\LiveCrypto.ssndb"

SERVER = r"MCLA-525Q374\LOCAL"
DATABASE = "SQLRACE02"

POLL_INTERVAL_S = 5        # API poll cadence
STREAM_INTERVAL_S = 0.01  # 100 Hz to ATLAS
SESSION_DURATION_S = 120  # 2 minutes

# 20 assets grouped by tier
SYMBOLS = {
    "Majors": ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"],
    "MidCap": ["DOGEUSDT", "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT"],
    "AltL1":  ["MATICUSDT", "NEARUSDT", "ATOMUSDT", "ALGOUSDT", "FILUSDT"],
    "Others": ["SHIBUSDT", "LTCUSDT", "TRXUSDT", "UNIUSDT", "XLMUSDT"],
}

ALL_SYMBOLS = [s for group in SYMBOLS.values() for s in group]

# Units for each parameter (Group/Asset → unit)
PARAM_UNITS = {
    f"{group}/{sym.replace('USDT', '')}": "USD"
    for group, syms in SYMBOLS.items()
    for sym in syms
}

# Thresholds
PRICE_SPIKE_PCT = 0.5    # % move between polls triggers HIGH event
VOLUME_SURGE_MULT = 1.5  # volume jump multiplier triggers LOW event

SESSION_DETAILS = {
    "Source": "Binance Public API",
    "Assets": str(len(ALL_SYMBOLS)),
    "Quote": "USDT",
    "Comment": "Live crypto ticker — Pandlas showcase",
}

# Binance endpoints
PRICE_URL = "https://api.binance.com/api/v3/ticker/price"
TICKER_24H_URL = "https://api.binance.com/api/v3/ticker/24hr"


def open_connection(identifier):
    if BACKEND == "sqlite":
        return SQLiteConnection(
            SQLITE_DB_DIR, identifier, mode="w", recorder=True,
        )
    return SQLRaceDBConnection(
        SERVER, DATABASE, identifier, mode="w", recorder=True,
    )


def clean_name(symbol: str) -> str:
    """BTCUSDT → BTC"""
    return symbol.replace("USDT", "")


def fetch_prices() -> dict | None:
    """Fetch current prices for all symbols. Returns {symbol: price}."""
    try:
        import json
        sym_str = json.dumps(ALL_SYMBOLS, separators=(",", ":"))
        url = f"{PRICE_URL}?symbols={sym_str}"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            return {item["symbol"]: float(item["price"]) for item in resp.json()}
        else:
            logger.warning("Price API returned %d: %s", resp.status_code, resp.text[:100])
    except Exception as exc:
        logger.warning("Price API failed: %s", exc)
    return None


def fetch_24h_stats() -> dict | None:
    """Fetch 24h stats for all symbols. Returns {symbol: {...}}."""
    try:
        resp = requests.get(TICKER_24H_URL, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            return {
                item["symbol"]: {
                    "high": float(item["highPrice"]),
                    "low": float(item["lowPrice"]),
                    "change_pct": float(item["priceChangePercent"]),
                    "volume": float(item["quoteVolume"]),
                }
                for item in data
                if item["symbol"] in ALL_SYMBOLS
            }
    except Exception as exc:
        logger.warning("24h stats API failed: %s", exc)
    return None


def main():
    print(f"\n{'=' * 64}")
    print("  📈  Pandlas Live Crypto Ticker")
    print(f"{'=' * 64}")
    print(f"  Backend:      {BACKEND}")
    print(f"  Assets:       {len(ALL_SYMBOLS)} crypto pairs")
    for group, symbols in SYMBOLS.items():
        names = ", ".join(clean_name(s) for s in symbols)
        print(f"    {group:10s}  {names}")
    print(f"  Poll rate:    every {POLL_INTERVAL_S} s (API)")
    print(f"  Stream rate:  {1/STREAM_INTERVAL_S:.0f} Hz to ATLAS")
    print(f"  Duration:     {SESSION_DURATION_S} s")
    print(f"  Spike alert:  >{PRICE_SPIKE_PCT}% move between polls")
    print(f"  API:          Binance (public, no auth)")
    print(f"{'=' * 64}\n")

    # Fetch initial 24h stats for baseline
    stats_24h = fetch_24h_stats() or {}

    # Event accumulators
    spike_events = []
    high_low_events = []
    info_events = []
    marker_log = []

    prev_prices = {}
    last_prices = None
    sample_count = 0

    with open_connection("Crypto Ticker") as session:

        set_session_details(session, SESSION_DETAILS)
        print("  Session is LIVE — open in ATLAS to watch data stream in.\n")

        t_start = time.time()
        next_poll = t_start

        while (time.time() - t_start) < SESSION_DURATION_S:
            now_wall = time.time()

            # ── Poll API at cadence ───────────────────────────────────
            if now_wall >= next_poll:
                prices = fetch_prices()
                if prices is not None:
                    now_ts = pd.Timestamp.now()

                    # Build grouped row: Group/Asset → price
                    last_prices = {}
                    for group, symbols in SYMBOLS.items():
                        for sym in symbols:
                            col = f"{group}/{clean_name(sym)}"
                            last_prices[col] = prices.get(sym, 0.0)

                    elapsed = now_wall - t_start
                    btc = prices.get("BTCUSDT", 0)
                    eth = prices.get("ETHUSDT", 0)
                    sol = prices.get("SOLUSDT", 0)
                    print(f"  [{elapsed:5.0f}s]  BTC ${btc:,.2f}  "
                          f"ETH ${eth:,.2f}  SOL ${sol:,.2f}  "
                          f"({len(prices)} assets)")

                    # ── Detect events ─────────────────────────────────
                    for sym in ALL_SYMBOLS:
                        name = clean_name(sym)
                        price = prices.get(sym, 0)

                        if sym in prev_prices and prev_prices[sym] > 0:
                            pct_change = (
                                (price - prev_prices[sym])
                                / prev_prices[sym] * 100
                            )

                            # HIGH: price spike
                            if abs(pct_change) > PRICE_SPIKE_PCT:
                                direction = "UP" if pct_change > 0 else "DOWN"
                                spike_events.append({
                                    "timestamp": now_ts,
                                    "Status": f"{name}-{direction}-"
                                              f"{abs(pct_change):.2f}%",
                                    "PctChange": abs(pct_change),
                                    "Price": price,
                                })
                                add_point_marker(
                                    session, now_ts,
                                    f"⚡ {name} {direction} "
                                    f"{abs(pct_change):.2f}%",
                                )
                                marker_log.append(
                                    f"{name} {direction} {abs(pct_change):.2f}%"
                                )

                        # MEDIUM: new 24h high or low
                        if sym in stats_24h:
                            s = stats_24h[sym]
                            if price > s["high"]:
                                high_low_events.append({
                                    "timestamp": now_ts,
                                    "Status": f"{name}-NEW-24H-HIGH",
                                    "Price": price,
                                })
                                add_point_marker(
                                    session, now_ts,
                                    f"🔺 {name} new 24h high ${price:,.4f}",
                                )
                                marker_log.append(
                                    f"{name} new 24h high"
                                )
                                stats_24h[sym]["high"] = price
                            elif price < s["low"]:
                                high_low_events.append({
                                    "timestamp": now_ts,
                                    "Status": f"{name}-NEW-24H-LOW",
                                    "Price": price,
                                })
                                add_point_marker(
                                    session, now_ts,
                                    f"🔻 {name} new 24h low ${price:,.4f}",
                                )
                                marker_log.append(
                                    f"{name} new 24h low"
                                )
                                stats_24h[sym]["low"] = price

                    prev_prices = dict(prices)

                next_poll = now_wall + POLL_INTERVAL_S

            # ── Stream last known values at 100 Hz ────────────────────
            if last_prices is not None:
                now_ts = pd.Timestamp.now()
                df = pd.DataFrame(
                    [{k: np.float32(v) for k, v in last_prices.items()}],
                    index=pd.DatetimeIndex([now_ts]),
                )
                df.atlas.parameter_group_separator = "/"
                df.atlas.units = PARAM_UNITS
                df.atlas.to_atlas_session(session, show_progress_bar=False)
                sample_count += 1

            time.sleep(STREAM_INTERVAL_S)

        # ── Write collected events ────────────────────────────────────
        total_events = 0
        if spike_events:
            n = add_events(
                session, pd.DataFrame(spike_events),
                status_column="Status",
                description="Price Spike",
                priority="high",
                application_group="Market",
            )
            total_events += n

        if high_low_events:
            n = add_events(
                session, pd.DataFrame(high_low_events),
                status_column="Status",
                description="24h High/Low",
                priority="medium",
                application_group="Market",
            )
            total_events += n

        if info_events:
            n = add_events(
                session, pd.DataFrame(info_events),
                status_column="Status",
                description="Volume Surge",
                priority="low",
                application_group="Market",
            )
            total_events += n

    # ── Summary ──────────────────────────────────────────────────────
    print(f"\n{'=' * 64}")
    print("  SESSION COMPLETE")
    print(f"{'=' * 64}")
    print(f"  Samples:    {sample_count:,} rows streamed to ATLAS")
    print(f"  Assets:     {len(ALL_SYMBOLS)} crypto pairs in 4 groups")
    print(f"  Markers:    {len(marker_log)}")
    for m in marker_log[:10]:
        print(f"    • {m}")
    if len(marker_log) > 10:
        print(f"    ... and {len(marker_log) - 10} more")
    print(f"  Events:     {total_events} total")
    if spike_events:
        print(f"    HIGH:     {len(spike_events)} price spikes")
    if high_low_events:
        print(f"    MEDIUM:   {len(high_low_events)} new 24h highs/lows")
    if info_events:
        print(f"    LOW:      {len(info_events)} volume surges")
    print(f"{'=' * 64}")


if __name__ == "__main__":
    main()
