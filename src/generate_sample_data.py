"""Generate the bundled SAMPLE EMA-screener-style signal data.

The records mimic the output of a 20/30 EMA screener (ticker, close, EMA20,
EMA30, daily change %, signal BUY / WATCH / AVOID). Prices are synthetic,
generated from a fixed seed so the sample is reproducible — this is SAMPLE
data for demonstrating the pipeline, NOT live trading results.

The signal rules match the screener:
  BUY   - close crossed above BOTH EMA20 and EMA30 (fresh breakout)
  WATCH - price is near (within 2%) of EMA20 or EMA30, no fresh cross
  AVOID - close below BOTH EMAs (breakdown)

Usage:
  python src/generate_sample_data.py            # writes data/sample_signals.csv
"""
from __future__ import annotations

import csv
import math
import random
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
OUT = BASE / "data" / "sample_signals.csv"
NEAR_PCT = 0.02

# (ticker, universe, start price, drift profile)
TICKERS = [
    ("AAPL", "US500", 232.0, "up"),
    ("MSFT", "US500", 428.0, "flat"),
    ("NVDA", "US500", 131.0, "breakout"),
    ("AMZN", "US500", 197.0, "down"),
    ("META", "US500", 585.0, "up"),
    ("GOOGL", "US500", 165.0, "flat"),
    ("JPM", "US500", 218.0, "breakout"),
    ("XOM", "US500", 119.0, "down"),
    ("UNH", "US500", 305.0, "down"),
    ("AVGO", "US500", 172.0, "breakout"),
    ("COST", "US500", 925.0, "flat"),
    ("CRM", "US500", 264.0, "up"),
    ("NFLX", "US500", 762.0, "breakout"),
    ("AMD", "US500", 122.0, "down"),
    ("RELIANCE.NS", "IN500", 2985.0, "flat"),
    ("TCS.NS", "IN500", 4110.0, "down"),
    ("INFY.NS", "IN500", 1890.0, "breakout"),
    ("HDFCBANK.NS", "IN500", 1745.0, "up"),
    ("SBIN.NS", "IN500", 812.0, "breakout"),
    ("TATAMOTORS.NS", "IN500", 985.0, "down"),
]

DRIFT = {"up": 0.0012, "flat": 0.0002, "down": -0.0022, "breakout": -0.0025}
HISTORY_DAYS = 60          # warm-up so EMA20/EMA30 are meaningful
EMIT_DAYS = 6              # days of events written to the sample file
DATES = [f"2026-10-{d:02d}" for d in (8, 9, 12, 13, 14, 15, 16, 19, 20, 21)]


def ema_series(values: list[float], span: int) -> list[float]:
    alpha = 2.0 / (span + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(alpha * v + (1 - alpha) * out[-1])
    return out


def signal_for(close: list[float]) -> str:
    e20 = ema_series(close, 20)
    e30 = ema_series(close, 30)
    c, pc = close[-1], close[-2]
    v20, pv20 = e20[-1], e20[-2]
    v30, pv30 = e30[-1], e30[-2]
    above = c > v20 and c > v30
    below = c < v20 and c < v30
    cross_up = above and (pc <= pv20 or pc <= pv30)
    cross_down = below and (pc >= pv20 or pc >= pv30)
    near = abs(c - v20) / v20 <= NEAR_PCT or abs(c - v30) / v30 <= NEAR_PCT
    if cross_up:
        return "BUY"
    if cross_down or below:
        return "AVOID"
    if near or above:
        return "WATCH"
    return "AVOID" if c < v20 else "WATCH"


def main() -> None:
    rng = random.Random(42)
    rows = []
    need = HISTORY_DAYS - (len(DATES) - EMIT_DAYS)
    for ticker, universe, start, profile in TICKERS:
        prices = [start]
        for i in range(HISTORY_DAYS - 1):
            drift = DRIFT[profile]
            if profile == "breakout" and i >= HISTORY_DAYS - 5:
                drift = 0.016  # sharp late push through both EMAs, inside
                # the emitted window -> a visible fresh BUY cross
            shock = rng.gauss(0, 0.008)
            prices.append(round(prices[-1] * math.exp(drift + shock), 2))
        e20 = ema_series(prices, 20)
        e30 = ema_series(prices, 30)
        for k in range(EMIT_DAYS):
            idx = HISTORY_DAYS - EMIT_DAYS + k
            chg = (prices[idx] - prices[idx - 1]) / prices[idx - 1] * 100
            rows.append({
                "ticker": ticker,
                "universe": universe,
                "trade_date": DATES[len(DATES) - EMIT_DAYS + k],
                "event_time": f"{DATES[len(DATES) - EMIT_DAYS + k]}T21:00:00Z",
                "signal": signal_for(prices[: idx + 1]),
                "close": prices[idx],
                "ema20": round(e20[idx], 2),
                "ema30": round(e30[idx], 2),
                "chg_pct": round(chg, 2),
                "source": "ema-screener-sample",
            })
            _ = need
    rows.sort(key=lambda r: (r["trade_date"], r["ticker"]))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    counts = {}
    for r in rows:
        counts[r["signal"]] = counts.get(r["signal"], 0) + 1
    print(f"wrote {OUT} ({len(rows)} SAMPLE records) {counts}")


if __name__ == "__main__":
    main()
