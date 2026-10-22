"""Read back the Delta tables and print counts + sample rows.

Usage: python src/show_outputs.py
"""
from __future__ import annotations

from pathlib import Path

from common import build_spark

BASE = Path(__file__).resolve().parent.parent
DELTA = BASE / "output" / "delta"
TABLES = [
    "bronze_signals",
    "silver_signals",
    "gold_latest_signal_per_ticker",
    "gold_signal_counts_by_day",
    "gold_new_buy_crossovers",
]


def main() -> None:
    spark = build_spark("ema-show-outputs")
    for name in TABLES:
        df = spark.read.format("delta").load(str(DELTA / name))
        print(f"\n=== {name}: {df.count()} rows ===")
        df.show(8, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
