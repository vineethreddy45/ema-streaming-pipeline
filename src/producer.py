"""Producer — emit EMA screener signal records as a stream.

Two modes:

  file  (default, runs anywhere, no broker needed)
        Replays the bundled sample CSV into a landing directory as one JSON
        file per trading day. Spark Structured Streaming watches that
        directory, so each file lands like a Kafka micro-batch. This is the
        local simulation of a Kafka producer.

  kafka (production shape)
        Publishes the same records to a Kafka topic as JSON messages, one
        key per ticker. Requires `kafka-python` and a reachable broker:
            KAFKA_BOOTSTRAP=localhost:9092 python src/producer.py --mode kafka

In production this role is played by the screener job (or any upstream
system) publishing one message per ticker per scan to the `ema.signals`
topic; the streaming pipeline does not care which mode produced the data.

Usage:
  python src/producer.py [--mode file|kafka] [--delay 1.0]
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
SAMPLE_CSV = BASE / "data" / "sample_signals.csv"
LANDING_DIR = BASE / "data" / "stream_landing"
TOPIC = os.environ.get("KAFKA_TOPIC", "ema.signals")


def load_records() -> list[dict]:
    with SAMPLE_CSV.open() as f:
        rows = list(csv.DictReader(f))
    for r in rows:  # type the numeric fields the same way the screener emits them
        for col in ("close", "ema20", "ema30", "chg_pct"):
            r[col] = float(r[col])
    return rows


def produce_file(records: list[dict], delay: float) -> None:
    """Write one JSON-lines file per trade_date into the landing directory."""
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    by_date: dict[str, list[dict]] = {}
    for r in records:
        by_date.setdefault(r["trade_date"], []).append(r)
    for date in sorted(by_date):
        path = LANDING_DIR / f"signals-{date}.json"
        with path.open("w") as f:
            for r in by_date[date]:
                f.write(json.dumps(r) + "\n")
        print(f"[producer:file] wrote {len(by_date[date])} records -> {path.name}")
        if delay:
            time.sleep(delay)


def produce_kafka(records: list[dict], delay: float) -> None:
    """Publish records to Kafka. Only used with --mode kafka."""
    from kafka import KafkaProducer  # kafka-python; not needed in file mode

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for r in records:
        producer.send(TOPIC, key=r["ticker"], value=r)
        if delay:
            time.sleep(delay / max(len(records), 1))
    producer.flush()
    producer.close()
    print(f"[producer:kafka] published {len(records)} records to topic '{TOPIC}'")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["file", "kafka"], default="file")
    ap.add_argument("--delay", type=float, default=0.0,
                    help="seconds between daily batches (file mode)")
    args = ap.parse_args()
    records = load_records()
    print(f"[producer] {len(records)} SAMPLE records loaded from {SAMPLE_CSV.name}")
    if args.mode == "kafka":
        produce_kafka(records, args.delay)
    else:
        produce_file(records, args.delay)


if __name__ == "__main__":
    main()
