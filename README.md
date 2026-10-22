# EMA Signal Streaming Pipeline — Kafka → Spark Structured Streaming → Delta Lake

A streaming data pipeline that turns stock-market screener signals into
analytics-ready Delta Lake tables, using the **medallion architecture**
(bronze → silver → gold) — the same pattern used for production pipelines
on Azure Databricks.

**In one sentence:** a producer streams EMA (exponential moving average)
screener signals — BUY / WATCH / AVOID per ticker — Spark Structured
Streaming lands them raw in a bronze Delta table, cleans and de-duplicates
them into silver, and gold tables answer the questions a trader or analyst
actually asks: *What is each ticker's latest signal? How many BUYs today?
Which tickers just crossed into BUY?*

> **About the data:** the bundled dataset is **sample data**, generated to
> mimic a real 20/30 EMA screener's output format so the pipeline can run
> anywhere. It is **not** live market data and **not** trading advice.

---

## Why this project exists

Batch screeners produce a fresh snapshot every day, but consumers want the
signals as a *stream of events* they can build on: alerting on fresh BUY
crossovers, tracking signal history, joining with other feeds. This project
takes signals in that shape — one event per ticker per scan — and processes
them with the streaming + medallion design used in production data
engineering:

- **Kafka** (or a local file-stream simulation of it) as the event source
- **Apache Spark Structured Streaming** as the processing engine
- **Delta Lake** as the storage layer — ACID tables, schema enforcement,
  and time travel at every layer

## Architecture

```
                 ┌────────────────────────── local simulation (default) ──────────────────────────┐
                 │                                                                                  │
 EMA screener    │   producer.py              pipeline.py (Spark Structured Streaming)             │
 (or sample      │   --mode file              ┌────────────┐   ┌────────────┐   ┌────────────────┐  │
  generator) ────┼──► landing dir  ──stream──►│  BRONZE    │──►│  SILVER    │──►│  GOLD          │  │
                 │   (1 JSON file             │ raw Delta  │   │ clean Delta│   │ latest signal  │  │
                 │    per day)                │ + ingest   │   │ typed,     │   │ counts by day  │  │
                 │                            │ metadata   │   │ deduped    │   │ BUY crossovers │  │
                 │                            └────────────┘   └────────────┘   └────────────────┘  │
                 └──────────────────────────────────────────────────────────────────────────────────┘

 Production shape: producer --mode kafka ──► Kafka topic `ema.signals`
                   ──► same pipeline with SOURCE_MODE=kafka ──► ADLS Gen2 (Delta) on Databricks
```

### The three layers

| Layer | Table | What happens |
|---|---|---|
| **Bronze** | `bronze_signals` | Records stored exactly as received, plus `ingestion_ts` and `source_file`. Append-only — the raw history is never thrown away. |
| **Silver** | `silver_signals` | Typed dates/prices, invalid signals filtered out, duplicates removed (one signal per ticker per day, using a watermark on event time), derived columns added: `% vs EMA20` and trend position (`ABOVE_BOTH` / `BELOW_BOTH` / `BETWEEN`). |
| **Gold** | `gold_latest_signal_per_ticker` | One row per ticker: its most recent signal and prices. |
| | `gold_signal_counts_by_day` | Signal mix per day — e.g. 4 BUY / 6 WATCH / 10 AVOID. |
| | `gold_new_buy_crossovers` | Tickers whose signal flipped to BUY from something else — the actionable events. |

## Run it locally (no Kafka, no cloud account)

**Prerequisites:** Python 3.10+ and Java 17 on your PATH (Spark needs a JVM).

```bash
git clone <this repo>
cd ema-streaming-pipeline

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

./run_pipeline.sh
```

That script runs the whole flow:

1. `src/producer.py` replays the bundled sample signals into
   `data/stream_landing/` as one JSON file per trading day (the simulated
   stream).
2. `src/pipeline.py` streams them through bronze → silver and builds gold.
3. `src/show_outputs.py` reads every Delta table back and prints row
   counts and sample rows.

Delta tables land in `output/delta/`; streaming checkpoints in
`output/checkpoints/`. Both are regenerated on every run and are
git-ignored.

> **Offline / proxied environments:** if your JVM can't reach Maven
> Central, download `delta-spark`, `delta-storage`, and `antlr4-runtime`
> jars yourself and point `DELTA_JARS` at them (comma-separated paths) —
> `src/common.py` then loads Delta from the local jars instead.

**Regenerate the sample data** (deterministic, fixed seed):

```bash
python src/generate_sample_data.py   # rewrites data/sample_signals.csv
```

## Switching to a real Kafka topic

Nothing downstream changes — only the source:

```bash
# publish the same records to Kafka
KAFKA_BOOTSTRAP=localhost:9092 python src/producer.py --mode kafka

# consume from Kafka instead of the landing directory
SOURCE_MODE=kafka KAFKA_BOOTSTRAP=localhost:9092 python src/pipeline.py
```

The Kafka consumer uses the identical record schema and the identical
bronze/silver/gold code. That swap-ability is the point of the design:
stream sources change; the medallion logic doesn't.

## How this maps to production (Azure Databricks)

This is a local, clone-and-run version of a pipeline shape I work with on
Azure Databricks:

| This repo (local) | Production (Databricks / Azure) |
|---|---|
| File-stream landing directory | Kafka topic (or Event Hubs' Kafka endpoint) |
| Local Delta tables on disk | Delta tables on ADLS Gen2 via Unity Catalog |
| `availableNow` trigger (drain & stop) | Continuous or scheduled streaming triggers with checkpoints in ADLS |
| Sample screener data | Live screener / market-data feed events |
| `requirements.txt` | Cluster libraries / Databricks Asset Bundles deployment |

## Project layout

```
├── README.md
├── requirements.txt
├── run_pipeline.sh            # one-command end-to-end run
├── data/
│   └── sample_signals.csv     # bundled SAMPLE data (120 records, 20 tickers, 6 days)
└── src/
    ├── generate_sample_data.py  # deterministic sample-data generator
    ├── producer.py              # file-stream simulation + Kafka producer mode
    ├── common.py                # Spark + Delta session setup
    ├── pipeline.py              # bronze -> silver streaming, gold aggregates
    └── show_outputs.py          # reads the Delta tables back
```

## Tech stack

Python · Apache Spark Structured Streaming (PySpark) · Delta Lake ·
Apache Kafka (optional, production mode) · Designed for Azure Databricks /
ADLS Gen2 in production.

---

*Signals are informational only and are not financial advice.*
