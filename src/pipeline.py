"""EMA signal streaming pipeline — Kafka/file -> Spark -> Delta medallion.

Layers
------
bronze_signals   Raw signal records exactly as ingested, plus ingestion
                 metadata. Append-only Delta table.
silver_signals   Cleaned, typed, de-duplicated signals with derived
                 columns (distance from EMA20, trend position). Delta.
gold_*           Business-ready aggregates computed from silver:
                   - gold_latest_signal_per_ticker
                   - gold_signal_counts_by_day
                   - gold_new_buy_crossovers

Source switch (env SOURCE_MODE)
-------------------------------
file  (default) Spark reads the producer's landing directory as a file
      stream — the local simulation of a Kafka feed, no broker required.
kafka Spark reads a real Kafka topic with the identical schema and the
      identical downstream code:
          SOURCE_MODE=kafka KAFKA_BOOTSTRAP=host:9092 python src/pipeline.py

On Azure Databricks this same script runs against ADLS Gen2 paths with
Kafka (or Event Hubs' Kafka endpoint) as the source; locally it runs on
plain files + local Delta tables so anyone can clone and run it.

Usage:
  python src/producer.py            # land the sample stream first
  python src/pipeline.py            # bronze -> silver (streaming), gold (batch)
"""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lag, lit, row_number, when,
)
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

from common import build_spark

BASE = Path(__file__).resolve().parent.parent
LANDING_DIR = BASE / "data" / "stream_landing"
OUT = BASE / "output"
BRONZE = OUT / "delta" / "bronze_signals"
SILVER = OUT / "delta" / "silver_signals"
CHECKPOINTS = OUT / "checkpoints"
SOURCE_MODE = os.environ.get("SOURCE_MODE", "file")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ema.signals")

SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("universe", StringType()),
    StructField("trade_date", StringType()),
    StructField("event_time", TimestampType()),
    StructField("signal", StringType()),
    StructField("close", DoubleType()),
    StructField("ema20", DoubleType()),
    StructField("ema30", DoubleType()),
    StructField("chg_pct", DoubleType()),
    StructField("source", StringType()),
])


def read_source(spark: SparkSession) -> DataFrame:
    """Return the raw signal stream. File mode is the local simulation;
    kafka mode is the production source — everything downstream is shared."""
    if SOURCE_MODE == "kafka":
        from pyspark.sql.functions import from_json
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
            .select(from_json(col("value").cast("string"), SCHEMA).alias("r"))
            .select("r.*")
        )
    return (
        spark.readStream.schema(SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .json(str(LANDING_DIR))
    )


def write_bronze(raw: DataFrame) -> None:
    bronze = (
        raw.withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
    )
    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(CHECKPOINTS / "bronze"))
        .trigger(availableNow=True)  # drain the landed files, then stop (demo-friendly)
        .start(str(BRONZE))
    )
    query.awaitTermination()
    print(f"[bronze] stream drained -> {BRONZE}")


def write_silver(spark: SparkSession) -> None:
    bronze_stream = spark.readStream.format("delta").load(str(BRONZE))
    silver = (
        bronze_stream
        .filter(col("ticker").isNotNull() & col("signal").isin("BUY", "WATCH", "AVOID"))
        .withColumn("trade_date", col("trade_date").cast("date"))
        .withColumn("pct_vs_ema20", (col("close") - col("ema20")) / col("ema20") * 100)
        .withColumn(
            "trend_position",
            when((col("close") > col("ema20")) & (col("close") > col("ema30")), "ABOVE_BOTH")
            .when((col("close") < col("ema20")) & (col("close") < col("ema30")), "BELOW_BOTH")
            .otherwise("BETWEEN"),
        )
        .withWatermark("event_time", "2 days")
        .dropDuplicates(["ticker", "trade_date"])  # one signal per ticker per day
        .drop("source_file")
    )
    query = (
        silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", str(CHECKPOINTS / "silver"))
        .trigger(availableNow=True)
        .start(str(SILVER))
    )
    query.awaitTermination()
    print(f"[silver] stream drained -> {SILVER}")


def write_gold(spark: SparkSession) -> None:
    silver = spark.read.format("delta").load(str(SILVER))

    w_latest = Window.partitionBy("ticker").orderBy(col("trade_date").desc())
    latest = (
        silver.withColumn("rn", row_number().over(w_latest))
        .filter(col("rn") == 1)
        .drop("rn")
        .select("ticker", "universe", "trade_date", "signal", "close",
                "ema20", "ema30", "chg_pct", "trend_position")
    )
    latest.write.format("delta").mode("overwrite").save(
        str(OUT / "delta" / "gold_latest_signal_per_ticker"))
    print("[gold] latest signal per ticker written")

    counts = silver.groupBy("trade_date", "signal").count().orderBy("trade_date", "signal")
    counts.write.format("delta").mode("overwrite").save(
        str(OUT / "delta" / "gold_signal_counts_by_day"))
    print("[gold] signal counts by day written")

    w_seq = Window.partitionBy("ticker").orderBy("trade_date")
    crossovers = (
        silver.withColumn("prev_signal", lag("signal").over(w_seq))
        .filter((col("signal") == "BUY") & (col("prev_signal") != "BUY"))
        .select("ticker", "universe", "trade_date", "close", "ema20", "ema30",
                "prev_signal", col("signal").alias("new_signal"))
    )
    crossovers.write.format("delta").mode("overwrite").save(
        str(OUT / "delta" / "gold_new_buy_crossovers"))
    print("[gold] new BUY crossovers written")


def main() -> None:
    spark = build_spark()
    print(f"[pipeline] source mode = {SOURCE_MODE}")
    write_bronze(read_source(spark))
    write_silver(spark)
    write_gold(spark)
    spark.stop()
    print("[pipeline] done")


if __name__ == "__main__":
    main()
