"""Shared Spark session setup (Delta Lake enabled).

Default: delta-spark resolves the Delta jars from Maven Central via
`configure_spark_with_delta_pip` — the standard local setup.

Offline / proxied environments: if the DELTA_JARS environment variable
points at a comma-separated list of local Delta jars (delta-spark,
delta-storage, antlr4-runtime), those are used instead and no Maven
access is needed.

On Azure Databricks neither path is required — the cluster runtime ships
Spark + Delta built in; only the session builder differs.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(app_name: str = "ema-streaming-pipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
    )
    local_jars = os.environ.get("DELTA_JARS")
    if local_jars:
        classpath = local_jars.replace(",", ":")
        builder = (
            builder.config("spark.driver.extraClassPath", classpath)
            .config("spark.executor.extraClassPath", classpath)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    from delta import configure_spark_with_delta_pip
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
