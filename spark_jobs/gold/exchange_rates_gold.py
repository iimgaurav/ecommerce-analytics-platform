"""
Gold Layer – Exchange Rates

Reads the Silver Parquet and produces business-level aggregations
ready for dashboards and analytics.

Input :  data/silver/exchange_rates/  (Parquet)
Output:  data/gold/exchange_rates/    (Parquet)

Tables produced
───────────────
  daily_summary   – min / max / avg rate per currency per date
  rate_changes    – day-over-day % change per currency
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    avg,
    min as spark_min,
    max as spark_max,
    count,
    lag,
    round as spark_round,
    when,
    current_timestamp,
)

logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("exchange_rates_gold")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def _build_daily_summary(df):
    """Aggregate min/max/avg rate per currency per date."""
    return (
        df.filter(col("is_valid") == True)  # noqa: E712
        .groupBy("date", "currency_code", "base")
        .agg(
            spark_min("rate").alias("min_rate"),
            spark_max("rate").alias("max_rate"),
            spark_round(avg("rate"), 6).alias("avg_rate"),
            spark_min("rate_usd").alias("min_rate_usd"),
            spark_max("rate_usd").alias("max_rate_usd"),
            spark_round(avg("rate_usd"), 6).alias("avg_rate_usd"),
            count("*").alias("record_count"),
        )
        .withColumn("gold_processed_ts", current_timestamp())
    )


def _build_rate_changes(df):
    """Compute day-over-day percentage change per currency."""
    window = Window.partitionBy("currency_code").orderBy("date")

    return (
        df.filter(col("is_valid") == True)  # noqa: E712
        .select("date", "currency_code", "rate", "rate_usd")
        .dropDuplicates(["date", "currency_code"])
        .withColumn("prev_rate", lag("rate").over(window))
        .withColumn("prev_rate_usd", lag("rate_usd").over(window))
        .withColumn(
            "pct_change",
            when(
                col("prev_rate").isNotNull() & (col("prev_rate") > 0),
                spark_round(
                    ((col("rate") - col("prev_rate")) / col("prev_rate")) * 100,
                    4,
                ),
            ),
        )
        .withColumn(
            "pct_change_usd",
            when(
                col("prev_rate_usd").isNotNull() & (col("prev_rate_usd") > 0),
                spark_round(
                    ((col("rate_usd") - col("prev_rate_usd"))
                     / col("prev_rate_usd")) * 100,
                    4,
                ),
            ),
        )
        .withColumn("gold_processed_ts", current_timestamp())
    )


def run(
    silver_dir: str | Path | None = None,
    gold_dir: str | Path | None = None,
    spark: SparkSession | None = None,
) -> None:
    """Read Silver and produce Gold aggregations."""
    from ingestion.config import SILVER_DIR, GOLD_DIR, ensure_data_dirs

    ensure_data_dirs()
    silver_path = str(silver_dir or SILVER_DIR)
    gold_path = str(gold_dir or GOLD_DIR)
    spark = spark or get_spark()

    logger.info("Gold: reading silver data from %s", silver_path)

    df = spark.read.parquet(silver_path)

    if df.rdd.isEmpty():
        logger.warning("Silver data is empty – nothing to do.")
        return

    # ── Daily summary ───────────────────────────────────────────
    summary_df = _build_daily_summary(df)
    summary_path = f"{gold_path}/daily_summary"
    summary_df.write.mode("overwrite").parquet(summary_path)
    logger.info(
        "Gold: wrote %d daily-summary rows → %s",
        summary_df.count(), summary_path,
    )

    # ── Rate changes ────────────────────────────────────────────
    changes_df = _build_rate_changes(df)
    changes_path = f"{gold_path}/rate_changes"
    changes_df.write.mode("overwrite").parquet(changes_path)
    logger.info(
        "Gold: wrote %d rate-change rows → %s",
        changes_df.count(), changes_path,
    )

    logger.info("Gold layer complete ✓")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
