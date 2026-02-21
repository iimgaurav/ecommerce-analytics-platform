"""
Silver Layer – Exchange Rates

Reads the Bronze Parquet, applies data-quality transformations,
and computes cross-rates from EUR to USD.

Input :  data/bronze/exchange_rates/  (Parquet)
Output:  data/silver/exchange_rates/  (Parquet, partitioned by date)
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    current_timestamp,
)

logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("exchange_rates_silver")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def _add_usd_cross_rate(df: DataFrame) -> DataFrame:
    """
    The free-tier API always returns EUR-based rates.
    Compute a USD-equivalent rate for every currency using:

        rate_in_usd = rate_eur / eur_to_usd

    where eur_to_usd is the EUR→USD rate from the same response.
    """
    # Get EUR→USD rate per date (should be one per date)
    eur_usd = (
        df.filter(col("currency_code") == "USD")
        .select(col("date"), col("rate").alias("eur_to_usd"))
    )

    df_with_usd = (
        df.join(eur_usd, on="date", how="left")
        .withColumn(
            "rate_usd",
            when(
                col("eur_to_usd").isNotNull() & (col("eur_to_usd") > 0),
                col("rate") / col("eur_to_usd"),
            ),
        )
        .drop("eur_to_usd")
    )
    return df_with_usd


def _validate_rates(df: DataFrame) -> DataFrame:
    """
    Flag rows with suspicious rate values (negative or zero).
    """
    return df.withColumn(
        "is_valid",
        when(col("rate").isNotNull() & (col("rate") > 0), lit(True))
        .otherwise(lit(False)),
    )


def run(
    bronze_dir: str | Path | None = None,
    silver_dir: str | Path | None = None,
    spark: SparkSession | None = None,
) -> None:
    """Read Bronze, transform, and write Silver."""
    from ingestion.config import BRONZE_DIR, SILVER_DIR, ensure_data_dirs

    ensure_data_dirs()
    bronze_path = str(bronze_dir or BRONZE_DIR)
    silver_path = str(silver_dir or SILVER_DIR)
    spark = spark or get_spark()

    logger.info("Silver: reading bronze data from %s", bronze_path)

    df = spark.read.parquet(bronze_path)

    if df.rdd.isEmpty():
        logger.warning("Bronze data is empty – nothing to do.")
        return

    # 1. Deduplicate (same currency + same date)
    df = df.dropDuplicates(["date", "currency_code"])

    # 2. Drop rows with null rates
    df = df.filter(col("rate").isNotNull())

    # 3. Add USD cross-rate
    df = _add_usd_cross_rate(df)

    # 4. Validate rates
    df = _validate_rates(df)

    # 5. Add processing timestamp
    df = df.withColumn("silver_processed_ts", current_timestamp())

    row_count = df.count()
    logger.info("Silver: writing %d rows to %s", row_count, silver_path)

    (
        df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(silver_path)
    )

    logger.info("Silver layer complete ✓")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
