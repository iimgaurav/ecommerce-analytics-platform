"""
Bronze Layer – Exchange Rates

Reads raw JSON files produced by the ingestion module and writes
them as structured Parquet with an explicit schema.

Input :  data/raw/exchange_rates/*.json
Output:  data/bronze/exchange_rates/  (Parquet)
"""

import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    from_unixtime,
    lit,
    current_timestamp,
    input_file_name,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    LongType,
    MapType,
)

logger = logging.getLogger(__name__)

# Schema that mirrors the exchangeratesapi.io response
RAW_SCHEMA = StructType([
    StructField("success", BooleanType(), True),
    StructField("timestamp", LongType(), True),
    StructField("base", StringType(), True),
    StructField("date", StringType(), True),
    StructField("rates", MapType(StringType(), DoubleType()), True),
])


def get_spark() -> SparkSession:
    """Create or return a local SparkSession."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("exchange_rates_bronze")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def run(
    raw_dir: str | Path | None = None,
    bronze_dir: str | Path | None = None,
    spark: SparkSession | None = None,
) -> None:
    """
    Read raw JSON files and write a clean Parquet table.

    Each JSON file is exploded so that every currency code becomes a
    separate row, making downstream queries far simpler.
    """
    from ingestion.config import RAW_DIR, BRONZE_DIR, ensure_data_dirs

    ensure_data_dirs()
    raw_path = str(raw_dir or RAW_DIR)
    bronze_path = str(bronze_dir or BRONZE_DIR)
    spark = spark or get_spark()

    logger.info("Bronze: reading raw JSON from %s", raw_path)

    df_raw = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("multiLine", True)
        .json(f"{raw_path}/*.json")
    )

    if df_raw.rdd.isEmpty():
        logger.warning("No raw JSON files found in %s – nothing to do.", raw_path)
        return

    # Explode the rates map into rows
    df_exploded = (
        df_raw
        .select(
            from_unixtime(col("timestamp")).alias("api_timestamp"),
            col("base"),
            col("date"),
            explode(col("rates")).alias("currency_code", "rate"),
            input_file_name().alias("source_file"),
        )
        .withColumn("ingestion_ts", current_timestamp())
    )

    logger.info("Bronze: writing %d rows to %s", df_exploded.count(), bronze_path)

    (
        df_exploded.write
        .mode("overwrite")
        .parquet(bronze_path)
    )

    logger.info("Bronze layer complete ✓")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
