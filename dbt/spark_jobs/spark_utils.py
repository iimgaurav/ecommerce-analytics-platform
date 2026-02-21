"""
spark_utils.py
Shared SparkSession builder for all PySpark jobs.
All jobs import get_spark_session() from here.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "DEProject") -> SparkSession:
    """
    Returns a configured SparkSession with Delta Lake support.

    Args:
        app_name: Name shown in Spark UI for this job.

    Returns:
        SparkSession with Delta extensions enabled.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .master("local[*]")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def stop_spark(spark: SparkSession) -> None:
    """Cleanly stop the SparkSession."""
    spark.stop()
    print("SparkSession stopped.")
