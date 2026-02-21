"""
spark_utils.py
Utilities for creating Spark sessions and common helper functions.
Configured for Delta Lake support.
"""

import logging

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "EcommerceAnalytics") -> SparkSession:
    """
    Creates or retrieves a SparkSession with Delta Lake support.

    Args:
        app_name: Name of the Spark application.

    Returns:
        SparkSession: A configured Spark session.
    """
    # Create the SparkSession with Delta configurations
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Optimization for local development
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
    )

    return builder.getOrCreate()
