import pytest
from pyspark.sql import SparkSession

from spark_jobs.spark_utils import get_spark_session


def test_get_spark_session():
    """Test get_spark_session creates a session with correct configurations."""
    app_name = "TestApp"
    spark = get_spark_session(app_name)

    try:
        assert isinstance(spark, SparkSession)
        assert spark.conf.get("spark.app.name") == app_name
        assert (
            spark.conf.get("spark.sql.extensions")
            == "io.delta.sql.DeltaSparkSessionExtension"
        )
        assert (
            spark.conf.get("spark.sql.catalog.spark_catalog")
            == "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        assert spark.conf.get("spark.sql.shuffle.partitions") == "2"
    finally:
        # We don't necessarily want to stop the session if it's shared,
        # but for local unit tests it's generally good practice if possible.
        # However, getOrCreate() might return an existing one.
        pass
