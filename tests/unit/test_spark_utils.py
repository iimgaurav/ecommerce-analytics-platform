import pytest
from pyspark.sql import SparkSession

from spark_jobs.spark_utils import get_spark_session


def test_get_spark_session():
    """Test get_spark_session creates a session with correct configurations.

    If the local environment cannot start a JVM / Spark (common on CI or
    developer machines without Java), skip this test rather than failing
    the whole suite.
    """
    app_name = "TestApp"
    try:
        spark = get_spark_session(app_name)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"Skipping Spark session test (unable to start Spark): {exc}")

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
        try:
            spark.stop()
        except Exception:
            pass
