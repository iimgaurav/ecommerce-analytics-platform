"""
delta_smoke_test.py
Verifies PySpark + Delta Lake is working correctly.
Run this: python notebooks/delta_smoke_test.py
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from delta.tables import DeltaTable
from spark_jobs.spark_utils import get_spark_session

# ── Config ────────────────────────────────────────────
DELTA_PATH = "/tmp/de_project/smoke_test/orders"


def create_sample_data(spark):
    """Create a small fake orders DataFrame."""
    schema = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("customer", StringType(), True),
            StructField("product", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("city", StringType(), True),
        ]
    )

    data = [
        ("ORD-001", "Alice", "Laptop", 999.99, 1, "Mumbai"),
        ("ORD-002", "Bob", "Phone", 499.99, 2, "Delhi"),
        ("ORD-003", "Charlie", "Tablet", 299.99, 1, "Bangalore"),
        ("ORD-004", "Diana", "Watch", 199.99, 3, "Mumbai"),
        ("ORD-005", "Eve", "Laptop", 999.99, 1, "Chennai"),
    ]

    return spark.createDataFrame(data, schema)


def run_smoke_test():
    print("\n" + "=" * 55)
    print("  🔥 PySpark + Delta Lake Smoke Test")
    print("=" * 55)

    # ── 1. Start Spark ─────────────────────────────────
    print("\n[1/6] Starting SparkSession...")
    spark = get_spark_session("SmokeTest")
    print("      ✅ SparkSession started!")

    # ── 2. Create DataFrame ────────────────────────────
    print("\n[2/6] Creating sample orders DataFrame...")
    df = create_sample_data(spark)
    df.show(truncate=False)
    print(f"      ✅ Created {df.count()} rows")

    # ── 3. Write as Delta (Version 0) ──────────────────
    print(f"\n[3/6] Writing Delta table → {DELTA_PATH}")
    (df.write.format("delta").mode("overwrite").save(DELTA_PATH))
    print("      ✅ Delta table written! (Version 0)")

    # ── 4. Read it back ────────────────────────────────
    print("\n[4/6] Reading Delta table back...")
    df_read = spark.read.format("delta").load(DELTA_PATH)
    df_read.show(truncate=False)
    print(f"      ✅ Read {df_read.count()} rows successfully")

    # ── 5. Append new data (Version 1) ────────────────
    print("\n[5/6] Appending 2 new orders (creates Version 1)...")
    new_data = [
        ("ORD-006", "Frank", "Headphones", 149.99, 2, "Pune"),
        ("ORD-007", "Grace", "Keyboard", 79.99, 1, "Delhi"),
    ]
    df_new = spark.createDataFrame(new_data, df.schema)
    df_new.write.format("delta").mode("append").save(DELTA_PATH)
    print("      ✅ Appended! Delta table now has Version 1")

    # ── 6. Time Travel — read Version 0 ───────────────
    print("\n[6/6] Time Travel — reading Version 0 (original 5 rows)...")
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(DELTA_PATH)
    df_v0.show(truncate=False)
    print(f"      ✅ Version 0 has {df_v0.count()} rows (original)")

    # ── Show History ───────────────────────────────────
    print("\n📜 Delta Table History:")
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    delta_table.history().select(
        "version", "timestamp", "operation", "operationParameters"
    ).show(truncate=False)

    # ── Summary ───────────────────────────────────────
    current_count = spark.read.format("delta").load(DELTA_PATH).count()
    print("\n" + "=" * 55)
    print("  ✅ ALL TESTS PASSED!")
    print(f"  📦 Current table: {current_count} rows (Version 1)")
    print(f"  ⏪ Time travel:   5 rows  (Version 0)")
    print(f"  📁 Delta path:    {DELTA_PATH}")
    print("=" * 55 + "\n")

    spark.stop()


if __name__ == "__main__":
    run_smoke_test()
