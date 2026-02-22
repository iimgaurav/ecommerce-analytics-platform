"""
ingest_orders_dag.py
====================
Daily synthetic orders generation DAG.
Schedule: 00:30 UTC — 30 min after weather DAG.
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "de_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="ingest_orders_daily",
    description="Daily synthetic orders generation",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="30 0 * * *",  # 00:30 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion", "orders"],
)


def task_setup_buckets(**context):
    import boto3
    from botocore.client import Config
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    for bucket in ["bronze", "silver", "gold"]:
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            client.create_bucket(Bucket=bucket)


def task_generate_orders(**context):
    execution_date = context["logical_date"]
    print(f"Generating orders for: {execution_date.date()}")

    project_root = "/opt/airflow"
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from ingestion.synthetic_orders import extract_orders
    orders = extract_orders(execution_date, count=200)

    total_rev = sum(o["total_amount"] for o in orders)
    print(f"Generated {len(orders)} orders — ${total_rev:,.2f} revenue")
    return {
        "order_count": len(orders),
        "total_revenue": round(total_rev, 2),
        "date": str(execution_date.date()),
    }


def task_validate_upload(**context):
    import boto3
    from botocore.client import Config

    execution_date = context["logical_date"]
    path = (
        f"orders/"
        f"year={execution_date.year}/"
        f"month={execution_date.month:02d}/"
        f"day={execution_date.day:02d}/"
        f"orders_raw.json"
    )

    client = boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'minio:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    try:
        obj = client.head_object(Bucket="bronze", Key=path)
        size_kb = obj["ContentLength"] // 1024
        print(f"Validated: s3://bronze/{path} ({size_kb} KB)")

        # pull order count from upstream XCom
        ti = context["ti"]
        upstream = ti.xcom_pull(task_ids="generate_orders")
        if upstream:
            print(f"Orders in file: {upstream.get('order_count')}")
            print(f"Revenue: ${upstream.get('total_revenue'):,.2f}")
    except Exception:
        raise FileNotFoundError(f"File not found: s3://bronze/{path}")


setup_buckets = PythonOperator(
    task_id="setup_buckets",
    python_callable=task_setup_buckets,
    dag=dag,
)

generate_orders = PythonOperator(
    task_id="generate_orders",
    python_callable=task_generate_orders,
    dag=dag,
)

validate = PythonOperator(
    task_id="validate_upload",
    python_callable=task_validate_upload,
    dag=dag,
)

setup_buckets >> generate_orders >> validate
```

---

### Step 3 — Trigger All 3 DAGs (15 min)
```
1. Open http://localhost:8080
2. You should see 3 DAGs:
   - ingest_weather_daily
   - ingest_exchange_rates_daily
   - ingest_orders_daily

3. Toggle ALL 3 ON

4. Trigger each manually:
   Click DAG → Trigger DAG ▶️

5. Watch all tasks go green
```

---

### Step 4 — Verify All 3 in MinIO (5 min)
```
http://localhost:9001 → bronze bucket

Expected folders:
📁 weather/year=.../month=.../day=.../weather_raw.json
📁 exchange_rates/year=.../month=.../day=.../exchange_rates_raw.json
📁 orders/year=.../month=.../day=.../orders_raw.json