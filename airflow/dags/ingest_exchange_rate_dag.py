"""
ingest_exchange_rate_dag.py
===========================
Daily exchange rate ingestion DAG.
Schedule: 00:15 UTC — 15 min after weather DAG.
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
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    dag_id="ingest_exchange_rates_daily",
    description="Daily exchange rate ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="15 0 * * *",  # 00:15 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "ingestion", "exchange_rates"],
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
            print(f"Created bucket: {bucket}")


def task_extract_exchange_rates(**context):
    execution_date = context["logical_date"]
    print(f"Extracting exchange rates for: {execution_date.date()}")

    project_root = "/opt/airflow"
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from ingestion.exchange_rate_api import extract_exchange_rates
    data = extract_exchange_rates(execution_date)

    rates = data.get("rates", {})
    print(f"Rates fetched: {list(rates.keys())}")
    return {"currencies": list(rates.keys()), "date": str(execution_date.date())}


def task_validate_upload(**context):
    import boto3
    from botocore.client import Config

    execution_date = context["logical_date"]
    path = (
        f"exchange_rates/"
        f"year={execution_date.year}/"
        f"month={execution_date.month:02d}/"
        f"day={execution_date.day:02d}/"
        f"exchange_rates_raw.json"
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
        print(f"Validated: s3://bronze/{path} ({obj['ContentLength']} bytes)")
    except Exception:
        raise FileNotFoundError(f"File not found: s3://bronze/{path}")


setup_buckets = PythonOperator(
    task_id="setup_buckets",
    python_callable=task_setup_buckets,
    dag=dag,
)

extract_rates = PythonOperator(
    task_id="extract_exchange_rates",
    python_callable=task_extract_exchange_rates,
    dag=dag,
)

validate = PythonOperator(
    task_id="validate_upload",
    python_callable=task_validate_upload,
    dag=dag,
)

setup_buckets >> extract_rates >> validate