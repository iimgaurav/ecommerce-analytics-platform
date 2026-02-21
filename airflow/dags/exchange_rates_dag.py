"""
Airflow DAG – Exchange Rates Pipeline

Schedule: @daily
Flow:     ingest_rates → bronze_transform → silver_transform → gold_aggregate

Each task wraps the corresponding Python module so the entire pipeline
can also be run outside of Airflow for development.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Default args ────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ── Task callables ──────────────────────────────────────────────────


def _ingest(**context):
    """Fetch latest exchange rates and save raw JSON."""
    from ingestion.exchange_rates import ingest_latest

    path = ingest_latest()
    context["ti"].xcom_push(key="raw_path", value=str(path))


def _bronze(**context):
    """Transform raw JSON → Bronze Parquet."""
    from spark_jobs.bronze.exchange_rates_bronze import run

    run()


def _silver(**context):
    """Transform Bronze → Silver Parquet."""
    from spark_jobs.silver.exchange_rates_silver import run

    run()


def _gold(**context):
    """Aggregate Silver → Gold Parquet."""
    from spark_jobs.gold.exchange_rates_gold import run

    run()


# ── DAG definition ──────────────────────────────────────────────────

with DAG(
    dag_id="exchange_rates_pipeline",
    default_args=default_args,
    description="Daily exchange-rate ingestion and medallion transformation",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["exchange-rates", "medallion", "ingestion"],
) as dag:
    ingest_rates = PythonOperator(
        task_id="ingest_rates",
        python_callable=_ingest,
    )

    bronze_transform = PythonOperator(
        task_id="bronze_transform",
        python_callable=_bronze,
    )

    silver_transform = PythonOperator(
        task_id="silver_transform",
        python_callable=_silver,
    )

    gold_aggregate = PythonOperator(
        task_id="gold_aggregate",
        python_callable=_gold,
    )

    # Linear dependency chain
    ingest_rates >> bronze_transform >> silver_transform >> gold_aggregate
