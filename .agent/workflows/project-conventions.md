---
description: Project conventions and custom instructions for the ecommerce-analytics-platform
---

# Ecommerce Analytics Platform – Custom Instructions

## Project Overview
This is a **data engineering** project that ingests, transforms, and aggregates ecommerce-related data using a **medallion architecture** (Bronze → Silver → Gold). It is orchestrated by **Apache Airflow** and uses **PySpark** for large-scale transformations.

## Tech Stack
- **Language**: Python 3.13
- **Orchestration**: Apache Airflow (DAGs in `airflow/dags/`)
- **Processing**: PySpark (jobs in `spark_jobs/`)
- **Data Quality**: Great Expectations (`great_expectations/`)
- **Data Modeling**: dbt (`dbt/`)
- **Testing**: pytest + pytest-mock
- **Config**: python-dotenv (`.env` file)
- **Containerization**: Docker (`docker/`)

## Directory Structure
```
ecommerce-analytics-platform/
├── airflow/dags/         # Airflow DAG definitions
├── airflow/plugins/      # Custom Airflow operators/hooks
├── ingestion/            # Python modules to fetch data from APIs
│   ├── config.py         # Central config loader (reads .env)
│   └── exchange_rates.py # Exchange Rates API ingestion
├── spark_jobs/
│   ├── bronze/           # Raw → structured (Parquet)
│   ├── silver/           # Cleaned, enriched, validated
│   └── gold/             # Business aggregations
├── dbt/models/           # staging / intermediate / marts
├── great_expectations/   # Data quality expectations & checkpoints
├── tests/unit/           # Unit tests (mocked, no network)
├── tests/integration/    # Integration tests (may hit APIs)
├── notebooks/            # Exploratory Jupyter notebooks
├── docs/                 # Architecture & pipeline documentation
├── docker/               # Dockerfiles and compose configs
├── data/                 # Local data lake (raw/bronze/silver/gold)
├── .env                  # Secrets & config (never commit)
└── requirements.txt      # Python dependencies
```

## Coding Conventions

### General
- Python 3.13+ features are allowed (type unions `X | Y`, etc.)
- Use **type hints** on all public function signatures
- Use **docstrings** (Google style) on all public functions and classes
- Use **logging** (not print) for operational messages
- Keep modules focused: one pipeline/data source per file

### Ingestion Modules (`ingestion/`)
- Every new data source gets its own file in `ingestion/`
- Always use `ingestion/config.py` for API keys, base URLs, and paths
- Implement **retry logic** with exponential backoff for API calls
- Save raw API responses as JSON to `data/raw/<source>/`
- Never hardcode API keys – always read from `.env`

### Spark Jobs (`spark_jobs/`)
- **Bronze**: Read raw data, impose schema, write Parquet. Minimal transformation.
- **Silver**: Deduplicate, null-handling, enrichment (e.g., cross-rates), validation flags
- **Gold**: Business aggregations (summary stats, trends, KPIs)
- Always use `snappy` compression for Parquet
- Silver layer should be **partitioned by date**
- Use `SparkSession.builder.master("local[*]")` for local dev

### Airflow DAGs (`airflow/dags/`)
- DAG ID = `<source>_pipeline` (e.g., `exchange_rates_pipeline`)
- Default schedule: `@daily`
- Set `catchup=False` by default
- Use `PythonOperator` wrapping the module functions
- Tags: include the data source name and `medallion`

### Testing (`tests/`)
- **Unit tests** must mock all external APIs (no network calls)
- Use `pytest` fixtures for mock responses
- Group tests in classes by function being tested
- File naming: `test_<module_name>.py`

### Documentation (`docs/`)
- Every pipeline gets a markdown doc in `docs/`
- Include: overview, API limitations, setup steps, mermaid architecture diagram, manual run commands

## Environment & Secrets
- All secrets go in `.env` (gitignored)
- Use `python-dotenv` via `ingestion/config.py` to load them
- Never log or print API keys

## Data Sources
| Source | API | Base Currency | Module |
|--------|-----|--------------|--------|
| Exchange Rates | exchangeratesapi.io (free) | EUR | `ingestion/exchange_rates.py` |
