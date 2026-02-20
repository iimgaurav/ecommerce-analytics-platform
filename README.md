# E-Commerce Analytics Platform
End-to-end Data Engineering project — Medallion Architecture with open source tools Visibility: Public  ← clients will see this 
✅ Add README 
✅ Add .gitignore → choose Python template

# 🏗️ E-Commerce Analytics Platform
> End-to-end Data Engineering pipeline built entirely on open source tools.
> Medallion Architecture (Bronze → Silver → Gold) running locally via Docker.

## 🛠️ Tech Stack
| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow |
| Storage | MinIO (S3-compatible) |
| Processing | PySpark + Delta Lake |
| Modeling | dbt Core |
| Warehouse | DuckDB |
| Quality | Great Expectations |
| Visualization | Apache Superset |



## 📐 Architecture
Raw APIs → [Bronze] → PySpark Clean → [Silver] → dbt Models → [Gold] → Superset Dashboards
