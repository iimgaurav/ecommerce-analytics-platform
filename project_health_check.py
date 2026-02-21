"""
project_health_check.py
========================
Automated health checker for the DE Medallion Architecture project.
Checks every task across all 8 days completed so far.

Usage:
    python project_health_check.py
    python project_health_check.py --week 1
    python project_health_check.py --fix   (shows fix commands for failures)

Requirements:
    pip install requests boto3 rich
"""

import os
import sys
import argparse
import subprocess
import json
from datetime import datetime
from pathlib import Path

# ── Try importing rich for beautiful output ────────────────────────
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.progress import track
    from rich import box
    RICH = True
    console = Console()
except ImportError:
    RICH = False
    print("💡 Tip: pip install rich  for a prettier output")


# ══════════════════════════════════════════════════════════════════
#  HELPER UTILITIES
# ══════════════════════════════════════════════════════════════════

def p(msg: str, style: str = ""):
    """Print with optional rich styling."""
    if RICH:
        console.print(msg, style=style)
    else:
        # strip rich markup for plain output
        import re
        clean = re.sub(r'\[.*?\]', '', msg)
        print(clean)


def check(name: str, fn) -> dict:
    """
    Run a single check function and return result dict.
    fn must return (bool, str) — (passed, detail_message)
    """
    try:
        passed, detail = fn()
        return {
            "name": name,
            "passed": passed,
            "detail": detail,
            "error": None,
        }
    except Exception as e:
        return {
            "name": name,
            "passed": False,
            "detail": "",
            "error": str(e),
        }


# ══════════════════════════════════════════════════════════════════
#  DAY 1 CHECKS — Git + Project Scaffold
# ══════════════════════════════════════════════════════════════════

def check_git_repo():
    result = subprocess.run(
        ["git", "rev-parse", "--git-dir"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        # get remote URL
        remote = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            capture_output=True, text=True
        )
        url = remote.stdout.strip() if remote.returncode == 0 else "no remote"
        return True, f"Git repo found → {url}"
    return False, "Not a git repository"


def check_folder_structure():
    required = [
        "airflow/dags",
        "airflow/plugins",
        "ingestion",
        "spark_jobs/bronze",
        "spark_jobs/silver",
        "spark_jobs/gold",
        "dbt/models/staging",
        "dbt/models/intermediate",
        "dbt/models/marts",
        "great_expectations",
        "docker",
        "notebooks",
        "tests/unit",
        "tests/integration",
        "docs",
    ]
    missing = [f for f in required if not Path(f).exists()]
    if not missing:
        return True, f"All {len(required)} folders present"
    return False, f"Missing: {', '.join(missing)}"


def check_core_files():
    required = [
        "requirements.txt",
        ".env.example",
        ".gitignore",
        "Makefile",
        "README.md",
        ".pre-commit-config.yaml",
    ]
    missing = [f for f in required if not Path(f).exists()]
    if not missing:
        return True, f"All {len(required)} core files present"
    return False, f"Missing: {', '.join(missing)}"


def check_env_not_committed():
    result = subprocess.run(
        ["git", "ls-files", ".env"],
        capture_output=True, text=True
    )
    if result.stdout.strip() == "":
        return True, ".env is NOT tracked by git ✅ (secure)"
    return False, "⚠️  .env IS committed to git — remove it immediately!"


def check_gitignore_has_env():
    if not Path(".gitignore").exists():
        return False, ".gitignore not found"
    content = Path(".gitignore").read_text()
    if ".env" in content:
        return True, ".env is in .gitignore"
    return False, ".env missing from .gitignore"


def check_first_commit():
    result = subprocess.run(
        ["git", "log", "--oneline"],
        capture_output=True, text=True
    )
    count = len(result.stdout.strip().splitlines())
    if count > 0:
        last = result.stdout.strip().splitlines()[0]
        return True, f"{count} commits found → latest: {last}"
    return False, "No commits found"


# ══════════════════════════════════════════════════════════════════
#  DAY 2 CHECKS — Docker Stack
# ══════════════════════════════════════════════════════════════════

def check_docker_compose_file():
    path = Path("docker/docker-compose.yml")
    if not path.exists():
        return False, "docker/docker-compose.yml not found"
    content = path.read_text()
    services = ["airflow", "minio", "postgres"]
    missing = [s for s in services if s not in content]
    if not missing:
        return True, "docker-compose.yml has airflow + minio + postgres"
    return False, f"Missing services in compose: {missing}"


def check_docker_running():
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return False, "Docker not running or not installed"
    containers = result.stdout.strip().splitlines()
    expected = ["airflow", "minio", "postgres"]
    found = [e for e in expected if any(e in c for c in containers)]
    if len(found) == len(expected):
        return True, f"All containers running: {', '.join(containers)}"
    missing = [e for e in expected if e not in found]
    return False, f"Containers not running: {missing}"


def check_minio_reachable():
    try:
        import requests  # noqa: E402
        r = requests.get("http://localhost:9001", timeout=5)
        if r.status_code in [200, 302, 403]:
            return True, "MinIO UI reachable at localhost:9001"
        return False, f"MinIO returned status {r.status_code}"
    except Exception as e:
        return False, f"MinIO not reachable: {e}"


def check_airflow_reachable():
    try:
        import requests  # noqa: E402
        r = requests.get("http://localhost:8080/health", timeout=10)
        if r.status_code == 200:
            data = r.json()
            status = data.get("metadatabase", {}).get("status", "unknown")
            return True, f"Airflow healthy — DB status: {status}"
        return False, f"Airflow returned status {r.status_code}"
    except Exception as e:
        return False, f"Airflow not reachable: {e}"


def check_minio_buckets():
    try:
        import boto3  # noqa: E402
        from botocore.client import Config  # noqa: E402
        client = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        response = client.list_buckets()
        bucket_names = [b["Name"] for b in response.get("Buckets", [])]
        required = ["bronze", "silver", "gold"]
        missing = [b for b in required if b not in bucket_names]
        if not missing:
            return True, f"All 3 buckets exist: {', '.join(bucket_names)}"
        return False, f"Missing buckets: {missing} | Found: {bucket_names}"
    except Exception as e:
        return False, f"Cannot connect to MinIO: {e}"


# ══════════════════════════════════════════════════════════════════
#  DAY 3 CHECKS — PySpark + Delta Lake
# ══════════════════════════════════════════════════════════════════

def check_pyspark_installed():
    try:
        import pyspark  # noqa: E402
        return True, f"PySpark {pyspark.__version__} installed"
    except ImportError:
        return False, "PySpark not installed — pip install pyspark==3.5.0"


def check_delta_installed():
    try:
        import delta  # noqa: E402
        return True, f"delta-spark {delta.__version__} installed"
    except ImportError:
        return False, "delta-spark not installed — pip install delta-spark==3.0.0"


def check_spark_utils_exists():
    path = Path("spark_jobs/spark_utils.py")
    if not path.exists():
        return False, "spark_jobs/spark_utils.py not found"
    content = path.read_text()
    if "get_spark_session" in content and "DeltaSparkSessionExtension" in content:
        return True, "spark_utils.py exists with Delta config"
    return False, "spark_utils.py missing get_spark_session or Delta config"


def check_smoke_test_exists():
    path = Path("notebooks/delta_smoke_test.py")
    if not path.exists():
        return False, "notebooks/delta_smoke_test.py not found"
    return True, "Delta smoke test script found"


def check_unit_tests_pass():
    result = subprocess.run(
        ["pytest", "tests/unit/", "-v", "--tb=short", "-q"],
        capture_output=True, text=True
    )
    lines = result.stdout.strip().splitlines()
    summary = lines[-1] if lines else "no output"
    if result.returncode == 0:
        return True, f"Unit tests passing → {summary}"
    return False, f"Unit tests failing → {summary}"


# ══════════════════════════════════════════════════════════════════
#  DAY 4 CHECKS — dbt + DuckDB
# ══════════════════════════════════════════════════════════════════

def check_dbt_installed():
    result = subprocess.run(
        ["dbt", "--version"],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        version_line = result.stdout.strip().splitlines()[0]
        return True, f"dbt installed → {version_line}"
    return False, "dbt not installed — pip install dbt-duckdb"


def check_dbt_project_file():
    path = Path("dbt/dbt_project.yml")
    if not path.exists():
        return False, "dbt/dbt_project.yml not found"
    content = path.read_text()
    if "ecommerce_dbt" in content:
        return True, "dbt_project.yml configured correctly"
    return False, "dbt_project.yml missing project name"


def check_dbt_models_exist():
    staging = list(Path("dbt/models/staging").glob("*.sql")) if Path("dbt/models/staging").exists() else []
    marts = list(Path("dbt/models/marts").glob("*.sql")) if Path("dbt/models/marts").exists() else []
    total = len(staging) + len(marts)
    if total >= 2:
        names = [f.name for f in staging + marts]
        return True, f"{total} SQL models found: {', '.join(names)}"
    return False, f"Need at least 2 SQL models, found {total}"


def check_dbt_seeds_exist():
    seeds = list(Path("dbt/seeds").glob("*.csv")) if Path("dbt/seeds").exists() else []
    if seeds:
        return True, f"Seeds found: {', '.join(f.name for f in seeds)}"
    return False, "No seed CSV files in dbt/seeds/"


def check_duckdb_warehouse():
    path = Path("data/warehouse.duckdb")
    if path.exists():
        size_kb = path.stat().st_size // 1024
        return True, f"DuckDB warehouse exists ({size_kb} KB)"
    return False, "data/warehouse.duckdb not found — run: cd dbt && dbt run"


def check_dbt_runs():
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        capture_output=True, text=True,
        cwd="dbt"
    )
    if result.returncode == 0:
        lines = result.stdout.strip().splitlines()
        summary = next((l for l in reversed(lines) if "Completed" in l or "OK" in l), "OK")
        return True, f"dbt run passes → {summary}"
    return False, f"dbt run failed → check dbt/logs/"


# ══════════════════════════════════════════════════════════════════
#  DAY 5 CHECKS — Superset
# ══════════════════════════════════════════════════════════════════

def check_superset_reachable():
    try:
        import requests  # noqa: E402
        r = requests.get("http://localhost:8088", timeout=10)
        if r.status_code in [200, 302]:
            return True, "Superset UI reachable at localhost:8088"
        return False, f"Superset returned status {r.status_code}"
    except Exception as e:
        return False, f"Superset not reachable: {e}"


def check_superset_in_compose():
    path = Path("docker/docker-compose.yml")
    if not path.exists():
        return False, "docker-compose.yml not found"
    content = path.read_text()
    if "superset" in content and "8088" in content:
        return True, "Superset defined in docker-compose.yml on port 8088"
    return False, "Superset missing from docker-compose.yml"


# ══════════════════════════════════════════════════════════════════
#  DAY 6 CHECKS — GitHub Actions CI
# ══════════════════════════════════════════════════════════════════

def check_ci_workflow_exists():
    path = Path(".github/workflows/ci.yml")
    if not path.exists():
        return False, ".github/workflows/ci.yml not found"
    content = path.read_text()
    jobs = []
    if "code-quality" in content or "flake8" in content:
        jobs.append("lint")
    if "pytest" in content or "unit-tests" in content:
        jobs.append("tests")
    if "dbt" in content:
        jobs.append("dbt")
    if jobs:
        return True, f"CI workflow found with jobs: {', '.join(jobs)}"
    return False, "CI workflow exists but has no jobs"


def check_ci_badge_in_readme():
    path = Path("README.md")
    if not path.exists():
        return False, "README.md not found"
    content = path.read_text()
    if "actions/workflows" in content and "badge.svg" in content:
        return True, "CI badge found in README.md"
    return False, "CI badge missing from README.md — add it for client credibility"


def check_pytest_ini():
    if Path("pytest.ini").exists():
        return True, "pytest.ini found"
    if Path("pyproject.toml").exists():
        content = Path("pyproject.toml").read_text()
        if "pytest" in content:
            return True, "pytest config found in pyproject.toml"
    return False, "No pytest.ini or pytest config in pyproject.toml"


# ══════════════════════════════════════════════════════════════════
#  DAY 7 CHECKS — Week 1 Polish
# ══════════════════════════════════════════════════════════════════

def check_week1_summary_doc():
    path = Path("docs/week1_summary.md")
    if path.exists():
        return True, "docs/week1_summary.md exists"
    return False, "docs/week1_summary.md missing — document your Week 1 work"


def check_v01_tag():
    result = subprocess.run(
        ["git", "tag"],
        capture_output=True, text=True
    )
    tags = result.stdout.strip().splitlines()
    if "v0.1.0" in tags:
        return True, f"v0.1.0 release tag exists"
    return False, f"v0.1.0 tag missing — run: git tag -a v0.1.0 -m 'Week 1 complete'"


# ══════════════════════════════════════════════════════════════════
#  DAY 8 CHECKS — Weather API Ingestion
# ══════════════════════════════════════════════════════════════════

def check_ingestion_files():
    required = [
        "ingestion/__init__.py",
        "ingestion/minio_client.py",
        "ingestion/weather_api.py",
    ]
    missing = [f for f in required if not Path(f).exists()]
    if not missing:
        return True, f"All ingestion files present"
    return False, f"Missing: {', '.join(missing)}"


def check_weather_data_in_minio():
    try:
        import boto3  # noqa: E402
        from botocore.client import Config  # noqa: E402
        client = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        response = client.list_objects_v2(Bucket="bronze", Prefix="weather/")
        objects = response.get("Contents", [])
        if objects:
            total_size = sum(o["Size"] for o in objects)
            return True, (
                f"Weather data in MinIO bronze! "
                f"{len(objects)} file(s), {total_size // 1024} KB total"
            )
        return False, "No weather data in bronze bucket yet — run: python -m ingestion.weather_api"
    except Exception as e:
        return False, f"Cannot check MinIO: {e}"


def check_weather_test_exists():
    path = Path("tests/unit/test_weather_api.py")
    if path.exists():
        return True, "tests/unit/test_weather_api.py exists"
    return False, "Weather API unit test missing"


def check_setup_minio_script():
    path = Path("ingestion/setup_minio.py")
    if path.exists():
        return True, "ingestion/setup_minio.py exists"
    return False, "setup_minio.py missing — create it to auto-create buckets"


# ══════════════════════════════════════════════════════════════════
#  FIX COMMANDS
# ══════════════════════════════════════════════════════════════════

FIX_COMMANDS = {
    "Git repo initialized": "git init && git remote add origin <your-repo-url>",
    "Folder structure complete": "mkdir -p airflow/dags airflow/plugins ingestion spark_jobs/bronze spark_jobs/silver spark_jobs/gold dbt/models/staging dbt/models/intermediate dbt/models/marts great_expectations docker notebooks tests/unit tests/integration docs",
    "Core files exist": "touch requirements.txt .env.example .gitignore Makefile README.md .pre-commit-config.yaml",
    ".env not committed to git": "git rm --cached .env && echo '.env' >> .gitignore",
    ".env in .gitignore": "echo '.env' >> .gitignore",
    "At least 1 commit pushed": "git add . && git commit -m 'feat: initial scaffold' && git push origin main",
    "docker-compose.yml exists": "Create docker/docker-compose.yml with airflow + minio + postgres services",
    "Docker containers running": "make up  OR  docker compose -f docker/docker-compose.yml up -d",
    "MinIO reachable": "docker compose -f docker/docker-compose.yml up -d minio",
    "Airflow reachable": "docker compose -f docker/docker-compose.yml up -d airflow-webserver",
    "MinIO buckets exist": "python -m ingestion.setup_minio",
    "PySpark installed": "pip install pyspark==3.5.0",
    "delta-spark installed": "pip install delta-spark==3.0.0",
    "spark_utils.py exists": "Create spark_jobs/spark_utils.py with get_spark_session()",
    "Delta smoke test exists": "Create notebooks/delta_smoke_test.py",
    "Unit tests passing": "pytest tests/unit/ -v",
    "dbt installed": "pip install dbt-duckdb==1.7.0 duckdb==0.10.0",
    "dbt_project.yml exists": "cd dbt && dbt init ecommerce_dbt",
    "dbt SQL models exist": "Create dbt/models/staging/stg_orders.sql and dbt/models/marts/mart_weather_sales.sql",
    "dbt seeds exist": "Create dbt/seeds/raw_orders.csv and dbt/seeds/raw_weather.csv",
    "DuckDB warehouse exists": "cd dbt && dbt seed && dbt run",
    "dbt run passes": "cd dbt && dbt run",
    "Superset reachable": "docker compose -f docker/docker-compose.yml up -d superset",
    "Superset in docker-compose": "Add superset service to docker/docker-compose.yml",
    "CI workflow exists": "Create .github/workflows/ci.yml",
    "CI badge in README": "Add badge URL to README.md top",
    "pytest.ini exists": "Create pytest.ini in project root",
    "Week 1 summary doc": "Create docs/week1_summary.md",
    "v0.1.0 release tag": "git tag -a v0.1.0 -m 'Week 1 complete' && git push origin v0.1.0",
    "Ingestion files exist": "Create ingestion/__init__.py, ingestion/minio_client.py, ingestion/weather_api.py",
    "Weather data in MinIO bronze": "python -m ingestion.weather_api",
    "Weather API unit test exists": "Create tests/unit/test_weather_api.py",
    "setup_minio.py exists": "Create ingestion/setup_minio.py",
}


# ══════════════════════════════════════════════════════════════════
#  ALL CHECKS REGISTRY
# ══════════════════════════════════════════════════════════════════

ALL_CHECKS = {
    1: {
        "title": "Day 1 — Git + Project Scaffold",
        "checks": [
            ("Git repo initialized",        check_git_repo),
            ("Folder structure complete",   check_folder_structure),
            ("Core files exist",            check_core_files),
            (".env not committed to git",   check_env_not_committed),
            (".env in .gitignore",          check_gitignore_has_env),
            ("At least 1 commit pushed",    check_first_commit),
        ],
    },
    2: {
        "title": "Day 2 — Docker Stack",
        "checks": [
            ("docker-compose.yml exists",   check_docker_compose_file),
            ("Docker containers running",   check_docker_running),
            ("MinIO reachable",             check_minio_reachable),
            ("Airflow reachable",           check_airflow_reachable),
            ("MinIO buckets exist",         check_minio_buckets),
        ],
    },
    3: {
        "title": "Day 3 — PySpark + Delta Lake",
        "checks": [
            ("PySpark installed",           check_pyspark_installed),
            ("delta-spark installed",       check_delta_installed),
            ("spark_utils.py exists",       check_spark_utils_exists),
            ("Delta smoke test exists",     check_smoke_test_exists),
            ("Unit tests passing",          check_unit_tests_pass),
        ],
    },
    4: {
        "title": "Day 4 — dbt + DuckDB",
        "checks": [
            ("dbt installed",               check_dbt_installed),
            ("dbt_project.yml exists",      check_dbt_project_file),
            ("dbt SQL models exist",        check_dbt_models_exist),
            ("dbt seeds exist",             check_dbt_seeds_exist),
            ("DuckDB warehouse exists",     check_duckdb_warehouse),
            ("dbt run passes",              check_dbt_runs),
        ],
    },
    5: {
        "title": "Day 5 — Apache Superset",
        "checks": [
            ("Superset in docker-compose",  check_superset_in_compose),
            ("Superset reachable",          check_superset_reachable),
        ],
    },
    6: {
        "title": "Day 6 — GitHub Actions CI",
        "checks": [
            ("CI workflow exists",          check_ci_workflow_exists),
            ("CI badge in README",          check_ci_badge_in_readme),
            ("pytest.ini exists",           check_pytest_ini),
        ],
    },
    7: {
        "title": "Day 7 — Week 1 Polish",
        "checks": [
            ("Week 1 summary doc",          check_week1_summary_doc),
            ("v0.1.0 release tag",          check_v01_tag),
        ],
    },
    8: {
        "title": "Day 8 — Weather API Ingestion",
        "checks": [
            ("Ingestion files exist",       check_ingestion_files),
            ("setup_minio.py exists",       check_setup_minio_script),
            ("Weather data in MinIO bronze", check_weather_data_in_minio),
            ("Weather API unit test exists", check_weather_test_exists),
        ],
    },
}


# ══════════════════════════════════════════════════════════════════
#  RUNNER
# ══════════════════════════════════════════════════════════════════

def run_checks(week_filter: int = None, show_fixes: bool = False):
    total_passed = 0
    total_failed = 0
    failures = []

    if RICH:
        console.print(Panel.fit(
            "[bold cyan]🏗️  DE Project Health Check[/bold cyan]\n"
            "[dim]Medallion Architecture · Open Source Stack[/dim]",
            border_style="cyan"
        ))
    else:
        print("\n" + "=" * 60)
        print("  🏗️  DE Project Health Check")
        print("  Medallion Architecture · Open Source Stack")
        print("=" * 60)

    days = ALL_CHECKS
    if week_filter:
        # week 1 = days 1-7, week 2 = days 8-14
        start = (week_filter - 1) * 7 + 1
        end = week_filter * 7
        days = {k: v for k, v in ALL_CHECKS.items() if start <= k <= end}

    for day_num, day_info in days.items():
        day_passed = 0
        day_failed = 0
        day_results = []

        for check_name, check_fn in day_info["checks"]:
            result = check(check_name, check_fn)
            day_results.append(result)
            if result["passed"]:
                day_passed += 1
                total_passed += 1
            else:
                day_failed += 1
                total_failed += 1
                failures.append((check_name, result))

        # Print day section
        day_pct = int(day_passed / len(day_results) * 100)
        status_icon = "✅" if day_failed == 0 else "⚠️ " if day_passed > 0 else "❌"

        if RICH:
            table = Table(
                box=box.SIMPLE,
                show_header=True,
                header_style="bold dim",
                expand=True,
            )
            table.add_column("Status", width=6, justify="center")
            table.add_column("Check", style="bold")
            table.add_column("Detail", style="dim")

            for r in day_results:
                if r["passed"]:
                    icon = "[green]  ✅[/green]"
                    detail = f"[green]{r['detail']}[/green]"
                else:
                    icon = "[red]  ❌[/red]"
                    msg = r["error"] if r["error"] else r["detail"]
                    detail = f"[red]{msg}[/red]"
                table.add_row(icon, r["name"], detail)

            console.print(f"\n[bold]{status_icon} {day_info['title']}[/bold] "
                          f"[dim]({day_passed}/{len(day_results)} — {day_pct}%)[/dim]")
            console.print(table)
        else:
            print(f"\n{status_icon} {day_info['title']} ({day_passed}/{len(day_results)} — {day_pct}%)")
            print("-" * 55)
            for r in day_results:
                icon = "  ✅" if r["passed"] else "  ❌"
                msg = r["detail"] if r["passed"] else (r["error"] or r["detail"])
                print(f"{icon}  {r['name']}")
                if msg:
                    print(f"      {msg}")

    # ── Summary ───────────────────────────────────────────────────
    total = total_passed + total_failed
    overall_pct = int(total_passed / total * 100) if total > 0 else 0

    if RICH:
        color = "green" if total_failed == 0 else "yellow" if total_passed > total_failed else "red"
        console.print(f"\n[bold]{'='*55}[/bold]")
        console.print(
            f"[bold {color}]  Overall: {total_passed}/{total} checks passing ({overall_pct}%)[/bold {color}]"
        )
        if total_failed == 0:
            console.print("[bold green]  🏆 ALL CHECKS PASSING — Project is healthy![/bold green]")
        else:
            console.print(f"[bold red]  ⚠️  {total_failed} check(s) need attention[/bold red]")
        console.print(f"[bold]{'='*55}[/bold]\n")
    else:
        print("\n" + "=" * 55)
        print(f"  Overall: {total_passed}/{total} checks passing ({overall_pct}%)")
        if total_failed == 0:
            print("  🏆 ALL CHECKS PASSING!")
        else:
            print(f"  ⚠️  {total_failed} check(s) need attention")
        print("=" * 55)

    # ── Fix commands ──────────────────────────────────────────────
    if show_fixes and failures:
        print("\n🔧 FIX COMMANDS FOR FAILED CHECKS:")
        print("-" * 55)
        for name, result in failures:
            fix = FIX_COMMANDS.get(name, "See project guide")
            print(f"\n  ❌ {name}")
            print(f"     Fix: {fix}")

    return total_passed, total_failed


# ══════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="DE Project Health Checker — verify all tasks completed"
    )
    parser.add_argument(
        "--week",
        type=int,
        choices=[1, 2, 3, 4],
        help="Check only a specific week (1-4)",
        default=None,
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Show fix commands for all failed checks",
    )
    args = parser.parse_args()

    passed, failed = run_checks(week_filter=args.week, show_fixes=args.fix)
    sys.exit(0 if failed == 0 else 1)