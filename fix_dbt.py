"""
fix_dbt_v2.py
=============
Fixed version — shows live pip output, no hanging.

Usage:
    python fix_dbt_v2.py
"""

import os
import sys
import subprocess
from pathlib import Path

# ══════════════════════════════════════════════════════════════════
#  HELPERS — use live output, no capture_output
# ══════════════════════════════════════════════════════════════════

def section(title, num):
    print(f"\n{'='*55}")
    print(f"  STEP {num}: {title}")
    print(f"{'='*55}")

def success(msg):
    print(f"  ✅ {msg}")

def fail(msg):
    print(f"  ❌ {msg}")

def info(msg):
    print(f"  → {msg}")

def run_live(cmd, cwd=None):
    """
    Run command with LIVE output shown in terminal.
    No capture — no hanging. Returns exit code only.
    """
    result = subprocess.run(cmd, cwd=cwd)
    return result.returncode

def run_silent(cmd, cwd=None):
    """
    Run command silently. Returns (code, stdout, stderr).
    Only used for quick commands like git tag, version check.
    """
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=cwd,
        timeout=30  # 30 second max for quick commands
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


# ══════════════════════════════════════════════════════════════════
#  STEP 1 — Uninstall all dbt packages
# ══════════════════════════════════════════════════════════════════

def step_uninstall():
    section("Uninstall all dbt packages", 1)

    packages = [
        "dbt-fusion",
        "dbt-duckdb",
        "dbt-core",
        "dbt-extractor",
        "dbt-semantic-interfaces",
        "dbt-adapters",
    ]

    for pkg in packages:
        info(f"Uninstalling {pkg}...")
        run_live([sys.executable, "-m", "pip", "uninstall", pkg, "-y"])

    success("Uninstall complete")


# ══════════════════════════════════════════════════════════════════
#  STEP 2 — Install stable dbt-duckdb (LIVE output)
# ══════════════════════════════════════════════════════════════════

def step_install():
    section("Install stable dbt-duckdb==1.7.0 + duckdb==0.10.0", 2)
    info("This may take 1-2 minutes — you will see live progress...")
    print()

    code = run_live([
        sys.executable, "-m", "pip", "install",
        "dbt-duckdb==1.7.0",
        "duckdb==0.10.0",
        "--force-reinstall",
    ])

    print()
    if code == 0:
        success("dbt-duckdb 1.7.0 installed!")
        return True
    else:
        fail(f"pip install failed with code {code}")
        return False


# ══════════════════════════════════════════════════════════════════
#  STEP 3 — Verify dbt version
# ══════════════════════════════════════════════════════════════════

def step_verify_version():
    section("Verify dbt version", 3)
    info("Checking dbt version...")

    code, out, err = run_silent(["dbt", "--version"])

    if code == 0:
        for line in out.splitlines():
            if line.strip():
                print(f"     {line}")
        if "fusion" in out.lower() or "preview" in out.lower():
            fail("Still showing fusion/preview version!")
            fail("Close this terminal, open a NEW terminal and re-run")
            return False
        success("Correct stable version confirmed")
        return True
    else:
        fail("dbt command not found after install")
        info("Try: close terminal, open new one, run again")
        return False


# ══════════════════════════════════════════════════════════════════
#  STEP 4 — Fix profiles.yml
# ══════════════════════════════════════════════════════════════════

def step_fix_profiles():
    section("Fix dbt/profiles.yml", 4)

    Path("data").mkdir(exist_ok=True)
    profiles_path = Path("dbt/profiles.yml")

    content = (
        "ecommerce_dbt:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        '      path: "../data/warehouse.duckdb"\n'
        "      threads: 4\n"
    )

    try:
        profiles_path.write_text(content, encoding="utf-8")
        success(f"profiles.yml fixed → {profiles_path}")
    except Exception as e:
        fail(f"Could not write profiles.yml: {e}")


# ══════════════════════════════════════════════════════════════════
#  STEP 5 — Ensure seed CSVs exist
# ══════════════════════════════════════════════════════════════════

def step_ensure_seeds():
    section("Ensure dbt seed files exist", 5)

    seeds_dir = Path("dbt/seeds")
    seeds_dir.mkdir(parents=True, exist_ok=True)

    orders = seeds_dir / "raw_orders.csv"
    weather = seeds_dir / "raw_weather.csv"

    if not orders.exists():
        orders.write_text(
            "order_id,customer_name,product,amount,currency,city,order_date\n"
            "ORD-001,Alice,Laptop,999.99,USD,Mumbai,2024-01-01\n"
            "ORD-002,Bob,Phone,499.99,USD,Delhi,2024-01-01\n"
            "ORD-003,Charlie,Tablet,299.99,USD,Bangalore,2024-01-02\n"
            "ORD-004,Diana,Watch,199.99,USD,Mumbai,2024-01-02\n"
            "ORD-005,Eve,Laptop,999.99,USD,Chennai,2024-01-03\n"
            "ORD-006,Frank,Headphones,149.99,USD,Pune,2024-01-03\n"
            "ORD-007,Grace,Keyboard,79.99,USD,Delhi,2024-01-04\n"
            "ORD-008,Henry,Monitor,349.99,USD,Bangalore,2024-01-04\n"
            "ORD-009,Iris,Laptop,999.99,USD,Mumbai,2024-01-05\n"
            "ORD-010,Jack,Phone,499.99,USD,Chennai,2024-01-05\n",
            encoding="utf-8"
        )
        success("raw_orders.csv created")
    else:
        success("raw_orders.csv already exists")

    if not weather.exists():
        weather.write_text(
            "city,date,temp_celsius,humidity,condition\n"
            "Mumbai,2024-01-01,28.5,80,Humid\n"
            "Delhi,2024-01-01,15.2,45,Clear\n"
            "Bangalore,2024-01-01,22.1,60,Cloudy\n"
            "Mumbai,2024-01-02,29.1,82,Humid\n"
            "Delhi,2024-01-02,14.8,42,Clear\n"
            "Bangalore,2024-01-02,21.8,58,Clear\n"
            "Mumbai,2024-01-03,27.9,78,Rainy\n"
            "Delhi,2024-01-03,16.1,50,Cloudy\n"
            "Bangalore,2024-01-03,23.2,65,Rainy\n"
            "Chennai,2024-01-01,32.1,88,Humid\n"
            "Chennai,2024-01-02,31.8,86,Humid\n"
            "Chennai,2024-01-03,33.2,90,Rainy\n"
            "Chennai,2024-01-04,32.5,87,Humid\n"
            "Chennai,2024-01-05,31.9,85,Humid\n"
            "Pune,2024-01-03,24.3,55,Clear\n",
            encoding="utf-8"
        )
        success("raw_weather.csv created")
    else:
        success("raw_weather.csv already exists")


# ══════════════════════════════════════════════════════════════════
#  STEP 6 — dbt debug (LIVE)
# ══════════════════════════════════════════════════════════════════

def step_dbt_debug():
    section("Run dbt debug", 6)
    info("Running dbt debug...")
    print()

    code = run_live(
        ["dbt", "debug", "--profiles-dir", "."],
        cwd="dbt"
    )

    print()
    if code == 0:
        success("dbt debug passed!")
        return True
    else:
        fail("dbt debug failed — check output above")
        return False


# ══════════════════════════════════════════════════════════════════
#  STEP 7 — dbt seed (LIVE)
# ══════════════════════════════════════════════════════════════════

def step_dbt_seed():
    section("Run dbt seed", 7)
    info("Loading seed CSV files into DuckDB...")
    print()

    code = run_live(
        ["dbt", "seed", "--profiles-dir", "."],
        cwd="dbt"
    )

    print()
    if code == 0:
        success("dbt seed passed!")
        return True
    else:
        fail("dbt seed failed — check output above")
        return False


# ══════════════════════════════════════════════════════════════════
#  STEP 8 — dbt run (LIVE)
# ══════════════════════════════════════════════════════════════════

def step_dbt_run():
    section("Run dbt run", 8)
    info("Building SQL models in DuckDB...")
    print()

    code = run_live(
        ["dbt", "run", "--profiles-dir", "."],
        cwd="dbt"
    )

    print()
    if code == 0:
        success("dbt run passed! All models built.")
        return True
    else:
        fail("dbt run failed — check output above")
        return False


# ══════════════════════════════════════════════════════════════════
#  STEP 9 — dbt test (LIVE)
# ══════════════════════════════════════════════════════════════════

def step_dbt_test():
    section("Run dbt test", 9)
    info("Running data quality tests...")
    print()

    code = run_live(
        ["dbt", "test", "--profiles-dir", "."],
        cwd="dbt"
    )

    print()
    if code == 0:
        success("dbt test passed!")
    else:
        fail("dbt test failed — check output above")


# ══════════════════════════════════════════════════════════════════
#  STEP 10 — Git commit + push
# ══════════════════════════════════════════════════════════════════

def step_commit():
    section("Commit + Push to GitHub", 10)

    run_live(["git", "add", "."])

    code, out, err = run_silent([
        "git", "commit", "-m",
        "fix: replace dbt-fusion with stable dbt-duckdb 1.7.0"
    ])

    if "nothing to commit" in out or "nothing to commit" in err:
        success("Nothing new to commit — already clean")
        return

    if code == 0:
        info("Pushing to GitHub...")
        push_code = run_live(["git", "push", "origin", "main"])
        if push_code == 0:
            success("Pushed to GitHub!")
        else:
            fail("Push failed — try: git push origin main")
    else:
        fail(f"Commit failed: {err[:60]}")


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "=" * 55)
    print("  🔧 dbt Fix Script v2 — Live Output")
    print("  Fixing dbt-fusion → stable dbt-duckdb 1.7.0")
    print("=" * 55)

    step_uninstall()

    installed = step_install()
    if not installed:
        print("\n❌ Install failed. Try manually:")
        print("   pip install dbt-duckdb==1.7.0 --force-reinstall")
        sys.exit(1)

    version_ok = step_verify_version()
    if not version_ok:
        print("\n⚠️  Close this terminal, open a NEW one and re-run.")
        sys.exit(1)

    step_fix_profiles()
    step_ensure_seeds()

    debug_ok = step_dbt_debug()
    if not debug_ok:
        print("\n❌ Fix dbt debug first, then re-run this script.")
        sys.exit(1)

    seed_ok = step_dbt_seed()
    if seed_ok:
        run_ok = step_dbt_run()
        if run_ok:
            step_dbt_test()

    step_commit()

    print("\n" + "=" * 55)
    print("  ✅ dbt fix complete!")
    print("  Run: python project_health_check.py")
    print("=" * 55 + "\n")