"""
Central configuration loader for the ecommerce-analytics-platform.
Loads environment variables from .env and exposes typed constants.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# ── Exchange Rates API ──────────────────────────────────────────────
EXCHANGE_RATES_API_KEY = os.getenv("EXCHANGE_RATES_API_KEY", "")
EXCHANGE_RATES_BASE_URL = os.getenv(
    "EXCHANGE_RATES_BASE_URL",
    "https://api.exchangeratesapi.io/v1",
)

# ── Data directories ────────────────────────────────────────────────
DATA_DIR = PROJECT_ROOT / os.getenv("DATA_DIR", "data")
RAW_DIR = DATA_DIR / "raw" / "exchange_rates"
BRONZE_DIR = DATA_DIR / "bronze" / "exchange_rates"
SILVER_DIR = DATA_DIR / "silver" / "exchange_rates"
GOLD_DIR = DATA_DIR / "gold" / "exchange_rates"

# ── API defaults ────────────────────────────────────────────────────
DEFAULT_SYMBOLS = [
    "USD", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY", "INR", "SGD", "HKD",
]
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2  # seconds


def ensure_data_dirs():
    """Create all data directories if they don't exist."""
    for d in (RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        d.mkdir(parents=True, exist_ok=True)
