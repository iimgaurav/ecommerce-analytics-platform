"""
exchange_rate_api.py
Extracts daily currency exchange rates.
Uses exchangerate.host API — free, no API key needed.

Usage:
    python -m ingestion.exchange_rate_api
    python -m ingestion.exchange_rate_api --date 2024-01-15
"""

import argparse
import uuid
from datetime import datetime, timedelta

import requests

from ingestion.minio_client import upload_json, check_bucket_exists

# ── Constants ──────────────────────────────────────────────────
API_URL = "https://api.exchangerate.host/live"
BRONZE_BUCKET = "bronze"
SOURCE = "exchange_rates"
BASE_CURRENCY = "USD"
TARGET_CURRENCIES = ["EUR", "GBP", "INR", "JPY", "AUD", "CAD"]


def fetch_exchange_rates(date: datetime) -> dict:
    """
    Fetch exchange rates for given date.
    Falls back to latest if historical not available on free tier.
    """
    date_str = date.strftime("%Y-%m-%d")
    print(f"  💱 Fetching exchange rates for {date_str}...")

    # try historical endpoint first
    params = {
        "source": BASE_CURRENCY,
        "currencies": ",".join(TARGET_CURRENCIES),
        "date": date_str,
        "format": 1,
    }

    try:
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception:
        # fallback — use open.er-api.com (always free, no key)
        print("  ⚠️  Primary API failed, using fallback...")
        fallback_url = f"https://open.er-api.com/v6/latest/{BASE_CURRENCY}"
        response = requests.get(fallback_url, timeout=30)
        response.raise_for_status()
        data = response.json()

    # normalize response — different APIs have different shapes
    if "quotes" in data:
        rates = {
            k.replace(BASE_CURRENCY, ""): v
            for k, v in data["quotes"].items()
        }
    elif "rates" in data:
        rates = {
            k: v for k, v in data["rates"].items()
            if k in TARGET_CURRENCIES
        }
    else:
        raise ValueError(f"Unexpected API response shape: {list(data.keys())}")

    # add metadata — bronze layer best practice
    payload = {
        "fetch_date": date_str,
        "base_currency": BASE_CURRENCY,
        "rates": rates,
        "target_currencies": TARGET_CURRENCIES,
        "_metadata": {
            "ingested_at": datetime.utcnow().isoformat(),
            "batch_id": str(uuid.uuid4()),
            "source_system": "exchangerate_api",
            "source_url": API_URL,
        },
    }

    return payload


def extract_exchange_rates(execution_date: datetime) -> dict:
    """Extract and land exchange rates in MinIO bronze."""
    print(f"\n{'='*55}")
    print(f"  💱 Exchange Rate Extraction — "
          f"{execution_date.strftime('%Y-%m-%d')}")
    print(f"{'='*55}")

    if not check_bucket_exists(BRONZE_BUCKET):
        raise RuntimeError(f"Bucket '{BRONZE_BUCKET}' not found")

    data = fetch_exchange_rates(execution_date)

    s3_path = upload_json(
        data=data,
        bucket=BRONZE_BUCKET,
        source=SOURCE,
        execution_date=execution_date,
        filename="exchange_rates_raw.json",
    )

    print(f"\n✅ Exchange rates extracted!")
    print(f"   Rates fetched: {list(data['rates'].keys())}")
    print(f"   Landed at    : {s3_path}")
    print(f"{'='*55}\n")

    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    args = parser.parse_args()

    execution_date = (
        datetime.strptime(args.date, "%Y-%m-%d")
        if args.date
        else datetime.utcnow() - timedelta(days=1)
    )
    extract_exchange_rates(execution_date)


if __name__ == "__main__":
    main()