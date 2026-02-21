"""
Exchange Rates API ingestion module.

Fetches live and historical exchange rate data from exchangeratesapi.io
and persists the raw JSON responses for downstream processing.

Free-tier limitations:
  - Base currency is always EUR (cannot change)
  - HTTPS supported
  - 250 requests/month
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

from ingestion.config import (
    EXCHANGE_RATES_API_KEY,
    EXCHANGE_RATES_BASE_URL,
    RAW_DIR,
    DEFAULT_SYMBOLS,
    MAX_RETRIES,
    RETRY_BACKOFF_FACTOR,
    ensure_data_dirs,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)


# ── Internal helpers ────────────────────────────────────────────────


def _build_params(
    symbols: Optional[List[str]] = None,
    extra: Optional[Dict] = None,
) -> Dict:
    """Build query-string parameters common to every request."""
    params: Dict = {"access_key": EXCHANGE_RATES_API_KEY}
    if symbols:
        params["symbols"] = ",".join(symbols)
    if extra:
        params.update(extra)
    return params


def _request_with_retry(url: str, params: Dict) -> Dict:
    """
    Make a GET request with exponential-backoff retry logic.

    Returns the parsed JSON response dict.
    Raises requests.exceptions.RequestException on persistent failure.
    """
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                "API request (attempt %d/%d): %s", attempt, MAX_RETRIES, url
            )
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            if not data.get("success", True):
                error_info = data.get("error", {})
                raise ValueError(
                    f"API error {error_info.get('code')}: "
                    f"{error_info.get('info', 'Unknown error')}"
                )
            return data

        except (requests.exceptions.RequestException, ValueError) as exc:
            last_exception = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_FACTOR ** attempt
                logger.warning(
                    "Request failed (%s). Retrying in %ds ...", exc, wait
                )
                time.sleep(wait)
            else:
                logger.error("All %d attempts failed.", MAX_RETRIES)

    raise last_exception  # type: ignore[misc]


# ── Public API ──────────────────────────────────────────────────────


def fetch_latest_rates(
    symbols: Optional[List[str]] = None,
) -> Dict:
    """
    Fetch the latest exchange rates (EUR base).

    Args:
        symbols: List of currency codes to fetch. Defaults to
                 DEFAULT_SYMBOLS if None.

    Returns:
        Full API response as a dict.
    """
    symbols = symbols or DEFAULT_SYMBOLS
    url = f"{EXCHANGE_RATES_BASE_URL}/latest"
    params = _build_params(symbols=symbols)
    logger.info("Fetching latest rates for %s", symbols)
    return _request_with_retry(url, params)


def fetch_historical_rates(
    date: str,
    symbols: Optional[List[str]] = None,
) -> Dict:
    """
    Fetch historical exchange rates for a given date.

    Args:
        date: Date string in YYYY-MM-DD format.
        symbols: List of currency codes. Defaults to DEFAULT_SYMBOLS.

    Returns:
        Full API response as a dict.
    """
    symbols = symbols or DEFAULT_SYMBOLS
    url = f"{EXCHANGE_RATES_BASE_URL}/{date}"
    params = _build_params(symbols=symbols)
    logger.info("Fetching historical rates for %s", date)
    return _request_with_retry(url, params)


def fetch_symbols() -> Dict:
    """
    Fetch all supported currency symbols.

    Returns:
        Dict mapping currency codes to their full names.
    """
    url = f"{EXCHANGE_RATES_BASE_URL}/symbols"
    params = _build_params()
    logger.info("Fetching supported currency symbols")
    data = _request_with_retry(url, params)
    return data.get("symbols", {})


def save_raw_response(
    data: Dict,
    filename: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> Path:
    """
    Persist a raw API response as a JSON file.

    Args:
        data: The API response dict to save.
        filename: Name for the file (without extension). Auto-generated
                  from the current timestamp if not provided.
        output_dir: Override the default raw data directory.

    Returns:
        Path to the saved file.
    """
    ensure_data_dirs()
    target_dir = output_dir or RAW_DIR

    if filename is None:
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
        date_label = data.get("date", "unknown")
        filename = f"exchange_rates_{date_label}_{ts}"

    filepath = target_dir / f"{filename}.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info("Saved raw response → %s (%d bytes)", filepath, filepath.stat().st_size)
    return filepath


# ── Convenience runner ──────────────────────────────────────────────


def ingest_latest() -> Path:
    """Fetch the latest rates and save the raw response. Returns path."""
    data = fetch_latest_rates()
    return save_raw_response(data)


def ingest_historical(date: str) -> Path:
    """Fetch historical rates for *date* and save. Returns path."""
    data = fetch_historical_rates(date)
    return save_raw_response(data)


if __name__ == "__main__":
    path = ingest_latest()
    print(f"✅ Ingested latest rates → {path}")
