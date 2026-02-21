"""
Unit tests for the Exchange Rates ingestion module.

All API calls are mocked – no real network traffic.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

# ── Fixtures ────────────────────────────────────────────────────────

MOCK_LATEST_RESPONSE = {
    "success": True,
    "timestamp": 1700000000,
    "base": "EUR",
    "date": "2025-11-14",
    "rates": {
        "USD": 1.08765,
        "GBP": 0.87123,
        "JPY": 163.456,
        "INR": 90.5432,
    },
}

MOCK_HISTORICAL_RESPONSE = {
    "success": True,
    "historical": True,
    "timestamp": 1695600000,
    "base": "EUR",
    "date": "2024-01-15",
    "rates": {
        "USD": 1.09012,
        "GBP": 0.86234,
    },
}

MOCK_SYMBOLS_RESPONSE = {
    "success": True,
    "symbols": {
        "USD": "United States Dollar",
        "EUR": "Euro",
        "GBP": "British Pound Sterling",
        "JPY": "Japanese Yen",
        "INR": "Indian Rupee",
    },
}

MOCK_ERROR_RESPONSE = {
    "success": False,
    "error": {
        "code": 101,
        "info": "No API Key was specified.",
    },
}


@pytest.fixture
def mock_successful_get():
    """Patch requests.get to return a successful latest-rates response."""
    with patch("ingestion.exchange_rates.requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = MOCK_LATEST_RESPONSE.copy()
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp
        yield mock_get


@pytest.fixture
def tmp_output_dir(tmp_path):
    """Provide a temporary directory for file-output tests."""
    return tmp_path / "raw" / "exchange_rates"


# ── Tests ───────────────────────────────────────────────────────────


class TestFetchLatestRates:
    """Tests for fetch_latest_rates()."""

    def test_success(self, mock_successful_get):
        from ingestion.exchange_rates import fetch_latest_rates

        result = fetch_latest_rates(symbols=["USD", "GBP"])

        assert result["success"] is True
        assert "rates" in result
        assert "USD" in result["rates"]
        mock_successful_get.assert_called_once()

    def test_uses_default_symbols_when_none(self, mock_successful_get):
        from ingestion.exchange_rates import fetch_latest_rates

        fetch_latest_rates()  # no symbols arg

        call_kwargs = mock_successful_get.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
        assert "symbols" in params

    def test_api_error_raises(self):
        """Should raise ValueError when API returns success=false."""
        with patch("ingestion.exchange_rates.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = MOCK_ERROR_RESPONSE.copy()
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            from ingestion.exchange_rates import fetch_latest_rates

            with pytest.raises(ValueError, match="No API Key"):
                fetch_latest_rates()


class TestFetchHistoricalRates:
    """Tests for fetch_historical_rates()."""

    def test_success(self):
        with patch("ingestion.exchange_rates.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = MOCK_HISTORICAL_RESPONSE.copy()
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            from ingestion.exchange_rates import fetch_historical_rates

            result = fetch_historical_rates("2024-01-15", symbols=["USD"])

            assert result["historical"] is True
            assert result["date"] == "2024-01-15"
            assert "USD" in result["rates"]

    def test_date_in_url(self):
        with patch("ingestion.exchange_rates.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = MOCK_HISTORICAL_RESPONSE.copy()
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            from ingestion.exchange_rates import fetch_historical_rates

            fetch_historical_rates("2024-01-15")

            called_url = mock_get.call_args[0][0]
            assert "2024-01-15" in called_url


class TestFetchSymbols:
    """Tests for fetch_symbols()."""

    def test_returns_symbols_dict(self):
        with patch("ingestion.exchange_rates.requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = MOCK_SYMBOLS_RESPONSE.copy()
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            from ingestion.exchange_rates import fetch_symbols

            symbols = fetch_symbols()

            assert isinstance(symbols, dict)
            assert "USD" in symbols
            assert symbols["EUR"] == "Euro"


class TestSaveRawResponse:
    """Tests for save_raw_response()."""

    def test_writes_json_file(self, tmp_output_dir):
        from ingestion.exchange_rates import save_raw_response

        filepath = save_raw_response(
            MOCK_LATEST_RESPONSE,
            filename="test_output",
            output_dir=tmp_output_dir,
        )

        assert filepath.exists()
        assert filepath.suffix == ".json"

        with open(filepath, "r") as f:
            saved = json.load(f)
        assert saved["success"] is True
        assert saved["rates"]["USD"] == MOCK_LATEST_RESPONSE["rates"]["USD"]

    def test_auto_generates_filename(self, tmp_output_dir):
        from ingestion.exchange_rates import save_raw_response

        filepath = save_raw_response(
            MOCK_LATEST_RESPONSE,
            output_dir=tmp_output_dir,
        )

        assert filepath.exists()
        assert "exchange_rates_" in filepath.name


class TestRetryLogic:
    """Tests for the retry mechanism."""

    @patch("ingestion.exchange_rates.time.sleep")  # skip actual waits
    def test_retries_on_http_error(self, mock_sleep):
        import requests as req

        with patch("ingestion.exchange_rates.requests.get") as mock_get:
            # First two calls fail, third succeeds
            fail_resp = MagicMock()
            fail_resp.raise_for_status.side_effect = req.exceptions.ConnectionError(
                "timeout"
            )

            ok_resp = MagicMock()
            ok_resp.status_code = 200
            ok_resp.json.return_value = MOCK_LATEST_RESPONSE.copy()
            ok_resp.raise_for_status = MagicMock()

            mock_get.side_effect = [fail_resp, fail_resp, ok_resp]

            from ingestion.exchange_rates import fetch_latest_rates

            result = fetch_latest_rates()

            assert result["success"] is True
            assert mock_get.call_count == 3
