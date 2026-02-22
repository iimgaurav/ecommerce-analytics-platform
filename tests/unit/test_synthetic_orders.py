"""Unit tests for synthetic orders generator."""

import pytest
from datetime import datetime
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), "../.."))

from ingestion.synthetic_orders import generate_order, generate_orders, CITIES


def test_order_has_required_fields():
    """Generated order must have all required fields."""
    order = generate_order(datetime(2024, 1, 15), 1)
    required = [
        "order_id", "customer_id", "customer_name",
        "product", "total_amount", "currency",
        "city", "order_date", "_metadata",
    ]
    for field in required:
        assert field in order, f"Missing field: {field}"


def test_order_id_format():
    """Order ID should include date."""
    order = generate_order(datetime(2024, 1, 15), 1)
    assert "20240115" in order["order_id"]
    assert order["order_id"].startswith("ORD-")


def test_city_is_valid():
    """City must be from approved list."""
    for i in range(20):
        order = generate_order(datetime(2024, 1, 15), i)
        assert order["city"] in CITIES


def test_amount_is_positive():
    """All amounts must be positive."""
    for i in range(10):
        order = generate_order(datetime(2024, 1, 15), i)
        assert order["total_amount"] > 0
        assert order["unit_price"] > 0


def test_batch_generates_correct_count():
    """Should generate expected number of orders."""
    orders, batch_id = generate_orders(datetime(2024, 1, 15), 100)
    assert len(orders) == 100
    assert batch_id is not None


def test_all_orders_share_batch_id():
    """All orders in same batch must share batch_id."""
    orders, batch_id = generate_orders(datetime(2024, 1, 15), 50)
    for order in orders:
        assert order["_metadata"]["batch_id"] == batch_id