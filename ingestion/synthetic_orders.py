"""
synthetic_orders.py
Generates realistic synthetic e-commerce orders using Faker.
Simulates real business patterns — weekday/weekend variation,
city-based pricing, seasonal trends.

Usage:
    python -m ingestion.synthetic_orders
    python -m ingestion.synthetic_orders --date 2024-01-15 --count 500
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

from ingestion.minio_client import upload_json, check_bucket_exists

fake = Faker("en_IN")  # Indian locale — matches our cities
random.seed(42)        # reproducible data

# ── Constants ──────────────────────────────────────────────────
BRONZE_BUCKET = "bronze"
SOURCE = "orders"

CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune"]
CITY_WEIGHTS = [0.30, 0.25, 0.20, 0.15, 0.10]  # Mumbai most orders

PRODUCTS = {
    "Laptop":      {"min": 800,  "max": 1500, "category": "Electronics"},
    "Phone":       {"min": 300,  "max": 800,  "category": "Electronics"},
    "Tablet":      {"min": 200,  "max": 600,  "category": "Electronics"},
    "Headphones":  {"min": 50,   "max": 300,  "category": "Electronics"},
    "Monitor":     {"min": 200,  "max": 700,  "category": "Electronics"},
    "Keyboard":    {"min": 30,   "max": 150,  "category": "Accessories"},
    "Mouse":       {"min": 20,   "max": 100,  "category": "Accessories"},
    "Watch":       {"min": 100,  "max": 500,  "category": "Wearables"},
    "Shoes":       {"min": 50,   "max": 300,  "category": "Fashion"},
    "Backpack":    {"min": 40,   "max": 200,  "category": "Fashion"},
}

PRODUCT_WEIGHTS = [0.15, 0.20, 0.10, 0.10, 0.08,
                   0.08, 0.07, 0.08, 0.07, 0.07]

CURRENCIES = ["USD", "INR", "EUR", "GBP"]
CURRENCY_WEIGHTS = [0.40, 0.40, 0.10, 0.10]

PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking", "wallet"]


def generate_order(execution_date: datetime, order_num: int) -> dict:
    """Generate a single realistic order."""
    product_name = random.choices(
        list(PRODUCTS.keys()), weights=PRODUCT_WEIGHTS
    )[0]
    product = PRODUCTS[product_name]

    city = random.choices(CITIES, weights=CITY_WEIGHTS)[0]
    currency = random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS)[0]

    base_amount = round(
        random.uniform(product["min"], product["max"]), 2
    )

    # simulate weekend boost
    is_weekend = execution_date.weekday() >= 5
    if is_weekend:
        base_amount *= random.uniform(1.0, 1.2)

    quantity = random.choices([1, 2, 3], weights=[0.7, 0.2, 0.1])[0]
    total_amount = round(base_amount * quantity, 2)

    return {
        "order_id": f"ORD-{execution_date.strftime('%Y%m%d')}-{order_num:04d}",
        "customer_id": f"CUST-{fake.numerify('####')}",
        "customer_name": fake.name(),
        "customer_email": fake.email(),
        "product": product_name,
        "category": product["category"],
        "unit_price": round(base_amount, 2),
        "quantity": quantity,
        "total_amount": total_amount,
        "currency": currency,
        "city": city,
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choices(
            ["completed", "pending", "cancelled"],
            weights=[0.85, 0.10, 0.05]
        )[0],
        "order_date": execution_date.strftime("%Y-%m-%d"),
        "order_timestamp": (
            execution_date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
            )
        ).isoformat(),
        "_metadata": {
            "ingested_at": datetime.utcnow().isoformat(),
            "batch_id": "",  # filled in at batch level
            "source_system": "synthetic_generator",
            "generator_version": "1.0.0",
        },
    }


def generate_orders(
    execution_date: datetime,
    count: int = 200
) -> list:
    """Generate a full batch of orders for the day."""
    batch_id = str(uuid.uuid4())

    # weekend gets more orders
    is_weekend = execution_date.weekday() >= 5
    actual_count = int(count * 1.3) if is_weekend else count

    orders = []
    for i in range(1, actual_count + 1):
        order = generate_order(execution_date, i)
        order["_metadata"]["batch_id"] = batch_id
        orders.append(order)

    return orders, batch_id


def extract_orders(
    execution_date: datetime,
    count: int = 200
) -> list:
    """Generate and land orders in MinIO bronze."""
    print(f"\n{'='*55}")
    print(f"  🛒 Orders Generation — "
          f"{execution_date.strftime('%Y-%m-%d')}")
    print(f"{'='*55}")

    if not check_bucket_exists(BRONZE_BUCKET):
        raise RuntimeError(f"Bucket '{BRONZE_BUCKET}' not found")

    orders, batch_id = generate_orders(execution_date, count)

    payload = {
        "extraction_date": execution_date.strftime("%Y-%m-%d"),
        "order_count": len(orders),
        "batch_id": batch_id,
        "ingested_at": datetime.utcnow().isoformat(),
        "orders": orders,
    }

    s3_path = upload_json(
        data=payload,
        bucket=BRONZE_BUCKET,
        source=SOURCE,
        execution_date=execution_date,
        filename="orders_raw.json",
    )

    # summary stats
    cities = {}
    products = {}
    for o in orders:
        cities[o["city"]] = cities.get(o["city"], 0) + 1
        products[o["product"]] = products.get(o["product"], 0) + 1

    top_city = max(cities, key=cities.get)
    top_product = max(products, key=products.get)
    total_revenue = sum(o["total_amount"] for o in orders)

    print(f"\n✅ Orders generated!")
    print(f"   Orders created  : {len(orders)}")
    print(f"   Total revenue   : ${total_revenue:,.2f}")
    print(f"   Top city        : {top_city} ({cities[top_city]} orders)")
    print(f"   Top product     : {top_product} ({products[top_product]} orders)")
    print(f"   Landed at       : {s3_path}")
    print(f"{'='*55}\n")

    return orders


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument(
        "--count", type=int, default=200,
        help="Number of orders to generate"
    )
    args = parser.parse_args()

    execution_date = (
        datetime.strptime(args.date, "%Y-%m-%d")
        if args.date
        else datetime.utcnow() - timedelta(days=1)
    )
    extract_orders(execution_date, args.count)


if __name__ == "__main__":
    main()