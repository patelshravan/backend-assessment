from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List

import dlt
import requests

from database import DATABASE_URL

FLASK_URL = "http://mock-server:5000/api/customers"

CUSTOMER_COLUMNS = {
    "customer_id": {"data_type": "text", "precision": 50, "nullable": False},
    "first_name": {"data_type": "text", "precision": 100, "nullable": False},
    "last_name": {"data_type": "text", "precision": 100, "nullable": False},
    "email": {"data_type": "text", "precision": 255, "nullable": False},
    "phone": {"data_type": "text", "precision": 20, "nullable": True},
    "address": {"data_type": "text", "nullable": True},
    "date_of_birth": {"data_type": "date", "nullable": True},
    "account_balance": {"data_type": "decimal", "precision": 15, "scale": 2, "nullable": True},
    # created_at: TIMESTAMP (without timezone in destination)
    "created_at": {"data_type": "timestamp", "nullable": True, "timezone": False},
}


def _coerce_customer_types(customer: Dict[str, Any]) -> Dict[str, Any]:
    """Make sure dlt sees proper Python types (Decimal/Date/Datetime) for schema hints."""
    coerced = dict(customer)

    if "account_balance" in coerced and coerced["account_balance"] is not None:
        coerced["account_balance"] = Decimal(str(coerced["account_balance"]))

    if "date_of_birth" in coerced and coerced["date_of_birth"] is not None:
        coerced["date_of_birth"] = date.fromisoformat(coerced["date_of_birth"])

    if "created_at" in coerced and coerced["created_at"] is not None:
        # Expected format: 2024-01-01T10:00:00 (no timezone)
        coerced["created_at"] = datetime.fromisoformat(coerced["created_at"])

    return coerced


def fetch_all_from_flask(page_size: int = 10) -> List[Dict[str, Any]]:
    page = 1
    all_data: List[Dict[str, Any]] = []

    while True:
        res = requests.get(f"{FLASK_URL}?page={page}&limit={page_size}").json()
        data = res.get("data", [])

        if not data:
            break

        all_data.extend(_coerce_customer_types(c) for c in data)
        page += 1

    return all_data


@dlt.resource(
    name="customers",
    primary_key="customer_id",
    write_disposition="merge",
    columns=CUSTOMER_COLUMNS,
)
def customers_resource(data: List[Dict[str, Any]]):
    # `yield from` allows dlt to stream rows into destination loaders.
    yield from data


def ingest() -> int:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    data = fetch_all_from_flask(page_size=10)

    pipeline = dlt.pipeline(
        pipeline_name="flask_to_postgres",
        destination=dlt.destinations.postgres(DATABASE_URL),
        dataset_name="public",
    )

    pipeline.run(customers_resource(data))

    return len(data)