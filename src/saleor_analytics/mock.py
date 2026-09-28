"""Deterministic local synthetic scenarios; no source-system mutation (B/C/D)."""

import json
from datetime import date, timedelta
from pathlib import Path

from saleor_analytics.config import Settings
from saleor_analytics.records import RecordError


def generate_mock(settings: Settings, output: Path, scenario: str = "baseline", count: int = 20):
    """Generate safe JSONL fixtures, including trend/update/duplicate/reject scenarios.

    Explicit local allow_mock is required. Existing different files are never
    overwritten, making repeated Airflow generation deterministic.
    """
    if not settings.allow_mock:
        raise RecordError("Mock generation disabled; set allow_mock=true in a demo config")
    if (
        scenario not in {"baseline", "trend", "update", "duplicate", "invalid"}
        or not 1 <= count <= 100000
    ):
        raise ValueError("Invalid scenario or count (1-100000)")
    rows = []
    for i in range(count):
        quantity = 3 if scenario == "update" and i == 0 else 2
        amount = f"{quantity * 10}.00"
        created = "2026-01-15T10:00:00Z"
        if scenario == "trend":
            created = f"{date(2026, 1, 16) + timedelta(days=i)}T10:00:00Z"
        prefix = "trend-" if scenario == "trend" else ""
        rows.append(
            {
                "id": f"generated-{prefix}order-{i}",
                "number": f"SYN-{prefix}{i}",
                "created": created,
                "updatedAt": "2026-01-16T10:00:00Z" if quantity == 3 else created,
                "status": "UNFULFILLED",
                "channel": {"slug": "synthetic-us", "currencyCode": "USD"},
                "total": {"gross": {"amount": amount, "currency": "USD"}},
                "lines": [
                    {
                        "id": f"generated-{prefix}line-{i}",
                        "productName": "Synthetic notebook",
                        "productSku": "SYN-NOTEBOOK",
                        "quantity": quantity,
                        "unitPrice": {"gross": {"amount": "10.00", "currency": "USD"}},
                        "totalPrice": {"gross": {"amount": amount, "currency": "USD"}},
                    }
                ],
            }
        )
    if scenario == "duplicate":
        rows.append(rows[0])
    if scenario == "invalid":
        rows[0]["lines"][0]["quantity"] = 0
    content = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
    if output.exists() and output.read_text(encoding="utf-8") != content:
        raise FileExistsError("Output exists with different data; use a new filename")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(content, encoding="utf-8")
    return {"scenario": scenario, "rows": len(rows), "path": str(output)}
