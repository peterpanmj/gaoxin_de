import json
from pathlib import Path

import duckdb
import pytest

from saleor_analytics.config import Settings
from saleor_analytics.pipeline import build_candidate, ingest_jsonl, publish_candidate
from saleor_analytics.records import RecordError, normalize_order


def order(*, order_id="order-1", updated="2026-01-15T10:30:00Z"):
    return {
        "id": order_id,
        "number": "#1001",
        "created": "2026-01-15T10:00:00Z",
        "updatedAt": updated,
        "status": "UNFULFILLED",
        "channel": {"slug": "default-channel", "currencyCode": "USD"},
        "total": {"gross": {"amount": "20.00", "currency": "USD"}},
        "lines": [
            {
                "id": "line-1",
                "productName": "Demo product",
                "variantSku": "DEMO-1",
                "quantity": 2,
                "unitPrice": {"gross": {"amount": "10.00", "currency": "USD"}},
                "totalPrice": {"gross": {"amount": "20.00", "currency": "USD"}},
            }
        ],
    }


def settings(tmp_path: Path) -> Settings:
    return Settings(root=tmp_path, reject_rate_limit=0.5)


def test_normalize_order_rejects_currency_mismatch():
    raw = order()
    raw["total"]["gross"]["currency"] = "EUR"
    with pytest.raises(RecordError, match="currency"):
        normalize_order(raw)


def test_ingestion_quarantines_invalid_rows_and_builds_candidate(tmp_path: Path):
    input_path = tmp_path / "source.jsonl"
    input_path.write_text(json.dumps(order()) + "\nnot-json\n", encoding="utf-8")
    manifest = ingest_jsonl(settings(tmp_path), input_path, "snapshot-001")
    assert manifest["accepted_count"] == 1
    assert manifest["rejected_count"] == 1
    candidate = build_candidate(settings(tmp_path), "release-001")
    connection = duckdb.connect(str(candidate), read_only=True)
    assert connection.execute("select count(*) from stg_order_versions").fetchone()[0] == 1
    connection.close()
    with pytest.raises(RecordError, match="validated"):
        publish_candidate(settings(tmp_path), candidate)
