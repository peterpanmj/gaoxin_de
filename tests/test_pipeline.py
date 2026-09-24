import json
from pathlib import Path

import duckdb
import pytest

from saleor_analytics.common import atomic_json
from saleor_analytics.config import Settings
from saleor_analytics.pipeline import (
    build_candidate,
    export_artifacts,
    ingest_jsonl,
    publish_candidate,
)
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


def test_export_artifacts_copies_release_and_source_evidence(tmp_path: Path):
    project = settings(tmp_path / "analytics")
    old_snapshot_id = "snapshot-000"
    snapshot_id = "snapshot-001"
    release_id = "release-001"
    old_bronze = project.root / "bronze" / old_snapshot_id
    old_bronze.mkdir(parents=True)
    (old_bronze / "orders.raw.jsonl").write_text('{"source": "old"}\n')
    (old_bronze / "orders.accepted.jsonl").write_text('{"source": "old"}\n')
    (old_bronze / "manifest.json").write_text("{}")
    bronze = project.root / "bronze" / snapshot_id
    bronze.mkdir(parents=True)
    (bronze / "orders.raw.jsonl").write_text('{"source": "raw"}\n')
    (bronze / "orders.accepted.jsonl").write_text('{"source": "accepted"}\n')
    (bronze / "manifest.json").write_text("{}")
    quarantine = project.root / "quarantine" / snapshot_id
    quarantine.mkdir(parents=True)
    (quarantine / "orders.rejected.jsonl").write_text('{"error": "example"}\n')
    extraction = project.root / "extractions" / snapshot_id
    extraction.mkdir(parents=True)
    (extraction / "request.json").write_text('{"mode": "incremental"}\n')
    release = project.root / "releases" / release_id
    release.mkdir(parents=True)
    (release / "analytics.duckdb").write_text("demo database")
    (release / "release.json").write_text(
        json.dumps(
            {
                "release_id": release_id,
                "inputs": [
                    {"snapshot_id": old_snapshot_id, "extracted_at": "2026-01-01T00:00:00Z"},
                    {"snapshot_id": snapshot_id, "extracted_at": "2026-01-02T00:00:00Z"},
                ],
            }
        )
    )
    atomic_json(project.root / "warehouse/current.json", {"release_id": release_id})

    result = export_artifacts(project, tmp_path / "exports")

    bundle = Path(result["export_directory"])
    assert (bundle / "releases" / release_id / "analytics.duckdb").exists()
    assert (bundle / "bronze" / snapshot_id / "orders.raw.jsonl").exists()
    assert (bundle / "quarantine" / snapshot_id / "orders.rejected.jsonl").exists()
    assert (bundle / "extractions" / snapshot_id / "request.json").exists()
    assert (bundle / "warehouse/current.json").exists()
    assert not (bundle / "bronze" / old_snapshot_id).exists()
    with pytest.raises(FileExistsError, match="already exists"):
        export_artifacts(project, tmp_path / "exports")

    full_result = export_artifacts(project, tmp_path / "exports-all", all_snapshots=True)
    assert (Path(full_result["export_directory"]) / "bronze" / old_snapshot_id).exists()
