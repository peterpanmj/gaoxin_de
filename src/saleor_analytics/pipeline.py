"""File ingestion and DuckDB release-building primitives."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import duckdb

from saleor_analytics.common import atomic_json, digest, identifier, now
from saleor_analytics.config import Settings
from saleor_analytics.records import RecordError, normalize_order


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, default=str, sort_keys=True) + "\n")


def ingest_jsonl(settings: Settings, input_path: Path, snapshot_id: str) -> dict[str, Any]:
    """Persist an immutable input snapshot and its accepted/rejected contract results."""
    snapshot_id = identifier(snapshot_id)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")
    snapshot_dir = settings.root / "bronze" / snapshot_id
    raw_path = snapshot_dir / "orders.raw.jsonl"
    accepted_path = snapshot_dir / "orders.accepted.jsonl"
    rejected_path = settings.root / "quarantine" / snapshot_id / "orders.rejected.jsonl"
    if (snapshot_dir / "manifest.json").exists():
        raise FileExistsError(f"Snapshot {snapshot_id} already exists; use a new ID")
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(input_path, raw_path)
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    with raw_path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                accepted.append(normalize_order(json.loads(line)).as_dict())
            except (json.JSONDecodeError, RecordError) as exc:
                rejected.append(
                    {"line_number": line_number, "error": str(exc), "raw": line.rstrip()}
                )
    _write_jsonl(accepted_path, accepted)
    _write_jsonl(rejected_path, rejected)
    total = len(accepted) + len(rejected)
    reject_rate = len(rejected) / total if total else 1.0
    manifest = {
        "snapshot_id": snapshot_id,
        "extracted_at": now(),
        "input_sha256": digest(raw_path),
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "reject_rate": reject_rate,
        "quality_gate": "passed"
        if total and reject_rate <= settings.reject_rate_limit
        else "failed",
        "contract_version": "saleor-order-v1",
    }
    atomic_json(snapshot_dir / "manifest.json", manifest)
    if manifest["quality_gate"] == "failed":
        raise RecordError(f"Snapshot failed reject-rate gate ({reject_rate:.1%})")
    return manifest


def _all_accepted_snapshots(root: Path) -> list[tuple[str, Path]]:
    snapshots: list[tuple[str, Path]] = []
    for manifest_path in sorted((root / "bronze").glob("*/manifest.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("quality_gate") == "passed":
            snapshots.append(
                (manifest["snapshot_id"], manifest_path.parent / "orders.accepted.jsonl")
            )
    return snapshots


def build_candidate(settings: Settings, release_id: str) -> Path:
    """Build an isolated DuckDB candidate from every quality-approved snapshot."""
    release_id = identifier(release_id)
    candidate_dir = settings.root / "releases" / release_id
    candidate_dir.mkdir(parents=True, exist_ok=False)
    database = candidate_dir / "analytics.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(
        """create table stg_order_versions (
        snapshot_id varchar, order_id varchar, order_number varchar, created_at timestamp,
        updated_at timestamp, status varchar, channel varchar, currency varchar,
        total_amount decimal(18,2), payload_json varchar)"""
    )
    connection.execute(
        """create table stg_order_line_versions (
        snapshot_id varchar, order_id varchar, line_id varchar, product_name varchar,
        sku varchar, quantity integer, unit_amount decimal(18,2), line_amount decimal(18,2))"""
    )
    for snapshot_id, accepted_path in _all_accepted_snapshots(settings.root):
        with accepted_path.open(encoding="utf-8") as stream:
            for line in stream:
                order = json.loads(line)
                connection.execute(
                    "insert into stg_order_versions values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        snapshot_id,
                        order["order_id"],
                        order["order_number"],
                        order["created_at"],
                        order["updated_at"],
                        order["status"],
                        order["channel"],
                        order["currency"],
                        order["total_amount"],
                        json.dumps(order, sort_keys=True),
                    ],
                )
                for item in order["lines"]:
                    connection.execute(
                        "insert into stg_order_line_versions values (?, ?, ?, ?, ?, ?, ?, ?)",
                        [
                            snapshot_id,
                            order["order_id"],
                            item["line_id"],
                            item["product_name"],
                            item["sku"],
                            item["quantity"],
                            item["unit_amount"],
                            item["line_amount"],
                        ],
                    )
    connection.close()
    atomic_json(candidate_dir / "release.json", {"release_id": release_id, "created_at": now()})
    return database


def publish_candidate(settings: Settings, database: Path) -> Path:
    """Atomically replace the serving warehouse only after its build succeeds."""
    target = settings.root / "warehouse" / "analytics.duckdb"
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".next.duckdb")
    shutil.copyfile(database, temporary)
    temporary.replace(target)
    return target


def run_dbt_build(database: Path) -> None:
    """Run models and data tests against a candidate; raises without publishing on failure."""
    project_root = Path(__file__).resolve().parents[2]
    environment = os.environ | {"ANALYTICS_DATABASE_PATH": str(database.resolve())}
    subprocess.run(
        [
            "dbt",
            "build",
            "--project-dir",
            str(project_root / "analytics"),
            "--profiles-dir",
            str(project_root / "analytics"),
            "--target",
            "candidate",
        ],
        check=True,
        env=environment,
    )
