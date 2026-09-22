"""Paged, read-only extraction of the minimal Saleor order contract."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from filelock import FileLock

from saleor_analytics.api import SaleorClient
from saleor_analytics.common import atomic_json, identifier
from saleor_analytics.config import Settings
from saleor_analytics.pipeline import (
    current_release,
    fingerprint,
    ingest_jsonl,
    read_json,
    verify_snapshot,
)
from saleor_analytics.records import RecordError

ORDER_QUERY = """
query Orders($first: Int!, $after: String, $filter: OrderFilterInput) {
  orders(first: $first, after: $after, filter: $filter,
         sortBy: {field: LAST_MODIFIED_AT, direction: ASC}) {
    pageInfo { hasNextPage endCursor }
    edges {
      node {
        id number created updatedAt status
        channel { slug currencyCode }
        total { gross { amount currency } }
        lines {
          id productName productSku quantity
          unitPrice { gross { amount currency } }
          totalPrice { gross { amount currency } }
        }
      }
    }
  }
}
"""


def extract_orders(
    settings: Settings,
    snapshot_id: str,
    *,
    mode: str = "full",
    clock=None,
    client_factory=SaleorClient,
) -> dict:
    """Read a bounded API window, then validate JSONL (A/B/D).

    Incremental polling re-reads five minutes before the published checkpoint.
    Watermarks advance only on publication. Persist the window for safe retries;
    retries after a completed snapshot reuse its bytes. This is not log CDC or
    historical snapshot isolation; source deletions are not inferred.
    """
    identifier(snapshot_id)
    if mode not in {"full", "incremental"}:
        raise ValueError("mode must be full or incremental")
    directory = settings.root / "extractions" / snapshot_id
    directory.mkdir(parents=True, exist_ok=True)
    with FileLock(str(directory / ".lock"), timeout=60):
        source = fingerprint(settings.saleor_url)
        plan_path = directory / "request.json"
        if plan_path.exists():
            plan = read_json(plan_path)
            if plan["mode"] != mode or plan["source"] != source:
                raise RecordError("Extraction ID belongs to a different source/mode")
        else:
            upper = (clock() if clock else datetime.now(UTC)) - timedelta(seconds=5)
            checkpoint = current_release(settings).get("watermarks", {}).get(source)
            if mode == "incremental" and not checkpoint:
                raise RecordError("Publish an initial full extraction before incremental polling")
            lower = (
                datetime.fromisoformat(checkpoint) - timedelta(minutes=5) if checkpoint else None
            )
            if checkpoint and upper < datetime.fromisoformat(checkpoint):
                raise RecordError("Clock precedes committed source watermark")
            plan = {
                "mode": mode,
                "source": source,
                "upper": upper.isoformat(),
                "lower": lower.isoformat() if mode == "incremental" else None,
                "base_watermark": checkpoint,
            }
            atomic_json(plan_path, plan)
        snapshot = settings.root / "bronze" / snapshot_id
        if (snapshot / "manifest.json").exists():
            manifest = verify_snapshot(snapshot)
            if manifest.get("extraction") != plan or manifest["quality_gate"] != "passed":
                raise RecordError("Existing extraction snapshot failed or has different provenance")
            return manifest
        output = directory / "orders.jsonl"
        # Ingestion interruption is resumed from already-landed bytes, never a new API view.
        if (snapshot / "orders.raw.jsonl").exists():
            return ingest_jsonl(
                settings, snapshot / "orders.raw.jsonl", snapshot_id, extraction=plan
            )
        client = client_factory(settings)
        try:
            client.authenticate()
            bounds = {"lte": plan["upper"]}
            if plan["lower"]:
                bounds["gte"] = plan["lower"]
            with output.open("w", encoding="utf-8") as stream:
                after, cursors = None, set()
                while True:
                    result = client.execute(
                        ORDER_QUERY,
                        {
                            "first": settings.page_size,
                            "after": after,
                            "filter": {"updatedAt": bounds},
                        },
                    )["orders"]
                    for edge in result["edges"]:
                        stream.write(json.dumps(edge["node"], sort_keys=True) + "\n")
                    page = result["pageInfo"]
                    if not page["hasNextPage"]:
                        break
                    after = page["endCursor"]
                    if not after or after in cursors:
                        raise RecordError("Pagination cursor did not advance")
                    cursors.add(after)
            return ingest_jsonl(settings, output, snapshot_id, extraction=plan)
        finally:
            client.close()
