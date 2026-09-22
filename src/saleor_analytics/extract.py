"""Paged, read-only extraction of the minimal Saleor order contract."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from saleor_analytics.api import SaleorClient
from saleor_analytics.config import Settings
from saleor_analytics.pipeline import ingest_jsonl

ORDER_QUERY = """
query Orders($first: Int!, $after: String) {
  orders(first: $first, after: $after,
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


def extract_orders(settings: Settings, snapshot_id: str) -> dict:
    """Fetch all current orders before committing a local immutable snapshot."""
    client = SaleorClient(settings)
    temporary_path: Path | None = None
    try:
        client.authenticate()
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".jsonl", delete=False
        ) as stream:
            temporary_path = Path(stream.name)
            after: str | None = None
            while True:
                result = client.execute(ORDER_QUERY, {"first": settings.page_size, "after": after})[
                    "orders"
                ]
                for edge in result["edges"]:
                    stream.write(json.dumps(edge["node"], sort_keys=True) + "\n")
                page = result["pageInfo"]
                if not page["hasNextPage"]:
                    break
                after = page["endCursor"]
        return ingest_jsonl(settings, temporary_path, snapshot_id)
    finally:
        client.close()
        if temporary_path:
            temporary_path.unlink(missing_ok=True)
