"""Validation and normalization for the Saleor order snapshot contract."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


class RecordError(ValueError):
    """A source record violates the versioned pipeline contract."""


def _required(value: Any, field: str) -> Any:
    if value is None or value == "":
        raise RecordError(f"{field} is required")
    return value


def _timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise RecordError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RecordError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise RecordError(f"{field} must include a timezone")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _money(value: Any, field: str, currency: str) -> str:
    if not isinstance(value, dict):
        raise RecordError(f"{field} must be a money object")
    if value.get("currency") != currency:
        raise RecordError(f"{field} currency must match order currency")
    try:
        amount = Decimal(str(_required(value.get("amount"), f"{field}.amount")))
    except (InvalidOperation, ValueError) as exc:
        raise RecordError(f"{field}.amount must be numeric") from exc
    if amount < 0:
        raise RecordError(f"{field}.amount must be non-negative")
    return str(amount.quantize(Decimal("0.01")))


@dataclass(frozen=True)
class OrderLine:
    line_id: str
    product_name: str
    sku: str | None
    quantity: int
    unit_amount: str
    line_amount: str


@dataclass(frozen=True)
class Order:
    order_id: str
    order_number: str
    created_at: str
    updated_at: str
    status: str
    channel: str
    currency: str
    total_amount: str
    lines: tuple[OrderLine, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_order(raw: Any) -> Order:
    """Return a canonical order, rejecting malformed or incomplete source data."""
    if not isinstance(raw, dict):
        raise RecordError("record must be a JSON object")
    channel = raw.get("channel") or {}
    if not isinstance(channel, dict):
        raise RecordError("channel must be an object")
    currency = _required(channel.get("currencyCode"), "channel.currencyCode")
    if not isinstance(currency, str) or len(currency) != 3 or not currency.isalpha():
        raise RecordError("channel.currencyCode must be a three-letter currency")
    currency = currency.upper()
    raw_lines = raw.get("lines") or []
    if isinstance(raw_lines, dict):
        raw_lines = raw_lines.get("edges") or raw_lines.get("nodes") or []
        raw_lines = [item.get("node", item) for item in raw_lines]
    if not isinstance(raw_lines, list) or not raw_lines:
        raise RecordError("lines must contain at least one line")

    lines: list[OrderLine] = []
    seen_line_ids: set[str] = set()
    for index, line in enumerate(raw_lines):
        if not isinstance(line, dict):
            raise RecordError(f"lines[{index}] must be an object")
        line_id = _required(line.get("id"), f"lines[{index}].id")
        if not isinstance(line_id, str) or line_id in seen_line_ids:
            raise RecordError("line IDs must be unique strings")
        seen_line_ids.add(line_id)
        quantity = line.get("quantity")
        if not isinstance(quantity, int) or quantity < 1:
            raise RecordError(f"lines[{index}].quantity must be a positive integer")
        lines.append(
            OrderLine(
                line_id=line_id,
                product_name=str(_required(line.get("productName"), f"lines[{index}].productName")),
                sku=line.get("productSku", line.get("variantSku")),
                quantity=quantity,
                unit_amount=_money(
                    line.get("unitPrice", {}).get("gross"),
                    f"lines[{index}].unitPrice.gross",
                    currency,
                ),
                line_amount=_money(
                    line.get("totalPrice", {}).get("gross"),
                    f"lines[{index}].totalPrice.gross",
                    currency,
                ),
            )
        )
    return Order(
        order_id=str(_required(raw.get("id"), "id")),
        order_number=str(_required(raw.get("number"), "number")),
        created_at=_timestamp(raw.get("created"), "created"),
        updated_at=_timestamp(raw.get("updatedAt"), "updatedAt"),
        status=str(_required(raw.get("status"), "status")),
        channel=str(_required(channel.get("slug"), "channel.slug")),
        currency=currency,
        total_amount=_money((raw.get("total") or {}).get("gross"), "total.gross", currency),
        lines=tuple(lines),
    )
