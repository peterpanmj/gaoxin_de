"""Python field validation and normalization for assessment B/C.

The saleor-order-v2 contract is recorded and checked on replay. Unknown fields
are ignored in normalized output and retained in raw input. Python checks field
types and supported currencies; dbt checks relational and aggregate invariants.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any


class RecordError(ValueError):
    """A source record violates the versioned pipeline contract."""


CONTRACT_VERSION = "saleor-order-v2"
STATUSES = {
    "DRAFT",
    "UNCONFIRMED",
    "UNFULFILLED",
    "PARTIALLY_FULFILLED",
    "FULFILLED",
    "PARTIALLY_RETURNED",
    "RETURNED",
    "CANCELED",
    "EXPIRED",
}


def _object(value: Any, field: str) -> dict:
    if not isinstance(value, dict):
        raise RecordError(f"{field} must be an object")
    return value


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RecordError(f"{field} must be a nonempty string")
    return value.strip()


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
        if not amount.is_finite() or amount < 0 or amount >= Decimal("1000000000000000"):
            raise RecordError(f"{field}.amount must be finite and in [0, 10^15)")
        return str(amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))
    except (InvalidOperation, ValueError) as exc:
        raise RecordError(f"{field}.amount must be numeric") from exc


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
    """Enforce the v2 order contract and return canonical data (assessment B/C).

    Unknown fields are ignored; breaking required-field/type changes raise
    RecordError. Normalize UTC timestamps, whitespace and two-place finite money.
    Order totals need not equal line totals because shipping/discounts may differ.
    """
    if not isinstance(raw, dict):
        raise RecordError("record must be a JSON object")
    channel = raw.get("channel") or {}
    if not isinstance(channel, dict):
        raise RecordError("channel must be an object")
    currency = _required(channel.get("currencyCode"), "channel.currencyCode")
    if not isinstance(currency, str) or len(currency) != 3 or not currency.isalpha():
        raise RecordError("channel.currencyCode must be a three-letter currency")
    currency = currency.upper()
    if currency not in {"USD", "EUR", "PLN"}:
        raise RecordError("currency must be USD, EUR or PLN")
    raw_lines = raw.get("lines") or []
    if isinstance(raw_lines, dict):
        raw_lines = raw_lines.get("edges") or raw_lines.get("nodes") or []
        if not isinstance(raw_lines, list):
            raise RecordError("line connection must contain a list")
        raw_lines = [_object(item, "line").get("node", item) for item in raw_lines]
    if not isinstance(raw_lines, list) or not raw_lines:
        raise RecordError("lines must contain at least one line")

    lines: list[OrderLine] = []
    seen_line_ids: set[str] = set()
    for index, line in enumerate(raw_lines):
        if not isinstance(line, dict):
            raise RecordError(f"lines[{index}] must be an object")
        line_id = _text(line.get("id"), f"lines[{index}].id")
        if not isinstance(line_id, str) or line_id in seen_line_ids:
            raise RecordError("line IDs must be unique strings")
        seen_line_ids.add(line_id)
        quantity = line.get("quantity")
        if type(quantity) is not int or not 1 <= quantity <= 2147483647:
            raise RecordError(f"lines[{index}].quantity must be a positive integer")
        lines.append(
            OrderLine(
                line_id=line_id,
                product_name=_text(line.get("productName"), f"lines[{index}].productName"),
                sku=_text(line.get("productSku", line.get("variantSku")), "sku")
                if line.get("productSku", line.get("variantSku")) is not None
                else None,
                quantity=quantity,
                unit_amount=_money(
                    _object(line.get("unitPrice"), "unitPrice").get("gross"),
                    f"lines[{index}].unitPrice.gross",
                    currency,
                ),
                line_amount=_money(
                    _object(line.get("totalPrice"), "totalPrice").get("gross"),
                    f"lines[{index}].totalPrice.gross",
                    currency,
                ),
            )
        )
    created = _timestamp(raw.get("created"), "created")
    updated = _timestamp(raw.get("updatedAt"), "updatedAt")
    if datetime.fromisoformat(updated) < datetime.fromisoformat(created):
        raise RecordError("updatedAt must not precede created")
    status = _text(raw.get("status"), "status")
    if status not in STATUSES:
        raise RecordError("unsupported order status")
    return Order(
        order_id=_text(raw.get("id"), "id"),
        order_number=_text(raw.get("number"), "number"),
        created_at=created,
        updated_at=updated,
        status=status,
        channel=_text(channel.get("slug"), "channel.slug"),
        currency=currency,
        total_amount=_money(
            _object(raw.get("total"), "total").get("gross"), "total.gross", currency
        ),
        lines=tuple(sorted(lines, key=lambda line: line.line_id)),
    )
