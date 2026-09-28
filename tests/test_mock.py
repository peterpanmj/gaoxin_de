import json

import pytest

from saleor_analytics.config import Settings
from saleor_analytics.mock import generate_mock
from saleor_analytics.records import RecordError


def test_generation_is_explicit_deterministic_and_non_overwriting(tmp_path):
    path = tmp_path / "mock.jsonl"
    with pytest.raises(RecordError, match="disabled"):
        generate_mock(Settings(tmp_path), path)
    settings = Settings(tmp_path, allow_mock=True)
    generate_mock(settings, path, "duplicate", 2)
    original = path.read_bytes()
    generate_mock(settings, path, "duplicate", 2)
    assert path.read_bytes() == original
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows) == 3 and rows[0] == rows[-1]
    with pytest.raises(FileExistsError):
        generate_mock(settings, path, "invalid", 2)


def test_trend_generation_uses_new_orders_on_consecutive_dates(tmp_path):
    path = tmp_path / "trend.jsonl"
    generate_mock(Settings(tmp_path, allow_mock=True), path, "trend", 3)
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row["id"] for row in rows] == [
        "generated-trend-order-0",
        "generated-trend-order-1",
        "generated-trend-order-2",
    ]
    assert [row["created"][:10] for row in rows] == ["2026-01-16", "2026-01-17", "2026-01-18"]
