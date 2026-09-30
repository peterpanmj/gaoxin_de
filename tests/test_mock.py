"""Verify deterministic local mock generation, safety guards, and scenario shapes."""

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


def test_dated_mock_is_replayable_and_distinct_from_other_days(tmp_path):
    settings = Settings(tmp_path, allow_mock=True)
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    generate_mock(settings, first, count=2, order_date="2026-09-29")
    generate_mock(settings, second, count=2, order_date="2026-09-30")
    generate_mock(settings, second, count=2, order_date="2026-09-30")
    first_rows = [json.loads(line) for line in first.read_text().splitlines()]
    second_rows = [json.loads(line) for line in second.read_text().splitlines()]
    assert {row["id"] for row in first_rows}.isdisjoint(row["id"] for row in second_rows)
    assert all(row["created"].startswith("2026-09-30") for row in second_rows)
    with pytest.raises(ValueError):
        generate_mock(settings, tmp_path / "invalid.jsonl", order_date="2026-09-31")


def test_dated_update_keeps_order_ids_and_advances_first_version(tmp_path):
    settings = Settings(tmp_path, allow_mock=True)
    baseline = tmp_path / "baseline.jsonl"
    update = tmp_path / "update.jsonl"
    generate_mock(settings, baseline, count=2, order_date="2026-09-30")
    generate_mock(settings, update, scenario="update", count=2, order_date="2026-09-30")
    before = [json.loads(line) for line in baseline.read_text().splitlines()]
    after = [json.loads(line) for line in update.read_text().splitlines()]
    assert before[0]["id"] == after[0]["id"]
    assert before[0]["updatedAt"] < after[0]["updatedAt"]
    assert after[0]["total"]["gross"]["amount"] == "30.00"
    assert before[1] == after[1]
