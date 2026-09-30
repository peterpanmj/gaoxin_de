"""Verify replay, failure recovery, watermark, and reporting invariants end to end.

These tests use independent expected totals and real dbt builds to ensure retries,
quarantine, releases, backfills, and the dashboard stay correct across failures.
"""

import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from saleor_analytics.common import atomic_json
from saleor_analytics.config import Settings
from saleor_analytics.dashboard import filter_report
from saleor_analytics.extract import extract_orders
from saleor_analytics.pipeline import (
    build_candidate,
    current_release,
    exclude_snapshot,
    fingerprint,
    ingest_jsonl,
    publish_candidate,
    read_json,
    run_dbt_build,
)
from saleor_analytics.records import RecordError, normalize_order


def fixture():
    return json.loads(Path("samples/orders-baseline.jsonl").read_text().splitlines()[0])


def write(path, *rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "field,value",
    [
        ("price", None),
        ("amount", "NaN"),
        ("amount", "Infinity"),
        ("quantity", True),
        ("quantity", 0),
        ("updatedAt", "2020-01-01T00:00:00Z"),
    ],
)
def test_bad_types_are_contract_errors(field, value):
    raw = fixture()
    if field == "price":
        raw["lines"][0]["unitPrice"] = value
    elif field == "amount":
        raw["total"]["gross"]["amount"] = value
    elif field == "quantity":
        raw["lines"][0]["quantity"] = value
    else:
        raw[field] = value
    with pytest.raises(RecordError):
        normalize_order(raw)


def test_dedup_retry_conflict_and_tamper(tmp_path):
    settings = Settings(tmp_path)
    raw = fixture()
    path = write(tmp_path / "input.jsonl", raw, raw)
    result = ingest_jsonl(settings, path, "one")
    assert (result["accepted_count"], result["duplicate_count"]) == (1, 1)
    assert ingest_jsonl(settings, path, "one") == result
    raw["total"]["gross"]["amount"] = "30.00"
    write(path, raw)
    with pytest.raises(RecordError, match="different input"):
        ingest_jsonl(settings, path, "one")
    path = write(path, fixture(), raw)
    with pytest.raises(RecordError, match="conflicts 1"):
        ingest_jsonl(settings, path, "conflict")
    accepted = tmp_path / "bronze/one/orders.accepted.jsonl"
    accepted.write_text("[]")
    with pytest.raises(RecordError, match="checksum"):
        build_candidate(settings, "tampered")


def test_currency_channels_and_empty_selection():
    frame = pd.DataFrame(
        {
            "currency": ["USD", "PLN", "USD"],
            "channel": ["a", "a", "b"],
            "order_date": pd.to_datetime(["2026-01-15"] * 3),
            "gross_amount": [20, 100, 30],
        }
    )
    daily, products = filter_report(frame, frame, "USD", ["a"], "2026-01-01", "2026-01-31")
    assert daily.gross_amount.sum() == products.gross_amount.sum() == 20
    assert filter_report(frame, frame, "USD", [], None, None)[0].empty

    unsorted = pd.DataFrame(
        {
            "currency": ["USD"] * 3,
            "channel": ["a"] * 3,
            "order_date": pd.to_datetime(["2026-01-15", "2026-01-18", "2026-01-16"]),
            "gross_amount": [400, 20, 20],
        }
    )
    chronological, _ = filter_report(unsorted, unsorted, "USD", ["a"], None, None)
    assert chronological.order_date.dt.day.tolist() == [15, 16, 18]


class Source:
    calls = []
    pages = []

    def __init__(self, _):
        pass

    def authenticate(self):
        pass

    def close(self):
        pass

    def execute(self, _, variables):
        self.calls.append(variables)
        page = self.pages.pop(0)
        if isinstance(page, Exception):
            raise page
        return {"orders": page}


def page(rows, next_cursor=None):
    return {
        "edges": [{"node": row} for row in rows],
        "pageInfo": {"hasNextPage": next_cursor is not None, "endCursor": next_cursor},
    }


def test_incremental_window_retries_and_empty_delta(tmp_path):
    settings = Settings(tmp_path)
    source = fingerprint(settings.saleor_url)
    atomic_json(
        tmp_path / "warehouse/current.json", {"watermarks": {source: "2026-01-15T11:00:00+00:00"}}
    )
    Source.pages, Source.calls = [page([fixture()], "cursor"), RuntimeError("source down")], []
    clock = lambda: datetime(2026, 1, 15, 12, tzinfo=UTC)  # noqa: E731
    with pytest.raises(RuntimeError):
        extract_orders(settings, "delta", mode="incremental", clock=clock, client_factory=Source)
    assert not (tmp_path / "bronze/delta/manifest.json").exists()
    Source.pages = [page([])]
    result = extract_orders(
        settings, "delta", mode="incremental", clock=clock, client_factory=Source
    )
    assert result["accepted_count"] == 0 and result["quality_gate"] == "passed"
    assert Source.calls[0]["filter"]["updatedAt"] == {
        "gte": "2026-01-15T10:55:00+00:00",
        "lte": "2026-01-15T11:59:55+00:00",
    }
    assert current_release(settings)["watermarks"][source] == "2026-01-15T11:00:00+00:00"
    count = len(Source.calls)
    assert extract_orders(settings, "delta", mode="incremental", client_factory=Source) == result
    assert len(Source.calls) == count


def test_backfill_uses_explicit_half_open_utc_window_without_advancing_watermark(tmp_path):
    settings = Settings(tmp_path)
    source = fingerprint(settings.saleor_url)
    # A historical replay remains valid even when the incremental checkpoint is newer.
    atomic_json(
        tmp_path / "warehouse/current.json", {"watermarks": {source: "2099-01-01T00:00:00+00:00"}}
    )
    in_range = fixture()
    at_end = copy.deepcopy(in_range)
    at_end["id"] = "order-at-end"
    at_end["updatedAt"] = "2026-01-16T00:00:00Z"
    Source.pages, Source.calls = [page([in_range, at_end])], []

    result = extract_orders(
        settings,
        "backfill-jan-15",
        mode="backfill",
        backfill_start="2026-01-15T00:00:00Z",
        backfill_end="2026-01-16T00:00:00Z",
        client_factory=Source,
    )

    assert result["accepted_count"] == 1
    assert result["extraction"] == {
        "mode": "backfill",
        "source": source,
        "upper": "2026-01-16T00:00:00+00:00",
        "lower": "2026-01-15T00:00:00+00:00",
        "base_watermark": "2099-01-01T00:00:00+00:00",
        "range_semantics": "[start, end)",
        "watermark_advance": False,
    }
    assert Source.calls[0]["filter"]["updatedAt"] == {
        "gte": "2026-01-15T00:00:00+00:00",
        "lte": "2026-01-16T00:00:00+00:00",
    }
    with pytest.raises(ValueError, match="both --start and --end"):
        extract_orders(
            settings,
            "missing-bound",
            mode="backfill",
            backfill_start="2026-01-15T00:00:00Z",
            client_factory=Source,
        )
    with pytest.raises(ValueError, match="UTC timestamp"):
        extract_orders(
            settings,
            "non-utc-bound",
            mode="backfill",
            backfill_start="2026-01-15T00:00:00+08:00",
            backfill_end="2026-01-16T00:00:00+08:00",
            client_factory=Source,
        )


@pytest.mark.integration
def test_validated_release_failure_recovery_and_removed_lines(tmp_path, monkeypatch):
    settings = Settings(tmp_path)
    raw = fixture()
    second_line = copy.deepcopy(raw["lines"][0])
    second_line["id"] = "removed-line"
    raw["lines"].append(second_line)
    raw["total"]["gross"]["amount"] = "40.00"
    provenance = {"mode": "full", "source": "synthetic", "upper": "2026-01-15T11:00:00+00:00"}
    ingest_jsonl(settings, write(tmp_path / "input.jsonl", raw), "one", extraction=provenance)
    first = build_candidate(settings, "first")
    run_dbt_build(first)
    publish_candidate(settings, first)
    backfill = copy.deepcopy(raw)
    backfill["created"] = "2026-01-14T10:00:00Z"
    backfill["updatedAt"] = "2026-01-14T12:00:00Z"
    backfill["lines"][0]["id"] = "backfill-line"
    ingest_jsonl(
        settings,
        write(tmp_path / "input.jsonl", backfill),
        "backfill",
        extraction={
            "mode": "backfill",
            "source": "synthetic",
            "lower": "2026-01-14T00:00:00+00:00",
            "upper": "2026-01-15T00:00:00+00:00",
            "range_semantics": "[start, end)",
            "watermark_advance": False,
        },
    )
    backfilled = build_candidate(settings, "backfilled")
    run_dbt_build(backfilled)
    publish_candidate(settings, backfilled)
    assert current_release(settings)["watermarks"] == {"synthetic": "2026-01-15T11:00:00+00:00"}

    old = current_release(settings)
    # A newer source version removes one line and changes amount.
    updated = fixture()
    updated["updatedAt"] = "2026-01-16T12:00:00Z"
    ingest_jsonl(settings, write(tmp_path / "input.jsonl", updated), "two")
    second = build_candidate(settings, "second")
    run_dbt_build(second)
    import saleor_analytics.pipeline as pipeline

    actual_write = pipeline.atomic_json

    def fail_pointer(path, value):
        if path.name == "current.json":
            raise OSError("simulated publication failure")
        actual_write(path, value)

    monkeypatch.setattr(pipeline, "atomic_json", fail_pointer)
    with pytest.raises(OSError):
        publish_candidate(settings, second)
    assert current_release(settings) == old
    monkeypatch.setattr(pipeline, "atomic_json", actual_write)
    publish_candidate(settings, second)
    with duckdb.connect(str(second), read_only=True) as conn:
        assert conn.sql("select count(*) from analytics.analytics.order_lines").fetchone()[0] == 1
        assert (
            conn.sql(
                "select sum(gross_amount) from analytics.analytics.daily_order_metrics"
            ).fetchone()[0]
            == 20
        )
    publish_candidate(settings, second)
    with pytest.raises(RecordError, match="Stale"):
        publish_candidate(settings, first)
    # Cross-snapshot conflicting version must fail dbt and preserve pointer.
    conflict = copy.deepcopy(updated)
    conflict["total"]["gross"]["amount"] = "99.00"
    ingest_jsonl(settings, write(tmp_path / "input.jsonl", conflict), "three")
    third = build_candidate(settings, "third")
    import subprocess

    with pytest.raises(subprocess.CalledProcessError):
        run_dbt_build(third)
    assert current_release(settings)["release_id"] == "second"
    assert read_json(third.parent / "release.json")["status"] == "failed"
    with pytest.raises(RecordError, match="validated"):
        publish_candidate(settings, third)
    exclude_snapshot(settings, "three", "Synthetic equal-version conflict; retain evidence")
    recovered = build_candidate(settings, "recovered")
    run_dbt_build(recovered)
    publish_candidate(settings, recovered)
    assert current_release(settings)["release_id"] == "recovered"
    with pytest.raises(RecordError, match="current release"):
        exclude_snapshot(settings, "one", "Must not discard committed history")
