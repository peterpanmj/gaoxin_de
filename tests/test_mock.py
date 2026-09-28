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
