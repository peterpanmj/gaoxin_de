"""Audit-safe filesystem utilities."""

import hashlib
import json
import os
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path


def now() -> str:
    """Return the current time as a timezone-aware UTC ISO-8601 string."""
    return datetime.now(UTC).isoformat()


def identifier(value: str) -> str:
    """Validate and return a filesystem-safe snapshot or release identifier."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", value):
        raise ValueError("IDs must be 1-80 alphanumeric, hyphen or underscore characters")
    return value


def digest(path: Path) -> str:
    """Return the SHA-256 checksum of a file's exact bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic_json(path: Path, value: dict) -> None:
    """Durably replace a JSON file so readers never observe a partial write."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, default=str)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
