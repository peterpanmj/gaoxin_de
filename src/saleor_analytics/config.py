"""Nonsecret configuration and environment-supplied credentials."""

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Nonsecret per-environment batch settings for assessment B/E/F.

    allow_mock permits only explicit synthetic-file generation, never source mutation.
    """

    root: Path
    saleor_url: str = "http://localhost:8000/graphql/"
    page_size: int = 50
    timeout_seconds: int = 30
    max_attempts: int = 3
    reject_rate_limit: float = 0.05
    allow_mock: bool = False

    @classmethod
    def load(cls, path: Path) -> "Settings":
        """Read TOML, resolve storage and validate selected bounds (B/F).

        TOML root is relative to the config file. A relative ANALYTICS_ROOT
        override instead resolves from the process working directory. SALEOR_URL
        overrides the endpoint. Parsing, unknown-key and validation errors
        propagate to the caller; credentials are loaded separately by the client.
        """
        path = path.resolve()
        with path.open("rb") as stream:
            values = tomllib.load(stream)
        values["root"] = Path(os.getenv("ANALYTICS_ROOT", path.parent / values["root"]))
        values["root"] = values["root"].resolve()
        values["saleor_url"] = os.getenv("SALEOR_URL", values.get("saleor_url", cls.saleor_url))
        result = cls(**values)
        if not 1 <= result.page_size <= 100 or not 1 <= result.max_attempts <= 5:
            raise ValueError("page_size must be 1-100 and max_attempts must be 1-5")
        if not 0 <= result.reject_rate_limit < 1:
            raise ValueError("reject_rate_limit must be in [0, 1)")
        return result
