"""Saleor GraphQL transport. Queries may retry; mutations never retry blindly."""

import os
import time

import httpx

from saleor_analytics.config import Settings


class SourceError(RuntimeError):
    """The source could not provide a trustworthy response."""


class SaleorClient:
    """Credential-aware GraphQL transport with bounded read retries (A/B/D).

    Environment proxy settings are disabled for the local source. Caller code
    owns closing the HTTP client; no source mutation is automatically retried.
    """

    def __init__(self, settings: Settings, transport=None, sleeper=time.sleep):
        self.settings = settings
        self.sleeper = sleeper
        self.client = httpx.Client(
            timeout=settings.timeout_seconds, transport=transport, trust_env=False
        )
        token = os.environ.get("SALEOR_TOKEN")
        if token:
            self.client.headers["Authorization"] = f"Bearer {token}"

    def close(self):
        self.client.close()

    def execute(self, query: str, variables: dict | None = None, *, mutation=False) -> dict:
        """Return GraphQL data or raise SourceError (assessment B/C/D).

        Retry transport errors, HTTP 429 and 5xx up to max_attempts for reads.
        Fail immediately on other HTTP failures, malformed JSON or GraphQL
        errors. mutation=True limits execution to one attempt; query text is not
        inspected to infer whether an operation mutates source state.
        """
        attempts = 1 if mutation else self.settings.max_attempts
        for attempt in range(attempts):
            try:
                response = self.client.post(
                    self.settings.saleor_url, json={"query": query, "variables": variables or {}}
                )
                if response.status_code == 429 or response.status_code >= 500:
                    raise SourceError(f"Transient source HTTP {response.status_code}")
                response.raise_for_status()
            except (httpx.TransportError, SourceError) as exc:
                if attempt + 1 == attempts:
                    raise SourceError(
                        "Source request failed; no complete snapshot produced"
                    ) from exc
                self.sleeper(min(2**attempt, 8))
                continue
            except httpx.HTTPStatusError as exc:
                raise SourceError(f"Source HTTP {response.status_code}; verify access") from exc
            try:
                body = response.json()
            except ValueError as exc:
                raise SourceError("Source returned invalid JSON") from exc
            if not isinstance(body, dict) or body.get("errors") or not body.get("data"):
                raise SourceError("GraphQL request failed; inspect query schema and permissions")
            return body["data"]
        raise SourceError("Source request exhausted")

    def authenticate(self):
        """Use SALEOR_TOKEN or exchange runtime email/password for a token (A/F).

        Tokens stay in the HTTP client's headers. Missing credentials and failed
        token creation raise SourceError; no credentials are written to files.
        """
        if "Authorization" in self.client.headers:
            return
        email, password = os.getenv("SALEOR_EMAIL"), os.getenv("SALEOR_PASSWORD")
        if not email or not password:
            raise SourceError("Set SALEOR_TOKEN or SALEOR_EMAIL and SALEOR_PASSWORD")
        result = self.execute(
            "mutation($email:String!,$password:String!){tokenCreate(email:$email,"
            "password:$password){token errors{code}}}",
            {"email": email, "password": password},
            mutation=True,
        )["tokenCreate"]
        if result.get("errors") or not result.get("token"):
            raise SourceError("Saleor authentication failed")
        self.client.headers["Authorization"] = f"Bearer {result['token']}"
