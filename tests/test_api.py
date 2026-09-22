import httpx
import pytest

from saleor_analytics.api import SaleorClient, SourceError
from saleor_analytics.config import Settings


def test_graphql_error_is_not_success(tmp_path):
    client = SaleorClient(
        Settings(tmp_path),
        httpx.MockTransport(
            lambda request: httpx.Response(200, json={"errors": [{"message": "denied"}]})
        ),
    )
    with pytest.raises(SourceError, match="GraphQL"):
        client.execute("query { shop { name } }")
    client.close()


@pytest.mark.parametrize("mutation,expected", [(False, 3), (True, 1)])
def test_transient_retry_boundary(tmp_path, mutation, expected):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503)

    client = SaleorClient(Settings(tmp_path), httpx.MockTransport(handler), sleeper=lambda _: None)
    with pytest.raises(SourceError):
        client.execute("query { shop { name } }", mutation=mutation)
    assert len(calls) == expected
    client.close()
