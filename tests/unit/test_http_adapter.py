from __future__ import annotations

import ast
import traceback
from collections.abc import Callable
from datetime import date
from pathlib import Path

import httpx
import pytest

from application.contracts import Provider
from application.errors import ProviderHttpError
from application.ports.http import HttpRequest, HttpResponse, HttpTransport, RequestContext
from application.ports.market_data import MarketDataProvider, ProviderRequest
from application.registry import series_contract
from application.retry import RetryPolicy
from ingestion.httpx_adapter import HttpxTransport, TimeoutConfig

CONTEXT = RequestContext(Provider.FRED, "us_10y", "DGS10")


def mock_transport(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


def test_application_does_not_import_httpx() -> None:
    for path in Path("application").rglob("*.py"):
        tree = ast.parse(path.read_text())
        imports = [
            node for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))
        ]
        assert not any(
            (isinstance(node, ast.Import) and any(alias.name == "httpx" for alias in node.names))
            or (isinstance(node, ast.ImportFrom) and node.module == "httpx")
            for node in imports
        )


def test_http_port_and_market_provider_protocol_are_substitutable() -> None:
    class FakeTransport:
        def send(self, request: HttpRequest, *, context: RequestContext) -> HttpResponse:
            return HttpResponse(200, b"ok", {})

    class FakeProvider:
        provider = Provider.FRED

        def fetch(self, series, request):
            return (series.series_id, request.logical_start, request.logical_end)

    transport: HttpTransport = FakeTransport()
    provider: MarketDataProvider = FakeProvider()
    response = transport.send(HttpRequest("GET", "https://example.test"), context=CONTEXT)
    request = ProviderRequest("update", date(2026, 8, 11), date(2026, 8, 19), False)
    assert response.status_code == 200
    assert provider.provider is Provider.FRED
    assert provider.fetch(series_contract("us_10y"), request) == (
        "us_10y",
        date(2026, 8, 11),
        date(2026, 8, 19),
    )


def test_provider_request_enforces_exact_operation_window_contract() -> None:
    update = ProviderRequest("update", date(2026, 8, 11), date(2026, 8, 19), False)
    assert update.logical_start == date(2026, 8, 11)
    assert update.logical_end == date(2026, 8, 19)
    assert not update.maximum_history
    assert ProviderRequest("bootstrap", None, date(2026, 8, 19), True).maximum_history
    assert ProviderRequest("reconcile", None, date(2026, 8, 19), True).maximum_history
    with pytest.raises(ValueError, match="bounded logical_start"):
        ProviderRequest("update", None, date(2026, 8, 19), False)
    with pytest.raises(ValueError, match="maximum_history"):
        ProviderRequest("reconcile", None, date(2026, 8, 19), False)
    with pytest.raises(ValueError, match="after logical_end"):
        ProviderRequest("update", date(2026, 8, 20), date(2026, 8, 19), False)


def test_retry_policy_validation_status_and_delays() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(initial_delay_seconds=-1)
    with pytest.raises(ValueError):
        RetryPolicy(multiplier=0.5)
    with pytest.raises(ValueError):
        RetryPolicy(max_delay_seconds=-1)
    policy = RetryPolicy(max_attempts=4, initial_delay_seconds=1, multiplier=2, max_delay_seconds=3)
    assert policy.retryable_status(429)
    assert policy.retryable_status(503)
    assert not policy.retryable_status(404)
    assert [policy.delay_after(i) for i in (1, 2, 3)] == [1, 2, 3]
    assert policy.delay_after(1, 10) == 3
    assert policy.delay_after(1, 0.25) == 0.25
    with pytest.raises(ValueError):
        policy.delay_after(0)


def test_success_and_explicit_timeout_config() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=b"ok", headers={"X-Test": "yes"})

    transport = HttpxTransport(
        timeout=TimeoutConfig(connect=1, read=2, write=3, pool=4),
        transport=mock_transport(handler),
    )
    response = transport.send(
        HttpRequest("GET", "https://example.test/data", params={"x": "1"}), context=CONTEXT
    )
    assert response.status_code == 200
    assert response.content == b"ok"
    assert response.headers["x-test"] == "yes"
    assert transport.timeout == TimeoutConfig(connect=1, read=2, write=3, pool=4)
    assert seen[0].url.params["x"] == "1"
    transport.close()


def test_non_retryable_4xx_returns_after_one_attempt() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(404, content=b"missing")

    transport = HttpxTransport(transport=mock_transport(handler))
    response = transport.send(HttpRequest("GET", "https://example.test/x"), context=CONTEXT)
    assert response.status_code == 404
    assert attempts == 1
    transport.close()


def test_429_and_5xx_retry_with_deterministic_sleep_and_retry_after_cap() -> None:
    statuses = iter(
        [
            httpx.Response(429, headers={"Retry-After": "10"}),
            httpx.Response(503),
            httpx.Response(200, content=b"ok"),
        ]
    )
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        return next(statuses)

    transport = HttpxTransport(
        retry_policy=RetryPolicy(
            max_attempts=3, initial_delay_seconds=0.5, multiplier=2, max_delay_seconds=3
        ),
        sleeper=sleeps.append,
        transport=mock_transport(handler),
    )
    result = transport.send(HttpRequest("GET", "https://example.test/x"), context=CONTEXT)
    assert result.status_code == 200
    assert sleeps == [3, 1.0]
    transport.close()


def test_invalid_retry_after_falls_back_to_exponential_delay() -> None:
    statuses = iter([httpx.Response(429, headers={"Retry-After": "tomorrow"}), httpx.Response(200)])
    sleeps: list[float] = []
    transport = HttpxTransport(
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.75),
        sleeper=sleeps.append,
        transport=mock_transport(lambda request: next(statuses)),
    )
    transport.send(HttpRequest("GET", "https://example.test/x"), context=CONTEXT)
    assert sleeps == [0.75]
    transport.close()


def test_transport_error_retries_then_raises_sanitized_error() -> None:
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("secret-token=DO-NOT-LEAK", request=request)

    transport = HttpxTransport(
        retry_policy=RetryPolicy(max_attempts=2, initial_delay_seconds=0.25),
        sleeper=sleeps.append,
        transport=mock_transport(handler),
    )
    request = HttpRequest(
        "GET",
        "https://user:password@example.test/data?api_key=SECRET&token=HIDDEN",
        headers={"Authorization": "Bearer ALSO-SECRET"},
    )
    with pytest.raises(ProviderHttpError) as captured:
        transport.send(request, context=CONTEXT)
    error = captured.value
    rendered_error = "\n".join(
        [
            str(error),
            repr(error),
            "".join(traceback.format_exception(error)),
            repr(error.__cause__),
            repr(error.__context__),
        ]
    )
    assert error.category == "transport_exhausted"
    assert sleeps == [0.25]
    assert "https://example.test/data" in rendered_error
    assert error.__cause__ is None
    assert error.__context__ is None
    for secret in ("SECRET", "HIDDEN", "password", "ALSO-SECRET", "DO-NOT-LEAK"):
        assert secret not in rendered_error
    transport.close()


def test_http_retry_exhaustion_is_typed_and_sanitized() -> None:
    transport = HttpxTransport(
        retry_policy=RetryPolicy(max_attempts=2),
        sleeper=lambda seconds: None,
        transport=mock_transport(lambda request: httpx.Response(503)),
    )
    with pytest.raises(ProviderHttpError) as captured:
        transport.send(
            HttpRequest("GET", "https://example.test/data?api_key=SECRET"), context=CONTEXT
        )
    assert captured.value.status_code == 503
    assert captured.value.category == "http_retry_exhausted"
    assert "SECRET" not in str(captured.value)
    transport.close()
