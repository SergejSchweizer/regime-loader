from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl
import pytest

from application.contracts import Provider
from application.errors import ProviderHttpError
from application.ports.http import HttpRequest, HttpResponse, RequestContext
from application.ports.market_data import ProviderRequest
from application.registry import series_contract
from ingestion.cboe_provider import CboeProvider

NOW = datetime(2026, 8, 19, 2, tzinfo=UTC)
CSV = (
    b"DATE,OPEN,HIGH,LOW,CLOSE\n"
    b"08/10/2026,20,21,19,20.5\n"
    b"08/11/2026,21,22,20,21.5\n"
    b"08/19/2026,22,23,21,22.5\n"
)


class FakeTransport:
    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        self.calls: list[tuple[HttpRequest, RequestContext]] = []

    def send(self, request: HttpRequest, *, context: RequestContext) -> HttpResponse:
        self.calls.append((request, context))
        return self.response


def update_request() -> ProviderRequest:
    return ProviderRequest("update", date(2026, 8, 11), date(2026, 8, 19), False)


def test_all_registered_routes_use_registry_source_and_exact_delta_filter() -> None:
    for series_id in ("vix", "vix9d", "vix3m", "vix6m", "vix1y"):
        transport = FakeTransport(HttpResponse(200, CSV, {}))
        provider = CboeProvider(transport, clock=lambda: NOW)
        contract = series_contract(series_id)
        frame = provider.fetch(contract, update_request())
        assert frame.get_column("observation_date").to_list() == [
            date(2026, 8, 11),
            date(2026, 8, 19),
        ]
        assert frame.columns == [
            "series_id",
            "provider",
            "observation_date",
            "fetched_at_utc",
            "source_id",
            "source_url",
            "open",
            "high",
            "low",
            "close",
        ]
        assert frame.get_column("provider").unique().item() == Provider.CBOE.value
        assert frame.get_column("source_id").unique().item() == contract.source_id
        sent, context = transport.calls[0]
        assert sent.url.endswith(f"/{contract.source_id}")
        assert not sent.params
        assert context.series_id == series_id


def test_bootstrap_and_reconcile_accept_full_exposed_payload() -> None:
    provider = CboeProvider(FakeTransport(HttpResponse(200, CSV, {})), clock=lambda: NOW)
    contract = series_contract("vix")
    bootstrap = provider.fetch(
        contract,
        ProviderRequest("bootstrap", None, date(2026, 8, 19), True),
    )
    reconcile = provider.fetch(
        contract,
        ProviderRequest("reconcile", None, date(2026, 8, 19), True),
    )
    assert bootstrap.height == 3
    assert reconcile.height == 3
    assert bootstrap.get_column("fetched_at_utc").dtype == pl.Datetime("us", "UTC")


def test_invalid_contract_update_and_clock_fail_deterministically() -> None:
    transport = FakeTransport(HttpResponse(200, CSV, {}))
    provider = CboeProvider(transport, clock=lambda: NOW)
    with pytest.raises(ValueError, match="unsupported CBOE"):
        provider.fetch(series_contract("us_10y"), update_request())
    with pytest.raises(ValueError, match="bounded logical delta"):
        provider.fetch(
            series_contract("vix"),
            ProviderRequest("update", date(2026, 8, 11), date(2026, 8, 19), True),
        )
    naive = CboeProvider(transport, clock=lambda: datetime(2026, 8, 19, 2))
    with pytest.raises(ValueError, match="timezone-aware"):
        naive.fetch(series_contract("vix"), update_request())


@pytest.mark.parametrize(
    "payload, message",
    [
        (b"DATE,OPEN,HIGH,LOW\n08/19/2026,1,2,0\n", "missing required"),
        (b"DATE,OPEN,HIGH,LOW,CLOSE\n08/19/2026,1,2,0,nan\n", "non-finite close"),
        (
            b"DATE,OPEN,HIGH,LOW,CLOSE\n08/19/2026,1,2,0,1\n08/19/2026,2,3,1,2\n",
            "duplicate observation",
        ),
        (b"DATE,OPEN,HIGH,LOW,CLOSE\nbad,1,2,0,1\n", "invalid or missing"),
    ],
)
def test_invalid_payloads_are_rejected(payload: bytes, message: str) -> None:
    provider = CboeProvider(FakeTransport(HttpResponse(200, payload, {})), clock=lambda: NOW)
    with pytest.raises(ValueError, match=message):
        provider.fetch(series_contract("vix"), update_request())


@pytest.mark.parametrize(
    "bar, message",
    [
        ("-1,2,0,1", "non-negative"),
        ("2,1,0,1", "high is below open"),
        ("1,1,0,2", "high is below open"),
        ("1,4,2,3", "low is above open"),
        ("3,4,2,1", "low is above open"),
        ("1,1,2,1", "high is below low"),
    ],
)
def test_ohlc_market_bar_invariants_are_rejected(bar: str, message: str) -> None:
    payload = f"DATE,OPEN,HIGH,LOW,CLOSE\n08/19/2026,{bar}\n".encode()
    provider = CboeProvider(FakeTransport(HttpResponse(200, payload, {})), clock=lambda: NOW)

    with pytest.raises(ValueError, match=message):
        provider.fetch(series_contract("vix"), update_request())


def test_source_unavailable_is_typed_and_safe() -> None:
    provider = CboeProvider(FakeTransport(HttpResponse(404, b"missing", {})), clock=lambda: NOW)
    with pytest.raises(ProviderHttpError) as captured:
        provider.fetch(series_contract("vix"), update_request())
    assert captured.value.category == "source_unavailable"
    assert captured.value.status_code == 404
    assert "VIX_History.csv" in str(captured.value)


def test_shorter_full_file_response_is_not_synthetically_expanded() -> None:
    short = b"DATE,OPEN,HIGH,LOW,CLOSE\n08/19/2026,22,23,21,22.5\n"
    provider = CboeProvider(FakeTransport(HttpResponse(200, short, {})), clock=lambda: NOW)
    frame = provider.fetch(series_contract("vix"), update_request())
    assert frame.height == 1
    assert frame.get_column("observation_date").item() == date(2026, 8, 19)
