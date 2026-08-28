from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from application.errors import ProviderHttpError
from application.ports.http import HttpRequest, HttpResponse, RequestContext
from application.ports.market_data import ProviderRequest
from application.registry import series_contract
from ingestion.fred_provider import FredProvider

NOW = datetime(2026, 8, 19, 2, tzinfo=UTC)
START = date(2026, 8, 11)
END = date(2026, 8, 19)
API_KEY = "a" * 32


def fred_payload(rows: list[tuple[str, str | None]]) -> bytes:
    return json.dumps(
        {"observations": [{"date": day, "value": value} for day, value in rows]}
    ).encode()


class FakeTransport:
    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        self.calls: list[tuple[HttpRequest, RequestContext]] = []

    def send(self, request: HttpRequest, *, context: RequestContext) -> HttpResponse:
        self.calls.append((request, context))
        return self.response


def update_request() -> ProviderRequest:
    return ProviderRequest("update", START, END, False)


def make_provider(transport: FakeTransport, *, clock=lambda: NOW) -> FredProvider:
    return FredProvider(transport, api_key=API_KEY, clock=clock)


def test_four_registered_series_send_exact_delta_and_scalar_schema() -> None:
    content = fred_payload([("2026-08-11", "4.1"), ("2026-08-19", "4.2")])
    expected = {
        "us_2y": "DGS2",
        "us_10y": "DGS10",
        "usd_broad": "DTWEXBGS",
        "euro_hy_oas": "BAMLHE00EHYIOAS",
    }
    for series_id, source_id in expected.items():
        transport = FakeTransport(HttpResponse(200, content, {}))
        provider = make_provider(transport)
        frame = provider.fetch(series_contract(series_id), update_request())
        sent, context = transport.calls[0]
        assert sent.params["series_id"] == source_id
        assert sent.params["observation_start"] == START.isoformat()
        assert sent.params["observation_end"] == END.isoformat()
        assert sent.params["api_key"] == API_KEY
        assert context.source_id == source_id
        assert frame.get_column("observation_date").to_list() == [START, END]
        assert frame.columns[-1] == "value"
        assert API_KEY not in frame.get_column("source_url").unique().item()
        assert API_KEY not in repr(provider)


def test_bootstrap_and_reconcile_are_max_history_without_start_bound() -> None:
    content = fred_payload([("1990-01-01", "1.0"), ("2026-08-19", "2.0")])
    for request in (
        ProviderRequest("bootstrap", None, END, True),
        ProviderRequest("reconcile", None, END, True),
    ):
        transport = FakeTransport(HttpResponse(200, content, {}))
        frame = make_provider(transport).fetch(series_contract("us_10y"), request)
        assert "observation_start" not in transport.calls[0][0].params
        assert transport.calls[0][0].params["observation_end"] == END.isoformat()
        assert frame.height == 2


def test_documented_missing_values_remain_absent() -> None:
    content = fred_payload(
        [
            ("2026-08-11", "."),
            ("2026-08-12", ""),
            ("2026-08-13", None),
            ("2026-08-19", "3.5"),
        ]
    )
    frame = make_provider(FakeTransport(HttpResponse(200, content, {}))).fetch(
        series_contract("us_10y"), update_request()
    )
    assert frame.get_column("observation_date").to_list() == [END]
    assert frame.get_column("value").item() == 3.5


@pytest.mark.parametrize(
    "value, error",
    [("bad", "invalid"), ("nan", "non-finite"), ("inf", "non-finite")],
)
def test_malformed_or_nonfinite_values_fail_closed(value: str, error: str) -> None:
    content = fred_payload([("2026-08-19", value)])
    provider = make_provider(FakeTransport(HttpResponse(200, content, {})))

    with pytest.raises(ValueError, match=error):
        provider.fetch(series_contract("us_10y"), update_request())


@pytest.mark.parametrize(
    "content, error",
    [
        (fred_payload([("2026-08-19", "1"), ("2026-08-19", "2")]), "duplicate"),
        (fred_payload([("not-a-date", "1")]), "invalid observation date"),
        (fred_payload([("2026-08-10", "1")]), "out-of-window"),
        (b"not-json", "invalid FRED"),
        (json.dumps({"observations": {}}).encode(), "must be a list"),
    ],
)
def test_invalid_payload_and_bounded_contract_cases(content: bytes, error: str) -> None:
    provider = make_provider(FakeTransport(HttpResponse(200, content, {})))
    with pytest.raises(ValueError, match=error):
        provider.fetch(series_contract("us_10y"), update_request())


def test_shortened_hy_history_and_historical_reconcile_do_not_imply_deletion() -> None:
    short = fred_payload([("2026-08-19", "3.0")])
    provider = make_provider(FakeTransport(HttpResponse(200, short, {})))
    frame = provider.fetch(
        series_contract("euro_hy_oas"), ProviderRequest("reconcile", None, END, True)
    )
    assert frame.height == 1
    revised = fred_payload([("2000-01-03", "9.9")])
    revised_frame = make_provider(FakeTransport(HttpResponse(200, revised, {}))).fetch(
        series_contract("euro_hy_oas"), ProviderRequest("reconcile", None, END, True)
    )
    assert revised_frame.get_column("value").item() == 9.9


def test_secret_redaction_http_error_contract_validation_and_clock() -> None:
    failing = make_provider(FakeTransport(HttpResponse(503, b"", {})))
    with pytest.raises(ProviderHttpError) as captured:
        failing.fetch(series_contract("us_10y"), update_request())
    assert API_KEY not in str(captured.value)
    empty_response = FakeTransport(HttpResponse(200, fred_payload([]), {}))
    with pytest.raises(ValueError, match="unsupported FRED"):
        make_provider(empty_response).fetch(series_contract("vix"), update_request())
    with pytest.raises(ValueError, match="exact bounded"):
        make_provider(empty_response).fetch(
            series_contract("us_10y"), ProviderRequest("update", START, END, True)
        )
    with pytest.raises(ValueError, match="api_key"):
        FredProvider(FakeTransport(HttpResponse(200, b"", {})), api_key="")
    naive = make_provider(
        FakeTransport(HttpResponse(200, fred_payload([("2026-08-19", "1")]), {})),
        clock=lambda: datetime(2026, 8, 19, 2),
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        naive.fetch(series_contract("us_10y"), update_request())
