from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from application.errors import ProviderHttpError
from application.ports.http import HttpRequest, HttpResponse, RequestContext
from application.ports.market_data import ProviderRequest
from application.registry import series_contract
from ingestion.ecb_provider import EcbProvider

NOW = datetime(2026, 8, 19, 2, tzinfo=UTC)
START = date(2026, 8, 11)
END = date(2026, 8, 19)
CSV = b"TIME_PERIOD,OBS_VALUE\n2026-08-11,0.31\n2026-08-13,.\n2026-08-19,0.45\n"


class FakeTransport:
    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        self.calls: list[tuple[HttpRequest, RequestContext]] = []

    def send(self, request: HttpRequest, *, context: RequestContext) -> HttpResponse:
        self.calls.append((request, context))
        return self.response


def update_request() -> ProviderRequest:
    return ProviderRequest("update", START, END, False)


def test_both_registered_series_map_exact_delta_bounds_and_scalar_schema() -> None:
    for series_id, flow in (("ciss", "CISS"), ("estr", "EST")):
        transport = FakeTransport(HttpResponse(200, CSV, {}))
        provider = EcbProvider(transport, clock=lambda: NOW)
        frame = provider.fetch(series_contract(series_id), update_request())
        sent, context = transport.calls[0]
        assert f"/{flow}/" in sent.url
        assert sent.params["startPeriod"] == "2026-08-11"
        assert sent.params["endPeriod"] == "2026-08-19"
        assert sent.params["format"] == "csvdata"
        assert context.series_id == series_id
        assert frame.get_column("observation_date").to_list() == [START, END]
        assert frame.get_column("value").to_list() == [0.31, 0.45]
        assert frame.columns[-1] == "value"


def test_bootstrap_and_reconcile_omit_start_and_keep_end_bound() -> None:
    for request in (
        ProviderRequest("bootstrap", None, END, True),
        ProviderRequest("reconcile", None, END, True),
    ):
        transport = FakeTransport(HttpResponse(200, CSV, {}))
        frame = EcbProvider(transport, clock=lambda: NOW).fetch(series_contract("ciss"), request)
        sent = transport.calls[0][0]
        assert "startPeriod" not in sent.params
        assert sent.params["endPeriod"] == END.isoformat()
        assert frame.height == 2


def test_calendar_gaps_and_missing_values_remain_absent() -> None:
    frame = EcbProvider(FakeTransport(HttpResponse(200, CSV, {})), clock=lambda: NOW).fetch(
        series_contract("estr"), update_request()
    )
    assert date(2026, 8, 12) not in frame.get_column("observation_date").to_list()
    assert date(2026, 8, 13) not in frame.get_column("observation_date").to_list()


@pytest.mark.parametrize("value", ["bad", "nan", "inf"])
def test_malformed_or_nonfinite_values_fail_closed(value: str) -> None:
    content = f"TIME_PERIOD,OBS_VALUE\n2026-08-19,{value}\n".encode()
    provider = EcbProvider(FakeTransport(HttpResponse(200, content, {})), clock=lambda: NOW)

    with pytest.raises(ValueError, match="invalid observation value"):
        provider.fetch(series_contract("ciss"), update_request())


def test_empty_and_semantic_no_result_are_valid_noops() -> None:
    empty = EcbProvider(FakeTransport(HttpResponse(200, b"", {})), clock=lambda: NOW).fetch(
        series_contract("ciss"), update_request()
    )
    assert empty.height == 0
    no_result = EcbProvider(
        FakeTransport(HttpResponse(404, b"No records found for query", {})), clock=lambda: NOW
    ).fetch(series_contract("ciss"), update_request())
    assert no_result.height == 0


@pytest.mark.parametrize(
    "content, error",
    [
        (b"WRONG,OBS_VALUE\n2026-08-19,1\n", "missing TIME_PERIOD"),
        (b"TIME_PERIOD,OBS_VALUE\nbad,1\n", "invalid observation"),
        (
            b"TIME_PERIOD,OBS_VALUE\n2026-08-19,1\n2026-08-19,2\n",
            "duplicate observation",
        ),
        (b"TIME_PERIOD,OBS_VALUE\n2026-08-10,1\n", "out-of-window"),
    ],
)
def test_invalid_and_out_of_window_payloads_fail(content: bytes, error: str) -> None:
    provider = EcbProvider(FakeTransport(HttpResponse(200, content, {})), clock=lambda: NOW)
    with pytest.raises(ValueError, match=error):
        provider.fetch(series_contract("ciss"), update_request())


def test_only_registered_contracts_and_strict_update_are_accepted() -> None:
    provider = EcbProvider(FakeTransport(HttpResponse(200, CSV, {})), clock=lambda: NOW)
    with pytest.raises(ValueError, match="unsupported ECB"):
        provider.fetch(series_contract("vix"), update_request())
    with pytest.raises(ValueError, match="exact bounded"):
        provider.fetch(series_contract("ciss"), ProviderRequest("update", START, END, True))


def test_revision_shortening_http_failure_and_naive_clock_are_deterministic() -> None:
    revised = b"TIME_PERIOD,OBS_VALUE\n2026-08-19,9.9\n"
    frame = EcbProvider(FakeTransport(HttpResponse(200, revised, {})), clock=lambda: NOW).fetch(
        series_contract("ciss"), update_request()
    )
    assert frame.height == 1
    assert frame.get_column("value").item() == 9.9
    failing = EcbProvider(FakeTransport(HttpResponse(503, b"server", {})), clock=lambda: NOW)
    with pytest.raises(ProviderHttpError):
        failing.fetch(series_contract("ciss"), update_request())
    naive = EcbProvider(
        FakeTransport(HttpResponse(200, revised, {})),
        clock=lambda: datetime(2026, 8, 19, 2),
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        naive.fetch(series_contract("ciss"), update_request())
