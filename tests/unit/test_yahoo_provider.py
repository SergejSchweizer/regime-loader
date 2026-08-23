from __future__ import annotations

import json
from datetime import UTC, date, datetime, time, timedelta

import pytest

from application.errors import ProviderHttpError
from application.ports.http import HttpRequest, HttpResponse, RequestContext
from application.ports.market_data import ProviderRequest
from application.registry import series_contract
from ingestion.yahoo_provider import YahooMoveProvider

NOW = datetime(2026, 8, 19, 2, tzinfo=UTC)
START = date(2026, 8, 11)
END = date(2026, 8, 19)


def epoch(day: date) -> int:
    return int(datetime.combine(day, time.min, tzinfo=UTC).timestamp())


def payload(days: list[date], closes: list[float | None] | None = None) -> bytes:
    close_values = closes if closes is not None else [20.5 + i for i in range(len(days))]
    quote = {
        "open": [20.0 + i for i in range(len(days))],
        "high": [21.0 + i for i in range(len(days))],
        "low": [19.0 + i for i in range(len(days))],
        "close": close_values,
        "volume": [123 for _ in days],
    }
    return json.dumps(
        {
            "chart": {
                "result": [
                    {"timestamp": [epoch(day) for day in days], "indicators": {"quote": [quote]}}
                ],
                "error": None,
            }
        }
    ).encode()


def payload_with_all_null_bar(days: list[date]) -> bytes:
    document = json.loads(payload(days))
    quote = document["chart"]["result"][0]["indicators"]["quote"][0]
    for name in ("open", "high", "low", "close"):
        quote[name][-1] = None
    return json.dumps(document).encode()


class FakeTransport:
    def __init__(self, response: HttpResponse) -> None:
        self.response = response
        self.calls: list[tuple[HttpRequest, RequestContext]] = []

    def send(self, request: HttpRequest, *, context: RequestContext) -> HttpResponse:
        self.calls.append((request, context))
        return self.response


def update_request() -> ProviderRequest:
    return ProviderRequest("update", START, END, False)


def test_update_sends_exact_bounded_request_and_normalizes_only_ohlc() -> None:
    transport = FakeTransport(HttpResponse(200, payload([START, END]), {}))
    provider = YahooMoveProvider(transport, clock=lambda: NOW)
    frame = provider.fetch(series_contract("move"), update_request())
    sent = transport.calls[0][0]
    assert sent.params["period1"] == epoch(START)
    assert sent.params["period2"] == epoch(END + timedelta(days=1))
    assert sent.params["interval"] == "1d"
    assert sent.url.endswith("/%5EMOVE")
    assert sent.headers["User-Agent"].startswith("regime-loader/")
    assert frame.get_column("observation_date").to_list() == [START, END]
    assert "volume" not in frame.columns
    assert frame.columns[-4:] == ["open", "high", "low", "close"]


def test_bootstrap_and_reconcile_are_explicit_max_history_requests() -> None:
    for operation in ("bootstrap", "reconcile"):
        transport = FakeTransport(HttpResponse(200, payload([date(2000, 1, 3), END]), {}))
        provider = YahooMoveProvider(transport, clock=lambda: NOW)
        request = ProviderRequest(operation, None, END, True)  # type: ignore[arg-type]
        frame = provider.fetch(series_contract("move"), request)
        assert transport.calls[0][0].params["period1"] == 0
        assert frame.height == 2


def test_empty_bounded_response_is_valid_noop() -> None:
    content = json.dumps({"chart": {"result": [], "error": None}}).encode()
    frame = YahooMoveProvider(
        FakeTransport(HttpResponse(200, content, {})), clock=lambda: NOW
    ).fetch(series_contract("move"), update_request())
    assert frame.height == 0
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


def test_all_null_ohlc_bar_is_ignored_without_fabricating_an_observation() -> None:
    provider = YahooMoveProvider(
        FakeTransport(HttpResponse(200, payload_with_all_null_bar([START, END]), {})),
        clock=lambda: NOW,
    )

    frame = provider.fetch(series_contract("move"), update_request())

    assert frame.get_column("observation_date").to_list() == [START]


def test_out_of_window_bounded_row_is_contract_failure() -> None:
    provider = YahooMoveProvider(
        FakeTransport(HttpResponse(200, payload([date(2026, 8, 10), END]), {})),
        clock=lambda: NOW,
    )
    with pytest.raises(ValueError, match="out-of-window"):
        provider.fetch(series_contract("move"), update_request())


@pytest.mark.parametrize(
    "content, error",
    [
        (payload([END], [None]), "missing close"),
        (payload([END, END]), "duplicate observation"),
        (b"not-json", "invalid Yahoo"),
        (
            json.dumps(
                {
                    "chart": {
                        "result": [{"timestamp": [epoch(END)], "indicators": {"quote": []}}],
                        "error": None,
                    }
                }
            ).encode(),
            "timestamps without OHLC",
        ),
    ],
)
def test_invalid_payloads_are_rejected(content: bytes, error: str) -> None:
    provider = YahooMoveProvider(FakeTransport(HttpResponse(200, content, {})), clock=lambda: NOW)
    with pytest.raises(ValueError, match=error):
        provider.fetch(series_contract("move"), update_request())


def test_only_move_contract_and_strict_update_semantics_are_accepted() -> None:
    provider = YahooMoveProvider(
        FakeTransport(HttpResponse(200, payload([END]), {})), clock=lambda: NOW
    )
    with pytest.raises(ValueError, match="unsupported Yahoo"):
        provider.fetch(series_contract("vix"), update_request())
    with pytest.raises(ValueError, match="exact bounded"):
        provider.fetch(
            series_contract("move"),
            ProviderRequest("update", START, END, True),
        )


def test_http_error_short_reconcile_and_naive_clock() -> None:
    unavailable = YahooMoveProvider(FakeTransport(HttpResponse(503, b"", {})), clock=lambda: NOW)
    with pytest.raises(ProviderHttpError):
        unavailable.fetch(series_contract("move"), update_request())
    short = YahooMoveProvider(
        FakeTransport(HttpResponse(200, payload([END]), {})), clock=lambda: NOW
    )
    assert (
        short.fetch(series_contract("move"), ProviderRequest("reconcile", None, END, True)).height
        == 1
    )
    naive = YahooMoveProvider(
        FakeTransport(HttpResponse(200, payload([END]), {})),
        clock=lambda: datetime(2026, 8, 19, 2),
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        naive.fetch(series_contract("move"), update_request())
