"""Yahoo chart adapter for the registered MOVE index."""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from datetime import UTC, date, datetime, time, timedelta
from urllib.parse import quote

import polars as pl

from application.contracts import FetchCapability, NativeShape, Provider, SeriesContract
from application.errors import ProviderHttpError
from application.ports.http import HttpRequest, HttpTransport, RequestContext
from application.ports.market_data import ProviderRequest
from ingestion.ohlc_validation import validate_ohlc_bar

Clock = Callable[[], datetime]
_DEFAULT_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
_YAHOO_USER_AGENT = "regime-loader/0.1 (+https://github.com/SergejSchweizer/regime-loader)"


def _system_utc_now() -> datetime:
    return datetime.now(UTC)


def _epoch_start(day: date) -> int:
    return int(datetime.combine(day, time.min, tzinfo=UTC).timestamp())


class YahooMoveProvider:
    """Adapter translating Yahoo's daily MOVE chart response into Bronze OHLC."""

    def __init__(
        self,
        transport: HttpTransport,
        *,
        clock: Clock | None = None,
        base_url: str = _DEFAULT_BASE_URL,
    ) -> None:
        self._transport = transport
        self._clock = clock if clock is not None else _system_utc_now
        self._base_url = base_url.rstrip("/")

    @property
    def provider(self) -> Provider:
        return Provider.YAHOO

    def fetch(self, series: SeriesContract, request: ProviderRequest) -> pl.DataFrame:
        self._validate_contract(series)
        url = f"{self._base_url}/{quote(series.source_id, safe='')}"
        params: dict[str, str | int | float] = {
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "false",
            "period2": _epoch_start(request.logical_end + timedelta(days=1)),
        }
        if request.operation == "update":
            if request.maximum_history or request.logical_start is None:
                raise ValueError("Yahoo update requires exact bounded dates")
            params["period1"] = _epoch_start(request.logical_start)
        else:
            params["period1"] = 0
        context = RequestContext(self.provider, series.series_id, series.source_id)
        response = self._transport.send(
            HttpRequest("GET", url, params=params, headers={"User-Agent": _YAHOO_USER_AGENT}),
            context=context,
        )
        if response.status_code != 200:
            raise ProviderHttpError(
                context=context,
                category="source_unavailable",
                request_path=url,
                status_code=response.status_code,
            )
        frame = self._parse(series, response.content, url)
        if request.operation == "update" and frame.height:
            assert request.logical_start is not None
            outside = frame.filter(
                ~pl.col("observation_date").is_between(
                    request.logical_start,
                    request.logical_end,
                    closed="both",
                )
            )
            if outside.height:
                raise ValueError("Yahoo bounded response contains out-of-window observations")
        return frame.sort("observation_date")

    def _validate_contract(self, series: SeriesContract) -> None:
        if series.provider is not self.provider or series.series_id != "move":
            raise ValueError("unsupported Yahoo series contract")
        if series.source_id != "^MOVE":
            raise ValueError("MOVE source identity must be ^MOVE")
        if series.native_shape is not NativeShape.OHLC:
            raise ValueError("MOVE must use OHLC Bronze shape")
        if series.fetch_capability is not FetchCapability.DATE_RANGE:
            raise ValueError("MOVE must use date_range capability")

    def _parse(self, series: SeriesContract, content: bytes, source_url: str) -> pl.DataFrame:
        try:
            payload = json.loads(content)
            chart = payload["chart"]
            if chart.get("error") is not None:
                raise ValueError("Yahoo chart response contains provider error")
            results = chart.get("result") or []
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise ValueError("invalid Yahoo chart payload") from exc
        if not results:
            return self._empty_frame()
        result = results[0]
        timestamps = result.get("timestamp") or []
        indicators = result.get("indicators") or {}
        quotes = indicators.get("quote") or []
        if not quotes:
            if timestamps:
                raise ValueError("Yahoo payload has timestamps without OHLC quote data")
            return self._empty_frame()
        quote_data = quotes[0]
        vectors = {name: quote_data.get(name) or [] for name in ("open", "high", "low", "close")}
        expected = len(timestamps)
        if any(len(values) != expected for values in vectors.values()):
            raise ValueError("Yahoo OHLC vectors do not match timestamp count")
        rows: list[dict[str, object]] = []
        for index, timestamp in enumerate(timestamps):
            values = {name: vectors[name][index] for name in vectors}
            if all(value is None for value in values.values()):
                continue
            if values["close"] is None:
                raise ValueError("Yahoo payload contains missing close")
            numeric: dict[str, float] = {}
            for name, raw in values.items():
                if raw is None:
                    raise ValueError(f"Yahoo payload contains missing {name}")
                try:
                    value = float(raw)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"Yahoo payload contains non-numeric {name}") from exc
                if not math.isfinite(value):
                    raise ValueError(f"Yahoo payload contains non-finite {name}")
                numeric[name] = value
            validate_ohlc_bar(numeric, provider="Yahoo")
            rows.append(
                {
                    "observation_date": datetime.fromtimestamp(int(timestamp), UTC).date(),
                    **numeric,
                }
            )
        frame = pl.DataFrame(
            rows,
            schema={
                "observation_date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
            },
        )
        if bool(frame.select(pl.col("observation_date").is_duplicated().any()).item()):
            raise ValueError("Yahoo payload contains duplicate observation dates")
        fetched_at = self._clock()
        if fetched_at.tzinfo is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return frame.with_columns(
            pl.lit(series.series_id).alias("series_id"),
            pl.lit(self.provider.value).alias("provider"),
            pl.lit(fetched_at.astimezone(UTC)).alias("fetched_at_utc"),
            pl.lit(series.source_id).alias("source_id"),
            pl.lit(source_url).alias("source_url"),
        ).select(
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
        )

    @staticmethod
    def _empty_frame() -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "series_id": pl.String,
                "provider": pl.String,
                "observation_date": pl.Date,
                "fetched_at_utc": pl.Datetime("us", "UTC"),
                "source_id": pl.String,
                "source_url": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
            }
        )
