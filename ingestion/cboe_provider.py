"""CBOE full-file adapter for the registered volatility-index family."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from io import BytesIO

import polars as pl

from application.contracts import FetchCapability, NativeShape, Provider, SeriesContract
from application.errors import ProviderHttpError
from application.ports.http import HttpRequest, HttpTransport, RequestContext
from application.ports.market_data import ProviderRequest

Clock = Callable[[], datetime]
_CBOE_SERIES = frozenset({"vix", "vix9d", "vix3m", "vix6m", "vix1y"})
_DEFAULT_BASE_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices"


def _system_utc_now() -> datetime:
    return datetime.now(UTC)


def _normalized_clock_value(clock: Clock) -> datetime:
    value = clock()
    if value.tzinfo is None:
        raise ValueError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _parse_date_expression() -> pl.Expr:
    text = pl.col("DATE").cast(pl.String)
    return text.str.strptime(pl.Date, "%m/%d/%Y", strict=False).fill_null(
        text.str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    )


class CboeProvider:
    """Adapter translating CBOE historical CSV files into canonical Bronze OHLC."""

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
        return Provider.CBOE

    def fetch(self, series: SeriesContract, request: ProviderRequest) -> pl.DataFrame:
        self._validate_contract(series)
        url = f"{self._base_url}/{series.source_id}"
        context = RequestContext(self.provider, series.series_id, series.source_id)
        response = self._transport.send(HttpRequest("GET", url), context=context)
        if response.status_code != 200:
            raise ProviderHttpError(
                context=context,
                category="source_unavailable",
                request_path=url,
                status_code=response.status_code,
            )
        frame = self._parse(series, response.content, url)
        if request.operation == "update":
            if request.maximum_history or request.logical_start is None:
                raise ValueError("CBOE update requires bounded logical delta semantics")
            frame = frame.filter(
                pl.col("observation_date").is_between(
                    request.logical_start,
                    request.logical_end,
                    closed="both",
                )
            )
        return frame.sort("observation_date")

    def _validate_contract(self, series: SeriesContract) -> None:
        if series.provider is not self.provider or series.series_id not in _CBOE_SERIES:
            raise ValueError("unsupported CBOE series contract")
        if series.native_shape is not NativeShape.OHLC:
            raise ValueError("CBOE volatility series must use OHLC Bronze shape")
        if series.fetch_capability is not FetchCapability.FULL_FILE:
            raise ValueError("CBOE volatility series must use full_file capability")

    def _parse(self, series: SeriesContract, content: bytes, source_url: str) -> pl.DataFrame:
        try:
            raw = pl.read_csv(BytesIO(content), infer_schema_length=1000)
        except Exception as exc:
            raise ValueError("invalid CBOE CSV payload") from exc
        raw = raw.rename({name: name.strip().upper() for name in raw.columns})
        required = {"DATE", "OPEN", "HIGH", "LOW", "CLOSE"}
        if not required.issubset(raw.columns):
            raise ValueError("CBOE CSV is missing required OHLC columns")
        frame = raw.select(
            _parse_date_expression().alias("observation_date"),
            pl.col("OPEN").cast(pl.Float64, strict=False).alias("open"),
            pl.col("HIGH").cast(pl.Float64, strict=False).alias("high"),
            pl.col("LOW").cast(pl.Float64, strict=False).alias("low"),
            pl.col("CLOSE").cast(pl.Float64, strict=False).alias("close"),
        )
        if frame.select(pl.any_horizontal(pl.all().is_null()).any()).item():
            raise ValueError("CBOE payload contains invalid or missing OHLC observations")
        valid_bar = (
            pl.all_horizontal(pl.col("open", "high", "low", "close").is_finite())
            & pl.all_horizontal(pl.col("open", "high", "low", "close") >= 0)
            & (pl.col("high") >= pl.col("low"))
            & (pl.col("high") >= pl.max_horizontal("open", "close"))
            & (pl.col("low") <= pl.min_horizontal("open", "close"))
        )
        frame = frame.filter(valid_bar)
        if frame.is_empty():
            raise ValueError("CBOE payload contains no valid OHLC observations")
        if bool(frame.select(pl.col("observation_date").is_duplicated().any()).item()):
            raise ValueError("CBOE payload contains duplicate observation dates")
        fetched_at = _normalized_clock_value(self._clock)
        return frame.with_columns(
            pl.lit(series.series_id).alias("series_id"),
            pl.lit(self.provider.value).alias("provider"),
            pl.lit(fetched_at).alias("fetched_at_utc"),
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
