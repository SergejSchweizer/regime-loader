"""FRED v1 observations adapter for rates, credit spread, and broad USD series."""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from datetime import UTC, date, datetime

import polars as pl

from application.contracts import FetchCapability, NativeShape, Provider, SeriesContract
from application.errors import ProviderHttpError
from application.ports.http import HttpRequest, HttpTransport, RequestContext
from application.ports.market_data import ProviderRequest

Clock = Callable[[], datetime]
_DEFAULT_URL = "https://api.stlouisfed.org/fred/series/observations"
_FRED_SERIES = frozenset({"us_2y", "us_10y", "usd_broad", "euro_hy_oas"})


def _system_utc_now() -> datetime:
    return datetime.now(UTC)


class FredProvider:
    """Adapter translating FRED observation JSON into canonical scalar Bronze."""

    def __init__(
        self,
        transport: HttpTransport,
        *,
        api_key: str,
        clock: Clock | None = None,
        endpoint: str = _DEFAULT_URL,
    ) -> None:
        if not api_key or not api_key.strip():
            raise ValueError("FRED api_key is required")
        self._transport = transport
        self._api_key = api_key
        self._clock = clock if clock is not None else _system_utc_now
        self._endpoint = endpoint

    @property
    def provider(self) -> Provider:
        return Provider.FRED

    def fetch(self, series: SeriesContract, request: ProviderRequest) -> pl.DataFrame:
        self._validate_contract(series)
        params: dict[str, str | int | float] = {
            "series_id": series.source_id,
            "api_key": self._api_key,
            "file_type": "json",
            "sort_order": "asc",
            "observation_end": request.logical_end.isoformat(),
        }
        if request.operation == "update":
            if request.maximum_history or request.logical_start is None:
                raise ValueError("FRED update requires exact bounded observation dates")
            params["observation_start"] = request.logical_start.isoformat()
        context = RequestContext(self.provider, series.series_id, series.source_id)
        response = self._transport.send(
            HttpRequest("GET", self._endpoint, params=params), context=context
        )
        if response.status_code != 200:
            raise ProviderHttpError(
                context=context,
                category="source_unavailable",
                request_path=self._endpoint,
                status_code=response.status_code,
            )
        frame = self._parse(series, response.content)
        if request.operation == "update" and frame.height:
            assert request.logical_start is not None
            if frame.filter(
                ~pl.col("observation_date").is_between(
                    request.logical_start,
                    request.logical_end,
                    closed="both",
                )
            ).height:
                raise ValueError("FRED bounded response contains out-of-window observations")
        return frame.sort("observation_date")

    def __repr__(self) -> str:
        return f"FredProvider(endpoint={self._endpoint!r}, api_key=<redacted>)"

    def _validate_contract(self, series: SeriesContract) -> None:
        if series.provider is not self.provider or series.series_id not in _FRED_SERIES:
            raise ValueError("unsupported FRED series contract")
        if series.native_shape is not NativeShape.SCALAR:
            raise ValueError("FRED regime series must use scalar Bronze shape")
        if series.fetch_capability is not FetchCapability.DATE_RANGE:
            raise ValueError("FRED regime series must use date_range capability")

    def _parse(self, series: SeriesContract, content: bytes) -> pl.DataFrame:
        try:
            payload = json.loads(content)
            observations = payload["observations"]
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            raise ValueError("invalid FRED observations payload") from exc
        if not isinstance(observations, list):
            raise ValueError("FRED observations must be a list")
        rows: list[dict[str, object]] = []
        seen: set[date] = set()
        for observation in observations:
            try:
                observation_date = date.fromisoformat(str(observation["date"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("FRED payload contains invalid observation date") from exc
            if observation_date in seen:
                raise ValueError("FRED payload contains duplicate observation dates")
            seen.add(observation_date)
            raw = observation.get("value")
            if raw is None or str(raw).strip() in {"", "."}:
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("FRED payload contains invalid observation value") from exc
            if not math.isfinite(value):
                raise ValueError("FRED payload contains non-finite observation value")
            rows.append({"observation_date": observation_date, "value": value})
        if not rows:
            return self._empty_frame()
        frame = pl.DataFrame(
            rows,
            schema={"observation_date": pl.Date, "value": pl.Float64},
        )
        fetched_at = self._clock()
        if fetched_at.tzinfo is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return frame.with_columns(
            pl.lit(series.series_id).alias("series_id"),
            pl.lit(self.provider.value).alias("provider"),
            pl.lit(fetched_at.astimezone(UTC)).alias("fetched_at_utc"),
            pl.lit(series.source_id).alias("source_id"),
            pl.lit(self._endpoint).alias("source_url"),
        ).select(
            "series_id",
            "provider",
            "observation_date",
            "fetched_at_utc",
            "source_id",
            "source_url",
            "value",
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
                "value": pl.Float64,
            }
        )
