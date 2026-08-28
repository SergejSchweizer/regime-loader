"""ECB Data Portal adapter for CISS and €STR regime series."""

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
_DEFAULT_BASE_URL = "https://data-api.ecb.europa.eu/service/data"
_ECB_SERIES = frozenset({"ciss", "estr"})


def _system_utc_now() -> datetime:
    return datetime.now(UTC)


class EcbProvider:
    """Adapter translating ECB SDMX CSV data into canonical scalar Bronze."""

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
        return Provider.ECB

    def fetch(self, series: SeriesContract, request: ProviderRequest) -> pl.DataFrame:
        self._validate_contract(series)
        flow, key = series.source_id.split(".", 1)
        url = f"{self._base_url}/{flow}/{key}"
        params: dict[str, str | int | float] = {"format": "csvdata", "detail": "dataonly"}
        if request.operation == "update":
            if request.maximum_history or request.logical_start is None:
                raise ValueError("ECB update requires exact bounded dates")
            params["startPeriod"] = request.logical_start.isoformat()
            params["endPeriod"] = request.logical_end.isoformat()
        else:
            params["endPeriod"] = request.logical_end.isoformat()
        context = RequestContext(self.provider, series.series_id, series.source_id)
        response = self._transport.send(HttpRequest("GET", url, params=params), context=context)
        if response.status_code != 200:
            if self._is_no_result(response.status_code, response.content):
                return self._empty_frame()
            raise ProviderHttpError(
                context=context,
                category="source_unavailable",
                request_path=url,
                status_code=response.status_code,
            )
        frame = self._parse(series, response.content, url)
        if request.operation == "update" and frame.height:
            assert request.logical_start is not None
            if frame.filter(
                ~pl.col("observation_date").is_between(
                    request.logical_start,
                    request.logical_end,
                    closed="both",
                )
            ).height:
                raise ValueError("ECB bounded response contains out-of-window observations")
        return frame.sort("observation_date")

    @staticmethod
    def _is_no_result(status_code: int, content: bytes) -> bool:
        if status_code not in {404, 406}:
            return False
        text = content.decode(errors="ignore").lower()
        return "no record" in text or "no result" in text or "no data" in text

    def _validate_contract(self, series: SeriesContract) -> None:
        if series.provider is not self.provider or series.series_id not in _ECB_SERIES:
            raise ValueError("unsupported ECB series contract")
        if series.native_shape is not NativeShape.SCALAR:
            raise ValueError("ECB regime series must use scalar Bronze shape")
        if series.fetch_capability is not FetchCapability.DATE_RANGE:
            raise ValueError("ECB regime series must use date_range capability")
        if "." not in series.source_id:
            raise ValueError("ECB source_id must contain dataflow and series key")

    def _parse(self, series: SeriesContract, content: bytes, source_url: str) -> pl.DataFrame:
        if not content.strip():
            return self._empty_frame()
        try:
            raw = pl.read_csv(BytesIO(content), infer_schema_length=1000)
        except Exception as exc:
            raise ValueError("invalid ECB CSV payload") from exc
        raw = raw.rename({name: name.strip().upper() for name in raw.columns})
        if "TIME_PERIOD" not in raw.columns or "OBS_VALUE" not in raw.columns:
            raise ValueError("ECB payload is missing TIME_PERIOD or OBS_VALUE")
        frame = raw.select(
            pl.col("TIME_PERIOD")
            .cast(pl.String)
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .alias("observation_date"),
            pl.col("OBS_VALUE").cast(pl.String).str.strip_chars().alias("raw_value"),
        )
        if bool(frame.select(pl.col("observation_date").is_null().any()).item()):
            raise ValueError("ECB payload contains invalid observation dates")
        if bool(frame.select(pl.col("observation_date").is_duplicated().any()).item()):
            raise ValueError("ECB payload contains duplicate observation dates")
        missing_value = pl.col("raw_value").is_null() | pl.col("raw_value").is_in(["", "."])
        parsed_value = pl.col("raw_value").cast(pl.Float64, strict=False)
        invalid_value = ~missing_value & (parsed_value.is_null() | ~parsed_value.is_finite())
        if bool(frame.select(invalid_value.any()).item()):
            raise ValueError("ECB payload contains invalid observation value")
        frame = frame.filter(~missing_value).with_columns(parsed_value.alias("value"))
        frame = frame.drop("raw_value")
        if not frame.height:
            return self._empty_frame()
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
