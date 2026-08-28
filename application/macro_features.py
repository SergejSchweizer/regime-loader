"""Deterministic macro and rates Gold feature Strategy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import polars as pl

from application.silver import SILVER_SCHEMA

MACRO_SERIES = ("ciss", "euro_hy_oas", "us_2y", "us_10y", "estr", "usd_broad")


@dataclass(frozen=True, slots=True)
class MacroFeaturePolicy:
    """Source-controlled observation-lag policy for macro features."""

    immediate_lag: int = 1
    short_lag: int = 5
    long_lag: int = 20

    def __post_init__(self) -> None:
        if (self.immediate_lag, self.short_lag, self.long_lag) != (1, 5, 20):
            raise ValueError("macro observation lags are fixed at 1, 5, and 20")


MACRO_POLICY = MacroFeaturePolicy()


def macro_delta_lags(policy: MacroFeaturePolicy) -> dict[str, tuple[int, ...]]:
    """Return the observation-lag contract executed for every macro source series."""
    return {
        "ciss": (policy.immediate_lag, policy.short_lag, policy.long_lag),
        "euro_hy_oas": (policy.immediate_lag, policy.short_lag, policy.long_lag),
        "us_2y": (policy.immediate_lag, policy.long_lag),
        "us_10y": (policy.immediate_lag, policy.long_lag),
        "estr": (policy.immediate_lag, policy.long_lag),
        "usd_broad": (policy.immediate_lag, policy.long_lag),
    }


def _timestamp_expression() -> pl.Expr:
    return (
        pl.col("observation_date")
        .cast(pl.Datetime("us"))
        .dt.replace_time_zone("UTC")
        .alias("timestamp_m1")
    )


def _validate_silver(series_id: str, frame: pl.DataFrame) -> pl.DataFrame:
    if frame.schema != SILVER_SCHEMA:
        raise ValueError(f"{series_id} Silver schema mismatch")
    identities = frame.get_column("series_id").unique().to_list()
    if identities and identities != [series_id]:
        raise ValueError(f"{series_id} Silver identity mismatch")
    if frame.get_column("value").null_count():
        raise ValueError(f"{series_id} Silver value cannot be null")
    if not bool(frame.get_column("value").is_finite().all()):
        raise ValueError(f"{series_id} Silver value must be finite")
    if bool(frame.get_column("observation_date").is_duplicated().any()):
        raise ValueError(f"{series_id} Silver observation dates must be unique")
    return frame.sort("observation_date")


def _series_frame(
    series_id: str,
    silver: pl.DataFrame,
    lags: tuple[int, ...],
) -> pl.DataFrame:
    level = f"{series_id}_level"
    frame = _validate_silver(series_id, silver).select(
        _timestamp_expression(), pl.col("value").alias(level)
    )
    expressions = [
        (pl.col(level) - pl.col(level).shift(lag)).alias(f"{series_id}_delta_{lag}obs")
        for lag in lags
    ]
    return frame.with_columns(expressions)


def _outer_join(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame(schema={"timestamp_m1": pl.Datetime("us", "UTC")})
    result = frames[0]
    for frame in frames[1:]:
        result = result.join(frame, on="timestamp_m1", how="full", coalesce=True)
    return result.sort("timestamp_m1")


def build_macro_features(
    silver_by_series: Mapping[str, pl.DataFrame],
    *,
    policy: MacroFeaturePolicy = MACRO_POLICY,
) -> pl.DataFrame:
    """Build exact macro feature family without fill, interpolation, or calendar synthesis."""
    missing = [series_id for series_id in MACRO_SERIES if series_id not in silver_by_series]
    if missing:
        raise KeyError(f"missing Silver macro series: {', '.join(missing)}")
    lags_by_series = macro_delta_lags(policy)
    frames = [
        _series_frame(series_id, silver_by_series[series_id], lags)
        for series_id, lags in lags_by_series.items()
    ]
    joined = _outer_join(frames).with_columns(
        (pl.col("us_10y_level") - pl.col("us_2y_level")).alias("us_10y_minus_us_2y")
    )
    numeric = [column for column in joined.columns if column != "timestamp_m1"]
    joined = joined.with_columns([pl.col(column).fill_nan(None) for column in numeric])
    for column in numeric:
        non_null = joined.get_column(column).drop_nulls()
        if non_null.len() and not bool(non_null.is_finite().all()):
            raise ValueError(f"non-finite macro feature: {column}")
    if joined.schema["timestamp_m1"] != pl.Datetime("us", "UTC"):
        raise TypeError("timestamp_m1 must be Datetime(us, UTC)")
    if bool(joined.get_column("timestamp_m1").is_duplicated().any()):
        raise ValueError("timestamp_m1 must be unique")
    return joined
