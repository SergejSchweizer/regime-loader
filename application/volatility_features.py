"""Deterministic volatility and term-structure Gold feature Strategy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import polars as pl

from application.silver import SILVER_SCHEMA

VOLATILITY_SERIES = ("vix", "vix9d", "vix3m", "vix6m", "vix1y", "vstoxx", "move")


@dataclass(frozen=True, slots=True)
class VolatilityFeaturePolicy:
    """Source-controlled observation-lag and rolling-window policy."""

    delta_lags: tuple[int, int, int] = (1, 5, 20)
    zscore_window: int = 60
    zscore_ddof: int = 0

    def __post_init__(self) -> None:
        if self.delta_lags != (1, 5, 20):
            raise ValueError("volatility delta lags are fixed at 1, 5, and 20 observations")
        if self.zscore_window != 60 or self.zscore_ddof != 0:
            raise ValueError("volatility z-score policy is fixed at window=60 and ddof=0")


VOLATILITY_POLICY = VolatilityFeaturePolicy()


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


def _series_features(
    series_id: str,
    silver: pl.DataFrame,
    policy: VolatilityFeaturePolicy,
) -> pl.DataFrame:
    frame = _validate_silver(series_id, silver).select(
        _timestamp_expression(), pl.col("value").alias(f"{series_id}_level")
    )
    level = f"{series_id}_level"
    mean = f"__{series_id}_mean60"
    std = f"__{series_id}_std60"
    return (
        frame.with_columns(
            (pl.col(level) - pl.col(level).shift(1)).alias(f"{series_id}_delta_1obs"),
            (pl.col(level) - pl.col(level).shift(5)).alias(f"{series_id}_delta_5obs"),
            (pl.col(level) - pl.col(level).shift(20)).alias(f"{series_id}_delta_20obs"),
            pl.col(level)
            .rolling_mean(window_size=policy.zscore_window, min_samples=policy.zscore_window)
            .alias(mean),
            pl.col(level)
            .rolling_std(
                window_size=policy.zscore_window,
                min_samples=policy.zscore_window,
                ddof=policy.zscore_ddof,
            )
            .alias(std),
        )
        .with_columns(
            pl.when(pl.col(std) > 0)
            .then((pl.col(level) - pl.col(mean)) / pl.col(std))
            .otherwise(None)
            .alias(f"{series_id}_zscore_60obs")
        )
        .drop(mean, std)
    )


def _outer_join(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame(schema={"timestamp_m1": pl.Datetime("us", "UTC")})
    result = frames[0]
    for frame in frames[1:]:
        result = result.join(frame, on="timestamp_m1", how="full", coalesce=True)
    return result.sort("timestamp_m1")


def _safe_ratio(numerator: str, denominator: str, output: str) -> pl.Expr:
    return (
        pl.when(pl.col(denominator).is_not_null() & (pl.col(denominator) > 0))
        .then(pl.col(numerator) / pl.col(denominator))
        .otherwise(None)
        .alias(output)
    )


def build_volatility_features(
    silver_by_series: Mapping[str, pl.DataFrame],
    *,
    policy: VolatilityFeaturePolicy = VOLATILITY_POLICY,
) -> pl.DataFrame:
    """Build exact volatility feature family without fill or calendar imputation."""
    missing = [series_id for series_id in VOLATILITY_SERIES if series_id not in silver_by_series]
    if missing:
        raise KeyError(f"missing Silver volatility series: {', '.join(missing)}")
    joined = _outer_join(
        [
            _series_features(series_id, silver_by_series[series_id], policy)
            for series_id in VOLATILITY_SERIES
        ]
    )
    joined = joined.with_columns(
        _safe_ratio("vix9d_level", "vix_level", "vix9d_vix_ratio"),
        _safe_ratio("vix_level", "vix3m_level", "vix_vix3m_ratio"),
        (pl.col("vix3m_level") - pl.col("vix_level")).alias("vix3m_minus_vix"),
        (pl.col("vix6m_level") - pl.col("vix_level")).alias("vix6m_minus_vix"),
        (pl.col("vix1y_level") - pl.col("vix_level")).alias("vix1y_minus_vix"),
    )
    numeric = [column for column in joined.columns if column != "timestamp_m1"]
    joined = joined.with_columns([pl.col(column).fill_nan(None) for column in numeric])
    for column in numeric:
        non_null = joined.get_column(column).drop_nulls()
        if non_null.len() and not bool(non_null.is_finite().all()):
            raise ValueError(f"non-finite volatility feature: {column}")
    if joined.schema["timestamp_m1"] != pl.Datetime("us", "UTC"):
        raise TypeError("timestamp_m1 must be Datetime(us, UTC)")
    if bool(joined.get_column("timestamp_m1").is_duplicated().any()):
        raise ValueError("timestamp_m1 must be unique")
    return joined
