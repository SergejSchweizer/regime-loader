"""Deterministic positive return-persistence (momentum) feature strategy."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import polars as pl


@dataclass(frozen=True, slots=True)
class MomentumFeaturePolicy:
    """Rolling lag/window policy for positive autocorrelation momentum."""

    lag_windows: tuple[tuple[int, int], ...] = ((1, 60), (5, 60), (20, 120))
    minimum_change_lag: int = 1
    ddof: int = 0

    def __post_init__(self) -> None:
        if self.lag_windows != ((1, 60), (5, 60), (20, 120)):
            raise ValueError("momentum lag/windows are fixed at (1, 60), (5, 60), and (20, 120)")
        if self.minimum_change_lag != 1:
            raise ValueError("momentum uses one-observation source changes")
        if self.ddof != 0:
            raise ValueError("momentum autocorrelation uses population standard deviation")


MOMENTUM_POLICY = MomentumFeaturePolicy()


def momentum_feature_columns(
    series_ids: Sequence[str],
    policy: MomentumFeaturePolicy = MOMENTUM_POLICY,
) -> tuple[str, ...]:
    """Return canonical momentum columns in deterministic series/horizon order."""
    return tuple(
        f"{series_id}_momentum_autocorr_{lag}_{window}obs"
        for series_id in series_ids
        for lag, window in policy.lag_windows
    )


def add_positive_momentum_features(
    frame: pl.DataFrame,
    *,
    series_id: str,
    level_column: str,
    policy: MomentumFeaturePolicy = MOMENTUM_POLICY,
) -> pl.DataFrame:
    """Append positive autocorrelation of one-observation source changes.

    The source series is already sorted and contains only valid observations. The
    one-observation change is therefore a return-like persistence input for all
    current market, macro, rate, and stress series. Negative correlations and
    undefined correlations are represented as zero and null respectively.
    """
    change = f"__{series_id}_momentum_change"
    result = frame.with_columns(
        (pl.col(level_column) - pl.col(level_column).shift(policy.minimum_change_lag)).alias(change)
    )
    temporary: list[str] = [change]
    for lag, window in policy.lag_windows:
        lagged = f"__{series_id}_momentum_lag_{lag}"
        mean_x = f"__{series_id}_momentum_mean_x_{lag}"
        mean_y = f"__{series_id}_momentum_mean_y_{lag}"
        mean_xy = f"__{series_id}_momentum_mean_xy_{lag}"
        std_x = f"__{series_id}_momentum_std_x_{lag}"
        std_y = f"__{series_id}_momentum_std_y_{lag}"
        temporary.extend((lagged, mean_x, mean_y, mean_xy, std_x, std_y))
        result = result.with_columns(
            pl.col(change).shift(lag).alias(lagged),
            pl.col(change).rolling_mean(window_size=window, min_samples=window).alias(mean_x),
            pl.col(change)
            .shift(lag)
            .rolling_mean(window_size=window, min_samples=window)
            .alias(mean_y),
            (pl.col(change) * pl.col(change).shift(lag))
            .rolling_mean(window_size=window, min_samples=window)
            .alias(mean_xy),
            pl.col(change)
            .rolling_std(window_size=window, min_samples=window, ddof=policy.ddof)
            .alias(std_x),
            pl.col(change)
            .shift(lag)
            .rolling_std(window_size=window, min_samples=window, ddof=policy.ddof)
            .alias(std_y),
        )
        covariance = pl.col(mean_xy) - pl.col(mean_x) * pl.col(mean_y)
        denominator = pl.col(std_x) * pl.col(std_y)
        correlation = covariance / denominator
        output = f"{series_id}_momentum_autocorr_{lag}_{window}obs"
        result = result.with_columns(
            pl.when(correlation.is_null() | ~correlation.is_finite())
            .then(None)
            .when(correlation > 0)
            .then(correlation)
            .otherwise(pl.lit(0.0))
            .alias(output)
        )
    return result.drop(temporary)
