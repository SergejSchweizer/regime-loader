"""Rolling geometric-return features derived from positive source levels."""

from __future__ import annotations

from collections.abc import Sequence

import polars as pl

RETURN_WINDOWS: tuple[int, ...] = (10, 25, 60, 120, 240)


def return_feature_columns(
    series_ids: Sequence[str],
    windows: Sequence[int] = RETURN_WINDOWS,
) -> tuple[str, ...]:
    """Return deterministic rolling geometric-return column names."""
    return tuple(
        f"{series_id}_return_geom_{window}obs_pct" for series_id in series_ids for window in windows
    )


def add_geometric_return_features(
    frame: pl.DataFrame,
    *,
    series_id: str,
    level_column: str,
    windows: Sequence[int] = RETURN_WINDOWS,
) -> pl.DataFrame:
    """Append rolling geometric means of one-observation simple returns.

    Source levels must be strictly positive. Invalid/non-positive transitions
    are represented as null, and a feature stays null until its full window is
    available. The result is expressed in percent, matching the requested
    ``100 * (prod(1 + return) ** (1 / window) - 1)`` definition.
    """
    change = f"__{series_id}_return_simple"
    result = frame.with_columns(
        pl.when((pl.col(level_column) > 0) & (pl.col(level_column).shift(1) > 0))
        .then(pl.col(level_column) / pl.col(level_column).shift(1) - 1.0)
        .otherwise(None)
        .alias(change)
    )
    for window in windows:
        if window < 1:
            raise ValueError("return windows must be positive")
        output = f"{series_id}_return_geom_{window}obs_pct"
        result = result.with_columns(
            (
                pl.col(change)
                .log1p()
                .rolling_mean(window_size=window, min_samples=window)
                .mul(window)
                .exp()
                .sub(1.0)
                .mul(100.0)
            ).alias(output)
        )
    return result.drop(change)
