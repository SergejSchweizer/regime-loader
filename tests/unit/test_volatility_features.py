from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from application.silver import SILVER_SCHEMA
from application.volatility_features import (
    VOLATILITY_SERIES,
    VolatilityFeaturePolicy,
    build_volatility_features,
)

START = date(2026, 1, 1)


def _silver(series_id: str, values: list[float], *, skip: set[int] | None = None) -> pl.DataFrame:
    skip = skip or set()
    rows = [(index, value) for index, value in enumerate(values) if index not in skip]
    return pl.DataFrame(
        {
            "observation_date": [START + timedelta(days=index * 2) for index, _ in rows],
            "series_id": [series_id for _ in rows],
            "value": [value for _, value in rows],
            "open": [None for _ in rows],
            "high": [None for _ in rows],
            "low": [None for _ in rows],
            "close": [None for _ in rows],
            "unit": ["index_points" for _ in rows],
            "provider": ["fixture" for _ in rows],
            "source_id": [series_id for _ in rows],
            "fetched_at_utc": [datetime(2026, 8, 19, 2, tzinfo=UTC) for _ in rows],
        },
        schema=SILVER_SCHEMA,
    )


def _all_linear(length: int = 61) -> dict[str, pl.DataFrame]:
    return {
        series_id: _silver(series_id, [float(index + 1) for index in range(length)])
        for series_id in VOLATILITY_SERIES
    }


def test_observation_lags_and_population_zscore_use_valid_rows_not_calendar_days() -> None:
    result = build_volatility_features(_all_linear())
    assert result.height == 61
    assert result.schema["timestamp_m1"] == pl.Datetime("us", "UTC")
    assert result.get_column("timestamp_m1")[0] == datetime(2026, 1, 1, tzinfo=UTC)
    assert result.get_column("vix_delta_1obs")[0] is None
    assert result.get_column("vix_delta_1obs")[1] == pytest.approx(1.0)
    assert result.get_column("vix_delta_5obs")[:5].null_count() == 5
    assert result.get_column("vix_delta_5obs")[5] == pytest.approx(5.0)
    assert result.get_column("vix_delta_20obs")[:20].null_count() == 20
    assert result.get_column("vix_delta_20obs")[20] == pytest.approx(20.0)
    assert result.get_column("vix_zscore_60obs")[:59].null_count() == 59
    assert result.get_column("vix_zscore_60obs")[59] is not None


def test_zero_variance_zscore_is_null_and_ratio_denominator_must_be_positive() -> None:
    frames = _all_linear()
    frames["move"] = _silver("move", [7.0] * 61)
    frames["vix"] = _silver("vix", [0.0] * 61)
    frames["vix9d"] = _silver("vix9d", [10.0] * 61)
    result = build_volatility_features(frames)
    assert result.get_column("move_zscore_60obs")[59] is None
    assert result.get_column("vix9d_vix_ratio").null_count() == result.height


def test_term_structure_is_same_timestamp_only_without_fill() -> None:
    frames = _all_linear(25)
    frames["vix"] = _silver("vix", [20.0] * 25)
    frames["vix9d"] = _silver("vix9d", [10.0] * 25)
    frames["vix3m"] = _silver("vix3m", [40.0] * 25, skip={10})
    frames["vix6m"] = _silver("vix6m", [30.0] * 25)
    frames["vix1y"] = _silver("vix1y", [50.0] * 25)
    result = build_volatility_features(frames)
    timestamp = datetime.combine(START + timedelta(days=20), datetime.min.time(), tzinfo=UTC)
    row = result.filter(pl.col("timestamp_m1") == timestamp)
    assert row.item(0, "vix9d_vix_ratio") == pytest.approx(0.5)
    assert row.item(0, "vix3m_level") is None
    assert row.item(0, "vix_vix3m_ratio") is None
    assert row.item(0, "vix3m_minus_vix") is None
    assert row.item(0, "vix6m_minus_vix") == pytest.approx(10.0)
    assert row.item(0, "vix1y_minus_vix") == pytest.approx(30.0)


def test_sparse_source_still_uses_previous_nth_valid_observation() -> None:
    frames = _all_linear(30)
    values = [float(index) for index in range(30)]
    frames["vix"] = _silver("vix", values, skip={2, 4, 6})
    result = build_volatility_features(frames)
    vix_rows = result.filter(pl.col("vix_level").is_not_null())
    assert vix_rows.get_column("vix_delta_5obs")[5] == pytest.approx(8.0)


def test_contract_validation_and_policy_are_fail_closed() -> None:
    frames = _all_linear(10)
    del frames["move"]
    with pytest.raises(KeyError, match="move"):
        build_volatility_features(frames)
    frames = _all_linear(10)
    frames["vix"] = frames["vix"].drop("unit")
    with pytest.raises(ValueError, match="schema mismatch"):
        build_volatility_features(frames)
    frames = _all_linear(10)
    frames["vix"] = frames["vix"].with_columns(pl.lit("wrong").alias("series_id"))
    with pytest.raises(ValueError, match="identity mismatch"):
        build_volatility_features(frames)
    with pytest.raises(ValueError, match="fixed at 1, 5, and 20"):
        VolatilityFeaturePolicy(delta_lags=(4, 20))
    with pytest.raises(ValueError, match="window=60"):
        VolatilityFeaturePolicy(zscore_window=20)


def test_duplicate_null_and_nonfinite_silver_values_are_rejected() -> None:
    frames = _all_linear(10)
    frames["vix"] = pl.concat([frames["vix"], frames["vix"].head(1)])
    with pytest.raises(ValueError, match="unique"):
        build_volatility_features(frames)

    frames = _all_linear(10)
    frames["vix"] = frames["vix"].with_columns(
        pl.when(pl.arange(0, pl.len()) == 0).then(None).otherwise(pl.col("value")).alias("value")
    )
    with pytest.raises(ValueError, match="cannot be null"):
        build_volatility_features(frames)

    frames = _all_linear(10)
    frames["vix"] = frames["vix"].with_columns(pl.lit(float("inf")).alias("value"))
    with pytest.raises(ValueError, match="finite"):
        build_volatility_features(frames)
