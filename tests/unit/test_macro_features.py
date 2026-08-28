from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from application.macro_features import MACRO_SERIES, MacroFeaturePolicy, build_macro_features
from application.silver import SILVER_SCHEMA

START = date(2026, 1, 1)


def _silver(series_id: str, values: list[float], *, skip: set[int] | None = None) -> pl.DataFrame:
    skip = skip or set()
    rows = [(index, value) for index, value in enumerate(values) if index not in skip]
    return pl.DataFrame(
        {
            "observation_date": [START + timedelta(days=index * 3) for index, _ in rows],
            "series_id": [series_id for _ in rows],
            "value": [value for _, value in rows],
            "open": [None for _ in rows],
            "high": [None for _ in rows],
            "low": [None for _ in rows],
            "close": [None for _ in rows],
            "unit": ["fixture" for _ in rows],
            "provider": ["fixture" for _ in rows],
            "source_id": [series_id for _ in rows],
            "fetched_at_utc": [datetime(2026, 8, 19, 2, tzinfo=UTC) for _ in rows],
        },
        schema=SILVER_SCHEMA,
    )


def _all_linear(length: int = 25) -> dict[str, pl.DataFrame]:
    return {
        series_id: _silver(series_id, [float(index + 1) for index in range(length)])
        for series_id in MACRO_SERIES
    }


def test_exact_macro_columns_and_observation_lags() -> None:
    result = build_macro_features(_all_linear())
    assert result.schema["timestamp_m1"] == pl.Datetime("us", "UTC")
    assert result.get_column("timestamp_m1")[0] == datetime(2026, 1, 1, tzinfo=UTC)
    for series_id in MACRO_SERIES:
        assert result.get_column(f"{series_id}_delta_1obs")[0] is None
        assert result.get_column(f"{series_id}_delta_1obs")[1] == pytest.approx(1.0)
    for series_id in ("ciss", "euro_hy_oas"):
        assert result.get_column(f"{series_id}_delta_5obs")[:5].null_count() == 5
        assert result.get_column(f"{series_id}_delta_5obs")[5] == pytest.approx(5.0)
        assert result.get_column(f"{series_id}_delta_20obs")[20] == pytest.approx(20.0)
    for series_id in ("us_2y", "us_10y", "estr", "usd_broad"):
        assert result.get_column(f"{series_id}_delta_20obs")[:20].null_count() == 20
        assert result.get_column(f"{series_id}_delta_20obs")[20] == pytest.approx(20.0)
    assert "ciss_delta_5obs" in result.columns
    assert "estr_delta_5obs" not in result.columns


def test_yield_spread_is_same_timestamp_only_without_fill() -> None:
    frames = _all_linear()
    frames["us_2y"] = _silver("us_2y", [2.0] * 25)
    frames["us_10y"] = _silver("us_10y", [5.0] * 25, skip={7})
    result = build_macro_features(frames)
    present_time = datetime.combine(START, datetime.min.time(), tzinfo=UTC)
    present = result.filter(pl.col("timestamp_m1") == present_time)
    assert present.item(0, "us_10y_minus_us_2y") == pytest.approx(3.0)
    missing_time = datetime.combine(START + timedelta(days=21), datetime.min.time(), tzinfo=UTC)
    missing = result.filter(pl.col("timestamp_m1") == missing_time)
    assert missing.item(0, "us_10y_level") is None
    assert missing.item(0, "us_10y_minus_us_2y") is None


def test_sparse_series_uses_previous_nth_valid_observation() -> None:
    frames = _all_linear(30)
    frames["ciss"] = _silver("ciss", [float(i) for i in range(30)], skip={1, 3, 5})
    result = build_macro_features(frames)
    rows = result.filter(pl.col("ciss_level").is_not_null())
    assert rows.get_column("ciss_delta_5obs")[5] == pytest.approx(8.0)
    assert rows.get_column("ciss_delta_20obs")[20] == pytest.approx(23.0)


def test_no_calendar_rows_are_synthesized() -> None:
    frames = _all_linear(4)
    result = build_macro_features(frames)
    assert result.height == 4
    assert result.get_column("timestamp_m1").to_list() == [
        datetime.combine(START + timedelta(days=3 * i), datetime.min.time(), tzinfo=UTC)
        for i in range(4)
    ]


def test_contract_validation_and_policy_are_fail_closed() -> None:
    frames = _all_linear()
    del frames["estr"]
    with pytest.raises(KeyError, match="estr"):
        build_macro_features(frames)
    frames = _all_linear()
    frames["ciss"] = frames["ciss"].drop("unit")
    with pytest.raises(ValueError, match="schema mismatch"):
        build_macro_features(frames)
    frames = _all_linear()
    frames["ciss"] = frames["ciss"].with_columns(pl.lit("wrong").alias("series_id"))
    with pytest.raises(ValueError, match="identity mismatch"):
        build_macro_features(frames)
    with pytest.raises(ValueError, match="fixed at 1, 5, and 20"):
        MacroFeaturePolicy(short_lag=4)


def test_duplicate_null_and_nonfinite_values_are_rejected() -> None:
    frames = _all_linear()
    frames["ciss"] = pl.concat([frames["ciss"], frames["ciss"].head(1)])
    with pytest.raises(ValueError, match="unique"):
        build_macro_features(frames)

    frames = _all_linear()
    frames["ciss"] = frames["ciss"].with_columns(
        pl.when(pl.arange(0, pl.len()) == 0).then(None).otherwise(pl.col("value")).alias("value")
    )
    with pytest.raises(ValueError, match="cannot be null"):
        build_macro_features(frames)

    frames = _all_linear()
    frames["ciss"] = frames["ciss"].with_columns(pl.lit(float("inf")).alias("value"))
    with pytest.raises(ValueError, match="finite"):
        build_macro_features(frames)
