from __future__ import annotations

import statistics
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from application.gold_frame import (
    GOLD_COLUMNS,
    GOLD_FEATURE_VERSION,
    GOLD_SCHEMA_VERSION,
    GOLD_SOURCE_SERIES,
    MACRO_FEATURE_COLUMNS,
    VOLATILITY_FEATURE_COLUMNS,
    GoldSemanticVersions,
    assemble_gold_frame,
)
from application.macro_features import MACRO_SERIES, build_macro_features
from application.silver import SILVER_SCHEMA
from application.volatility_features import VOLATILITY_SERIES, build_volatility_features

START = date(2026, 1, 1)


def _silver(series_id: str, values: list[float]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "observation_date": [START + timedelta(days=index) for index in range(len(values))],
            "series_id": [series_id for _ in values],
            "value": values,
            "open": [None for _ in values],
            "high": [None for _ in values],
            "low": [None for _ in values],
            "close": [None for _ in values],
            "unit": ["fixture" for _ in values],
            "provider": ["fixture" for _ in values],
            "source_id": [series_id for _ in values],
            "fetched_at_utc": [datetime(2026, 8, 19, 2, tzinfo=UTC) for _ in values],
        },
        schema=SILVER_SCHEMA,
    )


def _all_silver(length: int = 61) -> dict[str, pl.DataFrame]:
    return {
        series_id: _silver(series_id, [float(index + 1) for index in range(length)])
        for series_id in GOLD_SOURCE_SERIES
    }


def _feature_frame(
    columns: tuple[str, ...], timestamps: list[datetime], base: float
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp_m1": timestamps,
            **{column: [base + index for index in range(len(timestamps))] for column in columns},
        },
        schema={
            "timestamp_m1": pl.Datetime("us", "UTC"),
            **{column: pl.Float64 for column in columns},
        },
    )


def test_outer_union_exact_order_versions_and_provenance() -> None:
    t1 = datetime(2026, 1, 1, tzinfo=UTC)
    t2 = datetime(2026, 1, 2, tzinfo=UTC)
    t3 = datetime(2026, 1, 3, tzinfo=UTC)
    volatility = _feature_frame(VOLATILITY_FEATURE_COLUMNS, [t1, t2], 1.0)
    macro = _feature_frame(MACRO_FEATURE_COLUMNS, [t2, t3], 10.0)
    build = assemble_gold_frame(volatility, macro, _all_silver(3))
    assert build.frame.columns == list(GOLD_COLUMNS)
    assert build.frame.get_column("timestamp_m1").to_list() == [t1, t2, t3]
    assert build.frame.filter(pl.col("timestamp_m1") == t1).item(0, "ciss_level") is None
    assert build.frame.filter(pl.col("timestamp_m1") == t3).item(0, "vix_level") is None
    assert build.schema_version == GOLD_SCHEMA_VERSION == 2
    assert build.feature_version == GOLD_FEATURE_VERSION == 1
    assert [item.series_id for item in build.inputs] == list(GOLD_SOURCE_SERIES)
    assert all(len(item.sha256) == 64 for item in build.inputs)
    assert "observation_date" not in build.frame.columns


def test_real_feature_build_matches_hand_calculated_known_timestamp() -> None:
    silver = _all_silver()
    volatility = build_volatility_features({key: silver[key] for key in VOLATILITY_SERIES})
    macro = build_macro_features({key: silver[key] for key in MACRO_SERIES})
    build = assemble_gold_frame(volatility, macro, silver)
    timestamp = datetime(2026, 3, 1, tzinfo=UTC)
    row = build.frame.filter(pl.col("timestamp_m1") == timestamp)
    assert row.height == 1
    assert row.item(0, "vix_level") == pytest.approx(60.0)
    assert row.item(0, "vix_delta_1obs") == pytest.approx(1.0)
    assert row.item(0, "vix_delta_5obs") == pytest.approx(5.0)
    assert row.item(0, "vix_delta_20obs") == pytest.approx(20.0)
    expected_z = (60.0 - statistics.mean(range(1, 61))) / statistics.pstdev(range(1, 61))
    assert row.item(0, "vix_zscore_60obs") == pytest.approx(expected_z)
    assert row.item(0, "vix9d_vix_ratio") == pytest.approx(1.0)
    assert row.item(0, "vix3m_minus_vix") == pytest.approx(0.0)
    assert row.item(0, "us_10y_minus_us_2y") == pytest.approx(0.0)


def test_later_input_perturbation_cannot_change_earlier_gold_row() -> None:
    baseline_silver = _all_silver()
    baseline = assemble_gold_frame(
        build_volatility_features({key: baseline_silver[key] for key in VOLATILITY_SERIES}),
        build_macro_features({key: baseline_silver[key] for key in MACRO_SERIES}),
        baseline_silver,
    )
    changed_silver = dict(baseline_silver)
    changed_silver["vix"] = changed_silver["vix"].with_columns(
        pl.when(pl.col("observation_date") == START + timedelta(days=60))
        .then(pl.col("value") + 1000.0)
        .otherwise(pl.col("value"))
        .alias("value")
    )
    changed = assemble_gold_frame(
        build_volatility_features({key: changed_silver[key] for key in VOLATILITY_SERIES}),
        build_macro_features({key: changed_silver[key] for key in MACRO_SERIES}),
        changed_silver,
    )
    earlier = datetime(2026, 2, 9, tzinfo=UTC)
    baseline_row = baseline.frame.filter(pl.col("timestamp_m1") == earlier)
    changed_row = changed.frame.filter(pl.col("timestamp_m1") == earlier)
    assert baseline_row.equals(changed_row)
    baseline_vix_sig = next(item for item in baseline.inputs if item.series_id == "vix")
    changed_vix_sig = next(item for item in changed.inputs if item.series_id == "vix")
    assert baseline_vix_sig.sha256 != changed_vix_sig.sha256


def test_nan_is_normalized_to_null_and_infinity_is_rejected() -> None:
    timestamp = [datetime(2026, 1, 1, tzinfo=UTC)]
    volatility = _feature_frame(VOLATILITY_FEATURE_COLUMNS, timestamp, 1.0)
    macro = _feature_frame(MACRO_FEATURE_COLUMNS, timestamp, 1.0)
    volatility = volatility.with_columns(pl.lit(float("nan")).alias("vix_level"))
    build = assemble_gold_frame(volatility, macro, _all_silver(1))
    assert build.frame.item(0, "vix_level") is None
    macro = macro.with_columns(pl.lit(float("inf")).alias("ciss_level"))
    with pytest.raises(ValueError, match="non-finite canonical Gold"):
        assemble_gold_frame(volatility, macro, _all_silver(1))


def test_feature_schema_versions_and_provenance_are_fail_closed() -> None:
    timestamp = [datetime(2026, 1, 1, tzinfo=UTC)]
    volatility = _feature_frame(VOLATILITY_FEATURE_COLUMNS, timestamp, 1.0)
    macro = _feature_frame(MACRO_FEATURE_COLUMNS, timestamp, 1.0)
    with pytest.raises(ValueError, match="schema/order"):
        assemble_gold_frame(volatility.drop("vix_level"), macro, _all_silver(1))
    with pytest.raises(TypeError, match="Float64"):
        assemble_gold_frame(
            volatility.with_columns(pl.col("vix_level").cast(pl.Int64)),
            macro,
            _all_silver(1),
        )
    silver = _all_silver(1)
    del silver["move"]
    with pytest.raises(KeyError, match="move"):
        assemble_gold_frame(volatility, macro, silver)
    silver = _all_silver(1)
    silver["vix"] = silver["vix"].with_columns(pl.lit("wrong").alias("series_id"))
    with pytest.raises(ValueError, match="identity mismatch"):
        assemble_gold_frame(volatility, macro, silver)
    with pytest.raises(ValueError, match="schema_version"):
        GoldSemanticVersions(schema_version=1)
    with pytest.raises(ValueError, match="feature_version"):
        GoldSemanticVersions(feature_version=2)


def test_provenance_signature_is_deterministic() -> None:
    timestamp = [datetime(2026, 1, 1, tzinfo=UTC)]
    volatility = _feature_frame(VOLATILITY_FEATURE_COLUMNS, timestamp, 1.0)
    macro = _feature_frame(MACRO_FEATURE_COLUMNS, timestamp, 1.0)
    silver = _all_silver(2)
    first = assemble_gold_frame(volatility, macro, silver)
    second = assemble_gold_frame(volatility, macro, silver)
    assert first.inputs == second.inputs
    assert first.frame.equals(second.frame)
