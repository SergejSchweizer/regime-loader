import polars as pl

from application.momentum_features import (
    MOMENTUM_POLICY,
    add_positive_momentum_features,
    momentum_feature_columns,
)


def test_momentum_columns_are_deterministic() -> None:
    assert MOMENTUM_POLICY.lag_windows == ((1, 60), (5, 60), (20, 120))
    assert momentum_feature_columns(("vix",)) == (
        "vix_momentum_autocorr_1_60obs",
        "vix_momentum_autocorr_5_60obs",
        "vix_momentum_autocorr_20_120obs",
    )


def test_positive_autocorrelation_clips_negative_persistence() -> None:
    changes = [1.0 if index % 2 == 0 else 2.0 for index in range(140)]
    values = [0.0]
    for change in changes:
        values.append(values[-1] + change)
    frame = pl.DataFrame({"vix_level": values})

    result = add_positive_momentum_features(
        frame,
        series_id="vix",
        level_column="vix_level",
    )

    assert result.columns == ["vix_level", *momentum_feature_columns(("vix",))]
    assert result.get_column("vix_momentum_autocorr_1_60obs").tail(1).item() == 0.0
    assert result.get_column("vix_momentum_autocorr_5_60obs").tail(1).item() == 0.0
    assert result.get_column("vix_momentum_autocorr_20_120obs").tail(1).item() == 1.0
    assert result.get_column("vix_momentum_autocorr_20_120obs").head(120).null_count() == 120
