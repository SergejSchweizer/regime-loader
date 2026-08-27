"""Canonical storage-neutral Gold frame assembly and provenance contracts."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date

import polars as pl

from application.registry import SERIES_REGISTRY
from application.silver import SILVER_SCHEMA
from application.volatility_features import VOLATILITY_SERIES

GOLD_SCHEMA_VERSION = 2
GOLD_FEATURE_VERSION = 1
GOLD_SOURCE_SERIES = tuple(SERIES_REGISTRY)

VOLATILITY_FEATURE_COLUMNS = tuple(
    column
    for series_id in VOLATILITY_SERIES
    for column in (
        f"{series_id}_level",
        f"{series_id}_delta_1obs",
        f"{series_id}_delta_5obs",
        f"{series_id}_delta_20obs",
        f"{series_id}_zscore_60obs",
    )
) + (
    "vix9d_vix_ratio",
    "vix_vix3m_ratio",
    "vix3m_minus_vix",
    "vix6m_minus_vix",
    "vix1y_minus_vix",
)

MACRO_FEATURE_COLUMNS = (
    "ciss_level",
    "ciss_delta_1obs",
    "ciss_delta_5obs",
    "ciss_delta_20obs",
    "euro_hy_oas_level",
    "euro_hy_oas_delta_1obs",
    "euro_hy_oas_delta_5obs",
    "euro_hy_oas_delta_20obs",
    "us_2y_level",
    "us_2y_delta_1obs",
    "us_2y_delta_20obs",
    "us_10y_level",
    "us_10y_delta_1obs",
    "us_10y_delta_20obs",
    "estr_level",
    "estr_delta_1obs",
    "estr_delta_20obs",
    "usd_broad_level",
    "usd_broad_delta_1obs",
    "usd_broad_delta_20obs",
    "us_10y_minus_us_2y",
)
GOLD_COLUMNS = ("timestamp_m1", *VOLATILITY_FEATURE_COLUMNS, *MACRO_FEATURE_COLUMNS)


@dataclass(frozen=True, slots=True)
class GoldSemanticVersions:
    """Explicit source-controlled semantic version Value Object."""

    schema_version: int = GOLD_SCHEMA_VERSION
    feature_version: int = GOLD_FEATURE_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != GOLD_SCHEMA_VERSION:
            raise ValueError("schema_version is source-controlled and fixed at 1")
        if self.feature_version != GOLD_FEATURE_VERSION:
            raise ValueError("feature_version is source-controlled and fixed at 1")


GOLD_VERSIONS = GoldSemanticVersions()


@dataclass(frozen=True, slots=True)
class SilverInputSignature:
    """Deterministic selected-input provenance signature."""

    series_id: str
    row_count: int
    min_observation_date: date | None
    max_observation_date: date | None
    sha256: str


@dataclass(frozen=True, slots=True)
class GoldFrameBuild:
    """Storage-neutral canonical Gold payload plus reproducibility metadata."""

    frame: pl.DataFrame
    schema_version: int
    feature_version: int
    inputs: tuple[SilverInputSignature, ...]


def _validate_feature_frame(
    name: str,
    frame: pl.DataFrame,
    expected_features: tuple[str, ...],
) -> pl.DataFrame:
    expected = ["timestamp_m1", *expected_features]
    if frame.columns != expected:
        raise ValueError(f"{name} feature schema/order mismatch")
    if frame.schema["timestamp_m1"] != pl.Datetime("us", "UTC"):
        raise TypeError(f"{name} timestamp_m1 must be Datetime(us, UTC)")
    if frame.get_column("timestamp_m1").null_count():
        raise ValueError(f"{name} timestamp_m1 cannot be null")
    if bool(frame.get_column("timestamp_m1").is_duplicated().any()):
        raise ValueError(f"{name} timestamp_m1 must be unique")
    for column in expected_features:
        if frame.schema[column] != pl.Float64:
            raise TypeError(f"{name} feature {column} must be Float64")
    return frame.sort("timestamp_m1")


def _silver_signature(series_id: str, frame: pl.DataFrame) -> SilverInputSignature:
    if frame.schema != SILVER_SCHEMA:
        raise ValueError(f"{series_id} Silver schema mismatch for provenance")
    identities = frame.get_column("series_id").unique().to_list()
    if identities and identities != [series_id]:
        raise ValueError(f"{series_id} Silver identity mismatch for provenance")
    ordered = frame.sort("observation_date")
    dates = ordered.get_column("observation_date")
    minimum = dates.min()
    maximum = dates.max()
    if minimum is not None and not isinstance(minimum, date):
        raise TypeError("Silver min observation date must be Date")
    if maximum is not None and not isinstance(maximum, date):
        raise TypeError("Silver max observation date must be Date")
    payload = ordered.write_csv().encode("utf-8")
    return SilverInputSignature(
        series_id=series_id,
        row_count=ordered.height,
        min_observation_date=minimum,
        max_observation_date=maximum,
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def _input_signatures(
    silver_by_series: Mapping[str, pl.DataFrame],
) -> tuple[SilverInputSignature, ...]:
    missing = [series_id for series_id in GOLD_SOURCE_SERIES if series_id not in silver_by_series]
    if missing:
        raise KeyError(f"missing Silver provenance inputs: {', '.join(missing)}")
    return tuple(
        _silver_signature(series_id, silver_by_series[series_id])
        for series_id in GOLD_SOURCE_SERIES
    )


def assemble_gold_frame(
    volatility_features: pl.DataFrame,
    macro_features: pl.DataFrame,
    silver_by_series: Mapping[str, pl.DataFrame],
    *,
    versions: GoldSemanticVersions = GOLD_VERSIONS,
) -> GoldFrameBuild:
    """Outer-join both feature families into the exact canonical Gold frame."""
    volatility = _validate_feature_frame(
        "volatility", volatility_features, VOLATILITY_FEATURE_COLUMNS
    )
    macro = _validate_feature_frame("macro", macro_features, MACRO_FEATURE_COLUMNS)
    joined = volatility.join(macro, on="timestamp_m1", how="full", coalesce=True).sort(
        "timestamp_m1"
    )
    joined = joined.select(list(GOLD_COLUMNS))
    numeric = list(GOLD_COLUMNS[1:])
    joined = joined.with_columns([pl.col(column).fill_nan(None) for column in numeric])
    for column in numeric:
        values = joined.get_column(column).drop_nulls()
        if values.len() and not bool(values.is_finite().all()):
            raise ValueError(f"non-finite canonical Gold feature: {column}")
    timestamp = joined.get_column("timestamp_m1")
    if timestamp.null_count():
        raise ValueError("canonical Gold timestamp_m1 cannot be null")
    if bool(timestamp.is_duplicated().any()):
        raise ValueError("canonical Gold timestamp_m1 must be unique")
    if joined.columns != list(GOLD_COLUMNS):
        raise AssertionError("canonical Gold column order drift")
    if "observation_date" in joined.columns:
        raise AssertionError("canonical Gold cannot contain observation_date")
    return GoldFrameBuild(
        frame=joined,
        schema_version=versions.schema_version,
        feature_version=versions.feature_version,
        inputs=_input_signatures(silver_by_series),
    )
