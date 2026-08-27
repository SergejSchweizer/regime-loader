from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from application.gold_frame import GOLD_COLUMNS
from application.gold_sidecars import (
    GOLD_FORMULA_PARAMETERS,
    GoldSidecarBuilder,
    expected_manifest_keys,
    feature_set_sha256,
)
from ingestion.gold_sidecar_store import _profile_png

START = datetime(2026, 8, 19, 2, tzinfo=UTC)
GIT_SHA = "a" * 40


def _frame() -> pl.DataFrame:
    timestamps = [START, START + timedelta(days=1)]
    values: dict[str, object] = {"timestamp_m1": timestamps}
    for index, column in enumerate(GOLD_COLUMNS[1:]):
        values[column] = [float(index + 1), None]
    return pl.DataFrame(
        values,
        schema={
            "timestamp_m1": pl.Datetime("us", "UTC"),
            **{column: pl.Float64 for column in GOLD_COLUMNS[1:]},
        },
    )


def test_manifest_has_exact_deterministic_json_contract_without_publication_status() -> None:
    frame = _frame()
    builder = GoldSidecarBuilder(git_commit_hash=GIT_SHA)
    manifest = builder.build(
        frame,
        build_id="20260819T020000Z",
        started_at_utc=START,
        completed_at_utc=START + timedelta(minutes=2),
        data_path="versions/build_id=20260819T020000Z/data.parquet",
        data_sha256="b" * 64,
        plot_path="versions/build_id=20260819T020000Z/feature_profile.png",
    )
    payload = manifest.as_dict()
    assert sorted(payload) == list(expected_manifest_keys())
    assert payload["artifact_state"] == "built"
    assert "status" not in payload
    assert payload["dataset_id"] == "regime_features_daily"
    assert payload["columns"] == list(GOLD_COLUMNS)
    assert payload["row_count"] == 2
    assert payload["started_at_utc"] == "2026-08-19T02:00:00.000000Z"
    assert payload["completed_at_utc"] == "2026-08-19T02:02:00.000000Z"
    assert payload["git_commit_hash"] == GIT_SHA
    expected = (
        json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()
    assert manifest.to_json_bytes() == expected
    assert (
        builder.build(
            frame,
            build_id="20260819T020000Z",
            started_at_utc=START,
            completed_at_utc=START + timedelta(minutes=2),
            data_path="versions/build_id=20260819T020000Z/data.parquet",
            data_sha256="b" * 64,
            plot_path="versions/build_id=20260819T020000Z/feature_profile.png",
        ).to_json_bytes()
        == expected
    )


def test_feature_set_hash_covers_order_dtype_versions_and_formula_parameters() -> None:
    frame = _frame()
    base = feature_set_sha256(frame)
    assert len(base) == 64
    assert base == feature_set_sha256(frame)
    assert base != feature_set_sha256(frame, schema_version=3)
    assert base != feature_set_sha256(frame, feature_version=2)
    changed_formula = dict(GOLD_FORMULA_PARAMETERS)
    changed_formula["observation_delta_observations"] = [4, 20]
    assert base != feature_set_sha256(frame, formula_parameters=changed_formula)
    reordered = frame.select(["timestamp_m1", *reversed(GOLD_COLUMNS[1:])])
    assert base != feature_set_sha256(reordered)
    changed_dtype = frame.with_columns(pl.col("vix_level").cast(pl.Float32))
    assert base != feature_set_sha256(changed_dtype)


def test_git_identity_is_required_with_explicit_deterministic_test_fallback() -> None:
    with pytest.raises(ValueError, match="required"):
        GoldSidecarBuilder(git_commit_hash=None)
    with pytest.raises(ValueError, match="hexadecimal"):
        GoldSidecarBuilder(git_commit_hash="not-a-git-sha")
    fallback = GoldSidecarBuilder(git_commit_hash=None, allow_test_git_fallback=True)
    manifest = fallback.build(
        _frame(),
        build_id="20260819T020000Z",
        started_at_utc=START,
        completed_at_utc=START,
        data_path="versions/build_id=20260819T020000Z/data.parquet",
        data_sha256=hashlib.sha256(b"data").hexdigest(),
        plot_path="versions/build_id=20260819T020000Z/feature_profile.png",
    )
    assert manifest.git_commit_hash == "0" * 40


def test_feature_profile_is_a_diagnostic_png_with_all_features() -> None:
    png = _profile_png(_frame())
    assert png.startswith(b"\x89PNG\r\n\x1a\n")
    assert len(png) > 10_000


def test_builder_rejects_wrong_frame_order_naive_time_and_reverse_completion() -> None:
    builder = GoldSidecarBuilder(git_commit_hash=GIT_SHA)
    frame = _frame()
    kwargs = {
        "build_id": "20260819T020000Z",
        "started_at_utc": START,
        "completed_at_utc": START,
        "data_path": "versions/build_id=20260819T020000Z/data.parquet",
        "data_sha256": "b" * 64,
        "plot_path": "versions/build_id=20260819T020000Z/feature_profile.png",
    }
    with pytest.raises(ValueError, match="column order"):
        builder.build(frame.select(list(reversed(frame.columns))), **kwargs)
    with pytest.raises(ValueError, match="timezone-aware"):
        builder.build(frame, **{**kwargs, "started_at_utc": datetime(2026, 8, 19, 2)})
    with pytest.raises(ValueError, match="cannot precede"):
        builder.build(
            frame,
            **{
                **kwargs,
                "started_at_utc": START + timedelta(minutes=1),
                "completed_at_utc": START,
            },
        )
