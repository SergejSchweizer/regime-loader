from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from application.gold_frame import GOLD_COLUMNS, GOLD_SOURCE_SERIES, SilverInputSignature
from application.gold_sidecars import GoldSidecarBuilder
from application.paths import LakePaths
from ingestion.gold_build_store import GoldBuildStore
from ingestion.gold_sidecar_store import GoldSidecarStore, feature_profile_data

START = datetime(2026, 8, 19, 2, tzinfo=UTC)
GIT_SHA = "c" * 40
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
TEST_PNG = PNG_SIGNATURE + b"fast-test-renderer"


def _frame() -> pl.DataFrame:
    timestamps = [START, START + timedelta(days=1), START + timedelta(days=2)]
    values: dict[str, object] = {"timestamp_m1": timestamps}
    for index, column in enumerate(GOLD_COLUMNS[1:]):
        values[column] = [float(index), None, float(index + 2)]
    return pl.DataFrame(
        values,
        schema={
            "timestamp_m1": pl.Datetime("us", "UTC"),
            **{column: pl.Float64 for column in GOLD_COLUMNS[1:]},
        },
    )


def _inputs() -> tuple[SilverInputSignature, ...]:
    return tuple(
        SilverInputSignature(series_id, 3, date(2026, 8, 19), date(2026, 8, 21), f"{index:064x}")
        for index, series_id in enumerate(GOLD_SOURCE_SERIES)
    )


def _stores(
    tmp_path: Path,
    fault=None,
    *,
    profile_renderer=None,
) -> tuple[LakePaths, GoldBuildStore, GoldSidecarStore]:
    paths = LakePaths(tmp_path / "lake")
    build_store = GoldBuildStore(paths)
    renderer_options = {} if profile_renderer is None else {"profile_renderer": profile_renderer}
    sidecar_store = GoldSidecarStore(
        paths,
        build_store,
        GoldSidecarBuilder(git_commit_hash=GIT_SHA),
        fault_injector=fault,
        **renderer_options,
    )
    return paths, build_store, sidecar_store


@pytest.mark.integration
def test_sidecar_bundle_is_creation_only_valid_and_deterministic(tmp_path: Path) -> None:
    paths, build_store, sidecar_store = _stores(tmp_path)
    frame = _frame()
    first = build_store.create(frame, build_id="20260819T020000Z")
    first_sidecars = sidecar_store.create(
        first,
        started_at_utc=START,
        completed_at_utc=START + timedelta(minutes=1),
        inputs=_inputs(),
    )
    assert first_sidecars.manifest_path == paths.gold_build_manifest(first.build_id)
    assert first_sidecars.plot_path == paths.gold_build_profile(first.build_id)
    assert first_sidecars.manifest_path.read_bytes() == first_sidecars.manifest.to_json_bytes()
    assert first_sidecars.plot_path.read_bytes().startswith(PNG_SIGNATURE)
    assert first_sidecars.manifest.data_sha256 == first.data_sha256
    assert first_sidecars.manifest.row_count == frame.height
    assert first_sidecars.manifest.data_path.endswith(f"build_id={first.build_id}/data.parquet")
    assert first_sidecars.manifest.plot_path.endswith(
        f"build_id={first.build_id}/feature_profile.png"
    )
    assert first_sidecars.manifest.git_commit_hash == GIT_SHA
    names, coverage = feature_profile_data(frame)
    assert names == tuple(GOLD_COLUMNS[1:])
    assert "timestamp_m1" not in names
    assert coverage == pytest.approx(tuple(2 / 3 for _ in names))

    manifest_before = first_sidecars.manifest_path.read_bytes()
    plot_before = first_sidecars.plot_path.read_bytes()
    with pytest.raises(FileExistsError, match="already exist"):
        sidecar_store.create(
            first,
            started_at_utc=START,
            completed_at_utc=START + timedelta(minutes=1),
            inputs=_inputs(),
        )
    assert first_sidecars.manifest_path.read_bytes() == manifest_before
    assert first_sidecars.plot_path.read_bytes() == plot_before

    second = build_store.create(frame, build_id="20260819T020001Z")
    second_sidecars = sidecar_store.create(
        second,
        started_at_utc=START,
        completed_at_utc=START + timedelta(minutes=1),
        inputs=_inputs(),
    )
    assert second_sidecars.plot_path.read_bytes() == plot_before


@pytest.mark.integration
def test_sidecar_store_rejects_non_png_injected_renderer(tmp_path: Path) -> None:
    paths = LakePaths(tmp_path / "lake")
    build_store = GoldBuildStore(paths)
    sidecar_store = GoldSidecarStore(
        paths,
        build_store,
        GoldSidecarBuilder(git_commit_hash=GIT_SHA),
        profile_renderer=lambda frame: b"not a PNG",
    )
    artifact = build_store.create(_frame(), build_id="20260819T020000Z")

    with pytest.raises(ValueError, match="must return a PNG"):
        sidecar_store.create(
            artifact,
            started_at_utc=START,
            completed_at_utc=START + timedelta(minutes=1),
            inputs=_inputs(),
        )


@pytest.mark.integration
def test_bundle_validation_rejects_data_manifest_and_png_mismatch(tmp_path: Path) -> None:
    _, build_store, sidecar_store = _stores(tmp_path, profile_renderer=lambda frame: TEST_PNG)
    frame = _frame()
    artifact = build_store.create(frame, build_id="20260819T020000Z")
    sidecars = sidecar_store.create(
        artifact,
        started_at_utc=START,
        completed_at_utc=START + timedelta(minutes=1),
        inputs=_inputs(),
    )

    with pytest.raises(ValueError, match="row count"):
        sidecar_store.validate_bundle(replace(artifact, row_count=artifact.row_count + 1), sidecars)

    bad_manifest = replace(sidecars.manifest, feature_set_hash="0" * 64)
    bad_bytes = bad_manifest.to_json_bytes()
    sidecars.manifest_path.write_bytes(bad_bytes)
    bad_sidecars = replace(
        sidecars,
        manifest=bad_manifest,
        manifest_sha256=hashlib.sha256(bad_bytes).hexdigest(),
    )
    with pytest.raises(ValueError, match="feature-set hash"):
        sidecar_store.validate_bundle(artifact, bad_sidecars)

    sidecars.manifest_path.write_bytes(sidecars.manifest.to_json_bytes())
    sidecars.plot_path.write_bytes(b"not-png")
    with pytest.raises(ValueError, match="not a PNG"):
        sidecar_store.validate_bundle(artifact, sidecars)


@pytest.mark.integration
def test_sidecar_failure_leaves_incomplete_attempt_and_root_views_untouched(
    tmp_path: Path,
) -> None:
    def fail(stage: str) -> None:
        if stage == "after_plot_create":
            raise RuntimeError("injected sidecar failure")

    paths, build_store, sidecar_store = _stores(
        tmp_path,
        fault=fail,
        profile_renderer=lambda frame: TEST_PNG,
    )
    frame = _frame()
    artifact = build_store.create(frame, build_id="20260819T020000Z")
    paths.gold_dataset_root().mkdir(parents=True, exist_ok=True)
    paths.gold_manifest_json().write_bytes(b"root-json-sentinel")
    paths.gold_profile().write_bytes(b"root-png-sentinel")
    paths.gold_manifest_parquet().write_bytes(b"root-catalog-sentinel")

    with pytest.raises(RuntimeError, match="injected"):
        sidecar_store.create(
            artifact,
            started_at_utc=START,
            completed_at_utc=START + timedelta(minutes=1),
            inputs=_inputs(),
        )

    assert paths.gold_build_profile(artifact.build_id).is_file()
    assert not paths.gold_build_manifest(artifact.build_id).exists()
    assert paths.gold_manifest_json().read_bytes() == b"root-json-sentinel"
    assert paths.gold_profile().read_bytes() == b"root-png-sentinel"
    assert paths.gold_manifest_parquet().read_bytes() == b"root-catalog-sentinel"
