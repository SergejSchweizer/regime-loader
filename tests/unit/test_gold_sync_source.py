from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from application.gold_catalog import GoldBuildStatus, GoldCatalogRecord
from application.gold_frame import GOLD_COLUMNS, GOLD_SOURCE_SERIES, SilverInputSignature
from application.gold_sidecars import GoldSidecarBuilder
from application.paths import LakePaths
from ingestion.gold_build_store import GoldBuildStore
from ingestion.gold_sync_source import FilesystemGoldFrameSource

START = datetime(2026, 8, 22, tzinfo=UTC)
BUILD_ID = "20260822T000000Z"


def _frame() -> pl.DataFrame:
    values: dict[str, list[object]] = {"timestamp_m1": [START, START + timedelta(days=1)]}
    for index, column in enumerate(GOLD_COLUMNS[1:]):
        values[column] = [float(index), float(index + 1)]
    return pl.DataFrame(values).with_columns(pl.col("timestamp_m1").cast(pl.Datetime("us", "UTC")))


def _inputs() -> tuple[SilverInputSignature, ...]:
    return tuple(
        SilverInputSignature(series_id, 2, date(2026, 8, 21), date(2026, 8, 22), f"{index:064x}")
        for index, series_id in enumerate(GOLD_SOURCE_SERIES)
    )


def _bundle(tmp_path: Path) -> tuple[FilesystemGoldFrameSource, GoldCatalogRecord, Path]:
    paths = LakePaths(tmp_path / "lake")
    store = GoldBuildStore(paths)
    frame = _frame()
    artifact = store.create(frame, build_id=BUILD_ID)
    manifest = GoldSidecarBuilder(git_commit_hash="a" * 40).build(
        frame,
        build_id=BUILD_ID,
        started_at_utc=START,
        completed_at_utc=START,
        data_path=f"versions/build_id={BUILD_ID}/data.parquet",
        data_sha256=artifact.data_sha256,
        plot_path=f"versions/build_id={BUILD_ID}/feature_profile.png",
        inputs=_inputs(),
    )
    manifest_path = paths.gold_build_manifest(BUILD_ID)
    manifest_path.write_bytes(manifest.to_json_bytes())
    paths.gold_build_profile(BUILD_ID).write_bytes(b"png")
    record = GoldCatalogRecord(
        dataset_id="regime_features_daily",
        build_id=BUILD_ID,
        status=GoldBuildStatus.COMPLETE,
        current=True,
        started_at_utc=START,
        completed_at_utc=START,
        schema_version=2,
        feature_version=1,
        min_timestamp=START,
        max_timestamp=START + timedelta(days=1),
        row_count=2,
        data_path=f"versions/build_id={BUILD_ID}/data.parquet",
        build_manifest_path=f"versions/build_id={BUILD_ID}/manifest.json",
        plot_path=f"versions/build_id={BUILD_ID}/feature_profile.png",
        pruned_at_utc=None,
    )
    return FilesystemGoldFrameSource(paths, store), record, manifest_path


def test_selected_bundle_verification_accepts_exact_certified_artifacts(tmp_path: Path) -> None:
    source, record, _ = _bundle(tmp_path)

    source.validate_bundle(record)


@pytest.mark.parametrize("mutation", ["legacy", "wrong_hash", "invalid_input_date"])
def test_selected_bundle_verification_rejects_manifest_tampering(
    tmp_path: Path,
    mutation: str,
) -> None:
    source, record, manifest_path = _bundle(tmp_path)
    payload = json.loads(manifest_path.read_bytes())
    if mutation == "legacy":
        payload["manifest_version"] = 1
    elif mutation == "wrong_hash":
        payload["data_sha256"] = "0" * 64
    else:
        payload["inputs"][0]["min_observation_date"] = "invalid"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError):
        source.validate_bundle(record)
