from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from application.gold_catalog import GoldBuildStatus, GoldCatalogRecord
from application.gold_frame import GOLD_COLUMNS, GOLD_SOURCE_SERIES, SilverInputSignature
from application.gold_publication import GoldPublisher
from application.gold_sidecars import GoldSidecarBuilder
from application.paths import LakePaths
from ingestion.gold_build_store import GoldBuildStore
from ingestion.gold_catalog_repository import GoldCatalogRepository
from ingestion.gold_materialized_views import GoldMaterializedViewWriter
from ingestion.gold_publication_adapters import GoldBundleAdapter
from ingestion.gold_sidecar_store import GoldSidecarStore

START = datetime(2026, 8, 19, 2, tzinfo=UTC)
GIT_SHA = "d" * 40


class SequenceClock:
    def __init__(self, values: list[datetime]) -> None:
        self._values = iter(values)

    def __call__(self) -> datetime:
        return next(self._values)


def _frame(offset: float = 0.0) -> pl.DataFrame:
    timestamps = [START - timedelta(days=1), START]
    return pl.DataFrame(
        {
            "timestamp_m1": timestamps,
            **{
                column: [float(index) + offset, float(index + 1) + offset]
                for index, column in enumerate(GOLD_COLUMNS[1:])
            },
        },
        schema={
            "timestamp_m1": pl.Datetime("us", "UTC"),
            **{column: pl.Float64 for column in GOLD_COLUMNS[1:]},
        },
    )


def _inputs() -> tuple[SilverInputSignature, ...]:
    return tuple(
        SilverInputSignature(series_id, 2, date(2026, 8, 18), date(2026, 8, 19), f"{index:064x}")
        for index, series_id in enumerate(GOLD_SOURCE_SERIES)
    )


def _stack(
    tmp_path: Path,
    *,
    build_times: list[datetime],
    view_fault=None,
    bundle_fault=None,
) -> tuple[
    LakePaths,
    GoldCatalogRepository,
    GoldBuildStore,
    GoldSidecarStore,
    GoldBundleAdapter,
    GoldMaterializedViewWriter,
    GoldPublisher,
]:
    paths = LakePaths(tmp_path / "lake")
    catalog = GoldCatalogRepository(paths.gold_manifest_parquet())
    build_store = GoldBuildStore(paths, clock=SequenceClock(build_times))
    sidecar_store = GoldSidecarStore(
        paths,
        build_store,
        GoldSidecarBuilder(git_commit_hash=GIT_SHA),
    )
    bundle = GoldBundleAdapter(
        paths,
        build_store,
        sidecar_store,
        clock=lambda: START + timedelta(minutes=1),
        fault_injector=bundle_fault,
    )
    views = GoldMaterializedViewWriter(paths, fault_injector=view_fault)
    publisher = GoldPublisher(catalog, bundle, views, clock=lambda: START)
    return paths, catalog, build_store, sidecar_store, bundle, views, publisher


@pytest.mark.integration
def test_first_and_subsequent_publication_materialize_exact_catalog_current_and_plot(
    tmp_path: Path,
) -> None:
    paths, catalog, _, _, _, _, publisher = _stack(
        tmp_path,
        build_times=[START, START + timedelta(seconds=1)],
    )
    first = publisher.publish(_frame(), inputs=_inputs())
    assert first.build_id == "20260819T020000Z"
    records = catalog.read()
    assert len(records) == 1
    assert records[0].current and records[0].status is GoldBuildStatus.COMPLETE
    root = json.loads(paths.gold_manifest_json().read_bytes())
    assert root["build_id"] == first.build_id
    assert root["status"] == "complete"
    assert root["current"] is True
    assert (
        paths.gold_profile().read_bytes() == paths.gold_build_profile(first.build_id).read_bytes()
    )

    second = publisher.publish(_frame(offset=10.0), inputs=_inputs())
    assert second.build_id == "20260819T020001Z"
    records = catalog.read()
    assert len(records) == 2
    assert [record.build_id for record in records if record.current] == [second.build_id]
    old = next(record for record in records if record.build_id == first.build_id)
    assert old.status is GoldBuildStatus.COMPLETE and not old.current
    root = json.loads(paths.gold_manifest_json().read_bytes())
    assert root["build_id"] == second.build_id
    assert (
        paths.gold_profile().read_bytes() == paths.gold_build_profile(second.build_id).read_bytes()
    )


@pytest.mark.integration
def test_physical_candidate_corruption_blocks_promotion_and_preserves_old_current(
    tmp_path: Path,
) -> None:
    paths, catalog, build_store, sidecar_store, _, views, first_publisher = _stack(
        tmp_path,
        build_times=[START, START + timedelta(seconds=1)],
    )
    first = first_publisher.publish(_frame(), inputs=_inputs())

    def corrupt(stage: str) -> None:
        if stage == "after_bundle_create":
            paths.gold_build_profile("20260819T020001Z").write_bytes(b"corrupt-png")

    broken_bundle = GoldBundleAdapter(
        paths,
        build_store,
        sidecar_store,
        clock=lambda: START + timedelta(minutes=1),
        fault_injector=corrupt,
    )
    broken = GoldPublisher(catalog, broken_bundle, views, clock=lambda: START)
    with pytest.raises(ValueError, match="not a PNG|SHA-256 mismatch"):
        broken.publish(_frame(offset=20.0), inputs=_inputs())

    records = catalog.read()
    assert [record.build_id for record in records if record.current] == [first.build_id]
    failed = next(record for record in records if record.build_id == "20260819T020001Z")
    assert failed.status is GoldBuildStatus.FAILED
    assert failed.data_path is None
    assert json.loads(paths.gold_manifest_json().read_bytes())["build_id"] == first.build_id


@pytest.mark.integration
def test_bundle_adapter_rejects_artifact_path_escape_before_publication(tmp_path: Path) -> None:
    paths, _, build_store, sidecar_store, bundle, _, _ = _stack(
        tmp_path,
        build_times=[START],
    )
    artifact = build_store.create(_frame(), build_id="20260819T020000Z")
    sidecars = sidecar_store.create(
        artifact,
        started_at_utc=START,
        completed_at_utc=START + timedelta(minutes=1),
        inputs=_inputs(),
    )
    escaped = replace(artifact, data_path=tmp_path / "outside.parquet")
    with pytest.raises(ValueError, match="physical artifact path mismatch"):
        bundle._validate_candidate(escaped, sidecars, _inputs())
    assert paths.gold_manifest_parquet().exists() is False


@pytest.mark.integration
def test_postcommit_view_failure_preserves_new_catalog_and_next_reconcile_repairs_views(
    tmp_path: Path,
) -> None:
    paths, catalog, build_store, sidecar_store, _, _, first_publisher = _stack(
        tmp_path,
        build_times=[START, START + timedelta(seconds=1)],
    )
    first_publisher.publish(_frame(), inputs=_inputs())
    calls = 0

    def fail_third_json_refresh(stage: str) -> None:
        nonlocal calls
        if stage == "after_root_json":
            calls += 1
            if calls == 3:
                raise OSError("injected post-commit root view failure")

    failing_views = GoldMaterializedViewWriter(paths, fault_injector=fail_third_json_refresh)
    second_bundle = GoldBundleAdapter(
        paths,
        build_store,
        sidecar_store,
        clock=lambda: START + timedelta(minutes=1),
    )
    second_publisher = GoldPublisher(catalog, second_bundle, failing_views, clock=lambda: START)
    with pytest.raises(OSError, match="post-commit"):
        second_publisher.publish(_frame(offset=30.0), inputs=_inputs())

    records = catalog.read()
    assert [record.build_id for record in records if record.current] == ["20260819T020001Z"]
    assert next(record for record in records if record.current).status is GoldBuildStatus.COMPLETE

    healthy_views = GoldMaterializedViewWriter(paths)
    repair = GoldPublisher(catalog, second_bundle, healthy_views, clock=lambda: START)
    repair.reconcile()
    root = json.loads(paths.gold_manifest_json().read_bytes())
    assert root["build_id"] == "20260819T020001Z"
    assert (
        paths.gold_profile().read_bytes()
        == paths.gold_build_profile("20260819T020001Z").read_bytes()
    )


@pytest.mark.integration
def test_stale_building_with_complete_files_is_failed_never_auto_promoted(tmp_path: Path) -> None:
    paths, catalog, build_store, sidecar_store, bundle, views, publisher = _stack(
        tmp_path,
        build_times=[START],
    )
    current = publisher.publish(_frame(), inputs=_inputs())
    stale_id = "20260819T020100Z"
    physical = build_store.create(_frame(offset=50.0), build_id=stale_id)
    sidecar_store.create(
        physical,
        started_at_utc=START,
        completed_at_utc=START + timedelta(minutes=1),
        inputs=_inputs(),
    )
    catalog.append(
        GoldCatalogRecord(
            dataset_id="regime_features_daily",
            build_id=stale_id,
            status=GoldBuildStatus.BUILDING,
            current=False,
            started_at_utc=START - timedelta(hours=1),
            completed_at_utc=None,
            schema_version=1,
            feature_version=1,
            min_timestamp=None,
            max_timestamp=None,
            row_count=None,
            data_path=None,
            build_manifest_path=None,
            plot_path=None,
            pruned_at_utc=None,
        )
    )

    GoldPublisher(catalog, bundle, views, clock=lambda: START).reconcile()
    repaired = next(record for record in catalog.read() if record.build_id == stale_id)
    assert repaired.status is GoldBuildStatus.FAILED
    assert not repaired.current
    assert next(record for record in catalog.read() if record.build_id == current.build_id).current
    assert paths.gold_build_manifest(stale_id).is_file()
    assert json.loads(paths.gold_manifest_json().read_bytes())["build_id"] == current.build_id


@pytest.mark.integration
def test_materialized_view_writer_rejects_wrong_catalog_path_shape_and_clears_without_current(
    tmp_path: Path,
) -> None:
    paths = LakePaths(tmp_path / "lake")
    writer = GoldMaterializedViewWriter(paths)
    paths.gold_dataset_root().mkdir(parents=True, exist_ok=True)
    paths.gold_manifest_json().write_text("stale")
    paths.gold_profile().write_bytes(b"stale")
    writer.refresh([])
    assert not paths.gold_manifest_json().exists()
    assert not paths.gold_profile().exists()

    build_id = "20260819T020000Z"
    invalid = GoldCatalogRecord(
        dataset_id="regime_features_daily",
        build_id=build_id,
        status=GoldBuildStatus.COMPLETE,
        current=True,
        started_at_utc=START,
        completed_at_utc=START + timedelta(minutes=1),
        schema_version=1,
        feature_version=1,
        min_timestamp=START,
        max_timestamp=START,
        row_count=1,
        data_path="wrong/data.parquet",
        build_manifest_path=f"versions/build_id={build_id}/manifest.json",
        plot_path=f"versions/build_id={build_id}/feature_profile.png",
        pruned_at_utc=None,
    )
    with pytest.raises(ValueError, match="path shape mismatch"):
        writer.refresh([invalid])
