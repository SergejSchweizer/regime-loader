from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from application.gold_catalog import LATEST_COMPATIBLE, GoldBuildStatus, GoldCompatibility
from application.gold_frame import (
    GOLD_COLUMNS,
    GOLD_FEATURE_VERSION,
    GOLD_SCHEMA_VERSION,
    GOLD_SOURCE_SERIES,
    SilverInputSignature,
)
from application.gold_publication import GoldPublisher
from application.gold_retention import GoldRetentionService
from application.gold_sidecars import GoldSidecarBuilder
from application.paths import LakePaths
from ingestion.gold_build_store import GoldBuildStore
from ingestion.gold_catalog_repository import GoldCatalogRepository
from ingestion.gold_materialized_views import GoldMaterializedViewWriter
from ingestion.gold_publication_adapters import GoldBundleAdapter
from ingestion.gold_retention_store import GoldBundleSweeper
from ingestion.gold_sidecar_store import GoldSidecarStore

START = datetime(2026, 8, 19, 2, tzinfo=UTC)
GIT_SHA = "e" * 40
TEST_PNG = b"\x89PNG\r\n\x1a\nfast-test-renderer"


class SequenceClock:
    def __init__(self, values: list[datetime]) -> None:
        self._values = iter(values)

    def __call__(self) -> datetime:
        return next(self._values)


def _frame(offset: float) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp_m1": [START - timedelta(days=1), START],
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


def _published_stack(
    tmp_path: Path,
) -> tuple[LakePaths, GoldCatalogRepository, GoldMaterializedViewWriter]:
    paths = LakePaths(tmp_path / "lake")
    times = [START + timedelta(seconds=index) for index in range(6)]
    build_store = GoldBuildStore(paths, clock=SequenceClock(times))
    sidecar_store = GoldSidecarStore(
        paths,
        build_store,
        GoldSidecarBuilder(git_commit_hash=GIT_SHA),
        profile_renderer=lambda frame: TEST_PNG,
    )
    bundle = GoldBundleAdapter(
        paths,
        build_store,
        sidecar_store,
        clock=lambda: START + timedelta(minutes=1),
    )
    catalog = GoldCatalogRepository(paths.gold_manifest_parquet())
    views = GoldMaterializedViewWriter(paths)
    publisher = GoldPublisher(catalog, bundle, views, clock=lambda: START)
    for index in range(6):
        publisher.publish(_frame(float(index * 10)), inputs=_inputs())
    return paths, catalog, views


@pytest.mark.integration
def test_default_retention_marks_then_deletes_oldest_bundle_and_keeps_current_views(
    tmp_path: Path,
) -> None:
    paths, catalog, views = _published_stack(tmp_path)
    before = catalog.read()
    oldest = before[0]
    current = next(record for record in before if record.current)
    current_root_before = paths.gold_profile().read_bytes()

    service = GoldRetentionService(
        catalog,
        GoldBundleSweeper(paths),
        views,
        clock=lambda: START + timedelta(hours=1),
    )
    result = service.run()
    assert result.marked_build_ids == (oldest.build_id,)
    assert result.swept_build_ids == (oldest.build_id,)

    after = catalog.read()
    tombstone = next(record for record in after if record.build_id == oldest.build_id)
    assert tombstone.status is GoldBuildStatus.COMPLETE
    assert tombstone.pruned_at_utc == START + timedelta(hours=1)
    assert tombstone.data_path is None
    assert tombstone.build_manifest_path is None
    assert tombstone.plot_path is None
    assert not paths.gold_build_root(oldest.build_id).exists()
    assert next(record for record in after if record.build_id == current.build_id).current
    assert json.loads(paths.gold_manifest_json().read_bytes())["build_id"] == current.build_id
    assert paths.gold_profile().read_bytes() == current_root_before
    assert (
        LATEST_COMPATIBLE.resolve(
            after,
            GoldCompatibility(GOLD_SCHEMA_VERSION, GOLD_FEATURE_VERSION),
        ).build_id
        == current.build_id
    )

    rerun = service.run()
    assert rerun.marked_build_ids == ()
    assert rerun.swept_build_ids == (oldest.build_id,)
    assert not paths.gold_build_root(oldest.build_id).exists()


@pytest.mark.integration
def test_partial_sweep_failure_leaves_unselectable_tombstone_and_retry_cleans_orphan(
    tmp_path: Path,
) -> None:
    paths, catalog, views = _published_stack(tmp_path)
    oldest = catalog.read()[0]
    current = next(record for record in catalog.read() if record.current)

    def fail_after_data(stage: str) -> None:
        if stage == "after_delete:data":
            raise OSError("injected retention delete failure")

    failing = GoldRetentionService(
        catalog,
        GoldBundleSweeper(paths, fault_injector=fail_after_data),
        views,
        clock=lambda: START + timedelta(hours=1),
    )
    with pytest.raises(OSError, match="retention delete failure"):
        failing.run()

    tombstone = next(record for record in catalog.read() if record.build_id == oldest.build_id)
    assert tombstone.pruned_at_utc is not None
    assert tombstone.data_path is None
    assert not paths.gold_data(oldest.build_id).exists()
    assert paths.gold_build_manifest(oldest.build_id).exists()
    assert paths.gold_build_profile(oldest.build_id).exists()
    assert (
        LATEST_COMPATIBLE.resolve(
            catalog.read(),
            GoldCompatibility(GOLD_SCHEMA_VERSION, GOLD_FEATURE_VERSION),
        ).build_id
        == current.build_id
    )

    retry = GoldRetentionService(
        catalog,
        GoldBundleSweeper(paths),
        views,
        clock=lambda: START + timedelta(hours=2),
    )
    result = retry.run()
    assert result.marked_build_ids == ()
    assert result.swept_build_ids == (oldest.build_id,)
    assert not paths.gold_build_root(oldest.build_id).exists()
    assert json.loads(paths.gold_manifest_json().read_bytes())["build_id"] == current.build_id
