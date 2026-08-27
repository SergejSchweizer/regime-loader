from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from application.gold_catalog import GoldBuildStatus, GoldCatalogRecord
from application.gold_frame import GOLD_COLUMNS, GOLD_FEATURE_VERSION, GOLD_SCHEMA_VERSION
from application.gold_publication import GoldPublicationBundle, GoldPublisher

START = datetime(2026, 8, 19, 2, tzinfo=UTC)


def _frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "timestamp_m1": [START],
            **{column: [1.0] for column in GOLD_COLUMNS[1:]},
        },
        schema={
            "timestamp_m1": pl.Datetime("us", "UTC"),
            **{column: pl.Float64 for column in GOLD_COLUMNS[1:]},
        },
    )


def _complete(build_id: str, *, current: bool = True) -> GoldCatalogRecord:
    return GoldCatalogRecord(
        dataset_id="regime_features_daily",
        build_id=build_id,
        status=GoldBuildStatus.COMPLETE,
        current=current,
        started_at_utc=START - timedelta(minutes=2),
        completed_at_utc=START - timedelta(minutes=1),
        schema_version=1,
        feature_version=1,
        min_timestamp=START - timedelta(days=1),
        max_timestamp=START,
        row_count=2,
        data_path=f"versions/build_id={build_id}/data.parquet",
        build_manifest_path=f"versions/build_id={build_id}/manifest.json",
        plot_path=f"versions/build_id={build_id}/feature_profile.png",
        pruned_at_utc=None,
    )


def _building(build_id: str, *, current: bool = False) -> GoldCatalogRecord:
    return GoldCatalogRecord(
        dataset_id="regime_features_daily",
        build_id=build_id,
        status=GoldBuildStatus.BUILDING,
        current=current,
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


@dataclass
class FakeCatalog:
    records: list[GoldCatalogRecord]
    fail_on_replace_number: int | None = None

    def __post_init__(self) -> None:
        self.replace_count = 0
        self.snapshots: list[list[GoldCatalogRecord]] = []

    def read(self) -> list[GoldCatalogRecord]:
        return list(self.records)

    def replace(self, records: list[GoldCatalogRecord]) -> None:
        self.replace_count += 1
        if self.fail_on_replace_number == self.replace_count:
            raise OSError("injected catalog replace failure")
        self.records = list(records)
        self.snapshots.append(list(records))


@dataclass
class FakeBundle:
    build_id: str = "20260819T020000Z"
    fail: bool = False
    schema_version: int = GOLD_SCHEMA_VERSION
    feature_version: int = GOLD_FEATURE_VERSION

    def next_build_id(self) -> str:
        return self.build_id

    def create_and_validate(
        self,
        frame: pl.DataFrame,
        *,
        build_id: str,
        started_at_utc: datetime,
    ) -> GoldPublicationBundle:
        assert frame.columns == list(GOLD_COLUMNS)
        assert build_id == self.build_id
        assert started_at_utc.tzinfo is not None
        if self.fail:
            raise OSError("injected bundle failure")
        return GoldPublicationBundle(
            build_id=build_id,
            completed_at_utc=START + timedelta(minutes=1),
            schema_version=self.schema_version,
            feature_version=self.feature_version,
            row_count=1,
            min_timestamp=START,
            max_timestamp=START,
            data_path=f"versions/build_id={build_id}/data.parquet",
            build_manifest_path=f"versions/build_id={build_id}/manifest.json",
            plot_path=f"versions/build_id={build_id}/feature_profile.png",
        )


class FakeViews:
    def __init__(self) -> None:
        self.snapshots: list[list[GoldCatalogRecord]] = []

    def refresh(self, records: list[GoldCatalogRecord]) -> None:
        self.snapshots.append(list(records))


class FailSecondRefresh(FakeViews):
    def refresh(self, records: list[GoldCatalogRecord]) -> None:
        super().refresh(records)
        if len(self.snapshots) == 2:
            raise OSError("injected view refresh failure")


def test_first_and_subsequent_publication_have_one_current_and_commit_event_order() -> None:
    events: list[str] = []
    catalog = FakeCatalog([])
    publisher = GoldPublisher(
        catalog,
        FakeBundle(),
        FakeViews(),
        clock=lambda: START,
        event_sink=events.append,
    )
    current = publisher.publish(_frame())
    assert current.status is GoldBuildStatus.COMPLETE
    assert current.current
    assert sum(record.current for record in catalog.records) == 1
    assert events.index("bundle:validated") < events.index("catalog:promoted")
    assert events.index("catalog:promoted") < events.index("views:promoted")

    events.clear()
    publisher.bundle = FakeBundle(build_id="20260819T020001Z")
    newer = publisher.publish(_frame())
    assert newer.build_id == "20260819T020001Z"
    assert [record.build_id for record in catalog.records if record.current] == [newer.build_id]
    old = next(record for record in catalog.records if record.build_id == "20260819T020000Z")
    assert old.status is GoldBuildStatus.COMPLETE and not old.current


def test_bundle_or_precommit_view_failure_finalizes_attempt_without_changing_old_current() -> None:
    old = _complete("20260818T020000Z")
    for bundle, views, match in (
        (FakeBundle(fail=True), FakeViews(), "bundle failure"),
        (FakeBundle(), FailSecondRefresh(), "view refresh failure"),
    ):
        catalog = FakeCatalog([old])
        publisher = GoldPublisher(catalog, bundle, views, clock=lambda: START)
        with pytest.raises(OSError, match=match):
            publisher.publish(_frame())
        assert next(record for record in catalog.records if record.build_id == old.build_id).current
        attempt = next(record for record in catalog.records if record.build_id != old.build_id)
        assert attempt.status is GoldBuildStatus.FAILED
        assert not attempt.current
        assert attempt.data_path is None


def test_candidate_version_mismatch_prevents_promotion_and_marks_failed() -> None:
    catalog = FakeCatalog([])
    publisher = GoldPublisher(
        catalog,
        FakeBundle(schema_version=3),
        FakeViews(),
        clock=lambda: START,
    )
    with pytest.raises(ValueError, match="schema_version mismatch"):
        publisher.publish(_frame())
    assert catalog.records[0].status is GoldBuildStatus.FAILED
    assert not catalog.records[0].current


def test_promotion_replace_failure_marks_attempt_failed_and_preserves_old_current() -> None:
    old = _complete("20260818T020000Z")
    catalog = FakeCatalog([old], fail_on_replace_number=2)
    publisher = GoldPublisher(catalog, FakeBundle(), FakeViews(), clock=lambda: START)
    with pytest.raises(OSError, match="catalog replace failure"):
        publisher.publish(_frame())
    assert next(record for record in catalog.records if record.build_id == old.build_id).current
    attempt = next(record for record in catalog.records if record.build_id != old.build_id)
    assert attempt.status is GoldBuildStatus.FAILED


def test_postcommit_view_failure_never_rolls_back_promoted_catalog() -> None:
    old = _complete("20260818T020000Z")

    class FailOnlyAfterPromotion(FakeViews):
        def refresh(self, records: list[GoldCatalogRecord]) -> None:
            self.snapshots.append(list(records))
            new_current = any(
                record.current and record.build_id == "20260819T020000Z" for record in records
            )
            if new_current:
                raise OSError("post-commit view failure")

    catalog = FakeCatalog([old])
    publisher = GoldPublisher(
        catalog,
        FakeBundle(),
        FailOnlyAfterPromotion(),
        clock=lambda: START,
    )
    with pytest.raises(OSError, match="post-commit"):
        publisher.publish(_frame())
    assert [record.build_id for record in catalog.records if record.current] == ["20260819T020000Z"]
    assert (
        next(record for record in catalog.records if record.build_id == "20260819T020000Z").status
        is GoldBuildStatus.COMPLETE
    )


def test_reconcile_marks_stale_noncurrent_building_failed_without_inferring_filesystem_state() -> (
    None
):
    old = _complete("20260818T020000Z")
    stale = _building("20260819T010000Z")
    catalog = FakeCatalog([old, stale])
    views = FakeViews()
    publisher = GoldPublisher(catalog, FakeBundle(), views, clock=lambda: START)
    result = publisher.reconcile()
    repaired = next(record for record in result if record.build_id == stale.build_id)
    assert repaired.status is GoldBuildStatus.FAILED
    assert repaired.completed_at_utc == START
    assert next(record for record in result if record.build_id == old.build_id).current
    assert views.snapshots[-1] == result


def test_current_building_is_invariant_error_and_no_mutation_occurs() -> None:
    invalid = _building("20260819T010000Z", current=True)
    catalog = FakeCatalog([invalid])
    publisher = GoldPublisher(catalog, FakeBundle(), FakeViews(), clock=lambda: START)
    with pytest.raises(ValueError, match="current Gold building"):
        publisher.reconcile()
    assert catalog.replace_count == 0


def test_failure_finalizer_does_not_turn_already_complete_commit_into_failed() -> None:
    complete = _complete("20260819T020000Z")
    catalog = FakeCatalog([complete])
    publisher = GoldPublisher(catalog, FakeBundle(), FakeViews(), clock=lambda: START)
    publisher._finalize_failed_if_uncommitted(complete.build_id)
    assert catalog.records == [complete]
