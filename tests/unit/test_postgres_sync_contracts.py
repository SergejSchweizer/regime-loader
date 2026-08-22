from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.gold_catalog import GoldBuildStatus, GoldCatalogRecord
from application.gold_frame import GOLD_COLUMNS
from application.postgres_sync import (
    POSTGRES_CONSUMER_SCHEMA,
    POSTGRES_CONSUMER_TABLE,
    POSTGRES_DATASET_ID,
    POSTGRES_ROW_HASH_TABLE,
    POSTGRES_SESSION_TIMEZONE,
    POSTGRES_SYNC_SCHEMA,
    POSTGRES_SYNC_STATE_TABLE,
    POSTGRES_TIMESTAMP_COLUMN,
    POSTGRES_TIMESTAMP_SQL_TYPE,
    GoldDeltaPlan,
    GoldRowDigest,
    GoldRowPayload,
    GoldSyncRepository,
    GoldSyncResult,
    GoldSyncState,
    GoldTargetSummary,
    select_current_sync_record,
)


def _now(day: int = 1) -> datetime:
    return datetime(2026, 8, day, tzinfo=UTC)


def _record(*, schema: int = 1, feature: int = 1, current: bool = True) -> GoldCatalogRecord:
    return GoldCatalogRecord(
        dataset_id="regime_features_daily",
        build_id="20260822T100000Z",
        status=GoldBuildStatus.COMPLETE,
        current=current,
        started_at_utc=_now(22),
        completed_at_utc=_now(22),
        schema_version=schema,
        feature_version=feature,
        min_timestamp=_now(1),
        max_timestamp=_now(22),
        row_count=22,
        data_path="versions/build_id=20260822T100000Z/data.parquet",
        build_manifest_path="versions/build_id=20260822T100000Z/manifest.json",
        plot_path="versions/build_id=20260822T100000Z/feature_profile.png",
        pruned_at_utc=None,
    )


def test_only_one_gold_dataset_and_exact_postgres_identities() -> None:
    assert POSTGRES_DATASET_ID == "regime_features_daily"
    assert (POSTGRES_CONSUMER_SCHEMA, POSTGRES_CONSUMER_TABLE) == (
        "regime_data",
        "regime_features_daily",
    )
    assert (POSTGRES_SYNC_SCHEMA, POSTGRES_SYNC_STATE_TABLE, POSTGRES_ROW_HASH_TABLE) == (
        "regime_data_sync",
        "gold_sync_state",
        "gold_row_hashes",
    )
    assert "bronze" not in POSTGRES_DATASET_ID
    assert "silver" not in POSTGRES_DATASET_ID


def test_timestamp_contract_is_timestamptz_microseconds_utc() -> None:
    assert POSTGRES_TIMESTAMP_COLUMN == "timestamp_m1"
    assert POSTGRES_TIMESTAMP_SQL_TYPE == "TIMESTAMPTZ(6)"
    assert POSTGRES_SESSION_TIMEZONE == "UTC"


def test_contract_value_objects_and_counts() -> None:
    row = GoldRowPayload(_now(1), tuple(None for _ in GOLD_COLUMNS[1:]))
    digest = GoldRowDigest(_now(1), "a" * 64)
    plan = GoldDeltaPlan((row,), (), (), (), (digest,))
    state = GoldSyncState(
        dataset_id=POSTGRES_DATASET_ID,
        source_build_id="20260822T100000Z",
        data_sha256="b" * 64,
        schema_version=1,
        feature_version=1,
        row_count=1,
        min_timestamp=_now(1),
        max_timestamp=_now(1),
        synced_at_utc=_now(22),
    )
    result = GoldSyncResult(POSTGRES_DATASET_ID, state.source_build_id, 1, 0, 0, 0)
    assert (plan.inserted, plan.updated, plan.deleted, plan.unchanged_count) == (1, 0, 0, 0)
    assert (result.inserted, result.updated, result.deleted, result.unchanged) == (1, 0, 0, 0)
    assert state.min_timestamp == _now(1)


def test_delta_sets_must_be_disjoint() -> None:
    row = GoldRowPayload(_now(1), tuple(None for _ in GOLD_COLUMNS[1:]))
    with pytest.raises(ValueError, match="disjoint"):
        GoldDeltaPlan((row,), (), (_now(1),), (), ())


def test_current_complete_compatible_catalog_record_is_required() -> None:
    assert select_current_sync_record([_record()]).build_id == "20260822T100000Z"
    with pytest.raises(LookupError):
        select_current_sync_record([_record(schema=2)])
    with pytest.raises(LookupError):
        select_current_sync_record([_record(feature=2)])
    with pytest.raises(LookupError):
        select_current_sync_record([_record(current=False)])


def test_repository_protocol_can_be_implemented_without_psycopg() -> None:
    class FakeRepository:
        def ensure_schema(self) -> None:
            return None

        def read_state(self, dataset_id: str) -> GoldSyncState | None:
            del dataset_id
            return None

        def read_digests(self, dataset_id: str) -> tuple[GoldRowDigest, ...]:
            del dataset_id
            return ()

        def apply_delta(
            self,
            dataset_id: str,
            plan: GoldDeltaPlan,
            state: GoldSyncState,
        ) -> None:
            del dataset_id, plan, state

        def summary(self, dataset_id: str) -> GoldTargetSummary:
            del dataset_id
            return GoldTargetSummary(0, None, None)

    repository: GoldSyncRepository = FakeRepository()
    assert repository.read_digests(POSTGRES_DATASET_ID) == ()
    production = Path("application/postgres_sync.py").read_text(encoding="utf-8")
    assert "psycopg" not in production
