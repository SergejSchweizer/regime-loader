"""Real-psycopg coverage for the disposable PostgreSQL CI service."""

from __future__ import annotations

import os
from datetime import UTC, datetime

import psycopg
import pytest

import ingestion.postgres_gold_repository as postgres_module
from application.gold_frame import GOLD_COLUMNS
from application.postgres_sync import (
    POSTGRES_CONSUMER_SCHEMA,
    POSTGRES_CONSUMER_TABLE,
    POSTGRES_DATASET_ID,
    GoldDeltaPlan,
    GoldRowDigest,
    GoldRowPayload,
    GoldSyncState,
)
from ingestion.postgres_gold_repository import PostgresGoldSyncRepository, PostgresSyncConfig

_ROLLBACK_STATE_SQL = """
INSERT INTO regime_loader_sync.gold_sync_state (
    dataset_id, source_build_id, data_sha256, schema_version, feature_version,
    row_count, min_timestamp, max_timestamp, synced_at_utc
) VALUES ('rollback', 'build', %s, 2, 1, 0, NULL, NULL, %s)
"""


@pytest.fixture
def postgres_dsn() -> str:
    dsn = os.environ.get("POSTGRES_TEST_DSN")
    if dsn is None:
        pytest.skip("POSTGRES_TEST_DSN is required for disposable PostgreSQL integration coverage")
    return dsn


@pytest.fixture
def repository(postgres_dsn: str, monkeypatch: pytest.MonkeyPatch) -> PostgresGoldSyncRepository:
    with psycopg.connect(postgres_dsn, autocommit=True) as connection:
        connection.execute("DROP SCHEMA IF EXISTS regime_loader_sync CASCADE")
        connection.execute("DROP SCHEMA IF EXISTS regime_loader CASCADE")
        connection.execute("CREATE SCHEMA regime_loader")
        connection.execute("CREATE SCHEMA regime_loader_sync")
    monkeypatch.setattr(postgres_module, "POSTGRES_HOST", "localhost")
    monkeypatch.setattr(postgres_module, "POSTGRES_PORT", 5432)
    monkeypatch.setattr(postgres_module, "POSTGRES_USER", "regime_loader_test")
    return PostgresGoldSyncRepository(
        PostgresSyncConfig(
            "localhost", 5432, "regime_loader_test", "regime_loader_test", "regime_loader_test"
        )
    )


def _timestamp(day: int) -> datetime:
    return datetime(2026, 8, day, 12, 34, 56, 123456, tzinfo=UTC)


def _state(timestamp: datetime) -> GoldSyncState:
    return GoldSyncState(
        dataset_id=POSTGRES_DATASET_ID,
        source_build_id="20260828T000000Z",
        data_sha256="a" * 64,
        schema_version=2,
        feature_version=1,
        row_count=1,
        min_timestamp=timestamp,
        max_timestamp=timestamp,
        synced_at_utc=_timestamp(28),
    )


@pytest.mark.integration
def test_real_postgres_schema_utc_transaction_lock_and_round_trip(
    repository: PostgresGoldSyncRepository, postgres_dsn: str
) -> None:
    with psycopg.connect(postgres_dsn, autocommit=True) as connection:
        connection.execute(
            f"CREATE TABLE {POSTGRES_CONSUMER_SCHEMA}.{POSTGRES_CONSUMER_TABLE} "
            "(timestamp_m1 TIMESTAMPTZ(6) PRIMARY KEY)"
        )
    repository.ensure_schema()
    repository.ensure_schema()
    with psycopg.connect(postgres_dsn) as connection:
        columns = connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (POSTGRES_CONSUMER_SCHEMA, POSTGRES_CONSUMER_TABLE),
        ).fetchall()
    assert {column[0] for column in columns} == set(GOLD_COLUMNS)

    timestamp = _timestamp(20)
    row = GoldRowPayload(timestamp, tuple(1.0 for _ in GOLD_COLUMNS[1:]))
    digest = GoldRowDigest(timestamp, "b" * 64)
    repository.apply_delta(
        POSTGRES_DATASET_ID, GoldDeltaPlan((row,), (), (), (), (digest,)), _state(timestamp)
    )

    assert repository.summary(POSTGRES_DATASET_ID).row_count == 1
    assert repository.read_digests(POSTGRES_DATASET_ID) == (digest,)
    assert repository.read_state(POSTGRES_DATASET_ID) == _state(timestamp)
    repository_connection = repository._open()
    try:
        cursor = repository_connection.cursor()
        try:
            cursor.execute("SHOW TIME ZONE")
            assert cursor.fetchone() == ("UTC",)
        finally:
            cursor.close()
    finally:
        repository_connection.close()
    with psycopg.connect(postgres_dsn) as connection:
        connection.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (POSTGRES_DATASET_ID,))
        connection.execute(_ROLLBACK_STATE_SQL, ("c" * 64, timestamp))
        connection.rollback()
        assert connection.execute(
            "SELECT COUNT(*) FROM regime_loader_sync.gold_sync_state WHERE dataset_id = 'rollback'"
        ).fetchone() == (0,)
