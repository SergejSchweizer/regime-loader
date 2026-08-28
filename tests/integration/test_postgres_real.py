"""Real-psycopg coverage for the disposable PostgreSQL CI service."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from threading import Event, Thread

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
    GoldSyncTransaction,
)
from ingestion.postgres_gold_repository import (
    PostgresGoldSyncRepository,
    PostgresLockContentionError,
    PostgresSyncConfig,
    PostgresTimeoutPolicy,
)

pytestmark = pytest.mark.xdist_group("postgres-real")

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
def test_real_postgres_migrations_are_idempotent_and_round_trip(
    repository: PostgresGoldSyncRepository, postgres_dsn: str
) -> None:
    repository.ensure_schema()
    repository.ensure_schema()
    with psycopg.connect(postgres_dsn) as connection:
        columns = connection.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (POSTGRES_CONSUMER_SCHEMA, POSTGRES_CONSUMER_TABLE),
        ).fetchall()
        migrations = connection.execute(
            "SELECT version FROM regime_loader_sync.schema_migrations ORDER BY version"
        ).fetchall()
    assert {column[0] for column in columns} == set(GOLD_COLUMNS)
    assert migrations == [(1,), (2,), (3,)]

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
        connection.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (postgres_module._advisory_lock_key(POSTGRES_DATASET_ID),),
        )
        connection.execute(_ROLLBACK_STATE_SQL, ("c" * 64, timestamp))
        connection.rollback()
        assert connection.execute(
            "SELECT COUNT(*) FROM regime_loader_sync.gold_sync_state WHERE dataset_id = 'rollback'"
        ).fetchone() == (0,)


@pytest.mark.integration
def test_real_postgres_second_locked_transaction_reads_committed_state(
    repository: PostgresGoldSyncRepository,
) -> None:
    repository.ensure_schema()
    timestamp = _timestamp(21)
    row = GoldRowPayload(timestamp, tuple(1.0 for _ in GOLD_COLUMNS[1:]))
    digest = GoldRowDigest(timestamp, "d" * 64)
    state = _state(timestamp)
    first_locked = Event()
    release_first = Event()
    second_read = Event()
    failures: list[Exception] = []
    observed_states: list[GoldSyncState | None] = []

    def first_sync() -> None:
        try:

            def operation(transaction: GoldSyncTransaction) -> None:
                first_locked.set()
                assert release_first.wait(timeout=5)
                transaction.apply_delta(
                    POSTGRES_DATASET_ID,
                    GoldDeltaPlan((row,), (), (), (), (digest,)),
                    state,
                )

            repository.run_locked(operation)
        except Exception as exc:
            failures.append(exc)

    def second_sync() -> None:
        try:

            def operation(transaction: GoldSyncTransaction) -> None:
                observed_states.append(transaction.read_state(POSTGRES_DATASET_ID))
                second_read.set()

            repository.run_locked(operation)
        except Exception as exc:
            failures.append(exc)

    first = Thread(target=first_sync)
    second = Thread(target=second_sync)
    first.start()
    assert first_locked.wait(timeout=5)
    second.start()
    assert not second_read.wait(timeout=0.1)
    release_first.set()
    first.join(timeout=5)
    second.join(timeout=5)

    assert not first.is_alive()
    assert not second.is_alive()
    assert failures == []
    assert observed_states == [state]


@pytest.mark.integration
def test_real_postgres_session_timeouts_bound_lock_and_statement(
    postgres_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    with psycopg.connect(postgres_dsn, autocommit=True) as connection:
        connection.execute("DROP SCHEMA IF EXISTS regime_loader_sync CASCADE")
        connection.execute("DROP SCHEMA IF EXISTS regime_loader CASCADE")
        connection.execute("CREATE SCHEMA regime_loader")
        connection.execute("CREATE SCHEMA regime_loader_sync")
    monkeypatch.setattr(postgres_module, "POSTGRES_HOST", "localhost")
    monkeypatch.setattr(postgres_module, "POSTGRES_PORT", 5432)
    monkeypatch.setattr(postgres_module, "POSTGRES_USER", "regime_loader_test")
    policy = PostgresTimeoutPolicy(
        connect_timeout_seconds=5,
        lock_timeout_ms=200,
        statement_timeout_ms=500,
        idle_in_transaction_timeout_ms=500,
    )
    repository = PostgresGoldSyncRepository(
        PostgresSyncConfig(
            "localhost",
            5432,
            "regime_loader_test",
            "regime_loader_test",
            "regime_loader_test",
            policy,
        )
    )
    repository.ensure_schema()

    session = repository._open()
    try:
        cursor = session.cursor()
        try:
            assert cursor.execute("SHOW application_name").fetchone() == ("regime-loader",)
            assert cursor.execute("SHOW lock_timeout").fetchone() == ("200ms",)
            assert cursor.execute("SHOW statement_timeout").fetchone() == ("500ms",)
            assert cursor.execute("SHOW idle_in_transaction_session_timeout").fetchone() == (
                "500ms",
            )
            started = time.monotonic()
            with pytest.raises(psycopg.errors.QueryCanceled):
                cursor.execute("SELECT pg_sleep(1)")
            assert time.monotonic() - started < 1
        finally:
            cursor.close()
    finally:
        session.close()

    with psycopg.connect(postgres_dsn) as lock_holder:
        lock_holder.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (postgres_module._advisory_lock_key(POSTGRES_DATASET_ID),),
        )
        started = time.monotonic()
        with pytest.raises(PostgresLockContentionError):
            repository.apply_delta(
                POSTGRES_DATASET_ID, GoldDeltaPlan((), (), (), (), ()), _state(_timestamp(1))
            )
        assert time.monotonic() - started < 1
        lock_holder.rollback()
    assert repository.summary(POSTGRES_DATASET_ID).row_count == 0


@pytest.mark.parametrize(
    "drift_sql",
    (
        "ALTER TABLE regime_loader_sync.gold_row_hashes DROP COLUMN row_sha256",
        "ALTER TABLE regime_loader.regime_features_daily ADD COLUMN forbidden INTEGER",
        "ALTER TABLE regime_loader_sync.gold_sync_state "
        "ALTER COLUMN schema_version TYPE TEXT USING schema_version::text",
        "ALTER TABLE regime_loader.regime_features_daily "
        "ALTER COLUMN timestamp_m1 TYPE TIMESTAMPTZ(3)",
        "ALTER TABLE regime_loader_sync.gold_sync_state ALTER COLUMN source_build_id DROP NOT NULL",
        "ALTER TABLE regime_loader.regime_features_daily "
        "DROP CONSTRAINT regime_features_daily_pkey",
    ),
    ids=("missing", "extra", "wrong-type", "wrong-precision", "wrong-nullability", "wrong-key"),
)
def test_real_postgres_schema_drift_fails_closed_before_sync(
    repository: PostgresGoldSyncRepository,
    postgres_dsn: str,
    drift_sql: str,
) -> None:
    repository.ensure_schema()
    with psycopg.connect(postgres_dsn, autocommit=True) as connection:
        connection.execute(drift_sql)

    with pytest.raises(
        postgres_module.PostgresGoldRepositoryError, match="schema initialization failed"
    ):
        repository.ensure_schema()
