"""Real-psycopg coverage for the disposable PostgreSQL CI service."""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from threading import Event, Thread

import polars as pl
import psycopg
import pytest

import ingestion.postgres_gold_repository as postgres_module
from application.gold_catalog import GoldBuildStatus, GoldCatalogRecord
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
from application.postgres_sync_service import GoldPostgresDeltaSync
from ingestion.postgres_gold_repository import (
    PostgresAdminConfig,
    PostgresGoldSchemaMigrator,
    PostgresGoldSyncRepository,
    PostgresLockContentionError,
    PostgresSyncConfig,
    PostgresTimeoutPolicy,
)
from scripts.provision_postgres_role import provision_sql

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


@pytest.fixture
def migrator(postgres_dsn: str, monkeypatch: pytest.MonkeyPatch) -> PostgresGoldSchemaMigrator:
    monkeypatch.setattr(postgres_module, "POSTGRES_HOST", "localhost")
    monkeypatch.setattr(postgres_module, "POSTGRES_PORT", 5432)
    return PostgresGoldSchemaMigrator(
        PostgresAdminConfig(
            "localhost", 5432, "regime_loader_admin", "regime_loader_test", "admin"
        ),
        connection_factory=lambda _config: psycopg.connect(postgres_dsn),
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


def _gold_frame(timestamp: datetime) -> pl.DataFrame:
    data: dict[str, list[object]] = {"timestamp_m1": [timestamp]}
    for column in GOLD_COLUMNS[1:]:
        data[column] = [1.0]
    return pl.DataFrame(data).with_columns(pl.col("timestamp_m1").cast(pl.Datetime("us", "UTC")))


class _Catalog:
    def __init__(self, record: GoldCatalogRecord) -> None:
        self._record = record

    def read(self) -> list[GoldCatalogRecord]:
        return [self._record]


class _Source:
    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame

    def validate_bundle(self, record: GoldCatalogRecord) -> None:
        del record

    def sha256_path(self, relative_data_path: str) -> str:
        del relative_data_path
        return "a" * 64

    def read_path(self, relative_data_path: str) -> pl.DataFrame:
        del relative_data_path
        return self._frame


def _sync_service(
    repository: PostgresGoldSyncRepository, timestamp: datetime
) -> GoldPostgresDeltaSync:
    frame = _gold_frame(timestamp)
    record = GoldCatalogRecord(
        dataset_id=POSTGRES_DATASET_ID,
        build_id="20260828T000000Z",
        status=GoldBuildStatus.COMPLETE,
        current=True,
        started_at_utc=_timestamp(1),
        completed_at_utc=_timestamp(2),
        schema_version=2,
        feature_version=1,
        min_timestamp=timestamp,
        max_timestamp=timestamp,
        row_count=1,
        data_path="versions/build_id=20260828T000000Z/data.parquet",
        build_manifest_path="versions/build_id=20260828T000000Z/manifest.json",
        plot_path="versions/build_id=20260828T000000Z/feature_profile.png",
        pruned_at_utc=None,
    )
    return GoldPostgresDeltaSync(
        catalog=_Catalog(record),
        source=_Source(frame),
        repository=repository,
        clock=lambda: _timestamp(28),
    )


@pytest.mark.integration
def test_real_postgres_migrations_are_idempotent_and_round_trip(
    repository: PostgresGoldSyncRepository, migrator: PostgresGoldSchemaMigrator, postgres_dsn: str
) -> None:
    migrator.migrate()
    migrator.migrate()
    repository.preflight_schema()
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
    digest = GoldRowDigest(timestamp, postgres_module.gold_row_sha256(row))
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
@pytest.mark.parametrize(
    ("tamper", "statement"),
    (
        (
            "changed consumer row",
            'UPDATE regime_loader.regime_features_daily SET "vix_level" = 999.0',
        ),
        ("missing consumer row", "DELETE FROM regime_loader.regime_features_daily"),
        (
            "changed digest",
            "UPDATE regime_loader_sync.gold_row_hashes SET row_sha256 = 'f' || repeat('f', 63)",
        ),
        ("missing digest", "DELETE FROM regime_loader_sync.gold_row_hashes"),
        (
            "stale state",
            "UPDATE regime_loader_sync.gold_sync_state SET data_sha256 = 'f' || repeat('f', 63)",
        ),
        ("missing state", "DELETE FROM regime_loader_sync.gold_sync_state"),
    ),
)
def test_real_postgres_tampering_fails_closed(
    repository: PostgresGoldSyncRepository,
    migrator: PostgresGoldSchemaMigrator,
    postgres_dsn: str,
    tamper: str,
    statement: str,
) -> None:
    migrator.migrate()
    timestamp = _timestamp(20)
    service = _sync_service(repository, timestamp)
    assert service.sync().inserted == 1

    with psycopg.connect(postgres_dsn) as connection:
        connection.execute(statement)

    with pytest.raises(postgres_module.PostgresGoldRepositoryError, match="locked transaction"):
        service.sync()


@pytest.mark.integration
def test_real_postgres_second_locked_transaction_reads_committed_state(
    repository: PostgresGoldSyncRepository,
    migrator: PostgresGoldSchemaMigrator,
) -> None:
    migrator.migrate()
    timestamp = _timestamp(21)
    row = GoldRowPayload(timestamp, tuple(1.0 for _ in GOLD_COLUMNS[1:]))
    digest = GoldRowDigest(timestamp, postgres_module.gold_row_sha256(row))
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
    postgres_dsn: str, monkeypatch: pytest.MonkeyPatch, migrator: PostgresGoldSchemaMigrator
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
    migrator.migrate()

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
    migrator: PostgresGoldSchemaMigrator,
    postgres_dsn: str,
    drift_sql: str,
) -> None:
    migrator.migrate()
    with psycopg.connect(postgres_dsn, autocommit=True) as connection:
        connection.execute(drift_sql)

    with pytest.raises(
        postgres_module.PostgresGoldRepositoryError, match="schema preflight failed"
    ):
        repository.preflight_schema()


@pytest.mark.integration
def test_real_postgres_runtime_role_is_dml_only(postgres_dsn: str) -> None:
    with psycopg.connect(postgres_dsn) as admin_connection:
        admin_connection.execute(
            provision_sql("regime_loader_test", "runtime-secret", "regime_loader_test")
        )
        admin_connection.execute('CREATE ROLE "runtime-grant-probe" NOLOGIN')
        admin_connection.execute("CREATE SCHEMA unrelated")
        admin_connection.execute("CREATE TABLE unrelated.private_data (id INTEGER)")
        admin_connection.commit()

    runtime_dsn = postgres_dsn.replace(
        "regime_loader_test:regime_loader_test", "regime-loader:runtime-secret"
    )
    with psycopg.connect(runtime_dsn) as runtime_connection:
        runtime_connection.execute("SELECT * FROM regime_loader_sync.schema_migrations")
        runtime_connection.execute("DELETE FROM regime_loader_sync.gold_sync_state")
        runtime_connection.rollback()
        for operation, statement in (
            ("CREATE", "CREATE TABLE regime_loader.forbidden (id INTEGER)"),
            (
                "ALTER",
                "ALTER TABLE regime_loader.regime_features_daily ADD COLUMN forbidden INTEGER",
            ),
            ("DROP", "DROP TABLE regime_loader.regime_features_daily"),
            ("unrelated SELECT", "SELECT * FROM unrelated.private_data"),
        ):
            try:
                runtime_connection.execute(statement)
            except psycopg.errors.InsufficientPrivilege:
                pass
            else:
                pytest.fail(f"runtime role unexpectedly permitted {operation}")
            finally:
                runtime_connection.rollback()

        runtime_connection.execute(
            'GRANT SELECT ON regime_loader.regime_features_daily TO "runtime-grant-probe"'
        )
        grant_result = runtime_connection.execute(
            "SELECT has_table_privilege("
            "'runtime-grant-probe', "
            "'regime_loader.regime_features_daily', "
            "'SELECT'"
            ")"
        )
        assert grant_result.fetchone() == (False,)
        runtime_connection.rollback()
