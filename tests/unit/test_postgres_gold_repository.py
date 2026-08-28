from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

import ingestion.postgres_gold_repository as module
from application.gold_frame import GOLD_COLUMNS
from application.postgres_sync import (
    POSTGRES_DATASET_ID,
    GoldDeltaPlan,
    GoldRowDigest,
    GoldRowPayload,
    GoldSyncState,
    GoldSyncTransaction,
    GoldTargetSummary,
)
from ingestion.postgres_gold_repository import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    PostgresAdminConfig,
    PostgresGoldRepositoryError,
    PostgresGoldSchemaMigrator,
    PostgresGoldSchemaReconstructor,
    PostgresGoldSyncRepository,
    PostgresLockContentionError,
    PostgresSyncConfig,
    PostgresTimeoutPolicy,
)


def _ts(day: int) -> datetime:
    return datetime(2026, 8, day, tzinfo=UTC)


def _config(password: str = "repo-secret") -> PostgresSyncConfig:
    return PostgresSyncConfig(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, "quant_data", password)


def _admin_config(password: str = "admin-secret") -> PostgresAdminConfig:
    return PostgresAdminConfig(
        POSTGRES_HOST, POSTGRES_PORT, "regime-loader-admin", "quant_data", password
    )


def _row(day: int, value: float) -> GoldRowPayload:
    return GoldRowPayload(_ts(day), tuple(value for _ in GOLD_COLUMNS[1:]))


def _state(
    *, count: int = 2, minimum: datetime | None = None, maximum: datetime | None = None
) -> GoldSyncState:
    if minimum is None and count:
        minimum = _ts(1)
    if maximum is None and count:
        maximum = _ts(2)
    return GoldSyncState(
        dataset_id=POSTGRES_DATASET_ID,
        source_build_id="20260822T100000Z",
        data_sha256="a" * 64,
        schema_version=1,
        feature_version=1,
        row_count=count,
        min_timestamp=minimum,
        max_timestamp=maximum,
        synced_at_utc=_ts(22),
    )


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.one: tuple[object, ...] | None = None
        self.many: list[tuple[object, ...]] = []
        self._rowcount = 1

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def execute(self, query: str, params: object = None) -> object:
        self.connection.events.append(("execute", query, params))
        self._rowcount = self.connection.rowcounts.get(query, 1)
        if self.connection.fail_on and self.connection.fail_on in query:
            raise RuntimeError("database failure repo-secret")
        if query.startswith("SELECT version"):
            self.many = []
        elif "FROM information_schema.tables" in query:
            self.many = sorted(
                (specification.schema, specification.name)
                for specification in module._SCHEMA_SPECIFICATION
            )
        elif "FROM information_schema.columns" in query:
            self.many = [
                (
                    specification.schema,
                    specification.name,
                    ordinal,
                    column.name,
                    column.data_type,
                    column.precision if column.data_type == "timestamp with time zone" else None,
                    column.precision if column.data_type == "character" else None,
                    "YES" if column.nullable else "NO",
                )
                for specification in module._SCHEMA_SPECIFICATION
                for ordinal, column in enumerate(specification.columns, start=1)
            ]
        elif "FROM pg_constraint" in query:
            self.many = [
                (
                    specification.schema,
                    specification.name,
                    "p",
                    specification.primary_key,
                )
                for specification in module._SCHEMA_SPECIFICATION
            ]
        elif query.startswith("SELECT dataset_id"):
            self.one = self.connection.state_row
        elif query.startswith("SELECT timestamp_m1, row_sha256"):
            self.many = list(self.connection.digest_rows)
        elif query.startswith('SELECT "timestamp_m1"'):
            self.many = list(self.connection.consumer_rows)
        elif query.startswith("SELECT COUNT"):
            self.one = self.connection.summary_row
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self.one

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.many

    def close(self) -> None:
        self.connection.events.append(("cursor-close", None, None))


class FakeConnection:
    def __init__(
        self,
        *,
        state_row: tuple[object, ...] | None = None,
        digest_rows: tuple[tuple[object, ...], ...] = (),
        consumer_rows: tuple[tuple[object, ...], ...] = (),
        rowcounts: dict[str, int] | None = None,
        summary_row: tuple[object, ...] = (0, None, None),
        fail_on: str | None = None,
    ) -> None:
        self.state_row = state_row
        self.digest_rows = digest_rows
        self.consumer_rows = consumer_rows
        self.rowcounts = {} if rowcounts is None else rowcounts
        self.summary_row = summary_row
        self.fail_on = fail_on
        self.events: list[tuple[str, object, object]] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.events.append(("commit", None, None))

    def rollback(self) -> None:
        self.events.append(("rollback", None, None))

    def close(self) -> None:
        self.events.append(("close", None, None))


class Factory:
    def __init__(self, *connections: FakeConnection) -> None:
        self.connections = list(connections)
        self.configs: list[PostgresSyncConfig] = []

    def __call__(self, config: PostgresSyncConfig) -> FakeConnection:
        self.configs.append(config)
        if not self.connections:
            raise RuntimeError("no fake connection")
        return self.connections.pop(0)


def _execute_queries(connection: FakeConnection) -> list[str]:
    return [str(query) for kind, query, _ in connection.events if kind == "execute"]


def test_schema_reconstructor_recreates_only_loader_schemas_and_revalidates_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = FakeConnection()
    verified = False

    def verify(cursor: object) -> None:
        nonlocal verified
        verified = True

    monkeypatch.setattr(module.PostgresGoldSyncRepository, "_assert_schema_contract", verify)

    PostgresGoldSchemaReconstructor(
        _admin_config(), connection_factory=Factory(connection)
    ).recreate()

    queries = _execute_queries(connection)
    assert 'DROP SCHEMA IF EXISTS "regime_loader" CASCADE' in queries
    assert 'DROP SCHEMA IF EXISTS "regime_loader_sync" CASCADE' in queries
    assert all("DROP SCHEMA" not in query or "regime_loader" in query for query in queries)
    assert 'SET ROLE "regime-loader-owner"' in queries
    assert 'GRANT USAGE ON SCHEMA "regime_loader" TO "regime-loader"' in queries
    assert verified
    assert ("commit", None, None) in connection.events


def test_schema_reconstructor_rolls_back_and_sanitizes_failure() -> None:
    connection = FakeConnection(fail_on="DROP SCHEMA")

    with pytest.raises(PostgresGoldRepositoryError) as error:
        PostgresGoldSchemaReconstructor(
            _admin_config(), connection_factory=Factory(connection)
        ).recreate()

    assert "repo-secret" not in str(error.value)
    assert ("rollback", None, None) in connection.events


def test_config_requires_exact_endpoint_role_and_hides_password() -> None:
    config = PostgresSyncConfig.from_mapping(
        {
            "PGHOST": "10.10.1.3",
            "PGPORT": "54321",
            "PGUSER": "regime-loader",
            "PGDATABASE": "quant_data",
            "PGPASSWORD": "repo-secret",
        }
    )
    assert (config.host, config.port, config.user) == ("10.10.1.3", 54321, "regime-loader")
    assert "repo-secret" not in repr(config)
    with pytest.raises(ValueError, match="host"):
        PostgresSyncConfig("localhost", 54321, POSTGRES_USER, "quant_data", "x")
    with pytest.raises(ValueError, match="port"):
        PostgresSyncConfig(POSTGRES_HOST, 5432, POSTGRES_USER, "quant_data", "x")
    with pytest.raises(ValueError, match="user"):
        PostgresSyncConfig(POSTGRES_HOST, POSTGRES_PORT, "postgres", "quant_data", "x")
    with pytest.raises(ValueError, match="database"):
        PostgresSyncConfig(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, "", "x")
    with pytest.raises(ValueError, match="password"):
        PostgresSyncConfig(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, "quant_data", "")
    with pytest.raises(ValueError, match="PGPORT"):
        PostgresSyncConfig.from_mapping({"PGPORT": "invalid"})


def test_admin_config_uses_distinct_namespace_role_and_redacted_password() -> None:
    config = PostgresAdminConfig.from_mapping(
        {
            "MARKET_REGIME_POSTGRES_ADMIN_HOST": "10.10.1.3",
            "MARKET_REGIME_POSTGRES_ADMIN_PORT": "54321",
            "MARKET_REGIME_POSTGRES_ADMIN_USER": "regime-loader-admin",
            "MARKET_REGIME_POSTGRES_ADMIN_DATABASE": "quant_data",
            "MARKET_REGIME_POSTGRES_ADMIN_PASSWORD": "admin-secret",
        }
    )
    assert config.user == "regime-loader-admin"
    assert "admin-secret" not in repr(config)
    with pytest.raises(ValueError, match="host"):
        PostgresAdminConfig("localhost", POSTGRES_PORT, "regime-loader-admin", "quant_data", "x")
    with pytest.raises(ValueError, match="port"):
        PostgresAdminConfig(POSTGRES_HOST, 5432, "regime-loader-admin", "quant_data", "x")
    with pytest.raises(ValueError, match="user"):
        PostgresAdminConfig(POSTGRES_HOST, POSTGRES_PORT, "", "quant_data", "x")
    with pytest.raises(ValueError, match="database"):
        PostgresAdminConfig(POSTGRES_HOST, POSTGRES_PORT, "regime-loader-admin", "", "x")
    with pytest.raises(ValueError, match="password"):
        PostgresAdminConfig(POSTGRES_HOST, POSTGRES_PORT, "regime-loader-admin", "quant_data", "")
    with pytest.raises(ValueError, match="differ from runtime"):
        PostgresAdminConfig(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, "quant_data", "x")
    with pytest.raises(ValueError, match="password must differ"):
        PostgresAdminConfig.from_mapping(
            {
                "MARKET_REGIME_POSTGRES_ADMIN_HOST": "10.10.1.3",
                "MARKET_REGIME_POSTGRES_ADMIN_PORT": "54321",
                "MARKET_REGIME_POSTGRES_ADMIN_USER": "regime-loader-admin",
                "MARKET_REGIME_POSTGRES_ADMIN_DATABASE": "quant_data",
                "MARKET_REGIME_POSTGRES_ADMIN_PASSWORD": "shared-secret",
                "PGPASSWORD": "shared-secret",
            }
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("connect_timeout_seconds", 0),
        ("lock_timeout_ms", 0),
        ("statement_timeout_ms", -1),
        ("idle_in_transaction_timeout_ms", 0),
    ],
)
def test_timeout_policy_rejects_non_positive_bounds(field: str, value: int) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        PostgresTimeoutPolicy(**{field: value})


def test_default_connection_passes_connect_timeout_and_application_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def connect(**kwargs: object) -> FakeConnection:
        captured.update(kwargs)
        return FakeConnection()

    monkeypatch.setattr(module.psycopg, "connect", connect)
    policy = PostgresTimeoutPolicy(connect_timeout_seconds=9)
    module._default_connection(
        PostgresSyncConfig(
            POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, "quant_data", "secret", policy
        )
    )
    assert captured["connect_timeout"] == 9
    assert captured["application_name"] == "regime-loader"


def test_admin_schema_migrations_are_gold_only_timestamptz_and_idempotent() -> None:
    connection = FakeConnection()
    factory = Factory(connection)
    migrator = PostgresGoldSchemaMigrator(_admin_config(), connection_factory=factory)
    migrator.migrate()
    queries = _execute_queries(connection)
    assert queries[:5] == [
        "SET application_name = 'regime-loader'",
        "SET TIME ZONE 'UTC'",
        "SET lock_timeout = '5000ms'",
        "SET statement_timeout = '30000ms'",
        "SET idle_in_transaction_session_timeout = '30000ms'",
    ]
    ddl = "\n".join(queries[5:])
    assert '"timestamp_m1" TIMESTAMPTZ(6) NOT NULL PRIMARY KEY' in ddl
    for column in GOLD_COLUMNS[1:]:
        assert f'"{column}" DOUBLE PRECISION NULL' in ddl
    assert '"regime_loader"."regime_features_daily"' in ddl
    assert '"regime_loader_sync"."gold_sync_state"' in ddl
    assert '"regime_loader_sync"."gold_row_hashes"' in ddl
    assert '"regime_loader_sync"."schema_migrations"' in ddl
    assert queries.count(module._CONSUMER_DDL) == 1
    assert queries.count(module._SYNC_STATE_DDL) == 1
    assert queries.count(module._ROW_HASH_DDL) == 1
    assert "TRUNCATE" not in ddl
    assert "DROP TABLE" not in ddl
    assert "CREATE SCHEMA" not in ddl
    assert ("commit", None, None) in connection.events


def test_runtime_schema_preflight_is_read_only_and_contains_no_ddl() -> None:
    connection = FakeConnection()
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))

    repository.preflight_schema()

    queries = _execute_queries(connection)
    assert "SET TRANSACTION READ ONLY" in queries
    prohibited = ("CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT")
    assert not any(query.startswith(prohibited) for query in queries)
    assert ("rollback", None, None) in connection.events
    assert ("commit", None, None) not in connection.events


def test_read_state_round_trips_exact_sync_metadata() -> None:
    state = _state()
    connection = FakeConnection(
        state_row=(
            state.dataset_id,
            state.source_build_id,
            state.data_sha256,
            state.schema_version,
            state.feature_version,
            state.row_count,
            state.min_timestamp,
            state.max_timestamp,
            state.synced_at_utc,
        )
    )
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    assert repository.read_state(POSTGRES_DATASET_ID) == state

    empty = FakeConnection(state_row=None)
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(empty))
    assert repository.read_state(POSTGRES_DATASET_ID) is None


@pytest.mark.parametrize(
    "invalid_timestamp",
    [
        datetime(2026, 8, 1),
        datetime(2026, 8, 1, tzinfo=timezone(timedelta(hours=1))),
    ],
)
def test_read_state_rejects_invalid_database_timestamp(invalid_timestamp: datetime) -> None:
    state = _state()
    connection = FakeConnection(
        state_row=(
            state.dataset_id,
            state.source_build_id,
            state.data_sha256,
            state.schema_version,
            state.feature_version,
            state.row_count,
            invalid_timestamp,
            state.max_timestamp,
            state.synced_at_utc,
        )
    )
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))

    with pytest.raises(PostgresGoldRepositoryError, match="sync-state read failed"):
        repository.read_state(POSTGRES_DATASET_ID)


def test_read_digests_fetches_only_timestamp_and_hash_in_order() -> None:
    connection = FakeConnection(digest_rows=((_ts(1), "a" * 64), (_ts(2), "b" * 64)))
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    assert repository.read_digests(POSTGRES_DATASET_ID) == (
        GoldRowDigest(_ts(1), "a" * 64),
        GoldRowDigest(_ts(2), "b" * 64),
    )
    digest_query = next(
        query for query in _execute_queries(connection) if query.startswith("SELECT timestamp")
    )
    assert "row_sha256" in digest_query
    for feature in GOLD_COLUMNS[1:]:
        assert feature not in digest_query


def test_read_digests_rejects_invalid_database_timestamp() -> None:
    connection = FakeConnection(digest_rows=((datetime(2026, 8, 1), "a" * 64),))
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))

    with pytest.raises(PostgresGoldRepositoryError, match="digest read failed"):
        repository.read_digests(POSTGRES_DATASET_ID)


def test_read_consumer_digests_hashes_complete_rows_in_timestamp_order() -> None:
    first = _row(1, 1.0)
    second = _row(2, 2.0)
    connection = FakeConnection(
        consumer_rows=(
            (first.timestamp_m1, *first.values),
            (second.timestamp_m1, *second.values),
        )
    )
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))

    assert repository.read_consumer_digests(POSTGRES_DATASET_ID) == (
        GoldRowDigest(first.timestamp_m1, module.gold_row_sha256(first)),
        GoldRowDigest(second.timestamp_m1, module.gold_row_sha256(second)),
    )
    consumer_query = next(
        query for query in _execute_queries(connection) if query.startswith('SELECT "timestamp_m1"')
    )
    assert all(f'"{column}"' in consumer_query for column in GOLD_COLUMNS)


def test_summary_returns_count_and_utc_bounds() -> None:
    connection = FakeConnection(summary_row=(2, _ts(1), _ts(2)))
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    assert repository.summary(POSTGRES_DATASET_ID) == GoldTargetSummary(2, _ts(1), _ts(2))


def test_summary_rejects_non_utc_database_timestamp() -> None:
    connection = FakeConnection(
        summary_row=(2, _ts(1), datetime(2026, 8, 2, tzinfo=timezone(timedelta(hours=2))))
    )
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))

    with pytest.raises(PostgresGoldRepositoryError, match="summary read failed"):
        repository.summary(POSTGRES_DATASET_ID)


def test_apply_delta_is_locked_exact_and_state_is_last_before_commit() -> None:
    insert = _row(2, 2.0)
    update = _row(1, 10.0)
    delete = _ts(3)
    digests = (
        GoldRowDigest(update.timestamp_m1, module.gold_row_sha256(update)),
        GoldRowDigest(insert.timestamp_m1, module.gold_row_sha256(insert)),
    )
    plan = GoldDeltaPlan((insert,), (update,), (delete,), (), digests)
    state = _state()
    connection = FakeConnection(
        digest_rows=tuple((digest.timestamp_m1, digest.row_sha256) for digest in digests),
        consumer_rows=(
            (update.timestamp_m1, *update.values),
            (insert.timestamp_m1, *insert.values),
        ),
        summary_row=(2, _ts(1), _ts(2)),
    )
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    repository.apply_delta(POSTGRES_DATASET_ID, plan, state)

    queries = _execute_queries(connection)
    lock_index = next(i for i, query in enumerate(queries) if "pg_advisory_xact_lock" in query)
    insert_index = next(
        i for i, query in enumerate(queries) if query.startswith('INSERT INTO "regime_loader"')
    )
    update_index = next(
        i for i, query in enumerate(queries) if query.startswith('UPDATE "regime_loader"')
    )
    delete_index = next(
        i for i, query in enumerate(queries) if query.startswith('DELETE FROM "regime_loader"')
    )
    summary_index = next(i for i, query in enumerate(queries) if query.startswith("SELECT COUNT"))
    state_index = next(
        i
        for i, query in enumerate(queries)
        if 'INSERT INTO "regime_loader_sync"."gold_sync_state"' in query
    )
    assert lock_index < insert_index < update_index < delete_index < summary_index < state_index
    assert queries.count(module._INSERT_ROW_SQL) == 1
    assert queries.count(module._UPDATE_ROW_SQL) == 1
    assert queries.count(module._DELETE_ROW_SQL) == 1
    assert "TRUNCATE" not in "\n".join(queries)
    assert ("commit", None, None) in connection.events
    assert ("rollback", None, None) not in connection.events


def test_locked_transaction_uses_deterministic_namespaced_key_before_reads() -> None:
    connection = FakeConnection()
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    expected_key = module._advisory_lock_key(POSTGRES_DATASET_ID)

    def operation(transaction: GoldSyncTransaction) -> None:
        transaction.read_state(POSTGRES_DATASET_ID)

    repository.run_locked(operation)

    queries = _execute_queries(connection)
    lock_index = next(i for i, query in enumerate(queries) if "pg_advisory_xact_lock" in query)
    state_index = next(
        i for i, query in enumerate(queries) if query.startswith("SELECT dataset_id")
    )
    lock_event = next(
        event
        for event in connection.events
        if event[0] == "execute" and "pg_advisory_xact_lock" in str(event[1])
    )
    assert lock_index < state_index
    assert lock_event[2] == (expected_key,)
    assert expected_key == module._advisory_lock_key(POSTGRES_DATASET_ID)
    assert "regime-loader" in module.POSTGRES_ADVISORY_LOCK_NAMESPACE


def test_first_bootstrap_can_insert_complete_source_without_full_reload_sql() -> None:
    rows = (_row(1, 1.0), _row(2, 2.0))
    digests = (
        GoldRowDigest(rows[0].timestamp_m1, module.gold_row_sha256(rows[0])),
        GoldRowDigest(rows[1].timestamp_m1, module.gold_row_sha256(rows[1])),
    )
    plan = GoldDeltaPlan(rows, (), (), (), digests)
    connection = FakeConnection(
        digest_rows=tuple((digest.timestamp_m1, digest.row_sha256) for digest in digests),
        consumer_rows=tuple((row.timestamp_m1, *row.values) for row in rows),
        summary_row=(2, _ts(1), _ts(2)),
    )
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    repository.apply_delta(POSTGRES_DATASET_ID, plan, _state())
    queries = _execute_queries(connection)
    assert queries.count(module._INSERT_ROW_SQL) == 2
    assert all("TRUNCATE" not in query and "DROP TABLE" not in query for query in queries)


def test_transaction_failure_rolls_back_and_error_is_sanitized() -> None:
    row = _row(1, 1.0)
    plan = GoldDeltaPlan(
        (),
        (row,),
        (),
        (),
        (GoldRowDigest(_ts(1), "a" * 64),),
    )
    connection = FakeConnection(summary_row=(1, _ts(1), _ts(1)), fail_on="UPDATE")
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    with pytest.raises(PostgresGoldRepositoryError) as exc_info:
        repository.apply_delta(
            POSTGRES_DATASET_ID,
            plan,
            _state(count=1, minimum=_ts(1), maximum=_ts(1)),
        )
    assert "repo-secret" not in str(exc_info.value)
    assert ("rollback", None, None) in connection.events
    assert ("commit", None, None) not in connection.events


def test_missing_planned_update_rolls_back_before_state_write() -> None:
    row = _row(1, 1.0)
    plan = GoldDeltaPlan(
        (),
        (row,),
        (),
        (),
        (GoldRowDigest(row.timestamp_m1, module.gold_row_sha256(row)),),
    )
    connection = FakeConnection(
        rowcounts={module._UPDATE_ROW_SQL: 0},
        summary_row=(1, _ts(1), _ts(1)),
    )
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))

    with pytest.raises(PostgresGoldRepositoryError):
        repository.apply_delta(
            POSTGRES_DATASET_ID,
            plan,
            _state(count=1, minimum=_ts(1), maximum=_ts(1)),
        )

    queries = _execute_queries(connection)
    assert not any(
        'INSERT INTO "regime_loader_sync"."gold_sync_state"' in query for query in queries
    )
    assert ("rollback", None, None) in connection.events


def test_lock_timeout_is_typed_sanitized_and_rolls_back() -> None:
    class LockTimeoutError(RuntimeError):
        sqlstate = "55P03"

    class LockTimeoutCursor(FakeCursor):
        def execute(self, query: str, params: object = None) -> object:
            if "pg_advisory_xact_lock" in query:
                raise LockTimeoutError("postgresql://secret@host")
            return super().execute(query, params)

    class LockTimeoutConnection(FakeConnection):
        def cursor(self) -> LockTimeoutCursor:
            return LockTimeoutCursor(self)

    connection = LockTimeoutConnection(summary_row=(0, None, None))
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    with pytest.raises(PostgresLockContentionError) as exc_info:
        repository.apply_delta(
            POSTGRES_DATASET_ID, GoldDeltaPlan((), (), (), (), ()), _state(count=0)
        )
    assert "secret" not in str(exc_info.value)
    assert ("rollback", None, None) in connection.events


def test_statement_timeout_is_typed_and_sanitized() -> None:
    class StatementTimeoutError(RuntimeError):
        sqlstate = "57014"

    class StatementTimeoutCursor(FakeCursor):
        def execute(self, query: str, params: object = None) -> object:
            if query.startswith("SELECT COUNT"):
                raise StatementTimeoutError("postgresql://secret@host")
            return super().execute(query, params)

    class StatementTimeoutConnection(FakeConnection):
        def cursor(self) -> StatementTimeoutCursor:
            return StatementTimeoutCursor(self)

    connection = StatementTimeoutConnection()
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    with pytest.raises(module.PostgresOperationTimeoutError) as exc_info:
        repository.summary(POSTGRES_DATASET_ID)
    assert "secret" not in str(exc_info.value)


def test_post_write_verification_failure_rolls_back_before_state_write() -> None:
    connection = FakeConnection(summary_row=(99, _ts(1), _ts(2)))
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    with pytest.raises(PostgresGoldRepositoryError):
        repository.apply_delta(POSTGRES_DATASET_ID, GoldDeltaPlan((), (), (), (), ()), _state())
    queries = _execute_queries(connection)
    assert not any(
        'INSERT INTO "regime_loader_sync"."gold_sync_state"' in query for query in queries
    )
    assert ("rollback", None, None) in connection.events


def test_connection_failure_does_not_echo_password() -> None:
    def broken_factory(config: PostgresSyncConfig) -> FakeConnection:
        raise RuntimeError(f"cannot connect with {config.password}")

    repository = PostgresGoldSyncRepository(_config(), connection_factory=broken_factory)
    with pytest.raises(PostgresGoldRepositoryError) as exc_info:
        repository.preflight_schema()
    assert "repo-secret" not in str(exc_info.value)


def test_unsupported_dataset_and_missing_digest_fail_before_mutation() -> None:
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory())
    with pytest.raises(ValueError, match="unsupported"):
        repository.read_state("silver")
    row = _row(1, 1.0)
    with pytest.raises(ValueError, match="missing"):
        repository.apply_delta(
            POSTGRES_DATASET_ID,
            GoldDeltaPlan((row,), (), (), (), ()),
            _state(count=1, minimum=_ts(1), maximum=_ts(1)),
        )
