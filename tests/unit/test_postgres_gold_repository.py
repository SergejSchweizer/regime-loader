from __future__ import annotations

from datetime import UTC, datetime

import pytest

import ingestion.postgres_gold_repository as module
from application.gold_frame import GOLD_COLUMNS
from application.postgres_sync import (
    POSTGRES_DATASET_ID,
    GoldDeltaPlan,
    GoldRowDigest,
    GoldRowPayload,
    GoldSyncState,
    GoldTargetSummary,
)
from ingestion.postgres_gold_repository import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    PostgresGoldRepositoryError,
    PostgresGoldSyncRepository,
    PostgresSyncConfig,
)


def _ts(day: int) -> datetime:
    return datetime(2026, 8, day, tzinfo=UTC)


def _config(password: str = "repo-secret") -> PostgresSyncConfig:
    return PostgresSyncConfig(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, "quant_data", password)


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

    def execute(self, query: str, params: object = None) -> object:
        self.connection.events.append(("execute", query, params))
        if self.connection.fail_on and self.connection.fail_on in query:
            raise RuntimeError("database failure repo-secret")
        if query.startswith("SELECT dataset_id"):
            self.one = self.connection.state_row
        elif query.startswith("SELECT timestamp_m1, row_sha256"):
            self.many = list(self.connection.digest_rows)
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
        summary_row: tuple[object, ...] = (0, None, None),
        fail_on: str | None = None,
    ) -> None:
        self.state_row = state_row
        self.digest_rows = digest_rows
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


def test_schema_ddl_is_gold_only_timestamptz_and_idempotent() -> None:
    connection = FakeConnection()
    factory = Factory(connection)
    repository = PostgresGoldSyncRepository(_config(), connection_factory=factory)
    repository.ensure_schema()
    queries = _execute_queries(connection)
    assert queries[0] == "SET TIME ZONE 'UTC'"
    ddl = "\n".join(queries[1:])
    assert '"timestamp_m1" TIMESTAMPTZ(6) NOT NULL PRIMARY KEY' in ddl
    for column in GOLD_COLUMNS[1:]:
        assert f'"{column}" DOUBLE PRECISION NULL' in ddl
        assert f'ADD COLUMN IF NOT EXISTS "{column}" DOUBLE PRECISION NULL' in ddl
    assert '"regime_loader"."regime_features_daily"' in ddl
    assert '"regime_loader_sync"."gold_sync_state"' in ddl
    assert '"regime_loader_sync"."gold_row_hashes"' in ddl
    assert "TRUNCATE" not in ddl
    assert "DROP TABLE" not in ddl
    assert "CREATE SCHEMA" not in ddl
    assert ("commit", None, None) in connection.events


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


def test_summary_returns_count_and_utc_bounds() -> None:
    connection = FakeConnection(summary_row=(2, _ts(1), _ts(2)))
    repository = PostgresGoldSyncRepository(_config(), connection_factory=Factory(connection))
    assert repository.summary(POSTGRES_DATASET_ID) == GoldTargetSummary(2, _ts(1), _ts(2))


def test_apply_delta_is_locked_exact_and_state_is_last_before_commit() -> None:
    insert = _row(2, 2.0)
    update = _row(1, 10.0)
    delete = _ts(3)
    digests = (
        GoldRowDigest(update.timestamp_m1, "a" * 64),
        GoldRowDigest(insert.timestamp_m1, "b" * 64),
    )
    plan = GoldDeltaPlan((insert,), (update,), (delete,), (), digests)
    state = _state()
    connection = FakeConnection(summary_row=(2, _ts(1), _ts(2)))
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


def test_first_bootstrap_can_insert_complete_source_without_full_reload_sql() -> None:
    rows = (_row(1, 1.0), _row(2, 2.0))
    digests = (
        GoldRowDigest(_ts(1), "a" * 64),
        GoldRowDigest(_ts(2), "b" * 64),
    )
    plan = GoldDeltaPlan(rows, (), (), (), digests)
    connection = FakeConnection(summary_row=(2, _ts(1), _ts(2)))
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
        repository.ensure_schema()
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
