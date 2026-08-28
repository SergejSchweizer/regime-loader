from __future__ import annotations

import pytest

from application.postgres_conformance import PostgresDatabaseConformanceEvidence
from application.postgres_sync import POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA
from ingestion.postgres_conformance_verifier import PostgresLiveDatabaseConformanceInspector
from ingestion.postgres_gold_repository import (
    _SCHEMA_SPECIFICATION,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_USER,
    PostgresGoldRepositoryError,
    PostgresSyncConfig,
)
from scripts.provision_postgres_role import POSTGRES_OWNER_ROLE, POSTGRES_ROLE


class FakeCursor:
    def __init__(self, *, timezone: str = "UTC") -> None:
        self.timezone = timezone
        self.queries: list[str] = []
        self._one: tuple[object, ...] | None = None
        self._many: list[tuple[object, ...]] = []

    @property
    def rowcount(self) -> int:
        return 1

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> FakeCursor:
        self.queries.append(query)
        if "FROM information_schema.tables" in query:
            self._many = sorted((table.schema, table.name) for table in _SCHEMA_SPECIFICATION)
        elif "FROM information_schema.columns" in query:
            self._many = [
                (
                    table.schema,
                    table.name,
                    ordinal,
                    column.name,
                    column.data_type,
                    column.precision if column.data_type == "timestamp with time zone" else None,
                    column.precision if column.data_type == "character" else None,
                    "YES" if column.nullable else "NO",
                )
                for table in _SCHEMA_SPECIFICATION
                for ordinal, column in enumerate(table.columns, start=1)
            ]
        elif "FROM pg_constraint" in query:
            self._many = [
                (table.schema, table.name, "p", table.primary_key)
                for table in sorted(
                    _SCHEMA_SPECIFICATION, key=lambda item: (item.schema, item.name)
                )
            ]
        elif "FROM pg_roles" in query:
            self._many = [
                (POSTGRES_ROLE, True, False, False, False, False, False),
                (POSTGRES_OWNER_ROLE, False, False, False, False, False, False),
            ]
        elif "FROM pg_namespace AS namespaces" in query and "classes" not in query:
            self._many = [
                (schema, POSTGRES_OWNER_ROLE)
                for schema in sorted((POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA))
            ]
        elif "FROM pg_class AS classes" in query:
            self._many = [
                (table.schema, table.name, POSTGRES_OWNER_ROLE)
                for table in sorted(
                    _SCHEMA_SPECIFICATION, key=lambda item: (item.schema, item.name)
                )
            ]
        elif "has_schema_privilege" in query:
            self._one = (True, False)
        elif "has_table_privilege" in query:
            assert params is not None
            self._one = (True, "schema_migrations" not in str(params[1]))
            self._one = (self._one[0], self._one[1], self._one[1], self._one[1])
        elif query == "SHOW TIME ZONE":
            self._one = (self.timezone,)
        elif query.startswith("SHOW "):
            settings: dict[str, object] = {
                "SHOW application_name": "regime-loader",
                "SHOW lock_timeout": "5s",
                "SHOW statement_timeout": "30s",
                "SHOW idle_in_transaction_session_timeout": "30s",
            }
            self._one = (settings[query],)
        elif query.startswith("SELECT %s::TIMESTAMPTZ(6)"):
            assert params is not None
            self._one = (params[0],)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        return self._one

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._many

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.rollbacks = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return self._cursor

    def commit(self) -> None:
        raise AssertionError("live conformance inspection must not commit")

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


def _config() -> PostgresSyncConfig:
    return PostgresSyncConfig(POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, "quant_data", "secret")


def test_live_inspector_independently_checks_schema_roles_session_and_temporal_probes() -> None:
    cursor = FakeCursor()
    connection = FakeConnection(cursor)
    inspector = PostgresLiveDatabaseConformanceInspector(
        _config(), connection_factory=lambda _: connection
    )

    assert inspector.inspect() == PostgresDatabaseConformanceEvidence(4, 2, 2)
    assert connection.rollbacks == 1
    assert connection.closed
    assert cursor.queries.count("SELECT %s::TIMESTAMPTZ(6)") == 2


def test_live_inspector_fails_closed_and_rolls_back_when_session_timezone_drifts() -> None:
    connection = FakeConnection(FakeCursor(timezone="Europe/Berlin"))
    inspector = PostgresLiveDatabaseConformanceInspector(
        _config(), connection_factory=lambda _: connection
    )

    with pytest.raises(PostgresGoldRepositoryError, match="live conformance inspection failed"):
        inspector.inspect()
    assert connection.rollbacks == 1
    assert connection.closed


def test_live_inspector_rejects_duplicate_primary_key_metadata() -> None:
    class DuplicateKeyCursor(FakeCursor):
        def execute(
            self, query: str, params: tuple[object, ...] | None = None
        ) -> DuplicateKeyCursor:
            result = super().execute(query, params)
            if "FROM pg_constraint" in query:
                self._many.append(self._many[0])
            return result

    inspector = PostgresLiveDatabaseConformanceInspector(
        _config(), connection_factory=lambda _: FakeConnection(DuplicateKeyCursor())
    )

    with pytest.raises(PostgresGoldRepositoryError, match="live conformance inspection failed"):
        inspector.inspect()
    assert not inspector._setting_matches("unexpected", 5_000)
