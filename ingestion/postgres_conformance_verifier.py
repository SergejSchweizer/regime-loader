"""Independent, read-mostly inspection of the PostgreSQL serving replica."""

from __future__ import annotations

from datetime import UTC, datetime

from application.postgres_conformance import PostgresDatabaseConformanceEvidence
from application.postgres_sync import POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA
from ingestion.postgres_gold_repository import (
    _SCHEMA_SPECIFICATION,
    POSTGRES_APPLICATION_NAME,
    ConnectionFactory,
    ConnectionPort,
    CursorPort,
    PostgresGoldRepositoryError,
    PostgresSyncConfig,
    _default_connection,
    _session_configuration,
)
from scripts.provision_postgres_role import POSTGRES_OWNER_ROLE, POSTGRES_ROLE

_TEMPORAL_PROBES = (
    datetime(2026, 3, 29, 0, 59, 59, 123456, tzinfo=UTC),
    datetime(2026, 10, 25, 1, 30, 0, 654321, tzinfo=UTC),
)
_OWNED_TABLES_SQL = """SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN (%s, %s) AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name"""
_OWNED_COLUMNS_SQL = """SELECT table_schema, table_name, ordinal_position, column_name,
    data_type, datetime_precision, character_maximum_length, is_nullable
FROM information_schema.columns
WHERE table_schema IN (%s, %s)
ORDER BY table_schema, table_name, ordinal_position"""
_OWNED_KEYS_SQL = """SELECT namespaces.nspname, classes.relname, constraints.contype,
    array_agg(attributes.attname::text ORDER BY keys.ordinality)
FROM pg_constraint AS constraints
JOIN pg_class AS classes ON classes.oid = constraints.conrelid
JOIN pg_namespace AS namespaces ON namespaces.oid = classes.relnamespace
JOIN unnest(constraints.conkey) WITH ORDINALITY AS keys(attnum, ordinality) ON TRUE
JOIN pg_attribute AS attributes
    ON attributes.attrelid = classes.oid AND attributes.attnum = keys.attnum
WHERE namespaces.nspname IN (%s, %s) AND constraints.contype IN ('p', 'u')
GROUP BY namespaces.nspname, classes.relname, constraints.contype
ORDER BY namespaces.nspname, classes.relname, constraints.contype"""
_ROLE_SQL = """SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
    rolreplication, rolbypassrls
FROM pg_roles
WHERE rolname IN (%s, %s)
ORDER BY rolname"""
_SCHEMA_OWNER_SQL = """SELECT namespaces.nspname, roles.rolname
FROM pg_namespace AS namespaces
JOIN pg_roles AS roles ON roles.oid = namespaces.nspowner
WHERE namespaces.nspname IN (%s, %s)
ORDER BY namespaces.nspname"""
_TABLE_OWNER_SQL = """SELECT namespaces.nspname, classes.relname, roles.rolname
FROM pg_class AS classes
JOIN pg_namespace AS namespaces ON namespaces.oid = classes.relnamespace
JOIN pg_roles AS roles ON roles.oid = classes.relowner
WHERE namespaces.nspname IN (%s, %s) AND classes.relkind = 'r'
ORDER BY namespaces.nspname, classes.relname"""


class PostgresLiveDatabaseConformanceInspector:
    """Inspect administered database facts without invoking the sync mutation path."""

    def __init__(
        self,
        config: PostgresSyncConfig,
        *,
        connection_factory: ConnectionFactory = _default_connection,
    ) -> None:
        self._config = config
        self._connection_factory = connection_factory

    def inspect(self) -> PostgresDatabaseConformanceEvidence:
        connection: ConnectionPort | None = None
        try:
            connection = self._connection_factory(self._config)
            cursor = connection.cursor()
            try:
                for statement in _session_configuration(self._config.timeout_policy):
                    cursor.execute(statement)
                cursor.execute("SET TRANSACTION READ ONLY")
                self._assert_schema(cursor)
                self._assert_roles(cursor)
                self._assert_session(cursor)
                self._assert_temporal_round_trips(cursor)
            finally:
                cursor.close()
            connection.rollback()
        except Exception:
            if connection is not None:
                connection.rollback()
            raise PostgresGoldRepositoryError(
                "PostgreSQL live conformance inspection failed"
            ) from None
        finally:
            if connection is not None:
                connection.close()
        return PostgresDatabaseConformanceEvidence(
            schema_table_count=len(_SCHEMA_SPECIFICATION),
            role_count=2,
            temporal_probe_count=len(_TEMPORAL_PROBES),
        )

    @staticmethod
    def _assert_schema(cursor: CursorPort) -> None:
        schemas = (POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA)
        cursor.execute(_OWNED_TABLES_SQL, schemas)
        expected_tables = tuple(sorted((item.schema, item.name) for item in _SCHEMA_SPECIFICATION))
        if tuple(cursor.fetchall()) != expected_tables:
            raise ValueError("owned tables differ")

        cursor.execute(_OWNED_COLUMNS_SQL, schemas)
        columns: dict[tuple[str, str], list[tuple[object, ...]]] = {}
        for row in cursor.fetchall():
            if len(row) != 8:
                raise ValueError("column query width differs")
            columns.setdefault((str(row[0]), str(row[1])), []).append(row)
        for table in _SCHEMA_SPECIFICATION:
            expected_columns = tuple(
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
                for ordinal, column in enumerate(table.columns, start=1)
            )
            if tuple(columns.get((table.schema, table.name), ())) != expected_columns:
                raise ValueError("owned columns differ")

        cursor.execute(_OWNED_KEYS_SQL, schemas)
        actual_keys: dict[tuple[str, str], tuple[str, ...]] = {}
        for row in cursor.fetchall():
            if len(row) != 4 or row[2] != "p" or not isinstance(row[3], (list, tuple)):
                raise ValueError("owned keys differ")
            identity = (str(row[0]), str(row[1]))
            if identity in actual_keys or not all(isinstance(column, str) for column in row[3]):
                raise ValueError("owned keys differ")
            actual_keys[identity] = tuple(row[3])
        expected_keys = {
            (table.schema, table.name): table.primary_key for table in _SCHEMA_SPECIFICATION
        }
        if actual_keys != expected_keys:
            raise ValueError("owned keys differ")

    @staticmethod
    def _assert_roles(cursor: CursorPort) -> None:
        schemas = (POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA)
        cursor.execute(_ROLE_SQL, (POSTGRES_OWNER_ROLE, POSTGRES_ROLE))
        if tuple(cursor.fetchall()) != (
            (POSTGRES_ROLE, True, False, False, False, False, False),
            (POSTGRES_OWNER_ROLE, False, False, False, False, False, False),
        ):
            raise ValueError("roles differ")

        expected_schemas = tuple((schema, POSTGRES_OWNER_ROLE) for schema in sorted(schemas))
        cursor.execute(_SCHEMA_OWNER_SQL, schemas)
        if tuple(cursor.fetchall()) != expected_schemas:
            raise ValueError("schema ownership differs")
        expected_tables = tuple(
            (table.schema, table.name, POSTGRES_OWNER_ROLE)
            for table in sorted(_SCHEMA_SPECIFICATION, key=lambda item: (item.schema, item.name))
        )
        cursor.execute(_TABLE_OWNER_SQL, schemas)
        if tuple(cursor.fetchall()) != expected_tables:
            raise ValueError("table ownership differs")

        for schema in schemas:
            cursor.execute(
                "SELECT has_schema_privilege(%s, %s, 'USAGE'), "
                "has_schema_privilege(%s, %s, 'CREATE')",
                (POSTGRES_ROLE, schema, POSTGRES_ROLE, schema),
            )
            if cursor.fetchone() != (True, False):
                raise ValueError("runtime schema grants differ")
        for table in _SCHEMA_SPECIFICATION:
            has_dml = table.name != "schema_migrations"
            expected = (True, has_dml, has_dml, has_dml)
            cursor.execute(
                "SELECT has_table_privilege(%s, %s, 'SELECT'), "
                "has_table_privilege(%s, %s, 'INSERT'), "
                "has_table_privilege(%s, %s, 'UPDATE'), "
                "has_table_privilege(%s, %s, 'DELETE')",
                (POSTGRES_ROLE, f"{table.schema}.{table.name}") * 4,
            )
            if cursor.fetchone() != expected:
                raise ValueError("runtime table grants differ")

    def _assert_session(self, cursor: CursorPort) -> None:
        cursor.execute("SHOW TIME ZONE")
        if cursor.fetchone() != ("UTC",):
            raise ValueError("session timezone differs")
        expected: dict[str, int | str] = {
            "application_name": POSTGRES_APPLICATION_NAME,
            "lock_timeout": self._config.timeout_policy.lock_timeout_ms,
            "statement_timeout": self._config.timeout_policy.statement_timeout_ms,
            "idle_in_transaction_session_timeout": (
                self._config.timeout_policy.idle_in_transaction_timeout_ms
            ),
        }
        for setting, expected_value in expected.items():
            cursor.execute(f"SHOW {setting}")
            actual = cursor.fetchone()
            if (
                actual is None
                or len(actual) != 1
                or not self._setting_matches(actual[0], expected_value)
            ):
                raise ValueError(f"session {setting} differs")

    @staticmethod
    def _setting_matches(actual: object, expected: int | str) -> bool:
        if actual == expected:
            return True
        if isinstance(actual, str) and isinstance(expected, int):
            return actual in {f"{expected}ms", f"{expected / 1_000:g}s"}
        return False

    @staticmethod
    def _assert_temporal_round_trips(cursor: CursorPort) -> None:
        for timestamp in _TEMPORAL_PROBES:
            cursor.execute("SELECT %s::TIMESTAMPTZ(6)", (timestamp,))
            if cursor.fetchone() != (timestamp,):
                raise ValueError("microsecond timestamp round trip differs")
