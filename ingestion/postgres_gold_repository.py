"""Psycopg Adapter for the canonical Gold PostgreSQL serving-plane replica."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from typing import NoReturn, Protocol, TypeVar, cast

import psycopg

from application.gold_frame import GOLD_COLUMNS
from application.postgres_delta import gold_row_sha256
from application.postgres_sync import (
    POSTGRES_CONSUMER_SCHEMA,
    POSTGRES_CONSUMER_TABLE,
    POSTGRES_DATASET_ID,
    POSTGRES_ROW_HASH_TABLE,
    POSTGRES_SESSION_TIMEZONE,
    POSTGRES_SYNC_SCHEMA,
    POSTGRES_SYNC_STATE_TABLE,
    GoldDeltaPlan,
    GoldRowDigest,
    GoldRowPayload,
    GoldSyncState,
    GoldSyncTransaction,
    GoldTargetSummary,
)

POSTGRES_HOST = "10.10.1.3"
POSTGRES_PORT = 54321
POSTGRES_USER = "regime-loader"
POSTGRES_ADVISORY_LOCK_NAMESPACE = "regime-loader:postgres-gold-sync:v1"

TransactionResult = TypeVar("TransactionResult")
POSTGRES_APPLICATION_NAME = "regime-loader"


@dataclass(frozen=True, slots=True)
class PostgresTimeoutPolicy:
    """Bounded waits for every PostgreSQL session opened by this adapter."""

    connect_timeout_seconds: int = 5
    lock_timeout_ms: int = 5_000
    statement_timeout_ms: int = 30_000
    idle_in_transaction_timeout_ms: int = 30_000
    application_name: str = POSTGRES_APPLICATION_NAME

    def __post_init__(self) -> None:
        for name, value in (
            ("connect_timeout_seconds", self.connect_timeout_seconds),
            ("lock_timeout_ms", self.lock_timeout_ms),
            ("statement_timeout_ms", self.statement_timeout_ms),
            ("idle_in_transaction_timeout_ms", self.idle_in_transaction_timeout_ms),
        ):
            if value <= 0:
                raise ValueError(f"PostgreSQL {name} must be positive")
        if self.application_name != POSTGRES_APPLICATION_NAME:
            raise ValueError(f"PostgreSQL application_name must be {POSTGRES_APPLICATION_NAME}")


class PostgresGoldRepositoryError(RuntimeError):
    """Sanitized repository error that deliberately carries no connection secret."""


class PostgresOperationTimeoutError(PostgresGoldRepositoryError):
    """A bounded PostgreSQL operation exceeded its configured wait."""


class PostgresLockContentionError(PostgresOperationTimeoutError):
    """The PostgreSQL advisory lock was unavailable before its configured deadline."""


class CursorPort(Protocol):
    @property
    def rowcount(self) -> int: ...

    def execute(self, query: str, params: Sequence[object] | None = None) -> object: ...

    def fetchone(self) -> tuple[object, ...] | None: ...

    def fetchall(self) -> list[tuple[object, ...]]: ...

    def close(self) -> None: ...


class ConnectionPort(Protocol):
    def cursor(self) -> CursorPort: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PostgresSyncConfig:
    host: str
    port: int
    user: str
    database: str
    password: str = field(repr=False)
    timeout_policy: PostgresTimeoutPolicy = field(default_factory=PostgresTimeoutPolicy)

    def __post_init__(self) -> None:
        if self.host != POSTGRES_HOST:
            raise ValueError(f"PostgreSQL sync host must be {POSTGRES_HOST}")
        if self.port != POSTGRES_PORT:
            raise ValueError(f"PostgreSQL sync port must be {POSTGRES_PORT}")
        if self.user != POSTGRES_USER:
            raise ValueError(f"PostgreSQL sync user must be {POSTGRES_USER}")
        if not self.database:
            raise ValueError("PostgreSQL sync database is required")
        if not self.password:
            raise ValueError("PostgreSQL sync password is required")

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> PostgresSyncConfig:
        try:
            port = int(values.get("PGPORT", ""))
        except ValueError as exc:
            raise ValueError("PGPORT must be an integer") from exc
        return cls(
            host=values.get("PGHOST", ""),
            port=port,
            user=values.get("PGUSER", ""),
            database=values.get("PGDATABASE", ""),
            password=values.get("PGPASSWORD", ""),
        )

    @classmethod
    def from_env(cls) -> PostgresSyncConfig:
        return cls.from_mapping(os.environ)


@dataclass(frozen=True, slots=True)
class PostgresAdminConfig:
    """Protected configuration for explicit schema migration only."""

    host: str
    port: int
    user: str
    database: str
    password: str = field(repr=False)
    timeout_policy: PostgresTimeoutPolicy = field(default_factory=PostgresTimeoutPolicy)

    def __post_init__(self) -> None:
        if self.host != POSTGRES_HOST:
            raise ValueError(f"PostgreSQL admin host must be {POSTGRES_HOST}")
        if self.port != POSTGRES_PORT:
            raise ValueError(f"PostgreSQL admin port must be {POSTGRES_PORT}")
        if not self.user:
            raise ValueError("PostgreSQL admin user is required")
        if self.user == POSTGRES_USER:
            raise ValueError("PostgreSQL admin user must differ from runtime user")
        if not self.database:
            raise ValueError("PostgreSQL admin database is required")
        if not self.password:
            raise ValueError("PostgreSQL admin password is required")

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> PostgresAdminConfig:
        try:
            port = int(values.get("MARKET_REGIME_POSTGRES_ADMIN_PORT", ""))
        except ValueError as exc:
            raise ValueError("MARKET_REGIME_POSTGRES_ADMIN_PORT must be an integer") from exc
        config = cls(
            host=values.get("MARKET_REGIME_POSTGRES_ADMIN_HOST", ""),
            port=port,
            user=values.get("MARKET_REGIME_POSTGRES_ADMIN_USER", ""),
            database=values.get("MARKET_REGIME_POSTGRES_ADMIN_DATABASE", ""),
            password=values.get("MARKET_REGIME_POSTGRES_ADMIN_PASSWORD", ""),
        )
        runtime_password = values.get("PGPASSWORD", "")
        if runtime_password and runtime_password == config.password:
            raise ValueError("PostgreSQL admin password must differ from runtime password")
        return config

    @classmethod
    def from_env(cls) -> PostgresAdminConfig:
        return cls.from_mapping(os.environ)


PostgresConnectionConfig = PostgresSyncConfig | PostgresAdminConfig
ConnectionFactory = Callable[[PostgresConnectionConfig], ConnectionPort]


def _default_connection(config: PostgresConnectionConfig) -> ConnectionPort:
    connection = psycopg.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        dbname=config.database,
        password=config.password,
        connect_timeout=config.timeout_policy.connect_timeout_seconds,
        application_name=config.timeout_policy.application_name,
        autocommit=False,
    )
    return cast(ConnectionPort, connection)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


_CONSUMER = f"{_quote(POSTGRES_CONSUMER_SCHEMA)}.{_quote(POSTGRES_CONSUMER_TABLE)}"
_SYNC_STATE = f"{_quote(POSTGRES_SYNC_SCHEMA)}.{_quote(POSTGRES_SYNC_STATE_TABLE)}"
_ROW_HASHES = f"{_quote(POSTGRES_SYNC_SCHEMA)}.{_quote(POSTGRES_ROW_HASH_TABLE)}"
_FEATURE_COLUMNS = GOLD_COLUMNS[1:]
_MIGRATION_LEDGER_TABLE = "schema_migrations"
_MIGRATION_LEDGER = f"{_quote(POSTGRES_SYNC_SCHEMA)}.{_quote(_MIGRATION_LEDGER_TABLE)}"
_POSTGRES_OWNER_ROLE = "regime-loader-owner"

_CONSUMER_DDL = f"""CREATE TABLE IF NOT EXISTS {_CONSUMER} (
    {_quote("timestamp_m1")} TIMESTAMPTZ(6) NOT NULL PRIMARY KEY,
    {",\n    ".join(f"{_quote(column)} DOUBLE PRECISION NULL" for column in _FEATURE_COLUMNS)}
)"""
_SYNC_STATE_DDL = f"""CREATE TABLE IF NOT EXISTS {_SYNC_STATE} (
    dataset_id TEXT PRIMARY KEY,
    source_build_id TEXT NOT NULL,
    data_sha256 CHAR(64) NOT NULL,
    schema_version INTEGER NOT NULL,
    feature_version INTEGER NOT NULL,
    row_count BIGINT NOT NULL,
    min_timestamp TIMESTAMPTZ(6) NULL,
    max_timestamp TIMESTAMPTZ(6) NULL,
    synced_at_utc TIMESTAMPTZ(6) NOT NULL
)"""

_ROW_HASH_DDL = f"""CREATE TABLE IF NOT EXISTS {_ROW_HASHES} (
    dataset_id TEXT NOT NULL,
    timestamp_m1 TIMESTAMPTZ(6) NOT NULL,
    row_sha256 CHAR(64) NOT NULL,
    PRIMARY KEY (dataset_id, timestamp_m1)
)"""

_MIGRATION_LEDGER_DDL = f"""CREATE TABLE IF NOT EXISTS {_MIGRATION_LEDGER} (
    version INTEGER PRIMARY KEY,
    applied_at_utc TIMESTAMPTZ(6) NOT NULL
)"""


@dataclass(frozen=True, slots=True)
class PostgresColumnSpecification:
    name: str
    data_type: str
    precision: int | None
    nullable: bool


@dataclass(frozen=True, slots=True)
class PostgresTableSpecification:
    schema: str
    name: str
    columns: tuple[PostgresColumnSpecification, ...]
    primary_key: tuple[str, ...]


_TIMESTAMPTZ6_NOT_NULL = PostgresColumnSpecification(
    "timestamp_m1", "timestamp with time zone", 6, False
)
_SCHEMA_SPECIFICATION = (
    PostgresTableSpecification(
        POSTGRES_CONSUMER_SCHEMA,
        POSTGRES_CONSUMER_TABLE,
        (_TIMESTAMPTZ6_NOT_NULL,)
        + tuple(
            PostgresColumnSpecification(column, "double precision", None, True)
            for column in _FEATURE_COLUMNS
        ),
        ("timestamp_m1",),
    ),
    PostgresTableSpecification(
        POSTGRES_SYNC_SCHEMA,
        POSTGRES_SYNC_STATE_TABLE,
        (
            PostgresColumnSpecification("dataset_id", "text", None, False),
            PostgresColumnSpecification("source_build_id", "text", None, False),
            PostgresColumnSpecification("data_sha256", "character", 64, False),
            PostgresColumnSpecification("schema_version", "integer", None, False),
            PostgresColumnSpecification("feature_version", "integer", None, False),
            PostgresColumnSpecification("row_count", "bigint", None, False),
            PostgresColumnSpecification("min_timestamp", "timestamp with time zone", 6, True),
            PostgresColumnSpecification("max_timestamp", "timestamp with time zone", 6, True),
            PostgresColumnSpecification("synced_at_utc", "timestamp with time zone", 6, False),
        ),
        ("dataset_id",),
    ),
    PostgresTableSpecification(
        POSTGRES_SYNC_SCHEMA,
        POSTGRES_ROW_HASH_TABLE,
        (
            PostgresColumnSpecification("dataset_id", "text", None, False),
            _TIMESTAMPTZ6_NOT_NULL,
            PostgresColumnSpecification("row_sha256", "character", 64, False),
        ),
        ("dataset_id", "timestamp_m1"),
    ),
    PostgresTableSpecification(
        POSTGRES_SYNC_SCHEMA,
        _MIGRATION_LEDGER_TABLE,
        (
            PostgresColumnSpecification("version", "integer", None, False),
            PostgresColumnSpecification("applied_at_utc", "timestamp with time zone", 6, False),
        ),
        ("version",),
    ),
)
_MIGRATIONS = ((_CONSUMER_DDL,), (_SYNC_STATE_DDL,), (_ROW_HASH_DDL,))
_OWNED_TABLES_SQL = """SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN (%s, %s) AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name"""
_OWNED_COLUMNS_SQL = """SELECT table_schema, table_name, ordinal_position, column_name,
    data_type, datetime_precision, character_maximum_length, is_nullable
FROM information_schema.columns
WHERE table_schema IN (%s, %s)
ORDER BY table_schema, table_name, ordinal_position"""
_OWNED_KEYS_SQL = """SELECT constraints.table_schema, constraints.table_name,
    constraints.constraint_type,
    array_agg(keys.column_name::text ORDER BY keys.ordinal_position)
FROM information_schema.table_constraints AS constraints
JOIN information_schema.key_column_usage AS keys
    ON constraints.constraint_catalog = keys.constraint_catalog
    AND constraints.constraint_schema = keys.constraint_schema
    AND constraints.constraint_name = keys.constraint_name
WHERE constraints.table_schema IN (%s, %s)
    AND constraints.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
GROUP BY constraints.table_schema, constraints.table_name, constraints.constraint_type
ORDER BY constraints.table_schema, constraints.table_name, constraints.constraint_type"""

_INSERT_ROW_SQL = (
    f"INSERT INTO {_CONSUMER} ({', '.join(_quote(column) for column in GOLD_COLUMNS)}) "
    f"VALUES ({', '.join('%s' for _ in GOLD_COLUMNS)})"
)
_UPDATE_ROW_SQL = (
    f"UPDATE {_CONSUMER} SET "
    + ", ".join(f"{_quote(column)} = %s" for column in _FEATURE_COLUMNS)
    + f" WHERE {_quote('timestamp_m1')} = %s"
)
_DELETE_ROW_SQL = f"DELETE FROM {_CONSUMER} WHERE {_quote('timestamp_m1')} = %s"
_UPSERT_DIGEST_SQL = f"""INSERT INTO {_ROW_HASHES} (dataset_id, timestamp_m1, row_sha256)
VALUES (%s, %s, %s)
ON CONFLICT (dataset_id, timestamp_m1)
DO UPDATE SET row_sha256 = EXCLUDED.row_sha256"""
_DELETE_DIGEST_SQL = f"DELETE FROM {_ROW_HASHES} WHERE dataset_id = %s AND timestamp_m1 = %s"
_CONSUMER_ROWS_SQL = (
    f"SELECT {', '.join(_quote(column) for column in GOLD_COLUMNS)} "
    f"FROM {_CONSUMER} ORDER BY {_quote('timestamp_m1')}"
)
_TARGET_SUMMARY_SQL = (
    f"SELECT COUNT(*), MIN({_quote('timestamp_m1')}), MAX({_quote('timestamp_m1')}) "
    f"FROM {_CONSUMER}"
)
_UPSERT_STATE_SQL = f"""INSERT INTO {_SYNC_STATE} (
    dataset_id, source_build_id, data_sha256, schema_version, feature_version,
    row_count, min_timestamp, max_timestamp, synced_at_utc
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (dataset_id) DO UPDATE SET
    source_build_id = EXCLUDED.source_build_id,
    data_sha256 = EXCLUDED.data_sha256,
    schema_version = EXCLUDED.schema_version,
    feature_version = EXCLUDED.feature_version,
    row_count = EXCLUDED.row_count,
    min_timestamp = EXCLUDED.min_timestamp,
    max_timestamp = EXCLUDED.max_timestamp,
    synced_at_utc = EXCLUDED.synced_at_utc"""


def _session_configuration(policy: PostgresTimeoutPolicy) -> tuple[str, ...]:
    return (
        f"SET application_name = '{policy.application_name}'",
        f"SET TIME ZONE '{POSTGRES_SESSION_TIMEZONE}'",
        f"SET lock_timeout = '{policy.lock_timeout_ms}ms'",
        f"SET statement_timeout = '{policy.statement_timeout_ms}ms'",
        f"SET idle_in_transaction_session_timeout = '{policy.idle_in_transaction_timeout_ms}ms'",
    )


def _raise_sanitized_error(operation: str, error: Exception) -> NoReturn:
    sqlstate = getattr(error, "sqlstate", None)
    if sqlstate == "55P03":
        raise PostgresLockContentionError("PostgreSQL advisory lock wait timed out") from None
    if sqlstate == "57014":
        raise PostgresOperationTimeoutError(f"PostgreSQL {operation} timed out") from None
    raise PostgresGoldRepositoryError(f"PostgreSQL {operation} failed") from None


def _as_text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"PostgreSQL {name} must be text")
    return value


def _as_int(value: object, name: str) -> int:
    if not isinstance(value, int):
        raise TypeError(f"PostgreSQL {name} must be integer")
    return value


def _as_datetime(value: object | None, name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"PostgreSQL {name} must be datetime")
    return value


def _state_from_row(row: tuple[object, ...]) -> GoldSyncState:
    if len(row) != 9:
        raise ValueError("PostgreSQL Gold sync state row has unexpected width")
    return GoldSyncState(
        dataset_id=_as_text(row[0], "dataset_id"),
        source_build_id=_as_text(row[1], "source_build_id"),
        data_sha256=_as_text(row[2], "data_sha256"),
        schema_version=_as_int(row[3], "schema_version"),
        feature_version=_as_int(row[4], "feature_version"),
        row_count=_as_int(row[5], "row_count"),
        min_timestamp=_as_datetime(row[6], "min_timestamp"),
        max_timestamp=_as_datetime(row[7], "max_timestamp"),
        synced_at_utc=cast(datetime, _as_datetime(row[8], "synced_at_utc")),
    )


def _summary_from_row(row: tuple[object, ...]) -> GoldTargetSummary:
    if len(row) != 3:
        raise ValueError("PostgreSQL Gold target summary row has unexpected width")
    return GoldTargetSummary(
        row_count=_as_int(row[0], "row_count"),
        min_timestamp=_as_datetime(row[1], "min_timestamp"),
        max_timestamp=_as_datetime(row[2], "max_timestamp"),
    )


def _payload_from_row(row: tuple[object, ...]) -> GoldRowPayload:
    if len(row) != len(GOLD_COLUMNS):
        raise ValueError("PostgreSQL Gold consumer row has unexpected width")
    timestamp = _as_datetime(row[0], "timestamp_m1")
    if timestamp is None:
        raise ValueError("PostgreSQL Gold consumer timestamp cannot be null")
    values: list[float | None] = []
    for value in row[1:]:
        if value is not None and not isinstance(value, float):
            raise TypeError("PostgreSQL Gold consumer feature must be float or null")
        values.append(value)
    return GoldRowPayload(timestamp, tuple(values))


def _digest_map(digests: tuple[GoldRowDigest, ...]) -> dict[datetime, str]:
    return {digest.timestamp_m1: digest.row_sha256 for digest in digests}


def _state_params(state: GoldSyncState) -> tuple[object, ...]:
    return (
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


def _row_insert_params(row: GoldRowPayload) -> tuple[object, ...]:
    return (row.timestamp_m1, *row.values)


def _row_update_params(row: GoldRowPayload) -> tuple[object, ...]:
    return (*row.values, row.timestamp_m1)


def _advisory_lock_key(dataset_id: str) -> int:
    identity = f"{POSTGRES_ADVISORY_LOCK_NAMESPACE}:{dataset_id}".encode("ascii")
    return int.from_bytes(sha256(identity).digest()[:8], byteorder="big", signed=True)


class _PostgresGoldSyncTransaction:
    def __init__(self, cursor: CursorPort) -> None:
        self._cursor = cursor

    def read_state(self, dataset_id: str) -> GoldSyncState | None:
        PostgresGoldSyncRepository._require_dataset(dataset_id)
        self._cursor.execute(
            f"SELECT dataset_id, source_build_id, data_sha256, schema_version, "
            f"feature_version, row_count, min_timestamp, max_timestamp, synced_at_utc "
            f"FROM {_SYNC_STATE} WHERE dataset_id = %s",
            (dataset_id,),
        )
        row = self._cursor.fetchone()
        return None if row is None else _state_from_row(row)

    def read_digests(self, dataset_id: str) -> tuple[GoldRowDigest, ...]:
        PostgresGoldSyncRepository._require_dataset(dataset_id)
        self._cursor.execute(
            f"SELECT timestamp_m1, row_sha256 FROM {_ROW_HASHES} "
            "WHERE dataset_id = %s ORDER BY timestamp_m1",
            (dataset_id,),
        )
        result: list[GoldRowDigest] = []
        for row in self._cursor.fetchall():
            if len(row) != 2:
                raise ValueError("PostgreSQL Gold digest row has unexpected width")
            timestamp = _as_datetime(row[0], "timestamp_m1")
            if timestamp is None:
                raise ValueError("PostgreSQL Gold digest timestamp cannot be null")
            result.append(GoldRowDigest(timestamp, _as_text(row[1], "row_sha256")))
        return tuple(result)

    def read_consumer_digests(self, dataset_id: str) -> tuple[GoldRowDigest, ...]:
        PostgresGoldSyncRepository._require_dataset(dataset_id)
        self._cursor.execute(_CONSUMER_ROWS_SQL)
        return tuple(
            GoldRowDigest(payload.timestamp_m1, gold_row_sha256(payload))
            for payload in (_payload_from_row(row) for row in self._cursor.fetchall())
        )

    def summary(self, dataset_id: str) -> GoldTargetSummary:
        PostgresGoldSyncRepository._require_dataset(dataset_id)
        self._cursor.execute(_TARGET_SUMMARY_SQL)
        row = self._cursor.fetchone()
        if row is None:
            raise ValueError("PostgreSQL Gold summary query returned no row")
        return _summary_from_row(row)

    def apply_delta(
        self,
        dataset_id: str,
        plan: GoldDeltaPlan,
        state: GoldSyncState,
    ) -> None:
        PostgresGoldSyncRepository._require_dataset(dataset_id)
        if state.dataset_id != dataset_id:
            raise ValueError("Gold sync state dataset does not match requested dataset")
        digests = PostgresGoldSyncRepository._source_digest_map(plan.source_digests)
        for row in (*plan.inserts, *plan.updates):
            if row.timestamp_m1 not in digests:
                raise ValueError("Gold delta mutation is missing its source row digest")
        for row in plan.inserts:
            self._cursor.execute(_INSERT_ROW_SQL, _row_insert_params(row))
        for row in plan.updates:
            self._cursor.execute(_UPDATE_ROW_SQL, _row_update_params(row))
            self._require_exactly_one_row("update")
        for timestamp in plan.deletes:
            self._cursor.execute(_DELETE_ROW_SQL, (timestamp,))
            self._require_exactly_one_row("delete")
        for row in (*plan.inserts, *plan.updates):
            self._cursor.execute(
                _UPSERT_DIGEST_SQL,
                (dataset_id, row.timestamp_m1, digests[row.timestamp_m1]),
            )
        for timestamp in plan.deletes:
            self._cursor.execute(_DELETE_DIGEST_SQL, (dataset_id, timestamp))
        expected = GoldTargetSummary(state.row_count, state.min_timestamp, state.max_timestamp)
        if self.summary(dataset_id) != expected:
            raise ValueError("PostgreSQL Gold post-write summary does not match source")
        if _digest_map(self.read_digests(dataset_id)) != _digest_map(plan.source_digests):
            raise ValueError("PostgreSQL Gold post-write digest index does not match source")
        if _digest_map(self.read_consumer_digests(dataset_id)) != _digest_map(plan.source_digests):
            raise ValueError("PostgreSQL Gold post-write consumer rows do not match source")
        self._cursor.execute(_UPSERT_STATE_SQL, _state_params(state))

    def _require_exactly_one_row(self, operation: str) -> None:
        if self._cursor.rowcount != 1:
            raise ValueError(f"PostgreSQL Gold {operation} affected an unexpected row count")


class PostgresGoldSyncRepository:
    """Transactional Repository Adapter for the rebuildable Gold serving replica."""

    def __init__(
        self,
        config: PostgresSyncConfig,
        *,
        connection_factory: ConnectionFactory = _default_connection,
    ) -> None:
        self._config = config
        self._connection_factory = connection_factory

    def _open(self) -> ConnectionPort:
        connection: ConnectionPort | None = None
        try:
            connection = self._connection_factory(self._config)
            cursor = connection.cursor()
            try:
                for statement in _session_configuration(self._config.timeout_policy):
                    cursor.execute(statement)
            finally:
                cursor.close()
            return connection
        except Exception as exc:
            if connection is not None:
                connection.close()
            _raise_sanitized_error("connection initialization", exc)

    def preflight_schema(self) -> None:
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute("SET TRANSACTION READ ONLY")
                self._assert_schema_contract(cursor)
            finally:
                cursor.close()
            connection.rollback()
        except PostgresGoldRepositoryError:
            connection.rollback()
            raise
        except Exception as exc:
            connection.rollback()
            _raise_sanitized_error("Gold schema preflight", exc)
        finally:
            connection.close()

    def run_locked(
        self,
        operation: Callable[[GoldSyncTransaction], TransactionResult],
    ) -> TransactionResult:
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s)",
                    (_advisory_lock_key(POSTGRES_DATASET_ID),),
                )
                result = operation(_PostgresGoldSyncTransaction(cursor))
            finally:
                cursor.close()
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise PostgresGoldRepositoryError("PostgreSQL Gold locked transaction failed") from None
        finally:
            connection.close()

    def read_state(self, dataset_id: str) -> GoldSyncState | None:
        self._require_dataset(dataset_id)
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT dataset_id, source_build_id, data_sha256, schema_version, "
                    f"feature_version, row_count, min_timestamp, max_timestamp, synced_at_utc "
                    f"FROM {_SYNC_STATE} WHERE dataset_id = %s",
                    (dataset_id,),
                )
                row = cursor.fetchone()
            finally:
                cursor.close()
            return None if row is None else _state_from_row(row)
        except PostgresGoldRepositoryError:
            raise
        except Exception as exc:
            _raise_sanitized_error("Gold sync-state read", exc)
        finally:
            connection.close()

    def read_digests(self, dataset_id: str) -> tuple[GoldRowDigest, ...]:
        self._require_dataset(dataset_id)
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    f"SELECT timestamp_m1, row_sha256 FROM {_ROW_HASHES} "
                    "WHERE dataset_id = %s ORDER BY timestamp_m1",
                    (dataset_id,),
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()
            result: list[GoldRowDigest] = []
            for row in rows:
                if len(row) != 2:
                    raise ValueError("PostgreSQL Gold digest row has unexpected width")
                timestamp = _as_datetime(row[0], "timestamp_m1")
                if timestamp is None:
                    raise ValueError("PostgreSQL Gold digest timestamp cannot be null")
                result.append(GoldRowDigest(timestamp, _as_text(row[1], "row_sha256")))
            return tuple(result)
        except PostgresGoldRepositoryError:
            raise
        except Exception as exc:
            _raise_sanitized_error("Gold digest read", exc)
        finally:
            connection.close()

    def read_consumer_digests(self, dataset_id: str) -> tuple[GoldRowDigest, ...]:
        self._require_dataset(dataset_id)
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                return _PostgresGoldSyncTransaction(cursor).read_consumer_digests(dataset_id)
            finally:
                cursor.close()
        except PostgresGoldRepositoryError:
            raise
        except Exception as exc:
            _raise_sanitized_error("Gold consumer read", exc)
        finally:
            connection.close()

    def summary(self, dataset_id: str) -> GoldTargetSummary:
        self._require_dataset(dataset_id)
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_TARGET_SUMMARY_SQL)
                row = cursor.fetchone()
            finally:
                cursor.close()
            if row is None:
                raise ValueError("PostgreSQL Gold summary query returned no row")
            return _summary_from_row(row)
        except PostgresGoldRepositoryError:
            raise
        except Exception as exc:
            _raise_sanitized_error("Gold summary read", exc)
        finally:
            connection.close()

    def apply_delta(
        self,
        dataset_id: str,
        plan: GoldDeltaPlan,
        state: GoldSyncState,
    ) -> None:
        self._require_dataset(dataset_id)
        if state.dataset_id != dataset_id:
            raise ValueError("Gold sync state dataset does not match requested dataset")
        digest_by_timestamp = self._source_digest_map(plan.source_digests)
        for row in (*plan.inserts, *plan.updates):
            if row.timestamp_m1 not in digest_by_timestamp:
                raise ValueError("Gold delta mutation is missing its source row digest")

        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (_advisory_lock_key(dataset_id),)
                )
                self._assert_schema_contract(cursor)
                for row in plan.inserts:
                    cursor.execute(_INSERT_ROW_SQL, _row_insert_params(row))
                for row in plan.updates:
                    cursor.execute(_UPDATE_ROW_SQL, _row_update_params(row))
                    if cursor.rowcount != 1:
                        raise ValueError("PostgreSQL Gold update affected an unexpected row count")
                for timestamp in plan.deletes:
                    cursor.execute(_DELETE_ROW_SQL, (timestamp,))
                    if cursor.rowcount != 1:
                        raise ValueError("PostgreSQL Gold delete affected an unexpected row count")

                for row in (*plan.inserts, *plan.updates):
                    cursor.execute(
                        _UPSERT_DIGEST_SQL,
                        (dataset_id, row.timestamp_m1, digest_by_timestamp[row.timestamp_m1]),
                    )
                for timestamp in plan.deletes:
                    cursor.execute(_DELETE_DIGEST_SQL, (dataset_id, timestamp))

                cursor.execute(_TARGET_SUMMARY_SQL)
                summary_row = cursor.fetchone()
                if summary_row is None:
                    raise ValueError("PostgreSQL Gold verification query returned no row")
                actual = _summary_from_row(summary_row)
                expected = GoldTargetSummary(
                    state.row_count,
                    state.min_timestamp,
                    state.max_timestamp,
                )
                if actual != expected:
                    raise ValueError("PostgreSQL Gold post-write summary does not match source")
                if _digest_map(
                    _PostgresGoldSyncTransaction(cursor).read_digests(dataset_id)
                ) != _digest_map(plan.source_digests):
                    raise ValueError(
                        "PostgreSQL Gold post-write digest index does not match source"
                    )
                if _digest_map(
                    _PostgresGoldSyncTransaction(cursor).read_consumer_digests(dataset_id)
                ) != _digest_map(plan.source_digests):
                    raise ValueError("PostgreSQL Gold post-write consumer rows do not match source")
                cursor.execute(_UPSERT_STATE_SQL, _state_params(state))
            finally:
                cursor.close()
            connection.commit()
        except PostgresGoldRepositoryError:
            connection.rollback()
            raise
        except Exception as exc:
            connection.rollback()
            _raise_sanitized_error("Gold delta transaction", exc)
        finally:
            connection.close()

    @staticmethod
    def _require_dataset(dataset_id: str) -> None:
        if dataset_id != POSTGRES_DATASET_ID:
            raise ValueError("unsupported PostgreSQL Gold dataset_id")

    @staticmethod
    def _source_digest_map(digests: tuple[GoldRowDigest, ...]) -> dict[datetime, str]:
        result: dict[datetime, str] = {}
        for digest in digests:
            if digest.timestamp_m1 in result:
                raise ValueError("duplicate source Gold digest timestamp")
            result[digest.timestamp_m1] = digest.row_sha256
        return result

    @staticmethod
    def _assert_schema_contract(cursor: CursorPort) -> None:
        schemas = (POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA)
        cursor.execute(_OWNED_TABLES_SQL, schemas)
        actual_tables = tuple(
            (_as_text(row[0], "table schema"), _as_text(row[1], "table name"))
            for row in cursor.fetchall()
        )
        expected_tables = tuple(sorted((spec.schema, spec.name) for spec in _SCHEMA_SPECIFICATION))
        if actual_tables != expected_tables:
            raise ValueError("PostgreSQL owned table contract does not match specification")

        cursor.execute(_OWNED_COLUMNS_SQL, schemas)
        actual_columns: dict[tuple[str, str], list[PostgresColumnSpecification]] = {}
        for row in cursor.fetchall():
            if len(row) != 8:
                raise ValueError("PostgreSQL column contract query returned unexpected width")
            schema = _as_text(row[0], "column schema")
            table = _as_text(row[1], "column table")
            _as_int(row[2], "column ordinal position")
            precision = row[5] if row[5] is not None else row[6]
            if precision is not None:
                precision = _as_int(precision, "column precision")
            actual_columns.setdefault((schema, table), []).append(
                PostgresColumnSpecification(
                    _as_text(row[3], "column name"),
                    _as_text(row[4], "column type"),
                    precision,
                    _as_text(row[7], "column nullability") == "YES",
                )
            )
        for specification in _SCHEMA_SPECIFICATION:
            if (
                tuple(actual_columns.get((specification.schema, specification.name), ()))
                != specification.columns
            ):
                raise ValueError("PostgreSQL column contract does not match specification")

        cursor.execute(_OWNED_KEYS_SQL, schemas)
        actual_keys: dict[tuple[str, str], tuple[str, ...]] = {}
        for row in cursor.fetchall():
            if len(row) != 4:
                raise ValueError("PostgreSQL key contract query returned unexpected width")
            if _as_text(row[2], "key type") != "PRIMARY KEY":
                raise ValueError("PostgreSQL schema contains an unexpected unique key")
            columns = row[3]
            if not isinstance(columns, (list, tuple)) or not all(
                isinstance(column, str) for column in columns
            ):
                raise TypeError("PostgreSQL key columns must be text")
            key = (_as_text(row[0], "key schema"), _as_text(row[1], "key table"))
            if key in actual_keys:
                raise ValueError("PostgreSQL schema contains duplicate key constraints")
            actual_keys[key] = tuple(columns)
        expected_keys = {
            (specification.schema, specification.name): specification.primary_key
            for specification in _SCHEMA_SPECIFICATION
        }
        if actual_keys != expected_keys:
            raise ValueError("PostgreSQL key contract does not match specification")


class PostgresGoldSchemaMigrator:
    """Admin-only adapter for idempotent serving-schema migrations."""

    def __init__(
        self,
        config: PostgresAdminConfig,
        *,
        connection_factory: ConnectionFactory = _default_connection,
    ) -> None:
        self._config = config
        self._connection_factory = connection_factory

    def _open(self) -> ConnectionPort:
        connection: ConnectionPort | None = None
        try:
            connection = self._connection_factory(self._config)
            cursor = connection.cursor()
            try:
                for statement in _session_configuration(self._config.timeout_policy):
                    cursor.execute(statement)
            finally:
                cursor.close()
            return connection
        except Exception as exc:
            if connection is not None:
                connection.close()
            _raise_sanitized_error("admin connection initialization", exc)

    def migrate(self) -> None:
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_MIGRATION_LEDGER_DDL)
                cursor.execute(f"SELECT version FROM {_MIGRATION_LEDGER} ORDER BY version")
                applied_versions = tuple(
                    _as_int(row[0], "migration version") for row in cursor.fetchall()
                )
                expected_versions = tuple(range(1, len(_MIGRATIONS) + 1))
                if any(version not in expected_versions for version in applied_versions):
                    raise ValueError(
                        "PostgreSQL schema migration ledger contains an unknown version"
                    )
                for version, statements in enumerate(_MIGRATIONS, start=1):
                    if version in applied_versions:
                        continue
                    for statement in statements:
                        cursor.execute(statement)
                    cursor.execute(
                        f"INSERT INTO {_MIGRATION_LEDGER} (version, applied_at_utc) "
                        "VALUES (%s, CURRENT_TIMESTAMP)",
                        (version,),
                    )
                PostgresGoldSyncRepository._assert_schema_contract(cursor)
            finally:
                cursor.close()
            connection.commit()
        except PostgresGoldRepositoryError:
            connection.rollback()
            raise
        except Exception as exc:
            connection.rollback()
            _raise_sanitized_error("Gold schema migration", exc)
        finally:
            connection.close()


class PostgresGoldSchemaReconstructor:
    """Admin-only recreation of precisely the serving schemas before canonical migration."""

    def __init__(
        self,
        config: PostgresAdminConfig,
        *,
        connection_factory: ConnectionFactory = _default_connection,
    ) -> None:
        self._config = config
        self._connection_factory = connection_factory

    def recreate(self) -> None:
        connection: ConnectionPort | None = None
        try:
            connection = self._connection_factory(self._config)
            cursor = connection.cursor()
            try:
                for statement in _session_configuration(self._config.timeout_policy):
                    cursor.execute(statement)
                cursor.execute(f"DROP SCHEMA IF EXISTS {_quote(POSTGRES_CONSUMER_SCHEMA)} CASCADE")
                cursor.execute(f"DROP SCHEMA IF EXISTS {_quote(POSTGRES_SYNC_SCHEMA)} CASCADE")
                for schema in (POSTGRES_CONSUMER_SCHEMA, POSTGRES_SYNC_SCHEMA):
                    cursor.execute(
                        f"CREATE SCHEMA {_quote(schema)} "
                        f"AUTHORIZATION {_quote(_POSTGRES_OWNER_ROLE)}"
                    )
                    cursor.execute(f"REVOKE ALL ON SCHEMA {_quote(schema)} FROM PUBLIC")
                    cursor.execute(
                        f"REVOKE CREATE ON SCHEMA {_quote(schema)} FROM {_quote(POSTGRES_USER)}"
                    )
                    cursor.execute(
                        f"GRANT USAGE ON SCHEMA {_quote(schema)} TO {_quote(POSTGRES_USER)}"
                    )
                cursor.execute(f"SET ROLE {_quote(_POSTGRES_OWNER_ROLE)}")
                cursor.execute(_MIGRATION_LEDGER_DDL)
                for version, statements in enumerate(_MIGRATIONS, start=1):
                    for statement in statements:
                        cursor.execute(statement)
                    cursor.execute(
                        f"INSERT INTO {_MIGRATION_LEDGER} (version, applied_at_utc) "
                        "VALUES (%s, CURRENT_TIMESTAMP)",
                        (version,),
                    )
                cursor.execute("RESET ROLE")
                for schema, table in (
                    (POSTGRES_CONSUMER_SCHEMA, POSTGRES_CONSUMER_TABLE),
                    (POSTGRES_SYNC_SCHEMA, POSTGRES_SYNC_STATE_TABLE),
                    (POSTGRES_SYNC_SCHEMA, POSTGRES_ROW_HASH_TABLE),
                ):
                    cursor.execute(
                        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "
                        f"{_quote(schema)}.{_quote(table)} TO {_quote(POSTGRES_USER)}"
                    )
                cursor.execute(
                    f"GRANT SELECT ON TABLE {_MIGRATION_LEDGER} TO {_quote(POSTGRES_USER)}"
                )
                PostgresGoldSyncRepository._assert_schema_contract(cursor)
            finally:
                cursor.close()
            connection.commit()
        except Exception as exc:
            if connection is not None:
                connection.rollback()
            _raise_sanitized_error("Gold schema reconstruction", exc)
        finally:
            if connection is not None:
                connection.close()
