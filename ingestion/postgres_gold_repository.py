"""Psycopg Adapter for the canonical Gold PostgreSQL serving-plane replica."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from typing import Protocol, TypeVar, cast

import psycopg

from application.gold_frame import GOLD_COLUMNS
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


class PostgresGoldRepositoryError(RuntimeError):
    """Sanitized repository error that deliberately carries no connection secret."""


class CursorPort(Protocol):
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


ConnectionFactory = Callable[[PostgresSyncConfig], ConnectionPort]


def _default_connection(config: PostgresSyncConfig) -> ConnectionPort:
    connection = psycopg.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        dbname=config.database,
        password=config.password,
        autocommit=False,
    )
    return cast(ConnectionPort, connection)


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


_CONSUMER = f"{_quote(POSTGRES_CONSUMER_SCHEMA)}.{_quote(POSTGRES_CONSUMER_TABLE)}"
_SYNC_STATE = f"{_quote(POSTGRES_SYNC_SCHEMA)}.{_quote(POSTGRES_SYNC_STATE_TABLE)}"
_ROW_HASHES = f"{_quote(POSTGRES_SYNC_SCHEMA)}.{_quote(POSTGRES_ROW_HASH_TABLE)}"
_FEATURE_COLUMNS = GOLD_COLUMNS[1:]

_CONSUMER_DDL = f"""CREATE TABLE IF NOT EXISTS {_CONSUMER} (
    {_quote("timestamp_m1")} TIMESTAMPTZ(6) NOT NULL PRIMARY KEY,
    {",\n    ".join(f"{_quote(column)} DOUBLE PRECISION NULL" for column in _FEATURE_COLUMNS)}
)"""
_CONSUMER_COLUMN_MIGRATIONS = tuple(
    f"ALTER TABLE {_CONSUMER} ADD COLUMN IF NOT EXISTS {_quote(column)} DOUBLE PRECISION NULL"
    for column in _FEATURE_COLUMNS
)

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
        for timestamp in plan.deletes:
            self._cursor.execute(_DELETE_ROW_SQL, (timestamp,))
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
        self._cursor.execute(_UPSERT_STATE_SQL, _state_params(state))


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
        try:
            connection = self._connection_factory(self._config)
            cursor = connection.cursor()
            try:
                cursor.execute(f"SET TIME ZONE '{POSTGRES_SESSION_TIMEZONE}'")
            finally:
                cursor.close()
            return connection
        except Exception:
            raise PostgresGoldRepositoryError(
                "PostgreSQL connection initialization failed"
            ) from None

    def ensure_schema(self) -> None:
        connection = self._open()
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(_CONSUMER_DDL)
                for statement in _CONSUMER_COLUMN_MIGRATIONS:
                    cursor.execute(statement)
                cursor.execute(_SYNC_STATE_DDL)
                cursor.execute(_ROW_HASH_DDL)
            finally:
                cursor.close()
            connection.commit()
        except Exception:
            connection.rollback()
            raise PostgresGoldRepositoryError(
                "PostgreSQL Gold schema initialization failed"
            ) from None
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
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL Gold sync-state read failed") from None
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
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL Gold digest read failed") from None
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
        except Exception:
            raise PostgresGoldRepositoryError("PostgreSQL Gold summary read failed") from None
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
                for row in plan.inserts:
                    cursor.execute(_INSERT_ROW_SQL, _row_insert_params(row))
                for row in plan.updates:
                    cursor.execute(_UPDATE_ROW_SQL, _row_update_params(row))
                for timestamp in plan.deletes:
                    cursor.execute(_DELETE_ROW_SQL, (timestamp,))

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
                cursor.execute(_UPSERT_STATE_SQL, _state_params(state))
            finally:
                cursor.close()
            connection.commit()
        except Exception:
            connection.rollback()
            raise PostgresGoldRepositoryError("PostgreSQL Gold delta transaction failed") from None
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
