"""Application contracts for replicating canonical Gold into PostgreSQL."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, TypeVar

from application.gold_catalog import (
    STRICT_CURRENT,
    GoldCatalogRecord,
    GoldCompatibility,
)
from application.gold_frame import GOLD_COLUMNS, GOLD_FEATURE_VERSION, GOLD_SCHEMA_VERSION

POSTGRES_DATASET_ID = "regime_features_daily"
POSTGRES_CONSUMER_SCHEMA = "regime_loader"
POSTGRES_CONSUMER_TABLE = "regime_features_daily"
POSTGRES_SYNC_SCHEMA = "regime_loader_sync"
POSTGRES_SYNC_STATE_TABLE = "gold_sync_state"
POSTGRES_ROW_HASH_TABLE = "gold_row_hashes"
POSTGRES_TIMESTAMP_COLUMN = "timestamp_m1"
POSTGRES_TIMESTAMP_SQL_TYPE = "TIMESTAMPTZ(6)"
POSTGRES_SESSION_TIMEZONE = "UTC"

TransactionResult = TypeVar("TransactionResult")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("PostgreSQL Gold sync timestamps must be zero-offset UTC")
    return value


@dataclass(frozen=True, slots=True)
class GoldRowPayload:
    """One canonical Gold row independent of a concrete database client."""

    timestamp_m1: datetime
    values: tuple[float | None, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp_m1", _utc(self.timestamp_m1))
        if len(self.values) != len(GOLD_COLUMNS) - 1:
            raise ValueError("Gold row payload length does not match canonical feature columns")


@dataclass(frozen=True, slots=True)
class GoldRowDigest:
    timestamp_m1: datetime
    row_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "timestamp_m1", _utc(self.timestamp_m1))
        if len(self.row_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in self.row_sha256
        ):
            raise ValueError("row_sha256 must be 64 lowercase hexadecimal characters")


@dataclass(frozen=True, slots=True)
class GoldDeltaPlan:
    inserts: tuple[GoldRowPayload, ...]
    updates: tuple[GoldRowPayload, ...]
    deletes: tuple[datetime, ...]
    unchanged: tuple[datetime, ...]
    source_digests: tuple[GoldRowDigest, ...]

    def __post_init__(self) -> None:
        normalized_deletes = tuple(_utc(value) for value in self.deletes)
        normalized_unchanged = tuple(_utc(value) for value in self.unchanged)
        object.__setattr__(self, "deletes", normalized_deletes)
        object.__setattr__(self, "unchanged", normalized_unchanged)
        groups = (
            {row.timestamp_m1 for row in self.inserts},
            {row.timestamp_m1 for row in self.updates},
            set(normalized_deletes),
            set(normalized_unchanged),
        )
        total = sum(len(group) for group in groups)
        union = set().union(*groups)
        if len(union) != total:
            raise ValueError("Gold delta mutation/unchanged key sets must be disjoint")

    @property
    def inserted(self) -> int:
        return len(self.inserts)

    @property
    def updated(self) -> int:
        return len(self.updates)

    @property
    def deleted(self) -> int:
        return len(self.deletes)

    @property
    def unchanged_count(self) -> int:
        return len(self.unchanged)


@dataclass(frozen=True, slots=True)
class GoldSyncState:
    dataset_id: str
    source_build_id: str
    data_sha256: str
    schema_version: int
    feature_version: int
    row_count: int
    min_timestamp: datetime | None
    max_timestamp: datetime | None
    synced_at_utc: datetime

    def __post_init__(self) -> None:
        if self.dataset_id != POSTGRES_DATASET_ID:
            raise ValueError("unsupported PostgreSQL Gold dataset_id")
        if len(self.data_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in self.data_sha256
        ):
            raise ValueError("data_sha256 must be 64 lowercase hexadecimal characters")
        if self.row_count < 0:
            raise ValueError("row_count cannot be negative")
        object.__setattr__(self, "synced_at_utc", _utc(self.synced_at_utc))
        if self.min_timestamp is not None:
            object.__setattr__(self, "min_timestamp", _utc(self.min_timestamp))
        if self.max_timestamp is not None:
            object.__setattr__(self, "max_timestamp", _utc(self.max_timestamp))
        if self.row_count == 0 and (
            self.min_timestamp is not None or self.max_timestamp is not None
        ):
            raise ValueError("empty synchronized dataset cannot have timestamp bounds")
        if self.row_count > 0 and (self.min_timestamp is None or self.max_timestamp is None):
            raise ValueError("non-empty synchronized dataset requires timestamp bounds")
        if (
            self.min_timestamp is not None
            and self.max_timestamp is not None
            and self.max_timestamp < self.min_timestamp
        ):
            raise ValueError("max_timestamp cannot precede min_timestamp")


@dataclass(frozen=True, slots=True)
class GoldTargetSummary:
    row_count: int
    min_timestamp: datetime | None
    max_timestamp: datetime | None

    def __post_init__(self) -> None:
        if self.row_count < 0:
            raise ValueError("row_count cannot be negative")
        if self.min_timestamp is not None:
            object.__setattr__(self, "min_timestamp", _utc(self.min_timestamp))
        if self.max_timestamp is not None:
            object.__setattr__(self, "max_timestamp", _utc(self.max_timestamp))


@dataclass(frozen=True, slots=True)
class GoldSyncResult:
    dataset_id: str
    source_build_id: str
    inserted: int
    updated: int
    deleted: int
    unchanged: int

    def __post_init__(self) -> None:
        if self.dataset_id != POSTGRES_DATASET_ID:
            raise ValueError("unsupported PostgreSQL Gold dataset_id")
        if min(self.inserted, self.updated, self.deleted, self.unchanged) < 0:
            raise ValueError("Gold sync result counts cannot be negative")


class GoldSyncTransaction(Protocol):
    """Locked target-state operations performed within one database transaction."""

    def read_state(self, dataset_id: str) -> GoldSyncState | None: ...

    def read_digests(self, dataset_id: str) -> tuple[GoldRowDigest, ...]: ...

    def read_consumer_digests(self, dataset_id: str) -> tuple[GoldRowDigest, ...]: ...

    def apply_delta(
        self,
        dataset_id: str,
        plan: GoldDeltaPlan,
        state: GoldSyncState,
    ) -> None: ...

    def summary(self, dataset_id: str) -> GoldTargetSummary: ...


class GoldSyncRepository(Protocol):
    """Narrow serving-plane Repository port; concrete database client stays outside."""

    def preflight_schema(self) -> None:
        """Verify the administered schema is compatible before any row mutation."""

        ...

    def run_locked(
        self,
        operation: Callable[[GoldSyncTransaction], TransactionResult],
    ) -> TransactionResult: ...


class GoldSchemaMigrator(Protocol):
    """Admin-only port for applying PostgreSQL serving-schema migrations."""

    def migrate(self) -> None: ...


def require_sync_compatible(record: GoldCatalogRecord) -> None:
    if record.dataset_id != POSTGRES_DATASET_ID:
        raise LookupError("Gold current row belongs to unsupported PostgreSQL dataset")
    compatibility = GoldCompatibility(
        schema_version=GOLD_SCHEMA_VERSION,
        feature_version=GOLD_FEATURE_VERSION,
    )
    if not compatibility.matches(record):
        raise LookupError("Gold current row semantic versions are not PostgreSQL-sync compatible")


def select_current_sync_record(records: Sequence[GoldCatalogRecord]) -> GoldCatalogRecord:
    """Resolve exactly the catalog current compatible build; never inspect filesystem recency."""
    compatibility = GoldCompatibility(
        schema_version=GOLD_SCHEMA_VERSION,
        feature_version=GOLD_FEATURE_VERSION,
    )
    record = STRICT_CURRENT.resolve(records, compatibility)
    require_sync_compatible(record)
    return record
