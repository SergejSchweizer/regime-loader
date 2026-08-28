"""Application service reconciling canonical Gold into the PostgreSQL serving replica."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import polars as pl

from application.gold_catalog import GoldCatalogRecord
from application.postgres_delta import plan_gold_delta
from application.postgres_sync import (
    POSTGRES_DATASET_ID,
    GoldDeltaPlan,
    GoldSyncRepository,
    GoldSyncResult,
    GoldSyncState,
    GoldSyncTransaction,
    select_current_sync_record,
)

Clock = Callable[[], datetime]


class GoldSyncSourceError(RuntimeError):
    """Current Gold cannot be read or does not match its authoritative catalog metadata."""


class GoldSyncCompatibilityError(RuntimeError):
    """Existing serving state is not semantically compatible with current Gold."""


class GoldSyncVerificationError(RuntimeError):
    """Serving-plane checkpoint/digest invariants are inconsistent."""


class GoldCatalogReader(Protocol):
    def read(self) -> Sequence[GoldCatalogRecord]: ...


class GoldFrameSource(Protocol):
    """Read/hash exactly the catalog-selected immutable Gold data path."""

    def validate_bundle(self, record: GoldCatalogRecord) -> None: ...

    def sha256_path(self, relative_data_path: str) -> str: ...

    def read_path(self, relative_data_path: str) -> pl.DataFrame: ...


@dataclass(frozen=True, slots=True)
class GoldPostgresDeltaSync:
    """Facade that performs one complete-state Gold reconciliation."""

    catalog: GoldCatalogReader
    source: GoldFrameSource
    repository: GoldSyncRepository
    clock: Clock

    def sync(self) -> GoldSyncResult:
        record = select_current_sync_record(self.catalog.read())
        self._validate_bundle(record)
        self.repository.ensure_schema()
        data_path = self._required_data_path(record)
        data_sha256 = self.source.sha256_path(data_path)
        self._validate_sha256(data_sha256)
        desired_state = self._desired_state(record, data_sha256)
        return self.repository.run_locked(
            lambda transaction: self._sync_locked(transaction, record, data_path, desired_state)
        )

    def _sync_locked(
        self,
        transaction: GoldSyncTransaction,
        record: GoldCatalogRecord,
        data_path: str,
        desired_state: GoldSyncState,
    ) -> GoldSyncResult:
        prior_state = transaction.read_state(POSTGRES_DATASET_ID)
        self._require_state_compatible(prior_state, desired_state)
        target_digests = transaction.read_digests(POSTGRES_DATASET_ID)
        if prior_state is None and target_digests:
            raise GoldSyncVerificationError(
                "PostgreSQL Gold digests exist without authoritative sync state"
            )
        if prior_state is not None and len(target_digests) != prior_state.row_count:
            raise GoldSyncVerificationError(
                "PostgreSQL Gold digest count does not match authoritative sync state"
            )

        if prior_state is not None and self._same_data(prior_state, desired_state):
            transaction.apply_delta(
                POSTGRES_DATASET_ID,
                GoldDeltaPlan((), (), (), (), ()),
                desired_state,
            )
            return GoldSyncResult(
                dataset_id=POSTGRES_DATASET_ID,
                source_build_id=record.build_id,
                inserted=0,
                updated=0,
                deleted=0,
                unchanged=desired_state.row_count,
            )

        frame = self.source.read_path(data_path)
        self._validate_frame_metadata(frame, record)
        plan = plan_gold_delta(frame, target_digests, prior_state)
        transaction.apply_delta(POSTGRES_DATASET_ID, plan, desired_state)
        return GoldSyncResult(
            dataset_id=POSTGRES_DATASET_ID,
            source_build_id=record.build_id,
            inserted=plan.inserted,
            updated=plan.updated,
            deleted=plan.deleted,
            unchanged=plan.unchanged_count,
        )

    def _validate_bundle(self, record: GoldCatalogRecord) -> None:
        try:
            self.source.validate_bundle(record)
        except (OSError, TypeError, ValueError) as exc:
            raise GoldSyncSourceError("current Gold bundle integrity verification failed") from exc

    def _desired_state(self, record: GoldCatalogRecord, data_sha256: str) -> GoldSyncState:
        row_count = record.row_count
        if row_count is None:
            raise GoldSyncSourceError("current complete Gold catalog row has no row_count")
        now = self.clock()
        if now.tzinfo is None:
            raise GoldSyncSourceError("Gold PostgreSQL sync clock must be timezone-aware")
        return GoldSyncState(
            dataset_id=POSTGRES_DATASET_ID,
            source_build_id=record.build_id,
            data_sha256=data_sha256,
            schema_version=record.schema_version,
            feature_version=record.feature_version,
            row_count=row_count,
            min_timestamp=record.min_timestamp,
            max_timestamp=record.max_timestamp,
            synced_at_utc=now.astimezone(UTC),
        )

    @staticmethod
    def _required_data_path(record: GoldCatalogRecord) -> str:
        if not record.data_path:
            raise GoldSyncSourceError("current complete Gold catalog row has no data path")
        return record.data_path

    @staticmethod
    def _validate_sha256(value: str) -> None:
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise GoldSyncSourceError("current Gold data SHA-256 is invalid")

    @staticmethod
    def _require_state_compatible(
        prior: GoldSyncState | None,
        desired: GoldSyncState,
    ) -> None:
        if prior is None:
            return
        if (
            prior.schema_version == 1
            and desired.schema_version == 2
            and prior.feature_version == desired.feature_version
        ):
            return
        if (
            prior.schema_version != desired.schema_version
            or prior.feature_version != desired.feature_version
        ):
            raise GoldSyncCompatibilityError(
                "PostgreSQL Gold semantic versions differ from current canonical Gold"
            )

    @staticmethod
    def _same_data(prior: GoldSyncState, desired: GoldSyncState) -> bool:
        return (
            prior.data_sha256 == desired.data_sha256
            and prior.schema_version == desired.schema_version
            and prior.feature_version == desired.feature_version
            and prior.row_count == desired.row_count
            and prior.min_timestamp == desired.min_timestamp
            and prior.max_timestamp == desired.max_timestamp
        )

    @staticmethod
    def _validate_frame_metadata(frame: pl.DataFrame, record: GoldCatalogRecord) -> None:
        row_count = record.row_count
        if row_count is None:
            raise GoldSyncSourceError("current complete Gold catalog row has no row_count")
        if frame.height != row_count:
            raise GoldSyncSourceError("current Gold row count differs from catalog metadata")
        timestamps = frame.get_column("timestamp_m1")
        minimum = timestamps.min()
        maximum = timestamps.max()
        if minimum is not None and not isinstance(minimum, datetime):
            raise GoldSyncSourceError("current Gold minimum timestamp is invalid")
        if maximum is not None and not isinstance(maximum, datetime):
            raise GoldSyncSourceError("current Gold maximum timestamp is invalid")
        if minimum != record.min_timestamp or maximum != record.max_timestamp:
            raise GoldSyncSourceError("current Gold timestamp bounds differ from catalog metadata")
