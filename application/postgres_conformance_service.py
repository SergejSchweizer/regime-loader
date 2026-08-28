"""Read-only, fail-closed verification of the PostgreSQL Gold serving replica."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

import polars as pl

from application.gold_catalog import GoldCatalogRecord
from application.postgres_conformance import (
    PostgresConformanceReport,
    PostgresDatabaseConformanceInspector,
)
from application.postgres_delta import source_rows_and_digests
from application.postgres_sync import (
    POSTGRES_DATASET_ID,
    GoldRowDigest,
    GoldSyncRepository,
    GoldSyncState,
    GoldSyncTransaction,
    GoldTargetSummary,
    select_current_sync_record,
)


class GoldCatalogReader(Protocol):
    def read(self) -> Sequence[GoldCatalogRecord]: ...


class GoldFrameSource(Protocol):
    def validate_bundle(self, record: GoldCatalogRecord) -> None: ...

    def sha256_path(self, relative_data_path: str) -> str: ...

    def read_path(self, relative_data_path: str) -> pl.DataFrame: ...


class GoldPostgresConformanceVerifier:
    """Independently verifies source, consumer, index, and checkpoint agreement."""

    def __init__(
        self,
        *,
        catalog: GoldCatalogReader,
        source: GoldFrameSource,
        repository: GoldSyncRepository,
        inspector: PostgresDatabaseConformanceInspector,
    ) -> None:
        self._catalog = catalog
        self._source = source
        self._repository = repository
        self._inspector = inspector

    def verify(self) -> PostgresConformanceReport:
        try:
            record = select_current_sync_record(self._catalog.read())
            if record.data_path is None:
                raise ValueError("current Gold record has no data path")
            self._source.validate_bundle(record)
            frame = self._source.read_path(record.data_path)
            _, source_digests = source_rows_and_digests(frame)
            source_sha256 = self._source.sha256_path(record.data_path)
            evidence = self._inspector.inspect()
            state, digest_index, consumer_digests, summary = self._repository.run_locked(
                self._read_target
            )
            self._assert_agreement(
                record,
                source_sha256,
                source_digests,
                state,
                digest_index,
                consumer_digests,
                summary,
            )
        except Exception:
            return PostgresConformanceReport("FAIL", ("verification_failed",), {})
        summaries = dict(
            sorted(
                {
                    "consumer_row_count": summary.row_count,
                    "digest_row_count": len(digest_index),
                    **evidence.as_summary(),
                }.items()
            )
        )
        return PostgresConformanceReport(
            "PASS",
            (
                "consumer",
                "digest_index",
                "roles",
                "schema",
                "session",
                "source",
                "state",
                "summary",
                "temporal",
            ),
            summaries,
        )

    @staticmethod
    def _read_target(
        transaction: GoldSyncTransaction,
    ) -> tuple[
        GoldSyncState | None,
        tuple[GoldRowDigest, ...],
        tuple[GoldRowDigest, ...],
        GoldTargetSummary,
    ]:
        return (
            transaction.read_state(POSTGRES_DATASET_ID),
            transaction.read_digests(POSTGRES_DATASET_ID),
            transaction.read_consumer_digests(POSTGRES_DATASET_ID),
            transaction.summary(POSTGRES_DATASET_ID),
        )

    @staticmethod
    def _assert_agreement(
        record: GoldCatalogRecord,
        source_sha256: str,
        source_digests: tuple[GoldRowDigest, ...],
        state: GoldSyncState | None,
        digest_index: tuple[GoldRowDigest, ...],
        consumer_digests: tuple[GoldRowDigest, ...],
        summary: GoldTargetSummary,
    ) -> None:
        source = {digest.timestamp_m1: digest.row_sha256 for digest in source_digests}
        if source != {digest.timestamp_m1: digest.row_sha256 for digest in digest_index}:
            raise ValueError("digest index differs from source")
        if source != {digest.timestamp_m1: digest.row_sha256 for digest in consumer_digests}:
            raise ValueError("consumer differs from source")
        expected_summary = GoldTargetSummary(
            record.row_count or 0, record.min_timestamp, record.max_timestamp
        )
        if summary != expected_summary or state is None:
            raise ValueError("consumer summary or state differs from source")
        if (
            state.source_build_id != record.build_id
            or state.data_sha256 != source_sha256
            or state.schema_version != record.schema_version
            or state.feature_version != record.feature_version
            or state.row_count != expected_summary.row_count
            or state.min_timestamp != expected_summary.min_timestamp
            or state.max_timestamp != expected_summary.max_timestamp
        ):
            raise ValueError("sync state differs from source")
