from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import polars as pl
import pytest

from application.gold_catalog import GoldBuildStatus, GoldCatalogRecord
from application.gold_frame import GOLD_COLUMNS
from application.postgres_conformance import (
    POSTGRES_TEMPORAL_CONTRACT_VERSION,
    PostgresConformanceReport,
    PostgresDatabaseConformanceEvidence,
)
from application.postgres_conformance_service import GoldPostgresConformanceVerifier
from application.postgres_delta import source_rows_and_digests
from application.postgres_sync import POSTGRES_DATASET_ID, GoldSyncState, GoldTargetSummary


def test_report_is_deterministic_and_secret_safe() -> None:
    report = PostgresConformanceReport("PASS", ("consumer", "schema", "state"), {"row_count": 1})

    assert report.as_json() == (
        '{"checks": ["consumer", "schema", "state"], "status": "PASS", '
        '"summaries": {"row_count": 1}, '
        '"temporal_contract_version": "pg-temporal-v1"}'
    )
    assert report.temporal_contract_version == POSTGRES_TEMPORAL_CONTRACT_VERSION


@pytest.mark.parametrize("status", ("pass", "unknown", ""))
def test_report_rejects_non_fail_closed_status(status: str) -> None:
    with pytest.raises(ValueError, match="PASS or FAIL"):
        PostgresConformanceReport(status, (), {})


def test_report_rejects_unordered_or_duplicate_checks() -> None:
    with pytest.raises(ValueError, match="unique and sorted"):
        PostgresConformanceReport("FAIL", ("state", "consumer", "state"), {})
    with pytest.raises(ValueError, match="summaries must be sorted"):
        PostgresConformanceReport("FAIL", (), {"z_count": 1, "a_count": 1})
    with pytest.raises(ValueError, match="summary counts cannot be negative"):
        PostgresConformanceReport("FAIL", (), {"count": -1})


def test_verifier_fails_closed_without_exposing_adapter_error() -> None:
    class BrokenCatalog:
        def read(self) -> object:
            raise RuntimeError("postgresql://regime-loader:secret@example.test/db")

    verifier = GoldPostgresConformanceVerifier(
        catalog=BrokenCatalog(),  # type: ignore[arg-type]
        source=object(),  # type: ignore[arg-type]
        repository=object(),  # type: ignore[arg-type]
        inspector=object(),  # type: ignore[arg-type]
    )

    assert verifier.verify() == PostgresConformanceReport("FAIL", ("verification_failed",), {})


def test_database_evidence_rejects_negative_counts() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        PostgresDatabaseConformanceEvidence(4, 2, -1)


def test_verifier_reports_independent_evidence_for_matching_serving_state() -> None:
    timestamp = datetime(2026, 8, 20, tzinfo=UTC)
    frame = pl.DataFrame(
        {"timestamp_m1": [timestamp], **{column: [1.0] for column in GOLD_COLUMNS[1:]}}
    ).with_columns(pl.col("timestamp_m1").cast(pl.Datetime("us", "UTC")))
    _, digests = source_rows_and_digests(frame)
    record = GoldCatalogRecord(
        dataset_id=POSTGRES_DATASET_ID,
        build_id="20260820T000000Z",
        status=GoldBuildStatus.COMPLETE,
        current=True,
        started_at_utc=timestamp,
        completed_at_utc=timestamp,
        schema_version=2,
        feature_version=1,
        min_timestamp=timestamp,
        max_timestamp=timestamp,
        row_count=1,
        data_path="data.parquet",
        build_manifest_path="manifest.json",
        plot_path="plot.png",
        pruned_at_utc=None,
    )
    state = GoldSyncState(
        dataset_id=POSTGRES_DATASET_ID,
        source_build_id=record.build_id,
        data_sha256="a" * 64,
        schema_version=record.schema_version,
        feature_version=record.feature_version,
        row_count=1,
        min_timestamp=timestamp,
        max_timestamp=timestamp,
        synced_at_utc=timestamp,
    )

    class Catalog:
        def read(self) -> list[GoldCatalogRecord]:
            return [record]

    class Source:
        def validate_bundle(self, current: GoldCatalogRecord) -> None:
            assert current == record

        def sha256_path(self, relative_data_path: str) -> str:
            assert relative_data_path == "data.parquet"
            return "a" * 64

        def read_path(self, relative_data_path: str) -> pl.DataFrame:
            assert relative_data_path == "data.parquet"
            return frame

    class Transaction:
        def read_state(self, dataset_id: str) -> GoldSyncState:
            assert dataset_id == POSTGRES_DATASET_ID
            return state

        def read_digests(self, dataset_id: str) -> tuple[object, ...]:
            assert dataset_id == POSTGRES_DATASET_ID
            return digests

        def read_consumer_digests(self, dataset_id: str) -> tuple[object, ...]:
            assert dataset_id == POSTGRES_DATASET_ID
            return digests

        def summary(self, dataset_id: str) -> GoldTargetSummary:
            assert dataset_id == POSTGRES_DATASET_ID
            return GoldTargetSummary(1, timestamp, timestamp)

    class Repository:
        def run_locked(self, operation: Callable[[Transaction], object]) -> object:
            return operation(Transaction())

    class Inspector:
        def inspect(self) -> PostgresDatabaseConformanceEvidence:
            return PostgresDatabaseConformanceEvidence(4, 2, 2)

    assert GoldPostgresConformanceVerifier(
        catalog=Catalog(), source=Source(), repository=Repository(), inspector=Inspector()
    ).verify() == PostgresConformanceReport(
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
        {
            "consumer_row_count": 1,
            "digest_row_count": 1,
            "role_count": 2,
            "schema_table_count": 4,
            "temporal_probe_count": 2,
        },
    )


def test_agreement_requires_identical_source_consumer_index_summary_and_state() -> None:
    timestamp = datetime(2026, 8, 20, tzinfo=UTC)
    frame = pl.DataFrame(
        {"timestamp_m1": [timestamp], **{column: [1.0] for column in GOLD_COLUMNS[1:]}}
    ).with_columns(pl.col("timestamp_m1").cast(pl.Datetime("us", "UTC")))
    _, digests = source_rows_and_digests(frame)
    record = GoldCatalogRecord(
        dataset_id=POSTGRES_DATASET_ID,
        build_id="20260820T000000Z",
        status=GoldBuildStatus.COMPLETE,
        current=True,
        started_at_utc=timestamp,
        completed_at_utc=timestamp,
        schema_version=2,
        feature_version=1,
        min_timestamp=timestamp,
        max_timestamp=timestamp,
        row_count=1,
        data_path="data.parquet",
        build_manifest_path="manifest.json",
        plot_path="plot.png",
        pruned_at_utc=None,
    )
    state = GoldSyncState(
        dataset_id=POSTGRES_DATASET_ID,
        source_build_id=record.build_id,
        data_sha256="a" * 64,
        schema_version=record.schema_version,
        feature_version=record.feature_version,
        row_count=1,
        min_timestamp=timestamp,
        max_timestamp=timestamp,
        synced_at_utc=timestamp,
    )

    GoldPostgresConformanceVerifier._assert_agreement(
        record,
        "a" * 64,
        digests,
        state,
        digests,
        digests,
        GoldTargetSummary(1, timestamp, timestamp),
    )

    with pytest.raises(ValueError, match="consumer"):
        GoldPostgresConformanceVerifier._assert_agreement(
            record,
            "a" * 64,
            digests,
            state,
            digests,
            (),
            GoldTargetSummary(1, timestamp, timestamp),
        )
