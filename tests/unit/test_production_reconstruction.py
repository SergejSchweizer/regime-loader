from __future__ import annotations

import pytest

from application.postgres_conformance import PostgresConformanceReport
from application.postgres_sync import GoldSyncResult
from application.production_reconstruction import (
    ProductionReconstruction,
    ProductionReconstructionReport,
)
from application.registry import SERIES_REGISTRY


class OperationsStub:
    def __init__(self, *, failure: str | None = None, verification: str = "PASS") -> None:
        self.failure = failure
        self.verification = verification
        self.calls: list[str] = []
        self.replay = GoldSyncResult("regime_features_daily", "20260828T120000Z", 0, 0, 0, 1)

    def _call(self, name: str) -> None:
        self.calls.append(name)
        if name == self.failure:
            raise RuntimeError("private operational failure")

    def disable_scheduling(self) -> None:
        self._call("maintenance")

    def acquire_locks(self) -> None:
        self._call("locks")

    def preflight_endpoint(self) -> None:
        self._call("endpoint")

    def validate_backup(self) -> None:
        self._call("backup")

    def reconcile(self, series_ids: tuple[str, ...]) -> None:
        assert series_ids == tuple(SERIES_REGISTRY)
        self._call("reconcile")

    def rebuild_silver(self, series_ids: tuple[str, ...]) -> None:
        assert series_ids == tuple(SERIES_REGISTRY)
        self._call("silver")

    def publish_gold(self) -> None:
        self._call("gold")

    def recreate_schema(self) -> None:
        self._call("schema")

    def publish_postgres(self) -> GoldSyncResult:
        self._call("publication")
        return self.replay

    def verify_postgres(self) -> PostgresConformanceReport:
        self._call("verification")
        return PostgresConformanceReport(self.verification, ("schema",), {})

    def replay_postgres(self) -> GoldSyncResult:
        self._call("replay")
        return self.replay

    def verify_sunday_wrapper(self) -> None:
        self._call("sunday_wrapper")

    def enable_scheduling(self) -> None:
        self._call("enable")


def test_reconstruction_runs_exactly_all_stages_and_reenables_only_after_pass() -> None:
    operations = OperationsStub()

    report = ProductionReconstruction(operations, tuple(SERIES_REGISTRY)).run()

    assert report.status == "PASS"
    assert operations.calls == [
        "maintenance",
        "locks",
        "endpoint",
        "backup",
        "reconcile",
        "silver",
        "gold",
        "schema",
        "publication",
        "verification",
        "replay",
        "sunday_wrapper",
        "enable",
    ]
    assert report.as_json() == (
        '{"completed_stages": ["maintenance", "locks", "endpoint", "backup", "reconcile", '
        '"silver", "gold", "schema", "publication", "verification", "replay", '
        '"sunday_wrapper"], "failed_stage": null, "status": "PASS"}'
    )


@pytest.mark.parametrize("failure", ("locks", "endpoint", "backup", "schema"))
def test_reconstruction_fails_closed_before_or_during_destructive_work(failure: str) -> None:
    operations = OperationsStub(failure=failure)

    report = ProductionReconstruction(operations, tuple(SERIES_REGISTRY)).run()

    assert report.status == "FAIL"
    assert report.failed_stage == failure
    assert "enable" not in operations.calls
    assert "private operational failure" not in report.as_json()


def test_reconstruction_keeps_scheduling_disabled_after_failed_independent_verification() -> None:
    operations = OperationsStub(verification="FAIL")

    report = ProductionReconstruction(operations, tuple(SERIES_REGISTRY)).run()

    assert report == ProductionReconstructionReport(
        "FAIL",
        (
            "maintenance",
            "locks",
            "endpoint",
            "backup",
            "reconcile",
            "silver",
            "gold",
            "schema",
            "publication",
        ),
        "verification",
    )
    assert "replay" not in operations.calls
    assert "enable" not in operations.calls


def test_reconstruction_rejects_mutating_replay() -> None:
    operations = OperationsStub()
    operations.replay = GoldSyncResult("regime_features_daily", "20260828T120000Z", 1, 0, 0, 0)

    report = ProductionReconstruction(operations, tuple(SERIES_REGISTRY)).run()

    assert report.failed_stage == "replay"
    assert "sunday_wrapper" not in operations.calls
    assert "enable" not in operations.calls


def test_reconstruction_requires_exactly_thirteen_unique_series() -> None:
    with pytest.raises(ValueError, match="13 unique"):
        ProductionReconstruction(OperationsStub(), tuple(SERIES_REGISTRY)[:-1])


def test_report_rejects_out_of_order_or_sensitive_failure_values() -> None:
    with pytest.raises(ValueError, match="ordered prefix"):
        ProductionReconstructionReport("FAIL", ("backup",), "endpoint")
    with pytest.raises(ValueError, match="unknown failed stage"):
        ProductionReconstructionReport("FAIL", (), "postgresql://private")
