"""Fail-closed orchestration contract for the explicit production reconstruction."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from application.postgres_conformance import PostgresConformanceReport
from application.postgres_sync import GoldSyncResult

_STAGES = (
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
)


@dataclass(frozen=True, slots=True)
class ProductionReconstructionReport:
    """Deterministic, credential-free evidence suitable for source control."""

    status: str
    completed_stages: tuple[str, ...]
    failed_stage: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"PASS", "FAIL"}:
            raise ValueError("reconstruction status must be PASS or FAIL")
        if len(set(self.completed_stages)) != len(self.completed_stages):
            raise ValueError("reconstruction stages must be unique")
        if any(stage not in _STAGES for stage in self.completed_stages):
            raise ValueError("reconstruction report contains an unknown stage")
        if self.completed_stages != _STAGES[: len(self.completed_stages)]:
            raise ValueError("reconstruction stages must be an ordered prefix")
        if self.status == "PASS":
            if self.completed_stages != _STAGES or self.failed_stage is not None:
                raise ValueError("PASS report requires every stage and no failure")
        elif self.failed_stage not in _STAGES:
            raise ValueError("reconstruction report contains an unknown failed stage")
        elif self.failed_stage in self.completed_stages:
            raise ValueError("FAIL report requires one uncompleted known stage")

    def as_json(self) -> str:
        return json.dumps(
            {
                "completed_stages": list(self.completed_stages),
                "failed_stage": self.failed_stage,
                "status": self.status,
            },
            ensure_ascii=True,
            sort_keys=True,
        )


class ProductionReconstructionOperations(Protocol):
    """Infrastructure boundary for guarded reconstruction stages."""

    def disable_scheduling(self) -> None: ...

    def acquire_locks(self) -> None: ...

    def preflight_endpoint(self) -> None: ...

    def validate_backup(self) -> None: ...

    def reconcile(self, series_ids: Sequence[str]) -> None: ...

    def rebuild_silver(self, series_ids: Sequence[str]) -> None: ...

    def publish_gold(self) -> None: ...

    def recreate_schema(self) -> None: ...

    def publish_postgres(self) -> GoldSyncResult: ...

    def verify_postgres(self) -> PostgresConformanceReport: ...

    def replay_postgres(self) -> GoldSyncResult: ...

    def verify_sunday_wrapper(self) -> None: ...

    def enable_scheduling(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ProductionReconstruction:
    """Run all destructive steps only after maintenance, lock, and backup gates pass."""

    operations: ProductionReconstructionOperations
    series_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if len(self.series_ids) != 13 or len(set(self.series_ids)) != 13:
            raise ValueError("production reconstruction requires exactly 13 unique series")

    def run(self) -> ProductionReconstructionReport:
        completed: list[str] = []
        try:
            self.operations.disable_scheduling()
            completed.append("maintenance")
            self.operations.acquire_locks()
            completed.append("locks")
            self.operations.preflight_endpoint()
            completed.append("endpoint")
            self.operations.validate_backup()
            completed.append("backup")
            self.operations.reconcile(self.series_ids)
            completed.append("reconcile")
            self.operations.rebuild_silver(self.series_ids)
            completed.append("silver")
            self.operations.publish_gold()
            completed.append("gold")
            self.operations.recreate_schema()
            completed.append("schema")
            self.operations.publish_postgres()
            completed.append("publication")
            if self.operations.verify_postgres().status != "PASS":
                return ProductionReconstructionReport("FAIL", tuple(completed), "verification")
            completed.append("verification")
            replay = self.operations.replay_postgres()
            if replay.inserted or replay.updated or replay.deleted:
                return ProductionReconstructionReport("FAIL", tuple(completed), "replay")
            completed.append("replay")
            self.operations.verify_sunday_wrapper()
            completed.append("sunday_wrapper")
        except Exception:
            failed_stage = _STAGES[len(completed)]
            return ProductionReconstructionReport("FAIL", tuple(completed), failed_stage)

        self.operations.enable_scheduling()
        return ProductionReconstructionReport("PASS", tuple(completed))
