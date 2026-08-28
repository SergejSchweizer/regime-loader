"""Application contracts for independent PostgreSQL serving-plane verification."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

POSTGRES_TEMPORAL_CONTRACT_VERSION = "pg-temporal-v1"


@dataclass(frozen=True, slots=True)
class PostgresDatabaseConformanceEvidence:
    """Sanitized facts gathered independently from the serving database."""

    schema_table_count: int
    role_count: int
    temporal_probe_count: int

    def __post_init__(self) -> None:
        if min(self.schema_table_count, self.role_count, self.temporal_probe_count) < 0:
            raise ValueError("PostgreSQL conformance evidence counts cannot be negative")

    def as_summary(self) -> dict[str, int]:
        return {
            "role_count": self.role_count,
            "schema_table_count": self.schema_table_count,
            "temporal_probe_count": self.temporal_probe_count,
        }


@dataclass(frozen=True, slots=True)
class PostgresConformanceReport:
    """Deterministic, secret-safe result of an independent PostgreSQL inspection."""

    status: str
    checks: tuple[str, ...]
    summaries: dict[str, int]
    temporal_contract_version: str = POSTGRES_TEMPORAL_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if self.status not in {"PASS", "FAIL"}:
            raise ValueError("PostgreSQL conformance status must be PASS or FAIL")
        if self.temporal_contract_version != POSTGRES_TEMPORAL_CONTRACT_VERSION:
            raise ValueError("unsupported PostgreSQL temporal contract version")
        if tuple(sorted(set(self.checks))) != self.checks:
            raise ValueError("PostgreSQL conformance checks must be unique and sorted")
        if tuple(sorted(self.summaries)) != tuple(self.summaries):
            raise ValueError("PostgreSQL conformance summaries must be sorted")
        if any(value < 0 for value in self.summaries.values()):
            raise ValueError("PostgreSQL conformance summary counts cannot be negative")

    def as_json(self) -> str:
        return json.dumps(
            {
                "checks": list(self.checks),
                "status": self.status,
                "summaries": self.summaries,
                "temporal_contract_version": self.temporal_contract_version,
            },
            ensure_ascii=True,
            sort_keys=True,
        )


class PostgresConformanceVerifier(Protocol):
    """Read-only adapter port for independently verifying the serving replica."""

    def verify(self) -> PostgresConformanceReport: ...


class PostgresDatabaseConformanceInspector(Protocol):
    """Port for adapter-owned database facts outside the sync mutation path."""

    def inspect(self) -> PostgresDatabaseConformanceEvidence: ...
