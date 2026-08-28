from __future__ import annotations

from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

import ingestion.production_reconstruction_operations as module
from application.postgres_conformance import PostgresConformanceReport
from application.postgres_sync import GoldSyncResult
from ingestion.postgres_gold_repository import PostgresAdminConfig, PostgresSyncConfig


class Cursor:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row
        self.queries: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: str, parameters: tuple[object, ...] | None = None) -> None:
        self.queries.append((query, parameters))

    def fetchone(self) -> tuple[object, ...]:
        return self.row


class Connection:
    def __init__(self, row: tuple[object, ...] = (True,)) -> None:
        self.cursor_value = Cursor(row)
        self.closed = False

    def cursor(self) -> Cursor:
        return self.cursor_value

    def close(self) -> None:
        self.closed = True


class Pipeline:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[str, ...] | None]] = []

    def reconcile(self, series: tuple[str, ...], *, today: date) -> None:
        self.calls.append(("reconcile", series))

    def silver_build(self, series: tuple[str, ...]) -> None:
        self.calls.append(("silver", series))

    def gold_build(self) -> None:
        self.calls.append(("gold", None))


def _configurations() -> tuple[PostgresSyncConfig, PostgresAdminConfig]:
    runtime = PostgresSyncConfig("10.10.1.3", 54321, "regime-loader", "db", "runtime")
    admin = PostgresAdminConfig("10.10.1.3", 54321, "admin", "db", "admin")
    return runtime, admin


def _operations(tmp_path: Path) -> module.GuardedProductionReconstructionOperations:
    runtime, admin = _configurations()
    sync = SimpleNamespace(
        sync=lambda: GoldSyncResult("regime_features_daily", "20260828T000000Z", 0, 0, 0, 1)
    )
    verifier = SimpleNamespace(verify=lambda: PostgresConformanceReport("PASS", ("schema",), {}))
    return module.GuardedProductionReconstructionOperations(
        pipeline=Pipeline(),  # type: ignore[arg-type]
        sync=sync,  # type: ignore[arg-type]
        verifier=verifier,  # type: ignore[arg-type]
        reconstructor=SimpleNamespace(recreate=lambda: None),  # type: ignore[arg-type]
        runtime_config=runtime,
        admin_config=admin,
        catalog=SimpleNamespace(read=lambda: ()),  # type: ignore[arg-type]
        lake_root=tmp_path / "lake",
        project_root=tmp_path / "project",
        today=date(2026, 8, 28),
        sunday_runner=tmp_path / "runner",
    )


def test_locks_and_endpoint_preflight_require_exact_production_server(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    operations = _operations(tmp_path)
    connection = Connection()
    monkeypatch.setattr(module.psycopg, "connect", lambda **kwargs: connection)

    operations.disable_scheduling()
    operations.acquire_locks()
    connection.cursor_value.row = ("10.10.1.3", 54321)
    operations.preflight_endpoint()

    assert (operations.project_root / ".maintenance/regime-loader-reconstruction").is_file()
    assert connection.cursor_value.queries[0][1] == (module._lock_key(),)
    operations.enable_scheduling()
    assert connection.closed


def test_endpoint_preflight_accepts_a_held_lock_through_port_mapping(tmp_path: Path) -> None:
    operations = _operations(tmp_path)
    operations._maintenance_connection = Connection(("127.0.0.1", 54321))  # type: ignore[assignment]

    operations.preflight_endpoint()


def test_locks_reject_postgres_contention_and_release_runner_lock(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    operations = _operations(tmp_path)
    connection = Connection((False,))
    monkeypatch.setattr(module.psycopg, "connect", lambda **kwargs: connection)

    with pytest.raises(RuntimeError, match="maintenance lock"):
        operations.acquire_locks()

    assert connection.closed
    assert operations._runner_handle is None


def test_endpoint_preflight_requires_held_maintenance_lock(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="not held"):
        _operations(tmp_path).preflight_endpoint()


def test_maintenance_marker_is_idempotent_for_explicit_reconstruction_retries(
    tmp_path: Path,
) -> None:
    operations = _operations(tmp_path)

    operations.disable_scheduling()
    operations.disable_scheduling()

    assert (operations.project_root / ".maintenance/regime-loader-reconstruction").is_file()


def test_backup_snapshots_full_lake_and_private_database_dump(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    operations = _operations(tmp_path)
    gold_root = operations.lake_root / "gold/dataset=regime_features_daily"
    data_path = gold_root / "versions/build_id=20260828T000000Z/data.parquet"
    data_path.parent.mkdir(parents=True)
    (gold_root / "manifest.parquet").write_text("catalog")
    data_path.write_text("data")
    (operations.lake_root / "state").mkdir()
    (operations.lake_root / "state/ingestion_state.parquet").write_text("state")
    relative_data_path = "versions/build_id=20260828T000000Z/data.parquet"
    monkeypatch.setattr(
        module,
        "select_current_sync_record",
        lambda records: SimpleNamespace(data_path=relative_data_path),
    )

    def dump(command: list[str], **kwargs: object) -> SimpleNamespace:
        Path(command[command.index("--file") + 1]).write_text("dump")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(module.subprocess, "run", dump)
    operations.validate_backup()

    backup = next((operations.project_root / "artifacts/private/reconstruction-backups").iterdir())
    assert (backup / "lake/state/ingestion_state.parquet").read_text() == "state"
    assert (backup / "loader-schemas.sql.sha256").is_file()
    assert (backup / "RESTORE.txt").is_file()


def test_backup_rejects_missing_evidence_or_invalid_database_dump(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    operations = _operations(tmp_path)
    monkeypatch.setattr(
        module,
        "select_current_sync_record",
        lambda records: SimpleNamespace(data_path="data.parquet"),
    )
    with pytest.raises(RuntimeError, match="lake evidence"):
        operations.validate_backup()

    operations.lake_root.mkdir()
    gold_root = operations.lake_root / "gold/dataset=regime_features_daily"
    gold_root.mkdir(parents=True)
    (gold_root / "manifest.parquet").write_text("catalog")
    (gold_root / "data.parquet").write_text("data")
    (operations.lake_root / "state").mkdir()
    (operations.lake_root / "state/ingestion_state.parquet").write_text("state")
    monkeypatch.setattr(
        module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=1)
    )
    with pytest.raises(RuntimeError, match="backup validation"):
        operations.validate_backup()


def test_operations_delegate_pipeline_sync_and_wrapper_without_payload_output(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    operations = _operations(tmp_path)
    monkeypatch.setattr(
        module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0)
    )

    operations.reconcile(("vix",))
    operations.rebuild_silver(("vix",))
    operations.publish_gold()
    assert operations.publish_postgres().inserted == 0
    assert operations.verify_postgres().status == "PASS"
    assert operations.replay_postgres().deleted == 0
    operations.verify_sunday_wrapper()

    assert operations.pipeline.calls == [
        ("reconcile", ("vix",)),
        ("silver", ("vix",)),
        ("gold", None),
    ]


def test_wrapper_failure_and_missing_marker_keep_scheduling_disabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    operations = _operations(tmp_path)
    monkeypatch.setattr(
        module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=4)
    )

    with pytest.raises(RuntimeError, match="wrapper verification"):
        operations.verify_sunday_wrapper()
    with pytest.raises(RuntimeError, match="marker is missing"):
        operations.enable_scheduling()
