from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from api import cli
from application.gold_frame import GOLD_COLUMNS
from application.paths import LakePaths
from application.postgres_sync import GoldSyncResult
from ingestion.gold_build_store import GoldBuildStore
from ingestion.gold_sync_source import FilesystemGoldFrameSource


def _frame() -> pl.DataFrame:
    data: dict[str, list[object]] = {
        "timestamp_m1": [datetime(2026, 8, 22, tzinfo=UTC)],
    }
    for index, column in enumerate(GOLD_COLUMNS[1:], start=1):
        data[column] = [float(index)]
    return pl.DataFrame(data).with_columns(pl.col("timestamp_m1").cast(pl.Datetime("us", "UTC")))


def _postgres_env(monkeypatch: pytest.MonkeyPatch, *, password: str = "repo-secret") -> None:
    monkeypatch.setenv("PGHOST", "10.10.1.3")
    monkeypatch.setenv("PGPORT", "54321")
    monkeypatch.setenv("PGUSER", "regime-loader")
    monkeypatch.setenv("PGDATABASE", "quant_data")
    monkeypatch.setenv("PGPASSWORD", password)


def _postgres_admin_env(monkeypatch: pytest.MonkeyPatch, *, password: str = "admin-secret") -> None:
    monkeypatch.setenv("MARKET_REGIME_POSTGRES_ADMIN_HOST", "10.10.1.3")
    monkeypatch.setenv("MARKET_REGIME_POSTGRES_ADMIN_PORT", "54321")
    monkeypatch.setenv("MARKET_REGIME_POSTGRES_ADMIN_USER", "regime-loader-admin")
    monkeypatch.setenv("MARKET_REGIME_POSTGRES_ADMIN_DATABASE", "quant_data")
    monkeypatch.setenv("MARKET_REGIME_POSTGRES_ADMIN_PASSWORD", password)


def test_filesystem_gold_source_hashes_and_reads_only_contained_catalog_path(
    tmp_path: Path,
) -> None:
    paths = LakePaths(tmp_path / "lake")
    store = GoldBuildStore(paths)
    artifact = store.create(_frame(), build_id="20260822T100000Z")
    source = FilesystemGoldFrameSource(paths, store)
    relative = artifact.data_path.relative_to(paths.gold_dataset_root()).as_posix()

    assert source.sha256_path(relative) == artifact.data_sha256
    assert source.read_path(relative).equals(_frame())
    with pytest.raises(ValueError, match="relative"):
        source.read_path(str(artifact.data_path.resolve()))
    with pytest.raises(ValueError, match="escapes"):
        source.read_path("../../silver/data.parquet")


def test_postgres_runtime_composition_uses_exact_protected_endpoint_without_connecting(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _postgres_env(monkeypatch)
    captured: list[object] = []

    class RepositoryStub:
        def __init__(self, config: object) -> None:
            captured.append(config)

    monkeypatch.setattr(cli, "PostgresGoldSyncRepository", RepositoryStub)
    runtime = cli.build_postgres_sync_runtime(lake_root=tmp_path / "lake", stderr=io.StringIO())

    assert runtime.sync.repository is not None
    assert len(captured) == 1
    config = captured[0]
    assert isinstance(config, cli.PostgresSyncConfig)
    assert config.host == "10.10.1.3"
    assert config.port == 54321
    assert config.user == "regime-loader"
    assert config.database == "quant_data"
    assert config.password == "repo-secret"
    assert "repo-secret" not in repr(config)


def test_missing_postgres_config_fails_before_repository_or_provider_runtime_creation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    for name in ("PGHOST", "PGPORT", "PGUSER", "PGDATABASE", "PGPASSWORD"):
        monkeypatch.delenv(name, raising=False)
    repository_calls = 0
    provider_runtime_calls = 0

    def repository_stub(config: object) -> object:
        nonlocal repository_calls
        repository_calls += 1
        return config

    def provider_runtime_stub(**kwargs: object) -> object:
        nonlocal provider_runtime_calls
        provider_runtime_calls += 1
        return kwargs

    monkeypatch.setattr(cli, "PostgresGoldSyncRepository", repository_stub)
    monkeypatch.setattr(cli, "build_runtime", provider_runtime_stub)
    stderr = io.StringIO()

    code = cli.main(
        ["--lake-root", str(tmp_path / "lake"), "gold-sync-postgres"],
        stdout=io.StringIO(),
        stderr=stderr,
    )

    assert code == cli.EXIT_INPUT
    assert repository_calls == 0
    assert provider_runtime_calls == 0
    payload = json.loads(stderr.getvalue())
    assert payload["command"] == "gold-sync-postgres"
    assert payload["stage"] == "configuration"
    assert payload["status"] == "failed"


def test_postgres_migration_composition_uses_only_distinct_admin_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _postgres_admin_env(monkeypatch)
    captured: list[object] = []

    class MigratorStub:
        def __init__(self, config: object) -> None:
            captured.append(config)

    monkeypatch.setattr(cli, "PostgresGoldSchemaMigrator", MigratorStub)
    runtime = cli.build_postgres_migration_runtime(stderr=io.StringIO())

    assert runtime.migrator is not None
    assert len(captured) == 1
    config = captured[0]
    assert isinstance(config, cli.PostgresAdminConfig)
    assert config.user == "regime-loader-admin"
    assert "admin-secret" not in repr(config)


def test_postgres_migration_failure_redacts_admin_password(monkeypatch: pytest.MonkeyPatch) -> None:
    _postgres_admin_env(monkeypatch)
    stderr = io.StringIO()

    def broken_runtime(**kwargs: object) -> cli.PostgresMigrationRuntime:
        raise RuntimeError("migration failed: admin-secret")

    monkeypatch.setattr(cli, "build_postgres_migration_runtime", broken_runtime)
    code = cli.main(["postgres-migrate"], stdout=io.StringIO(), stderr=stderr)

    assert code == cli.EXIT_PIPELINE
    assert "admin-secret" not in stderr.getvalue()
    assert json.loads(stderr.getvalue())["command"] == "postgres-migrate"


def test_gold_sync_command_dispatches_only_sync_service_and_logs_exact_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _postgres_env(monkeypatch)
    sync_calls = 0
    provider_runtime_calls = 0
    stderr = io.StringIO()

    class SyncStub:
        def sync(self) -> GoldSyncResult:
            nonlocal sync_calls
            sync_calls += 1
            return GoldSyncResult(
                dataset_id="regime_features_daily",
                source_build_id="20260822T100000Z",
                inserted=2,
                updated=1,
                deleted=1,
                unchanged=100,
            )

    def provider_runtime_stub(**kwargs: object) -> object:
        nonlocal provider_runtime_calls
        provider_runtime_calls += 1
        return kwargs

    runtime = cli.PostgresSyncRuntime(
        sync=SyncStub(),  # type: ignore[arg-type]
        event_sink=cli.JsonEventSink(cli._logger(stderr), secrets=("repo-secret",)),
    )
    monkeypatch.setattr(cli, "build_postgres_sync_runtime", lambda **kwargs: runtime)
    monkeypatch.setattr(cli, "build_runtime", provider_runtime_stub)

    code = cli.main(["gold-sync-postgres"], stdout=io.StringIO(), stderr=stderr)

    assert code == cli.EXIT_SUCCESS
    assert sync_calls == 1
    assert provider_runtime_calls == 0
    payload = json.loads(stderr.getvalue())
    assert payload == {
        "command": "gold-sync-postgres",
        "dataset_id": "regime_features_daily",
        "deleted": 1,
        "inserted": 2,
        "source_build_id": "20260822T100000Z",
        "stage": "postgres_sync",
        "status": "success",
        "unchanged": 100,
        "updated": 1,
    }


def test_postgres_failure_is_nonzero_and_redacts_password_and_credential_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _postgres_env(monkeypatch)
    stderr = io.StringIO()
    credential_text = "postgresql://regime-loader:repo-secret@10.10.1.3:54321/quant_data"

    def broken_runtime(**kwargs: object) -> cli.PostgresSyncRuntime:
        raise RuntimeError(f"database failed: {credential_text}")

    monkeypatch.setattr(cli, "build_postgres_sync_runtime", broken_runtime)
    code = cli.main(["gold-sync-postgres"], stdout=io.StringIO(), stderr=stderr)

    assert code == cli.EXIT_PIPELINE
    text = stderr.getvalue()
    assert "repo-secret" not in text
    assert credential_text not in text
    payload = json.loads(text)
    assert payload["command"] == "gold-sync-postgres"
    assert payload["stage"] == "pipeline"
    assert payload["status"] == "failed"
