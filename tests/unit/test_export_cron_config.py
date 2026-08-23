from __future__ import annotations

import shlex
from pathlib import Path

import pytest

from scripts.export_cron_config import export


def _config(*, password: str = "repo secret", user: str = "regime-loader") -> str:
    return f"""runtime:
  home: /home/a
  path: /bin
  project_root: /project path
  lake_root: /lake
  log_path: /project path/.logs/regime-loader.log
  ssl_cert_file: /cert
  gold_mirror_root: /mirror
  postgres_host: 10.10.1.3
  postgres_port: "54321"
  postgres_user: {user}
  postgres_database: quant_data
secrets:
  fred_api_key: secret key
  postgres_password: {password!r}
"""


def _exports(text: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in text.splitlines():
        prefix, assignment = line.split(" ", 1)
        assert prefix == "export"
        name, value = assignment.split("=", 1)
        parsed = shlex.split(f"x={value}")[0]
        result[name] = parsed.split("=", 1)[1]
    return result


def test_export_cron_config_quotes_required_runtime_and_postgres_values(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(_config(), encoding="utf-8")
    values = _exports(export(config))
    assert values["FRED_API_KEY"] == "secret key"
    assert values["PROJECT_ROOT"] == "/project path"
    assert values["PGHOST"] == "10.10.1.3"
    assert values["PGPORT"] == "54321"
    assert values["PGUSER"] == "regime-loader"
    assert values["PGDATABASE"] == "quant_data"
    assert values["PGPASSWORD"] == "repo secret"
    assert values["LOG_PATH"] == "/project path/.logs/regime-loader.log"


def test_export_rejects_wrong_postgres_endpoint_or_user(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(_config().replace("10.10.1.3", "localhost"), encoding="utf-8")
    with pytest.raises(ValueError, match="postgres_host"):
        export(config)

    config.write_text(_config(user="postgres"), encoding="utf-8")
    with pytest.raises(ValueError, match="postgres_user"):
        export(config)


def test_export_rejects_missing_postgres_password_without_leaking_secret(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        _config().replace("  postgres_password: 'repo secret'\n", ""), encoding="utf-8"
    )
    with pytest.raises(ValueError, match="postgres_password") as exc_info:
        export(config)
    assert "repo secret" not in str(exc_info.value)


def test_export_rejects_noncanonical_log_path(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        _config().replace(
            "/project path/.logs/regime-loader.log",
            "/var/log/regime-loader.log",
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match=r"\.logs/regime-loader\.log"):
        export(config)


def test_repository_ignores_protected_config_and_logs() -> None:
    gitignore = Path(".gitignore").read_text(encoding="utf-8")
    assert "config.yaml" in gitignore
    assert ".logs/" in gitignore
