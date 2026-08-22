"""Export protected operational YAML configuration as shell-safe environment assignments."""

from __future__ import annotations

import shlex
import sys
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

POSTGRES_HOST = "10.10.1.3"
POSTGRES_PORT = "54321"
POSTGRES_USER = "regime-data-loader"
LOG_BASENAME = "regime-data-loader.log"


def _value(config: dict[str, Any], section: str, key: str) -> str:
    value = config.get(section, {}).get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing {section}.{key} in config.yaml")
    return value


def _canonical_log_path(project_root: str) -> str:
    return str(PurePosixPath(project_root) / ".logs" / LOG_BASENAME)


def _postgres_values(config: dict[str, Any]) -> dict[str, str]:
    host = _value(config, "runtime", "postgres_host")
    port = _value(config, "runtime", "postgres_port")
    user = _value(config, "runtime", "postgres_user")
    database = _value(config, "runtime", "postgres_database")
    password = _value(config, "secrets", "postgres_password")
    if host != POSTGRES_HOST:
        raise ValueError(f"runtime.postgres_host must be {POSTGRES_HOST}")
    if port != POSTGRES_PORT:
        raise ValueError(f"runtime.postgres_port must be {POSTGRES_PORT}")
    if user != POSTGRES_USER:
        raise ValueError(f"runtime.postgres_user must be {POSTGRES_USER}")
    return {
        "PGHOST": host,
        "PGPORT": port,
        "PGUSER": user,
        "PGDATABASE": database,
        "PGPASSWORD": password,
    }


def export(path: Path) -> str:
    parsed = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise ValueError("config.yaml must be a mapping")
    project_root = _value(parsed, "runtime", "project_root")
    canonical_log = _canonical_log_path(project_root)
    configured_log = parsed.get("runtime", {}).get("log_path")
    if configured_log is not None and configured_log != canonical_log:
        raise ValueError(f"runtime.log_path must be {canonical_log}")
    values = {
        "FRED_API_KEY": _value(parsed, "secrets", "fred_api_key"),
        "SSL_CERT_FILE": _value(parsed, "runtime", "ssl_cert_file"),
        "MARKET_REGIME_GOLD_MIRROR_ROOT": _value(parsed, "runtime", "gold_mirror_root"),
        "PATH": _value(parsed, "runtime", "path"),
        "HOME": _value(parsed, "runtime", "home"),
        "PROJECT_ROOT": project_root,
        "LAKE_ROOT": _value(parsed, "runtime", "lake_root"),
        "LOG_PATH": canonical_log,
        **_postgres_values(parsed),
    }
    return "\n".join(f"export {name}={shlex.quote(value)}" for name, value in values.items())


def main() -> int:
    try:
        print(export(Path(sys.argv[1])))
    except (IndexError, OSError, ValueError, yaml.YAMLError) as error:
        print(f"config export failed: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
