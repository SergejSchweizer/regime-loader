"""Provision the dedicated least-privilege PostgreSQL role for this repository."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass

POSTGRES_HOST = "10.10.1.3"
POSTGRES_PORT = 54321
POSTGRES_ROLE = "regime-data-loader"
POSTGRES_SCHEMAS = ("regime_data", "regime_data_sync")


def _identifier(value: str) -> str:
    if not value:
        raise ValueError("PostgreSQL identifier cannot be empty")
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True, slots=True)
class ProvisioningConfig:
    host: str
    port: int
    database: str
    admin_user: str
    admin_password: str
    app_password: str

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> ProvisioningConfig:
        host = env.get("MARKET_REGIME_POSTGRES_HOST", POSTGRES_HOST).strip()
        port_text = env.get("MARKET_REGIME_POSTGRES_PORT", str(POSTGRES_PORT)).strip()
        database = env.get("MARKET_REGIME_POSTGRES_DATABASE", "").strip()
        admin_user = env.get("MARKET_REGIME_POSTGRES_ADMIN_USER", "").strip()
        admin_password = env.get("MARKET_REGIME_POSTGRES_ADMIN_PASSWORD", "")
        app_password = env.get("MARKET_REGIME_POSTGRES_PASSWORD", "")
        try:
            port = int(port_text)
        except ValueError as exc:
            raise ValueError("MARKET_REGIME_POSTGRES_PORT must be an integer") from exc
        if host != POSTGRES_HOST or port != POSTGRES_PORT:
            raise ValueError(f"provisioning endpoint must be {POSTGRES_HOST}:{POSTGRES_PORT}")
        if not database:
            raise ValueError("MARKET_REGIME_POSTGRES_DATABASE is required")
        if not admin_user:
            raise ValueError("MARKET_REGIME_POSTGRES_ADMIN_USER is required")
        if not admin_password:
            raise ValueError("MARKET_REGIME_POSTGRES_ADMIN_PASSWORD is required")
        if not app_password:
            raise ValueError("MARKET_REGIME_POSTGRES_PASSWORD is required")
        if admin_password == app_password:
            raise ValueError("application password must differ from administrator password")
        return cls(host, port, database, admin_user, admin_password, app_password)


def provision_sql(database: str, app_password: str) -> str:
    """Return idempotent fail-closed DDL without exposing administrator credentials."""
    role_i = _identifier(POSTGRES_ROLE)
    role_l = _literal(POSTGRES_ROLE)
    database_i = _identifier(database)
    password_l = _literal(app_password)
    schemas = "\n".join(
        f"CREATE SCHEMA IF NOT EXISTS {_identifier(schema)} AUTHORIZATION {role_i};\n"
        f"REVOKE ALL ON SCHEMA {_identifier(schema)} FROM PUBLIC;\n"
        f"GRANT USAGE, CREATE ON SCHEMA {_identifier(schema)} TO {role_i};"
        for schema in POSTGRES_SCHEMAS
    )
    ownership_checks = " OR ".join(
        "EXISTS (SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner "
        f"WHERE n.nspname={_literal(schema)} AND r.rolname<>{role_l})"
        for schema in POSTGRES_SCHEMAS
    )
    return f"""DO $provision$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {role_l}) THEN
        CREATE ROLE {role_i}
            LOGIN PASSWORD {password_l}
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    ELSE
        IF EXISTS (
            SELECT 1 FROM pg_roles
            WHERE rolname = {role_l}
              AND (NOT rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole
                   OR rolreplication OR rolbypassrls)
        ) THEN
            RAISE EXCEPTION 'existing regime-data-loader role has incompatible privileges';
        END IF;
        ALTER ROLE {role_i} PASSWORD {password_l};
    END IF;
END
$provision$;

GRANT CONNECT ON DATABASE {database_i} TO {role_i};
{schemas}

DO $verify$
BEGIN
    IF {ownership_checks} THEN
        RAISE EXCEPTION 'regime-data-loader schema ownership is incompatible';
    END IF;
END
$verify$;
"""


def psql_command(config: ProvisioningConfig) -> list[str]:
    return [
        "psql",
        "--host",
        config.host,
        "--port",
        str(config.port),
        "--username",
        config.admin_user,
        "--dbname",
        config.database,
        "--no-psqlrc",
        "--set",
        "ON_ERROR_STOP=1",
    ]


def sanitized(text: str, config: ProvisioningConfig) -> str:
    result = text
    for secret in (config.admin_password, config.app_password):
        if secret:
            result = result.replace(secret, "***")
    return result


def run(config: ProvisioningConfig) -> None:
    environment = dict(os.environ)
    environment["PGPASSWORD"] = config.admin_password
    completed = subprocess.run(
        psql_command(config),
        input=provision_sql(config.database, config.app_password),
        text=True,
        capture_output=True,
        env=environment,
        check=False,
    )
    if completed.returncode != 0:
        detail = sanitized(completed.stderr.strip(), config)
        raise RuntimeError(f"PostgreSQL role provisioning failed: {detail or 'psql failed'}")


def main() -> int:
    try:
        run(ProvisioningConfig.from_env(os.environ))
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
