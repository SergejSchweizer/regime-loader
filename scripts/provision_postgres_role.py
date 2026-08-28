"""Provision the dedicated least-privilege PostgreSQL role for this repository."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass

POSTGRES_HOST = "10.10.1.3"
POSTGRES_PORT = 54321
POSTGRES_ROLE = "regime-loader"
POSTGRES_OWNER_ROLE = "regime-loader-owner"
POSTGRES_SCHEMAS = ("regime_loader", "regime_loader_sync")
POSTGRES_TABLES = (
    ("regime_loader", "regime_features_daily"),
    ("regime_loader_sync", "gold_sync_state"),
    ("regime_loader_sync", "gold_row_hashes"),
    ("regime_loader_sync", "schema_migrations"),
)


def _identifier(value: str) -> str:
    if not value:
        raise ValueError("PostgreSQL identifier cannot be empty")
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _table_identifier(schema: str, table: str) -> str:
    return f"{_identifier(schema)}.{_identifier(table)}"


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


def provision_sql(database: str, app_password: str, admin_user: str) -> str:
    """Return idempotent fail-closed DDL without exposing administrator credentials."""
    role_i = _identifier(POSTGRES_ROLE)
    role_l = _literal(POSTGRES_ROLE)
    owner_i = _identifier(POSTGRES_OWNER_ROLE)
    owner_l = _literal(POSTGRES_OWNER_ROLE)
    admin_i = _identifier(admin_user)
    database_i = _identifier(database)
    password_l = _literal(app_password)
    schemas = "\n".join(
        f"CREATE SCHEMA IF NOT EXISTS {_identifier(schema)} AUTHORIZATION {owner_i};\n"
        f"ALTER SCHEMA {_identifier(schema)} OWNER TO {owner_i};\n"
        f"REVOKE ALL ON SCHEMA {_identifier(schema)} FROM PUBLIC;\n"
        f"REVOKE CREATE ON SCHEMA {_identifier(schema)} FROM {role_i};\n"
        f"GRANT USAGE ON SCHEMA {_identifier(schema)} TO {role_i};"
        for schema in POSTGRES_SCHEMAS
    )
    table_ownership = "\n".join(
        f"ALTER TABLE IF EXISTS {_table_identifier(schema, table)} OWNER TO {owner_i};"
        for schema, table in POSTGRES_TABLES
    )
    table_grants = (
        "\n".join(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "
            f"{_table_identifier(schema, table)} TO {role_i};"
            for schema, table in POSTGRES_TABLES[:-1]
        )
        + f"\nGRANT SELECT ON TABLE {_table_identifier(*POSTGRES_TABLES[-1])} TO {role_i};"
    )
    default_privileges = "\n".join(
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {owner_i} IN SCHEMA {_identifier(schema)} "
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {role_i};\n"
        f"ALTER DEFAULT PRIVILEGES FOR ROLE {admin_i} IN SCHEMA {_identifier(schema)} "
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {role_i};"
        for schema in POSTGRES_SCHEMAS
    )
    return f"""DO $provision$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {owner_l}) THEN
        CREATE ROLE {owner_i} NOLOGIN;
    END IF;
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
            RAISE EXCEPTION 'existing regime-loader role has incompatible privileges';
        END IF;
        ALTER ROLE {role_i} PASSWORD {password_l};
    END IF;
END
$provision$;

GRANT CONNECT ON DATABASE {database_i} TO {role_i};
GRANT {owner_i} TO {admin_i};
{schemas}
{table_ownership}
REVOKE ALL ON ALL TABLES IN SCHEMA "regime_loader" FROM {role_i};
REVOKE ALL ON ALL TABLES IN SCHEMA "regime_loader_sync" FROM {role_i};
{table_grants}
{default_privileges}
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
        input=provision_sql(config.database, config.app_password, config.admin_user),
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
