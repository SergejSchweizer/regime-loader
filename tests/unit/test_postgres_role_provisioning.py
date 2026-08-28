from __future__ import annotations

from scripts.provision_postgres_role import (
    POSTGRES_HOST,
    POSTGRES_OWNER_ROLE,
    POSTGRES_PORT,
    POSTGRES_ROLE,
    POSTGRES_SCHEMAS,
    POSTGRES_TABLES,
    ProvisioningConfig,
    provision_sql,
    psql_command,
    sanitized,
)


def _env() -> dict[str, str]:
    return {
        "MARKET_REGIME_POSTGRES_HOST": "10.10.1.3",
        "MARKET_REGIME_POSTGRES_PORT": "54321",
        "MARKET_REGIME_POSTGRES_DATABASE": "quant_data",
        "MARKET_REGIME_POSTGRES_ADMIN_USER": "postgres",
        "MARKET_REGIME_POSTGRES_ADMIN_PASSWORD": "admin-secret",
        "MARKET_REGIME_POSTGRES_PASSWORD": "repo-secret",
    }


def test_exact_endpoint_and_repository_role() -> None:
    config = ProvisioningConfig.from_env(_env())
    assert (config.host, config.port) == (POSTGRES_HOST, POSTGRES_PORT)
    assert POSTGRES_ROLE == "regime-loader"
    command = psql_command(config)
    assert command[command.index("--host") + 1] == "10.10.1.3"
    assert command[command.index("--port") + 1] == "54321"


def test_sql_is_least_privilege_and_schema_scoped() -> None:
    sql = provision_sql("quant_data", "repo-secret", "postgres-admin")
    assert 'CREATE ROLE "regime-loader"' in sql
    assert f'CREATE ROLE "{POSTGRES_OWNER_ROLE}" NOLOGIN' in sql
    for token in (
        "LOGIN PASSWORD",
        "NOSUPERUSER",
        "NOCREATEDB",
        "NOCREATEROLE",
        "NOREPLICATION",
        "NOBYPASSRLS",
    ):
        assert token in sql
    assert POSTGRES_SCHEMAS == ("regime_loader", "regime_loader_sync")
    assert (
        f'CREATE SCHEMA IF NOT EXISTS "regime_loader" AUTHORIZATION "{POSTGRES_OWNER_ROLE}"' in sql
    )
    assert (
        f'CREATE SCHEMA IF NOT EXISTS "regime_loader_sync" AUTHORIZATION "{POSTGRES_OWNER_ROLE}"'
        in sql
    )
    assert 'REVOKE CREATE ON SCHEMA "regime_loader" FROM "regime-loader"' in sql
    assert 'GRANT USAGE ON SCHEMA "regime_loader" TO "regime-loader"' in sql
    assert "GRANT USAGE, CREATE" not in sql
    for schema, table in POSTGRES_TABLES[:-1]:
        assert (
            f'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "{schema}"."{table}" TO "regime-loader"'
            in sql
        )
    assert (
        'GRANT SELECT ON TABLE "regime_loader_sync"."schema_migrations" TO "regime-loader"' in sql
    )
    assert "GRANT ALL" not in sql
    assert "public" not in {schema.lower() for schema in POSTGRES_SCHEMAS}


def test_sql_is_idempotent_and_fails_on_incompatible_existing_state() -> None:
    sql = provision_sql("quant_data", "repo-secret", "postgres-admin")
    assert "IF NOT EXISTS (SELECT 1 FROM pg_roles" in sql
    assert "CREATE SCHEMA IF NOT EXISTS" in sql
    assert "existing regime-loader role has incompatible privileges" in sql
    assert f'ALTER SCHEMA "regime_loader" OWNER TO "{POSTGRES_OWNER_ROLE}"' in sql
    assert f'ALTER DEFAULT PRIVILEGES FOR ROLE "{POSTGRES_OWNER_ROLE}"' in sql
    assert "to_regclass('regime_loader_sync.schema_migrations') IS NOT NULL" in sql


def test_admin_and_application_passwords_must_be_distinct() -> None:
    env = _env()
    env["MARKET_REGIME_POSTGRES_PASSWORD"] = env["MARKET_REGIME_POSTGRES_ADMIN_PASSWORD"]
    try:
        ProvisioningConfig.from_env(env)
    except ValueError as exc:
        assert "must differ" in str(exc)
    else:
        raise AssertionError("shared admin/application password must fail")


def test_wrong_endpoint_and_missing_secrets_fail() -> None:
    env = _env()
    env["MARKET_REGIME_POSTGRES_HOST"] = "localhost"
    try:
        ProvisioningConfig.from_env(env)
    except ValueError as exc:
        assert "10.10.1.3:54321" in str(exc)
    else:
        raise AssertionError("wrong endpoint must fail")

    env = _env()
    env["MARKET_REGIME_POSTGRES_PASSWORD"] = ""
    try:
        ProvisioningConfig.from_env(env)
    except ValueError as exc:
        assert "MARKET_REGIME_POSTGRES_PASSWORD" in str(exc)
    else:
        raise AssertionError("missing application password must fail")


def test_sanitizer_removes_both_credentials() -> None:
    config = ProvisioningConfig.from_env(_env())
    text = "admin-secret failed while repo-secret was applied"
    result = sanitized(text, config)
    assert "admin-secret" not in result
    assert "repo-secret" not in result
    assert result.count("***") == 2
