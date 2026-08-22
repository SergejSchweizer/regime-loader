from pathlib import Path

CRON_TEMPLATE = Path("ops/regime-data-loader.cron")
CRON_RUNNER = Path("ops/run-regime-data-loader-sunday.sh")
QUALITY_GATES_WORKFLOW = Path(".github/workflows/quality-gates.yml")


def _job_line() -> str:
    return next(
        line
        for line in CRON_TEMPLATE.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    )


def test_sunday_gold_sync_cron_template_is_operational() -> None:
    job = _job_line()
    runner = CRON_RUNNER.read_text(encoding="utf-8")

    assert job.startswith("0 10 * * 0 ")
    assert job == "0 10 * * 0 /srv/regime-data-loader/ops/run-regime-data-loader-sunday.sh"
    assert '"$PROJECT_ROOT/scripts/export_cron_config.py" "$CONFIG_FILE"' in runner
    assert 'mkdir -p "$LOG_DIR"' in runner
    assert 'exec >>"$LOG_PATH" 2>&1' in runner
    assert '"$CLI" --lake-root "$LAKE_ROOT" run-daily' in runner
    assert '"$CLI" --lake-root "$LAKE_ROOT" gold-sync-postgres' in runner
    assert runner.index("run-daily") < runner.index("gold-sync-postgres")
    assert "/var/log" not in job
    assert "reconcile" not in job + runner


def test_cron_template_has_exactly_one_job_and_no_database_secret_literal() -> None:
    text = CRON_TEMPLATE.read_text(encoding="utf-8")
    jobs = [line for line in text.splitlines() if line and not line.startswith("#")]

    assert jobs == [_job_line()]
    assert "PGPASSWORD=" not in text
    assert "postgresql://" not in text
    assert "repo-secret" not in text


def test_ingestion_is_not_scheduled_in_github_actions() -> None:
    assert "schedule:" not in QUALITY_GATES_WORKFLOW.read_text(encoding="utf-8")
