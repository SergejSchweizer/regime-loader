import fcntl
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

CRON_TEMPLATE = Path("ops/regime-loader.cron")
CRON_RUNNER = Path("ops/run-regime-loader-sunday.sh")
QUALITY_GATES_WORKFLOW = Path(".github/workflows/quality-gates.yml")


def _job_line() -> str:
    return next(
        line
        for line in CRON_TEMPLATE.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#") and not line.startswith("CRON_TZ=")
    )


def test_sunday_gold_sync_cron_template_has_explicit_vienna_timezone_and_dst() -> None:
    text = CRON_TEMPLATE.read_text(encoding="utf-8")
    vienna = ZoneInfo("Europe/Vienna")

    assert "CRON_TZ=Europe/Vienna" in text
    assert datetime(2026, 1, 4, 10, tzinfo=vienna).utcoffset() == timedelta(hours=1)
    assert datetime(2026, 7, 5, 10, tzinfo=vienna).utcoffset() == timedelta(hours=2)


def test_sunday_gold_sync_cron_template_is_operational() -> None:
    job = _job_line()
    runner = CRON_RUNNER.read_text(encoding="utf-8")

    assert job.startswith("0 10 * * 0 ")
    assert job == "0 10 * * 0 /home/dev_market/regime-loader/ops/run-regime-loader-sunday.sh"
    assert '"$PROJECT_ROOT/scripts/export_cron_config.py" "$CONFIG_FILE"' in runner
    assert 'cd "$PROJECT_ROOT"' in runner
    assert 'git -C "$PROJECT_ROOT" rev-parse --verify HEAD' in runner
    assert "export REGIME_LOADER_GIT_SHA" in runner
    assert 'LOCK_PATH="$LOCK_DIR/regime-loader-sunday.lock"' in runner
    assert "if ! flock -n 9; then" in runner
    assert "exit 3" in runner
    assert 'mkdir -p "$LOG_DIR"' in runner
    assert 'exec >>"$LOG_PATH" 2>&1' in runner
    assert '"$CLI" --lake-root "$LAKE_ROOT" run-daily' in runner
    assert '"$CLI" --lake-root "$LAKE_ROOT" gold-sync-postgres' in runner
    assert runner.index("run-daily") < runner.index("gold-sync-postgres")
    assert "/var/log" not in job
    assert "reconcile" not in job + runner


def test_cron_template_has_exactly_one_job_and_no_database_secret_literal() -> None:
    text = CRON_TEMPLATE.read_text(encoding="utf-8")
    jobs = [
        line
        for line in text.splitlines()
        if line and not line.startswith("#") and not line.startswith("CRON_TZ=")
    ]

    assert jobs == [_job_line()]
    assert "PGPASSWORD=" not in text
    assert "postgresql://" not in text
    assert "repo-secret" not in text


def test_ingestion_is_not_scheduled_in_github_actions() -> None:
    assert "schedule:" not in QUALITY_GATES_WORKFLOW.read_text(encoding="utf-8")


def _write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def _runner_fixture(tmp_path: Path, git_exit_code: int = 0) -> tuple[Path, Path]:
    project_root = tmp_path / "project"
    (project_root / "ops").mkdir(parents=True)
    (project_root / "scripts").mkdir()
    (project_root / ".venv" / "bin").mkdir(parents=True)
    runner_path = project_root / "ops" / "run-regime-loader-sunday.sh"
    runner_path.write_text(CRON_RUNNER.read_text(encoding="utf-8"), encoding="utf-8")
    runner_path.chmod(0o755)
    (project_root / "config.yaml").write_text("fixture\n", encoding="utf-8")
    _write_executable(
        project_root / ".venv" / "bin" / "python",
        "#!/usr/bin/env bash\nprintf 'export LAKE_ROOT=%q\\n' fixture-lake\n",
    )
    _write_executable(
        project_root / ".venv" / "bin" / "regime-loader",
        "#!/usr/bin/env bash\n"
        'printf \'%s|%s|%s\\n\' "$PWD" "$REGIME_LOADER_GIT_SHA" "$*" '
        '>> "$RUNNER_RECORD"\n'
        'if [[ "${RUNNER_FAIL_DAILY:-}" == "true" && "$*" == *"run-daily" ]]; then exit 7; fi\n',
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _write_executable(
        bin_dir / "git",
        f"#!/usr/bin/env bash\nif [[ {git_exit_code} -ne 0 ]]; "
        f"then exit {git_exit_code}; fi\nprintf 'fixture-sha\\n'\n",
    )
    return project_root, bin_dir


def test_sunday_runner_uses_repository_root_and_exports_one_git_identity(tmp_path: Path) -> None:
    project_root, bin_dir = _runner_fixture(tmp_path)
    record_path = tmp_path / "runner-record"
    unrelated_cwd = tmp_path / "unrelated"
    unrelated_cwd.mkdir()
    environment = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "RUNNER_RECORD": str(record_path),
    }

    completed = subprocess.run(
        [str(project_root / "ops" / "run-regime-loader-sunday.sh")],
        cwd=unrelated_cwd,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert record_path.read_text(encoding="utf-8").splitlines() == [
        f"{project_root}|fixture-sha|--lake-root fixture-lake run-daily",
        f"{project_root}|fixture-sha|--lake-root fixture-lake gold-sync-postgres",
    ]


def test_sunday_runner_stops_before_commands_when_git_identity_is_unavailable(
    tmp_path: Path,
) -> None:
    project_root, bin_dir = _runner_fixture(tmp_path, git_exit_code=1)
    record_path = tmp_path / "runner-record"
    environment = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "RUNNER_RECORD": str(record_path),
    }

    completed = subprocess.run(
        [str(project_root / "ops" / "run-regime-loader-sunday.sh")],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    assert "Unable to resolve repository Git identity" in completed.stderr
    assert not record_path.exists()


def test_sunday_runner_rejects_lock_contention_before_any_cli_command(tmp_path: Path) -> None:
    project_root, bin_dir = _runner_fixture(tmp_path)
    lock_dir = project_root / ".locks"
    lock_dir.mkdir()
    lock_path = lock_dir / "regime-loader-sunday.lock"
    record_path = tmp_path / "runner-record"
    environment = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "RUNNER_RECORD": str(record_path),
    }

    with lock_path.open("w", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        completed = subprocess.run(
            [str(project_root / "ops" / "run-regime-loader-sunday.sh")],
            cwd=tmp_path,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )

    assert completed.returncode == 3
    assert "already running" in completed.stderr
    assert not record_path.exists()


def test_sunday_runner_releases_lock_after_daily_failure_and_skips_postgres_sync(
    tmp_path: Path,
) -> None:
    project_root, bin_dir = _runner_fixture(tmp_path)
    record_path = tmp_path / "runner-record"
    environment = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "RUNNER_RECORD": str(record_path),
        "RUNNER_FAIL_DAILY": "true",
    }

    failed = subprocess.run(
        [str(project_root / "ops" / "run-regime-loader-sunday.sh")],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    retry = subprocess.run(
        [str(project_root / "ops" / "run-regime-loader-sunday.sh")],
        cwd=tmp_path,
        env={key: value for key, value in environment.items() if key != "RUNNER_FAIL_DAILY"},
        check=False,
        capture_output=True,
        text=True,
    )

    assert failed.returncode == 7
    assert retry.returncode == 0
    assert record_path.read_text(encoding="utf-8").splitlines() == [
        f"{project_root}|fixture-sha|--lake-root fixture-lake run-daily",
        f"{project_root}|fixture-sha|--lake-root fixture-lake run-daily",
        f"{project_root}|fixture-sha|--lake-root fixture-lake gold-sync-postgres",
    ]
