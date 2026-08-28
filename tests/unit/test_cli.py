from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest

from api import cli
from application.daily_pipeline import ProviderBatchError


class DummyTransport:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class DummyPipeline:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple[str, tuple[str, ...], date | None]] = []

    def _call(self, command: str, series: tuple[str, ...] = (), today: date | None = None) -> None:
        self.calls.append((command, series, today))
        if self.error is not None:
            raise self.error

    def bootstrap(self, series: tuple[str, ...], *, today: date) -> None:
        self._call("bootstrap", series, today)

    def update(self, series: tuple[str, ...], *, today: date) -> None:
        self._call("update", series, today)

    def reconcile(self, series: tuple[str, ...], *, today: date) -> None:
        self._call("reconcile", series, today)

    def silver_build(self, series: tuple[str, ...]) -> None:
        self._call("silver-build", series)

    def gold_build(self) -> None:
        self._call("gold-build")

    def run_daily(self, series: tuple[str, ...], *, today: date) -> None:
        self._call("run-daily", series, today)

    def inventory(self) -> None:
        self._call("inventory")


@dataclass
class DummyRuntime:
    pipeline: DummyPipeline
    transport: DummyTransport
    paths: object = None

    def close(self) -> None:
        self.transport.close()


def _patched_runtime(monkeypatch: pytest.MonkeyPatch, pipeline: DummyPipeline) -> DummyRuntime:
    runtime = DummyRuntime(pipeline, DummyTransport())
    monkeypatch.setattr(cli, "build_runtime", lambda **kwargs: runtime)
    return runtime


def test_parser_exposes_exact_operational_command_surface() -> None:
    parser = cli.build_parser()
    commands = {action.dest: action for action in parser._actions if action.dest == "command"}
    choices = set(commands["command"].choices)
    assert choices == {
        "bootstrap",
        "update",
        "reconcile",
        "silver-build",
        "gold-build",
        "gold-sync-postgres",
        "postgres-migrate",
        "postgres-verify",
        "inventory",
        "run-daily",
    }


def test_main_dispatches_run_daily_with_injected_today_and_series(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = DummyPipeline()
    runtime = _patched_runtime(monkeypatch, pipeline)
    stdout = io.StringIO()
    stderr = io.StringIO()
    code = cli.main(
        [
            "--lake-root",
            "/tmp/lake",
            "--today",
            "2026-08-19",
            "run-daily",
            "--series",
            "vix",
            "--series",
            "us_10y",
        ],
        stdout=stdout,
        stderr=stderr,
    )
    assert code == cli.EXIT_SUCCESS
    assert pipeline.calls == [("run-daily", ("vix", "us_10y"), date(2026, 8, 19))]
    assert runtime.transport.closed


def test_stable_exit_codes_separate_provider_input_and_pipeline_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cases = [
        (ProviderBatchError(["us_10y"]), cli.EXIT_PROVIDER),
        (ValueError("invalid setting"), cli.EXIT_INPUT),
        (OSError("disk failed"), cli.EXIT_PIPELINE),
    ]
    for error, expected in cases:
        pipeline = DummyPipeline(error=error)
        _patched_runtime(monkeypatch, pipeline)
        stderr = io.StringIO()
        code = cli.main(
            ["--today", "2026-08-19", "run-daily", "--series", "us_10y"],
            stdout=io.StringIO(),
            stderr=stderr,
        )
        assert code == expected
        payload = json.loads(stderr.getvalue().splitlines()[-1])
        assert payload["exit_code"] == expected
        assert payload["status"] == "failed"


def test_json_event_sink_recursively_redacts_secrets() -> None:
    output = io.StringIO()
    logger = logging.getLogger("test.cli.secret")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(output)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    sink = cli.JsonEventSink(logger, secrets=("TOPSECRET",))
    sink(
        {
            "run_id": "run-1",
            "command": "run-daily",
            "stage": "provider",
            "status": "failed",
            "message": "url?api_key=TOPSECRET",
            "nested": ["TOPSECRET", {"value": "xTOPSECRETx"}],
        }
    )
    text = output.getvalue()
    assert "TOPSECRET" not in text
    payload = json.loads(text)
    assert payload["message"] == "url?api_key=***"
    assert payload["nested"] == ["***", {"value": "x***x"}]


def test_required_provider_selection_is_series_scoped_and_validated() -> None:
    assert cli._required_provider_ids("inventory", ()) == set()
    assert cli._required_provider_ids("run-daily", ("vix",)) == {cli.Provider.CBOE}
    with pytest.raises(ValueError, match="unknown series"):
        cli._required_provider_ids("run-daily", ("missing",))


def test_overlap_days_and_git_config_errors_are_input_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "_git_commit_hash", lambda: "f" * 40)
    with pytest.raises(ValueError, match="non-negative"):
        cli.build_runtime(
            lake_root=Path("lake"),
            command="inventory",
            series_ids=(),
            overlap_days=-1,
            stderr=io.StringIO(),
        )
