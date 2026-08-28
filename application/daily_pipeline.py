"""Operational command Facade for the daily medallion pipeline."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Protocol

import polars as pl

from application.bronze_orchestration import BatchRunResult, BronzeOrchestrator
from application.contracts import SeriesContract
from application.gold_frame import GoldFrameBuild, assemble_gold_frame
from application.gold_publication import GoldPublisher
from application.gold_retention import GoldRetentionResult, GoldRetentionService
from application.macro_features import MACRO_SERIES, build_macro_features
from application.parallelism import PolarsExecutionPolicy
from application.planner import OperationMode
from application.volatility_features import VOLATILITY_SERIES, build_volatility_features

RunIdFactory = Callable[[str], str]
EventSink = Callable[[dict[str, object]], None]


def _default_run_id(command: str) -> str:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{command}-{stamp}"


def _no_event(event: dict[str, object]) -> None:
    del event


class SilverSeriesPort(Protocol):
    def build(self, contract: SeriesContract) -> object: ...

    def read(self, contract: SeriesContract) -> pl.DataFrame: ...


class InventoryRefreshPort(Protocol):
    def refresh(self) -> object: ...


@dataclass(frozen=True, slots=True)
class PipelineCommandResult:
    command: str
    run_id: str
    selected_series: tuple[str, ...]
    bronze: BatchRunResult | None = None
    gold_build_id: str | None = None
    retention: GoldRetentionResult | None = None


class ProviderBatchError(RuntimeError):
    """Daily/source command error after isolated successful Bronze commits are preserved."""

    def __init__(self, failures: Sequence[str]) -> None:
        self.failures = tuple(failures)
        super().__init__(f"provider failures: {', '.join(self.failures)}")


class DailyMedallionPipeline:
    """Facade exposing explicit source commands and the strict delta-only daily workflow."""

    def __init__(
        self,
        *,
        series_registry: Mapping[str, SeriesContract],
        bronze: BronzeOrchestrator,
        silver: SilverSeriesPort,
        publisher: GoldPublisher,
        retention: GoldRetentionService,
        inventory: InventoryRefreshPort,
        run_id_factory: RunIdFactory | None = None,
        event_sink: EventSink | None = None,
        polars_execution: PolarsExecutionPolicy | None = None,
    ) -> None:
        self._series_registry = series_registry
        self._bronze = bronze
        self._silver = silver
        self._publisher = publisher
        self._retention = retention
        self._inventory = inventory
        self._run_id_factory = run_id_factory if run_id_factory is not None else _default_run_id
        self._event_sink = event_sink if event_sink is not None else _no_event
        self._polars_execution = (
            polars_execution
            if polars_execution is not None
            else PolarsExecutionPolicy.all_available_cores()
        )

    def bootstrap(self, series_ids: Sequence[str], *, today: date) -> PipelineCommandResult:
        return self._source_command("bootstrap", series_ids, OperationMode.BOOTSTRAP, today=today)

    def update(self, series_ids: Sequence[str], *, today: date) -> PipelineCommandResult:
        return self._source_command("update", series_ids, OperationMode.UPDATE, today=today)

    def reconcile(self, series_ids: Sequence[str], *, today: date) -> PipelineCommandResult:
        """Explicit operator-only maximum-history reconciliation command."""
        return self._source_command("reconcile", series_ids, OperationMode.RECONCILE, today=today)

    def silver_build(self, series_ids: Sequence[str]) -> PipelineCommandResult:
        selected = self.resolve_series(series_ids)
        run_id = self._start("silver-build", selected)
        try:
            self._build_selected_silver(selected, run_id=run_id)
            self._finish(run_id, "silver-build", "success")
            return PipelineCommandResult("silver-build", run_id, selected)
        except Exception:
            self._finish(run_id, "silver-build", "failed")
            raise

    def gold_build(self) -> PipelineCommandResult:
        run_id = self._start("gold-build", ())
        try:
            self._event(run_id, "gold-build", stage="recovery", status="started")
            self._publisher.reconcile()
            self._event(run_id, "gold-build", stage="recovery", status="success")
            gold = self._canonical_gold(run_id=run_id, command="gold-build")
            published = self._publisher.publish(gold.frame, inputs=gold.inputs)
            self._event(
                run_id,
                "gold-build",
                stage="gold-publish",
                status="success",
                build_id=published.build_id,
            )
            self._finish(run_id, "gold-build", "success")
            return PipelineCommandResult("gold-build", run_id, (), gold_build_id=published.build_id)
        except Exception:
            self._finish(run_id, "gold-build", "failed")
            raise

    def inventory(self) -> PipelineCommandResult:
        run_id = self._start("inventory", ())
        try:
            self._inventory.refresh()
            self._finish(run_id, "inventory", "success")
            return PipelineCommandResult("inventory", run_id, ())
        except Exception:
            self._finish(run_id, "inventory", "failed")
            raise

    def run_daily(self, series_ids: Sequence[str], *, today: date) -> PipelineCommandResult:
        """Strict daily delta pipeline; source reconciliation is intentionally unreachable here."""
        selected = self.resolve_series(series_ids)
        run_id = self._start("run-daily", selected)
        try:
            self._event(run_id, "run-daily", stage="recovery", status="started")
            self._publisher.reconcile()
            self._event(run_id, "run-daily", stage="recovery", status="success")

            self._event(run_id, "run-daily", stage="bronze", status="started")
            bronze = self._bronze.run_many(
                selected,
                operation=OperationMode.UPDATE,
                today=today,
            )
            self._log_bronze_results(run_id, "run-daily", bronze)
            if bronze.failures:
                raise ProviderBatchError(bronze.failures)

            self._build_selected_silver(selected, run_id=run_id, command="run-daily")
            gold = self._canonical_gold(run_id=run_id, command="run-daily")
            published = self._publisher.publish(gold.frame, inputs=gold.inputs)
            self._event(
                run_id,
                "run-daily",
                stage="gold-publish",
                status="success",
                build_id=published.build_id,
            )
            retention = self._retention.run()
            self._event(
                run_id,
                "run-daily",
                stage="retention",
                status="success",
                marked=list(retention.marked_build_ids),
                swept=list(retention.swept_build_ids),
            )
            self._inventory.refresh()
            self._event(run_id, "run-daily", stage="inventory", status="success")
            self._finish(run_id, "run-daily", "success")
            return PipelineCommandResult(
                "run-daily",
                run_id,
                selected,
                bronze=bronze,
                gold_build_id=published.build_id,
                retention=retention,
            )
        except Exception:
            self._finish(run_id, "run-daily", "failed")
            raise

    def resolve_series(self, series_ids: Sequence[str]) -> tuple[str, ...]:
        selected = tuple(series_ids) if series_ids else tuple(self._series_registry)
        duplicates = [series_id for series_id in selected if selected.count(series_id) > 1]
        if duplicates:
            raise ValueError(f"duplicate --series value: {sorted(set(duplicates))}")
        unknown = [series_id for series_id in selected if series_id not in self._series_registry]
        if unknown:
            raise ValueError(f"unknown series: {', '.join(sorted(unknown))}")
        return selected

    def _source_command(
        self,
        command: str,
        series_ids: Sequence[str],
        operation: OperationMode,
        *,
        today: date,
    ) -> PipelineCommandResult:
        selected = self.resolve_series(series_ids)
        run_id = self._start(command, selected)
        try:
            batch = self._bronze.run_many(selected, operation=operation, today=today)
            self._log_bronze_results(run_id, command, batch)
            if batch.failures:
                raise ProviderBatchError(batch.failures)
            self._finish(run_id, command, "success")
            return PipelineCommandResult(command, run_id, selected, bronze=batch)
        except Exception:
            self._finish(run_id, command, "failed")
            raise

    def _log_bronze_results(
        self,
        run_id: str,
        command: str,
        batch: BatchRunResult,
    ) -> None:
        for item in batch.successes:
            self._event(
                run_id,
                command,
                stage="bronze-series",
                status="success",
                series=item.series_id,
                provider=item.provider.value,
                mode=item.mode.value,
                request_start=None
                if item.request_start is None
                else item.request_start.isoformat(),
                request_end=item.request_end.isoformat(),
                maximum_history=item.maximum_history,
                inserted_rows=item.inserted_rows,
                revised_rows=item.revised_rows,
                written_partitions=item.written_partitions,
            )
        for series_id in batch.failures:
            self._event(
                run_id,
                command,
                stage="bronze-series",
                status="failed",
                series=series_id,
                provider=self._series_registry[series_id].provider.value,
            )
        self._event(
            run_id,
            command,
            stage="bronze",
            status="success" if not batch.failures else "failed",
            failures=list(batch.failures),
        )

    def _build_selected_silver(
        self,
        selected: Sequence[str],
        *,
        run_id: str,
        command: str = "silver-build",
    ) -> None:
        self._event(run_id, command, stage="silver", status="started")
        self._polars_execution.map(
            lambda series_id: self._silver.build(self._series_registry[series_id]), selected
        )
        self._event(run_id, command, stage="silver", status="success")

    def _canonical_gold(self, *, run_id: str, command: str) -> GoldFrameBuild:
        self._event(run_id, command, stage="gold-frame", status="started")
        series_items = tuple(self._series_registry.items())
        silver_by_series = dict(
            self._polars_execution.map(
                lambda item: (item[0], self._silver.read(item[1])), series_items
            )
        )
        missing = [series_id for series_id, frame in silver_by_series.items() if frame.is_empty()]
        if missing:
            raise ValueError(
                "full Gold requires non-empty Silver for every canonical series; missing: "
                + ", ".join(missing)
            )
        feature_builders: tuple[Callable[[], pl.DataFrame], Callable[[], pl.DataFrame]] = (
            lambda: build_volatility_features(
                {series_id: silver_by_series[series_id] for series_id in VOLATILITY_SERIES}
            ),
            lambda: build_macro_features(
                {series_id: silver_by_series[series_id] for series_id in MACRO_SERIES}
            ),
        )
        volatility, macro = self._polars_execution.map(lambda build: build(), feature_builders)
        result = assemble_gold_frame(volatility, macro, silver_by_series)
        self._event(
            run_id,
            command,
            stage="gold-frame",
            status="success",
            row_count=result.frame.height,
        )
        return result

    def _start(self, command: str, selected: Sequence[str]) -> str:
        run_id = self._run_id_factory(command)
        self._event(
            run_id,
            command,
            stage="command",
            status="started",
            selected_series=list(selected),
        )
        return run_id

    def _finish(self, run_id: str, command: str, status: str) -> None:
        self._event(run_id, command, stage="command", status=status)

    def _event(
        self,
        run_id: str,
        command: str,
        *,
        stage: str,
        status: str,
        **context: object,
    ) -> None:
        self._event_sink(
            {
                "run_id": run_id,
                "command": command,
                "stage": stage,
                "status": status,
                **context,
            }
        )
