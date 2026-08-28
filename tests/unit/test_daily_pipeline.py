from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from application.bronze_orchestration import BatchRunResult, SeriesRunResult
from application.contracts import SeriesContract
from application.daily_pipeline import DailyMedallionPipeline, ProviderBatchError
from application.gold_catalog import GoldBuildStatus, GoldCatalogRecord
from application.gold_frame import GOLD_COLUMNS, GOLD_SOURCE_SERIES
from application.gold_retention import GoldRetentionResult
from application.parallelism import PolarsExecutionPolicy
from application.planner import OperationMode
from application.registry import SERIES_REGISTRY
from application.silver import SILVER_SCHEMA

TODAY = date(2026, 8, 19)
START = date(2026, 6, 1)


def _silver(series_id: str, length: int = 80) -> pl.DataFrame:
    values = [float(index + 1) for index in range(length)]
    return pl.DataFrame(
        {
            "observation_date": [START + timedelta(days=index) for index in range(length)],
            "series_id": [series_id for _ in values],
            "value": values,
            "open": [None for _ in values],
            "high": [None for _ in values],
            "low": [None for _ in values],
            "close": [None for _ in values],
            "unit": [SERIES_REGISTRY[series_id].unit for _ in values],
            "provider": [SERIES_REGISTRY[series_id].provider.value for _ in values],
            "source_id": [SERIES_REGISTRY[series_id].source_id for _ in values],
            "fetched_at_utc": [datetime(2026, 8, 19, 2, tzinfo=UTC) for _ in values],
        },
        schema=SILVER_SCHEMA,
    )


class FakeBronze:
    def __init__(self, *, failures: tuple[str, ...] = ()) -> None:
        self.failures = failures
        self.calls: list[tuple[tuple[str, ...], OperationMode, date]] = []

    def run_many(
        self,
        series_ids: tuple[str, ...],
        *,
        operation: OperationMode,
        today: date,
    ) -> BatchRunResult:
        ids = tuple(series_ids)
        self.calls.append((ids, operation, today))
        successes = tuple(
            SeriesRunResult(
                series_id=series_id,
                provider=SERIES_REGISTRY[series_id].provider,
                run_id=f"bronze-{series_id}",
                mode=OperationMode.BOOTSTRAP if series_id == "vix" else operation,
                request_start=None if series_id == "vix" else date(2026, 8, 11),
                request_end=today,
                maximum_history=series_id == "vix",
                inserted_rows=1,
                revised_rows=0,
                written_partitions=1,
            )
            for series_id in ids
            if series_id not in self.failures
        )
        return BatchRunResult(successes, self.failures)


class FakeSilver:
    def __init__(self, *, fail_series: str | None = None) -> None:
        self.frames = {series_id: _silver(series_id) for series_id in GOLD_SOURCE_SERIES}
        self.build_calls: list[str] = []
        self.read_calls: list[str] = []
        self.fail_series = fail_series

    def build(self, contract: SeriesContract) -> object:
        self.build_calls.append(contract.series_id)
        if contract.series_id == self.fail_series:
            raise OSError("injected silver failure")
        return object()

    def read(self, contract: SeriesContract) -> pl.DataFrame:
        self.read_calls.append(contract.series_id)
        return self.frames[contract.series_id]


class FakePublisher:
    def __init__(self, *, fail_after_commit: bool = False) -> None:
        self.reconcile_count = 0
        self.publish_count = 0
        self.frames: list[pl.DataFrame] = []
        self.fail_after_commit = fail_after_commit

    def reconcile(self) -> list[GoldCatalogRecord]:
        self.reconcile_count += 1
        return []

    def publish(self, frame: pl.DataFrame, *, inputs: object = ()) -> GoldCatalogRecord:
        del inputs
        self.publish_count += 1
        self.frames.append(frame)
        record = GoldCatalogRecord(
            dataset_id="regime_features_daily",
            build_id=f"20260819T0200{self.publish_count:02d}Z",
            status=GoldBuildStatus.COMPLETE,
            current=True,
            started_at_utc=datetime(2026, 8, 19, 2, tzinfo=UTC),
            completed_at_utc=datetime(2026, 8, 19, 2, 1, tzinfo=UTC),
            schema_version=1,
            feature_version=1,
            min_timestamp=datetime(2026, 6, 1, tzinfo=UTC),
            max_timestamp=datetime(2026, 8, 19, tzinfo=UTC),
            row_count=frame.height,
            data_path="versions/data.parquet",
            build_manifest_path="versions/manifest.json",
            plot_path="versions/feature_profile.png",
            pruned_at_utc=None,
        )
        if self.fail_after_commit:
            raise OSError("injected post-promotion view error")
        return record


class FakeRetention:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls = 0
        self.fail = fail

    def run(self) -> GoldRetentionResult:
        self.calls += 1
        if self.fail:
            raise OSError("injected retention error")
        return GoldRetentionResult((), ())


class FakeInventory:
    def __init__(self) -> None:
        self.calls = 0

    def refresh(self) -> object:
        self.calls += 1
        return object()


def _pipeline(
    *,
    bronze: FakeBronze | None = None,
    silver: FakeSilver | None = None,
    publisher: FakePublisher | None = None,
    retention: FakeRetention | None = None,
    inventory: FakeInventory | None = None,
    events: list[dict[str, object]] | None = None,
) -> tuple[
    DailyMedallionPipeline,
    FakeBronze,
    FakeSilver,
    FakePublisher,
    FakeRetention,
    FakeInventory,
]:
    actual_bronze = bronze or FakeBronze()
    actual_silver = silver or FakeSilver()
    actual_publisher = publisher or FakePublisher()
    actual_retention = retention or FakeRetention()
    actual_inventory = inventory or FakeInventory()
    pipeline = DailyMedallionPipeline(
        series_registry=SERIES_REGISTRY,
        bronze=actual_bronze,  # type: ignore[arg-type]
        silver=actual_silver,
        publisher=actual_publisher,  # type: ignore[arg-type]
        retention=actual_retention,  # type: ignore[arg-type]
        inventory=actual_inventory,
        run_id_factory=lambda command: f"run-{command}",
        event_sink=None if events is None else events.append,
        polars_execution=PolarsExecutionPolicy(1),
    )
    return (
        pipeline,
        actual_bronze,
        actual_silver,
        actual_publisher,
        actual_retention,
        actual_inventory,
    )


def test_run_daily_can_only_reach_update_and_series_restricts_bronze_silver_not_gold() -> None:
    events: list[dict[str, object]] = []
    pipeline, bronze, silver, publisher, retention, inventory = _pipeline(events=events)
    result = pipeline.run_daily(["vix", "us_10y"], today=TODAY)
    assert bronze.calls == [(("vix", "us_10y"), OperationMode.UPDATE, TODAY)]
    assert silver.build_calls == ["vix", "us_10y"]
    assert set(silver.read_calls) == set(GOLD_SOURCE_SERIES)
    assert publisher.reconcile_count >= 1
    assert publisher.publish_count == 1
    assert publisher.frames[0].columns == list(GOLD_COLUMNS)
    assert retention.calls == 1
    assert inventory.calls == 1
    assert result.gold_build_id == "20260819T020001Z"
    bronze_events = [event for event in events if event["stage"] == "bronze-series"]
    vix = next(event for event in bronze_events if event["series"] == "vix")
    us10 = next(event for event in bronze_events if event["series"] == "us_10y")
    assert vix["provider"] == "cboe" and vix["maximum_history"] is True
    assert us10["provider"] == "fred"
    assert us10["request_start"] == "2026-08-11"
    assert us10["request_end"] == "2026-08-19"
    assert us10["maximum_history"] is False


def test_explicit_source_commands_are_distinct_and_reconcile_is_never_hidden() -> None:
    pipeline, bronze, *_ = _pipeline()
    pipeline.bootstrap(["us_10y"], today=TODAY)
    pipeline.update(["us_10y"], today=TODAY)
    pipeline.reconcile(["us_10y"], today=TODAY)
    assert [call[1] for call in bronze.calls] == [
        OperationMode.BOOTSTRAP,
        OperationMode.UPDATE,
        OperationMode.RECONCILE,
    ]


def test_provider_failure_preserves_isolated_bronze_and_stops_before_silver_gold() -> None:
    bronze = FakeBronze(failures=("us_10y",))
    pipeline, _, silver, publisher, retention, inventory = _pipeline(bronze=bronze)
    with pytest.raises(ProviderBatchError, match="us_10y"):
        pipeline.run_daily(["us_2y", "us_10y"], today=TODAY)
    assert [
        item.series_id
        for item in bronze.run_many(
            ["us_2y"], operation=OperationMode.UPDATE, today=TODAY
        ).successes
    ] == ["us_2y"]
    assert silver.build_calls == []
    assert publisher.publish_count == 0
    assert retention.calls == 0
    assert inventory.calls == 0


def test_prepromotion_silver_failure_never_calls_gold_publish() -> None:
    silver = FakeSilver(fail_series="us_10y")
    pipeline, _, _, publisher, retention, inventory = _pipeline(silver=silver)
    with pytest.raises(OSError, match="silver failure"):
        pipeline.run_daily(["us_10y"], today=TODAY)
    assert publisher.publish_count == 0
    assert retention.calls == 0
    assert inventory.calls == 0


def test_postpromotion_retention_error_is_nonrollback_pipeline_error() -> None:
    retention = FakeRetention(fail=True)
    pipeline, _, _, publisher, _, inventory = _pipeline(retention=retention)
    with pytest.raises(OSError, match="retention error"):
        pipeline.run_daily(["us_10y"], today=TODAY)
    assert publisher.publish_count == 1
    assert inventory.calls == 0


def test_gold_build_recovers_and_always_reads_full_silver_schema() -> None:
    pipeline, _, silver, publisher, retention, inventory = _pipeline()
    result = pipeline.gold_build()
    assert publisher.reconcile_count >= 1
    assert set(silver.read_calls) == set(GOLD_SOURCE_SERIES)
    assert publisher.frames[0].columns == list(GOLD_COLUMNS)
    assert result.gold_build_id is not None
    assert retention.calls == 0
    assert inventory.calls == 0


def test_inventory_and_silver_build_are_local_commands() -> None:
    pipeline, bronze, silver, publisher, retention, inventory = _pipeline()
    pipeline.silver_build(["vix"])
    pipeline.inventory()
    assert silver.build_calls == ["vix"]
    assert inventory.calls == 1
    assert bronze.calls == []
    assert publisher.publish_count == 0
    assert retention.calls == 0


def test_series_validation_is_fail_closed() -> None:
    pipeline, *_ = _pipeline()
    with pytest.raises(ValueError, match="unknown series"):
        pipeline.resolve_series(["missing"])
    with pytest.raises(ValueError, match="duplicate"):
        pipeline.resolve_series(["vix", "vix"])
    assert pipeline.resolve_series([]) == tuple(SERIES_REGISTRY)
