from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from application.bronze_orchestration import BronzeOrchestrator
from application.contracts import NativeShape, Provider, SeriesContract
from application.daily_pipeline import DailyMedallionPipeline
from application.gold_catalog import LATEST_COMPATIBLE, STRICT_CURRENT, GoldCompatibility
from application.gold_frame import GOLD_COLUMNS, GOLD_FEATURE_VERSION, GOLD_SCHEMA_VERSION
from application.gold_publication import GoldPublisher
from application.gold_retention import GoldRetentionService
from application.gold_sidecars import GoldSidecarBuilder
from application.paths import LakePaths
from application.ports.market_data import ProviderRequest
from application.registry import SERIES_REGISTRY
from ingestion.bronze_uow import FilesystemBronzeUnitOfWork
from ingestion.gold_build_store import GoldBuildStore
from ingestion.gold_catalog_repository import GoldCatalogRepository
from ingestion.gold_materialized_views import GoldMaterializedViewWriter
from ingestion.gold_publication_adapters import GoldBundleAdapter
from ingestion.gold_retention_store import GoldBundleSweeper
from ingestion.gold_sidecar_store import GoldSidecarStore
from ingestion.inventory_refresh import InventoryRefreshService
from ingestion.operational_repository import read_inventory
from ingestion.silver_repository import SilverSeriesRepository

pytestmark = pytest.mark.integration
INITIAL_TODAY = date(2026, 8, 18)
NEXT_TODAY = date(2026, 8, 19)
NOW = datetime(2026, 8, 19, 2, tzinfo=UTC)
GIT_SHA = "f" * 40


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class SequenceClock:
    def __init__(self, values: list[datetime]) -> None:
        self._values = iter(values)

    def __call__(self) -> datetime:
        return next(self._values)


class FixtureProvider:
    """Offline provider fixture preserving each registered adapter capability/shape contract."""

    def __init__(self, provider: Provider, source: dict[str, list[tuple[date, float]]]) -> None:
        self.provider = provider
        self.source = source
        self.requests: list[tuple[str, ProviderRequest]] = []
        self.fetch_count = 0

    def fetch(self, series: SeriesContract, request: ProviderRequest) -> pl.DataFrame:
        assert series.provider is self.provider
        self.requests.append((series.series_id, request))
        rows = list(self.source[series.series_id])
        if request.operation == "update":
            assert request.logical_start is not None
            rows = [row for row in rows if request.logical_start <= row[0] <= request.logical_end]
        self.fetch_count += 1
        fetched_at = NOW + timedelta(seconds=self.fetch_count)
        common: dict[str, object] = {
            "series_id": [series.series_id for _ in rows],
            "provider": [series.provider.value for _ in rows],
            "observation_date": [day for day, _ in rows],
            "fetched_at_utc": [fetched_at for _ in rows],
            "source_id": [series.source_id for _ in rows],
            "source_url": [f"https://fixture.invalid/{series.series_id}" for _ in rows],
        }
        schema: dict[str, pl.DataType] = {
            "series_id": pl.String,
            "provider": pl.String,
            "observation_date": pl.Date,
            "fetched_at_utc": pl.Datetime("us", "UTC"),
            "source_id": pl.String,
            "source_url": pl.String,
        }
        if series.native_shape is NativeShape.OHLC:
            values = [value for _, value in rows]
            common.update(
                {
                    "open": [value - 0.2 for value in values],
                    "high": [value + 0.5 for value in values],
                    "low": [value - 0.5 for value in values],
                    "close": values,
                }
            )
            schema.update(
                {
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                }
            )
        else:
            common["value"] = [value for _, value in rows]
            schema["value"] = pl.Float64
        return pl.DataFrame(common, schema=schema)


def _source_data() -> dict[str, list[tuple[date, float]]]:
    start = INITIAL_TODAY - timedelta(days=59)
    result: dict[str, list[tuple[date, float]]] = {}
    for series_index, series_id in enumerate(SERIES_REGISTRY):
        rows = [
            (start + timedelta(days=index), float(10 + series_index + index)) for index in range(60)
        ]
        if series_id == "us_10y":
            rows.insert(0, (date(2000, 1, 3), 6.5))
        result[series_id] = rows
    return result


def _providers(
    source: dict[str, list[tuple[date, float]]],
) -> dict[Provider, FixtureProvider]:
    return {
        provider: FixtureProvider(provider, source)
        for provider in Provider
        if any(contract.provider is provider for contract in SERIES_REGISTRY.values())
    }


def _stack(
    tmp_path: Path,
    source: dict[str, list[tuple[date, float]]],
    providers: dict[Provider, FixtureProvider],
) -> tuple[DailyMedallionPipeline, LakePaths, GoldCatalogRepository]:
    paths = LakePaths(tmp_path / "lake")
    uow = FilesystemBronzeUnitOfWork(paths)
    bronze = BronzeOrchestrator(
        series_registry=SERIES_REGISTRY,
        providers=providers,
        unit_of_work=uow,
        clock=lambda: NOW,
        run_id_factory=lambda series_id: (
            f"e2e-{series_id}-{len(providers[SERIES_REGISTRY[series_id].provider].requests)}"
        ),
    )
    silver = SilverSeriesRepository(paths)
    build_times = [NOW + timedelta(seconds=index) for index in range(8)]
    build_store = GoldBuildStore(paths, clock=SequenceClock(build_times))
    sidecars = GoldSidecarStore(
        paths,
        build_store,
        GoldSidecarBuilder(git_commit_hash=GIT_SHA),
        profile_renderer=lambda frame: b"\x89PNG\r\n\x1a\nfast-test-renderer",
    )
    bundle = GoldBundleAdapter(
        paths,
        build_store,
        sidecars,
        clock=lambda: NOW + timedelta(minutes=1),
    )
    catalog = GoldCatalogRepository(paths.gold_manifest_parquet())
    views = GoldMaterializedViewWriter(paths)
    publisher = GoldPublisher(catalog, bundle, views, clock=lambda: NOW)
    retention = GoldRetentionService(
        catalog,
        GoldBundleSweeper(paths),
        views,
        clock=lambda: NOW + timedelta(hours=1),
    )
    inventory = InventoryRefreshService(paths)
    pipeline = DailyMedallionPipeline(
        series_registry=SERIES_REGISTRY,
        bronze=bronze,
        silver=silver,
        publisher=publisher,
        retention=retention,
        inventory=inventory,
        run_id_factory=lambda command: f"e2e-{command}",
    )
    return pipeline, paths, catalog


def _replace_source_value(
    source: dict[str, list[tuple[date, float]]],
    series_id: str,
    day: date,
    value: float,
) -> None:
    source[series_id] = [
        (row_day, value if row_day == day else row_value)
        for row_day, row_value in source[series_id]
    ]


def _append_next_day(source: dict[str, list[tuple[date, float]]]) -> None:
    for index, series_id in enumerate(SERIES_REGISTRY):
        source[series_id].append((NEXT_TODAY, float(100 + index)))


def test_full_offline_daily_delta_reconcile_publication_retention_and_inventory(
    tmp_path: Path,
) -> None:
    source = _source_data()
    providers = _providers(source)
    pipeline, paths, catalog = _stack(tmp_path, source, providers)

    first = pipeline.run_daily([], today=INITIAL_TODAY)
    assert first.gold_build_id == "20260819T020000Z"
    assert first.bronze is not None
    assert all(result.mode.value == "bootstrap" for result in first.bronze.successes)
    assert all(result.maximum_history for result in first.bronze.successes)
    assert paths.gold_manifest_parquet().is_file()
    assert paths.gold_manifest_json().is_file()
    assert paths.gold_profile().is_file()

    us10_provider = providers[Provider.FRED]
    assert us10_provider.requests[0][0] == "us_10y" or any(
        series_id == "us_10y" for series_id, _ in us10_provider.requests
    )
    bootstrap_us10 = next(
        request for series_id, request in us10_provider.requests if series_id == "us_10y"
    )
    assert bootstrap_us10.maximum_history and bootstrap_us10.logical_start is None

    july_silver = paths.silver_month("us_10y", date(2026, 7, 15))
    august_silver = paths.silver_month("us_10y", NEXT_TODAY)
    july_hash_before = _sha(july_silver)
    august_hash_before = _sha(august_silver)

    revision_day = date(2026, 8, 15)
    _replace_source_value(source, "us_10y", revision_day, 9.99)
    old_euro_hy_min = source["euro_hy_oas"][0][0]
    _append_next_day(source)
    source["euro_hy_oas"] = [row for row in source["euro_hy_oas"] if row[0] >= date(2026, 8, 11)]

    second = pipeline.run_daily([], today=NEXT_TODAY)
    assert second.bronze is not None
    assert all(result.mode.value == "update" for result in second.bronze.successes)
    assert all(result.request_start == date(2026, 8, 11) for result in second.bronze.successes)
    assert all(result.request_end == NEXT_TODAY for result in second.bronze.successes)
    assert all(not result.maximum_history for result in second.bronze.successes)
    assert _sha(july_silver) == july_hash_before
    assert _sha(august_silver) != august_hash_before
    assert pl.read_parquet(august_silver).filter(pl.col("observation_date") == revision_day).item(
        0, "value"
    ) == pytest.approx(9.99)

    euro_hy_bronze = sorted(
        (paths.root / "bronze" / "provider=fred" / "series=euro_hy_oas").glob(
            "year=*/month=*/data.parquet"
        )
    )
    euro_hy = pl.concat([pl.read_parquet(path) for path in euro_hy_bronze])
    assert euro_hy.get_column("observation_date").min() == old_euro_hy_min

    update_us10 = [
        request
        for series_id, request in us10_provider.requests
        if series_id == "us_10y" and request.operation == "update"
    ][-1]
    assert update_us10.logical_start == date(2026, 8, 11)
    assert update_us10.logical_start != date(2000, 1, 3)
    assert not update_us10.maximum_history

    cboe_update = [
        request
        for series_id, request in providers[Provider.CBOE].requests
        if series_id == "vix" and request.operation == "update"
    ][-1]
    assert cboe_update.logical_start == date(2026, 8, 11)
    assert not cboe_update.maximum_history

    pipeline.reconcile(["us_10y"], today=NEXT_TODAY)
    reconcile_request = us10_provider.requests[-1][1]
    assert reconcile_request.operation == "reconcile"
    assert reconcile_request.maximum_history
    assert reconcile_request.logical_start is None

    bronze_august = paths.bronze_month(Provider.FRED, "us_10y", NEXT_TODAY)
    bronze_hash_before_noop = _sha(bronze_august)
    bronze_mtime_before_noop = bronze_august.stat().st_mtime_ns
    silver_hash_before_noop = _sha(august_silver)
    noop = pipeline.run_daily([], today=NEXT_TODAY)
    assert noop.bronze is not None
    assert sum(result.written_partitions for result in noop.bronze.successes) == 0
    assert _sha(bronze_august) == bronze_hash_before_noop
    assert bronze_august.stat().st_mtime_ns == bronze_mtime_before_noop
    assert _sha(august_silver) == silver_hash_before_noop

    for _ in range(3):
        pipeline.run_daily([], today=NEXT_TODAY)

    records = catalog.read()
    complete_unpruned = [record for record in records if record.selectable_complete]
    assert len(complete_unpruned) == 5
    pruned = [record for record in records if record.pruned_at_utc is not None]
    assert len(pruned) == 1
    assert pruned[0].data_path is None
    current = next(record for record in records if record.current)
    compatibility = GoldCompatibility(GOLD_SCHEMA_VERSION, GOLD_FEATURE_VERSION)
    assert STRICT_CURRENT.resolve(records, compatibility) == current
    assert LATEST_COMPATIBLE.resolve(records, compatibility) == current
    assert not paths.gold_build_root(pruned[0].build_id).exists()

    gold = pl.read_parquet(paths.gold_dataset_root() / current.data_path)
    assert gold.columns == list(GOLD_COLUMNS)
    assert gold.schema["timestamp_m1"] == pl.Datetime("us", "UTC")
    assert gold.get_column("timestamp_m1").is_sorted()
    assert gold.get_column("timestamp_m1").is_duplicated().sum() == 0
    assert "observation_date" not in gold.columns
    assert gold.filter(pl.col("timestamp_m1") == datetime(2026, 8, 19, tzinfo=UTC)).height == 1
    assert (
        paths.gold_profile().read_bytes() == paths.gold_build_profile(current.build_id).read_bytes()
    )

    inventory = read_inventory(paths.inventory())
    assert len(inventory) == len(SERIES_REGISTRY)
    us10_inventory = next(record for record in inventory if record.series_id == "us_10y")
    assert us10_inventory.min_observation_date == date(2000, 1, 3)
    assert us10_inventory.max_observation_date == NEXT_TODAY
    assert us10_inventory.duplicate_key_count == 0

    for provider, fixture in providers.items():
        assert fixture.requests, f"provider fixture not exercised: {provider.value}"
