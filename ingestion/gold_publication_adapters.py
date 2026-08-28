"""Physical Gold publication adapters for validated immutable bundles."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from application.gold_frame import GOLD_FEATURE_VERSION, GOLD_SCHEMA_VERSION, SilverInputSignature
from application.gold_publication import GoldPublicationBundle
from application.paths import LakePaths
from ingestion.gold_build_store import GoldBuildArtifact, GoldBuildStore
from ingestion.gold_sidecar_store import GoldSidecarArtifacts, GoldSidecarStore

Clock = Callable[[], datetime]
FaultInjector = Callable[[str], None]


def _system_utc_now() -> datetime:
    return datetime.now(UTC)


def _no_fault(stage: str) -> None:
    del stage


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Gold bundle completion clock must be timezone-aware")
    return value.astimezone(UTC)


class GoldBundleAdapter:
    """Adapter composing immutable Parquet and sidecars into one validated candidate bundle."""

    def __init__(
        self,
        paths: LakePaths,
        build_store: GoldBuildStore,
        sidecar_store: GoldSidecarStore,
        *,
        clock: Clock | None = None,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        self._paths = paths
        self._build_store = build_store
        self._sidecar_store = sidecar_store
        self._clock = clock if clock is not None else _system_utc_now
        self._fault = fault_injector if fault_injector is not None else _no_fault

    def next_build_id(self) -> str:
        return self._build_store.next_build_id()

    def create_and_validate(
        self,
        frame: pl.DataFrame,
        *,
        build_id: str,
        started_at_utc: datetime,
        inputs: tuple[SilverInputSignature, ...],
    ) -> GoldPublicationBundle:
        if not inputs:
            raise ValueError("Gold publication requires certified Silver input provenance")
        artifact = self._build_store.create(frame, build_id=build_id)
        completed = _utc(self._clock())
        sidecars = self._sidecar_store.create(
            artifact,
            started_at_utc=started_at_utc,
            completed_at_utc=completed,
            inputs=inputs,
        )
        self._fault("after_bundle_create")
        self._validate_candidate(artifact, sidecars, inputs)
        return GoldPublicationBundle(
            build_id=artifact.build_id,
            completed_at_utc=completed,
            schema_version=sidecars.manifest.schema_version,
            feature_version=sidecars.manifest.feature_version,
            row_count=artifact.row_count,
            min_timestamp=artifact.min_timestamp,
            max_timestamp=artifact.max_timestamp,
            data_path=self._relative(artifact.data_path),
            build_manifest_path=self._relative(sidecars.manifest_path),
            plot_path=self._relative(sidecars.plot_path),
        )

    def _validate_candidate(
        self,
        artifact: GoldBuildArtifact,
        sidecars: GoldSidecarArtifacts,
        inputs: tuple[SilverInputSignature, ...],
    ) -> None:
        build_id = artifact.build_id
        build_root = self._paths.gold_build_root(build_id)
        expected_data = self._paths.gold_data(build_id)
        expected_manifest = self._paths.gold_build_manifest(build_id)
        expected_plot = self._paths.gold_build_profile(build_id)
        physical = (artifact.data_path, sidecars.manifest_path, sidecars.plot_path)
        expected = (expected_data, expected_manifest, expected_plot)
        if physical != expected:
            raise ValueError("Gold candidate physical artifact path mismatch")
        if any(path.parent != build_root for path in physical):
            raise ValueError("Gold candidate artifact escapes expected build directory")
        if sidecars.manifest.build_id != build_id:
            raise ValueError("Gold candidate build manifest build_id mismatch")
        if sidecars.manifest.schema_version != GOLD_SCHEMA_VERSION:
            raise ValueError("Gold candidate build manifest schema_version mismatch")
        if sidecars.manifest.feature_version != GOLD_FEATURE_VERSION:
            raise ValueError("Gold candidate build manifest feature_version mismatch")
        if not sidecars.manifest.provenance_certified or sidecars.manifest.inputs != inputs:
            raise ValueError("Gold candidate input provenance mismatch")
        if sidecars.manifest.row_count != artifact.row_count:
            raise ValueError("Gold candidate build manifest row count mismatch")
        if sidecars.manifest.data_sha256 != artifact.data_sha256:
            raise ValueError("Gold candidate build manifest data SHA-256 mismatch")
        if sidecars.manifest.data_path != self._relative(expected_data):
            raise ValueError("Gold candidate build manifest data path mismatch")
        if sidecars.manifest.plot_path != self._relative(expected_plot):
            raise ValueError("Gold candidate build manifest plot path mismatch")
        self._sidecar_store.validate_bundle(artifact, sidecars)
        self._build_store.read_build(build_id)

    def _relative(self, path: Path) -> str:
        root = self._paths.gold_dataset_root()
        try:
            return path.relative_to(root).as_posix()
        except ValueError as exc:
            raise ValueError("Gold candidate artifact must be inside dataset root") from exc
