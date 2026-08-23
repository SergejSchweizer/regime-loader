"""Creation-only filesystem and plotting Adapter for immutable Gold build sidecars."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path

import polars as pl
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure

from application.gold_frame import GOLD_COLUMNS
from application.gold_sidecars import (
    GoldBuildManifest,
    GoldSidecarBuilder,
    expected_manifest_keys,
)
from application.paths import LakePaths
from ingestion.gold_build_store import GoldBuildArtifact, GoldBuildStore

FaultInjector = Callable[[str], None]
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _no_fault(stage: str) -> None:
    del stage


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _create_bytes(path: Path, payload: bytes) -> None:
    """Durably create a file without an overwrite path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def feature_profile_data(frame: pl.DataFrame) -> tuple[tuple[str, ...], tuple[float, ...]]:
    """Return canonical feature order and deterministic non-null fractions for plotting."""
    features = tuple(GOLD_COLUMNS[1:])
    if frame.columns != list(GOLD_COLUMNS):
        raise ValueError("Gold feature profile requires exact canonical column order")
    denominator = max(frame.height, 1)
    coverage = tuple(
        (frame.height - frame.get_column(column).null_count()) / denominator for column in features
    )
    return features, coverage


def _profile_png(frame: pl.DataFrame) -> bytes:
    features, coverage = feature_profile_data(frame)
    timestamps = frame.get_column("timestamp_m1").to_list()
    figure = Figure(figsize=(18, max(8, len(features) * 2.25)), dpi=100, facecolor="#080d19")
    canvas = FigureCanvasAgg(figure)
    axes = figure.subplots(
        nrows=len(features), ncols=2, squeeze=False, gridspec_kw={"width_ratios": [4, 1]}
    )
    for index, (feature, fraction) in enumerate(zip(features, coverage, strict=True)):
        series_axis, histogram_axis = axes[index]
        values = frame.get_column(feature).to_list()
        observed = [
            (stamp, float(value))
            for stamp, value in zip(timestamps, values, strict=True)
            if value is not None
        ]
        for axis in (series_axis, histogram_axis):
            axis.set_facecolor("#101827")
            axis.grid(color="#26354f", alpha=0.45, linewidth=0.5)
            axis.tick_params(colors="#b8c5d9", labelsize=6)
            for spine in axis.spines.values():
                spine.set_color("#31435f")
        series_axis.set_ylabel(feature, color="#cdd7e6", fontsize=7)
        if not observed:
            series_axis.text(
                0.02,
                0.5,
                f"feature: {feature}\nno data",
                color="#dce5f2",
                transform=series_axis.transAxes,
                va="center",
            )
            histogram_axis.text(
                0.5,
                0.5,
                "no data",
                color="#dce5f2",
                transform=histogram_axis.transAxes,
                ha="center",
                va="center",
                fontsize=7,
            )
            continue
        dates, numeric = zip(*observed, strict=True)
        series_axis.plot(dates, numeric, color="#76c7e8", linewidth=0.8)
        series_axis.axvspan(timestamps[0], dates[0], color="#7d2949", alpha=0.32)
        summary = (
            f"coverage: {fraction:.1%}\nn: {len(numeric)}\n"
            f"mean: {sum(numeric) / len(numeric):.5g}\n"
            f"min/max: {min(numeric):.5g} / {max(numeric):.5g}"
        )
        series_axis.text(
            0.01,
            0.96,
            summary,
            transform=series_axis.transAxes,
            va="top",
            color="#dce5f2",
            fontsize=6,
            bbox={"facecolor": "#0b1322", "edgecolor": "#31435f", "alpha": 0.85, "pad": 2},
        )
        bins = min(30, max(5, int(math.sqrt(len(numeric)))))
        histogram_axis.hist(numeric, bins=bins, color="#b7c98a", edgecolor="#9aac75", linewidth=0.3)
        histogram_axis.set_xlabel("value", color="#cdd7e6", fontsize=7)
    figure.suptitle("Gold daily feature profile", color="#e8eef8", fontsize=16, fontweight="bold")
    figure.text(
        0.5,
        0.985,
        f"{frame.height:,} rows | {timestamps[0]:%Y-%m-%d} → {timestamps[-1]:%Y-%m-%d}",
        color="#b8c5d9",
        ha="center",
        va="top",
        fontsize=9,
    )
    figure.subplots_adjust(left=0.17, right=0.98, top=0.97, bottom=0.03, hspace=0.8, wspace=0.08)
    buffer = BytesIO()
    canvas.print_png(  # type: ignore[no-untyped-call]
        buffer,
        metadata={"Software": "regime-loader"},
    )
    return buffer.getvalue()


@dataclass(frozen=True, slots=True)
class GoldSidecarArtifacts:
    manifest: GoldBuildManifest
    manifest_path: Path
    plot_path: Path
    manifest_sha256: str
    plot_sha256: str


class GoldSidecarStore:
    """Adapter creating and validating JSON/PNG within one immutable build directory."""

    def __init__(
        self,
        paths: LakePaths,
        build_store: GoldBuildStore,
        builder: GoldSidecarBuilder,
        *,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        self._paths = paths
        self._build_store = build_store
        self._builder = builder
        self._fault = fault_injector if fault_injector is not None else _no_fault

    def create(
        self,
        artifact: GoldBuildArtifact,
        *,
        started_at_utc: datetime,
        completed_at_utc: datetime,
    ) -> GoldSidecarArtifacts:
        manifest_path = self._paths.gold_build_manifest(artifact.build_id)
        plot_path = self._paths.gold_build_profile(artifact.build_id)
        if manifest_path.exists() or plot_path.exists():
            raise FileExistsError(f"Gold sidecars already exist for build {artifact.build_id}")
        frame = self._validated_frame(artifact)
        plot_bytes = _profile_png(frame)
        self._fault("after_plot_bytes")
        _create_bytes(plot_path, plot_bytes)
        self._fault("after_plot_create")
        manifest = self._builder.build(
            frame,
            build_id=artifact.build_id,
            started_at_utc=started_at_utc,
            completed_at_utc=completed_at_utc,
            data_path=self._relative(artifact.data_path),
            data_sha256=artifact.data_sha256,
            plot_path=self._relative(plot_path),
        )
        manifest_bytes = manifest.to_json_bytes()
        self._fault("after_manifest_bytes")
        _create_bytes(manifest_path, manifest_bytes)
        self._fault("after_manifest_create")
        sidecars = GoldSidecarArtifacts(
            manifest=manifest,
            manifest_path=manifest_path,
            plot_path=plot_path,
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            plot_sha256=hashlib.sha256(plot_bytes).hexdigest(),
        )
        self.validate_bundle(artifact, sidecars)
        return sidecars

    def validate_bundle(
        self,
        artifact: GoldBuildArtifact,
        sidecars: GoldSidecarArtifacts,
    ) -> None:
        frame = self._validated_frame(artifact)
        if sidecars.manifest_path != self._paths.gold_build_manifest(artifact.build_id):
            raise ValueError("Gold build manifest path mismatch")
        if sidecars.plot_path != self._paths.gold_build_profile(artifact.build_id):
            raise ValueError("Gold feature profile path mismatch")
        if not sidecars.manifest_path.is_file() or not sidecars.plot_path.is_file():
            raise FileNotFoundError("Gold build bundle is incomplete")
        manifest_bytes = sidecars.manifest_path.read_bytes()
        if manifest_bytes != sidecars.manifest.to_json_bytes():
            raise ValueError("Gold build manifest bytes mismatch")
        if hashlib.sha256(manifest_bytes).hexdigest() != sidecars.manifest_sha256:
            raise ValueError("Gold build manifest SHA-256 mismatch")
        plot_bytes = sidecars.plot_path.read_bytes()
        if not plot_bytes.startswith(_PNG_SIGNATURE):
            raise ValueError("Gold feature profile is not a PNG")
        if hashlib.sha256(plot_bytes).hexdigest() != sidecars.plot_sha256:
            raise ValueError("Gold feature profile SHA-256 mismatch")
        parsed = json.loads(manifest_bytes)
        if sorted(parsed) != list(expected_manifest_keys()):
            raise ValueError("Gold build manifest key set mismatch")
        manifest = sidecars.manifest
        if manifest.artifact_state != "built":
            raise ValueError("Gold build sidecar artifact_state must be built")
        if "status" in parsed:
            raise ValueError("Gold build manifest must not contain publication status")
        if manifest.build_id != artifact.build_id:
            raise ValueError("Gold build identity mismatch")
        if manifest.data_path != self._relative(artifact.data_path):
            raise ValueError("Gold build data path mismatch")
        if manifest.plot_path != self._relative(sidecars.plot_path):
            raise ValueError("Gold build plot path mismatch")
        if manifest.data_sha256 != artifact.data_sha256:
            raise ValueError("Gold build manifest data SHA-256 mismatch")
        if manifest.row_count != artifact.row_count:
            raise ValueError("Gold build manifest row count mismatch")
        expected_feature_hash = self._builder.feature_set_hash(
            frame,
            schema_version=manifest.schema_version,
            feature_version=manifest.feature_version,
        )
        if manifest.feature_set_hash != expected_feature_hash:
            raise ValueError("Gold build feature-set hash mismatch")

    def _validated_frame(self, artifact: GoldBuildArtifact) -> pl.DataFrame:
        expected_path = self._paths.gold_data(artifact.build_id)
        if artifact.data_path != expected_path:
            raise ValueError("Gold build artifact data path mismatch")
        if not expected_path.is_file():
            raise FileNotFoundError(expected_path)
        if _sha256(expected_path) != artifact.data_sha256:
            raise ValueError("Gold build artifact data SHA-256 mismatch")
        frame = self._build_store.read_build(artifact.build_id)
        timestamps = frame.get_column("timestamp_m1")
        minimum = timestamps.min()
        maximum = timestamps.max()
        if frame.height != artifact.row_count:
            raise ValueError("Gold build artifact row count mismatch")
        if minimum != artifact.min_timestamp or maximum != artifact.max_timestamp:
            raise ValueError("Gold build artifact timestamp bounds mismatch")
        return frame

    def _relative(self, path: Path) -> str:
        try:
            return path.relative_to(self._paths.gold_dataset_root()).as_posix()
        except ValueError as exc:
            raise ValueError("Gold artifact path must be inside dataset root") from exc
