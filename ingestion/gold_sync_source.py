"""Read-only filesystem Adapter for catalog-selected immutable Gold sync input."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from application.gold_catalog import GoldCatalogRecord
from application.gold_frame import (
    GOLD_COLUMNS,
    GOLD_FEATURE_VERSION,
    GOLD_SCHEMA_VERSION,
    GOLD_SOURCE_SERIES,
)
from application.gold_sidecars import CURRENT_MANIFEST_VERSION, feature_set_sha256
from application.paths import LakePaths
from ingestion.gold_build_store import GoldBuildStore

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class FilesystemGoldFrameSource:
    """Resolve only paths contained by the canonical Gold dataset root."""

    def __init__(self, paths: LakePaths, build_store: GoldBuildStore) -> None:
        self._paths = paths
        self._build_store = build_store

    def sha256_path(self, relative_data_path: str) -> str:
        path = self._resolve(relative_data_path)
        if not path.is_file():
            raise FileNotFoundError(path)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def validate_bundle(self, record: GoldCatalogRecord) -> None:
        """Verify the catalog-selected immutable bundle before PostgreSQL is contacted."""
        data_relative = record.data_path
        manifest_relative = record.build_manifest_path
        plot_relative = record.plot_path
        if (
            not isinstance(data_relative, str)
            or not data_relative
            or not isinstance(manifest_relative, str)
            or not manifest_relative
            or not isinstance(plot_relative, str)
            or not plot_relative
        ):
            raise ValueError("Gold catalog record has incomplete artifact paths")
        data_path = self._resolve(data_relative)
        manifest_path = self._resolve(manifest_relative)
        plot_path = self._resolve(plot_relative)
        expected_root = self._paths.gold_build_root(record.build_id).resolve()
        if (
            data_path != (expected_root / "data.parquet")
            or manifest_path != (expected_root / "manifest.json")
            or plot_path != (expected_root / "feature_profile.png")
        ):
            raise ValueError("Gold catalog artifact path does not match selected build")
        if not data_path.is_file() or not manifest_path.is_file() or not plot_path.is_file():
            raise FileNotFoundError("Gold selected bundle artifact is missing")

        payload = json.loads(manifest_path.read_bytes())
        if not isinstance(payload, dict):
            raise ValueError("Gold build manifest must be an object")
        self._validate_manifest(payload, record)
        frame = self._build_store.read_path(data_path)
        if hashlib.sha256(data_path.read_bytes()).hexdigest() != payload["data_sha256"]:
            raise ValueError("Gold selected data SHA-256 differs from build manifest")
        if (
            feature_set_sha256(
                frame,
                schema_version=record.schema_version,
                feature_version=record.feature_version,
            )
            != payload["feature_set_hash"]
        ):
            raise ValueError("Gold selected feature set hash differs from build manifest")

    @staticmethod
    def _validate_manifest(payload: dict[str, object], record: GoldCatalogRecord) -> None:
        expected_data = f"versions/build_id={record.build_id}/data.parquet"
        expected_manifest = f"versions/build_id={record.build_id}/manifest.json"
        expected_plot = f"versions/build_id={record.build_id}/feature_profile.png"
        expected = {
            "dataset_id": record.dataset_id,
            "build_id": record.build_id,
            "artifact_state": "built",
            "manifest_version": CURRENT_MANIFEST_VERSION,
            "schema_version": record.schema_version,
            "feature_version": record.feature_version,
            "row_count": record.row_count,
            "columns": list(GOLD_COLUMNS),
            "data_path": expected_data,
            "plot_path": expected_plot,
        }
        for key, value in expected.items():
            if payload.get(key) != value:
                raise ValueError(f"Gold build manifest {key} mismatch")
        if record.data_path != expected_data or record.build_manifest_path != expected_manifest:
            raise ValueError("Gold catalog path differs from selected build identity")
        if (
            record.schema_version != GOLD_SCHEMA_VERSION
            or record.feature_version != GOLD_FEATURE_VERSION
        ):
            raise ValueError("Gold catalog semantic version is unsupported")
        data_sha256 = payload.get("data_sha256")
        if not isinstance(data_sha256, str) or _SHA256_RE.fullmatch(data_sha256) is None:
            raise ValueError("Gold build manifest data SHA-256 is invalid")
        feature_set_hash = payload.get("feature_set_hash")
        if not isinstance(feature_set_hash, str) or _SHA256_RE.fullmatch(feature_set_hash) is None:
            raise ValueError("Gold build manifest feature set hash is invalid")
        git_commit_hash = payload.get("git_commit_hash")
        if not isinstance(git_commit_hash, str) or not re.fullmatch(
            r"[0-9a-f]{40,64}", git_commit_hash
        ):
            raise ValueError("Gold build manifest Git identity is invalid")
        if payload.get("min_timestamp") != FilesystemGoldFrameSource._timestamp_text(
            record.min_timestamp
        ):
            raise ValueError("Gold build manifest minimum timestamp mismatch")
        if payload.get("max_timestamp") != FilesystemGoldFrameSource._timestamp_text(
            record.max_timestamp
        ):
            raise ValueError("Gold build manifest maximum timestamp mismatch")
        inputs = payload.get("inputs")
        if not isinstance(inputs, list) or [
            item.get("series_id") for item in inputs if isinstance(item, dict)
        ] != list(GOLD_SOURCE_SERIES):
            raise ValueError("Gold build manifest input provenance is not certified")
        if len(inputs) != len(GOLD_SOURCE_SERIES) or any(
            not isinstance(item, dict)
            or not isinstance(item.get("row_count"), int)
            or item["row_count"] < 0
            or not isinstance(item.get("sha256"), str)
            or _SHA256_RE.fullmatch(item["sha256"]) is None
            or not FilesystemGoldFrameSource._valid_input_dates(item)
            for item in inputs
        ):
            raise ValueError("Gold build manifest input provenance is invalid")

    @staticmethod
    def _valid_input_dates(input_signature: dict[str, object]) -> bool:
        minimum = input_signature.get("min_observation_date")
        maximum = input_signature.get("max_observation_date")
        if minimum is None or maximum is None:
            return minimum is None and maximum is None
        if not isinstance(minimum, str) or not isinstance(maximum, str):
            return False
        try:
            return datetime.fromisoformat(minimum).date() <= datetime.fromisoformat(maximum).date()
        except ValueError:
            return False

    @staticmethod
    def _timestamp_text(value: datetime | None) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("Gold catalog timestamp must be timezone-aware")
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")

    def read_path(self, relative_data_path: str) -> pl.DataFrame:
        return self._build_store.read_path(self._resolve(relative_data_path))

    def _resolve(self, relative_data_path: str) -> Path:
        relative = Path(relative_data_path)
        if relative.is_absolute():
            raise ValueError("Gold sync data path must be relative to the Gold dataset root")
        root = self._paths.gold_dataset_root().resolve()
        candidate = (root / relative).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise ValueError("Gold sync data path escapes the Gold dataset root") from exc
        return candidate
