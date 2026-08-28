"""Pure builders for deterministic immutable Gold build sidecar metadata."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import polars as pl

from application.gold_frame import GOLD_COLUMNS, GOLD_FEATURE_VERSION, GOLD_SCHEMA_VERSION

_DATASET_ID = "regime_features_daily"
_GIT_HASH_RE = re.compile(r"^[0-9a-f]{40,64}$")
_TEST_GIT_FALLBACK = "0" * 40

GOLD_FORMULA_PARAMETERS: dict[str, object] = {
    "cross_series_alignment": "same timestamp only",
    "missing_data_policy": "no fill interpolation centered window or asof carry",
    "observation_delta_semantics": "source-unit absolute difference over valid observations",
    "observation_delta_observations": [1, 5, 20],
    "volatility_zscore": {"ddof": 0, "window_observations": 60},
}


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("Gold sidecar timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")


def feature_set_sha256(
    frame: pl.DataFrame,
    *,
    schema_version: int = GOLD_SCHEMA_VERSION,
    feature_version: int = GOLD_FEATURE_VERSION,
    formula_parameters: Mapping[str, object] = GOLD_FORMULA_PARAMETERS,
) -> str:
    """Hash semantic versions, ordered names/dtypes, and formula-policy parameters."""
    columns = [{"dtype": str(frame.schema[column]), "name": column} for column in frame.columns]
    payload = {
        "columns": columns,
        "feature_version": feature_version,
        "formula_parameters": dict(formula_parameters),
        "schema_version": schema_version,
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


@dataclass(frozen=True, slots=True)
class GoldBuildManifest:
    """Immutable build-level artifact metadata; publication status lives in the catalog."""

    dataset_id: str
    build_id: str
    artifact_state: str
    schema_version: int
    feature_version: int
    started_at_utc: str
    completed_at_utc: str
    row_count: int
    columns: tuple[str, ...]
    min_timestamp: str | None
    max_timestamp: str | None
    data_path: str
    data_sha256: str
    feature_set_hash: str
    git_commit_hash: str
    plot_path: str

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_state": self.artifact_state,
            "build_id": self.build_id,
            "columns": list(self.columns),
            "completed_at_utc": self.completed_at_utc,
            "data_path": self.data_path,
            "data_sha256": self.data_sha256,
            "dataset_id": self.dataset_id,
            "feature_set_hash": self.feature_set_hash,
            "feature_version": self.feature_version,
            "git_commit_hash": self.git_commit_hash,
            "max_timestamp": self.max_timestamp,
            "min_timestamp": self.min_timestamp,
            "plot_path": self.plot_path,
            "row_count": self.row_count,
            "schema_version": self.schema_version,
            "started_at_utc": self.started_at_utc,
        }

    def to_json_bytes(self) -> bytes:
        return _canonical_json(self.as_dict())


class GoldSidecarBuilder:
    """Builder for deterministic artifact metadata independent of physical persistence."""

    def __init__(
        self,
        *,
        git_commit_hash: str | None,
        allow_test_git_fallback: bool = False,
        formula_parameters: Mapping[str, object] = GOLD_FORMULA_PARAMETERS,
    ) -> None:
        resolved_git = git_commit_hash
        if resolved_git is None and allow_test_git_fallback:
            resolved_git = _TEST_GIT_FALLBACK
        if resolved_git is None:
            raise ValueError("git_commit_hash is required for Gold build sidecars")
        if _GIT_HASH_RE.fullmatch(resolved_git) is None:
            raise ValueError("git_commit_hash must be 40..64 lowercase hexadecimal characters")
        self._git_commit_hash = resolved_git
        self._formula_parameters = dict(formula_parameters)

    def feature_set_hash(
        self,
        frame: pl.DataFrame,
        *,
        schema_version: int = GOLD_SCHEMA_VERSION,
        feature_version: int = GOLD_FEATURE_VERSION,
    ) -> str:
        return feature_set_sha256(
            frame,
            schema_version=schema_version,
            feature_version=feature_version,
            formula_parameters=self._formula_parameters,
        )

    def build(
        self,
        frame: pl.DataFrame,
        *,
        build_id: str,
        started_at_utc: datetime,
        completed_at_utc: datetime,
        data_path: str,
        data_sha256: str,
        plot_path: str,
        schema_version: int = GOLD_SCHEMA_VERSION,
        feature_version: int = GOLD_FEATURE_VERSION,
    ) -> GoldBuildManifest:
        if frame.columns != list(GOLD_COLUMNS):
            raise ValueError("Gold sidecar frame column order mismatch")
        started = _utc_text(started_at_utc)
        completed = _utc_text(completed_at_utc)
        if completed_at_utc.astimezone(UTC) < started_at_utc.astimezone(UTC):
            raise ValueError("Gold build completion cannot precede start")
        timestamps = frame.get_column("timestamp_m1")
        minimum = timestamps.min()
        maximum = timestamps.max()
        if minimum is not None and not isinstance(minimum, datetime):
            raise TypeError("Gold sidecar min timestamp must be datetime")
        if maximum is not None and not isinstance(maximum, datetime):
            raise TypeError("Gold sidecar max timestamp must be datetime")
        return GoldBuildManifest(
            dataset_id=_DATASET_ID,
            build_id=build_id,
            artifact_state="built",
            schema_version=schema_version,
            feature_version=feature_version,
            started_at_utc=started,
            completed_at_utc=completed,
            row_count=frame.height,
            columns=tuple(frame.columns),
            min_timestamp=None if minimum is None else _utc_text(minimum),
            max_timestamp=None if maximum is None else _utc_text(maximum),
            data_path=data_path,
            data_sha256=data_sha256,
            feature_set_hash=self.feature_set_hash(
                frame,
                schema_version=schema_version,
                feature_version=feature_version,
            ),
            git_commit_hash=self._git_commit_hash,
            plot_path=plot_path,
        )


def expected_manifest_keys() -> Sequence[str]:
    """Stable key contract used by physical validation and offline tests."""
    return tuple(sorted(GoldBuildManifest.__dataclass_fields__))
