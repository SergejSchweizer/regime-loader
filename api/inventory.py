"""Read-only inventory command adapter."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TextIO

from application.contracts import Provider
from application.operational_records import InventoryRecord
from application.paths import LakePaths
from application.registry import SERIES_REGISTRY
from ingestion.operational_repository import read_inventory

InventoryReader = Callable[[], Sequence[InventoryRecord]]
INVENTORY_FIELDS = (
    "series_id",
    "provider",
    "min_observation_date",
    "max_observation_date",
    "row_count",
    "duplicate_key_count",
    "file_count",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="regime-loader inventory")
    parser.add_argument("--lake-root", type=Path, default=Path("lake"))
    parser.add_argument("--series", action="append", default=[])
    parser.add_argument("--provider", action="append", default=[])
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def _validate_filters(series: Sequence[str], providers: Sequence[str]) -> None:
    unknown_series = sorted(set(series).difference(SERIES_REGISTRY))
    if unknown_series:
        raise ValueError(f"unknown series filter: {', '.join(unknown_series)}")
    allowed_providers = {provider.value for provider in Provider}
    unknown_providers = sorted(set(providers).difference(allowed_providers))
    if unknown_providers:
        raise ValueError(f"unknown provider filter: {', '.join(unknown_providers)}")


def _filter_records(
    records: Sequence[InventoryRecord],
    *,
    series: Sequence[str],
    providers: Sequence[str],
) -> list[InventoryRecord]:
    series_set = set(series)
    provider_set = set(providers)
    selected = [
        record
        for record in records
        if (not series_set or record.series_id in series_set)
        and (not provider_set or record.provider.value in provider_set)
    ]
    return sorted(selected, key=lambda record: (record.series_id, record.provider.value))


def _logical_row(record: InventoryRecord) -> dict[str, object]:
    return {
        "series_id": record.series_id,
        "provider": record.provider.value,
        "min_observation_date": None
        if record.min_observation_date is None
        else record.min_observation_date.isoformat(),
        "max_observation_date": None
        if record.max_observation_date is None
        else record.max_observation_date.isoformat(),
        "row_count": record.row_count,
        "duplicate_key_count": record.duplicate_key_count,
        "file_count": record.file_count,
    }


def render_text(records: Sequence[InventoryRecord]) -> str:
    lines = ["\t".join(INVENTORY_FIELDS)]
    for record in records:
        row = _logical_row(record)
        lines.append(
            "\t".join("" if row[field] is None else str(row[field]) for field in INVENTORY_FIELDS)
        )
    return "\n".join(lines) + "\n"


def render_json(records: Sequence[InventoryRecord]) -> str:
    return (
        json.dumps(
            [_logical_row(record) for record in records],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        + "\n"
    )


def run_inventory(
    argv: Sequence[str],
    *,
    reader: InventoryReader,
    stdout: TextIO,
    stderr: TextIO,
) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(list(argv))
        _validate_filters(args.series, args.provider)
        records = _filter_records(reader(), series=args.series, providers=args.provider)
        stdout.write(render_json(records) if args.as_json else render_text(records))
        return 0
    except (OSError, ValueError) as exc:
        stderr.write(f"inventory: {exc}\n")
        return 2


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    parsed, _ = parser.parse_known_args(args)
    paths = LakePaths(parsed.lake_root)
    return run_inventory(
        args,
        reader=lambda: read_inventory(paths.inventory()),
        stdout=sys.stdout,
        stderr=sys.stderr,
    )


if __name__ == "__main__":
    raise SystemExit(main())
