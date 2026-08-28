"""Shared semantic validation for provider-supplied OHLC market bars."""

from __future__ import annotations

import math
from collections.abc import Mapping


def validate_ohlc_bar(values: Mapping[str, float], *, provider: str) -> None:
    """Reject invalid index-level and high/low relationships before persistence."""
    open_value = values["open"]
    high_value = values["high"]
    low_value = values["low"]
    close_value = values["close"]
    if not all(math.isfinite(value) and value >= 0 for value in values.values()):
        raise ValueError(f"{provider} payload contains invalid non-negative OHLC levels")
    if high_value < low_value:
        raise ValueError(f"{provider} payload high is below low")
    if high_value < max(open_value, close_value):
        raise ValueError(f"{provider} payload high is below open or close")
    if low_value > min(open_value, close_value):
        raise ValueError(f"{provider} payload low is above open or close")
