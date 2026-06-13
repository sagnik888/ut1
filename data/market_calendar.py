"""Versioned exchange holiday calendar helpers."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Set


NSE_HOLIDAYS_BY_YEAR = {
    2026: {
        "2026-01-26",
        "2026-03-03",
        "2026-03-26",
        "2026-03-31",
        "2026-04-03",
        "2026-04-14",
        "2026-05-01",
        "2026-05-28",
        "2026-06-26",
        "2026-09-14",
        "2026-10-02",
        "2026-10-20",
        "2026-11-10",
        "2026-11-24",
        "2026-12-25",
    }
}


def _normalize_dates(values: Iterable[object]) -> Set[str]:
    dates: Set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        try:
            dates.add(datetime.fromisoformat(text[:10]).strftime("%Y-%m-%d"))
        except ValueError:
            continue
    return dates


def load_nse_holidays(path: str | Path = "data_store/market_holidays.json") -> Set[str]:
    """Load bundled holidays plus optional user-maintained overrides."""
    holidays: Set[str] = set()
    for year_dates in NSE_HOLIDAYS_BY_YEAR.values():
        holidays.update(year_dates)

    override_path = Path(path)
    if not override_path.exists():
        return holidays

    try:
        payload = json.loads(override_path.read_text(encoding="utf-8"))
    except Exception:
        return holidays

    if isinstance(payload, dict):
        for values in payload.values():
            if isinstance(values, list):
                holidays.update(_normalize_dates(values))
    elif isinstance(payload, list):
        holidays.update(_normalize_dates(payload))
    return holidays

