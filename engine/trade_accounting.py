"""Shared live, ledger, and backtest transaction-cost accounting."""

from __future__ import annotations

from typing import Any


def estimate_trade_charges(
    entry_price: float,
    exit_price: float,
    quantity: int,
    inst_type: str,
    settings: Any,
    multiplier: float = 1.0,
) -> float:
    """Return the configured round-trip charge used by every trade view."""
    base = (
        float(getattr(settings, "opt_cost", 80.0) or 80.0)
        if str(inst_type or "FUT").upper() == "OPT"
        else float(getattr(settings, "fut_cost", 200.0) or 200.0)
    )
    return round(base, 2)
