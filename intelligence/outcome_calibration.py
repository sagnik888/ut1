"""Advisory outcome-calibration dataset builder.

This stores compact rows for later analysis only. It must not change live
trading decisions until a separate validation step proves an edge.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List


CALIBRATION_FIELDS = [
    "trade_id",
    "instrument",
    "timeframe",
    "inst_type",
    "direction",
    "grade",
    "confidence",
    "entry_time",
    "exit_time",
    "pnl",
    "mfe",
    "mae",
    "exit_reason",
    "regime",
    "pcr",
    "oi_signal",
    "ofr",
    "iv_percentile",
]


class OutcomeCalibrationStore:
    """Append-only compact CSV store for post-trade research."""

    def __init__(self, path: str = "data_store/outcome_calibration.csv") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def build_row(self, trade: Any, intel: Dict[str, Any] | None = None) -> Dict[str, Any]:
        intel = intel or {}
        pcr = intel.get("pcr", {}) if isinstance(intel.get("pcr"), dict) else {}
        oi = intel.get("oi", {}) if isinstance(intel.get("oi"), dict) else {}
        flow = intel.get("order_flow", {}) if isinstance(intel.get("order_flow"), dict) else {}
        greeks = intel.get("greeks", {}) if isinstance(intel.get("greeks"), dict) else {}
        regime = intel.get("regime", {}) if isinstance(intel.get("regime"), dict) else {}
        return {
            "trade_id": getattr(trade, "id", ""),
            "instrument": getattr(trade, "instrument", ""),
            "timeframe": getattr(trade, "timeframe", ""),
            "inst_type": getattr(trade, "inst_type", ""),
            "direction": getattr(trade, "direction", ""),
            "grade": getattr(trade, "grade", ""),
            "confidence": float(getattr(trade, "confidence", 0.0) or 0.0),
            "entry_time": getattr(getattr(trade, "entry_time", None), "isoformat", lambda: "")(),
            "exit_time": getattr(getattr(trade, "exit_time", None), "isoformat", lambda: "")(),
            "pnl": float(getattr(trade, "pnl", 0.0) or 0.0),
            "mfe": float(getattr(trade, "peak_pnl", 0.0) or 0.0),
            "mae": float(getattr(trade, "max_drawdown", 0.0) or 0.0),
            "exit_reason": getattr(trade, "exit_reason", ""),
            "regime": regime.get("regime", ""),
            "pcr": pcr.get("primary_pcr", pcr.get("pcr_oi", pcr.get("pcr", ""))),
            "oi_signal": oi.get("signal", ""),
            "ofr": flow.get("ratio", flow.get("buy_sell_ratio", "")),
            "iv_percentile": greeks.get("iv_percentile", ""),
        }

    def append_rows(self, rows: Iterable[Dict[str, Any]]) -> int:
        rows = list(rows)
        if not rows:
            return 0
        write_header = not self.path.exists() or self.path.stat().st_size == 0
        with self.path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=CALIBRATION_FIELDS, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return len(rows)

    def compact_from_trades(self, trades: Iterable[Any], intel_by_instrument: Dict[str, Any] | None = None) -> int:
        intel_by_instrument = intel_by_instrument or {}
        rows: List[Dict[str, Any]] = []
        for trade in trades:
            rows.append(self.build_row(trade, intel_by_instrument.get(getattr(trade, "instrument", ""))))
        return self.append_rows(rows)
