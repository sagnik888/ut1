import math
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, Iterable, Optional

import pandas as pd


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        if math.isfinite(result):
            return result
    except Exception:
        pass
    return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


class MarketIntelSnapshotStore:
    """
    Keeps async option-chain/depth intelligence aligned to closed OHLCV bars.

    Raw option-chain/depth snapshots arrive on their own cadence. The scanner
    freezes the latest derived intelligence at each bar close so charting,
    audit, and live decisions share one timestamp axis.
    """

    def __init__(self, max_points: int = 1500):
        self.max_points = max_points
        self._raw: Dict[str, Deque[Dict[str, Any]]] = defaultdict(lambda: deque(maxlen=max_points))
        self._bars: Dict[str, Dict[str, Deque[Dict[str, Any]]]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=max_points))
        )

    def push_snapshot(
        self,
        instrument: str,
        options_chain: Optional[pd.DataFrame],
        intel_result: Optional[Dict[str, Any]] = None,
        depth: Optional[Dict[str, Any]] = None,
        quality: Optional[Dict[str, Any]] = None,
        timestamp: Optional[float] = None,
    ) -> Dict[str, Any]:
        snap = self._build_snapshot(
            instrument=instrument,
            timestamp=float(timestamp or time.time()),
            options_chain=options_chain,
            intel_result=intel_result or {},
            depth=depth or {},
            quality=quality or {},
        )
        self._raw[instrument].append(snap)
        return snap

    def freeze_bar(self, instrument: str, timeframe: str, bar_close_ts: Any) -> Dict[str, Any]:
        bar_ts = self._epoch_seconds(bar_close_ts)
        snap = self._latest_at_or_before(instrument, bar_ts) or self._latest(instrument)
        if not snap:
            snap = self._empty_snapshot(instrument, bar_ts)
        row = dict(snap)
        row["time"] = int(bar_ts)
        row["timeframe"] = timeframe

        bars = self._bars[instrument][timeframe]
        if bars and bars[-1].get("time") == row["time"]:
            bars[-1] = row
        else:
            bars.append(row)
        return row

    def history(
        self,
        instrument: str,
        timeframe: str,
        times: Optional[Iterable[int]] = None,
        limit: int = 1000,
    ) -> Dict[str, Any]:
        rows = list(self._bars.get(instrument, {}).get(timeframe, []))
        if times is not None:
            allowed = {int(t) for t in times}
            rows = [r for r in rows if int(r.get("time", 0)) in allowed]
        rows = rows[-limit:]
        return {
            "pcr": [
                {"time": int(r["time"]), "value": _safe_float(r.get("primary_pcr"))}
                for r in rows
                if _safe_float(r.get("primary_pcr")) > 0
            ],
            "ofr": [
                {"time": int(r["time"]), "value": _safe_float(r.get("ofr"), 1.0)}
                for r in rows
                if _safe_float(r.get("ofr"), 1.0) > 0
            ],
            "oi_delta": [
                {
                    "time": int(r["time"]),
                    "value": abs(_safe_float(r.get("net_oi_change"))),
                    "color": "rgba(34,197,94,0.35)"
                    if _safe_float(r.get("net_oi_change")) >= 0
                    else "rgba(239,68,68,0.35)",
                }
                for r in rows
                if _safe_float(r.get("net_oi_change")) != 0
            ],
            "support": [
                {"time": int(r["time"]), "value": _safe_float(r.get("support_level"))}
                for r in rows
                if _safe_float(r.get("support_level")) > 0
            ],
            "resistance": [
                {"time": int(r["time"]), "value": _safe_float(r.get("resistance_level"))}
                for r in rows
                if _safe_float(r.get("resistance_level")) > 0
            ],
            "latest": rows[-1] if rows else {},
        }

    def latest_bar(self, instrument: str, timeframe: str) -> Dict[str, Any]:
        bars = self._bars.get(instrument, {}).get(timeframe)
        return dict(bars[-1]) if bars else {}

    def _latest(self, instrument: str) -> Optional[Dict[str, Any]]:
        rows = self._raw.get(instrument)
        return dict(rows[-1]) if rows else None

    def _latest_at_or_before(self, instrument: str, epoch_seconds: int) -> Optional[Dict[str, Any]]:
        rows = self._raw.get(instrument)
        if not rows:
            return None
        for row in reversed(rows):
            if int(row.get("snapshot_ts", 0)) <= epoch_seconds:
                return dict(row)
        return None

    @staticmethod
    def _epoch_seconds(value: Any) -> int:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            # Candle indexes in this project are naive IST; keep them aligned
            # with scanner chart conversion by localizing to Asia/Kolkata.
            ts = ts.tz_localize("Asia/Kolkata")
        else:
            ts = ts.tz_convert("Asia/Kolkata")
        return int(ts.timestamp())

    @staticmethod
    def _build_snapshot(
        instrument: str,
        timestamp: float,
        options_chain: Optional[pd.DataFrame],
        intel_result: Dict[str, Any],
        depth: Dict[str, Any],
        quality: Dict[str, Any],
    ) -> Dict[str, Any]:
        pcr = intel_result.get("pcr", {}) if isinstance(intel_result.get("pcr"), dict) else {}
        oi = intel_result.get("oi", {}) if isinstance(intel_result.get("oi"), dict) else {}
        greeks = intel_result.get("greeks", {}) if isinstance(intel_result.get("greeks"), dict) else {}
        order_flow = (
            intel_result.get("order_flow", {})
            if isinstance(intel_result.get("order_flow"), dict)
            else {}
        )
        call = greeks.get("call", {}) if isinstance(greeks.get("call"), dict) else {}
        put = greeks.get("put", {}) if isinstance(greeks.get("put"), dict) else {}
        chain = options_chain if options_chain is not None else pd.DataFrame()

        call_iv = _safe_float(call.get("iv"))
        put_iv = _safe_float(put.get("iv"))
        if call_iv > 1:
            call_iv /= 100.0
        if put_iv > 1:
            put_iv /= 100.0

        return {
            "instrument": instrument,
            "snapshot_ts": int(timestamp),
            "primary_pcr": _safe_float(pcr.get("primary_pcr") or pcr.get("pcr_oi")),
            "pcr_oi": _safe_float(pcr.get("pcr_oi")),
            "pcr_near_oi": _safe_float(pcr.get("pcr_near_oi")),
            "pcr_volume": _safe_float(pcr.get("pcr_volume")),
            "pcr_sentiment": pcr.get("sentiment", "UNKNOWN"),
            "pcr_contrarian_signal": pcr.get("signal", "NEUTRAL"),
            "pcr_trend": pcr.get("trend", "UNKNOWN"),
            "net_oi_change": _safe_float(oi.get("net_oi_change")),
            "support_level": _safe_float(oi.get("support_level")),
            "resistance_level": _safe_float(oi.get("resistance_level")),
            "oi_activity": oi.get("activity", "UNKNOWN"),
            "oi_signal": oi.get("signal", "NEUTRAL"),
            "call_iv": call_iv,
            "put_iv": put_iv,
            "iv_skew": round(call_iv - put_iv, 4),
            "ofr": _safe_float(depth.get("ofr") or order_flow.get("ratio"), 1.0),
            "ofr_source": depth.get("source") or order_flow.get("source") or "candle_proxy",
            "chain_source": quality.get("source", "unknown"),
            "chain_age_seconds": _safe_float(quality.get("age_seconds")),
            "chain_score": _safe_int(quality.get("score")),
            "strike_count": int(len(chain)),
        }

    @staticmethod
    def _empty_snapshot(instrument: str, bar_ts: int) -> Dict[str, Any]:
        return {
            "instrument": instrument,
            "snapshot_ts": int(bar_ts),
            "primary_pcr": 0.0,
            "pcr_oi": 0.0,
            "pcr_near_oi": 0.0,
            "pcr_volume": 0.0,
            "pcr_sentiment": "UNKNOWN",
            "pcr_contrarian_signal": "NEUTRAL",
            "pcr_trend": "UNKNOWN",
            "net_oi_change": 0.0,
            "support_level": 0.0,
            "resistance_level": 0.0,
            "oi_activity": "UNKNOWN",
            "oi_signal": "NEUTRAL",
            "call_iv": 0.0,
            "put_iv": 0.0,
            "iv_skew": 0.0,
            "ofr": 1.0,
            "ofr_source": "none",
            "chain_source": "none",
            "chain_age_seconds": 0.0,
            "chain_score": 0,
            "strike_count": 0,
        }
