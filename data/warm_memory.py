"""
Rolling warm-memory checkpoint for fast live restart recovery.

This is intentionally short lived: it keeps only the latest live-session
runtime context inside a strict rolling TTL and never becomes a historical DB.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import pandas as pd
from loguru import logger

from trading.trade_manager import IST


class WarmMemoryStore:
    def __init__(
        self,
        path: str | Path = "data_store/warm_memory/live_warm_memory.json",
        ttl_minutes: int = 15,
        save_interval_seconds: float = 15.0,
        clock: Optional[Callable[[], datetime]] = None,
    ):
        self.path = Path(path)
        self.ttl = timedelta(minutes=int(ttl_minutes))
        self.save_interval_seconds = float(save_interval_seconds)
        self.clock = clock or (lambda: datetime.now(IST))
        self._last_save_monotonic = 0.0

    def should_save(self) -> bool:
        return (time.monotonic() - self._last_save_monotonic) >= self.save_interval_seconds

    def save(self, payload: Dict[str, Any], force: bool = False) -> bool:
        if not force and not self.should_save():
            return False
        now = self._now()
        pruned = self._prune_payload(dict(payload or {}), now)
        pruned.update({
            "version": 1,
            "saved_at": now.isoformat(),
            "ttl_minutes": int(self.ttl.total_seconds() // 60),
        })
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix="warm_memory_", suffix=".json", dir=str(self.path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(pruned, fh, separators=(",", ":"), ensure_ascii=True)
            os.replace(tmp_name, self.path)
            self._last_save_monotonic = time.monotonic()
            return True
        except Exception as exc:
            try:
                os.unlink(tmp_name)
            except Exception:
                pass
            logger.debug(f"Warm memory save skipped: {exc}")
            return False

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning(f"Warm memory read failed; ignoring checkpoint: {exc}")
            return {}
        now = self._now()
        saved_at = self._parse_dt(payload.get("saved_at"))
        if not saved_at or now - saved_at > self.ttl:
            self.clear()
            return {}
        return self._prune_payload(payload, now)

    def clear(self) -> None:
        try:
            self.path.unlink(missing_ok=True)
        except Exception:
            pass

    def _now(self) -> datetime:
        now = self.clock()
        if getattr(now, "tzinfo", None) is None:
            return IST.localize(now)
        return now.astimezone(IST)

    def _cutoff(self, now: datetime) -> datetime:
        return now - self.ttl

    def _parse_dt(self, raw: Any) -> Optional[datetime]:
        if not raw:
            return None
        try:
            dt = datetime.fromisoformat(str(raw))
            if getattr(dt, "tzinfo", None) is None:
                return IST.localize(dt)
            return dt.astimezone(IST)
        except Exception:
            return None

    def _prune_payload(self, payload: Dict[str, Any], now: datetime) -> Dict[str, Any]:
        cutoff = self._cutoff(now)
        payload["candles"] = self._prune_candles(payload.get("candles") or {}, cutoff)
        payload["session_candidates"] = self._prune_candidates(payload.get("session_candidates") or {}, cutoff)
        payload["pending_live_signals"] = self._prune_pending_signals(
            payload.get("pending_live_signals") or [],
            cutoff,
        )
        payload["latest_results"] = self._prune_latest_results(payload.get("latest_results") or {}, cutoff)
        return payload

    def _row_date_allowed(self, raw: Any, cutoff: datetime) -> bool:
        dt = self._parse_dt(raw)
        return bool(dt and dt >= cutoff)

    def _is_recovery_candidate(self, row: Dict[str, Any]) -> bool:
        action = str(row.get("action") or row.get("status") or "").upper()
        return bool(row.get("accepted_by_gate")) or action == "EXIT"

    def _prune_candidates(self, candidates: Dict[str, Any], cutoff: datetime) -> Dict[str, Any]:
        pruned: Dict[str, Any] = {}
        for instrument, book in (candidates or {}).items():
            rows = []
            if isinstance(book, dict):
                rows = list(book.values())
            elif isinstance(book, list):
                rows = list(book)
            kept = [
                row for row in rows
                if isinstance(row, dict)
                and not str(row.get("id") or row.get("trade_id") or "").startswith(("H_", "EOD_"))
                and (
                    self._is_recovery_candidate(row)
                    or self._row_date_allowed(row.get("timestamp") or row.get("signal_timestamp"), cutoff)
                )
            ]
            if kept:
                pruned[instrument] = kept[-150:]
        return pruned

    def _prune_pending_signals(self, pending: Any, cutoff: datetime) -> list[Dict[str, Any]]:
        return [
            item for item in (pending or [])
            if isinstance(item, dict)
            and isinstance(item.get("candidate"), dict)
            and self._row_date_allowed(item.get("buffered_at"), cutoff)
            and self._row_date_allowed(
                item.get("sig_timestamp")
                or item["candidate"].get("signal_timestamp"),
                cutoff,
            )
        ]

    def _prune_latest_results(self, latest: Dict[str, Any], cutoff: datetime) -> Dict[str, Any]:
        if not isinstance(latest, dict):
            return {}
        ts = latest.get("timestamp")
        if ts and not self._row_date_allowed(ts, cutoff):
            return {}
        slim = {
            k: v for k, v in latest.items()
            if k not in {"activity_log"} and k != "instruments"
        }
        instruments = {}
        for name, data in (latest.get("instruments") or {}).items():
            if not isinstance(data, dict):
                continue
            row = {k: v for k, v in data.items() if k != "chart"}
            chart = data.get("chart") or {}
            if isinstance(chart, dict):
                compact_chart = {}
                for tf, tf_chart in chart.items():
                    if not isinstance(tf_chart, dict):
                        continue
                    compact_chart[tf] = {
                        "state": tf_chart.get("state"),
                        "candles": (tf_chart.get("candles") or [])[-3:],
                        "markers": (tf_chart.get("markers") or [])[-10:],
                    }
                row["chart"] = compact_chart
            instruments[name] = row
        slim["instruments"] = instruments
        return slim

    def _prune_candles(self, candles: Dict[str, Any], cutoff: datetime) -> Dict[str, Any]:
        pruned: Dict[str, Any] = {}
        for instrument, tf_map in (candles or {}).items():
            if not isinstance(tf_map, dict):
                continue
            out_tf = {}
            for tf, rows in tf_map.items():
                kept = [
                    row for row in (rows or [])
                    if isinstance(row, dict) and self._row_date_allowed(row.get("timestamp"), cutoff)
                ]
                if kept:
                    out_tf[tf] = kept
            if out_tf:
                pruned[instrument] = out_tf
        return pruned


def dataframe_to_records(df: Optional[pd.DataFrame], cutoff: datetime) -> list[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    work = df.copy()
    if work.index.tz is not None:
        work.index = work.index.tz_convert(IST).tz_localize(None)
    cutoff_naive = cutoff.astimezone(IST).replace(tzinfo=None)
    work = work[work.index >= cutoff_naive]
    records = []
    for ts, row in work.iterrows():
        records.append({
            "timestamp": pd.Timestamp(ts).isoformat(),
            "open": float(row.get("open", 0.0) or 0.0),
            "high": float(row.get("high", 0.0) or 0.0),
            "low": float(row.get("low", 0.0) or 0.0),
            "close": float(row.get("close", 0.0) or 0.0),
            "volume": float(row.get("volume", 0.0) or 0.0),
        })
    return records


def records_to_dataframe(records: list[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    index = []
    for item in records or []:
        try:
            ts = datetime.fromisoformat(str(item.get("timestamp")))
            if getattr(ts, "tzinfo", None) is not None:
                ts = ts.astimezone(IST).replace(tzinfo=None)
            index.append(ts)
            rows.append({
                "open": float(item.get("open") or 0.0),
                "high": float(item.get("high") or 0.0),
                "low": float(item.get("low") or 0.0),
                "close": float(item.get("close") or 0.0),
                "volume": float(item.get("volume") or 0.0),
            })
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, index=pd.DatetimeIndex(index)).sort_index()
