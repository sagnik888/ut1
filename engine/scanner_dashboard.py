"""Prebuilt dashboard/API snapshots for the scanner."""

from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Dict


class ScannerDashboardCache:
    """Store lightweight snapshots so REST APIs do not rebuild heavy payloads."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._state: Dict[str, Any] = {}
        self._trades: Dict[str, Any] = {"open": [], "closed": [], "signals": [], "summary": {}}
        self._diagnostics: Dict[str, Any] = {}
        self._updated_at = 0.0

    def update(self, results: Dict[str, Any] | None) -> None:
        if not isinstance(results, dict):
            return
        with self._lock:
            self._state = results
            if isinstance(results.get("trades"), dict):
                self._trades = results["trades"]
            if isinstance(results.get("diagnostics"), dict):
                self._diagnostics = results["diagnostics"]
            self._updated_at = time.time()

    def state(self) -> Dict[str, Any]:
        with self._lock:
            return self._state or {}

    def trades(self) -> Dict[str, Any]:
        with self._lock:
            return self._trades or {"open": [], "closed": [], "signals": [], "summary": {}}

    def diagnostics(self) -> Dict[str, Any]:
        with self._lock:
            return self._diagnostics or {}

    def cache_age_ms(self) -> int | None:
        with self._lock:
            if not self._updated_at:
                return None
            return round(max(0.0, time.time() - self._updated_at) * 1000.0)

    def status(self, scanner: Any, connected_clients: int) -> Dict[str, Any]:
        with self._lock:
            state = self._state or {}
            gateway_status = state.get("gateway_status") or {}
            config = state.get("config") or {}
            latency = state.get("latency")
        fyers_auth = {}
        data = getattr(scanner, "data", None)
        if data is not None and hasattr(data, "get_fyers_auth_status"):
            try:
                fyers_auth = data.get_fyers_auth_status()
            except Exception:
                fyers_auth = {}
        last_scan = getattr(scanner, "last_scan_time", None)
        freshness = {}
        if scanner is not None and hasattr(scanner, "_scanner_freshness_payload"):
            try:
                freshness = scanner._scanner_freshness_payload()
            except Exception:
                freshness = {}
        return {
            "status": "running",
            "connected_clients": int(connected_clients),
            "scanner_running": bool(getattr(scanner, "is_running", False)),
            "mode": getattr(scanner, "mode", None),
            "config": config or (
                scanner.get_broadcast_config()
                if scanner is not None and hasattr(scanner, "get_broadcast_config")
                else {}
            ),
            "last_scan": last_scan.isoformat() if last_scan else None,
            "scan_count": int(getattr(scanner, "scan_count", 0) or 0),
            "latency": latency,
            "pid": __import__("os").getpid(),
            "gateway_status": gateway_status,
            "fyers_auth": fyers_auth,
            "cache_age_ms": self.cache_age_ms(),
            "scanner_freshness": freshness,
            "timestamp": datetime.now().isoformat(),
        }
