"""Production safety checks for REAL mode startup and switching."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pytz


IST = pytz.timezone("Asia/Kolkata")


def validate_real_mode_readiness(settings: Any, data_provider: Any = None, now: datetime | None = None) -> Dict[str, Any]:
    """Return a structured readiness decision before REAL mode is allowed."""
    now = now or datetime.now(IST)
    if now.tzinfo is None:
        now = IST.localize(now)

    blockers: List[str] = []
    warnings: List[str] = []

    env_path = Path(".env")
    if not env_path.exists():
        blockers.append(".env missing")

    required = {
        "angelone_api_key": getattr(settings, "angelone_api_key", ""),
        "angelone_client_id": getattr(settings, "angelone_client_id", ""),
        "angelone_password": getattr(settings, "angelone_password", ""),
        "angelone_totp_secret": getattr(settings, "angelone_totp_secret", ""),
    }
    for key, value in required.items():
        if not str(value or "").strip():
            blockers.append(f"{key} missing")

    if data_provider is not None:
        if not bool(getattr(data_provider, "is_connected", False)):
            blockers.append("AngelOne broker not connected")
        try:
            health = data_provider.get_source_health()
            if health.get("all_brokers_unavailable"):
                blockers.append("all broker sources unavailable")
            if health.get("broker_degraded"):
                warnings.append("broker source degraded")
        except Exception as exc:
            warnings.append(f"source health unavailable: {exc}")

        try:
            if data_provider.is_market_holiday(now):
                blockers.append("market holiday")
        except Exception as exc:
            warnings.append(f"holiday calendar check unavailable: {exc}")

    if now.weekday() >= 5:
        blockers.append("weekend session")

    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if not (start <= now <= end):
        blockers.append("outside regular market hours")

    return {
        "ok": not blockers,
        "blockers": blockers,
        "warnings": warnings,
        "checked_at": now.isoformat(),
    }
