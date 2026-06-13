"""Process-isolated historical recalculation preparation.

The live scanner owns trading state and should never be pickled into a worker.
This module warms historical candle cache files in a separate Python process and
returns a compact manifest that the scanner can apply quickly.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


def _fetch_days(user_days: int, timeframe: str) -> int:
    fetch_days = max(15, int(user_days or 1) * 3)
    if timeframe == "1min":
        return min(7, fetch_days)
    return min(30, max(20, fetch_days))


def _worker_target(job: Dict[str, Any], out_path: str) -> None:
    started = time.time()
    result: Dict[str, Any] = {
        "status": "ok",
        "started_at": started,
        "finished_at": None,
        "duration_seconds": 0.0,
        "files": [],
        "errors": [],
        "pid": __import__("os").getpid(),
    }
    try:
        from config.settings import get_settings
        from data.market_data import MarketDataProvider

        settings = get_settings()
        provider = MarketDataProvider(
            api_key=settings.angelone_api_key,
            client_id=settings.angelone_client_id,
            password=settings.angelone_password,
            totp_secret=settings.angelone_totp_secret,
            start_streams=False,
        )
        try:
            provider.connect()
        except Exception as exc:
            result["errors"].append(f"connect: {exc}")

        indices = job.get("indices") or {}
        timeframes = job.get("timeframes") or ["1min", "5min", "15min"]
        backtest_days = int(job.get("backtest_days") or 1)
        for name, cfg in indices.items():
            for tf in timeframes:
                try:
                    days_back = _fetch_days(backtest_days, tf)
                    df = provider.get_historical_candles(
                        cfg.get("token", ""),
                        cfg.get("exchange", "NSE"),
                        tf,
                        days_back=days_back,
                        instrument_name=name,
                    )
                    cache_file = Path("data_store/candles") / f"{name}_{tf}.csv"
                    result["files"].append(
                        {
                            "instrument": name,
                            "timeframe": tf,
                            "path": str(cache_file),
                            "rows": int(len(df)) if df is not None else 0,
                            "days_back": days_back,
                            "exists": cache_file.exists(),
                        }
                    )
                except Exception as exc:
                    result["errors"].append(f"{name} {tf}: {exc}")
    except Exception as exc:
        result["status"] = "error"
        result["errors"].append(f"{type(exc).__name__}: {exc}")
        result["traceback"] = traceback.format_exc()
    finally:
        result["finished_at"] = time.time()
        result["duration_seconds"] = round(result["finished_at"] - started, 3)
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        Path(out_path).write_text(json.dumps(result, indent=2), encoding="utf-8")
        # Third-party broker SDKs can leave helper threads alive even after the
        # history job is complete. This worker process owns no app state, so exit
        # explicitly once the manifest is flushed instead of making the parent
        # wait until timeout.
        os._exit(0)


@dataclass
class ProcessRecalculationWorker:
    timeout_seconds: float = 120.0
    output_dir: str = "data_store/recalculation_jobs"

    def run(self, job: Dict[str, Any]) -> Dict[str, Any]:
        out_dir = Path(self.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"recalc_{int(time.time() * 1000)}.json"
        process = mp.Process(target=_worker_target, args=(job, str(out_path)), daemon=True)
        process.start()
        process.join(max(1.0, float(self.timeout_seconds or 120.0)))
        if process.is_alive():
            process.terminate()
            process.join(5)
            return {
                "status": "timeout",
                "pid": process.pid,
                "duration_seconds": self.timeout_seconds,
                "files": [],
                "errors": [f"worker exceeded {self.timeout_seconds:.1f}s"],
            }
        if out_path.exists():
            try:
                return json.loads(out_path.read_text(encoding="utf-8"))
            except Exception as exc:
                return {"status": "error", "files": [], "errors": [f"read result: {exc}"]}
        return {
            "status": "error",
            "pid": process.pid,
            "exitcode": process.exitcode,
            "files": [],
            "errors": ["worker did not write a result file"],
        }


def candle_rows_from_manifest(files: Iterable[Dict[str, Any]]) -> int:
    return sum(int(item.get("rows", 0) or 0) for item in files or [])
