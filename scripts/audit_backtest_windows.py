import argparse
import asyncio
import json
import time
from pathlib import Path
from urllib.parse import quote

import requests
import websockets


BASE_URL = "http://localhost:7000"


def get_json(path):
    response = requests.get(f"{BASE_URL}{path}", timeout=30)
    response.raise_for_status()
    return response.json()


async def configure_days(days):
    client_config = get_json("/api/client_config")
    token = quote(str(client_config.get("dashboard_token") or ""))
    uri = f"ws://localhost:7000/ws?token={token}"
    async with websockets.connect(uri, origin=BASE_URL) as websocket:
        await websocket.send(json.dumps({"cmd": "configure", "backtest_days": days}))
        deadline = time.time() + 30
        while time.time() < deadline:
            message = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
            if message.get("type") == "config_ack":
                if message.get("data", {}).get("status") != "ok":
                    raise RuntimeError(f"Configuration rejected for {days} days: {message}")
                return
        raise TimeoutError(f"No config acknowledgement for {days} days")


def collect_result(days, timeout_seconds):
    deadline = time.time() + timeout_seconds
    stable_key = None
    stable_count = 0
    last_seen = {}

    while time.time() < deadline:
        state = get_json("/api/state")
        diagnostics = state.get("diagnostics") or {}
        simulation = diagnostics.get("simulation") or {}
        trades = get_json("/api/trades")
        closed = trades.get("closed") or []
        summary = trades.get("summary") or {}

        by_type = {}
        for row in closed:
            inst_type = str(row.get("inst_type") or "UNKNOWN")
            bucket = by_type.setdefault(inst_type, {"count": 0, "pnl": 0.0, "wins": 0, "losses": 0})
            pnl = float(row.get("pnl") or 0.0)
            bucket["count"] += 1
            bucket["pnl"] += pnl
            bucket["wins" if pnl > 0 else "losses"] += 1

        last_seen = {
            "days": days,
            "simulation_id": simulation.get("id"),
            "diagnostic_days": simulation.get("backtest_days"),
            "is_calculating": bool(state.get("is_calculating")),
            "scan_count": state.get("scan_count"),
            "closed_rows": len(closed),
            "summary": summary,
            "row_pnl": round(sum(float(row.get("pnl") or 0.0) for row in closed), 2),
            "by_type": {
                key: {
                    **value,
                    "pnl": round(value["pnl"], 2),
                    "win_rate": round(value["wins"] / value["count"] * 100.0, 2) if value["count"] else 0.0,
                }
                for key, value in by_type.items()
            },
            "rejects": diagnostics.get("rejects") or {},
            "instrument_selection": diagnostics.get("instrument_selection") or {},
            "option_history": diagnostics.get("option_history") or {},
            "sources": diagnostics.get("sources") or {},
        }

        key = (
            simulation.get("id"),
            simulation.get("backtest_days"),
            len(closed),
            round(float(summary.get("daily_pnl") or 0.0), 2),
            tuple(sorted((diagnostics.get("rejects") or {}).items())),
        )
        # is_calculating also wraps every short recurring scan, so it is not a
        # reliable full-rebuild completion flag. Stable rows/diagnostics across
        # multiple scans are the authoritative completion signal.
        ready = simulation.get("backtest_days") == days and int(state.get("scan_count") or 0) >= 3
        if ready and key == stable_key:
            stable_count += 1
        elif ready:
            stable_key = key
            stable_count = 1
        else:
            stable_key = None
            stable_count = 0

        if stable_count >= 3:
            return last_seen
        time.sleep(2)

    raise TimeoutError(f"Backtest window {days} did not stabilize: {last_seen}")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", nargs="+", type=int, default=[1, 14, 22, 30])
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--output", default="reports/backtest_window_diagnostics_20260607.json")
    args = parser.parse_args()

    results = []
    for days in args.days:
        await configure_days(days)
        results.append(collect_result(days, args.timeout))
        print(json.dumps(results[-1], indent=2))

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2), encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())
