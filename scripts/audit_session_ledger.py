from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple


def _load_rows(path: Path) -> list[Dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return list(payload.get("candidates") or [])


def _key(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (
        str(row.get("instrument") or ""),
        str(row.get("timeframe") or ""),
        str(row.get("direction") or ""),
        str(row.get("timestamp") or row.get("signal_timestamp") or ""),
    )


def _price(row: Dict[str, Any], *keys: str) -> float:
    for key in keys:
        try:
            value = float(row.get(key) or 0.0)
            if value > 0:
                return value
        except Exception:
            continue
    return 0.0


def audit_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = list(rows)
    entries = [r for r in rows if str(r.get("action") or "ENTRY").upper() == "ENTRY"]
    exits = [r for r in rows if str(r.get("action") or "").upper() == "EXIT"]
    exit_by_key = {_key(r): r for r in exits}

    unmatched_exits = [r for r in exits if _key(r) not in {_key(e) for e in entries}]
    active_entries = [r for r in entries if _key(r) not in exit_by_key]

    clamp_mismatches = []
    for row in exits:
        reason = str(row.get("exit_reason") or "").upper()
        exit_px = _price(row, "exit_price", "current_price", "price")
        if reason == "STOP_HIT":
            expected = _price(row, "stop")
        elif reason == "TARGET_HIT":
            expected = _price(row, "target")
        else:
            continue
        if expected > 0 and abs(exit_px - expected) > 0.05:
            clamp_mismatches.append({
                "instrument": row.get("instrument"),
                "timeframe": row.get("timeframe"),
                "direction": row.get("direction"),
                "timestamp": row.get("timestamp"),
                "reason": reason,
                "exit_price": exit_px,
                "expected": expected,
            })

    by_instrument: dict[str, dict[str, Any]] = defaultdict(lambda: {"entries": 0, "exits": 0, "pnl": 0.0})
    for row in entries:
        by_instrument[str(row.get("instrument") or "--")]["entries"] += 1
    for row in exits:
        bucket = by_instrument[str(row.get("instrument") or "--")]
        bucket["exits"] += 1
        bucket["pnl"] += float(row.get("pnl") or 0.0)

    return {
        "rows": len(rows),
        "entries": len(entries),
        "exits": len(exits),
        "active_entries": len(active_entries),
        "unmatched_exits": len(unmatched_exits),
        "stop_target_clamp_mismatches": len(clamp_mismatches),
        "clamp_mismatch_rows": clamp_mismatches[:20],
        "by_instrument": {
            key: {**value, "pnl": round(value["pnl"], 2)}
            for key, value in sorted(by_instrument.items())
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit persisted session signal ledger consistency.")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"), help="Session date YYYY-MM-DD")
    parser.add_argument("--path", default="", help="Explicit session candidate JSON path")
    args = parser.parse_args()

    path = Path(args.path) if args.path else Path("data_store") / "session_candidates" / f"{args.date}.json"
    result = {"path": str(path), **audit_rows(_load_rows(path))}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
