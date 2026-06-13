from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date
import json
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import get_settings
from engine.ut_bot_core import UTBotEngine


TF_MINUTES = {"1min": 1, "5min": 5, "15min": 15}


@dataclass
class WindowSummary:
    days: int
    trading_dates: list[str]
    trades: int
    wins: int
    losses: int
    win_rate: float
    gross_pnl: float
    charges: float
    net_pnl: float
    max_drawdown: float
    avg_trade: float
    by_instrument: dict
    by_timeframe: dict


def load_instruments() -> dict:
    payload = json.loads(Path("config/instruments.json").read_text(encoding="utf-8"))
    return payload.get("indices", {})


def load_candles(instrument: str, timeframe: str) -> pd.DataFrame:
    path = Path("data_store") / "candles" / f"{instrument}_{timeframe}.csv"
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.set_index("timestamp").sort_index()


def ut_params(settings) -> dict:
    return {
        "key_value": settings.ut_atr_multiplier,
        "atr_period": settings.ut_atr_period,
        "use_heikin_ashi": settings.ut_use_heikin_ashi,
        "signal_mode": settings.ut_signal_mode,
        "adx_filter": settings.ut_adx_filter,
        "adx_period": settings.ut_adx_period,
        "adx_threshold": settings.ut_adx_threshold,
        "strict_adx": settings.ut_strict_adx,
        "session_filter": settings.ut_session_filter,
        "session_start": settings.ut_session_start,
        "session_end": settings.ut_session_end,
    }


def compute_signals(instrument: str, timeframe: str, lot_size: int, settings) -> tuple[pd.DataFrame, list]:
    df = load_candles(instrument, timeframe)
    engine = UTBotEngine(**ut_params(settings))
    result = engine.process(
        df=df,
        instrument=instrument,
        timeframe=timeframe,
        capital=settings.capital_fut,
        risk_pct=settings.risk_fut_pct,
        lots=1,
        lot_size=lot_size,
    )
    return df, list(result.get("signals") or [])


def capped_fut_stop(signal, direction: str, settings) -> float:
    entry = float(signal.price)
    trail = float(signal.trailing_stop)
    max_dist = entry * (float(settings.futures_sl_pct) / 100.0)
    if direction == "LONG":
        return max(trail, entry - max_dist)
    return min(trail, entry + max_dist)


def find_exit(
    signal,
    next_signal,
    df_1m: pd.DataFrame,
    df_tf: pd.DataFrame,
    direction: str,
    stop: float,
) -> tuple[pd.Timestamp, float, str]:
    entry_ts = pd.Timestamp(signal.timestamp)
    if next_signal is not None:
        planned_exit_ts = pd.Timestamp(next_signal.timestamp)
        planned_exit_px = float(next_signal.price)
    else:
        planned_exit_ts = df_tf.index[-1]
        planned_exit_px = float(df_tf["close"].iloc[-1])

    window = df_1m.loc[(df_1m.index >= entry_ts) & (df_1m.index <= planned_exit_ts)]
    if not window.empty:
        if direction == "LONG":
            hit = window[window["low"].astype(float) <= stop]
        else:
            hit = window[window["high"].astype(float) >= stop]
        if not hit.empty:
            return pd.Timestamp(hit.index[0]), float(stop), "SL_HIT"

    return planned_exit_ts, planned_exit_px, "NEXT_SIGNAL" if next_signal is not None else "WINDOW_CLOSE"


def max_drawdown(equity: list[float]) -> float:
    peak = 0.0
    max_dd = 0.0
    for value in equity:
        peak = max(peak, value)
        max_dd = max(max_dd, peak - value)
    return round(max_dd, 2)


def summarize_trades(trades: list[dict], days: int, trading_dates: list[date]) -> WindowSummary:
    wins = sum(1 for t in trades if t["net_pnl"] > 0)
    losses = sum(1 for t in trades if t["net_pnl"] <= 0)
    gross = round(sum(t["gross_pnl"] for t in trades), 2)
    charges = round(sum(t["charges"] for t in trades), 2)
    net = round(sum(t["net_pnl"] for t in trades), 2)
    equity = []
    running = 0.0
    for trade in sorted(trades, key=lambda t: t["exit_ts"]):
        running += trade["net_pnl"]
        equity.append(running)

    by_inst = defaultdict(lambda: {"trades": 0, "net_pnl": 0.0, "wins": 0})
    by_tf = defaultdict(lambda: {"trades": 0, "net_pnl": 0.0, "wins": 0})
    for trade in trades:
        for bucket in (by_inst[trade["instrument"]], by_tf[trade["timeframe"]]):
            bucket["trades"] += 1
            bucket["net_pnl"] += trade["net_pnl"]
            if trade["net_pnl"] > 0:
                bucket["wins"] += 1

    def finish(bucket: dict) -> dict:
        out = {}
        for key, value in sorted(bucket.items()):
            out[key] = {
                "trades": value["trades"],
                "wins": value["wins"],
                "win_rate": round((value["wins"] / value["trades"] * 100.0) if value["trades"] else 0.0, 2),
                "net_pnl": round(value["net_pnl"], 2),
            }
        return out

    return WindowSummary(
        days=days,
        trading_dates=[d.isoformat() for d in trading_dates],
        trades=len(trades),
        wins=wins,
        losses=losses,
        win_rate=round((wins / len(trades) * 100.0) if trades else 0.0, 2),
        gross_pnl=gross,
        charges=charges,
        net_pnl=net,
        max_drawdown=max_drawdown(equity),
        avg_trade=round((net / len(trades)) if trades else 0.0, 2),
        by_instrument=finish(by_inst),
        by_timeframe=finish(by_tf),
    )


def main() -> None:
    settings = get_settings()
    instruments = load_instruments()
    active = [i for i in settings.active_indices if i in instruments]

    # Build one shared set of available trading dates from active index 5m candles.
    all_dates = set()
    for instrument in active:
        df = load_candles(instrument, "5min")
        all_dates.update(df.index.date)
    trading_dates = sorted(all_dates)

    signal_book = {}
    for instrument in active:
        lot_size = int(instruments[instrument]["lot_size"])
        signal_book[instrument] = {
            "1min": load_candles(instrument, "1min"),
            "5min": compute_signals(instrument, "5min", lot_size, settings),
            "15min": compute_signals(instrument, "15min", lot_size, settings),
            "lot_size": lot_size,
            "lots": int(getattr(settings, "lots_fut", {}).get(instrument, 1) if hasattr(settings, "lots_fut") else 1),
        }

    summaries = []
    detail_counts = {}
    for days in [7, 14, 21, 28]:
        selected_dates = trading_dates[-min(days, len(trading_dates)):]
        selected = set(selected_dates)
        trades = []
        for instrument, book in signal_book.items():
            lot_size = int(book["lot_size"])
            lots = int(book["lots"])
            qty = lot_size * lots
            for tf in ["5min", "15min"]:
                df_tf, signals = book[tf]
                for idx, signal in enumerate(signals):
                    if pd.Timestamp(signal.timestamp).date() not in selected:
                        continue
                    direction = "LONG" if signal.signal_type == "BUY" else "SHORT"
                    next_signal = signals[idx + 1] if idx + 1 < len(signals) else None
                    stop = capped_fut_stop(signal, direction, settings)
                    exit_ts, exit_px, reason = find_exit(
                        signal,
                        next_signal,
                        book["1min"],
                        df_tf,
                        direction,
                        stop,
                    )
                    entry = float(signal.price)
                    gross = (exit_px - entry) * qty if direction == "LONG" else (entry - exit_px) * qty
                    charges = 200.0
                    trades.append(
                        {
                            "instrument": instrument,
                            "timeframe": tf,
                            "direction": direction,
                            "entry_ts": pd.Timestamp(signal.timestamp),
                            "exit_ts": exit_ts,
                            "entry": entry,
                            "exit": exit_px,
                            "reason": reason,
                            "gross_pnl": round(gross, 2),
                            "charges": charges,
                            "net_pnl": round(gross - charges, 2),
                        }
                    )
        summary = summarize_trades(trades, days, selected_dates)
        summaries.append(asdict(summary))
        detail_counts[str(days)] = len(trades)

    print(json.dumps({
        "model": "FUT-only raw UTBot replay; all 5m/15m signals pass; no grade/conf/choppy/concurrency/no-entry filters; exit on next opposite UTBot signal or 1m stop hit first.",
        "settings": {
            "futures_sl_pct": settings.futures_sl_pct,
            "charge_per_trade": 200.0,
            "active_indices": active,
        },
        "summaries": summaries,
    }, indent=2))


if __name__ == "__main__":
    main()
