from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import get_settings
from engine.signal_manager import SignalManager
from engine.signal_processor import SignalProcessor
from engine.ut_bot_core import UTBotEngine
from intelligence.regime_detector import RegimeDetector


LOT_SIZES = {"NIFTY": 65, "BANKNIFTY": 30, "SENSEX": 20, "MIDCPNIFTY": 120}
TF_MINUTES = {"1min": 1, "5min": 5, "15min": 15}


@dataclass
class Decision:
    instrument: str
    timeframe: str
    time: str
    signal: str
    price: float
    grade: str
    confidence: float
    adx: float
    regime: str
    confluence: float
    direction_15m: int
    current: str
    strict: str
    max_relaxed: str
    reason: str


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


def pos_at(result: dict, df: pd.DataFrame, ts: pd.Timestamp) -> tuple[int, bool]:
    if result is None or df is None or df.empty:
        return 0, False
    idx = df.index.searchsorted(ts, side="right") - 1
    if idx < 0:
        return 0, False
    idx = min(idx, len(result.get("positions", [])) - 1)
    if idx < 0:
        return 0, False
    pos = int(result["positions"][idx])
    adx_arr = result.get("adx", [])
    adx_val = float(adx_arr[idx]) if idx < len(adx_arr) and pd.notna(adx_arr[idx]) else 0.0
    return pos, adx_val > get_settings().ut_adx_threshold


def confluence_at(results: dict, candles: dict, instrument: str, ts: pd.Timestamp) -> tuple[float, str, int]:
    weights = {"1min": 0.15, "5min": 0.40, "15min": 0.45}
    total = 0.0
    score = 0.0
    pos15 = 0
    for tf, weight in weights.items():
        pos, trending = pos_at(results.get(tf), candles.get(tf), ts)
        if tf == "15min":
            pos15 = pos
        eff = weight * (1.3 if trending else 0.7)
        score += pos * eff
        total += eff
    score = round(score / total, 3) if total else 0.0
    label = "BUY" if score > 0.35 else ("SELL" if score < -0.35 else "HOLD")
    return score, label, pos15


def grade_rank(grade: str) -> int:
    return {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4}.get(str(grade or "C").split()[0], 0)


def is_choppy(regime: str) -> bool:
    return str(regime or "UNKNOWN").upper() in {
        "CHOPPY",
        "SIDEWAYS",
        "MEAN_REVERTING",
        "RANGING",
        "RANGEBOUND",
        "VOLATILE",
        "UNKNOWN",
    }


def relaxed(value: float, pct: float) -> float:
    return float(value) * (1.0 - min(max(float(pct), 0.0), 0.20))


def impulse_reversal(sig, df_5m: pd.DataFrame, confidence: float, pct: float, choppy_gate: float) -> bool:
    candle = getattr(sig, "raw_candle", {}) or {}
    atr = max(float(getattr(sig, "atr_value", 0.0) or 0.0), 1e-9)
    open_px = float(candle.get("open") or 0.0)
    high_px = float(candle.get("high") or 0.0)
    low_px = float(candle.get("low") or 0.0)
    close_px = float(candle.get("close") or getattr(sig, "price", 0.0) or 0.0)
    body_atr = abs(close_px - open_px) / atr
    range_atr = abs(high_px - low_px) / atr
    close_near_extreme = (
        close_px >= high_px - (0.25 * atr)
        if sig.signal_type == "BUY"
        else close_px <= low_px + (0.25 * atr)
    )
    strong_adx = float(sig.adx_value or 0.0) >= 28.0 and confidence >= relaxed(choppy_gate, pct)
    impulse_body = body_atr >= 0.80 and range_atr >= 1.05 and close_near_extreme
    extreme_range = range_atr >= 1.45 and close_near_extreme and confidence >= relaxed(0.78, pct)

    volume_confirmed = False
    idx = int(getattr(sig, "bar_index", -1) or -1)
    if 0 <= idx < len(df_5m):
        current_vol = float(df_5m.iloc[idx].get("volume", 0.0) or 0.0)
        lookback = df_5m.iloc[max(0, idx - 12):idx]
        vols = [float(v) for v in lookback.get("volume", pd.Series(dtype=float)).tolist() if float(v or 0) > 0]
        avg_vol = sum(vols) / len(vols) if vols else 0.0
        volume_confirmed = avg_vol > 0 and current_vol >= (1.35 * avg_vol)

    return strong_adx or ((impulse_body or extreme_range) and (volume_confirmed or confidence >= relaxed(0.82, pct)))


def decide(sig, tf: str, grade: str, conf: float, regime: str, pos15: int, df_5m: pd.DataFrame, pct: float) -> tuple[str, str]:
    settings = get_settings()
    rank = grade_rank(grade)
    choppy_gate = float(getattr(settings, "live_choppy_gate_confidence", 0.84) or 0.84)

    if rank <= 0:
        return "REJECT", "C grade blocked"
    if conf < 0.55:
        return "REJECT", "below 55% confidence gate"

    if tf == "5min" and is_choppy(regime):
        direction_pos = 1 if sig.signal_type == "BUY" else -1
        if pos15 != direction_pos and not impulse_reversal(sig, df_5m, conf, pct, choppy_gate):
            return "REJECT", "5m choppy gate lacks 15m agreement or impulse"

    policy = str(getattr(settings, "ut_timeframe_entry_policy", "PRIMARY_15")).upper()
    if policy == "PRIMARY_15" and tf == "5min":
        return "REJECT", "5m blocked by timeframe policy"

    if tf == "5min":
        min_conf = relaxed(float(getattr(settings, "ut_5min_option_min_confidence", 0.90) or 0.90), pct)
        if rank < 3 and conf < min_conf:
            return "REJECT", f"5m needs A grade or {min_conf:.0%} confidence"
        if is_choppy(regime) and not ((rank >= 3 and conf >= relaxed(0.72, pct)) or conf >= min_conf):
            return "REJECT", "5m choppy quality gate"

    if is_choppy(regime) and rank < 3 and conf < relaxed(choppy_gate, pct):
        return "REJECT", f"choppy gate below {relaxed(choppy_gate, pct):.0%}"

    return "ACCEPT", "passed replay gates"


def main() -> None:
    settings = get_settings()
    params = ut_params(settings)
    today = date.today()
    signal_manager = SignalManager()
    regime_detector = RegimeDetector()

    decisions: list[Decision] = []
    raw_counts = defaultdict(dict)

    for instrument in settings.active_indices:
        candles = {tf: load_candles(instrument, tf) for tf in ["1min", "5min", "15min"]}
        results = {}
        for tf, df in candles.items():
            engine = UTBotEngine(**params)
            results[tf] = engine.process(
                df=df,
                instrument=instrument,
                timeframe=tf,
                capital=settings.capital_fut,
                risk_pct=settings.risk_fut_pct,
                lots=1,
                lot_size=LOT_SIZES[instrument],
            )
        for tf in ["5min", "15min"]:
            today_sigs = [s for s in results[tf].get("signals", []) if s.timestamp.date() == today]
            raw_counts[instrument][tf] = len(today_sigs)
            for sig in today_sigs:
                sig_ts = pd.Timestamp(sig.timestamp)
                df5_cut = candles["5min"][candles["5min"].index <= sig_ts]
                regime = regime_detector.detect(df5_cut, instrument, "5min").get("regime", "UNKNOWN")
                confluence, _, pos15 = confluence_at(results, candles, instrument, sig_ts)
                grade, conf, _ = signal_manager._grade_signal(sig, confluence, 0.0, regime)
                current_pct = float(getattr(settings, "live_filter_leniency_pct", 0.0) or 0.0)
                current, reason = decide(sig, tf, grade, conf, regime, pos15, candles["5min"], current_pct)
                strict, _ = decide(sig, tf, grade, conf, regime, pos15, candles["5min"], 0.0)
                max_relaxed, _ = decide(sig, tf, grade, conf, regime, pos15, candles["5min"], 0.20)
                decisions.append(
                    Decision(
                        instrument=instrument,
                        timeframe=tf,
                        time=sig.timestamp.strftime("%H:%M"),
                        signal=sig.signal_type,
                        price=round(float(sig.price), 2),
                        grade=grade,
                        confidence=round(float(conf), 3),
                        adx=round(float(sig.adx_value), 1),
                        regime=regime,
                        confluence=confluence,
                        direction_15m=pos15,
                        current=current,
                        strict=strict,
                        max_relaxed=max_relaxed,
                        reason=reason,
                    )
                )

    current_counts = Counter(d.current for d in decisions)
    strict_counts = Counter(d.strict for d in decisions)
    max_relaxed_counts = Counter(d.max_relaxed for d in decisions)
    reject_reasons = Counter(d.reason for d in decisions if d.current == "REJECT")

    accepted = [d.__dict__ for d in decisions if d.current == "ACCEPT"]
    marginal = [
        d.__dict__
        for d in decisions
        if d.current == "REJECT" and d.max_relaxed == "ACCEPT"
    ]
    nearest = sorted(
        [d for d in decisions if d.current == "REJECT"],
        key=lambda d: (grade_rank(d.grade), d.confidence, d.adx),
        reverse=True,
    )[:12]

    out = {
        "date": str(today),
        "settings": {
            "live_filter_leniency_pct": settings.live_filter_leniency_pct,
            "live_choppy_gate_confidence": settings.live_choppy_gate_confidence,
            "ut_5min_option_min_confidence": settings.ut_5min_option_min_confidence,
            "ut_timeframe_entry_policy": settings.ut_timeframe_entry_policy,
            "signal_grade_preference": settings.signal_grade_preference,
        },
        "raw_signal_counts": raw_counts,
        "decision_counts": {
            "strict_0pct": strict_counts,
            f"current_{int(round(float(getattr(settings, 'live_filter_leniency_pct', 0.0) or 0.0) * 100))}pct": current_counts,
            "max_20pct": max_relaxed_counts,
        },
        "reject_reasons_current": reject_reasons,
        "accepted_current": accepted,
        "would_pass_only_at_20pct": marginal,
        "nearest_rejected_current": [d.__dict__ for d in nearest],
    }
    print(json.dumps(out, indent=2, default=dict))


if __name__ == "__main__":
    main()
