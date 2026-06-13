from datetime import date
import json
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import get_settings
from engine.ut_bot_core import UTBotEngine


LOT_SIZES = {"NIFTY": 65, "BANKNIFTY": 30, "SENSEX": 20, "MIDCPNIFTY": 120}


def load_candles(instrument: str, timeframe: str) -> pd.DataFrame:
    path = Path("data_store") / "candles" / f"{instrument}_{timeframe}.csv"
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp").sort_index()
    return df


def main() -> None:
    settings = get_settings()
    params = {
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
    today = date.today()
    out = {"settings": params, "signals": {}}
    for instrument in settings.active_indices:
        out["signals"][instrument] = {}
        for tf in ["5min", "15min"]:
            df = load_candles(instrument, tf)
            engine = UTBotEngine(**params)
            res = engine.process(
                df=df,
                instrument=instrument,
                timeframe=tf,
                capital=settings.capital_fut,
                risk_pct=settings.risk_fut_pct,
                lots=1,
                lot_size=LOT_SIZES[instrument],
            )
            rows = []
            for sig in res.get("signals", []):
                if sig.timestamp.date() != today:
                    continue
                candle_close = sig.timestamp + pd.Timedelta(minutes=5 if tf == "5min" else 15)
                if candle_close.time() < pd.Timestamp("09:18").time():
                    continue
                rows.append(
                    {
                        "time": sig.timestamp.strftime("%H:%M"),
                        "type": sig.signal_type,
                        "price": round(float(sig.price), 2),
                        "trail": round(float(sig.trailing_stop), 2),
                        "adx": round(float(sig.adx_value), 1),
                        "atr": round(float(sig.atr_value), 2),
                        "bar": int(sig.bar_index),
                    }
                )
            out["signals"][instrument][tf] = rows
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
