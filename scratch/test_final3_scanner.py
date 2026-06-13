import sys
from unittest.mock import patch
sys.stdout.reconfigure(encoding='utf-8')
from pathlib import Path
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Target the newly copied repo files
sys.path.insert(0, str(Path("c:/Users/sagnik/Desktop/ut index 2/ut1-index-final3").resolve()))

from config.settings import get_settings
from engine.signal_processor import SignalProcessor
from scanner import Scanner

IST = pytz.timezone('Asia/Kolkata')
LOT_SIZES = {"NIFTY": 65, "BANKNIFTY": 30, "SENSEX": 20, "MIDCPNIFTY": 120}

class MockBroker:
    def get_positions(self):
        return {"data": []}
    def place_order(self, **kwargs):
        return {"data": {"orderid": "MOCK_ORDER_123"}}

def simulate():
    settings = get_settings()
    settings.ut_timeframe_entry_policy = "INCLUDE_5MIN"
    settings.signal_grade_preference = "B"
    settings.ut_dynamic_confidence = True
    settings.ut_regime_adaptation = True
    
    # Initialize components
    from unittest.mock import MagicMock
    from data.market_data import MarketDataProvider
    MarketDataProvider._load_fyers_token = MagicMock()
    MarketDataProvider.start_websocket = MagicMock()
    
    from data.candle_builder import CandleBuilder
    from engine.multi_timeframe import MultiTimeframeEngine
    from engine.signal_manager import SignalManager
    from intelligence.intelligence_aggregator import IntelligenceAggregator
    from trading.trade_manager import TradeManager
    
    LOT_SIZES = {"NIFTY": 65, "BANKNIFTY": 30, "SENSEX": 20, "MIDCPNIFTY": 120}
    data = MarketDataProvider()
    candles = CandleBuilder()
    mtf = MultiTimeframeEngine({
        "key_value": settings.ut_atr_multiplier,
        "atr_period": settings.ut_atr_period,
        "adx_filter": settings.ut_adx_filter,
        "adx_threshold": settings.ut_adx_threshold,
        "strict_adx": settings.ut_strict_adx
    })
    signals = SignalManager()
    intel = IntelligenceAggregator()
    trades = TradeManager(broker=MockBroker())
    
    # Load instrument configs
    instr_config = json.load(open('config/instruments.json'))
    
    class LoggedScanner(Scanner):
        def log_signal_decision_once(self, key, msg, category):
            print(f"  [DECISION LOG] {key} -> {msg}")
            
    scanner = LoggedScanner(data, candles, mtf, signals, intel, trades, None, None)
    scanner.mode = "REAL"  # To use live filters
    scanner.auto_mode = True
    
    # Load all candles
    for name in settings.active_indices:
        for tf in ["1min", "5min", "15min"]:
            path = Path("data_store") / "candles" / f"{name}_{tf}.csv"
            if path.exists():
                df = pd.read_csv(path)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.set_index("timestamp").sort_index()
                # filter for today
                df_today = df[df.index.normalize() == "2026-06-10"]
                if not df_today.empty:
                    scanner.candles.update_candles(name, df_today, tf)
                    scanner.mtf.update_data(name, tf, df_today)
                    
    print("=== RUNNING SIGNAL PROCESSOR SIMULATION (ut1-index-final3) ===")
    
    target_times = [
        # (time, tf)
        ("13:30", "15min"),
        ("13:40", "5min"),
        ("13:45", "15min"),
        ("14:00", "15min"),
        ("14:30", "5min"),
        ("14:35", "5min"),
    ]
    
    # Pre-process multi timeframe results
    config = {"indices": {name: {} for name in settings.active_indices}}
    mtf_results = scanner.mtf.process_all(config, 100000, 1.0, {name: 1 for name in settings.active_indices})
    
    sp = SignalProcessor(scanner)
    
    for name in settings.active_indices:
        lot_size = LOT_SIZES[name]
        cfg = instr_config["indices"].get(name, {})
        strike_interval = cfg.get("strike_interval", 100)
        
        for t_str, tf in target_times:
            # Let's see if there is a signal
            res = mtf_results.get(name)
            if not res: continue
            
            # Find spot price at that time from 1min candles
            df_1m = scanner.candles.get_candles(name, "1min")
            if df_1m is None or df_1m.empty:
                continue
                
            t_dt = pd.Timestamp(f"2026-06-10 {t_str}:00")
            df_1m_cut = df_1m[df_1m.index <= t_dt]
            if df_1m_cut.empty:
                continue
            spot = float(df_1m_cut["close"].iloc[-1])
            atm_strike = round(spot / strike_interval) * strike_interval if spot > 0 else 0
            
            # Compute intelligence
            df_5m = scanner.candles.get_candles(name, "5min")
            df_5m_cut = df_5m[df_5m.index <= t_dt] if df_5m is not None else pd.DataFrame()
            
            try:
                intel_result = scanner.intel.analyze(name, "5min", df_5m_cut)
            except Exception as e:
                intel_result = {
                    "pcr": {"pcr": 1.0, "signal": "NEUTRAL"},
                    "oi": {"signal": "NEUTRAL", "cumulative_analysis": {}},
                    "greeks": {},
                    "volume": {"buy_sell_ratio": 1.0},
                    "order_flow": {"ratio": 1.0},
                    "regime": {"regime": "UNKNOWN"},
                    "aggregate": {"score": 0.0}
                }
            
            from engine.signal_processor import normalize_intelligence_score
            intel_score = normalize_intelligence_score(
                intel_result.get("aggregate", {}).get("score", 0.0)
            )
            regime = intel_result.get("regime", {}).get("regime", "UNKNOWN")
            
            # Reconstruct mtf_result representing ONLY this timestamp cut
            tf_res = getattr(res, f"results_{tf}")
            if not tf_res: continue
            
            sigs = tf_res.get("signals", [])
            matching_sig = None
            for sig in sigs:
                if sig.timestamp.date() == datetime(2026,6,10).date() and sig.timestamp.strftime("%H:%M") == t_str:
                    matching_sig = sig
                    break
                    
            if matching_sig:
                print(f"\nEvaluating: {name} {tf} @ {t_str} {matching_sig.signal_type} @ {matching_sig.price:.2f}")
                
                # Mock scanner active charts and data
                scanner.active_chart_instrument = name
                scanner.active_chart_tf = tf
                
                # We need to construct a custom mtf_result containing only signals up to this timestamp
                from types import SimpleNamespace
                filtered_res = {}
                for k in ["results_5min", "results_15min"]:
                    v = getattr(res, k)
                    if v:
                        tf_sigs = v.get("signals", [])
                        filtered_sigs = [s for s in tf_sigs if s.timestamp <= t_dt]
                        filtered_res[k] = {"signals": filtered_sigs, "state": v.get("state", {})}
                    else:
                        filtered_res[k] = None
                
                mock_mtf_result = SimpleNamespace(
                    results_5min=filtered_res.get("results_5min"),
                    results_15min=filtered_res.get("results_15min"),
                    confluence_score=res.confluence_score if hasattr(res, "confluence_score") else 0.0
                )
                
                # Run process_best_signal
                with patch('engine.signal_processor.datetime') as mock_dt:
                    mock_dt.now.return_value = t_dt.tz_localize(IST) + timedelta(minutes=5)
                    mock_dt.min = datetime.min
                    mock_dt.combine = datetime.combine
                    mock_dt.strptime = datetime.strptime
                    mock_dt.fromisoformat = datetime.fromisoformat
                    print(f"15min sigs before sp: {[s.timestamp for s in mock_mtf_result.results_15min.get('signals', [])]}")
                
                candidates = sp.process_best_signal(
                    name, mock_mtf_result, intel_result, intel_score, regime,
                    1, lot_size, cfg, spot, atm_strike
                )
                if candidates:
                    print(f"  Result: ACCEPTED {len(candidates)} candidates: {[c.instrument + ' ' + c.direction for c in candidates]}")
                else:
                    print(f"  Result: REJECTED")

if __name__ == "__main__":
    simulate()
