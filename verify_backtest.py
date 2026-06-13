import os
import sys
sys.path.append('.')
import pandas as pd
import numpy as np
import json
from datetime import datetime, time, timedelta
from loguru import logger

# Import system components
from data.market_data import MarketDataProvider
from data.candle_builder import CandleBuilder
from engine.multi_timeframe import MultiTimeframeEngine
from engine.signal_manager import SignalManager
from intelligence.intelligence_aggregator import IntelligenceAggregator
from trading.trade_manager import TradeManager

# Disable logging for cleaner output
logger.remove()
logger.add(sys.stderr, level="ERROR")

def run_system_backtest(days=1, use_optimized_logic=True):
    """
    Runs a backtest using the actual system components and logic.
    """
    config = json.load(open('config/instruments.json'))
    data = MarketDataProvider(start_streams=False)
    candles = CandleBuilder()
    
    if use_optimized_logic:
        # Actual optimized settings from system files
        engine_params = {
            "key_value": 1.5,
            "atr_period": 10,
            "adx_filter": True,
            "adx_threshold": 25.0,
            "strict_adx": True
        }
    else:
        # Baseline settings (Restore Point 8)
        engine_params = {
            "key_value": 1.0,
            "atr_period": 10,
            "adx_filter": True,
            "adx_threshold": 25.0,
            "strict_adx": False
        }
    
    mtf = MultiTimeframeEngine(engine_params)
    signals = SignalManager()
    intel_agg = IntelligenceAggregator()
    
    # 1. Load Local Data
    for name in config['indices']:
        for tf in ['1min', '5min', '15min']:
            filename = f"data_store/candles/{name}_{tf}.csv"
            if os.path.exists(filename):
                df = pd.read_csv(filename)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                if df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                
                # Filter for last N days
                unique_dates = df.index.normalize().unique()
                target_dates = unique_dates[-days:] if len(unique_dates) >= days else unique_dates
                df = df[df.index.normalize().isin(target_dates)]
                
                candles.update_candles(name, df, tf)
                mtf.update_data(name, tf, df)
    
    # 2. Run Multi-Timeframe Engine
    mtf_results = mtf.process_all(config, 100000, 1.0, {"NIFTY": 1, "BANKNIFTY": 1, "SENSEX": 1})
    
    all_trades = []
    
    for name in config['indices']:
        lot_size = config['indices'][name]['lot_size']
        res = mtf_results.get(name)
        if not res: continue
        
        # We simulate the scanner's logic for 5min and 15min signals
        for tf_name in ["5min", "15min"]:
            tf_res = getattr(res, f"results_{tf_name}")
            if not tf_res: continue
            
            sigs = tf_res.get('signals', [])
            
            i = 0
            while i < len(sigs):
                sig = sigs[i]
                
                # Fetch intelligence for the instrument (Regime) dynamically up to sig.timestamp
                df_for_intel = candles.get_candles(name, tf_name)
                regime = "TRENDING" # Default
                if use_optimized_logic and df_for_intel is not None and not df_for_intel.empty:
                    hist_candles = df_for_intel[df_for_intel.index <= sig.timestamp]
                    # Corrected Argument Order: instrument, timeframe, candle_df
                    intel_res = intel_agg.analyze(name, tf_name, hist_candles)
                    regime = intel_res.get("regime", {}).get("regime", "TRENDING")
                
                # Session filter: 9:18 to 15:29
                s1_time = sig.timestamp.time()
                if s1_time < time(9, 18) or s1_time > time(15, 29):
                    i += 1
                    continue

                # --- QUALITY FILTERING ---
                grade, conf, _ = signals._grade_signal(sig, 0.5, 0.0, regime=regime)
                base_grade = grade.split()[0]
                
                grade_hierarchy = {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4}
                sig_rank = grade_hierarchy.get(base_grade, 0)
                
                if use_optimized_logic:
                    # Optimized Logic: Regime-Aware
                    if regime in ["RANGING", "SIDEWAYS", "VOLATILE", "MEAN_REVERTING"]:
                        min_rank = 3 # A/A+
                    else:
                        min_rank = 2 # B+ or higher
                else:
                    # Baseline Logic: Anything B or higher
                    min_rank = 1
                
                if sig_rank < min_rank:
                    i += 1
                    continue

                # --- EXIT SIMULATION ---
                s2 = None
                exit_price = None
                exit_time = None
                
                for j in range(i + 1, len(sigs)):
                    if sigs[j].signal_type != sig.signal_type and sigs[j].timestamp.date() == sig.timestamp.date():
                        s2 = sigs[j]
                        exit_price = s2.price
                        exit_time = s2.timestamp
                        i = j 
                        break
                    elif sigs[j].timestamp.date() != sig.timestamp.date():
                        break
                
                if s2 is None:
                    # EOD Exit
                    candle_df = candles.get_candles(name, "1min")
                    if candle_df is not None:
                        day_data = candle_df[candle_df.index.date == sig.timestamp.date()]
                        eod_data = day_data[day_data.index.time <= time(15, 18)]
                        if not eod_data.empty:
                            last_candle = eod_data.iloc[-1]
                            exit_price = float(last_candle['close'])
                            exit_time = eod_data.index[-1]
                    
                    # Advance i to next day
                    next_day_found = False
                    for j in range(i + 1, len(sigs)):
                        if sigs[j].timestamp.date() > sig.timestamp.date():
                            i = j
                            next_day_found = True
                            break
                    if not next_day_found:
                        i = len(sigs)
                
                if exit_price is not None:
                    qty = 1 * lot_size
                    pnl = (exit_price - sig.price) * qty if sig.signal_type == "BUY" else (sig.price - exit_price) * qty
                    pnl -= 80 # Charges
                    
                    all_trades.append({
                        "inst": name,
                        "tf": tf_name,
                        "pnl": pnl,
                        "grade": grade
                    })
                else:
                    i += 1
                    
    df_trades = pd.DataFrame(all_trades)
    return df_trades

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"{'BACKTEST COMPARISON: BASELINE vs OPTIMIZED':^60}")
    print(f"{'='*60}\n")
    print(f"{'Mode':<15} | {'Period':<8} | {'Trades':<7} | {'WinRate':<8} | {'Net P&L':<12}")
    print("-" * 60)
    
    for d_label, d_val in [("1 Day", 1), ("7 Days", 7)]:
        # Baseline
        df_base = run_system_backtest(days=d_val, use_optimized_logic=False)
        if not df_base.empty:
            wr_base = (df_base['pnl'] > 0).mean() * 100
            pnl_base = df_base['pnl'].sum()
            print(f"{'Baseline':<15} | {d_label:<8} | {len(df_base):<7} | {wr_base:>7.1f}% | Rs.{pnl_base:>10.0f}")
        
        # Optimized
        df_opt = run_system_backtest(days=d_val, use_optimized_logic=True)
        if not df_opt.empty:
            wr_opt = (df_opt['pnl'] > 0).mean() * 100
            pnl_opt = df_opt['pnl'].sum()
            print(f"{'Optimized':<15} | {d_label:<8} | {len(df_opt):<7} | {wr_opt:>7.1f}% | Rs.{pnl_opt:>10.0f}")
        print("-" * 60)
