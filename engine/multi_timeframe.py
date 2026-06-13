"""
Multi-Timeframe Analysis Engine
═══════════════════════════════════════════════════════════════

Runs UT Bot across 1min, 5min, 15min simultaneously for each index.
- Signal timeframes (5min, 15min): Generate trade entries
- Tracking timeframe (1min): Tracks active trades for trailing

Confluence scoring: signal on 5min + aligned regime on 15min = higher confidence
"""

import pandas as pd
from typing import Dict, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, field
from loguru import logger

from engine.ut_bot_core import UTBotEngine, UTBotSignal


@dataclass
class MultiTFResult:
    """Result of multi-timeframe analysis for one instrument"""
    instrument: str
    timestamp: datetime
    results_1min: Optional[Dict] = None
    results_5min: Optional[Dict] = None
    results_15min: Optional[Dict] = None
    confluence_score: float = 0.0
    confluence_signal: str = "HOLD"  # BUY / SELL / HOLD
    active_signals: List[UTBotSignal] = field(default_factory=list)


class MultiTimeframeEngine:
    """
    Runs UT Bot on multiple timeframes and computes confluence.

    Each instrument gets 3 independent UT Bot instances (1min, 5min, 15min).
    Signals from 5min and 15min are considered for entries.
    1min is used for trade tracking and early exit detection.
    """

    def __init__(self, engine_params: Dict = None):
        """
        Args:
            engine_params: Dict of UT Bot parameters to override defaults
        """
        params = engine_params or {}
        self.engine_params = params
        self.engines: Dict[str, UTBotEngine] = {}
        engine_init_params = {
            key: value for key, value in params.items()
            if key in {
                "key_value",
                "atr_period",
                "use_heikin_ashi",
                "signal_mode",
                "adx_filter",
                "adx_period",
                "adx_threshold",
                "strict_adx",
                "session_filter",
                "session_start",
                "session_end",
            }
        }

        # Create engines for each instrument-timeframe combo
        instruments = ["NIFTY", "BANKNIFTY", "SENSEX", "MIDCPNIFTY"]
        timeframes = ["1min", "5min", "15min"]

        for inst in instruments:
            for tf in timeframes:
                key = f"{inst}_{tf}"
                self.engines[key] = UTBotEngine(**engine_init_params)

        # Store latest OHLCV data per instrument-timeframe
        self._data_cache: Dict[str, pd.DataFrame] = {}

        # Confluence weights
        self.tf_weights = {
            "1min": 0.15,
            "5min": 0.40,
            "15min": 0.45,
        }

        # ═══ PROCESS RESULT CACHE — skip reprocessing unchanged data ═══
        self._result_cache: Dict[str, Dict] = {}
        self._data_hash: Dict[str, int] = {}

    @staticmethod
    def _hash_df(df) -> int:
        """Quick hash of DataFrame to detect changes.
        Uses the second-to-last (confirmed) candle to avoid cache misses
        from live tick updates on the forming bar."""
        if df is None or len(df) == 0:
            return 0

        cols = [c.lower() for c in df.columns]
        if 'close' not in cols:
            return hash(len(df))

        try:
            c_col = 'close' if 'close' in df.columns else ('Close' if 'Close' in df.columns else None)
            h_col = 'high' if 'high' in df.columns else ('High' if 'High' in df.columns else None)
            l_col = 'low' if 'low' in df.columns else ('Low' if 'Low' in df.columns else None)

            if not c_col: return hash(len(df))

            # Use live forming candle so trailing stop updates instantly on ticks.
            # Engine vectorization makes full processing <1ms, so cache misses on ticks are cheap.
            ref_idx = -1
            last_timestamp = df.index[ref_idx].timestamp() if hasattr(df.index[ref_idx], "timestamp") else hash(str(df.index[ref_idx]))
            
            return hash((
                len(df),
                last_timestamp,
                float(df[c_col].iloc[ref_idx]),
                float(df[h_col].iloc[ref_idx]) if h_col else 0.0,
                float(df[l_col].iloc[ref_idx]) if l_col else 0.0,
            ))
        except Exception as e:

            logger.debug(f"Caught bare exception: {e}")
            return hash(len(df))

    def update_data(self, instrument: str, timeframe: str, df: pd.DataFrame):
        """Update candle data for an instrument-timeframe pair"""
        key = f"{instrument}_{timeframe}"
        self._data_cache[key] = df

    def process_instrument(
        self,
        instrument: str,
        lot_size: int = 25,
        lots: int = 1,
        capital: float = 100000.0,
        risk_pct: float = 1.0,
    ) -> MultiTFResult:
        """
        Run UT Bot on all 3 timeframes for one instrument.

        Returns MultiTFResult with confluence scoring.
        """
        result = MultiTFResult(
            instrument=instrument,
            timestamp=datetime.now(),
        )

        tf_results = {}
        for tf in ["1min", "5min", "15min"]:
            key = f"{instrument}_{tf}"
            engine = self.engines.get(key)
            data = self._data_cache.get(key)

            if engine is None or data is None or len(data) < 15:
                # Use cached result if available
                if key in self._result_cache:
                    tf_results[tf] = self._result_cache[key]
                continue

            # ═══ SKIP if data hasn't changed ═══
            new_hash = self._hash_df(data)
            if new_hash == self._data_hash.get(key, 0) and key in self._result_cache:
                tf_results[tf] = self._result_cache[key]
            else:
                try:
                    res = engine.process(
                        df=data,
                        instrument=instrument,
                        timeframe=tf,
                        capital=capital,
                        risk_pct=risk_pct,
                        lots=lots,
                        lot_size=lot_size,
                    )
                    tf_results[tf] = res
                    self._result_cache[key] = res
                    self._data_hash[key] = new_hash
                except Exception as e:
                    logger.error(f"Error processing {key}: {e}")
                    if key in self._result_cache:
                        tf_results[tf] = self._result_cache[key]

            if tf == "1min":
                result.results_1min = tf_results.get(tf)
            elif tf == "5min":
                result.results_5min = tf_results.get(tf)
            elif tf == "15min":
                result.results_15min = tf_results.get(tf)

        # ── Confluence Scoring ──
        result.confluence_score, result.confluence_signal = self._compute_confluence(
            tf_results, instrument
        )

        # Collect active signals from signal timeframes
        for tf in ["5min", "15min"]:
            res = tf_results.get(tf)
            if res and res.get("signals"):
                # Only include recent signals (last 3 bars) but EXCLUDE the live bar (last index)
                # to prevent repainting issues!
                data_len = len(self._data_cache.get(f"{instrument}_{tf}", []))
                for sig in res["signals"]:
                    if sig.bar_index >= data_len - 3 and sig.bar_index < data_len - 1:
                        result.active_signals.append(sig)

        return result

    def _compute_confluence(
        self,
        tf_results: Dict[str, Dict],
        instrument: str
    ) -> tuple:
        """
        Compute confluence score from multi-timeframe results.

        Scoring:
        - Each timeframe contributes its weight × direction
        - Direction: +1 (LONG), -1 (SHORT), 0 (FLAT)
        - Score > 0.5 → BUY, Score < -0.5 → SELL, else HOLD
        """
        score = 0.0
        total_weight = 0.0

        for tf, weight in self.tf_weights.items():
            res = tf_results.get(tf)
            if res and res.get("state"):
                pos = res["state"]["position"]
                trending = res["state"].get("is_trending", False)

                # Weight trending confirmations more
                effective_weight = weight * (1.3 if trending else 0.7)
                score += pos * effective_weight
                total_weight += effective_weight

        # Normalize to [-1, 1]
        if total_weight > 0:
            score = score / total_weight

        # Determine signal
        if score > 0.35:
            signal = "BUY"
        elif score < -0.35:
            signal = "SELL"
        else:
            signal = "HOLD"

        return round(score, 3), signal

    def apply_engine_params(self, params: dict):
        """Dynamically update UT Engine parameters across all internal engines"""
        self.engine_params = params
        for key, engine in self.engines.items():
            if "key_value" in params:
                engine.key_value = params["key_value"]
            if "atr_period" in params:
                engine.atr_period = params["atr_period"]
            if "signal_mode" in params:
                engine.signal_mode = params["signal_mode"]
            if "adx_filter" in params:
                engine.adx_filter = params["adx_filter"]
            if "adx_threshold" in params:
                engine.adx_threshold = params["adx_threshold"]
            if "strict_adx" in params:
                engine.strict_adx = params["strict_adx"]

    def get_all_states(self) -> Dict[str, Dict]:
        """Get current state for all instrument-timeframe combos"""
        states = {}
        for key, engine in self.engines.items():
            state = engine.get_state(key)
            states[key] = {
                "position": state.position,
                "trailing_stop": state.trailing_stop,
                "last_entry_price": state.last_entry_price,
                "last_exit_price": state.last_exit_price,
                "bar_count": state.bar_count,
            }
        return states

    def process_all(
        self,
        instruments_config: Dict,
        capital: float = 100000.0,
        risk_pct: float = 1.0,
        user_lots: Dict[str, int] = None,
    ) -> Dict[str, MultiTFResult]:
        """
        Process all instruments at once.

        Args:
            instruments_config: From instruments.json
            capital: Trading capital
            risk_pct: Risk per trade %
            user_lots: Optional dict of {instrument: lots}

        Returns:
            Dict of instrument -> MultiTFResult
        """
        results = {}
        lots_config = user_lots or {}

        for name, config in instruments_config.get("indices", {}).items():
            lot_size = config.get("lot_size", 25)
            lots = lots_config.get(name, 1)

            result = self.process_instrument(
                instrument=name,
                lot_size=lot_size,
                lots=lots,
                capital=capital,
                risk_pct=risk_pct,
            )
            results[name] = result

        return results
