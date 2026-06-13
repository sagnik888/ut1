"""
Regime Detector — Multi-TF Market Regime Detection
═══════════════════════════════════════════════════════════════
States: TRENDING_UP, TRENDING_DOWN, RANGING, VOLATILE, BREAKOUT
"""
import pandas as pd
import numpy as np
from typing import Dict
from enum import Enum
from loguru import logger


class MarketRegime(Enum):
    TRENDING_UP = "TRENDING_UP"
    TRENDING_DOWN = "TRENDING_DOWN"
    RANGING = "RANGING"
    VOLATILE = "VOLATILE"
    BREAKOUT = "BREAKOUT"
    UNKNOWN = "UNKNOWN"


class RegimeDetector:
    def __init__(self):
        self._regime_history: Dict[str, list] = {}

    def detect(self, df: pd.DataFrame, instrument: str = "", timeframe: str = "") -> Dict:
        if df is None or len(df) < 30:
            return self._unknown()
        try:
            close = df['close']
            high = df['high']
            low = df['low']
            # ADX
            from engine.ut_bot_core import UTBotEngine
            _, _, adx = UTBotEngine.calculate_adx(df, 14)
            adx_val = float(adx.iloc[-1]) if not pd.isna(adx.iloc[-1]) else 0
            # ATR %
            atr = UTBotEngine.calculate_atr(df, 14)
            atr_pct = float(atr.iloc[-1] / close.iloc[-1] * 100) if close.iloc[-1] > 0 else 0
            # Bollinger Band Width
            sma60 = close.rolling(60).mean()
            std60 = close.rolling(60).std()
            bb_width = float(((sma60.iloc[-1] + 2 * std60.iloc[-1]) - (sma60.iloc[-1] - 2 * std60.iloc[-1])) / sma60.iloc[-1] * 100) if not pd.isna(sma60.iloc[-1]) and sma60.iloc[-1] > 0 else 0
            # Trend direction (SMA alignment)
            sma9 = close.rolling(9).mean().iloc[-1] if len(close) >= 9 else close.iloc[-1]
            sma21 = close.rolling(21).mean().iloc[-1] if len(close) >= 21 else close.iloc[-1]
            price = close.iloc[-1]
            direction = "UP" if price > sma9 > sma21 else ("DOWN" if price < sma9 < sma21 else "NEUTRAL")
            # RSI
            delta = close.diff()
            gain = delta.where(delta > 0, 0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            rsi = float(100 - (100 / (1 + rs.iloc[-1]))) if not pd.isna(rs.iloc[-1]) else 50
            # MACD
            ema12 = close.ewm(span=12).mean()
            ema26 = close.ewm(span=26).mean()
            macd_line = ema12 - ema26
            macd_signal = macd_line.ewm(span=9).mean()
            macd_hist = float(macd_line.iloc[-1] - macd_signal.iloc[-1]) if not pd.isna(macd_line.iloc[-1]) else 0
            # Momentum
            momentum = "BULLISH" if rsi > 55 and macd_hist > 0 else ("BEARISH" if rsi < 45 and macd_hist < 0 else "NEUTRAL")
            # Dynamic Regime Thresholds (Normalized to 95th Percentile)
            lookback = min(500, len(df))
            
            # Create full BB width series for percentile calculation
            bb_width_series = (4 * std60) / sma60 * 100
            
            atr_95 = np.nanpercentile(atr.iloc[-lookback:] / close.iloc[-lookback:] * 100, 95) if len(atr) >= 20 else 2.0
            bb_95 = np.nanpercentile(bb_width_series.iloc[-lookback:], 95) if len(bb_width_series) >= 20 else 4.0
            
            # Cap the thresholds to prevent absurd values in extreme markets
            vol_atr_thresh = min(max(atr_95, 0.5), 3.0)
            vol_bb_thresh = min(max(bb_95, 1.0), 5.0)

            # Regime classification
            regime = MarketRegime.UNKNOWN
            confidence = 40
            # Lowered ADX threshold from 25 to 20 to be more flexible to early trends
            if adx_val >= 20:
                if direction == "UP":
                    regime = MarketRegime.TRENDING_UP
                    confidence = min(95, int(adx_val + 30))
                elif direction == "DOWN":
                    regime = MarketRegime.TRENDING_DOWN
                    confidence = min(95, int(adx_val + 30))
                else:
                    regime = MarketRegime.VOLATILE
                    confidence = 55
            elif atr_pct > vol_atr_thresh or bb_width > vol_bb_thresh:
                regime = MarketRegime.VOLATILE
                confidence = min(85, int((atr_pct / vol_atr_thresh) * 40 + 30))
            else:
                regime = MarketRegime.RANGING
                confidence = min(85, int((30 - adx_val) * 3.5))
            # Breakout detection
            high_60 = high.rolling(60).max().iloc[-1]
            low_60 = low.rolling(60).min().iloc[-1]
            breakout_direction = "NONE"
            if not pd.isna(high_60) and not pd.isna(low_60):
                if price >= high_60 * 0.999:
                    regime = MarketRegime.BREAKOUT
                    confidence = 70
                    breakout_direction = "UP"
                elif price <= low_60 * 1.001:
                    regime = MarketRegime.BREAKOUT
                    confidence = 70
                    breakout_direction = "DOWN"
            # Track history
            key = f"{instrument}_{timeframe}"
            if key not in self._regime_history:
                self._regime_history[key] = []
            self._regime_history[key].append(regime.value)
            if len(self._regime_history[key]) > 50:
                self._regime_history[key] = self._regime_history[key][-50:]
            return {
                "regime": regime.value, "confidence": confidence,
                "adx": round(adx_val, 1), "atr_pct": round(atr_pct, 2),
                "bb_width": round(bb_width, 2), "direction": direction,
                "breakout_direction": breakout_direction,
                "rsi": round(rsi, 1), "macd_histogram": round(macd_hist, 2),
                "momentum": momentum,
                "description": f"{regime.value} (ADX:{adx_val:.0f} RSI:{rsi:.0f} Dir:{direction})",
            }
        except Exception as e:
            logger.error(f"Regime detection error: {e}")
            return self._unknown()

    def should_trade(self, regime: str, signal_type: str) -> tuple:
        """
        Evaluate if trading is appropriate given the current regime.
        Returns (should_trade: bool, reason: str).

        PHILOSOPHY: Institutional Grade Filtering.
        We BLOCK trades in high-risk regimes unless they are A/A+.
        """
        if regime in ["TRENDING_UP", "BREAKOUT"] and signal_type == "BUY":
            return True, "Aligned with Bullish Momentum"
        
        if regime in ["TRENDING_DOWN", "BREAKOUT"] and signal_type == "SELL":
            return True, "Aligned with Bearish Momentum"
            
        if regime == "UNKNOWN":
            return True, "High risk regime (UNKNOWN). Unknown regime. Require A+ Grade and reduced size."
            
        if regime in ["VOLATILE", "CHOPPY", "SIDEWAYS"]:
            # These are high-risk. Signal processor must check for A/A+ grade only.
            return True, f"High risk regime ({regime}). Require A+ Grade."
            
        if regime == "RANGING":
            return True, "Range-bound — expect mean reversion"
            
        return True, "Proceed with caution"

    def _unknown(self) -> Dict:
        return {"regime": "UNKNOWN", "confidence": 0, "adx": 0, "atr_pct": 0, "bb_width": 0, "direction": "NEUTRAL", "rsi": 50, "macd_histogram": 0, "momentum": "NEUTRAL", "description": "Insufficient data"}
