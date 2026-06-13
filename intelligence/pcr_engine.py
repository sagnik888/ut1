"""
PCR Engine — Put-Call Ratio Analysis
═══════════════════════════════════════════════════════════════

PCR Interpretation (Contrarian):
- PCR > 1.0: More puts than calls (Bearish sentiment → contrarian bullish)
- PCR < 0.7: More calls than puts (Bullish sentiment → contrarian bearish)
- PCR 0.7-1.0: Neutral
"""

import pandas as pd
import numpy as np
from typing import Dict, List
import threading
from collections import deque
from loguru import logger


class PCREngine:
    """Put-Call Ratio analysis engine with trend tracking"""

    def __init__(self, history_size: int = 50):
        self._pcr_history: Dict[str, deque] = {}
        self.history_size = history_size
        self._lock = threading.RLock()
        self.thresholds = {
            "extreme_fear": 1.5,
            "bearish": 1.0,
            "neutral_high": 0.9,
            "neutral_low": 0.7,
            "bullish": 0.5,
            "extreme_greed": 0.3,
        }

    def analyze(
        self,
        options_chain: pd.DataFrame,
        instrument: str = "",
        spot_price: float = 0.0,
        strike_interval: int = 50,
        near_strikes: int = 5,
    ) -> Dict:
        """
        Calculate and analyze PCR.

        Returns PCR value, sentiment, contrarian signal, and trend.
        """
        if options_chain is None or len(options_chain) == 0:
            return self._empty_result()

        try:
            # ── OI-based PCR ──
            total_put_oi = options_chain['put_oi'].sum()
            total_call_oi = options_chain['call_oi'].sum()
            pcr_oi = float(total_put_oi / total_call_oi) if total_call_oi > 0 else 0

            # ── Volume-based PCR ──
            total_put_vol = options_chain['put_volume'].sum()
            total_call_vol = options_chain['call_volume'].sum()
            pcr_vol = float(total_put_vol / total_call_vol) if total_call_vol > 0 else 0

            pcr_basis = "BROAD"
            near_pcr_oi = pcr_oi
            near_pcr_vol = pcr_vol
            near_count = len(options_chain)
            if spot_price > 0 and strike_interval > 0 and 'strike' in options_chain.columns:
                atm = round(spot_price / strike_interval) * strike_interval
                min_strike = atm - (near_strikes * strike_interval)
                max_strike = atm + (near_strikes * strike_interval)
                near_chain = options_chain[(options_chain['strike'] >= min_strike) & (options_chain['strike'] <= max_strike)]
                if len(near_chain) >= 3:
                    near_put_oi = near_chain['put_oi'].sum()
                    near_call_oi = near_chain['call_oi'].sum()
                    near_put_vol = near_chain['put_volume'].sum()
                    near_call_vol = near_chain['call_volume'].sum()
                    near_pcr_oi = float(near_put_oi / near_call_oi) if near_call_oi > 0 else 0
                    near_pcr_vol = float(near_put_vol / near_call_vol) if near_call_vol > 0 else 0
                    near_count = len(near_chain)
                    pcr_basis = "NEAR_MONEY"

            # ── Interpret PCR ──
            primary_pcr = near_pcr_oi if pcr_basis == "NEAR_MONEY" else pcr_oi
            sentiment, signal = self._interpret(primary_pcr)

            # ── Track History ──
            with self._lock:
                if instrument not in self._pcr_history:
                    self._pcr_history[instrument] = deque(maxlen=self.history_size)
                self._pcr_history[instrument].append(primary_pcr)
    
                # ── PCR Trend ──
                history = list(self._pcr_history[instrument])
                
            trend = "STABLE"
            if len(history) >= 5:
                recent = np.mean(history[-3:])
                prior = np.mean(history[-6:-3]) if len(history) >= 6 else np.mean(history[:3])
                change = recent - prior
                if change > 0.1:
                    trend = "RISING"  # Becoming more bearish → contrarian bullish
                elif change < -0.1:
                    trend = "FALLING"  # Becoming more bullish → contrarian bearish
                else:
                    trend = "STABLE"

            return {
                "pcr_oi": round(pcr_oi, 3),
                "pcr_volume": round(pcr_vol, 3),
                "pcr_oi_broad": round(pcr_oi, 3),
                "pcr_volume_broad": round(pcr_vol, 3),
                "pcr_near_oi": round(near_pcr_oi, 3),
                "pcr_near_volume": round(near_pcr_vol, 3),
                "pcr_basis": pcr_basis,
                "near_strike_count": int(near_count),
                "primary_pcr": round(primary_pcr, 3),
                "total_put_oi": int(total_put_oi),
                "total_call_oi": int(total_call_oi),
                "sentiment": sentiment,
                "signal": signal,
                "raw_sentiment": sentiment,
                "contrarian_signal": signal,
                "interpretation_model": "CONTRARIAN",
                "trend": trend,
                "pcr_history": history[-10:] if len(history) > 0 else [],
                "interpretation": self._get_interpretation(primary_pcr, sentiment, signal, trend),
            }

        except Exception as e:
            logger.error(f"PCR analysis error: {e}")
            return self._empty_result()

    def _interpret(self, pcr: float) -> tuple:
        """Interpret PCR value → (sentiment, contrarian_signal)"""
        if pcr >= self.thresholds["extreme_fear"]:
            return "EXTREME_FEAR", "STRONG_BULLISH"
        elif pcr >= self.thresholds["bearish"]:
            return "BEARISH", "BULLISH"
        elif pcr >= self.thresholds["neutral_low"]:
            return "NEUTRAL", "NEUTRAL"
        elif pcr >= self.thresholds["bullish"]:
            return "BULLISH", "BEARISH"
        else:
            return "EXTREME_GREED", "STRONG_BEARISH"

    def _get_interpretation(self, pcr: float, sentiment: str, signal: str, trend: str) -> str:
        """Generate human-readable interpretation"""
        trend_text = {
            "RISING": "rising (more puts being added)",
            "FALLING": "falling (more calls being added)",
            "STABLE": "stable",
        }
        return (
            f"PCR {pcr:.3f} → {sentiment} sentiment | "
            f"Contrarian: {signal} | Trend: {trend_text.get(trend, trend)}"
        )

    def _empty_result(self) -> Dict:
        return {
            "pcr_oi": 0, "pcr_volume": 0, "pcr_oi_broad": 0, "pcr_volume_broad": 0,
            "pcr_near_oi": 0, "pcr_near_volume": 0, "pcr_basis": "NONE",
            "near_strike_count": 0, "primary_pcr": 0, "total_put_oi": 0, "total_call_oi": 0,
            "sentiment": "UNKNOWN", "signal": "NEUTRAL", "trend": "UNKNOWN",
            "raw_sentiment": "UNKNOWN", "contrarian_signal": "NEUTRAL", "interpretation_model": "CONTRARIAN",
            "pcr_history": [], "interpretation": "No data",
        }
