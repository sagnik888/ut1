"""
Volume Profiler — Intraday Volume Analysis
═══════════════════════════════════════════════════════════════

Tracks:
- Cumulative intraday volume vs previous day
- Volume surge detection (>1.5x average)
- VWAP calculation and deviation
- Buy/sell volume ratio estimation
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
from loguru import logger


class VolumeProfiler:
    """Intraday volume analysis engine"""

    def __init__(self):
        self._prev_day_volumes: Dict[str, pd.Series] = {}
        self._today_volumes: Dict[str, float] = {}

    def analyze(self, df: pd.DataFrame, instrument: str = "") -> Dict:
        """
        Analyze volume patterns in OHLCV data.

        Returns:
            Dict with volume metrics and signals
        """
        if df is None or len(df) < 10:
            return self._empty_result()

        try:
            volume = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
            close = df['close']
            high = df['high']
            low = df['low']
            nonzero_volume = volume.where(volume > 0)
            nonzero_count = int(nonzero_volume.count())
            volume_for_stats = nonzero_volume if nonzero_count >= 5 else volume

            # ── Intraday Daily VWAP (Resets every day at 09:15) ──
            df_copy = df.copy()
            df_copy['volume'] = volume
            df_copy['date'] = df_copy.index.date
            # Group by date to calculate daily cumulative values
            grouped = df_copy.groupby('date')
            
            typical_price = (high + low + close) / 3
            cum_vol = grouped['volume'].cumsum()
            cum_pv = (typical_price * df_copy['volume']).groupby(df_copy.index.date).cumsum()
            vwap = cum_pv / cum_vol
            vwap_current = float(vwap.iloc[-1]) if not pd.isna(vwap.iloc[-1]) else 0
            vwap_deviation = ((close.iloc[-1] - vwap_current) / vwap_current * 100) if vwap_current > 0 else 0

            # ── Volume Moving Average ──
            current_idx = volume.index[-1]
            if nonzero_count:
                current_idx = nonzero_volume.last_valid_index()
                current_vol = float(nonzero_volume.loc[current_idx])
            else:
                current_vol = float(volume.iloc[-1])

            vol_ma = volume_for_stats.rolling(20, min_periods=5).mean()
            avg_at_current = vol_ma.loc[current_idx] if current_idx in vol_ma.index else vol_ma.iloc[-1]
            if pd.isna(avg_at_current) or avg_at_current <= 0:
                fallback_avg = volume_for_stats.tail(20).mean()
                avg_vol = float(fallback_avg) if not pd.isna(fallback_avg) and fallback_avg > 0 else max(current_vol, 1.0)
            else:
                avg_vol = float(avg_at_current)

            vol_ratio = current_vol / avg_vol if avg_vol > 0 else 1.0

            tod_avg = 0.0
            relative_volume_ratio = 1.0
            volume_confidence = 50
            if isinstance(df.index, pd.DatetimeIndex) and len(df) >= 30:
                last_ts = current_idx
                same_slot = df[(df.index.time == last_ts.time()) & (df.index.date != last_ts.date())]
                if len(same_slot) >= 2:
                    same_slot_volume = pd.to_numeric(same_slot['volume'], errors='coerce').fillna(0)
                    same_slot_nonzero = same_slot_volume[same_slot_volume > 0]
                    tod_source = same_slot_nonzero if len(same_slot_nonzero) >= 2 else same_slot_volume
                    tod_avg = float(tod_source.tail(10).mean())
                    if tod_avg > 0:
                        relative_volume_ratio = current_vol / tod_avg
                        volume_confidence = 75

            # ── Volume Surge Detection ──
            effective_ratio = max(vol_ratio, relative_volume_ratio if volume_confidence >= 75 else 1.0)
            is_surge = effective_ratio > 1.5
            surge_level = "EXTREME" if effective_ratio > 3.0 else ("HIGH" if effective_ratio > 2.0 else ("MODERATE" if effective_ratio > 1.5 else "NORMAL"))

            # ── Cumulative Volume Today ──
            total_volume = float(volume.sum())

            # ── Volume Trend (last 10 bars) ──
            trend_volume = volume_for_stats.dropna()
            if len(trend_volume) >= 10:
                recent_avg = float(trend_volume.iloc[-5:].mean())
                prior_avg = float(trend_volume.iloc[-10:-5].mean())
                vol_trend = "INCREASING" if recent_avg > prior_avg * 1.2 else (
                    "DECREASING" if recent_avg < prior_avg * 0.8 else "STABLE"
                )
            else:
                vol_trend = "INSUFFICIENT"

            # ── Buy/Sell Volume Estimation ──
            # Approximate using close position within high-low range
            close_position = (close - low) / (high - low)
            close_position = close_position.fillna(0.5)
            buy_volume = (volume * close_position).sum()
            sell_volume = (volume * (1 - close_position)).sum()
            buy_sell_ratio = float(buy_volume / sell_volume) if sell_volume > 0 else 1.0

            # ── Signal Generation ──
            signal = "NEUTRAL"
            if is_surge:
                if close.iloc[-1] > close.iloc[-2]:
                    signal = "BULLISH_VOLUME"
                else:
                    signal = "BEARISH_VOLUME"

            score = 50
            if signal == "BULLISH_VOLUME":
                score += 20
            elif signal == "BEARISH_VOLUME":
                score -= 20
            if buy_sell_ratio > 1.5:
                score += 15
            elif buy_sell_ratio < 0.6:
                score -= 15
            if effective_ratio > 2.0:
                score += 5 if signal == "BULLISH_VOLUME" else -5
            score = int(max(0, min(100, score)))

            return {
                "vwap": round(vwap_current, 2),
                "vwap_deviation_pct": round(float(vwap_deviation), 2),
                "current_volume": int(current_vol),
                "avg_volume": int(avg_vol),
                "volume_ratio": round(vol_ratio, 2),
                "relative_volume_ratio": round(relative_volume_ratio, 2),
                "time_of_day_avg_volume": int(tod_avg),
                "volume_confidence": volume_confidence,
                "is_surge": is_surge,
                "surge_level": surge_level,
                "total_volume": int(total_volume),
                "volume_trend": vol_trend,
                "buy_sell_ratio": round(buy_sell_ratio, 2),
                "buy_pct": round(buy_sell_ratio / (1 + buy_sell_ratio) * 100, 1),
                "signal": signal,
                "score": score,
            }

        except Exception as e:
            logger.error(f"Volume analysis error: {e}")
            return self._empty_result()

    def _empty_result(self) -> Dict:
        return {
            "vwap": 0, "vwap_deviation_pct": 0, "current_volume": 0,
            "avg_volume": 0, "volume_ratio": 1.0, "relative_volume_ratio": 1.0,
            "time_of_day_avg_volume": 0, "volume_confidence": 0, "is_surge": False,
            "surge_level": "NORMAL", "total_volume": 0, "volume_trend": "INSUFFICIENT",
            "buy_sell_ratio": 1.0, "buy_pct": 50.0, "signal": "NEUTRAL", "score": 50,
        }
