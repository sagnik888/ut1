"""
OI Tracker — Open Interest Analysis
═══════════════════════════════════════════════════════════════

OI Change Patterns:
- OI ↑ + Price ↑ = Long Buildup (Bullish)
- OI ↑ + Price ↓ = Short Buildup (Bearish)
- OI ↓ + Price ↑ = Short Covering (Bullish)
- OI ↓ + Price ↓ = Long Unwinding (Bearish)
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, List
from loguru import logger


class OITracker:
    """Tracks and analyzes Open Interest changes"""

    def __init__(self):
        self._oi_history: Dict[str, List[Dict]] = {}

    def analyze(
        self,
        options_chain: pd.DataFrame,
        price_change: float = 0.0,
        spot_price: Optional[float] = None,
        strike_interval: int = 50,
        near_strikes: int = 5,
    ) -> Dict:
        """
        Analyze OI changes with price movement.

        Args:
            options_chain: DataFrame with columns [strike, call_oi, call_oi_change,
                          put_oi, put_oi_change, call_volume, put_volume, ...]
            price_change: Price change percentage of the underlying

        Returns:
            OI analysis dictionary
        """
        if options_chain is None or len(options_chain) == 0:
            return self._empty_result()

        try:
            analysis_chain = options_chain
            analysis_basis = "FULL_CHAIN"
            if spot_price and spot_price > 0 and "strike" in options_chain.columns:
                interval = max(float(strike_interval or 0), 1.0)
                atm = round(float(spot_price) / interval) * interval
                width = max(int(near_strikes or 0), 0) * interval
                near_chain = options_chain[
                    (options_chain["strike"].astype(float) >= atm - width)
                    & (options_chain["strike"].astype(float) <= atm + width)
                ]
                if not near_chain.empty:
                    analysis_chain = near_chain
                    analysis_basis = "NEAR_MONEY"

            total_call_oi = int(analysis_chain['call_oi'].sum())
            total_put_oi = int(analysis_chain['put_oi'].sum())
            total_call_oi_change = int(analysis_chain['call_oi_change'].sum())
            total_put_oi_change = int(analysis_chain['put_oi_change'].sum())
            net_oi_change = total_call_oi_change + total_put_oi_change

            # ── Activity Classification ──
            oi_increasing = net_oi_change > 0
            price_up = price_change > 0

            if oi_increasing and price_up:
                activity = "LONG_BUILDUP"
                signal = "BULLISH"
                interpretation = "Fresh buying — bullish momentum likely to continue"
            elif oi_increasing and not price_up:
                activity = "SHORT_BUILDUP"
                signal = "BEARISH"
                interpretation = "Fresh selling — bearish momentum likely to continue"
            elif not oi_increasing and price_up:
                activity = "SHORT_COVERING"
                signal = "BULLISH"
                interpretation = "Shorts covering — strong bullish reversal signal"
            else:
                activity = "LONG_UNWINDING"
                signal = "BEARISH"
                interpretation = "Longs exiting — weak hands selling"

            # ── Key Strikes (max OI) ──
            score = 75 if signal == "BULLISH" else 25
            if abs(price_change) < 0.05:
                score = 55 if signal == "BULLISH" else 45

            max_call_oi_strike = int(analysis_chain.loc[analysis_chain['call_oi'].idxmax(), 'strike'])
            max_put_oi_strike = int(analysis_chain.loc[analysis_chain['put_oi'].idxmax(), 'strike'])

            # ── Top OI Change Strikes ──
            top_call_buildup = analysis_chain.nlargest(3, 'call_oi_change')[
                ['strike', 'call_oi_change']
            ].to_dict('records')
            top_put_buildup = analysis_chain.nlargest(3, 'put_oi_change')[
                ['strike', 'put_oi_change']
            ].to_dict('records')

            return {
                "total_call_oi": total_call_oi,
                "total_put_oi": total_put_oi,
                "total_call_oi_change": total_call_oi_change,
                "total_put_oi_change": total_put_oi_change,
                "net_oi_change": net_oi_change,
                "activity": activity,
                "signal": signal,
                "score": score,
                "interpretation": interpretation,
                "max_call_oi_strike": max_call_oi_strike,
                "max_put_oi_strike": max_put_oi_strike,
                "resistance_level": max_call_oi_strike,
                "support_level": max_put_oi_strike,
                "top_call_buildup": top_call_buildup,
                "top_put_buildup": top_put_buildup,
                "analysis_basis": analysis_basis,
                "analysis_strike_count": int(len(analysis_chain)),
            }

        except Exception as e:
            logger.error(f"OI analysis error: {e}")
            return self._empty_result()

    def _empty_result(self) -> Dict:
        return {
            "total_call_oi": 0, "total_put_oi": 0,
            "total_call_oi_change": 0, "total_put_oi_change": 0,
            "net_oi_change": 0, "activity": "UNKNOWN", "signal": "NEUTRAL", "score": 50,
            "interpretation": "No data", "max_call_oi_strike": 0,
            "max_put_oi_strike": 0, "resistance_level": 0, "support_level": 0,
            "top_call_buildup": [], "top_put_buildup": [],
            "analysis_basis": "NONE", "analysis_strike_count": 0,
        }
