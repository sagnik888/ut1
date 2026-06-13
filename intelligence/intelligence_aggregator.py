"""
Intelligence Aggregator — Combines all intelligence into a unified score
═══════════════════════════════════════════════════════════════
"""
import pandas as pd
from typing import Dict, Optional
from loguru import logger
from intelligence.volume_profiler import VolumeProfiler
from intelligence.oi_tracker import OITracker
from intelligence.pcr_engine import PCREngine
from intelligence.greeks_engine import GreeksEngine
from intelligence.regime_detector import RegimeDetector
from intelligence.order_flow import OrderFlowAnalyzer


class IntelligenceAggregator:
    """Combines all market intelligence sources into a single score"""

    def __init__(self):
        self.volume = VolumeProfiler()
        self.oi = OITracker()
        self.pcr = PCREngine()
        self.greeks = GreeksEngine()
        self.regime = RegimeDetector()
        self.order_flow = OrderFlowAnalyzer()

    def _implied_iv_from_price(self, spot: float, strike: float, days: float, premium: float, opt_type: str) -> Optional[float]:
        if spot <= 0 or strike <= 0 or days <= 0 or premium <= 0:
            return None
        intrinsic = max(0.0, spot - strike) if opt_type == "call" else max(0.0, strike - spot)
        if premium + 0.05 < intrinsic:
            return None
        low, high = 0.01, 3.0
        for _ in range(40):
            mid = (low + high) / 2.0
            model_price = float(self.greeks.calculate(spot, strike, days, mid, opt_type).get("price", 0.0) or 0.0)
            if model_price < premium:
                low = mid
            else:
                high = mid
        solved = max(0.01, min(3.0, (low + high) / 2.0))
        if solved <= 0.0101 or solved >= 2.999:
            return None
        return solved

    def _chain_iv_context(self, options_chain: pd.DataFrame, spot_price: float, days_to_expiry: float) -> Dict:
        if options_chain is None or options_chain.empty or "strike" not in options_chain.columns:
            return {"call_iv": 0.15, "put_iv": 0.15, "source": "default"}
        row = options_chain.iloc[(options_chain["strike"].astype(float) - float(spot_price)).abs().argsort()[:1]]
        atm_row = row.iloc[0]
        call_iv = None
        put_iv = None
        for key in ("call_iv", "ce_iv", "iv_call"):
            if key in atm_row and pd.notna(atm_row.get(key)):
                call_iv = float(atm_row.get(key))
                break
        for key in ("put_iv", "pe_iv", "iv_put"):
            if key in atm_row and pd.notna(atm_row.get(key)):
                put_iv = float(atm_row.get(key))
                break
        if call_iv and call_iv > 1:
            call_iv /= 100.0
        if put_iv and put_iv > 1:
            put_iv /= 100.0
        source = "chain_iv" if call_iv or put_iv else "default"
        strike = float(atm_row.get("strike") or 0.0)
        if not call_iv and float(atm_row.get("call_ltp", 0.0) or 0.0) > 0:
            call_iv = self._implied_iv_from_price(spot_price, strike, days_to_expiry, float(atm_row.get("call_ltp")), "call")
            source = "premium_implied_iv" if call_iv else source
        if not put_iv and float(atm_row.get("put_ltp", 0.0) or 0.0) > 0:
            put_iv = self._implied_iv_from_price(spot_price, strike, days_to_expiry, float(atm_row.get("put_ltp")), "put")
            source = "premium_implied_iv" if put_iv else source
        if call_iv is None and put_iv is not None:
            call_iv = put_iv
            source = f"{source}_put_proxy"
        elif put_iv is None and call_iv is not None:
            put_iv = call_iv
            source = f"{source}_call_proxy"
        elif call_iv is None and put_iv is None:
            source = "default_low_quality"
        return {
            "call_iv": call_iv or 0.15,
            "put_iv": put_iv or 0.15,
            "source": source,
            "quality_ok": source != "default_low_quality",
        }

    def analyze(
        self,
        instrument: str,
        timeframe: str,
        candle_df: pd.DataFrame,
        candle_1min_df: Optional[pd.DataFrame] = None, # Added 1m support
        options_chain: Optional[pd.DataFrame] = None,
        spot_price: float = 0,
        strike_interval: int = 50,
        days_to_expiry: float = 7,
        price_change_pct: float = 0,
        chain_quality: Optional[Dict] = None,
    ) -> Dict:
        # Run all intelligence modules and produce aggregate score
        results = {}
        score = 0.0  # -1.0 (bearish) to +1.0 (bullish)
        components = 0

        # Use 1m data for high-resolution intelligence if available
        intel_df = candle_1min_df if candle_1min_df is not None and not candle_1min_df.empty else candle_df

        # 1. Volume
        vol = self.volume.analyze(intel_df, instrument)
        results["volume"] = vol
        if vol["signal"] == "BULLISH_VOLUME":
            score += 0.2 # Increased from 0.15
        elif vol["signal"] == "BEARISH_VOLUME":
            score -= 0.2 # Increased from 0.15
        if vol["buy_sell_ratio"] > 1.5: # Increased threshold
            score += 0.15
        elif vol["buy_sell_ratio"] < 0.6: # Lowered threshold
            score -= 0.15
        components += 1

        # 2. OI
        if options_chain is not None:
            oi_result = self.oi.analyze(options_chain, price_change_pct, spot_price, strike_interval)
            if not isinstance(oi_result, dict): oi_result = {}
            results["oi"] = oi_result
            

            if oi_result["signal"] == "BULLISH":
                score += 0.2
            elif oi_result["signal"] == "BEARISH":
                score -= 0.2
            components += 1

            # 3. PCR
            pcr_result = self.pcr.analyze(options_chain, instrument, spot_price, strike_interval)
            if not isinstance(pcr_result, dict): pcr_result = {}
            results["pcr"] = pcr_result
            signal_map = {"STRONG_BULLISH": 0.2, "BULLISH": 0.1, "NEUTRAL": 0, "BEARISH": -0.1, "STRONG_BEARISH": -0.2}
            score += signal_map.get(pcr_result.get("signal", "NEUTRAL"), 0)
            components += 1

        # 4. Greeks & Gamma Awareness
        if options_chain is not None and spot_price > 0:
            iv_ctx = self._chain_iv_context(options_chain, spot_price, days_to_expiry)
            greeks_atm = self.greeks.analyze_atm(
                spot_price,
                strike_interval,
                days_to_expiry,
                iv_call=iv_ctx["call_iv"],
                iv_put=iv_ctx["put_iv"],
            )
            greeks_chain = self.greeks.analyze_chain(spot_price, strike_interval, days_to_expiry)
            # Dynamic IV Percentile based on Price Velocity (Mocked but Reactive)
            iv_context = greeks_atm.get("call", {}).get("iv", 15.0)
            # Volatility increases with price change speed
            v_factor = min(80, abs(price_change_pct) * 20)
            iv_percentile = max(10, min(90, 40 + v_factor + (iv_context - 15) * 2))
            
            # ── Institutional Edge: Proximity to major OI levels ──
            oi_data = results.get("oi", {})
            res_level = oi_data.get("resistance_level", 0)
            sup_level = oi_data.get("support_level", 0)
            
            dist_res = 999.0
            dist_sup = 999.0
            
            # Normalize threshold so that 0.25% at 24000 NIFTY (~60 pts) translates
            # proportionally to other instruments (e.g. ~0.50% at 12000 MIDCPNIFTY).
            dynamic_threshold = 0.25 * (24000.0 / max(1.0, spot_price))
            
            if res_level > 0:
                dist_res = (res_level - spot_price) / spot_price * 100
                if 0 < dist_res < dynamic_threshold: # Approaching major resistance wall
                    score -= 0.2 # Penalize buying into a wall
            
            if sup_level > 0:
                dist_sup = (spot_price - sup_level) / spot_price * 100
                if 0 < dist_sup < dynamic_threshold: # Approaching major support floor
                    score += 0.2 # Boost buying from a floor

            results["entry_quality"] = "EXCELLENT" if (dist_sup < dynamic_threshold or dist_res < dynamic_threshold) else "GOOD"
            results["greeks"] = {**greeks_atm, "iv_percentile": iv_percentile, "chain": greeks_chain["strikes"], "iv_source": iv_ctx["source"]}
            components += 1
        elif options_chain is None and spot_price > 0:
            # Fallback for when options chain is missing but we have price
            results["oi"] = self.oi._empty_result()
            results["pcr"] = self.pcr._empty_result()
            results["greeks"] = {
                "is_fallback": True,
                "msg": "Options data unavailable, using price-only fallback"
            }
        else:
            results["oi"] = self.oi._empty_result()
            results["pcr"] = self.pcr._empty_result()
            results["greeks"] = {}

        # 5. Regime
        regime_result = self.regime.detect(candle_df, instrument, timeframe)
        results["regime"] = regime_result
        if regime_result["regime"] in ["TRENDING_UP", "BREAKOUT"] and regime_result["direction"] == "UP":
            score += 0.25 # Increased from 0.15
        elif regime_result["regime"] in ["TRENDING_DOWN", "BREAKOUT"] and regime_result["direction"] == "DOWN":
            score -= 0.25 # Increased from 0.15
        elif regime_result["regime"] in ["VOLATILE", "SIDEWAYS", "CHOPPY"]:
            score *= 0.15  # Institutional Dampening (was 0.3)
        components += 1

        # 6. Order Flow
        flow_result = self.order_flow.analyze(
            intel_df,
            options_chain=options_chain,
            spot_price=spot_price,
            strike_interval=strike_interval,
            market_context=chain_quality,
        )
        results["order_flow"] = flow_result
        results["candle_pressure"] = {**flow_result, "source": "ohlcv_close_position"}
        if flow_result["signal"] == "BULLISH":
            score += 0.2 # Increased from 0.15
        elif flow_result["signal"] == "BEARISH":
            score -= 0.2 # Increased from 0.15
        components += 1

        # ═══ STRESS TEST: Manipulation & Divergence Detection ═══
        # Outsmarting "Stop-loss Hunting" and "False Breakouts"
        divergence_detected = False
        # Case A: Price Up but Order Flow Bearish (Distribution Trap)
        if price_change_pct > 0.1 and flow_result["ratio"] < 0.8:
            score -= 0.3
            divergence_detected = True
            logger.warning(f"⚠️ DIVERGENCE: {instrument} Price rising but Order Flow shows Distribution (Trap?)")
        # Case B: Price Down but Order Flow Bullish (Accumulation Trap)
        elif price_change_pct < -0.1 and flow_result["ratio"] > 1.25:
            score += 0.3
            divergence_detected = True
            logger.warning(f"⚠️ DIVERGENCE: {instrument} Price falling but Order Flow shows Accumulation (Shakeout?)")
        
        results["divergence_alert"] = divergence_detected

        # ═══ STRESS TEST: System CA OI (Cumulative Analysis) ═══
        if options_chain is not None and not options_chain.empty:
            # Analyze distribution of OI across entire chain (Wide Format Support)
            if 'call_oi' in options_chain.columns:
                total_call_oi = options_chain['call_oi'].sum()
                total_put_oi = options_chain['put_oi'].sum()
            else:
                total_call_oi = options_chain[options_chain['type'] == 'CE']['oi'].sum()
                total_put_oi = options_chain[options_chain['type'] == 'PE']['oi'].sum()
                
            oi_imbalance = (total_call_oi - total_put_oi) / (total_call_oi + total_put_oi) if (total_call_oi + total_put_oi) > 0 else 0
            
            # Calculate nearby strikes (+/- 5) for strike-specific analysis
            nearby_strikes = []
            if spot_price > 0:
                atm = round(spot_price / strike_interval) * strike_interval
                min_strike = atm - (5 * strike_interval)
                max_strike = atm + (5 * strike_interval)
                
                # Filter for wide format
                if 'strike' in options_chain.columns:
                    nearby_chain = options_chain[(options_chain['strike'] >= min_strike) & (options_chain['strike'] <= max_strike)]
                    nearby_strikes = nearby_chain[['strike', 'call_oi', 'put_oi', 'call_volume', 'put_volume']].to_dict('records')
            
            results["oi"]["cumulative_analysis"] = {
                "total_call_oi": int(total_call_oi),
                "total_put_oi": int(total_put_oi),
                "imbalance": round(oi_imbalance, 3),
                "sentiment": "BEARISH_PRESSURE" if oi_imbalance > 0.1 else ("BULLISH_PRESSURE" if oi_imbalance < -0.1 else "BALANCED"),
                "nearby_strikes": nearby_strikes
            }

        if chain_quality is None:
            strike_count = int(len(options_chain)) if options_chain is not None else 0
            chain_quality = {
                "source": "unknown" if strike_count else "none",
                "strike_count": strike_count,
                "score": 95 if strike_count >= 10 else (35 if strike_count == 1 else 0),
                "fallback": strike_count <= 1,
                "age_seconds": 0,
            }
        results["data_quality"] = chain_quality

        # Final Score Aggregation (Scaled to -100 to +100 for SignalManager)
        final_score = (score / components) * 100 if components > 0 else 0

        # Normalize score to [-1, 1] for legacy logic
        score = max(-1.0, min(1.0, score))

        # Final signal
        if score > 0.3:
            final_signal = "STRONG_BUY" if score > 0.6 else "BUY"
        elif score < -0.3:
            final_signal = "STRONG_SELL" if score < -0.6 else "SELL"
        else:
            final_signal = "HOLD"

        # Safely extract values for final results dict
        vol_data = results.get("volume", {})
        if not isinstance(vol_data, dict): vol_data = {}
        
        flow_data = results.get("order_flow", {})
        if not isinstance(flow_data, dict): flow_data = {}
        
        regime_data = results.get("regime", {})
        if not isinstance(regime_data, dict): regime_data = {}

        results["aggregate"] = {
            "score": round(final_score, 1),
            "signal": final_signal,
            "components_analyzed": components,
            "instrument": instrument,
            "timeframe": timeframe,
            "futures_volume": vol_data.get("current_volume", 0),
            "order_ratio": flow_data.get("ratio", 1.0),
            "regime": regime_data.get("regime", "RANGING")
        }

        return results
