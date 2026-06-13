"""
Greeks Engine — Options Greeks Calculator (Black-Scholes)
"""
import numpy as np
from scipy.stats import norm
from typing import Dict
from loguru import logger


class GreeksEngine:
    def __init__(self, risk_free_rate: float = 0.065):
        self.r = risk_free_rate

    def calculate(self, spot: float, strike: float, days: float, iv: float, opt_type: str = "call") -> Dict:
        try:
            T = days / 365.0
            if T <= 0 or iv <= 0:
                return self._zero()
            d1 = (np.log(spot / strike) + (self.r + 0.5 * iv**2) * T) / (iv * np.sqrt(T))
            d2 = d1 - iv * np.sqrt(T)
            if opt_type.lower() == "call":
                delta = norm.cdf(d1)
                theta = (-(spot * norm.pdf(d1) * iv) / (2 * np.sqrt(T)) - self.r * strike * np.exp(-self.r * T) * norm.cdf(d2))
                price = spot * norm.cdf(d1) - strike * np.exp(-self.r * T) * norm.cdf(d2)
            else:
                delta = norm.cdf(d1) - 1
                theta = (-(spot * norm.pdf(d1) * iv) / (2 * np.sqrt(T)) + self.r * strike * np.exp(-self.r * T) * norm.cdf(-d2))
                price = strike * np.exp(-self.r * T) * norm.cdf(-d2) - spot * norm.cdf(-d1)
            gamma = norm.pdf(d1) / (spot * iv * np.sqrt(T))
            vega = spot * norm.pdf(d1) * np.sqrt(T) / 100
            mn = "ATM" if abs(spot - strike) / spot < 0.005 else ("ITM" if (spot > strike if opt_type == "call" else spot < strike) else "OTM")
            return {"delta": round(float(delta), 4), "gamma": round(float(gamma), 6), "theta": round(float(theta / 365), 2), "vega": round(float(vega), 2), "price": round(float(max(0, price)), 2), "iv": round(iv * 100, 1), "moneyness": mn, "days_to_expiry": days}
        except Exception as e:
            logger.error(f"Greeks error: {e}")
            return self._zero()

    def analyze_atm(self, spot: float, strike_interval: int, days: float, iv_call: float = 0.15, iv_put: float = 0.15) -> Dict:
        atm = round(spot / strike_interval) * strike_interval
        c = self.calculate(spot, atm, days, iv_call, "call")
        p = self.calculate(spot, atm, days, iv_put, "put")
        tw = "⚠️ EXPIRY DAY!" if days <= 1 else ("⚠️ High theta near expiry" if days <= 2 else "")
        return {"atm_strike": atm, "spot": spot, "days_to_expiry": days, "call": c, "put": p, "net_delta": round(c["delta"] + p["delta"], 4), "total_gamma": round(c["gamma"] + p["gamma"], 6), "total_theta": round(c["theta"] + p["theta"], 2), "total_vega": round(c["vega"] + p["vega"], 2), "straddle_price": round(c["price"] + p["price"], 2), "theta_warning": tw}

    def analyze_chain(self, spot: float, strike_interval: int, days: float, num_strikes: int = 5) -> Dict:
        """Analyze a range of strikes around ATM for Heatmap"""
        atm = round(spot / strike_interval) * strike_interval
        results = {"strikes": []}
        
        # Scan range: ATM +/- num_strikes
        start_strike = atm - (num_strikes * strike_interval)
        for i in range(num_strikes * 2 + 1):
            strike = start_strike + (i * strike_interval)
            c = self.calculate(spot, strike, days, 0.15, "call")
            p = self.calculate(spot, strike, days, 0.15, "put")
            
            results["strikes"].append({
                "strike": strike,
                "call_delta": c["delta"],
                "call_gamma": c["gamma"],
                "call_theta": c["theta"],
                "put_delta": p["delta"],
                "put_gamma": p["gamma"],
                "put_theta": p["theta"],
                "is_atm": strike == atm
            })
        return results

    def _zero(self) -> Dict:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "price": 0.0, "iv": 0.0, "moneyness": "UNKNOWN", "days_to_expiry": 0}
