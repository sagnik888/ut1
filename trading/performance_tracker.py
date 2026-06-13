"""
Performance Tracker — Win Rate, Sharpe, Profit Factor, Drawdown
"""
import numpy as np
from typing import Dict, List
from trading.trade_manager import Trade


class PerformanceTracker:
    def reset(self):
        """Reset performance metrics"""
        pass

    def calculate(self, trades: List[Trade]) -> Dict:
        valid_trades = [t for t in trades if not getattr(t, 'is_ghost', False)]
        if not valid_trades:
            return self._empty()
        pnls = [t.pnl for t in valid_trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        total_pnl = sum(pnls)
        win_rate = len(wins) / len(pnls) * 100 if pnls else 0
        avg_win = np.mean(wins) if wins else 0
        avg_loss = abs(np.mean(losses)) if losses else 1
        profit_factor = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else 99.99
        # Sharpe (daily approximation) - Pure Python to avoid numpy edge cases
        if len(pnls) > 1:
            pnl_mean = sum(pnls) / len(pnls)
            variance = sum((x - pnl_mean) ** 2 for x in pnls) / len(pnls)
            std = variance ** 0.5
            sharpe = (pnl_mean / std) * (252 ** 0.5) if std > 0 else 0.0
        else:
            sharpe = 0.0
        # Max drawdown
        equity = np.cumsum(pnls)
        peak = np.maximum.accumulate(equity)
        dd = peak - equity
        max_dd = float(np.max(dd)) if len(dd) > 0 else 0
        # Expectancy
        expectancy = (win_rate / 100 * avg_win) - ((1 - win_rate / 100) * avg_loss)
        return {
            "total_trades": len(trades),
            "wins": len(wins), "losses": len(losses),
            "win_rate": round(win_rate, 1),
            "total_pnl": round(total_pnl, 0),
            "avg_win": round(avg_win, 0),
            "avg_loss": round(avg_loss, 0),
            "profit_factor": round(profit_factor, 2),
            "sharpe_ratio": round(sharpe, 2),
            "max_drawdown": round(max_dd, 0),
            "expectancy": round(expectancy, 1),
            "rr_ratio": round(avg_win / avg_loss, 2) if avg_loss > 0 else 0,
        }

    def reset(self):
        """No internal state to reset, but provided for API compatibility"""
        pass

    def _empty(self) -> Dict:
        return {"total_trades": 0, "wins": 0, "losses": 0, "win_rate": 0, "total_pnl": 0, "avg_win": 0, "avg_loss": 0, "profit_factor": 0, "sharpe_ratio": 0, "max_drawdown": 0, "expectancy": 0, "rr_ratio": 0}
