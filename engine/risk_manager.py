import asyncio
from loguru import logger
from datetime import datetime
from typing import Dict, List, Optional
from trading.trade_manager import IST, TradeManager

class RiskManager:
    """
    Handles all risk-related logic:
    - Daily loss limits (Circuit Breakers)
    - Stop loss & Target management
    - Position sizing
    - Trailing SL logic
    """
    
    def __init__(self, trade_manager: TradeManager, capital_total: float):
        self.trades = trade_manager
        self.capital_total = capital_total
        
        # State
        self.daily_loss_breached = False
        self.fut_loss_breached = False
        self.opt_loss_breached = False
        self._session_date = datetime.now(IST).date()
        
        # Risk settings
        self.max_daily_loss_pct = 3.0
        self.risk_fut_pct = 2.0
        self.risk_opt_pct = 5.0
        self.options_sl_pct = 15.0
        self.circuit_breaker_slippage_bps = 10.0
        
        # Capital allocation
        self.capital_fut = capital_total * 0.5
        self.capital_opt = capital_total * 0.5

    def update_settings(self, **kwargs):
        """Update risk parameters dynamically"""
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
        logger.info(f"🛡️ RiskManager updated: {kwargs}")

    def check_circuit_breaker(self) -> bool:
        """
        Hard stop if daily loss exceeds limits.
        Returns True if system should HALT.
        """
        self.reset_for_session(datetime.now(IST).date())
        if self.capital_total <= 0: return False
        
        today = datetime.now(IST).date()
        fut_pnl = 0.0
        opt_pnl = 0.0
        
        # Calculate Realized + Unrealized per segment
        # 1. Closed Trades for today
        for t in self.trades.closed_trades:
            try:
                et = t.entry_time.astimezone(IST) if t.entry_time.tzinfo else IST.localize(t.entry_time)
                if et.date() == today:
                    if t.inst_type == "FUT": fut_pnl += t.pnl
                    else: opt_pnl += t.pnl
            except Exception:
                pass
                
        # 2. Open Trades (Unrealized)
        for t in self.trades.open_trades.values():
            if str(getattr(t, "execution_status", "") or "").upper() == "RECOVERY_REQUIRED":
                continue
            try:
                et = t.entry_time.astimezone(IST) if t.entry_time.tzinfo else IST.localize(t.entry_time)
                if et.date() == today:
                    open_pnl = float(getattr(t, 'unrealized_pnl', 0) or 0.0)
                    exit_price = float(getattr(t, "current_price", 0.0) or getattr(t, "entry_price", 0.0) or 0.0)
                    quantity = max(1, int(getattr(t, "lots", 1) or 1) * int(getattr(t, "lot_size", 1) or 1))
                    multiplier = abs(float(getattr(t, "instrument_multiplier", 1.0) or 1.0))
                    slippage_buffer = (
                        exit_price
                        * quantity
                        * multiplier
                        * max(0.0, float(self.circuit_breaker_slippage_bps or 0.0))
                        / 10000.0
                    )
                    open_pnl -= slippage_buffer
                    if t.inst_type == "FUT": fut_pnl += open_pnl
                    else: opt_pnl += open_pnl
            except Exception:
                pass
        
        # Calculate limits in absolute terms
        fut_limit = -(self.capital_fut * (self.risk_fut_pct / 100.0))
        opt_limit = -(self.capital_opt * (self.risk_opt_pct / 100.0))
        global_limit = -(self.capital_total * (self.max_daily_loss_pct / 100.0))
        
        total_pnl = fut_pnl + opt_pnl
        
        # ── 🚨 GLOBAL BREACH 🚨 ──
        if total_pnl < global_limit and not self.daily_loss_breached:
            self.daily_loss_breached = True
            msg = f"💀 GLOBAL DAILY LOSS LIMIT BREACHED: {abs(total_pnl/self.capital_total*100):.1f}%"
            logger.error(msg)
            if hasattr(self.trades, '_log_ui'): self.trades._log_ui(msg, "error")
            self.halt_trading("GLOBAL_DAILY_LOSS_LIMIT")
            return True
            
        # ── 🚨 FUT BREACH 🚨 ──
        if fut_pnl < fut_limit and not self.fut_loss_breached:
            self.fut_loss_breached = True
            msg = f"🚨 FUT DAILY LOSS LIMIT BREACHED: {abs(fut_pnl/self.capital_fut*100):.1f}%"
            logger.warning(msg)
            if hasattr(self.trades, '_log_ui'): self.trades._log_ui(msg, "warning")
            self.close_segment_trades("FUT", "FUT_DAILY_LOSS_LIMIT")
            
        # ── 🚨 OPT BREACH 🚨 ──
        if opt_pnl < opt_limit and not self.opt_loss_breached:
            self.opt_loss_breached = True
            msg = f"🚨 OPT DAILY LOSS LIMIT BREACHED: {abs(opt_pnl/self.capital_opt*100):.1f}%"
            logger.warning(msg)
            if hasattr(self.trades, '_log_ui'): self.trades._log_ui(msg, "warning")
            self.close_segment_trades("OPT", "OPT_DAILY_LOSS_LIMIT")
            
        return self.daily_loss_breached

    def halt_trading(self, reason: str):
        """Emergency stop: Close ALL positions and disable entry"""
        logger.error(f"🛑 HALTING ALL TRADING: {reason}")
        for tid, t in list(self.trades.open_trades.items()):
            if str(getattr(t, "execution_status", "") or "").upper() == "RECOVERY_REQUIRED":
                logger.critical(f"Skipped auto-halt close for recovery-required trade {tid}; broker reconciliation required.")
                continue
            self.trades.close_trade(tid, t.current_price, reason)

    def close_segment_trades(self, segment: str, reason: str):
        """Close all positions in a specific segment (FUT or OPT)"""
        logger.warning(f"🚫 Closing all {segment} positions: {reason}")
        for tid, t in list(self.trades.open_trades.items()):
            if str(getattr(t, "execution_status", "") or "").upper() == "RECOVERY_REQUIRED":
                logger.critical(f"Skipped segment auto-close for recovery-required trade {tid}; broker reconciliation required.")
                continue
            if t.inst_type == segment:
                self.trades.close_trade(tid, t.current_price, reason)

    def can_trade(self, inst_type: str) -> bool:
        """Check if trading is allowed for a segment"""
        self.reset_for_session(datetime.now(IST).date())
        if self.daily_loss_breached: return False
        if inst_type == "FUT" and self.fut_loss_breached: return False
        if inst_type == "OPT" and self.opt_loss_breached: return False
        return True

    def reset_for_session(self, session_date) -> bool:
        """Reset latched daily breakers exactly once when the session date changes."""
        if self._session_date == session_date:
            return False
        self._session_date = session_date
        self.daily_loss_breached = False
        self.fut_loss_breached = False
        self.opt_loss_breached = False
        logger.info(f"Risk circuit breakers reset for session {session_date}.")
        return True
