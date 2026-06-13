"""
Trade Manager — Position Lifecycle with Smart Exits
═══════════════════════════════════════════════════════════════

Handles:
1. Trade open/close with full audit trail
2. 1-min trailing stop updates
3. Automatic trailing stop exit detection
4. Session-end auto-exit
5. P&L tracking
"""

import uuid
import re
import threading
import time
from datetime import date, datetime, time as dtime, timedelta
import pytz
IST = pytz.timezone('Asia/Kolkata')
from typing import Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, replace
from loguru import logger


def _setting_enabled(value, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    return bool(value)


@dataclass
class Trade:
    id: str
    instrument: str
    timeframe: str
    direction: str          # LONG or SHORT
    entry_price: float
    entry_time: datetime
    trailing_stop: float    # Initial trailing stop at entry
    current_stop: float     # Current trailing stop (1-min updated)
    lots: int
    lot_size: int
    grade: str
    # ═══ Execution Details ═══
    broker_order_id: str = ""
    pending_execution: bool = False
    exit_order_id: str = ""
    execution_status: str = "OPEN"
    execution_error: str = ""
    pending_exit: bool = False
    broker_quantity: int = 0
    trading_symbol: str = ""
    symbol_token: str = ""
    # ═══ NEW: Instrument & signal details ═══
    atm_strike: float = 0.0
    option_type: str = ""   # CE or PE
    target: float = 0.0     # Target price (1.5× risk)
    rr_ratio: float = 1.5   # Risk-reward ratio
    confidence: float = 0.0 # Signal confidence 0.0-1.0
    inst_type: str = "FUT"  # FUT or OPT
    instrument_multiplier: float = 1.0 # 1.0 for FUT, Delta (~0.5) for OPT
    exec_type: str = "A"    # A for Auto, M for Manual
    status: str = "OPEN"    # OPEN / CLOSED
    current_price: float = 0.0
    entry_spot: float = 0.0  # Spot price at entry (for option tracking)
    spot_stop: float = 0.0   # Original index/spot stop for premium-led option trades
    spot_target: float = 0.0 # Original index/spot target for premium-led option trades
    exit_price: float = 0.0
    exit_time: Optional[datetime] = None
    exit_reason: str = ""
    pnl: float = 0.0
    charges: float = 0.0
    peak_pnl: float = 0.0
    max_drawdown: float = 0.0
    is_ghost: bool = False

    def to_dict(self) -> Dict:
        """Convert trade to dictionary for dashboard sync"""
        display_name = self.instrument
        if self.inst_type == "OPT" and self.atm_strike is not None and self.atm_strike > 0:
            display_name = f"{self.instrument} {int(self.atm_strike)} {self.option_type}"
            
        current_display_price = self.current_price
        if self.status == "CLOSED":
            current_display_price = (
                self.current_price
                if self.current_price is not None and self.current_price > 0 and abs(self.current_price - self.exit_price) > 1e-9
                else 0.0
            )
            
        return {
            "id": self.id,
            "status": self.status,
            "is_ghost": getattr(self, 'is_ghost', False),
            "execution_status": self.execution_status,
            "execution_error": self.execution_error,
            "pending_execution": self.pending_execution,
            "pending_exit": self.pending_exit,
            "broker_order_id": self.broker_order_id,
            "exit_order_id": self.exit_order_id,
            "broker_quantity": self.broker_quantity,
            "instrument": display_name,
            "direction": self.direction,
            "entry_price": self.entry_price,
            "current_price": current_display_price,
            "exit_price": self.exit_price if self.status == "CLOSED" else 0.0,
            "trailing_stop": self.current_stop if self.current_stop is not None else 0.0,
            "target": self.target if self.target is not None else 0.0,
            "pnl": round((self.unrealized_pnl if self.status == "OPEN" else self.pnl) or 0.0, 0),
            "charges": self.charges,
            "lots": self.lots,
            "lot_size": self.lot_size,
            "grade": self.grade,
            "confidence": round(self.confidence or 0.0, 2),
            "rr_ratio": self.rr_ratio,
            "atm_strike": self.atm_strike,
            "option_type": self.option_type,
            "inst_type": self.inst_type,
            "timeframe": self.timeframe,
            "entry_time": self.entry_time.strftime("%d %b %H:%M:%S") if self.entry_time else "--",
            "exit_time": self.exit_time.strftime("%d %b %H:%M:%S") if self.exit_time else "--",
            "entry_timestamp": self.entry_time.timestamp() if self.entry_time else 0,
            "exit_timestamp": self.exit_time.isoformat() if self.exit_time else "",
            "exit_reason": self.exit_reason,
            "exec_type": self.exec_type,
            "entry_spot": self.entry_spot,
            "spot_stop": self.spot_stop,
            "spot_target": self.spot_target,
            "instrument_multiplier": self.instrument_multiplier,
            "peak_pnl": self.peak_pnl,
            "max_drawdown": self.max_drawdown,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'Trade':
        """Reconstruct a Trade from a serialized dictionary with IST awareness"""
        import pytz
        ist = pytz.timezone('Asia/Kolkata')
        
        ts = data.get("entry_timestamp", 0)
        entry_time = datetime.fromtimestamp(ts, ist) if ts else datetime.now(ist)
        
        exit_time = None
        if data.get("exit_timestamp"):
            try:
                exit_time = datetime.fromisoformat(str(data["exit_timestamp"]))
                if exit_time.tzinfo is None:
                    exit_time = ist.localize(exit_time)
                else:
                    exit_time = exit_time.astimezone(ist)
            except Exception as e:
                logger.debug(f"Failed to parse canonical exit timestamp: {e}")
                exit_time = None
        if exit_time is None and data.get("exit_time") and data["exit_time"] != "--":
            try:
                # Parse display string but force IST context
                exit_year = datetime.now(ist).year
                exit_time = datetime.strptime(f"{exit_year} {data['exit_time']}", "%Y %d %b %H:%M:%S")
                exit_time = ist.localize(exit_time)
            except Exception as e:

                logger.debug(f"Caught bare exception: {e}")
                exit_time = None
        
        # Extract base instrument name (strip strike info like "NIFTY 24100 CE" -> "NIFTY")
        raw_instrument = data.get("instrument", "")
        base_instrument = raw_instrument.split()[0] if raw_instrument else ""
        
        return cls(
            id=data.get("id", ""),
            instrument=base_instrument,
            timeframe=data.get("timeframe", "5min"),
            direction=data.get("direction", "LONG"),
            entry_price=data.get("entry_price", 0.0),
            entry_time=entry_time,
            trailing_stop=data.get("trailing_stop", 0.0),
            current_stop=data.get("trailing_stop", 0.0),
            lots=data.get("lots", 1),
            lot_size=data.get("lot_size", 1),
            grade=data.get("grade", "B"),
            atm_strike=data.get("atm_strike", 0.0),
            option_type=data.get("option_type", ""),
            target=data.get("target", 0.0),
            rr_ratio=data.get("rr_ratio", 1.5),
            confidence=data.get("confidence", 0.0),
            inst_type=data.get("inst_type", "FUT"),
            instrument_multiplier=data.get("instrument_multiplier", 1.0),
            exec_type=data.get("exec_type", "A"),
            status=data.get("status", "CLOSED"),
            broker_order_id=data.get("broker_order_id", ""),
            exit_order_id=data.get("exit_order_id", ""),
            execution_status=data.get("execution_status") or data.get("status", "CLOSED"),
            execution_error=data.get("execution_error", ""),
            pending_execution=bool(data.get("pending_execution", False)),
            pending_exit=bool(data.get("pending_exit", False)),
            broker_quantity=int(data.get("broker_quantity", 0) or 0),
            trading_symbol=data.get("trading_symbol", ""),
            symbol_token=data.get("symbol_token", ""),
            current_price=data.get("current_price", 0.0),
            entry_spot=data.get("entry_spot", 0.0),
            spot_stop=data.get("spot_stop", 0.0),
            spot_target=data.get("spot_target", 0.0),
            exit_price=data.get("exit_price", data.get("current_price", 0.0)),
            exit_time=exit_time,
            exit_reason=data.get("exit_reason", ""),
            pnl=data.get("pnl", 0.0),
            peak_pnl=data.get("peak_pnl", 0.0),
            max_drawdown=data.get("max_drawdown", 0.0),
            is_ghost=bool(data.get("is_ghost", False)),
        )

    @property
    def quantity(self) -> int:
        return self.lots * self.lot_size

    @property
    def unrealized_pnl(self) -> float:
        """Net P&L including estimated charges and instrument multipliers"""
        if self.status != "OPEN":
            return 0.0
        
        from config.settings import get_settings
        settings = get_settings()
        from engine.trade_accounting import estimate_trade_charges
        est_charges = estimate_trade_charges(
            self.entry_price,
            self.current_price,
            self.quantity,
            self.inst_type,
            settings,
            self.instrument_multiplier,
        )
        
        # NOTE: self.current_price is ALREADY adjusted for Delta in update_trade()
        # For Options, we are always LONG (buying CE or PE).
        if self.inst_type == "OPT":
            gross = (self.current_price - self.entry_price) * self.quantity
        else:
            # For Futures, it depends on direction
            if self.direction == "LONG":
                gross = (self.current_price - self.entry_price) * self.quantity
            else:
                gross = (self.entry_price - self.current_price) * self.quantity
            
        return gross - est_charges


import json
import os
from data.sqlite_db import db

class TradeManager:
    """Full trade lifecycle management with persistence"""

    def _log_ui(self, message: str, level: str = "info"):
        if self.ui_logger:
            try:
                self.ui_logger(message, level)
            except Exception as e:
                logger.error(f"Failed to dispatch UI log: {e}")

    def __init__(
        self,
        broker=None,
        max_positions: int = 3,
        session_end: str = "15:25",
        product_type: str = "CARRYFORWARD",
    ):
        self.broker = broker
        self.ui_logger = None
        self.max_positions = max_positions
        self.session_end = self._parse_time(session_end)
        self.product_type = str(product_type or "CARRYFORWARD").upper()
        self.open_trades: Dict[str, Trade] = {}
        self.closed_trades: List[Trade] = []
        self._trade_counter = 0
        self._lock = threading.RLock()
        self._state_file = "data_store/trade_state.json"
        self._last_mark_to_market_save = 0.0
        self.orphan_positions: Dict[str, Dict] = {}
        
        # Risk settings (updated via Scanner.configure)
        self.futures_sl_pct = 0.30
        self.options_sl_pct = 15.0
        
        # Load state on init
        self.load_state()
        
        # Start background broker position sync
        self._sync_active = True
        self._sync_thread = threading.Thread(target=self._position_sync_loop, daemon=True, name="PositionSync")
        self._sync_thread.start()

    def _position_sync_loop(self):
        """Background loop to sync internal open_trades with broker positions."""
        import time
        while getattr(self, "_sync_active", True):
            time.sleep(5) # Faster sync to free up concurrency slots for next signals
            try:
                self.reconcile_broker_positions()
            except Exception as exc:
                logger.error(f"Error in position sync loop: {exc}")
            continue
            if not self.broker or not self.open_trades:
                continue

            get_positions = getattr(self.broker, "get_positions", None)
            if not callable(get_positions):
                continue

            closed_tids = []
            try:
                positions_resp = get_positions()
                if not positions_resp or not isinstance(positions_resp, dict):
                    continue
                    
                data_list = positions_resp.get("data", [])
                if not isinstance(data_list, list):
                    continue
                
                # Map broker positions by symbol
                broker_pos = {}
                allowed_prefixes = ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX")
                for p in data_list:
                    sym = p.get("tradingsymbol", "")
                    if sym and sym.upper().startswith(allowed_prefixes):
                        broker_pos[sym] = p

                # Check each open trade against the broker
                with self._lock:
                    open_snapshot = list(self.open_trades.items())
                for tid, trade in open_snapshot:
                    # AngelOne sometimes drops CE/PE suffixes or formats uniquely, but generally tradingsymbol matches.
                    sym = trade.trading_symbol or trade.instrument
                    if sym in broker_pos:
                        p_data = broker_pos[sym]
                        net_qty = int(p_data.get("netqty", 0))
                        
                        # If netqty is 0, the user manually closed it (or SL hit broker-side)
                        if net_qty == 0:
                            logger.info(f"🔄 Manual broker exit detected for {sym}. Auto-closing in bot.")
                            realised_pnl = float(p_data.get("realised", 0.0))
                            
                            trade.status = "CLOSED"
                            trade.exit_price = float(p_data.get("sellavgprice", 0.0)) if trade.direction in ["LONG", "BUY"] else float(p_data.get("buyavgprice", 0.0))
                            if trade.exit_price == 0.0:
                                trade.exit_price = trade.current_price
                            trade.exit_time = datetime.now(IST)
                            trade.exit_reason = "MANUAL_SYNC"
                            trade.pnl = realised_pnl
                            
                            closed_tids.append((tid, trade))
                            
            except Exception as e:
                logger.error(f"Error in position sync loop: {e}")
                
            # Process the synced closed trades
            with self._lock:
                for tid, trade in closed_tids:
                    if tid in self.open_trades:
                        self.closed_trades.append(trade)
                        del self.open_trades[tid]
            
            if closed_tids:
                self.save_state()

    @staticmethod
    def _position_quantity(position: Dict) -> int:
        try:
            return int(float(position.get("netqty", 0) or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _position_exit_price(trade: Trade, position: Dict) -> float:
        value = (
            position.get("sellavgprice", 0.0)
            if trade.direction in {"LONG", "BUY"} or trade.inst_type == "OPT"
            else position.get("buyavgprice", 0.0)
        )
        try:
            price = float(value or 0.0)
        except (TypeError, ValueError):
            price = 0.0
        return price if price > 0 else (trade.current_price or trade.entry_price)

    def _finalize_closed_trade(
        self,
        trade_id: str,
        trade: Trade,
        exit_price: float,
        reason: str,
        exit_time: Optional[datetime] = None,
        realised_pnl: Optional[float] = None,
    ) -> bool:
        from config.settings import get_settings
        from engine.trade_accounting import estimate_trade_charges

        with self._lock:
            if trade_id not in self.open_trades:
                return False
            trade.status = "CLOSED"
            trade.execution_status = "CLOSED"
            trade.pending_execution = False
            trade.pending_exit = False
            trade.execution_error = ""
            trade.exit_price = float(exit_price or trade.current_price or trade.entry_price)
            trade.current_price = trade.exit_price
            trade.exit_time = exit_time or datetime.now(IST)
            trade.exit_reason = reason
            if trade.inst_type == "OPT":
                gross = (trade.exit_price - trade.entry_price) * trade.quantity
            elif trade.direction == "LONG":
                gross = (trade.exit_price - trade.entry_price) * trade.quantity
            else:
                gross = (trade.entry_price - trade.exit_price) * trade.quantity
            trade.charges = estimate_trade_charges(
                trade.entry_price,
                trade.exit_price,
                trade.quantity,
                trade.inst_type,
                get_settings(),
                trade.instrument_multiplier,
            )
            trade.pnl = float(realised_pnl) if realised_pnl is not None else gross - trade.charges
            self.closed_trades.append(trade)
            del self.open_trades[trade_id]
        self.save_state()
        
        profit_str = f"+₹{trade.pnl:.2f}" if trade.pnl >= 0 else f"-₹{abs(trade.pnl):.2f}"
        msg = f"✅ Trade Closed: {trade.instrument} @ {trade.exit_price:.2f} (Reason: {reason}) | PnL: {profit_str}"
        logger.info(msg)
        self._log_ui(msg, "trade")
        
        return True

    def reconcile_broker_positions(self, positions_resp: Optional[Dict] = None) -> Dict:
        """Reconcile local trades and broker-only positions in both directions."""
        if not self.broker:
            return {"checked": False, "orphans": 0, "closed": 0, "confirmed": 0}
        get_positions = getattr(self.broker, "get_positions", None)
        if positions_resp is None:
            if not callable(get_positions):
                return {"checked": False, "orphans": 0, "closed": 0, "confirmed": 0}
            positions_resp = get_positions()
        if not isinstance(positions_resp, dict):
            return {"checked": False, "orphans": 0, "closed": 0, "confirmed": 0}
        rows = positions_resp.get("data", [])
        if not isinstance(rows, list):
            return {"checked": False, "orphans": 0, "closed": 0, "confirmed": 0}

        allowed_prefixes = ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX")
        broker_pos = {
            str(row.get("tradingsymbol") or ""): row
            for row in rows
            if str(row.get("tradingsymbol") or "") and str(row.get("tradingsymbol") or "").upper().startswith(allowed_prefixes)
        }
        closed = 0
        confirmed = 0
        changed = False
        with self._lock:
            local_snapshot = list(self.open_trades.items())
        local_symbols = set()
        for trade_id, trade in local_snapshot:
            symbol = str(trade.trading_symbol or trade.instrument)
            local_symbols.add(symbol)
            position = broker_pos.get(symbol)
            if position is None:
                continue
            net_qty = self._position_quantity(position)
            trade.broker_quantity = abs(net_qty)
            expected_qty = max(1, trade.quantity)
            if net_qty == 0:
                reason = (
                    trade.exit_reason
                    if trade.execution_status == "EXIT_PENDING" and trade.exit_reason
                    else "MANUAL_SYNC"
                )
                realised = position.get("realised")
                try:
                    realised_pnl = float(realised) if realised not in (None, "") else None
                except (TypeError, ValueError):
                    realised_pnl = None
                if self._finalize_closed_trade(
                    trade_id,
                    trade,
                    self._position_exit_price(trade, position),
                    reason,
                    realised_pnl=realised_pnl,
                ):
                    closed += 1
                continue
            if trade.execution_status in {"ENTRY_PENDING", "ENTRY_SUBMITTED", "RECOVERY_REQUIRED"}:
                trade.pending_execution = False
                trade.execution_status = "OPEN" if abs(net_qty) == expected_qty else "PARTIAL"
                
                # ACTUAL BROKER FILL PRICE SYNC
                fill_price = float(position.get("buyavgprice", 0.0) if trade.direction in {"LONG", "BUY"} or trade.inst_type == "OPT" else position.get("sellavgprice", 0.0))
                if fill_price > 0.1 and abs(fill_price - trade.entry_price) / max(1, trade.entry_price) < 0.2:
                    trade.entry_price = fill_price
                
                trade.execution_error = ""
                confirmed += 1
                changed = True
            elif abs(net_qty) != expected_qty and trade.execution_status != "EXIT_PENDING":
                trade.execution_status = "PARTIAL"
                changed = True

        orphans = {}
        for symbol, position in broker_pos.items():
            if self._position_quantity(position) != 0 and symbol not in local_symbols:
                orphan = dict(position)
                orphan["execution_status"] = "ORPHANED"
                orphan["status"] = "ORPHANED"
                orphans[symbol] = orphan
        if orphans != self.orphan_positions:
            self.orphan_positions = orphans
            changed = True
            if orphans:
                logger.critical(
                    "Broker-only positions require reconciliation: "
                    + ", ".join(sorted(orphans))
                )
        if changed:
            self.save_state()
        return {
            "checked": True,
            "orphans": len(orphans),
            "closed": closed,
            "confirmed": confirmed,
        }

    def update_risk_settings(self, futures_sl_pct: float = None, options_sl_pct: float = None):
        """Update settings and re-calibrate open trades if needed"""
        if futures_sl_pct is not None:
            self.futures_sl_pct = futures_sl_pct
        if options_sl_pct is not None:
            self.options_sl_pct = options_sl_pct
            
        # Optional: Sync open trades to new settings (Aggressive approach)
        # For now, we just update the manager state so NEW trades get the correct SL.
        # If we want to move EXISTING stops, we would loop through self.open_trades here.
        logger.info(f"🛡️ Risk Settings Synced: FUT SL={self.futures_sl_pct}%, OPT SL={self.options_sl_pct}%")

    def save_state(self):
        """Save active and recent closed trades to SQLite for crash recovery"""
        try:
            with self._lock:
                open_tr = list(self.open_trades.values())
            with self._lock:
                self.closed_trades = self.closed_trades[-500:]
                closed = list(self.closed_trades)
            db.save_trades([t.to_dict() for t in open_tr + closed])
        except Exception as e:
            logger.error(f"Failed to save trade state to DB: {e}")

    def load_state(self):
        """Load state from SQLite on startup"""
        try:
            open_records = db.load_open_trades()
            closed_records = db.load_closed_trades()
            
            for data in open_records:
                tid = data.get("id", "")
                if not tid: continue
                t = Trade.from_dict(data)
                t.status = "OPEN"
                if t.entry_time.date() < datetime.now(IST).date():
                    t.execution_status = "RECOVERY_REQUIRED"
                    t.execution_error = "Previous-session trade requires broker reconciliation"
                    logger.critical(f"Previous-session active trade retained for reconciliation: {tid}")
                elif not t.execution_status:
                    t.execution_status = "OPEN"
                self.open_trades[tid] = t
                continue
                # Reconstruct Trade object with IST awareness
                import pytz
                ist = pytz.timezone('Asia/Kolkata')
                
                ts = data.get("entry_timestamp")
                entry_time = datetime.fromtimestamp(ts, ist) if ts else datetime.now(ist)
                
                t = Trade(
                    id=tid,
                    instrument=data.get("instrument", ""),
                    timeframe=data.get("timeframe", "5min"),
                    direction=data.get("direction", "LONG"),
                    entry_price=data.get("entry_price", 0.0),
                    entry_time=entry_time,
                    trailing_stop=data.get("trailing_stop", 0.0),
                    current_stop=data.get("trailing_stop", 0.0),
                    lots=data.get("lots", 1),
                    lot_size=data.get("lot_size", 1),
                    grade=data.get("grade", "B"),
                    atm_strike=data.get("atm_strike", 0.0),
                    option_type=data.get("option_type", ""),
                    target=data.get("target", 0.0),
                    rr_ratio=data.get("rr_ratio", 1.5),
                    confidence=data.get("confidence", 0.0),
                    inst_type=data.get("inst_type", "FUT"),
                    instrument_multiplier=data.get("instrument_multiplier", 1.0),
                    exec_type=data.get("exec_type", "A"),
                    entry_spot=data.get("entry_spot", data.get("entry_price", 0.0)),
                    status="OPEN"
                )
                
                # ═══ STALE TRADE GUARD ═══
                # Auto-close trades left open from previous days
                trade_date = entry_time.date()
                if trade_date < datetime.now(ist).date():
                    from config.settings import get_settings
                    force_exit = self._parse_time(
                        str(getattr(get_settings(), "ut_force_exit_time", "15:25") or "15:25")
                    )
                    t.status = "CLOSED"
                    t.exit_time = ist.localize(datetime.combine(trade_date, force_exit))
                    t.exit_reason = "SESSION_END"
                    self.closed_trades.append(t)
                    logger.info(f"🧹 Expired stale open trade from {trade_date}: {tid}")
                else:
                    self.open_trades[tid] = t
            
            # Load Closed Trades (History)
            for data in closed_records:
                try:
                    t = Trade.from_dict(data)
                    t.status = "CLOSED"
                    self.closed_trades.append(t)
                except Exception as e:
                    logger.warning(f"Failed to parse historical trade: {e}")
            
            self._recover_trade_counter()
            if self.open_trades or self.closed_trades:
                logger.success(f"♻️ Recovered {len(self.open_trades)} active and {len(self.closed_trades)} historical trades from DB.")
        except Exception as e:
            logger.error(f"Failed to load trade state from DB: {e}")

    def _recover_trade_counter(self) -> None:
        """Continue generated trade IDs above every persisted T-number."""
        persisted = list(self.open_trades.values()) + list(self.closed_trades)
        for trade in persisted:
            match = re.match(r"^T(\d+)_", str(getattr(trade, "id", "") or ""))
            if match:
                self._trade_counter = max(self._trade_counter, int(match.group(1)))

    def reset_pnl(self):
        """Reset all trades and P&L for a fresh simulation"""
        with self._lock:
            self.open_trades.clear()
            self.closed_trades.clear()
        try:
            db.conn.execute("DELETE FROM trades")
            db.conn.commit()
        except Exception:
            pass
        logger.info("🧹 Trade Manager: Cleared all trades and P&L from DB.")

    def reset_session(self, session_date: date) -> Dict[str, int]:
        """Start a new accounting session without deleting active broker state."""
        with self._lock:
            active = len(self.open_trades)
            closed = len(self.closed_trades)
            for trade in self.open_trades.values():
                if trade.entry_time.date() < session_date:
                    trade.execution_status = "RECOVERY_REQUIRED"
                    trade.execution_error = "Previous-session trade requires broker reconciliation"
        self.save_state()
        if active:
            logger.critical(
                f"Session rollover retained {active} active trade(s); broker reconciliation required."
            )
        return {"active_retained": active, "closed_history_retained": closed}

    def _parse_time(self, t: str) -> dtime:
        parts = t.split(":")
        return dtime(int(parts[0]), int(parts[1]))

    def can_open_trade(self, is_recovery: bool = False) -> Tuple[bool, str]:
        """Check if we can open a new trade"""
        with self._lock:
            if not is_recovery and self.orphan_positions:
                return False, "Broker-only orphan position requires reconciliation"
            if not is_recovery and len(self.open_trades) >= self.max_positions:
                return False, f"Max positions ({self.max_positions}) reached"
        return True, ""

    @staticmethod
    def _extract_order_id(result) -> str:
        if not result:
            return ""
        if isinstance(result, dict):
            if result.get("status") is False or result.get("errorcode"):
                return ""
            data = result.get("data", result)
            if isinstance(data, dict):
                return str(data.get("orderid") or data.get("uniqueorderid") or "")
            return str(data or "")
        return str(result)

    def open_trade(
        self,
        instrument: str,
        timeframe: str,
        direction: str,
        price: float,
        trailing_stop: float,
        lots: int,
        lot_size: int,
        grade: str,
        atm_strike: float = 0.0,
        option_type: str = "",
        target: float = 0.0,
        rr_ratio: float = 1.5,
        confidence: float = 0.0,
        instrument_multiplier: float = 1.0,
        trading_symbol: str = "",
        symbol_token: str = "",
        inst_type: str = "FUT",
        exec_type: str = "A",
        entry_spot: float = 0.0,
        spot_stop: float = 0.0,
        spot_target: float = 0.0,
        is_recovery: bool = False,
        is_ghost: bool = False,
        entry_time: Optional[datetime] = None,
        signal_time: Optional[datetime] = None,
        on_execution_result: Optional[Callable[[Trade, bool], None]] = None,
    ) -> Optional[Trade]:
        """Open a new trade and execute via broker"""
        if entry_time is not None:
            now = entry_time
        elif is_recovery and signal_time is not None:
            now = signal_time
        else:
            now = datetime.now(IST)
        with self._lock:
            can, reason = self.can_open_trade(is_recovery)
            if not can:
                logger.warning(f"Cannot open trade: {reason}")
                return None
            known_ids = set(self.open_trades)
            known_ids.update(str(trade.id) for trade in self.closed_trades)
            while True:
                self._trade_counter += 1
                trade_id = f"T{self._trade_counter:04d}_{instrument}_{now.strftime('%m%d%H%M')}"
                if trade_id not in known_ids:
                    break

        trade = Trade(
            id=trade_id,
            instrument=instrument,
            timeframe=timeframe,
            direction=direction,
            entry_price=price,
            entry_time=now,
            trailing_stop=trailing_stop,
            current_stop=trailing_stop,
            lots=lots,
            lot_size=lot_size,
            grade=grade,
            atm_strike=atm_strike,
            option_type=option_type,
            target=target,
            rr_ratio=rr_ratio,
            confidence=confidence,
            inst_type=inst_type,
            instrument_multiplier=instrument_multiplier,
            current_price=price,
            trading_symbol=trading_symbol,
            symbol_token=symbol_token,
            exec_type=exec_type,
            entry_spot=entry_spot or price,
            spot_stop=spot_stop,
            spot_target=spot_target,
            is_ghost=is_ghost
        )
        
        trade.pending_execution = not is_recovery
        trade.execution_status = "OPEN" if (is_recovery or is_ghost) else "ENTRY_PENDING"
        with self._lock:
            self.open_trades[trade_id] = trade
        self.save_state()

        qty = lots * lot_size
        logger.info(
            f"🟢 TRADE LOGGED: {trade_id} | {direction} {trading_symbol or instrument} @ {price:.2f} | "
            f"Stop: {trailing_stop:.2f} | {lots}×{lot_size}={qty}{' [GHOST]' if is_ghost else ''}"
        )

        # ⚡ REAL BROKER ENTRY (Background Thread) ⚡ 
        is_warmup = getattr(self, "is_warmup", False)
        if self.broker and trading_symbol and symbol_token and not is_warmup and not is_ghost:
            order_side = "BUY" if inst_type == "OPT" else ("BUY" if direction == "LONG" else "SELL")
            
            def place_bg():
                try:
                    order_result = self.broker.place_order(
                        symbol=trading_symbol,
                        token=symbol_token,
                        qty=trade.quantity,
                        side=order_side,
                        product_type=self.product_type,
                        price=price
                    )
                    order_id = self._extract_order_id(order_result)
                    if order_id:
                        trade.broker_order_id = order_id
                        trade.pending_execution = False
                        trade.execution_status = "ENTRY_SUBMITTED"
                        trade.execution_error = ""
                        self.save_state()
                        if on_execution_result:
                            on_execution_result(trade, True)
                        msg = f"✅ Broker order placed successfully for {trading_symbol}: {order_id}"
                        logger.info(msg)
                        self._log_ui(msg, "trade")
                    else:
                        msg = f"❌ Broker failed to place order for {trading_symbol}. Rejecting trade."
                        logger.error(msg)
                        self._log_ui(msg, "error")
                        with self._lock:
                            if trade.id in self.open_trades:
                                trade.status = "CLOSED"
                                trade.exit_reason = "BROKER_REJECT"
                                trade.exit_time = datetime.now(IST)
                                trade.exit_price = trade.entry_price
                                trade.pending_execution = False
                                trade.execution_status = "REJECTED"
                                trade.execution_error = "Broker rejected entry order"
                                self.closed_trades.append(trade)
                                del self.open_trades[trade.id]
                        self.save_state()
                        if on_execution_result:
                            on_execution_result(trade, False)
                except Exception as e:
                    msg = f"❌ Exception in place_order for {trading_symbol}: {e}"
                    logger.error(msg)
                    self._log_ui(msg, "error")
                    with self._lock:
                        if trade.id in self.open_trades:
                            trade.status = "CLOSED"
                            trade.exit_reason = f"BROKER_ERROR: {str(e)[:30]}"
                            trade.exit_time = datetime.now(IST)
                            trade.exit_price = trade.entry_price
                            trade.pending_execution = False
                            trade.execution_status = "REJECTED"
                            trade.execution_error = str(e)
                            self.closed_trades.append(trade)
                            del self.open_trades[trade.id]
                    self.save_state()
                    if on_execution_result:
                        on_execution_result(trade, False)
            
            from engine.async_queue import execution_queue
            execution_queue.submit(f"PlaceOrder_{trading_symbol}", place_bg)
        else:
            if getattr(self, "mode", "HISTORICAL") == "REAL" and not is_recovery:
                with self._lock:
                    trade.status = "CLOSED"
                    trade.pending_execution = False
                    trade.execution_status = "REJECTED"
                    trade.execution_error = "REAL broker or executable contract details unavailable"
                    trade.exit_reason = "BROKER_UNAVAILABLE"
                    trade.exit_time = datetime.now(IST)
                    trade.exit_price = trade.entry_price
                    self.closed_trades.append(trade)
                    self.open_trades.pop(trade.id, None)
                self.save_state()
                if on_execution_result:
                    on_execution_result(trade, False)
            else:
                trade.pending_execution = False
                trade.execution_status = "OPEN"
                if on_execution_result:
                    on_execution_result(trade, True)

        return trade

    def update_trade(
        self, 
        trade_id: str, 
        current_spot: float, 
        new_trailing_stop: float = None,
        current_time: Optional[datetime] = None,
        real_premium: Optional[float] = None,
        real_fut_price: Optional[float] = None,
    ):
        """
        Update trade with latest price and trailing stop from 1-min data.
        Automatically closes trade if trailing stop is hit.
        """
        trade = self.open_trades.get(trade_id)
        if not trade or trade.status != "OPEN":
            return
        if str(getattr(trade, "execution_status", "") or "").upper() == "RECOVERY_REQUIRED":
            logger.critical(f"Skipped mark-to-market close checks for recovery-required trade {trade_id}; broker reconciliation required.")
            return
        if getattr(trade, "pending_execution", False):
            return  # Wait for broker confirmation before triggering stops

        # If it's an option, current_price is the synthetic premium. 
        # Premium change = (Spot Change * Signed Delta)
        is_option = trade.inst_type == "OPT" or "CE" in getattr(trade, 'trading_symbol', '') or "PE" in getattr(trade, 'trading_symbol', '')
        
        if is_option:
            if (
                real_premium is not None
                and real_premium > 0
                and self._option_premium_quote_plausible(trade, real_premium, current_spot)
            ):
                trade.current_price = float(real_premium)
            elif real_premium is not None and real_premium > 0:
                logger.warning(
                    "Ignored implausible option premium %.2f for %s %s; using synthetic premium fallback.",
                    float(real_premium),
                    trade.instrument,
                    trade.trading_symbol,
                )
                if trade.entry_spot > 0:
                    spot_change = current_spot - trade.entry_spot
                    premium_move = spot_change * trade.instrument_multiplier
                    trade.current_price = trade.entry_price + premium_move
                else:
                    trade.current_price = trade.entry_price
            elif trade.entry_spot > 0:
                spot_change = current_spot - trade.entry_spot
                
                # Premium Move = Spot Move * Signed Delta (instrument_multiplier)
                # PE has negative delta, so spot down (-100) * delta (-0.5) = +50 premium.
                premium_move = spot_change * trade.instrument_multiplier
                trade.current_price = trade.entry_price + premium_move
            else:
                # Fallback: If entry_spot is missing, do NOT use spot price as option price!
                # Keep it at entry_price to avoid massive fake P&L.
                trade.current_price = trade.entry_price
                
            # Ensure premium doesn't go below 1.0 (option value floor)
            trade.current_price = max(0.05, trade.current_price)
        else:
            if real_fut_price is not None and real_fut_price > 0:
                trade.current_price = float(real_fut_price)
            elif trade.entry_spot > 0:
                trade.current_price = trade.entry_price + (current_spot - trade.entry_spot)
            else:
                trade.current_price = current_spot

        logger.debug(f"💓 Heartbeat {trade.id}: Price {trade.current_price:.2f} | Stop {trade.current_stop:.2f} | PnL ₹{trade.pnl:.0f}")

        # Update trailing stop (only ratchet — never move against trade)
        if new_trailing_stop is not None:
            if is_option and trade.entry_spot > 0:
                spot_sl_move = trade.entry_spot - new_trailing_stop
                new_isolated_sl = trade.entry_price - (spot_sl_move * trade.instrument_multiplier)
                if new_isolated_sl > trade.current_stop:
                    trade.current_stop = new_isolated_sl
            elif not is_option:
                if trade.direction == "LONG":
                    if new_trailing_stop > trade.current_stop:
                        trade.current_stop = new_trailing_stop
                elif trade.direction == "SHORT":
                    if new_trailing_stop < trade.current_stop:
                        trade.current_stop = new_trailing_stop

        # Calculate unrealized P&L
        if trade.inst_type == "OPT":
            # For Options, we always BUY (either CE or PE). 
            # Profit = (Current Premium - Entry Premium) * Quantity
            trade.pnl = (trade.current_price - trade.entry_price) * trade.quantity
        else:
            # For Futures, it depends on Direction
            if trade.direction == "LONG":
                trade.pnl = (trade.current_price - trade.entry_price) * trade.quantity
            else: # SHORT
                trade.pnl = (trade.entry_price - trade.current_price) * trade.quantity

        # Track peak and drawdown
        trade.peak_pnl = max(trade.peak_pnl, trade.pnl)
        trade.max_drawdown = max(trade.max_drawdown, trade.peak_pnl - trade.pnl)

        # ── EXIT CHECKS (Stop Loss & Target) ──
        # Option trades are premium-led when real premium is available.
        # Futures are contract-price-led; spot is only an analysis input/fallback.
        if trade.inst_type == "OPT":
            if trade.current_price <= trade.current_stop:
                self.close_trade(trade_id, trade.current_price, "TRAILING_STOP", exit_time=current_time)
            elif trade.current_price >= trade.target and trade.target > 0:
                self.close_trade(trade_id, trade.current_price, "TARGET_HIT", exit_time=current_time)
        elif trade.direction == "LONG":
            if trade.current_price <= trade.current_stop:
                self.close_trade(trade_id, trade.current_price, "TRAILING_STOP", exit_time=current_time)
            elif trade.current_price >= trade.target and trade.target > 0:
                self.close_trade(trade_id, trade.current_price, "TARGET_HIT", exit_time=current_time)
        else: # SHORT
            if trade.current_price >= trade.current_stop:
                self.close_trade(trade_id, trade.current_price, "TRAILING_STOP", exit_time=current_time)
            elif trade.current_price <= trade.target and trade.target > 0:
                self.close_trade(trade_id, trade.current_price, "TARGET_HIT", exit_time=current_time)
        
        if trade_id in self.open_trades:
            from config.settings import get_settings
            checkpoint = max(
                0.25,
                float(getattr(get_settings(), "trade_state_checkpoint_seconds", 1.0) or 1.0),
            )
            now = time.monotonic()
            if now - self._last_mark_to_market_save >= checkpoint:
                self._last_mark_to_market_save = now
                self.save_state()

    @staticmethod
    def _option_premium_quote_plausible(trade: Trade, premium: float, current_spot: float = 0.0) -> bool:
        """Reject option LTPs that are obviously index/futures prices."""
        try:
            premium = float(premium or 0.0)
        except (TypeError, ValueError):
            return False
        if premium <= 0:
            return False

        entry = float(getattr(trade, "entry_price", 0.0) or 0.0)
        target = float(getattr(trade, "target", 0.0) or 0.0)
        spot = float(current_spot or getattr(trade, "entry_spot", 0.0) or 0.0)
        strike = float(getattr(trade, "atm_strike", 0.0) or 0.0)
        opt_type = str(getattr(trade, "option_type", "") or "").upper()
        intrinsic = 0.0
        if spot > 0 and strike > 0:
            intrinsic = max(0.0, spot - strike) if opt_type == "CE" else max(0.0, strike - spot)

        bounds = [1000.0]
        if entry > 0:
            bounds.append(entry * 4.0)
        if target > 0:
            bounds.append(target * 3.0)
        if spot > 0:
            bounds.append(intrinsic + spot * 0.05)
            bounds.append(spot * 0.12)
        return premium <= max(bounds)

    def _request_broker_exit(
        self,
        trade: Trade,
        on_exit_result: Optional[Callable[[Trade, bool], None]] = None,
    ) -> None:
        def close_bg():
            last_error = ""
            order_id = ""
            for attempt in range(3):
                try:
                    exit_side = "SELL" if trade.direction == "LONG" or trade.inst_type == "OPT" else "BUY"
                    result = self.broker.close_order(
                        symbol=trade.trading_symbol,
                        token=trade.symbol_token,
                        qty=trade.quantity,
                        side=exit_side,
                        product_type=self.product_type,
                    )
                    order_id = self._extract_order_id(result)
                    if order_id:
                        break
                    last_error = "Broker rejected exit order"
                except Exception as exc:
                    last_error = str(exc)
                if attempt < 2:
                    time.sleep(0.5 * (2 ** attempt))

            if order_id:
                trade.exit_order_id = order_id
                trade.execution_status = "EXIT_PENDING"
                trade.execution_error = ""
                self.save_state()
                if getattr(self, "mode", "HISTORICAL") != "REAL":
                    self._finalize_closed_trade(
                        trade.id,
                        trade,
                        trade.exit_price,
                        trade.exit_reason,
                        exit_time=trade.exit_time,
                    )
                if on_exit_result:
                    on_exit_result(trade, True)
                return

            trade.pending_exit = False
            trade.execution_status = "EXIT_FAILED"
            trade.execution_error = last_error or "Unknown broker exit failure"
            self.save_state()
            logger.critical(
                f"[BROKER CLOSE FAILED] Position remains active for {trade.trading_symbol}: "
                f"{trade.execution_error}"
            )
            if on_exit_result:
                on_exit_result(trade, False)

        from engine.async_queue import execution_queue
        execution_queue.submit(f"CloseOrder_{trade.trading_symbol}", close_bg)

    def close_trade(
        self,
        trade_id: str,
        exit_price: float,
        reason: str = "MANUAL",
        exit_time: Optional[datetime] = None,
        on_exit_result: Optional[Callable[[Trade, bool], None]] = None,
    ):
        """Close a trade with IST aware timestamping"""
        with self._lock:
            trade = self.open_trades.get(trade_id)
            if not trade or trade.status != "OPEN":
                return False
            if trade.pending_execution or trade.execution_status in {"ENTRY_PENDING"}:
                return False
            if trade.execution_status == "EXIT_PENDING":
                return False
            trade.exit_price = float(exit_price or trade.current_price or trade.entry_price)
            trade.current_price = trade.exit_price
            trade.exit_time = exit_time or datetime.now(IST)
            trade.exit_reason = reason
            trade.pending_exit = True
            trade.execution_status = "EXIT_PENDING"
            trade.execution_error = ""
        self.save_state()

        if self.broker and trade.symbol_token and not getattr(trade, 'is_ghost', False):
            self._request_broker_exit(trade, on_exit_result=on_exit_result)
        else:
            self._finalize_closed_trade(
                trade_id,
                trade,
                trade.exit_price,
                reason,
                exit_time=trade.exit_time,
            )
            if on_exit_result:
                on_exit_result(trade, True)
        return True

    def check_session_end(self, current_time: Optional[datetime] = None):
        """
        Simple & Robust EOD Exit (Strict Session Bound)
        """
        if current_time:
            now = IST.localize(current_time) if current_time.tzinfo is None else current_time
        else:
            now = datetime.now(IST)
        now_t = now.time()
        
        from config.settings import get_settings
        force_exit_label = str(getattr(get_settings(), "ut_force_exit_time", "15:25") or "15:25")
        kill_threshold = self._parse_time(force_exit_label)
        
        if now_t < kill_threshold:
            return

        with self._lock:
            snapshot = list(self.open_trades.items())
            
        for tid, trade in snapshot:
            # Strategy-owned square-off. CARRYFORWARD avoids broker intraday auto-squareoff.
            self.close_trade(tid, trade.current_price, "FORCE_EOD_KILL", exit_time=now)

    def expire_stale_open_trades(self, now: Optional[datetime] = None, reason: str = "STALE_SESSION_END") -> int:
        """Retain previous-session trades until broker reconciliation proves closure."""
        now = now or datetime.now(IST)
        if now.tzinfo is None:
            now = IST.localize(now)
        marked = 0
        with self._lock:
            for trade in self.open_trades.values():
                entry_time = trade.entry_time
                if entry_time.tzinfo is None:
                    entry_time = IST.localize(entry_time)
                else:
                    entry_time = entry_time.astimezone(IST)
                if entry_time.date() >= now.date():
                    continue
                trade.execution_status = "RECOVERY_REQUIRED"
                trade.execution_error = "Previous-session trade requires broker reconciliation"
                marked += 1
        if marked:
            self.save_state()
            try:
                self.reconcile_broker_positions()
            except Exception as exc:
                logger.error(f"Stale-position reconciliation failed: {exc}")
        return marked

        # Legacy local-expiry implementation retained below for restore-point readability.
        expired = 0
        with self._lock:
            trades_to_check = list(self.open_trades.items())
            
        for tid, trade in trades_to_check:
            entry_time = trade.entry_time
            if entry_time.tzinfo is None:
                entry_time = IST.localize(entry_time)
            else:
                entry_time = entry_time.astimezone(IST)

            if entry_time.date() >= now.date():
                continue

            trade.status = "CLOSED"
            from config.settings import get_settings
            force_exit = self._parse_time(
                str(getattr(get_settings(), "ut_force_exit_time", "15:25") or "15:25")
            )
            trade.exit_time = IST.localize(datetime.combine(entry_time.date(), force_exit))
            trade.exit_reason = reason
            trade.exit_price = trade.current_price if trade.current_price > 0 else trade.entry_price

            if trade.inst_type == "OPT":
                gross = (trade.exit_price - trade.entry_price) * trade.quantity
            elif trade.direction == "LONG":
                gross = (trade.exit_price - trade.entry_price) * trade.quantity
            else:
                gross = (trade.entry_price - trade.exit_price) * trade.quantity
            from config.settings import get_settings
            settings = get_settings()
            trade.charges = float(settings.fut_cost) if trade.inst_type == "FUT" else float(settings.opt_cost)
            trade.pnl = gross - trade.charges

            with self._lock:
                if tid in self.open_trades:
                    self.closed_trades.append(trade)
                    del self.open_trades[tid]
            expired += 1
            logger.warning(f"Expired stale open trade {tid} from {entry_time.date()} as {reason}.")

        if expired:
            self.save_state()
        return expired

    def update_stops(self, prices: Dict[str, float]):
        """Update trailing stops for all open trades based on favor-movements"""
        with self._lock:
            trades_snapshot = list(self.open_trades.items())
            
        for tid, trade in trades_snapshot:
            spot = prices.get(trade.instrument.split(' ')[0])
            if not spot: continue
            
            # Update current price for real-time PnL tracking
            self.update_trade(tid, spot)
            
            # Simple Trailing logic: if price moves in favor, trail the stop
            # Note: This is a backup; MTF usually manages this, but we keep it for safety.
            pass

    def get_dashboard_payload(self, is_historical: bool = False, backtest_days: int = 1, inst_pref: str = "AUTO") -> Dict:
        """Get filtered trades and summary based on active mode"""
        now_ist = datetime.now(IST)
        # If it's before 9:00 AM IST, we are likely reviewing the PREVIOUS day's session
        if now_ist.hour < 9:
            today = (now_ist - timedelta(days=1)).date()
        else:
            today = now_ist.date()
        
        # 1. Unified Session Identification
        today_str = today.strftime('%Y-%m-%d')
        
        def trade_session_date(trade: Trade) -> Optional[date]:
            value = getattr(trade, "entry_time", None)
            if value is None:
                return None
            try:
                import pandas as pd
                ts = pd.Timestamp(value)
                if ts.tzinfo is not None:
                    ts = ts.tz_convert(IST).tz_localize(None)
                return ts.date()
            except Exception:
                return None

        with self._lock:
            snapshot_open = list(self.open_trades.values())
            snapshot_closed = list(self.closed_trades)

        if not is_historical:
            # Live/manual mode must not project older sessions as current trades.
            session_closed = [t for t in snapshot_closed if trade_session_date(t) == today]
        else:
            all_trades = snapshot_closed + snapshot_open
            unique_dates = sorted({d for d in (trade_session_date(t) for t in all_trades) if d is not None})
            if unique_dates:
                cutoff_date = unique_dates[-min(len(unique_dates), backtest_days)]
                session_closed = [t for t in snapshot_closed if (trade_session_date(t) or date.min) >= cutoff_date]
            else:
                session_closed = []
            
        # 2. Get Open Trades
        if not is_historical:
            open_trades = [t for t in snapshot_open if trade_session_date(t) == today]
        else:
            def close_intraday_projection(trade: Trade) -> Trade:
                entry_time = trade.entry_time
                if entry_time.tzinfo is None:
                    entry_time = IST.localize(entry_time)
                else:
                    entry_time = entry_time.astimezone(IST)

                exit_price = trade.current_price if trade.current_price > 0 else trade.entry_price
                from config.settings import get_settings
                settings = get_settings()
                from engine.trade_accounting import estimate_trade_charges
                charges = trade.charges or estimate_trade_charges(
                    trade.entry_price,
                    exit_price,
                    trade.quantity,
                    trade.inst_type,
                    settings,
                    trade.instrument_multiplier,
                )
                if trade.inst_type == "OPT" or trade.direction == "LONG":
                    gross = (exit_price - trade.entry_price) * trade.quantity
                else:
                    gross = (trade.entry_price - exit_price) * trade.quantity

                projected = replace(trade)
                projected.status = "CLOSED"
                from config.settings import get_settings
                force_exit = self._parse_time(
                    str(getattr(get_settings(), "ut_force_exit_time", "15:25") or "15:25")
                )
                projected.exit_time = IST.localize(datetime.combine(entry_time.date(), force_exit))
                projected.exit_reason = "SESSION_END"
                projected.exit_price = exit_price
                projected.current_price = exit_price
                projected.charges = charges
                projected.pnl = gross - charges
                return projected

            filtered_open = []
            try:
                if 'unique_dates' in locals() and unique_dates:
                    filtered_open = [t for t in snapshot_open if (trade_session_date(t) or date.min) >= cutoff_date]
                else:
                    filtered_open = snapshot_open
            except Exception:
                filtered_open = snapshot_open
                
            session_closed = session_closed + [close_intraday_projection(t) for t in filtered_open]
            open_trades = []

        pref = (inst_pref or "AUTO").upper()
        if pref in {"FUT", "OPT"}:
            session_closed = [t for t in session_closed if (getattr(t, "inst_type", "FUT") or "FUT").upper() == pref]
            open_trades = [t for t in open_trades if (getattr(t, "inst_type", "FUT") or "FUT").upper() == pref]

        display_closed = self._filter_concurrency_compliant_closed(session_closed)
        display_closed = [t for t in display_closed if self._valid_closed_chronology(t)]
        display_closed = [t for t in display_closed if t.status == "CLOSED"]
         
        # 3. Calculate Summary based on the EXACT same session pool
        summary = self.get_summary(filtered_trades=display_closed, open_trades=open_trades)
        
        # 4. Format all trades for UI
        all_formatted = []
        def get_naive_entry(t):
            import pandas as pd
            return pd.to_datetime(t.entry_time).tz_localize(None)
            
        for t in sorted(open_trades + display_closed, key=get_naive_entry, reverse=True)[:2000]:
            display_name = t.instrument
            if t.inst_type == "OPT" and t.atm_strike is not None and t.atm_strike > 0:
                display_name = f"{t.instrument} {int(t.atm_strike)} {t.option_type}"
            display_stop = t.current_stop
            display_target = t.target
            # Live option trades may store spot-based risk levels, while historical
            # option rows are already premium-based from the simulator.
            if t.inst_type == "OPT" and t.entry_spot is not None and t.entry_spot > 0:
                looks_spot_stop = t.current_stop is not None and t.current_stop > (t.entry_price * 5.0)
                looks_spot_target = t.target is not None and t.target > (t.entry_price * 5.0)
                if looks_spot_stop or looks_spot_target:
                    stop_dist_spot = abs(t.entry_spot - t.current_stop)
                    target_dist_spot = abs(t.target - t.entry_spot)
                    display_stop = max(0.1, t.entry_price - (stop_dist_spot * abs(t.instrument_multiplier)))
                    display_target = t.entry_price + (target_dist_spot * abs(t.instrument_multiplier))

                    max_sl_dist = t.entry_price * (self.options_sl_pct / 100.0)
                    risk_stop = t.entry_price - max_sl_dist
                    display_stop = max(display_stop, risk_stop)
            
            # ═══ TIMESTAMP NORMALIZATION (IST Canonical) ═══
            # Candle timestamps are naive-IST, live trades are IST-aware.
            # Normalize everything to IST-aware for .timestamp() accuracy,
            # then strip TZ for strftime (since the time is already IST).
            def to_ist_naive(dt):
                """Convert any datetime to a naive IST datetime for display"""
                if dt is None:
                    return None
                import pandas as pd
                ts = pd.Timestamp(dt)
                if ts.tzinfo is not None:
                    # Already aware — convert to IST then strip
                    return ts.tz_convert(IST).tz_localize(None).to_pydatetime()
                else:
                    # Naive — already in IST (candle data convention)
                    return ts.to_pydatetime()
            
            entry_display = to_ist_naive(t.entry_time)
            exit_display = to_ist_naive(t.exit_time)
            current_display_price = t.current_price
            if t.status == "CLOSED":
                current_display_price = (
                    t.current_price
                    if t.current_price is not None and t.current_price > 0 and abs(t.current_price - t.exit_price) > 1e-9
                    else 0.0
                )
            
            all_formatted.append({
                "id": t.id,
                "status": t.status,
                "execution_status": t.execution_status,
                "execution_error": t.execution_error,
                "pending_execution": t.pending_execution,
                "pending_exit": t.pending_exit,
                "broker_order_id": t.broker_order_id,
                "exit_order_id": t.exit_order_id,
                "broker_quantity": t.broker_quantity,
                "instrument": display_name,
                "direction": t.direction,
                "entry_price": t.entry_price,
                "current_price": current_display_price,
                "exit_price": t.exit_price if t.status == "CLOSED" else 0.0,
                "trailing_stop": round(display_stop, 2) if display_stop is not None else 0.0,
                "target": round(display_target, 2) if display_target is not None and display_target > 0 else 0,
                "pnl": round(t.pnl, 0) if t.pnl is not None else 0.0,
                "lots": t.lots,
                "lot_size": t.lot_size,
                "grade": t.grade,
                "confidence": round(t.confidence, 2) if t.confidence is not None else 0.0,
                "rr_ratio": t.rr_ratio,
                "atm_strike": t.atm_strike,
                "option_type": t.option_type,
                "inst_type": t.inst_type,
                "timeframe": t.timeframe,
                "entry_time": entry_display.strftime("%H:%M:%S") if entry_display else "--",
                "entry_date": entry_display.strftime("%d %b %y") if entry_display else "--",
                "entry_timestamp": IST.localize(entry_display).timestamp() if entry_display else 0,
                "exit_time": exit_display.strftime("%H:%M:%S") if exit_display else None,
                "exit_date": exit_display.strftime("%d %b %y") if exit_display else None,
                "exit_timestamp": IST.localize(exit_display).isoformat() if exit_display else "",
                "exit_reason": t.exit_reason,
                "exec_type": t.exec_type,
            })

        if not is_historical:
            for symbol, position in sorted(self.orphan_positions.items()):
                all_formatted.append({
                    "id": f"ORPHAN|{symbol}",
                    "status": "ORPHANED",
                    "execution_status": "ORPHANED",
                    "execution_error": "Broker position has no matching local trade",
                    "pending_execution": False,
                    "pending_exit": False,
                    "broker_order_id": "",
                    "exit_order_id": "",
                    "broker_quantity": abs(self._position_quantity(position)),
                    "instrument": symbol,
                    "direction": "LONG" if self._position_quantity(position) > 0 else "SHORT",
                    "entry_price": float(position.get("buyavgprice") or position.get("sellavgprice") or 0.0),
                    "current_price": float(position.get("ltp") or 0.0),
                    "exit_price": 0.0,
                    "trailing_stop": 0.0,
                    "target": 0.0,
                    "pnl": float(position.get("unrealised") or position.get("pnl") or 0.0),
                    "lots": 0,
                    "lot_size": 0,
                    "grade": "RECOVERY",
                    "confidence": 0.0,
                    "rr_ratio": 0.0,
                    "atm_strike": 0.0,
                    "option_type": "CE" if "CE" in symbol else ("PE" if "PE" in symbol else ""),
                    "inst_type": "OPT" if "CE" in symbol or "PE" in symbol else "FUT",
                    "timeframe": "--",
                    "entry_time": "--",
                    "entry_date": "--",
                    "entry_timestamp": 0,
                    "exit_time": None,
                    "exit_date": None,
                    "exit_timestamp": "",
                    "exit_reason": "BROKER_ORPHAN",
                    "exec_type": "B",
                })
        
        return {
            "open": [t for t in all_formatted if t["status"] in {"OPEN", "ORPHANED"}],
            "closed": [t for t in all_formatted if t["status"] == "CLOSED"],
            "summary": summary,
            "analytics": self._build_session_analytics(display_closed, open_trades),
        }

    def get_open_trades_list(self) -> List[Dict]:
        """Backward compatibility for existing API calls"""
        payload = self.get_dashboard_payload(is_historical=False)
        return payload["open"] + payload["closed"]



    def _grade_rank(self, grade: str) -> int:
        base = str(grade or "C").split()[0]
        return {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4, "Recovered": 4}.get(base, 0)

    def _is_aplus_setup(self, trade: Trade) -> bool:
        return self._grade_rank(getattr(trade, "grade", "")) >= 4 or float(getattr(trade, "confidence", 0.0) or 0.0) >= 0.90

    def _valid_closed_chronology(self, trade: Trade) -> bool:
        if trade.status != "CLOSED" or not trade.entry_time or not trade.exit_time:
            return True
        entry_ts = trade.entry_time.replace(tzinfo=None) if trade.entry_time.tzinfo else trade.entry_time
        exit_ts = trade.exit_time.replace(tzinfo=None) if trade.exit_time.tzinfo else trade.exit_time
        return exit_ts > entry_ts

    def _filter_concurrency_compliant_closed(self, trades: List[Trade]) -> List[Trade]:
        """Apply live-style overlap rules to historical/session display metrics."""
        try:
            from config.settings import get_settings
            if not _setting_enabled(getattr(get_settings(), "ut_concurrency_guard", True), True):
                return sorted(
                    trades,
                    key=lambda t: (
                        t.entry_time.replace(tzinfo=None) if getattr(t.entry_time, "tzinfo", None) else t.entry_time,
                        str(getattr(t, "id", "") or ""),
                    ),
                    reverse=True,
                )
        except Exception:
            pass

        accepted: List[Trade] = []
        removed = {"same_index_cap": 0, "same_index_overlap": 0, "correlated_cap": 0}

        def is_futures_shadow(trade: Trade) -> bool:
            return (
                str(getattr(trade, "trading_symbol", "") or "").upper() == "FUT_SHADOW"
                or "FUT SHADOW" in str(getattr(trade, "grade", "") or "").upper()
            )

        ordered = sorted(
            trades,
            key=lambda t: (
                t.entry_time.replace(tzinfo=None) if getattr(t.entry_time, "tzinfo", None) else t.entry_time,
                -float(getattr(t, "confidence", 0.0) or 0.0),
                1 if is_futures_shadow(t) else 0,
                str(getattr(t, "instrument", "") or ""),
                str(getattr(t, "timeframe", "") or ""),
                str(getattr(t, "inst_type", "") or ""),
                str(getattr(t, "id", "") or ""),
            ),
        )

        for trade in ordered:
            entry = trade.entry_time.replace(tzinfo=None) if getattr(trade.entry_time, "tzinfo", None) else trade.entry_time
            active = []
            for existing in accepted:
                exit_time = existing.exit_time or existing.entry_time
                ex_entry = existing.entry_time.replace(tzinfo=None) if getattr(existing.entry_time, "tzinfo", None) else existing.entry_time
                ex_exit = exit_time.replace(tzinfo=None) if getattr(exit_time, "tzinfo", None) else exit_time
                if ex_entry <= entry < ex_exit:
                    active.append(existing)

            same_index = [t for t in active if t.instrument.split()[0] == trade.instrument.split()[0]]
            if len(same_index) >= 2:
                removed["same_index_cap"] += 1
                continue
            if same_index:
                existing = same_index[0]
                same_tf = getattr(existing, "timeframe", "") == getattr(trade, "timeframe", "")
                same_type = getattr(existing, "inst_type", "FUT") == getattr(trade, "inst_type", "FUT")
                higher_grade = self._grade_rank(trade.grade) > self._grade_rank(existing.grade) or self._is_aplus_setup(trade)
                if same_tf or same_type or not higher_grade:
                    removed["same_index_overlap"] += 1
                    continue

            same_dir = [t for t in active if t.direction == trade.direction]
            if len(same_dir) >= 2:
                all_aplus = all(self._is_aplus_setup(t) for t in same_dir) and self._is_aplus_setup(trade)
                if not all_aplus:
                    removed["correlated_cap"] += 1
                    continue

            accepted.append(trade)

        summary_key = (len(trades), len(accepted), tuple(removed.values()))
        now = time.time()
        if (
            summary_key != getattr(self, "_last_concurrency_filter_log_key", None)
            or now - getattr(self, "_last_concurrency_filter_log_time", 0.0) >= 60.0
        ):
            logger.info(
                "Historical Concurrency Guard: "
                f"accepted={len(accepted)}/{len(trades)}, removed={sum(removed.values())} "
                f"(index_cap={removed['same_index_cap']}, "
                f"overlap={removed['same_index_overlap']}, "
                f"correlated_cap={removed['correlated_cap']})."
            )
            self._last_concurrency_filter_log_key = summary_key
            self._last_concurrency_filter_log_time = now

        return sorted(
            accepted,
            key=lambda t: (
                t.entry_time.replace(tzinfo=None) if getattr(t.entry_time, "tzinfo", None) else t.entry_time,
                str(getattr(t, "id", "") or ""),
            ),
            reverse=True,
        )

    def _build_session_analytics(self, closed_trades: List[Trade], open_trades: List[Trade]) -> Dict:
        def bucketize(trades, key_fn):
            buckets = {}
            for t in trades:
                key = key_fn(t) or "--"
                bucket = buckets.setdefault(key, {"count": 0, "pnl": 0.0, "wins": 0, "losses": 0})
                pnl = float(getattr(t, "pnl", 0.0) or 0.0)
                bucket["count"] += 1
                bucket["pnl"] += pnl
                if pnl > 0:
                    bucket["wins"] += 1
                elif pnl < 0:
                    bucket["losses"] += 1
            for bucket in buckets.values():
                bucket["pnl"] = round(bucket["pnl"], 0)
                closed_count = bucket["wins"] + bucket["losses"]
                bucket["win_rate"] = round((bucket["wins"] / closed_count) * 100, 1) if closed_count else 0.0
            return dict(sorted(buckets.items(), key=lambda item: abs(item[1]["pnl"]), reverse=True))

        closed = list(closed_trades or [])
        return {
            "by_timeframe": bucketize(closed, lambda t: getattr(t, "timeframe", "--")),
            "by_grade": bucketize(closed, lambda t: str(getattr(t, "grade", "--")).split()[0]),
            "by_instrument": bucketize(closed, lambda t: str(getattr(t, "instrument", "--")).split()[0]),
            "by_type": bucketize(closed, lambda t: getattr(t, "inst_type", "--")),
            "by_exit_reason": bucketize(closed, lambda t: getattr(t, "exit_reason", "--")),
            "open_by_type": bucketize(list(open_trades or []), lambda t: getattr(t, "inst_type", "--")),
        }

    def get_summary(self, filtered_trades: Optional[List] = None, open_trades: Optional[List] = None) -> Dict:
        """Get comprehensive performance analytics for dashboard"""
        closed = list(filtered_trades) if filtered_trades is not None else list(self.closed_trades)
        closed = [
            trade for trade in closed
            if getattr(trade, "execution_status", "") != "REJECTED"
        ]
        open_for_summary = list(open_trades) if open_trades is not None else list(self.open_trades.values())
        closed_total = len(closed)
        total = closed_total + len(open_for_summary)
        
        wins = sum(1 for t in closed if t.pnl > 0)
        losses = sum(1 for t in closed if t.pnl < 0)
        
        win_rate = (wins / closed_total * 100) if closed_total > 0 else 0.0
        
        # Profit Factor
        total_profit = sum(t.pnl for t in closed if t.pnl > 0)
        total_loss = abs(sum(t.pnl for t in closed if t.pnl < 0))
        profit_factor = (total_profit / total_loss) if total_loss > 0 else (total_profit if total_profit > 0 else 1.0)
        
        # Tactical Sharpe Ratio (Annualized Approximation: Mean / StdDev * sqrt(252))
        sharpe = 0.0
        if closed_total > 2:
            import numpy as np
            pnl_list = [t.pnl for t in closed]
            avg = np.mean(pnl_list)
            std = np.std(pnl_list)
            # Multiply by sqrt(252) to match standard annualized Sharpe Ratio expectations
            sharpe = (avg / std) * np.sqrt(252) if std > 0 else 0.0
            
        # Max Drawdown
        max_dd = 0.0
        peak = 0.0
        running_equity = 0.0
        for t in closed:
            running_equity += t.pnl
            peak = max(peak, running_equity)
            max_dd = max(max_dd, peak - running_equity)

        # Calculate P&L dynamically (Closed + Open Unrealized)
        now_ist = datetime.now(IST)
        if now_ist.hour < 9:
            today_str = (now_ist - timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            today_str = now_ist.strftime('%Y-%m-%d')
        
        def is_option_trade(trade) -> bool:
            trading_symbol = str(getattr(trade, "trading_symbol", "") or "").upper()
            instrument = str(getattr(trade, "instrument", "") or "").upper()
            return (
                str(getattr(trade, "inst_type", "") or "").upper() == "OPT"
                or "CE" in trading_symbol
                or "PE" in trading_symbol
                or "CE" in instrument
                or "PE" in instrument
            )

        total_pnl = sum(t.pnl for t in closed)
        fut_pnl = sum(t.pnl for t in closed if not is_option_trade(t))
        opt_pnl = sum(t.pnl for t in closed if is_option_trade(t))

        for ot in open_for_summary:
            try:
                if ot.entry_time.strftime('%Y-%m-%d') == today_str:
                    unrealized = getattr(ot, "unrealized_pnl", 0.0) or 0.0
                    total_pnl += unrealized
                    is_opt = is_option_trade(ot)
                    if is_opt:
                        opt_pnl += unrealized
                    else:
                        fut_pnl += unrealized
            except Exception:
                pass
        
        return {
            "daily_pnl": round(total_pnl, 0),
            "fut_pnl": round(fut_pnl, 0),
            "opt_pnl": round(opt_pnl, 0),
            "total_trades": total,
            "open_count": len(open_for_summary),
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "sharpe_ratio": sharpe,
            "max_drawdown": max_dd
        }

