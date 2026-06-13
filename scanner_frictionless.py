"""
Scanner â€” Continuous Multi-Instrument Multi-Timeframe Scanner
â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
v3.0 â€” UT1 Intelligent Scanning Engine
"""

import asyncio
import time
import math
import uuid
import json
import pandas as pd
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Dict, Optional, Callable, List, Tuple, Any
from loguru import logger

from config.settings import get_settings
from data.market_data import MarketDataProvider
from data.candle_builder import CandleBuilder
from engine.multi_timeframe import MultiTimeframeEngine
from engine.signal_manager import SignalManager
from intelligence.intelligence_aggregator import IntelligenceAggregator
from intelligence.memory import IntelligenceMemory
from trading.trade_manager import TradeManager, IST
from trading.performance_tracker import PerformanceTracker
from engine.risk_manager import RiskManager
from engine.signal_processor import SignalProcessor, TradeCandidate


class TokenBucket:
    def __init__(self, capacity: int, fill_rate: float):
        self.capacity = float(capacity)
        self._tokens = float(capacity)
        self.fill_rate = float(fill_rate)
        self.timestamp = time.time()

    async def consume(self, tokens: int = 1):
        while True:
            now = time.time()
            if self._tokens < self.capacity:
                self._tokens += self.fill_rate * (now - self.timestamp)
                if self._tokens > self.capacity:
                    self._tokens = self.capacity
            self.timestamp = now
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            await asyncio.sleep(0.1)

class Scanner:
    """Continuous scanning engine â€” optimized, best-signal selection"""

    def __init__(
        self,
        data_provider: MarketDataProvider,
        candle_builder: CandleBuilder,
        mtf_engine: MultiTimeframeEngine,
        signal_manager: SignalManager,
        intelligence: IntelligenceAggregator,
        trade_manager: TradeManager,
        performance: PerformanceTracker,
        instruments_config: Dict,
        trading_mode: str = "paper",
        on_update: Optional[Callable] = None,
        on_notification: Optional[Callable] = None,
    ):
        self.data = data_provider
        self.candles = candle_builder
        self.mtf = mtf_engine
        self.signals = signal_manager
        self.intel = intelligence
        self.trades = trade_manager
        self.trades.scanner = self
        self.trades.mode = trading_mode
        self.perf = performance
        self.config = instruments_config
        self.on_update = on_update
        self.on_notification = on_notification

        # â• â•  SUBSCRIPTION MODEL FOR CHARTS â• â•
        # Default to NIFTY 5min to reduce initial payload size
        self.active_chart_instrument = "NIFTY"
        self.active_chart_tf = "5min"

        self.is_running = False
        self.scan_interval = 1.0
        self.last_scan_time = None
        self.scan_count = 0
        self.mode = trading_mode # Master Mode Control
        self._state_file = "data_store/trade_state.json"

        # Load VIX history if exists
        self._vix_data = {}
        import os
        if os.path.exists("data_store/vix_history.json"):
            try:
                import json
                with open("data_store/vix_history.json", "r") as f:
                    self._vix_data = json.load(f)
                logger.info(f"ðŸ“ˆ Loaded {len(self._vix_data)} VIX data points for backtest.")
            except Exception as e:
                logger.error(f"â Œ Failed to load VIX history: {e}")

        # Risk settings (updated via configure)
        self.futures_sl_pct = 0.30
        self.options_sl_pct = 15.0

        # Load state on initialize from global settings
        settings = get_settings()
        self.user_lots: Dict[str, int] = {"NIFTY": 1, "BANKNIFTY": 1, "SENSEX": 1}
        self.user_lots_fut: Dict[str, int] = {"NIFTY": 1, "BANKNIFTY": 1, "SENSEX": 1}
        self.capital_fut = settings.capital_fut
        self.capital_opt = settings.capital_opt
        self.capital_total = getattr(settings, "capital_total", 500000.0)
        self.risk_fut_pct = settings.risk_fut_pct
        self.risk_opt_pct = settings.risk_opt_pct
        self.futures_sl_pct = settings.futures_sl_pct
        self.options_sl_pct = settings.options_sl_pct
        self.backtest_days = settings.default_backtest_days
        self.auto_mode = False
        self.inst_pref = getattr(settings, "inst_pref", "AUTO")

        self.intel_memory = IntelligenceMemory()
        self.latest_results: Dict = {}
        self._last_signal_time: Dict[str, datetime] = {}
        self._last_live_signal_candle_time: Dict[str, datetime] = {}
        self._last_data_fetch: Dict[str, float] = {}
        self._api_semaphore = asyncio.Semaphore(1) # Background history semaphore (serialized to prevent rate limits)
        self._ltp_semaphore = asyncio.Semaphore(5) # High-priority price semaphore (LTP)
        self._ltp_rate_limiter = TokenBucket(capacity=3, fill_rate=2.0)
        self._state_lock = asyncio.Lock()
        self._data_fetch_interval = 60
        self.max_daily_loss_pct = 3.0
        self._last_vol_alert_time: Dict[str, float] = {}

        # ðŸ›¡ï¸  Initialize Risk Manager
        self.risk_manager = RiskManager(self.trades, self.capital_total)
        # Override the 50/50 default capital split inside RiskManager with
        # the configured FUT/OPT capitals; use risk_opt_pct (segment risk) for
        # the OPT circuit-breaker (NOT options_sl_pct, which is per-trade SL).
        self.risk_manager.update_settings(
            max_daily_loss_pct=self.max_daily_loss_pct,
            risk_fut_pct=self.risk_fut_pct,
            risk_opt_pct=self.risk_opt_pct,
            capital_fut=self.capital_fut,
            capital_opt=self.capital_opt,
            capital_total=self.capital_total,
        )

        # ðŸ§  Initialize Signal Processor
        self.signal_processor = SignalProcessor(self)
        self.latest_regimes: Dict[str, str] = {} # For adaptive trailing
        self._intel_cache: Dict[str, Dict] = {}
        self._latest_trade_candidates: Dict[str, List[TradeCandidate]] = {}
        self._latest_exit_candidates: Dict[str, List[TradeCandidate]] = {}
        self._latest_filtered_candidates: Dict[str, List[TradeCandidate]] = {}
        self._session_trade_candidates: Dict[str, Dict[str, TradeCandidate]] = {}
        # Determine the current session day based on 9:15 AM rollover
        now_ist_init = datetime.now(IST)
        self._process_started_at = now_ist_init.replace(tzinfo=None)
        if now_ist_init.hour < 9 or (now_ist_init.hour == 9 and now_ist_init.minute < 15):
            self._session_candidate_day = (now_ist_init - timedelta(days=1)).date().isoformat()
        else:
            self._session_candidate_day = now_ist_init.date().isoformat()
        self._session_candidate_dir = Path("data_store/session_candidates")
        self._load_session_trade_candidates()
        
        # ── CONVERT SESSION CANDIDATES TO TRADES FOR HISTORICAL MODE (INIT) ──
        # If starting in HISTORICAL mode, convert session candidates to Trade objects
        # so the trades panel shows real data immediately.
        if self.mode == "HISTORICAL":
            session_candidates = getattr(self, "_session_trade_candidates", {})
            if session_candidates:
                import pytz
                ist = pytz.timezone('Asia/Kolkata')
                
                # Collect ENTRY and EXIT rows by match key
                entry_map = {}
                exit_map = {}
                
                for instrument, book in session_candidates.items():
                    for key, cand in book.items():
                        action = getattr(cand, 'action', 'ENTRY')
                        match_key = (
                            getattr(cand, 'instrument', instrument),
                            getattr(cand, 'direction', 'LONG'),
                            getattr(cand, 'timeframe', '5min'),
                            getattr(cand, 'timestamp', '')
                        )
                        if action == 'ENTRY':
                            entry_map[match_key] = cand
                        elif action == 'EXIT':
                            exit_map[match_key] = cand
                
                # Pair ENTRY with EXIT rows to create Trade objects
                trades_created = 0
                for match_key, entry_cand in entry_map.items():
                    cand_id = getattr(entry_cand, 'trade_id', None) or f"SESSION_{match_key[0]}_{match_key[3]}"
                    if any(ct.id == cand_id for ct in self.trades.closed_trades):
                        continue
                    
                    signal_ts = getattr(entry_cand, 'signal_timestamp', None)
                    if not signal_ts:
                        try:
                            signal_ts = datetime.fromisoformat(getattr(entry_cand, 'timestamp', ''))
                            if signal_ts.tzinfo is None:
                                signal_ts = IST.localize(signal_ts)
                        except Exception:
                            continue
                    
                    entry_time = signal_ts if signal_ts.tzinfo else IST.localize(signal_ts)
                    
                    exit_cand = exit_map.get(match_key)
                    exit_time = None
                    exit_price = 0.0
                    exit_reason = 'SESSION_END'
                    pnl = float(getattr(entry_cand, 'pnl', 0.0))
                    
                    if exit_cand:
                        exit_ts_val = getattr(exit_cand, 'exit_timestamp', None)
                        if exit_ts_val:
                            if isinstance(exit_ts_val, datetime):
                                exit_time = exit_ts_val
                            elif isinstance(exit_ts_val, str) and exit_ts_val.strip():
                                try:
                                    exit_time = datetime.fromisoformat(exit_ts_val)
                                except Exception:
                                    pass
                            if exit_time and exit_time.tzinfo is None:
                                exit_time = IST.localize(exit_time)
                        exit_price = float(getattr(exit_cand, 'exit_price', 0.0)) if getattr(exit_cand, 'exit_price', None) else 0.0
                        if exit_price == 0:
                            exit_price = float(getattr(exit_cand, 'current_price', 0.0))
                        exit_reason = getattr(exit_cand, 'exit_reason', 'OPPOSITE_SIGNAL')
                        pnl = float(getattr(exit_cand, 'pnl', 0.0))
                    
                    from trading.trade_manager import Trade
                    trade = Trade(
                        id=cand_id,
                        instrument=getattr(entry_cand, 'instrument', match_key[0]),
                        timeframe=getattr(entry_cand, 'timeframe', '5min'),
                        direction=getattr(entry_cand, 'direction', 'LONG'),
                        entry_price=float(getattr(entry_cand, 'price', 0.0)),
                        entry_time=entry_time,
                        trailing_stop=float(getattr(entry_cand, 'stop', 0.0)),
                        current_stop=float(getattr(entry_cand, 'stop', 0.0)),
                        lots=int(getattr(entry_cand, 'lots', 1)),
                        lot_size=int(getattr(entry_cand, 'lot_size', 1)),
                        grade=getattr(entry_cand, 'grade', 'B'),
                        atm_strike=float(getattr(entry_cand, 'atm_strike', 0.0)),
                        option_type=getattr(entry_cand, 'option_type', ''),
                        target=float(getattr(entry_cand, 'target', 0.0)),
                        rr_ratio=float(getattr(entry_cand, 'rr', 1.5)),
                        confidence=float(getattr(entry_cand, 'confidence', 0.0)),
                        inst_type=getattr(entry_cand, 'inst_type', 'FUT'),
                        instrument_multiplier=1.0,
                        exec_type=getattr(entry_cand, 'exec_type', 'A'),
                        current_price=float(getattr(entry_cand, 'current_price', getattr(entry_cand, 'price', 0.0))),
                        entry_spot=float(getattr(entry_cand, 'entry_spot', getattr(entry_cand, 'price', 0.0))),
                        status="CLOSED" if exit_price > 0 else "OPEN",
                        broker_order_id=getattr(entry_cand, 'trading_symbol', ''),
                        trading_symbol=getattr(entry_cand, 'trading_symbol', ''),
                        symbol_token=getattr(entry_cand, 'symbol_token', ''),
                        peak_pnl=0.0,
                        max_drawdown=0.0,
                        charges=200.0 if getattr(entry_cand, 'inst_type') == 'FUT' else 100.0,
                    )
                    
                    if exit_price > 0 and exit_time:
                        trade.exit_price = exit_price
                        trade.exit_time = exit_time
                        trade.exit_reason = exit_reason
                        trade.pnl = pnl
                        trade.status = "CLOSED"
                        self.trades.closed_trades.append(trade)
                    else:
                        self.trades.open_trades[trade.id] = trade
                    trades_created += 1
                
                logger.info(f"📊 INIT: Converted {trades_created} session candidates to trades for HISTORICAL mode")
        
        self._last_opposite_exit_signal_time: Dict[str, datetime] = {}
        self._last_intel_fetch: Dict[str, float] = {}
        self._chain_cache: Dict[str, pd.DataFrame] = {}
        self._chain_quality_cache: Dict[str, Dict] = {}
        self._last_chain_fetch: Dict[str, float] = {}
        self._premium_cache: Dict[str, Dict] = {}
        self._candidate_ltp_cache: Dict[str, Dict] = {}
        self._candidate_process_cache: Dict[str, Dict] = {}
        self._intel_save_inflight = False
        self.active_indices = settings.active_indices
        self.system_power = "ON"
        
        from data.data_manager import DataManager
        self.data_manager = DataManager(self.data)
        
        self.is_calculating = False
        self.is_warmup = False
        self.simulation_id = int(time.time())
        self._daily_reset_done = False
        self._daily_maintenance_done = False
        self._pending_live_signals = {}

        # â• â• â•  CACHED PROCESS RESULTS (avoid double-processing) â• â• â•
        self._cached_results: Dict[str, Dict] = {}

        # â• â• â•  SYSTEM ACTIVITY LOG â• â• â•
        import collections
        self.activity_log = collections.deque(maxlen=2000)
        self._max_log_size = 50
        self.log_event("UT1 System Initialized", "system")

        # â”€â”€ Over-trading Guards (New) â”€â”€
        self._trades_today = {} # {instrument: count}
        self._losses_today = {} # {instrument: count}
        self._last_exit_time = {} # {instrument: timestamp}
        self._last_reset_date = None

        # â• â• â•  PRE-MARKET ANALYSIS (AngelOne Master Integration) â• â• â•
        from engine.expiry_manager import expiry_manager
        self.expiry = expiry_manager
        self.market_info = self.expiry.pre_market_check()

    def _reset_daily_counters(self):
        """Resets daily trading counters at 09:15 AM IST"""
        today = datetime.now().date()
        if self._last_reset_date != today:
            self._trades_today = {}
            self._losses_today = {}
            self._last_exit_time = {}
            self._last_reset_date = today
            logger.info("ðŸ“… Daily over-trading counters reset.")

    def _can_trade_instrument(self, instrument: str, signal_time: datetime) -> Tuple[bool, str]:
        """Checks if we should allow another trade for this instrument today"""
        self._reset_daily_counters()

        # Rule 1: Max Trades per Index (Prevent over-churn)
        max_trades = 5
        count = self._trades_today.get(instrument, 0)
        if count >= max_trades:
            return False, f"Max daily trades ({max_trades}) reached"

        # Rule 2: Max Consecutive Losses (Prevent death spirals)
        losses = self._losses_today.get(instrument, 0)
        if losses >= 3:
            return False, f"Index HALTED: 3 consecutive losses"

        # Rule 3: Cooldown Period (Prevent revenge trading)
        last_exit = self._last_exit_time.get(instrument)
        if last_exit:
            # 30 minute cooldown between trades of same instrument
            cooldown_mins = 30
            # For historical simulation, use signal timestamp; for live, use now
            diff = (signal_time - last_exit.replace(tzinfo=None)).total_seconds() / 60.0
            if diff < cooldown_mins:
                return False, f"Cooldown active: {cooldown_mins - diff:.0f}m remaining"

        return True, ""

    def get_broadcast_config(self):
        """Standardized config payload for UI broadcasting"""
        return {
            "capital_total": self.capital_total,
            "capital_fut": self.capital_fut,
            "risk_fut_pct": self.risk_fut_pct,
            "capital_opt": self.capital_opt,
            "risk_opt_pct": self.risk_opt_pct,
            "lots": self.user_lots,
            "lots_fut": self.user_lots_fut,
            "fut_sl": self.futures_sl_pct,
            "opt_sl": self.options_sl_pct,
            "backtest_days": self.backtest_days,
            "auto_mode": self.auto_mode,
            "inst_pref": self.inst_pref,
            "active_indices": self.active_indices,
            "strike_selection": getattr(get_settings(), "option_strike_selection", "BOTH"),
            "sl_mode": getattr(get_settings(), "sl_mode", "NATURAL"),
            "grade_preference": getattr(get_settings(), "signal_grade_preference", "auto"),
            "ut_regime_adaptation": getattr(get_settings(), "ut_regime_adaptation", False),
            "ut_concurrency_guard": getattr(get_settings(), "ut_concurrency_guard", True),
            "ut_no_entry_after": getattr(get_settings(), "ut_no_entry_after", "15:01"),
            "intelligence_cache_ttl_seconds": getattr(get_settings(), "intelligence_cache_ttl_seconds", 30.0),
            "history_cache_ttl_seconds": getattr(get_settings(), "history_cache_ttl_seconds", 60.0),
            "ut_option_history_mode": getattr(get_settings(), "ut_option_history_mode", "fetch_or_synthetic"),
            "ut_timeframe_entry_policy": getattr(get_settings(), "ut_timeframe_entry_policy", "PRIMARY_15"),
            "ut_5min_option_min_confidence": getattr(get_settings(), "ut_5min_option_min_confidence", 0.90),
            "ut_5min_loss_cooldown_minutes": getattr(get_settings(), "ut_5min_loss_cooldown_minutes", 45),
            "ut_preset": getattr(get_settings(), "ut_preset", "AGGRESSIVE")
        }

    def configure(self, capital_total=None, capital_fut=None, capital_opt=None, risk_fut_pct=None, risk_opt_pct=None,
                  lots=None, lots_fut=None, mode=None, reset=False, futures_sl_pct=None, options_sl_pct=None,
                  backtest_days=None, auto_mode=None, inst_pref=None, strike_selection=None, active_indices=None,
                  grade_preference=None, concurrency_guard=None, dynamic_risk_enabled=None,
                  dynamic_risk_hybrid=None, confirm_real_mode=False, real_mode_verification=None, ut_preset=None,
                  timeframe_entry_policy=None, **kwargs):
        from config.settings import get_settings
        settings = get_settings()

        if mode == "REAL" and getattr(self, "mode", None) != "REAL":
            verification_text = str(real_mode_verification or kwargs.get("real_mode_verification", "")).strip().upper()
            if not confirm_real_mode or verification_text not in {"YES", "REAL"}:
                logger.warning("REAL mode blocked: two-step verification is required.")
                if self.on_notification:
                    self.on_notification("REAL mode blocked. Two-step verification is required.", "error")
                return False

        needs_full_refresh = False

        if concurrency_guard is not None:
            old_val = getattr(settings, "ut_concurrency_guard", True)
            new_val = bool(concurrency_guard)
            if old_val != new_val:
                settings.ut_concurrency_guard = new_val
                needs_full_refresh = True
                logger.info(f"ðŸ›¡ï¸  Concurrency Guard updated via configure: {new_val}")

        if strike_selection is not None:
            settings.option_strike_selection = strike_selection
            logger.info(f"ðŸŽ¯ Strike Selection updated: {strike_selection}")

        if grade_preference is not None:
            settings.signal_grade_preference = grade_preference
            logger.info(f"ðŸŽ¯ Signal Grade Preference updated: {grade_preference}")

        if ut_preset is not None:
            preset = str(ut_preset or "AGGRESSIVE").upper()
            settings.ut_preset = preset
            if hasattr(self.mtf, "apply_engine_params"):
                self.mtf.apply_engine_params(settings.get_ut_engine_params())
                needs_full_refresh = True
            logger.info(f"ðŸŽ¯ UT Bot Preset updated: {ut_preset}")

        if timeframe_entry_policy is not None:
            policy = str(timeframe_entry_policy or "PRIMARY_15").upper()
            if policy not in {"PRIMARY_15", "INCLUDE_5MIN"}:
                policy = "PRIMARY_15"
            if getattr(settings, "ut_timeframe_entry_policy", "PRIMARY_15") != policy:
                settings.ut_timeframe_entry_policy = policy
                needs_full_refresh = True
                logger.info(f"Timeframe entry policy updated: {policy}")

        if capital_total is not None:
            self.capital_total = float(capital_total)
            logger.info(f"ðŸ’° Total Capital updated: â‚¹{self.capital_total:,.0f}")

        if auto_mode is not None:
            self.auto_mode = bool(auto_mode)
            logger.info(f"ðŸ”„ Auto Mode: {'ENABLED' if self.auto_mode else 'DISABLED'}")

        if inst_pref and inst_pref != self.inst_pref:
            self.inst_pref = inst_pref
            needs_full_refresh = True
            logger.info(f"ðŸ”„ Instrument Preference changed to: {self.inst_pref}")

        if backtest_days and int(backtest_days) != self.backtest_days:
            self.backtest_days = int(backtest_days)
            needs_full_refresh = True
            logger.info(f"ðŸ”„ Backtest window changed to: {self.backtest_days} days")
            self.simulation_id = int(time.time()) # Update sim ID to force full broadcast
            self._hist_candidates_loaded = False  # Force reload of session candidates

        if capital_fut and capital_fut != self.capital_fut:
            self.capital_fut = capital_fut
            needs_full_refresh = True
        if capital_opt and capital_opt != self.capital_opt:
            self.capital_opt = capital_opt
            needs_full_refresh = True
        if risk_fut_pct and risk_fut_pct != self.risk_fut_pct:
            self.risk_fut_pct = risk_fut_pct
            needs_full_refresh = True
        if risk_opt_pct and risk_opt_pct != self.risk_opt_pct:
            self.risk_opt_pct = risk_opt_pct
            needs_full_refresh = True

        if lots and lots != self.user_lots:
            self.user_lots.update(lots)
            needs_full_refresh = True

        if lots_fut and lots_fut != self.user_lots_fut:
            self.user_lots_fut.update(lots_fut)
            needs_full_refresh = True

        if futures_sl_pct and futures_sl_pct != self.futures_sl_pct:
            self.futures_sl_pct = futures_sl_pct
            needs_full_refresh = True
        if options_sl_pct and options_sl_pct != self.options_sl_pct:
            self.options_sl_pct = options_sl_pct
            needs_full_refresh = True

        # â• â•  SYNC TO TRADE MANAGER â• â•
        self.trades.update_risk_settings(self.futures_sl_pct, self.options_sl_pct)

        # â• â•  SYNC TO RISK MANAGER (Circuit Breaker) â• â•
        self.risk_manager.update_settings(
            max_daily_loss_pct=self.max_daily_loss_pct,
            risk_fut_pct=self.risk_fut_pct,
            risk_opt_pct=self.risk_opt_pct,
            capital_fut=self.capital_fut,
            capital_opt=self.capital_opt,
            capital_total=self.capital_total
        )

        if active_indices is not None and active_indices != self.active_indices:
            self.active_indices = active_indices
            needs_full_refresh = True
            logger.info(f"ðŸ”„ Active Indices updated: {self.active_indices}")

        if mode:
            # Mode Map: HISTORICAL, REAL
            old_mode = self.mode

            # â”€â”€â”€ REAL MODE SAFETY CONFIRMATION â”€â”€â”€
            # Prevent accidental hot-swap to REAL mode without an explicit override
            if mode == "REAL" and old_mode != "REAL":
                safety_confirm = confirm_real_mode or kwargs.get("confirm_real_mode", False)
                verification_text = str(real_mode_verification or kwargs.get("real_mode_verification", "")).strip().upper()
                if safety_confirm and verification_text not in {"YES", "REAL"}:
                    logger.warning("REAL mode blocked: two-step verification is required.")
                    if self.on_notification:
                        self.on_notification("REAL mode blocked. Two-step verification is required.", "error")
                    return False
                if not safety_confirm:
                    logger.warning("ðŸš¨ BLOCKED: Attempted to switch to REAL mode without explicit confirmation.")
                    return
                logger.warning("âš ï¸  ATTENTION: System switching to REAL mode. Orders will use real money!")

            self.mode = mode
            self.trades.mode = mode
            logger.info(f"ðŸ”„ Mode switched: {old_mode} âž” {mode}")

            # If switching TO or FROM Historical, we need a full state purge
            if mode == "HISTORICAL" or old_mode == "HISTORICAL":
                needs_full_refresh = True

            # â• â• â•  HOT-SWAP BROKER â• â• â•
            if mode == "REAL":
                # â”€â”€â”€ REAL BROKER INITIALIZATION (Dynamic) â”€â”€â”€
                # If real_broker is missing, try to connect and initialize it now
                if not hasattr(self.trades, "real_broker") or not self.trades.real_broker:
                    logger.info("ðŸ“¡ Attempting dynamic REAL broker connection...")
                    if not self.data.is_connected:
                        self.data.connect()

                    if self.data.is_connected:
                        from trading.broker import SmartApiBroker
                        new_real_broker = SmartApiBroker(self.data.smart_api)
                        self.trades.real_broker = new_real_broker
                        logger.info("âœ… Dynamic Broker Connection Successful")
                    else:
                        logger.error("â Œ CRITICAL: Failed to connect AngelOne for REAL mode!")

                if hasattr(self.trades, "real_broker") and self.trades.real_broker:
                    self.trades.broker = self.trades.real_broker
                    logger.info("ðŸ“¡ Broker Hot-Swapped to: LIVE EXECUTION (AngelOne)")
                else:
                    logger.error("â Œ CRITICAL: Attempted REAL mode without Broker connection!")
                    self.mode = "HISTORICAL" # Safety fallback
                    self.trades.broker = getattr(self.trades, "mock_broker", self.trades.broker)
                    # Sync trade manager mode back to fallback
                    self.trades.mode = self.mode
                    if self.on_notification:
                        self.on_notification("Failed to switch to REAL mode! AngelOne not connected.", "error")
            else:
                # Historical Mode: Signals analysed from Real Historical Data
                self.trades.broker = getattr(self.trades, "mock_broker", self.trades.broker)
                logger.info(f"ðŸ“¡ Broker Mode: {mode} (Signal-Based Analysis)")

            self.log_event(f"System Mode: {mode}", "system")

        if reset:
            # User explicitly requested a full reset of P&L and cache
            self.trades.reset_pnl()
            self.perf.reset()
            self._cached_results.clear()
            for attr in list(self.__dict__.keys()):
                if attr.startswith("chart_cache_"):
                    delattr(self, attr)
            if hasattr(self, "_cached_hist_signals"):
                delattr(self, "_cached_hist_signals")
            if hasattr(self, "_cached_hist_closed_count"):
                delattr(self, "_cached_hist_closed_count")

            self.simulation_id = int(time.time())

            if hasattr(self.mtf, "_result_cache"):
                self.mtf._result_cache.clear()
            if hasattr(self.mtf, "_data_hash"):
                self.mtf._data_hash.clear()
            if hasattr(self, "_session_trade_candidates"):
                self._session_trade_candidates.clear()
                self._persist_session_trade_candidates()
            if hasattr(self, "_latest_trade_candidates"):
                self._latest_trade_candidates.clear()
            if hasattr(self, "_latest_exit_candidates"):
                self._latest_exit_candidates.clear()
            if hasattr(self, "_last_signal_time"):
                self._last_signal_time.clear()
            if hasattr(self, "_last_live_signal_candle_time"):
                self._last_live_signal_candle_time.clear()
            if hasattr(self, "_pending_live_signals"):
                self._pending_live_signals.clear()

            # Reset scan count to trigger initial logic in next cycle
            self.scan_count = 0
            # Force immediate history fetch and wait for it
            asyncio.create_task(self._perform_full_recalculation())
        elif needs_full_refresh:
            # Switch modes or settings refresh - DO NOT clear real closed/open trades of today!
            # Just clear historical/backtest simulated trades from memory
            self.trades.closed_trades = [
                t for t in self.trades.closed_trades 
                if not (t.id.startswith("H_") or t.id.startswith("EOD_"))
            ]
            self.perf.reset()
            self._cached_results.clear()
            for attr in list(self.__dict__.keys()):
                if attr.startswith("chart_cache_"):
                    delattr(self, attr)
            if hasattr(self, "_cached_hist_signals"):
                delattr(self, "_cached_hist_signals")
            if hasattr(self, "_cached_hist_closed_count"):
                delattr(self, "_cached_hist_closed_count")

            self.simulation_id = int(time.time())

            if hasattr(self.mtf, "_result_cache"):
                self.mtf._result_cache.clear()
            if hasattr(self.mtf, "_data_hash"):
                self.mtf._data_hash.clear()
            
            # DO NOT clear self._session_trade_candidates, as it holds today's live/paper session signals.
            # Only clear the volatile latest display caches
            if hasattr(self, "_latest_trade_candidates"):
                self._latest_trade_candidates.clear()
            if hasattr(self, "_latest_exit_candidates"):
                self._latest_exit_candidates.clear()
            if hasattr(self, "_last_signal_time"):
                self._last_signal_time.clear()
            if hasattr(self, "_last_live_signal_candle_time"):
                self._last_live_signal_candle_time.clear()
            if hasattr(self, "_pending_live_signals"):
                self._pending_live_signals.clear()

            # Reload session candidates from the correct path for the new mode/settings
            self._load_session_trade_candidates()

            self.scan_count = 0
            asyncio.create_task(self._perform_full_recalculation())

        return True

    async def _perform_full_recalculation(self):
        """Sequential sequence to ensure re-simulation uses NEW data"""
        if getattr(self, "_calculation_lock", False):
            logger.warning("ðŸš« Re-simulation delayed: Calculation lock active.")
            # Wait for the current cycle to finish
            for _ in range(10):
                await asyncio.sleep(1)
                if not getattr(self, "_calculation_lock", False): break

        self._calculation_lock = True
        self._lock_time = time.time()
        try:
            logger.info("ðŸ”„ RE-SIMULATION SEQUENCE: Fetching deep history...")

            # CLEAR TRADES: Preserve real session trades, only remove simulated historical ones.
            # Real trades (not starting with H_ or EOD_) come from actual live sessions and
            # should be kept for accurate historical playback. Simulated trades are rebuilt
            # from candle data and may diverge from real execution.
            real_trades = [ct for ct in self.trades.closed_trades if not ct.id.startswith("H_") and not ct.id.startswith("EOD_")]
            self.trades.closed_trades = real_trades
            self.trades.open_trades = {}

            # ── CONVERT SESSION CANDIDATES TO TRADES FOR HISTORICAL MODE ──
            # In HISTORICAL mode, if session candidates exist (from a real live session),
            # convert them to Trade objects so the trades panel shows real data.
            if self.mode == "HISTORICAL":
                self._load_session_trade_candidates()
                session_candidates = getattr(self, "_session_trade_candidates", {})
                if session_candidates:
                    import pytz
                    ist = pytz.timezone('Asia/Kolkata')
                    
                    # First, collect all ENTRY and EXIT rows by their match key
                    entry_map = {}  # (instrument, direction, timeframe, timestamp) -> candidate
                    exit_map = {}   # (instrument, direction, timeframe, timestamp) -> candidate
                    
                    for instrument, book in session_candidates.items():
                        for key, cand in book.items():
                            action = getattr(cand, 'action', 'ENTRY')
                            match_key = (
                                getattr(cand, 'instrument', instrument),
                                getattr(cand, 'direction', 'LONG'),
                                getattr(cand, 'timeframe', '5min'),
                                getattr(cand, 'timestamp', '')
                            )
                            if action == 'ENTRY':
                                entry_map[match_key] = cand
                            elif action == 'EXIT':
                                exit_map[match_key] = cand
                    
                    # Now pair ENTRY with EXIT rows to create complete Trade objects
                    trades_created = 0
                    for match_key, entry_cand in entry_map.items():
                        # Skip if already in closed_trades (avoid duplicates)
                        cand_id = getattr(entry_cand, 'trade_id', None) or f"SESSION_{match_key[0]}_{match_key[3]}"
                        if any(ct.id == cand_id for ct in self.trades.closed_trades):
                            continue
                        
                        signal_ts = getattr(entry_cand, 'signal_timestamp', None)
                        if not signal_ts:
                            try:
                                signal_ts = datetime.fromisoformat(getattr(entry_cand, 'timestamp', ''))
                                if signal_ts.tzinfo is None:
                                    signal_ts = IST.localize(signal_ts)
                            except Exception:
                                continue
                        
                        entry_time = signal_ts if signal_ts.tzinfo else IST.localize(signal_ts)
                        
                        # Find matching EXIT row
                        exit_cand = exit_map.get(match_key)
                        exit_time = None
                        exit_price = 0.0
                        exit_reason = 'SESSION_END'
                        pnl = float(getattr(entry_cand, 'pnl', 0.0))
                        
                        if exit_cand:
                            exit_ts_val = getattr(exit_cand, 'exit_timestamp', None)
                            if exit_ts_val:
                                if isinstance(exit_ts_val, datetime):
                                    exit_time = exit_ts_val
                                elif isinstance(exit_ts_val, str) and exit_ts_val.strip():
                                    try:
                                        exit_time = datetime.fromisoformat(exit_ts_val)
                                    except Exception:
                                        pass
                                if exit_time and exit_time.tzinfo is None:
                                    exit_time = IST.localize(exit_time)
                            exit_price = float(getattr(exit_cand, 'exit_price', 0.0)) if getattr(exit_cand, 'exit_price', None) else 0.0
                            if exit_price == 0:
                                exit_price = float(getattr(exit_cand, 'current_price', 0.0))
                            exit_reason = getattr(exit_cand, 'exit_reason', 'OPPOSITE_SIGNAL')
                            pnl = float(getattr(exit_cand, 'pnl', 0.0))
                        
                        trade = Trade(
                            id=cand_id,
                            instrument=getattr(entry_cand, 'instrument', match_key[0]),
                            timeframe=getattr(entry_cand, 'timeframe', '5min'),
                            direction=getattr(entry_cand, 'direction', 'LONG'),
                            entry_price=float(getattr(entry_cand, 'price', 0.0)),
                            entry_time=entry_time,
                            trailing_stop=float(getattr(entry_cand, 'stop', 0.0)),
                            current_stop=float(getattr(entry_cand, 'stop', 0.0)),
                            lots=int(getattr(entry_cand, 'lots', 1)),
                            lot_size=int(getattr(entry_cand, 'lot_size', 1)),
                            grade=getattr(entry_cand, 'grade', 'B'),
                            atm_strike=float(getattr(entry_cand, 'atm_strike', 0.0)),
                            option_type=getattr(entry_cand, 'option_type', ''),
                            target=float(getattr(entry_cand, 'target', 0.0)),
                            rr_ratio=float(getattr(entry_cand, 'rr', 1.5)),
                            confidence=float(getattr(entry_cand, 'confidence', 0.0)),
                            inst_type=getattr(entry_cand, 'inst_type', 'FUT'),
                            instrument_multiplier=1.0,
                            exec_type=getattr(entry_cand, 'exec_type', 'A'),
                            current_price=float(getattr(entry_cand, 'current_price', getattr(entry_cand, 'price', 0.0))),
                            entry_spot=float(getattr(entry_cand, 'entry_spot', getattr(entry_cand, 'price', 0.0))),
                            status="CLOSED" if exit_price > 0 else "OPEN",
                            broker_order_id=getattr(entry_cand, 'trading_symbol', ''),
                            trading_symbol=getattr(entry_cand, 'trading_symbol', ''),
                            symbol_token=getattr(entry_cand, 'symbol_token', ''),
                            peak_pnl=0.0,
                            max_drawdown=0.0,
                            charges=200.0 if getattr(entry_cand, 'inst_type') == 'FUT' else 100.0,
                        )
                        
                        if exit_price > 0 and exit_time:
                            trade.exit_price = exit_price
                            trade.exit_time = exit_time
                            trade.exit_reason = exit_reason
                            trade.pnl = pnl
                            trade.status = "CLOSED"
                        
                        self.trades.closed_trades.append(trade)
                        trades_created += 1
                    
                    logger.info(f"📊 Converted {trades_created} session candidates to trades for HISTORICAL mode")

            # Filter indices based on active_indices setting
            all_indices = self.config.get("indices", {})
            indices = {k: v for k, v in all_indices.items() if k in self.active_indices}
            await self._fetch_all_data(indices, force=True)

            logger.info("ðŸ”„ RE-SIMULATION SEQUENCE: Recalculating signals and trades...")
            self._last_signal_time.clear() # Clear here ONLY after data is ready
            self.simulation_id = int(time.time())
            # The next _scan_cycle will now pick up the new days_back and empty last_signal_time
            logger.info("ðŸ§¹ System state RESET with FORCED history fetch.")
        finally:
            self._calculation_lock = False

    async def perform_system_recalibration(self):
        """
        Recalibrates the system by releasing locks, resetting HFT semaphores,
        clearing calculation/UI caches, and forcing a fresh market data sync,
        while preserving all live/historical trades and session data.
        """
        logger.info("🔄 SYSTEM RECALIBRATION: Initializing 1-second system refresh...")
        
        # 1. Release stuck locks
        self._calculation_lock = False
        self.is_calculating = False
        if hasattr(self, "_ltp_semaphore"):
            self._ltp_semaphore = asyncio.Semaphore(5)

        # 2. Clear chart serialization, math, and engine caches
        self._cached_results.clear()
        
        # Clear specific chart cache attributes
        for attr in list(self.__dict__.keys()):
            if attr.startswith("chart_cache_"):
                delattr(self, attr)

        if hasattr(self.mtf, "_result_cache"):
            self.mtf._result_cache.clear()
        if hasattr(self.mtf, "_data_hash"):
            self.mtf._data_hash.clear()

        # 3. Force re-fetching data without clearing trades or P&L
        all_indices = self.config.get("indices", {})
        indices = {k: v for k, v in all_indices.items() if k in self.active_indices}
        
        # Reset fetch timestamps to force data retrieval even in offline markets
        self._last_data_fetch.clear()
        
        # Fetch fresh data
        try:
            await self._fetch_all_data(indices, force=True)
        except Exception as e:
            logger.error(f"Error fetching data during recalibration: {e}")
            
        # 4. Trigger scan cycle calculation update immediately
        self.simulation_id = int(time.time()) # Signal UI of refresh
        logger.success("✅ SYSTEM RECALIBRATION: System refreshed and recalibrated successfully!")

    async def _background_initial_setup(self, indices: Dict):
        """Heavy initialization run in background to prevent event loop freeze"""
        try:
            # 1. Force initial fetch once
            await self._fetch_all_data(indices, force=True)

            # 2. SYNC INTELLIGENCE HISTORY (Zero-Skip Memory)
            if self.mode == "REAL" and self.data.is_market_open():
                logger.info("Live market startup: skipping deep historical intelligence replay to keep manual REAL mode responsive.")
            else:
                await self._sync_intelligence_history(indices)

            # 2b. PRE-FETCH PREVIOUS CLOSES (Latency Fix)
            logger.info("ðŸ“¡ Pre-fetching previous closes for latency optimization...")
            for name in indices:
                self.data.get_previous_close(name)
            self.log_event("ðŸ“¡ Market Data Baselines (Prev Closes) Synchronized", "data")

            # 3. If we are in a live mode after market hours, re-simulate TODAY
            # This populates the Results table with today's performance if state was empty
            if (
                self.mode == "REAL"
                and not self.data.is_market_open()
                and not self.trades.open_trades
                and not self.trades.closed_trades
            ):
                logger.info("ðŸ”¥ WARM STARTUP: Re-simulating today's activity for dashboard population...")
                # Temporarily enable calculation to backfill
                old_power = self.system_power
                self.system_power = "ON"
                self.is_warmup = True

                # Process all instruments once to trigger signal/trade generation from history
                tasks = [self._process_instrument_async(name, cfg) for name, cfg in indices.items()]
                trade_signals = await asyncio.gather(*tasks)

                self.system_power = old_power
                self.is_warmup = False
                self.log_event("ðŸ”¥ Warm Startup Complete: All index histories restored", "success")
                logger.info("ðŸ”¥ WARM STARTUP: All index histories restored.")

        except Exception as e:
            logger.error(f"â Œ Error in background initial setup: {e}")
            self.log_event(f"â Œ Error in background initial setup: {e}", "error")

    async def run(self):
        self.is_running = True
        logger.info("ðŸš€ Scanner started â€” 3 indices Ã— 3 TFs")

        # â• â• â•  Launch High-Priority Background Workers â• â• â•
        asyncio.create_task(self._background_ltp_worker())
        asyncio.create_task(self._background_intel_worker())
        asyncio.create_task(self._background_history_worker())
        asyncio.create_task(self._background_hft_worker())

        while self.is_running:
            try:
                # â• â• â•  CONTINUOUS SCANNING (Calculation Only - Fast Path) â• â• â•
                await self._scan_cycle()
                await asyncio.sleep(self.scan_interval)
            except Exception as e:
                import traceback
                logger.error(f"Scanner error: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)

    async def _background_hft_worker(self):
        """High-Frequency Trade Monitoring (100ms loop) for Spike Readiness"""
        logger.info("âš¡ HFT Trade Monitor Started (100ms loop)")
        while self.is_running:
            try:
                self._update_active_trades()
                await asyncio.sleep(0.1) # 100ms check loop
            except Exception as e:
                logger.error(f"Error in HFT worker: {e}")
                await asyncio.sleep(1)

    async def _background_ltp_worker(self):
        """Constant high-priority LTP polling utilizing 85% of SmartAPI throughput"""
        while self.is_running:
            try:
                indices = {k: v for k, v in self.config.get("indices", {}).items() if k in self.active_indices}
                SPOT_TOKENS = {"NIFTY": "99926000", "BANKNIFTY": "99926009", "SENSEX": "99919000"}

                tasks = []
                for name in indices:
                    token = SPOT_TOKENS.get(name)
                    if token:
                        tasks.append(self._fetch_ltp_raw(name, token))

                if tasks:
                    await asyncio.gather(*tasks)

                # Stagger to utilize ~15 pings/sec (Safe under 20/sec limit)
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.debug(f"LTP Worker error: {e}")
                await asyncio.sleep(1)

    async def _fetch_ltp_raw(self, name, token):
        try:
            loop = asyncio.get_event_loop()
            await self._ltp_rate_limiter.consume(1)
            async with self._ltp_semaphore:
                live_price = await loop.run_in_executor(
                    None,
                    lambda: self.data.get_latest_price_by_token(
                        token=token, symbol=name,
                        exchange="NSE" if "SENSEX" not in name else "BSE"
                    )
                )
                if live_price and live_price > 0:
                    self.candles.update_latest_price(name, live_price)
        except: pass

    async def _background_intel_worker(self):
        """Background Options Intel Fetcher (PCR, OI, Greeks) â€” Non-Blocking"""
        while self.is_running:
            try:
                indices = {k: v for k, v in self.config.get("indices", {}).items() if k in self.active_indices}
                for name, cfg in indices.items():
                    spot = self.candles.get_latest_price(name)
                    if spot <= 0: continue

                    try:
                        options_chain, chain_quality = await asyncio.get_event_loop().run_in_executor(
                            None, lambda: self._get_option_chain_cached(name, cfg)
                        )
                        if options_chain is not None:
                            # Pre-calculate intel in background
                            candle_5min = self.candles.get_candles(name, "5min")
                            candle_1min = self.candles.get_candles(name, "1min")

                            # Cache the result for the main loop
                            intel_res = self.intel.analyze(
                                instrument=name, timeframe="5min", candle_df=candle_5min,
                                candle_1min_df=candle_1min, options_chain=options_chain,
                                spot_price=spot, strike_interval=cfg.get("strike_interval", 50),
                                days_to_expiry=7, price_change_pct=0, # Approx
                                chain_quality=chain_quality
                            )
                            self._intel_cache[name] = intel_res
                            if not hasattr(self, '_intel_cache_time'):
                                self._intel_cache_time = {}
                            self._intel_cache_time[name] = time.time()
                            self._last_intel_fetch[name] = time.time()
                    except: pass
                    await asyncio.sleep(5) # Stagger between instruments
                await asyncio.sleep(25) # Main interval
            except Exception as e:
                await asyncio.sleep(5)

    async def _background_history_worker(self):
        """Background OHLCV sync. In REAL mode keep scans light and refresh deep candles separately."""
        while self.is_running:
            try:
                indices = {k: v for k, v in self.config.get("indices", {}).items() if k in self.active_indices}
                if self.mode == "REAL":
                    await self._fetch_all_data(indices, timeframes=["1min"])
                    last_full = getattr(self, "_last_full_history_refresh", 0.0)
                    if time.time() - last_full >= 300.0:
                        await self._fetch_all_data(indices, timeframes=["5min", "15min"])
                        self._last_full_history_refresh = time.time()
                else:
                    await self._fetch_all_data(indices)
                await asyncio.sleep(self._data_fetch_interval)
            except:
                await asyncio.sleep(10)

    def log_event(self, message: str, type: str = "info"):
        """Add message to system activity log with visual icons"""
        # Suppress trade-related logs during historical warmup to avoid UI noise
        if getattr(self, "is_warmup", False) and type == "trade":
            return

        icons = {
            "info": "ℹ️",
            "success": "✅",
            "warning": "⚠️",
            "error": "❌",
            "trade": "🔔",
            "system": "⚙️",
            "data": "📊",
            "intel": "🧠"
        }
        icon = icons.get(type, "🔹")
        self.activity_log.appendleft({
            "time": datetime.now().strftime("%H:%M:%S"),
            "msg": f"{icon} {message}",
            "type": type
        })

    async def _scan_cycle(self):
        # ═══ LOCK WATCHDOG (Must be at the very top to break deadlocks) ═══
        if getattr(self, "_calculation_lock", False):
            if hasattr(self, "_lock_time") and time.time() - self._lock_time > 30:
                logger.error("🚨 CRITICAL: Calculation lock stuck for 30s. Forcing reset.")
                self._calculation_lock = False
            else:
                # logger.warning("🚫 Scan cycle blocked: Calculation lock active.")
                return

        self.scan_count += 1
        t0 = time.time()
        self.risk_manager.check_circuit_breaker()
        results = {"timestamp": datetime.now().isoformat(), "instruments": {}, "activity_log": self.activity_log}

        # Filter indices based on active_indices setting
        all_indices = self.config.get("indices", {})
        indices = {k: v for k, v in all_indices.items() if k in self.active_indices}

        if self.scan_count == 1:
            self.log_event("📥 Initializing deep history fetch & warm startup in background...", "system")
            # Run the heavy initialization in the background to prevent screen freeze
            asyncio.create_task(self._background_initial_setup(indices))

        # ══ DAILY SESSION RESET (At 09:00 AM IST) ══
        now_ist = datetime.now(IST)
        if now_ist.hour == 9 and now_ist.minute == 0:
            if not getattr(self, "_daily_reset_done", False):
                self.log_event("🧹 New Session Prep: Clearing yesterday's trades and cache...", "system")
                if hasattr(self, "trades") and getattr(self.trades, "open_trades", {}):
                    logger.critical("🚨 ORPHANED TRADES DETECTED at 09:00 AM! Broker reconciliation required.")
                self.trades.reset_pnl()
                self._session_candidate_day = now_ist.date().isoformat()
                self._session_trade_candidates.clear()
                self._persist_session_trade_candidates()
                
                # Evict all options-specific in-memory caches to prevent memory bloat
                self._last_opposite_exit_signal_time.clear()
                self._last_intel_fetch.clear()
                self._chain_cache.clear()
                self._chain_quality_cache.clear()
                self._last_chain_fetch.clear()
                self._premium_cache.clear()
                self._candidate_ltp_cache.clear()
                self._candidate_process_cache.clear()
                self._cached_results.clear()
                
                # Prune market data provider tick caches to clear yesterday's tokens
                if hasattr(self.data, "_ws_cache"):
                    self.data._ws_cache.clear()
                if hasattr(self.data, "_ltp_cache"):
                    self.data._ltp_cache.clear()
                if hasattr(self.data, "_api_hist_cache"):
                    self.data._api_hist_cache.clear()
                if hasattr(self.data, "_last_api_hist_fetch"):
                    self.data._last_api_hist_fetch.clear()
                logger.info("🧹 In-memory options, history, greeks, and WS caches evicted.")
                
                try:
                    self.market_info = self.expiry.pre_market_check()
                    logger.info("🔄 Pre-Session Expiries Master reloaded successfully.")
                except Exception as e:
                    logger.error(f"❌ Failed to run pre-market check at daily reset: {e}")
                self._daily_reset_done = True
                logger.info("🧹 Pre-Session Refresh: TradeManager reset for the new day.")
        elif now_ist.hour == 9 and now_ist.minute == 17:
            self._daily_reset_done = False # Reset readiness flag for next session

        # ══ DAILY DATA MAINTENANCE (At 15:45 PM IST) ══
        if now_ist.hour == 15 and now_ist.minute == 45:
            if not getattr(self, "_daily_maintenance_done", False):
                self.log_event("🧹 Running Daily Data Maintenance & Pruning...", "system")
                asyncio.get_event_loop().run_in_executor(None, lambda: self.data_manager.run_daily_maintenance(list(self.active_indices)))
                self._daily_maintenance_done = True
        elif now_ist.hour == 15 and now_ist.minute == 47:
            self._daily_maintenance_done = False

        # â• â•  PERIODIC OFFICIAL CANDLE SYNC (Every 60s) â• â•
        # Ensures chart is 100% accurate with broker even if polling missed a tick
        # â• â•  SYSTEM POWER & MARKET STATUS CHECK â• â•
        is_market_open = self.data.is_market_open()

        # EOD Square-off monitoring (Always active if trades or manual signal rows exist)
        session_now = self._session_squareoff_clock()
        self._force_exit_session_candidates_at_eod(session_now)
        if self.trades.open_trades:
            if self.mode != "HISTORICAL":
                expired = self.trades.expire_stale_open_trades(now=session_now, reason="STALE_SESSION_END")
                if expired:
                    self.log_event(f"Closed {expired} stale prior-session trade(s)", "warning")
            self.trades.check_session_end(current_time=session_now)

        # Calculation Gate: Only blocked by SYSTEM POWER (not market hours)
        if self.system_power == "OFF":
            self.is_calculating = False
            results.update({
                "system_power": "OFF",
                "is_calculating": False,
                "market_status": "SYSTEM OFF",
                "mode": self.mode,
                "gateway_status": self.data.get_source_health(),
                "config": self.get_broadcast_config(),
                "trades": self._build_dashboard_trade_payload(),
                "scan_count": self.scan_count,
                "latency": 0,
                "timestamp": datetime.now(IST).isoformat()
            })
            self.latest_results = results
            if self.on_update:
                try: await self.on_update(results)
                except Exception as e:
                    logger.error(f"Failed to broadcast update: {e}")
            return

        # Market Status Tagging
        market_status = "OPEN" if is_market_open else "CLOSED"
        results["market_status"] = market_status

        self.is_calculating = True

        # Optimization: Clear results to prevent stale data ghosting
        results["instruments"] = {}

        self._calculation_lock = True
        self._lock_time = time.time()
        try:
            # Force full chart broadcast if simulation_id changed or first scan
            is_full_chart_needed = (
                getattr(self, "_last_sent_sim_id", 0) != self.simulation_id
            )

            # If we need a full chart, force a data fetch to ensure candles match the new days_back
            if is_full_chart_needed and self.scan_count > 1 and self.mode != "REAL":
                logger.info(f"ðŸ”„ FORCING FULL HISTORY FETCH ({self.backtest_days} days) for Simulation: {self.simulation_id}")
                await self._fetch_all_data(indices, force=True)

            async def process_task(name, cfg):
                try:
                    # 1. Use the price already fetched in the parallel loop at start of _scan_cycle
                    spot = self.candles.get_latest_price(name)

                    # If for some reason it's missing, try a quick local fetch
                    if spot <= 0:
                        spot = await asyncio.get_event_loop().run_in_executor(None, lambda: self.data.get_ltp(cfg.get("exchange", "NSE"), name, cfg.get("token", "")))
                        if spot and spot > 0: self.candles.update_latest_price(name, spot)

                    # 2. Run Analysis (in thread pool with timeout to avoid hanging)
                    loop = asyncio.get_event_loop()
                    try:
                        ui_data, candidate = await asyncio.wait_for(
                            loop.run_in_executor(None, lambda: self._process_instrument(name, cfg)),
                            timeout=60.0 if self.mode == "HISTORICAL" else 15.0
                        )
                        return name, (ui_data, candidate)
                    except asyncio.TimeoutError:
                        logger.error(f"â Œ Timeout processing {name}! Signal generation might be hanging.")
                        return name, ({"error": "Timeout"}, [])
                    except Exception as e:
                        logger.error(f"â Œ Error processing {name}: {e}")
                        return name, ({"error": str(e)}, [])
                except Exception as e:
                    logger.error(f"â Œ Outer error processing {name}: {e}")
                    return name, ({"error": str(e)}, [])

            # â• â•  Process each instrument in PARALLEL â• â•
            tasks = []
            for name in self.active_indices:
                cfg = self.config.get("indices", {}).get(name)
                if cfg:
                    tasks.append(process_task(name, cfg))

            if not tasks:
                # logger.warning("âš ï¸  No active indices selected for scanning.")
                return

            scan_results = await asyncio.gather(*tasks)

            # â• â•  COORDINATE SIGNALS (Correlation Filter) â• â•
            candidates: List[TradeCandidate] = []
            for name, result_pair in scan_results:
                if isinstance(result_pair, tuple) and len(result_pair) == 2:
                    ui_data, candidate = result_pair
                    results["instruments"][name] = ui_data
                    if candidate:
                        if isinstance(candidate, list):
                            candidates.extend(candidate)
                        else:
                            candidates.append(candidate)
                else:
                    results["instruments"][name] = result_pair

            if candidates and self.mode != "HISTORICAL":
                fresh_candidates = []
                for candidate in candidates:
                    if self._is_stale_realtime_entry_candidate(candidate):
                        signal_ts = getattr(candidate, "signal_timestamp", None)
                        logger.warning(
                            f"Ignoring stale REAL-mode signal candidate after restart: "
                            f"{candidate.instrument} {candidate.timeframe} {candidate.direction} @ {signal_ts}"
                        )
                        continue
                    fresh_candidates.append(candidate)
                candidates = fresh_candidates

            if candidates or self._pending_live_signals:
                if self.mode != "HISTORICAL":
                    display_only_candidates = [
                        c for c in candidates
                        if getattr(c, "action", "ENTRY") == "NO_ENTRY" or not self.data.is_market_open()
                    ]
                    if display_only_candidates:
                        self._remember_trade_candidates(display_only_candidates)
                await self._coordinate_and_execute(candidates)
            self._refresh_session_signal_payloads(results)

            # Update active trades (PnL and Trailing Stops)
            self._update_active_trades()

            elapsed = time.time() - t0
            latency_ms = round(elapsed * 1000)

            # â• â• â•  Final Payload Construction â• â• â•
            perf_stats = self.perf.calculate(self.trades.closed_trades)
            summary_data = self.trades.get_summary()
            summary_data.update(perf_stats)

            # Build intelligence map
            intel_map = {}
            for name, ui_data in results.get("instruments", {}).items():
                if isinstance(ui_data, dict):
                    intel_map[name] = ui_data.get("intelligence", {})

                    # CRITICAL PERFORMANCE FIX: Strip heavy chart data except on resets/init
                    # Also strip for non-active tab to save bandwidth
                    if not is_full_chart_needed:
                        if "chart" in ui_data:
                            stripped_chart = {}
                            for tf, v in ui_data["chart"].items():
                                stripped_chart[tf] = {
                                    "state": v.get("state"),
                                    "candles": [v["candles"][-1]] if v.get("candles") else [],
                                    "trailing_stop": [v["trailing_stop"][-1]] if v.get("trailing_stop") else [],
                                    "markers": v.get("markers", [])
                                }
                            ui_data["chart"] = stripped_chart

            # â• â•  BROADCAST CONFIG â• â•
            config_data = self.get_broadcast_config()

            results.update({
                "system_power": self.system_power,
                "is_calculating": self.is_calculating,
                "mode": self.mode,
                "gateway_status": self.data.get_source_health(),
                "instruments": results.get("instruments", {}),
                "config": config_data,
                "intelligence": intel_map,
                "trades": self._build_dashboard_trade_payload(),
                "scan_count": self.scan_count,
                "latency": latency_ms,
                "timestamp": datetime.now(IST).isoformat(),
                "simulation_id": self.simulation_id
            })
            self.latest_results = results
            self.last_scan_time = datetime.now(IST)
            self._last_sent_sim_id = self.simulation_id

            if latency_ms > 10000:
                logger.warning(f"⚠️ High Latency: {latency_ms}ms")

            if latency_ms > 30000:
                self.log_event(f"⚠️ System Freeze/Hang Detected (Latency: {latency_ms}ms). Please click the UT1 Logo (Top-Left) to hard-refresh.", "error")

            # â• â•  DASHBOARD THROTTLE â• â•
            # Force broadcast if data updated OR at least every 1s
            now_time = time.time()
            should_broadcast = (
                is_full_chart_needed or
                len(candidates) > 0 or
                (now_time - getattr(self, "_last_broadcast_time", 0)) >= 1.0
            )

            if self.on_update and should_broadcast:
                self._last_broadcast_time = now_time
                try:
                    await self.on_update(results)
                except Exception as e:
                    logger.error(f"Failed to broadcast update: {e}")

            if self.scan_count % 10 == 0:
                logger.info(f"âœ… Scan Cycle {self.scan_count} Finished | Latency: {latency_ms}ms")
        finally:
            self._calculation_lock = False
            self.is_calculating = False

    # â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â•
    # PARALLEL DATA FETCH â€” major latency fix
    # â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â•
    async def _fetch_all_data(self, indices: Dict, force=False, timeframes: Optional[List[str]] = None):
        """Fetch data for all instruments, only when cache expired"""
        now = time.time()
        tasks = []
        selected_timeframes = timeframes or ["1min", "5min", "15min"]
        for name, cfg in indices.items():
            for tf in selected_timeframes:
                cache_key = f"{name}_{tf}"
                last = self._last_data_fetch.get(cache_key, 0)
                if force or now - last >= self._data_fetch_interval:
                    tasks.append(self._fetch_one(name, cfg, tf))
                    self._last_data_fetch[cache_key] = now

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_one(self, name, cfg, tf):
        """Fetch one instrument-timeframe from Yahoo (in thread pool)"""
        loop = asyncio.get_event_loop()
        # Use instance-specific settings (reflects UI changes)
        user_days = getattr(self, "backtest_days", 2)

        # Weekend/Holiday Buffer: Fetch 3x the requested days to ensure enough 'market days'
        # Yahoo Finance period counts calendar days, not trading days.
        # STABILITY FIX: Always fetch at least 15 days to ensure EMA/ATR convergence
        fetch_days = max(15, user_days * 3)

        # Scaling & Limits:
        # 1m: Max 7 days (Yahoo hard limit)
        # 5m/15m: Always fetch max available (approx 59 days) for absolute indicator stability
        if tf == "1min":
            final_fetch_days = min(7, fetch_days)
        else:
            # Convergence: 20 days is plenty for indicator stability while keeping it fast
            final_fetch_days = min(30, max(20, fetch_days))

        try:
            async with self._api_semaphore:
                # Increased stagger to 0.45s to safely stay under 3 calls/sec
                await asyncio.sleep(0.45)
                df = await loop.run_in_executor(
                    None,
                    lambda: self.data.get_historical_candles(
                        cfg["token"], cfg["exchange"], tf,
                        days_back=final_fetch_days, instrument_name=name,
                    ),
                )
            if df is not None and len(df) > 0:
                self.candles.update_candles(name, df, tf)
        except Exception as e:
            logger.error(f"Fetch failed for {name} {tf}: {e}")

    async def _sync_intelligence_history(self, indices: Dict):
        """
        Populate intelligence memory for the current session's history.
        Ensures 'Memory' is full from 9:15 AM to current time.
        """
        logger.info("ðŸ§  Syncing Intelligence History (9:15 AM to Now)...")
        today = datetime.now().date()
        loop = asyncio.get_event_loop()

        for name, cfg in indices.items():
            df_5m = self.candles.get_candles(name, "5min")
            if df_5m is None or df_5m.empty: continue

            # Filter for today's session
            today_data = df_5m[df_5m.index.date == today]
            if today_data.empty: continue

            # Process only the last 12 candles (1 hour) to build recent history and avoid O(N^2) startup lag
            tasks = []
            start_idx = max(0, len(today_data) - 12)
            for i in range(start_idx, len(today_data)):
                # We can't get historical options chain, but we can reconstruct Technical Intelligence
                sub_df = today_data.iloc[:i+1]
                ts = sub_df.index[-1].timestamp()

                # Check if already in memory
                if name in self.intel_memory.memory and ts in self.intel_memory.memory[name]["timestamps"]:
                    continue

                # Analyze Technical components only for history (Options chain is current-only)
                tasks.append(loop.run_in_executor(
                    None,
                    lambda s=sub_df, t=ts: (t, self.intel.analyze(
                        instrument=name, timeframe="5min", candle_df=s,
                        options_chain=None,
                        spot_price=s['close'].iloc[-1],
                        price_change_pct=0
                    ))
                ))

            if tasks:
                results = await asyncio.gather(*tasks)
                for ts, intel in results:
                    self.intel_memory.record(name, ts, intel)

        self._schedule_intel_memory_save()
        self.log_event(f"âœ… Intelligence & History Sync Complete for all indices", "success")
        logger.info("âœ… Intelligence Memory Synchronized")

    # â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â•
    # PROCESS INSTRUMENT â€” cache results, avoid double-compute
    # â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â•
    def _schedule_intel_memory_save(self) -> None:
        """Persist intelligence memory away from the scan hot path."""
        if getattr(self, "_intel_save_inflight", False):
            return
        self._intel_save_inflight = True

        def _save():
            try:
                self.intel_memory.save()
            finally:
                self._intel_save_inflight = False

        try:
            asyncio.get_event_loop().run_in_executor(None, _save)
        except RuntimeError:
            _save()

    def _assess_chain_quality(self, name: str, chain: Optional[pd.DataFrame], meta: Optional[Dict] = None) -> Dict:
        meta = dict(meta or {})
        strike_count = int(len(chain)) if chain is not None else 0
        fetched_at = float(meta.get("fetched_at", time.time()))
        age = max(0.0, time.time() - fetched_at)
        fallback = bool(meta.get("fallback", strike_count <= 1))
        if strike_count >= 10 and not fallback:
            score = 95
        elif strike_count >= 5 and not fallback:
            score = 75
        elif strike_count == 1:
            score = 35
        else:
            score = 0
        return {
            "instrument": name,
            "source": meta.get("source", "unknown" if strike_count else "none"),
            "strike_count": strike_count,
            "age_seconds": round(age, 1),
            "score": score,
            "fallback": fallback,
            "stale": False,
        }

    def _get_option_chain_cached(self, name: str, cfg: Dict, force: bool = False, allow_fetch: bool = True) -> Tuple[pd.DataFrame, Dict]:
        settings = get_settings()
        ttl = float(getattr(settings, "intelligence_cache_ttl_seconds", 30.0))
        now = time.time()
        cached = self._chain_cache.get(name)
        last = self._last_chain_fetch.get(name, 0.0)
        if not force and cached is not None and now - last <= ttl:
            quality = dict(self._chain_quality_cache.get(name, self._assess_chain_quality(name, cached)))
            quality["age_seconds"] = round(now - last, 1)
            quality["stale"] = False
            return cached, quality

        if not allow_fetch:
            if cached is not None:
                quality = dict(self._chain_quality_cache.get(name, self._assess_chain_quality(name, cached)))
                quality["age_seconds"] = round(now - last, 1)
                quality["stale"] = True
                return cached, quality
            empty = pd.DataFrame()
            quality = self._assess_chain_quality(
                name,
                empty,
                {"source": "deferred", "fallback": True, "fetched_at": now},
            )
            quality["stale"] = True
            return empty, quality

        try:
            chain = self.data.get_option_chain(name, cfg.get("option_exchange", "NFO"))
        except Exception as e:
            logger.warning(f"Option chain fetch failed for {name}: {e}")
            chain = cached if cached is not None else pd.DataFrame()
        finally:
            self._last_chain_fetch[name] = now

        if chain is None:
            chain = pd.DataFrame()
        meta = getattr(self.data, "_option_chain_meta", {}).get(name, {})
        quality = self._assess_chain_quality(name, chain, meta)
        if not chain.empty:
            self._chain_cache[name] = chain
            self._chain_quality_cache[name] = quality
        elif cached is not None:
            quality = dict(self._chain_quality_cache.get(name, self._assess_chain_quality(name, cached)))
            quality["age_seconds"] = round(now - last, 1)
            quality["stale"] = True
            chain = cached
        return chain, quality

    def _get_live_premium_cached(self, trade) -> Optional[float]:
        if not getattr(trade, "symbol_token", "") or not getattr(trade, "trading_symbol", ""):
            return None
        settings = get_settings()
        ttl = float(getattr(settings, "option_premium_cache_ttl_seconds", 2.0))
        key = trade.symbol_token
        now = time.time()
        cached = self._premium_cache.get(key)
        if cached and now - cached.get("time", 0.0) <= ttl:
            return cached.get("price")
        exchange = "BFO" if "SENSEX" in trade.trading_symbol.upper() else "NFO"
        try:
            price = self.data.get_ltp(exchange, trade.trading_symbol, trade.symbol_token)
            if price and price > 0:
                self._premium_cache[key] = {"price": price, "time": now}
                return price
        except Exception as e:
            logger.debug(f"Premium fetch failed for {trade.trading_symbol}: {e}")
        return cached.get("price") if cached else None

    def _live_analysis_window(self, df: Optional[pd.DataFrame], timeframe: str) -> Optional[pd.DataFrame]:
        """Keep REAL-mode scans focused on today's session plus indicator warmup bars."""
        if self.mode != "REAL" or df is None or df.empty:
            return df
        try:
            today = datetime.now(IST).date()
            idx_dates = pd.Series(df.index.date, index=df.index)
            today_df = df[idx_dates == today]
            warmup_bars = {"1min": 180, "5min": 60, "15min": 40}.get(timeframe, 60)
            fallback_bars = {"1min": 360, "5min": 120, "15min": 80}.get(timeframe, 120)
            if today_df.empty:
                return df.tail(fallback_bars)
            warmup_df = df[df.index < today_df.index[0]].tail(warmup_bars)
            window = pd.concat([warmup_df, today_df])
            return window[~window.index.duplicated(keep="last")].sort_index()
        except Exception as e:
            logger.debug(f"Live analysis window fallback for {timeframe}: {e}")
            return df

    def _process_instrument(self, name: str, cfg: Dict) -> Tuple[Dict, List[TradeCandidate]]:
        """Main Analysis Pipeline â€” focused on speed and real-time accuracy"""
        lot_size = cfg.get("lot_size", 25)
        lots = self.user_lots.get(name, 1)
        strike_interval = cfg.get("strike_interval", 50)
        combined_cap = self.capital_fut + self.capital_opt

        # 1. Use background-cached LTP to prevent API rate limits and speed up processing
        spot = self.candles.get_latest_price(name)
        if spot <= 0:
            logger.warning(f"âš ï¸  Spot price for {name} is {spot}. Skipping scan.")
            return {"error": f"Invalid spot price: {spot}"}, []

        # â• â•  Daily Change (Optimized: Use Cache) â• â•
        if not hasattr(self, "_prev_close_cache"):
            self._prev_close_cache = {}

        prev_close = self._prev_close_cache.get(name, 0)
        if prev_close <= 0:
            prev_close = self.data.get_previous_close(name)
            self._prev_close_cache[name] = prev_close

        change_points = (spot - prev_close) if prev_close > 0 else 0
        change_pct = ((change_points) / prev_close * 100) if prev_close > 0 else 0

        # 2. Update MTF and Analysis
        for tf in ["1min", "5min", "15min"]:
            data = self.candles.get_candles(name, tf)
            if data is not None:
                data = self._live_analysis_window(data, tf)
                self.mtf.update_data(name, tf, data)

        # Multi-TF analysis
        mtf_result = self.mtf.process_instrument(name, lot_size, lots, combined_cap, self.risk_fut_pct)

        # Cache per-TF process results for chart builder (avoid re-processing)
        for tf_key in ["1min", "5min", "15min"]:
            key = f"{name}_{tf_key}"
            tf_result = getattr(mtf_result, f"results_{tf_key}", None)
            if tf_result:
                self._cached_results[key] = tf_result
                logger.debug(f"Cached results for {key}")
            else:
                logger.debug(f"No results for {key}")

        results = {} # Will be populated with intelligence below

        # â• â•  MASTER SPOT RESOLUTION (Broker Priority Fix) â• â•
        candle_5min = self._live_analysis_window(self.candles.get_candles(name, "5min"), "5min")

        # â• â•  MASTER SPOT RESOLUTION (Broker Priority Fix) â• â•
        # Priority: Official Broker Spot -> Historical Fallback
        live_spot = self.candles.get_latest_price(name)
        hist_spot = candle_5min['close'].iloc[-1] if candle_5min is not None and not candle_5min.empty else 0

        # Use broker price if available, otherwise fallback to candle close
        spot = live_spot if (live_spot and live_spot > 0) else hist_spot

        # Debug Sync
        if self.scan_count % 10 == 0:
            logger.info(f"ðŸ”  Price Resolution for {name}: Broker={live_spot}, Hist={hist_spot} -> Final={spot}")

        # simulation_id is only set during actual re-simulations (see configure())

        # â• â•  INTELLIGENCE â• â•
        allow_chain_fetch = self.mode == "HISTORICAL" or self._chain_cache.get(name) is None
        chain, chain_quality = self._get_option_chain_cached(name, cfg, allow_fetch=allow_chain_fetch)
        candle_1min = self._live_analysis_window(self.candles.get_candles(name, "1min"), "1min")

        # â• â•  Volume Spike Detection â• â•
        import time
        now = time.time()
        if candle_1min is not None and len(candle_1min) > 12:
            last_volume = candle_1min['volume'].iloc[-1]
            avg_volume = candle_1min['volume'].iloc[:-1].tail(12).mean()

            if avg_volume > 0 and last_volume >= 2 * avg_volume:
                last_alert = self._last_vol_alert_time.get(name, 0)
                if now - last_alert >=  30.0:
                    last_open = float(candle_1min['open'].iloc[-1])
                    last_close = float(candle_1min['close'].iloc[-1])
                    pressure = "BUYING" if last_close > last_open else "SELLING"
                    msg = f"🔈 VOLUME SPIKE ({pressure} PRESSURE) detected for {name}: Current volume ({last_volume:.0f}) is 2x+ the average ({avg_volume:.0f})!"
                    logger.warning(msg)
                    if self.on_notification:
                        self.on_notification(msg, "warning")
                    self._last_vol_alert_time[name] = now

        # Use background-cached intel if fresh enough (avoids redundant heavy computation)
        intel_ttl = getattr(get_settings(), 'intelligence_cache_ttl_seconds', 90.0)
        cached_intel = self._intel_cache.get(name)
        cached_intel_time = getattr(self, '_intel_cache_time', {}).get(name, 0)
        intel_is_fresh = cached_intel is not None and (now - cached_intel_time) < intel_ttl

        if intel_is_fresh:
            intel_result = cached_intel
        else:
            try:
                intel_result = self.intel.analyze(
                    instrument=name,
                    timeframe="5min",
                    candle_df=candle_5min if candle_5min is not None else pd.DataFrame(),
                    candle_1min_df=candle_1min,
                    options_chain=chain,
                    spot_price=spot,
                    strike_interval=strike_interval,
                    days_to_expiry=7,
                    price_change_pct=change_pct,
                    chain_quality=chain_quality
                )
                self._intel_cache[name] = intel_result
                if not hasattr(self, '_intel_cache_time'):
                    self._intel_cache_time = {}
                self._intel_cache_time[name] = now

                # Log for verification
                pcr_val = intel_result.get('pcr', {}).get('pcr_oi', 0)
                call_delta = intel_result.get('greeks', {}).get('call', {}).get('delta', 0)
                logger.info(f"Intelligence for {name}: PCR={pcr_val}, Call Delta={call_delta}")
            except Exception as e:
                import traceback
                logger.error(f"Intelligence analysis failed for {name}: {e}\n{traceback.format_exc()}")
                intel_result = cached_intel or {
                    "pcr": {"pcr": 1.0, "signal": "NEUTRAL"},
                    "oi": {"signal": "NEUTRAL", "cumulative_analysis": {}},
                    "greeks": {},
                    "volume": {"buy_sell_ratio": 1.0},
                    "order_flow": {"ratio": 1.0},
                    "regime": {"regime": "UNKNOWN"}
                }

        results["intelligence"] = intel_result # Store in results dict

        # â• â• â•  Record Intelligence Memory â• â• â•
        scan_ts = time.time()
        self.intel_memory.record(name, scan_ts, intel_result)
        if self.scan_count % 300 == 0: self._schedule_intel_memory_save() # Periodically persist

        intel_score = intel_result.get("aggregate", {}).get("score", 0)
        regime = intel_result.get("regime", {}).get("regime", "UNKNOWN")
        self.latest_regimes[name] = regime # Store for adaptive trailing

        # â• â• â•  ATM Strike Resolution â• â• â•
        atm_strike = round(spot / strike_interval) * strike_interval if spot > 0 else 0

        # â• â• â•  BEST SIGNAL SELECTION across timeframes â• â• â•
        signal_parts = []
        for tf_name, tf_result in (("5min", mtf_result.results_5min), ("15min", mtf_result.results_15min)):
            sigs = tf_result.get("signals", []) if tf_result else []
            last_sig = sigs[-1] if sigs else None
            if last_sig:
                signal_parts.append(
                    f"{tf_name}:{last_sig.timestamp}:{last_sig.signal_type}:{round(float(last_sig.price or 0), 2)}"
                )
            else:
                signal_parts.append(f"{tf_name}:none")
        decision_key = "|".join(signal_parts + [str(self.inst_pref), str(getattr(get_settings(), "signal_grade_preference", ""))])
        decision_cache = self._candidate_process_cache.get(name)
        if (
            self.mode == "REAL"
            and decision_cache
            and decision_cache.get("key") == decision_key
            and now - decision_cache.get("time", 0.0) <= 15.0
        ):
            candidates = decision_cache.get("candidates", [])
        else:
            candidates = self.signal_processor.process_best_signal(
                name, mtf_result, intel_result, intel_score, regime,
                lots, lot_size, cfg, spot, atm_strike,
            )
            if self.mode == "REAL":
                self._candidate_process_cache[name] = {
                    "key": decision_key,
                    "time": now,
                    "candidates": candidates,
                }
        display_candidates = self._get_session_trade_candidates(name)
        if not display_candidates:
            display_candidates = (
                self._latest_exit_candidates.get(name, [])
                + self._latest_trade_candidates.get(name, [])
            )
        if not display_candidates and candidates:
            display_candidates = candidates
        self._refresh_trade_candidates(display_candidates, cfg)
        # Use first candidate for RR display if any
        primary_candidate = candidates[0] if candidates else None

        # â• â• â•  Build chart from CACHED results â€” NO re-processing â• â• â•
        chart_data = self._build_chart_from_cache(name)

        # Optimization: Strip heavy chart data for instruments NOT in active_indices
        # (Though active_indices filter already applied in _scan_cycle, this is a safety check)

        ui_data = {
            "mtf": {
                "confluence_score": mtf_result.confluence_score,
                "confluence_signal": mtf_result.confluence_signal,
                "state_1min": mtf_result.results_1min.get("state") if mtf_result.results_1min else None,
                "state_5min": mtf_result.results_5min.get("state") if mtf_result.results_5min else None,
                "state_15min": mtf_result.results_15min.get("state") if mtf_result.results_15min else None,
            },
            "intelligence": {
                "volume": intel_result.get("volume", {}),
                "oi": intel_result.get("oi", {}),
                "pcr": intel_result.get("pcr", {}),
                "greeks": intel_result.get("greeks", {}),
                "regime": intel_result.get("regime", {}),
                "order_flow": intel_result.get("order_flow", {}),
                "aggregate": intel_result.get("aggregate", {}),
                "potential_rr": primary_candidate.rr if primary_candidate else 1.5,
            },
            "chart": chart_data,
            "signals": self.signals.get_active_signals(mode=self.mode),
            "trade_candidates": [self._serialize_trade_candidate(c) for c in display_candidates],
            "simulation_id": self.simulation_id,
            "atm_strike": atm_strike,
            "ltp": spot,
            "change_points": change_points,
            "change_pct": change_pct,
            "spot_price": spot,
        }

        # â• â•  BROADCAST OPTIMIZATION â• â•
        # If this is the currently selected instrument in the dashboard, we MUST include chart data.
        # Otherwise, we strip it to save 90% of bandwidth.
        # Note: The main loop in _scan_cycle handles stripping based on is_full_chart_needed.

        return ui_data, candidates

    def _serialize_trade_candidate(self, candidate: TradeCandidate) -> Dict:
        """Serialize final post-filter trade candidates for the dashboard signal panel."""
        is_option = str(getattr(candidate, "inst_type", "") or "").upper() == "OPT"
        option_type = str(getattr(candidate, "option_type", "") or "").upper()
        return {
            "instrument": candidate.instrument,
            "direction": candidate.direction,
            "underlying_direction": candidate.direction,
            "trade_side": f"BUY {option_type}" if is_option and option_type else candidate.direction,
            "price": candidate.price,
            "current_price": candidate.current_price or candidate.price,
            "pnl": candidate.pnl,
            "stop": candidate.stop,
            "target": candidate.target,
            "lots": candidate.lots,
            "lot_size": candidate.lot_size,
            "grade": candidate.grade,
            "confidence": candidate.confidence,
            "timeframe": candidate.timeframe,
            "inst_type": candidate.inst_type,
            "option_type": candidate.option_type,
            "atm_strike": candidate.atm_strike,
            "trading_symbol": candidate.trading_symbol,
            "symbol_token": candidate.symbol_token,
            "rr": candidate.rr,
            "score": candidate.score,
            "reasons": candidate.reasons,
            "status": getattr(candidate, "status", "TRADE SIGNAL"),
            "action": getattr(candidate, "action", "ENTRY"),
            "exit_reason": getattr(candidate, "exit_reason", ""),
            "is_exit": getattr(candidate, "action", "ENTRY") == "EXIT",
            "exit_timestamp": (
                getattr(candidate, "exit_timestamp").isoformat()
                if getattr(candidate, "exit_timestamp", None)
                else ""
            ),
            "exit_price": (
                float(getattr(candidate, "exit_price", 0.0) or candidate.current_price or candidate.price)
                if getattr(candidate, "action", "ENTRY") == "EXIT"
                else 0.0
            ),
            "timestamp": (
                candidate.signal_timestamp.isoformat()
                if getattr(candidate, "signal_timestamp", None)
                else ""
            ),
        }

    def _is_premium_long_candidate(self, candidate: TradeCandidate) -> bool:
        return str(getattr(candidate, "inst_type", "") or "").upper() == "OPT"

    def _candidate_gross_pnl(self, candidate: TradeCandidate, exit_px: float) -> float:
        if getattr(candidate, "action", "ENTRY") == "NO_ENTRY":
            return 0.0
        qty = int(getattr(candidate, "lots", 1) or 1) * int(getattr(candidate, "lot_size", 1) or 1)
        if self._is_premium_long_candidate(candidate):
            return (exit_px - candidate.price) * qty
        if candidate.direction == "LONG":
            return (exit_px - candidate.price) * qty
        return (candidate.price - exit_px) * qty

    def _candidate_net_pnl(self, candidate: TradeCandidate, exit_px: float) -> float:
        if getattr(candidate, "action", "ENTRY") == "NO_ENTRY":
            return 0.0
        charges = 80.0 if self._is_premium_long_candidate(candidate) else 200.0
        return round(self._candidate_gross_pnl(candidate, exit_px) - charges, 2)

    def _candidate_stop_hit(self, candidate: TradeCandidate, current_price: float) -> bool:
        stop = float(getattr(candidate, "stop", 0.0) or 0.0)
        if stop <= 0:
            return False
        if self._is_premium_long_candidate(candidate) or candidate.direction == "LONG":
            return current_price <= stop
        return current_price >= stop

    def _candidate_target_hit(self, candidate: TradeCandidate, current_price: float) -> bool:
        target = float(getattr(candidate, "target", 0.0) or 0.0)
        if target <= 0:
            return False
        if self._is_premium_long_candidate(candidate) or candidate.direction == "LONG":
            return current_price >= target
        return current_price <= target

    def _candidate_exit_resolution(
        self,
        candidate: TradeCandidate,
        observed_price: float,
        default_reason: str,
        default_text: str,
    ) -> Tuple[float, str, str]:
        """Clamp manual/live exits to already-breached SL/TP before using a later signal price."""
        if self._candidate_stop_hit(candidate, observed_price):
            return float(candidate.stop), "STOP_HIT", "Stoploss hit"
        if self._candidate_target_hit(candidate, observed_price):
            return float(candidate.target), "TARGET_HIT", "Target hit"
        return observed_price, default_reason, default_text

    def _normalize_loaded_exit_candidate(self, candidate: TradeCandidate) -> bool:
        """Repair persisted live exits created by older builds before showing/accounting them."""
        if str(getattr(candidate, "action", "ENTRY") or "").upper() != "EXIT":
            return False
        observed_price = float(
            getattr(candidate, "exit_price", 0.0)
            or getattr(candidate, "current_price", 0.0)
            or getattr(candidate, "price", 0.0)
            or 0.0
        )
        if observed_price <= 0:
            return False

        original_reason = str(getattr(candidate, "exit_reason", "") or "EXIT_SIGNAL")
        exit_px, exit_reason, reason_text = self._candidate_exit_resolution(
            candidate,
            observed_price,
            original_reason,
            original_reason.replace("_", " ").title(),
        )
        if exit_reason not in {"STOP_HIT", "TARGET_HIT"}:
            return False

        changed = abs(float(exit_px) - observed_price) > 1e-9 or original_reason != exit_reason
        if not changed:
            return False

        candidate.current_price = float(exit_px)
        candidate.pnl = self._candidate_net_pnl(candidate, float(exit_px))
        candidate.exit_reason = exit_reason
        setattr(candidate, "exit_price", float(exit_px))
        reasons = list(getattr(candidate, "reasons", []) or [])
        repair_reason = f"{reason_text} reconciled from persisted exit {observed_price:.2f}"
        if repair_reason not in reasons:
            candidate.reasons = reasons + [repair_reason]
        return True

    def _candidate_history_key(self, candidate: TradeCandidate) -> str:
        """Stable per-session key so refreshed signal rows update in place.
        Buckets to the minute to prevent jitter between live signals and backfill."""
        ts = getattr(candidate, "signal_timestamp", None)
        if ts is None:
            ts = datetime.now(IST).replace(tzinfo=None)
            candidate.signal_timestamp = ts
        elif getattr(ts, "tzinfo", None) is not None:
            ts = ts.astimezone(IST).replace(tzinfo=None)
            candidate.signal_timestamp = ts

        ts_minute = ts.replace(second=0, microsecond=0)
        return "|".join([
            candidate.instrument,
            candidate.timeframe,
            getattr(candidate, "action", "ENTRY") or "ENTRY",
            candidate.direction,
            ts_minute.isoformat(),
        ])

    def _get_current_session_day(self) -> str:
        now_ist = datetime.now(IST)
        if now_ist.hour < 9 or (now_ist.hour == 9 and now_ist.minute < 15):
            return (now_ist - timedelta(days=1)).date().isoformat()
        else:
            return now_ist.date().isoformat()

    def _session_candidates_path(self) -> List[Path]:
        """Return list of session candidate file paths to load.
        In HISTORICAL mode with backtest_days > 1, returns multiple date files.
        In REAL mode or 1-day backtest, returns single day file."""
        if getattr(self, "mode", "") == "HISTORICAL":
            backtest_days = getattr(self, "backtest_days", 1)
            if backtest_days <= 1:
                # For 1-day backtest, load yesterday's session (most recent trading day)
                day = self._get_current_session_day()
                candidate_path = self._session_candidate_dir / f"{day}.json"
                if candidate_path.exists():
                    return [candidate_path]
                return [self._session_candidate_dir / "historical.json"]
            
            # For multi-day backtest, load the last N session files
            # Get all available session files sorted by date
            all_files = sorted(
                [f for f in self._session_candidate_dir.glob("*.json") if f.name != "historical.json"],
                key=lambda x: x.name
            )
            # Take the last backtest_days files
            return all_files[-backtest_days:] if len(all_files) >= backtest_days else all_files
        
        # REAL mode: load today's session file
        day = getattr(self, "_session_candidate_day", None) or self._get_current_session_day()
        return [self._session_candidate_dir / f"{day}.json"]

    def _candidate_from_payload(self, payload: Dict[str, Any]) -> Optional[TradeCandidate]:
        try:
            raw_ts = payload.get("timestamp") or payload.get("signal_timestamp")
            signal_ts = None
            if raw_ts:
                signal_ts = datetime.fromisoformat(str(raw_ts))
                if signal_ts.tzinfo is not None:
                    signal_ts = signal_ts.astimezone(IST).replace(tzinfo=None)
            candidate = TradeCandidate(
                instrument=str(payload.get("instrument", "")),
                direction=str(payload.get("direction", "")),
                price=float(payload.get("price") or 0.0),
                stop=float(payload.get("stop") or 0.0),
                target=float(payload.get("target") or 0.0),
                lots=int(payload.get("lots") or 1),
                lot_size=int(payload.get("lot_size") or 1),
                grade=str(payload.get("grade") or "B+"),
                confidence=float(payload.get("confidence") or 0.0),
                timeframe=str(payload.get("timeframe") or "5min"),
                inst_type=str(payload.get("inst_type") or ""),
                option_type=str(payload.get("option_type") or ""),
                atm_strike=float(payload.get("atm_strike") or 0.0),
                multiplier=float(payload.get("multiplier") or payload.get("lot_size") or 1.0),
                trading_symbol=str(payload.get("trading_symbol") or ""),
                symbol_token=str(payload.get("symbol_token") or ""),
                rr=float(payload.get("rr") or 1.5),
                score=float(payload.get("score") or 0.0),
                reasons=list(payload.get("reasons") or []),
                spot_stop=float(payload.get("spot_stop") or 0.0),
                spot_target=float(payload.get("spot_target") or 0.0),
                signal_timestamp=signal_ts,
                current_price=float(payload.get("current_price") or payload.get("price") or 0.0),
                pnl=float(payload.get("pnl") or 0.0),
                status=str(payload.get("status") or "TRADE SIGNAL"),
                action=str(payload.get("action") or "ENTRY"),
                exit_reason=str(payload.get("exit_reason") or ""),
            )
            raw_exit_ts = payload.get("exit_timestamp")
            exit_ts = None
            if raw_exit_ts:
                exit_ts = datetime.fromisoformat(str(raw_exit_ts))
                if exit_ts.tzinfo is not None:
                    exit_ts = exit_ts.astimezone(IST).replace(tzinfo=None)
            if exit_ts:
                setattr(candidate, "exit_timestamp", exit_ts)
            if str(getattr(candidate, "action", "ENTRY") or "").upper() == "EXIT":
                exit_price = float(payload.get("exit_price") or candidate.current_price or candidate.price or 0.0)
                if exit_price > 0:
                    candidate.current_price = exit_price
                    setattr(candidate, "exit_price", exit_price)
            return candidate
        except Exception as exc:
            logger.debug(f"Skipping invalid session candidate payload: {exc}")
            return None

    def _active_session_entry_candidates(self, instrument: str, timeframe: Optional[str] = None, as_of: Optional[datetime] = None) -> List[TradeCandidate]:
        """Return remembered entry rows that do not already have a later exit row."""
        rows = self._get_session_trade_candidates(instrument)
        exit_cutoffs: Dict[Tuple[str, str], datetime] = {}
        for row in rows:
            if getattr(row, "action", "ENTRY") != "EXIT":
                continue
            key = (row.timeframe, row.direction)
            exit_ts = getattr(row, "exit_timestamp", None) or getattr(row, "signal_timestamp", None)
            if not exit_ts:
                continue
            if as_of and exit_ts > as_of:
                continue
            if key not in exit_cutoffs or exit_ts > exit_cutoffs[key]:
                exit_cutoffs[key] = exit_ts

        active = []
        for row in rows:
            if getattr(row, "action", "ENTRY") in ("EXIT", "NO_ENTRY"):
                continue
            if timeframe and row.timeframe != timeframe:
                continue
            entry_ts = getattr(row, "signal_timestamp", None)
            if as_of and entry_ts and entry_ts > as_of:
                continue
            exit_ts = exit_cutoffs.get((row.timeframe, row.direction))
            if exit_ts and entry_ts and exit_ts >= entry_ts:
                continue
            active.append(row)
        return active

    def _load_session_trade_candidates(self) -> None:
        """Restore session signal ledger from one or more files.
        In HISTORICAL mode with backtest_days > 1, loads and merges multiple date files."""
        # Guard against repeated loading in HISTORICAL mode
        if getattr(self, "mode", "") == "HISTORICAL" and getattr(self, "_hist_candidates_loaded", False):
            return
        
        self._session_trade_candidates = {}
        paths = self._session_candidates_path()
        
        # Handle both single path (legacy) and list of paths (multi-day)
        if isinstance(paths, Path):
            paths = [paths]
        
        total_count = 0
        for path in paths:
            if not path.exists():
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                # In HISTORICAL mode, load whatever file without date validation
                if getattr(self, "mode", "") != "HISTORICAL":
                    if payload.get("date") != getattr(self, "_session_candidate_day", ""):
                        continue
                count = 0
                for item in payload.get("candidates", []):
                    candidate = self._candidate_from_payload(item)
                    if not candidate or not candidate.instrument:
                        continue
                    self._normalize_loaded_exit_candidate(candidate)
                    book = self._session_trade_candidates.setdefault(candidate.instrument, {})
                    book[self._candidate_history_key(candidate)] = candidate
                    count += 1
                if count:
                    logger.info(f"Restored {count} session signal row(s) from {path.name}")
                    total_count += count
            except Exception as exc:
                logger.warning(f"Could not restore session signal ledger {path}: {exc}")
        
        if total_count:
            logger.info(f"Total session candidates loaded: {total_count} from {len(paths)} file(s)")
        
        # Mark as loaded in HISTORICAL mode
        if getattr(self, "mode", "") == "HISTORICAL":
            self._hist_candidates_loaded = True

    def _persist_session_trade_candidates(self) -> None:
        """Persist today's dashboard signal ledger without touching broker state.
        In HISTORICAL mode, does nothing (candidates are loaded from historical files)."""
        # In HISTORICAL mode, don't persist - candidates are loaded from historical files
        if getattr(self, "mode", "") == "HISTORICAL":
            return
        
        try:
            self._session_candidate_dir.mkdir(parents=True, exist_ok=True)
            rows = []
            for book in getattr(self, "_session_trade_candidates", {}).values():
                for candidate in book.values():
                    self._normalize_loaded_exit_candidate(candidate)
                    rows.append(self._serialize_trade_candidate(candidate))
            
            # In REAL mode, _session_candidates_path() returns a list with today's file
            paths = self._session_candidates_path()
            target_path = paths[0] if isinstance(paths, list) else paths
            
            if target_path.exists():
                try:
                    existing_payload = json.loads(target_path.read_text(encoding="utf-8"))
                    if existing_payload.get("date") == getattr(self, "_session_candidate_day", self._get_current_session_day()):
                        merged = {}
                        for row in existing_payload.get("candidates", []) or []:
                            key = (
                                row.get("instrument"),
                                row.get("timeframe"),
                                row.get("direction"),
                                row.get("timestamp"),
                                row.get("action"),
                            )
                            merged[key] = row
                        for row in rows:
                            key = (
                                row.get("instrument"),
                                row.get("timeframe"),
                                row.get("direction"),
                                row.get("timestamp"),
                                row.get("action"),
                            )
                            merged[key] = row
                        if len(merged) > len(rows):
                            logger.warning(
                                f"Session ledger merge protected {target_path}: "
                                f"in-memory rows={len(rows)}, merged rows={len(merged)}"
                            )
                            rows = list(merged.values())
                except Exception as exc:
                    logger.debug(f"Could not merge existing session ledger before persist: {exc}")
            rows.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
            tmp_path = target_path.with_suffix(".json.tmp")
            tmp_path.write_text(
                json.dumps(
                    {
                        "date": getattr(self, "_session_candidate_day", self._get_current_session_day()),
                        "updated_at": datetime.now(IST).isoformat(),
                        "candidates": rows[:500],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            tmp_path.replace(target_path)
            
            # Cleanup: Keep only last 30 days of session candidate files
            self._cleanup_old_session_candidates()
        except Exception as exc:
            logger.debug(f"Failed to persist session trade candidates: {exc}")

    def _cleanup_old_session_candidates(self) -> None:
        """Remove session candidate files older than 30 days to manage disk space."""
        try:
            if not self._session_candidate_dir.exists():
                return
            
            cutoff_date = datetime.now(IST) - timedelta(days=30)
            cutoff_str = cutoff_date.strftime("%Y-%m-%d")
            
            for file_path in self._session_candidate_dir.glob("*.json"):
                # Extract date from filename (format: YYYY-MM-DD.json)
                file_date_str = file_path.stem
                if file_date_str < cutoff_str:
                    try:
                        file_path.unlink()
                        logger.info(f"🗑️ Cleaned up old session candidate file: {file_path.name}")
                    except Exception as exc:
                        logger.debug(f"Failed to delete old session file {file_path.name}: {exc}")
        except Exception as exc:
            logger.debug(f"Failed to cleanup old session candidates: {exc}")

    def _ensure_session_candidate_day(self) -> None:
        if getattr(self, "mode", "") == "HISTORICAL":
            # In HISTORICAL mode, load session candidates only once to avoid repeated I/O
            if not getattr(self, "_hist_candidates_loaded", False):
                self._load_session_trade_candidates()
                self._hist_candidates_loaded = True
            return
        today = self._get_current_session_day()
        if today == getattr(self, "_session_candidate_day", today):
            return
        logger.info(f"Session candidate day rolling over from {self._session_candidate_day} to {today}")
        self._session_candidate_day = today
        self._session_trade_candidates = {}
        self._persist_session_trade_candidates()

    def _remember_trade_candidates(self, candidates: List[TradeCandidate]) -> None:
        """Keep session signal rows visible after later signals arrive."""
        if not candidates:
            return
        # Filter out NO_ENTRY candidates from persistence
        candidates = [c for c in candidates if getattr(c, "action", "ENTRY") != "NO_ENTRY"]
        if not candidates:
            return
        self._ensure_session_candidate_day()
        store = getattr(self, "_session_trade_candidates", None)
        if store is None:
            self._session_trade_candidates = {}
            store = self._session_trade_candidates

        for candidate in candidates:
            book = store.setdefault(candidate.instrument, {})
            book[self._candidate_history_key(candidate)] = candidate
            if len(book) > 150:
                ordered_keys = sorted(
                    book,
                    key=lambda k: getattr(book[k], "signal_timestamp", None) or datetime.min,
                    reverse=True,
                )
                for old_key in ordered_keys[150:]:
                    book.pop(old_key, None)
        self._persist_session_trade_candidates()

    def _remember_filtered_candidate(self, candidate: TradeCandidate) -> None:
        """Keep recent historical/backtest rejected rows visible in the dashboard."""
        if not candidate or not getattr(candidate, "instrument", ""):
            return
        store = getattr(self, "_latest_filtered_candidates", None)
        if store is None:
            self._latest_filtered_candidates = {}
            store = self._latest_filtered_candidates
        rows = list(store.get(candidate.instrument, []))
        key = self._candidate_history_key(candidate)
        rows = [row for row in rows if self._candidate_history_key(row) != key]
        rows.insert(0, candidate)
        store[candidate.instrument] = rows[:50]

    def _record_historical_reject(
        self,
        signal,
        instrument: str,
        timeframe: str,
        direction: str,
        grade: str,
        confidence: float,
        reason: str,
        inst_type: str,
        lot_size: int,
        atm_strike: float,
        option_type: str = "",
        target: float = 0.0,
    ) -> None:
        """Publish filtered backtest candidates so the signal panel does not look idle."""
        try:
            candidate = TradeCandidate(
                instrument=instrument,
                direction=direction,
                price=float(getattr(signal, "price", 0.0) or 0.0),
                stop=float(getattr(signal, "trailing_stop", 0.0) or 0.0),
                target=float(target or 0.0),
                lots=0,
                lot_size=int(lot_size or 1),
                grade=str(grade or "C"),
                confidence=float(confidence or 0.0),
                timeframe=timeframe,
                inst_type=str(inst_type or "FUT"),
                option_type=str(option_type or ""),
                atm_strike=float(atm_strike or 0.0),
                multiplier=1.0,
                trading_symbol="",
                symbol_token="",
                rr=0.0,
                score=float(confidence or 0.0),
                reasons=[reason],
                spot_stop=float(getattr(signal, "trailing_stop", 0.0) or 0.0),
                spot_target=float(target or 0.0),
                signal_timestamp=getattr(signal, "timestamp", None),
                current_price=float(getattr(signal, "price", 0.0) or 0.0),
                pnl=0.0,
                status=f"NO ENTRY - {reason}",
                action="NO_ENTRY",
            )
            self._remember_filtered_candidate(candidate)
        except Exception as exc:
            logger.debug(f"Failed to record filtered historical row: {exc}")

    def _is_stale_realtime_entry_candidate(self, candidate: TradeCandidate) -> bool:
        """Block REAL-mode startup/backfill signals from becoming fresh entries."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return False
        if str(getattr(candidate, "action", "ENTRY") or "ENTRY").upper() != "ENTRY":
            return False
        signal_ts = getattr(candidate, "signal_timestamp", None)
        started_at = getattr(self, "_process_started_at", None)
        if not signal_ts or not started_at:
            return False
        if getattr(signal_ts, "tzinfo", None) is not None:
            signal_ts = signal_ts.astimezone(IST).replace(tzinfo=None)
        if getattr(started_at, "tzinfo", None) is not None:
            started_at = started_at.astimezone(IST).replace(tzinfo=None)
        # Allow a small grace window for a signal formed on the just-closed candle,
        # but never let an old candle re-enter after a restart.
        return signal_ts < (started_at - timedelta(minutes=2))

    def _session_squareoff_clock(self) -> datetime:
        """Use the strongest IST clock available for session-end exits."""
        wall_now = datetime.now(IST)
        try:
            market_ts = self.candles.get_max_timestamp()
            if market_ts is not None:
                if getattr(market_ts, "tzinfo", None) is None:
                    market_now = IST.localize(market_ts)
                else:
                    market_now = market_ts.astimezone(IST)
                if market_now > wall_now:
                    return market_now
        except Exception as exc:
            logger.debug(f"Session squareoff clock fallback to wall time: {exc}")
        return wall_now

    def _force_exit_session_candidates_at_eod(self, now: Optional[datetime] = None) -> int:
        """Close manual/live signal-ledger rows at the hard 15:18 IST square-off."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return 0

        now = now or self._session_squareoff_clock()
        if now.tzinfo is None:
            now = IST.localize(now)
        else:
            now = now.astimezone(IST)

        trigger_time = dtime(15, 18)
        exit_time = dtime(15, 20)
        if now.time() < trigger_time:
            return 0

        exit_ts = IST.localize(datetime.combine(now.date(), exit_time))
        exits: List[TradeCandidate] = []
        for instrument in list((getattr(self, "_session_trade_candidates", {}) or {}).keys()):
            for previous in self._active_session_entry_candidates(instrument):
                prev_ts = getattr(previous, "signal_timestamp", None)
                if not prev_ts:
                    continue
                prev_ist = IST.localize(prev_ts) if prev_ts.tzinfo is None else prev_ts.astimezone(IST)
                if prev_ist.date() != now.date() or prev_ist >= exit_ts:
                    continue

                dedupe_key = f"EOD_{instrument}_{previous.timeframe}_{previous.direction}_{prev_ist.isoformat()}"
                if self._last_opposite_exit_signal_time.get(dedupe_key) == exit_ts:
                    continue

                # check if an exit candidate already exists in today's ledger!
                store = getattr(self, "_session_trade_candidates", {}) or {}
                book = store.get(instrument, {})
                hist_key = self._candidate_history_key(previous)
                if hist_key in book and getattr(book[hist_key], "action", "") == "EXIT":
                    continue

                proposed_exit_px = previous.current_price or previous.price
                exit_px, exit_reason, reason_text = self._candidate_exit_resolution(
                    previous,
                    proposed_exit_px,
                    "FORCE_EOD_KILL",
                    "Hard square-off at 15:18 IST",
                )
                calc_pnl = self._candidate_net_pnl(previous, exit_px)

                exit_candidate = TradeCandidate(
                    instrument=previous.instrument,
                    direction=previous.direction,
                    price=previous.price,
                    stop=previous.stop,
                    target=0.0,
                    lots=previous.lots,
                    lot_size=previous.lot_size,
                    grade=previous.grade,
                    confidence=previous.confidence,
                    timeframe=previous.timeframe,
                    inst_type=previous.inst_type,
                    option_type=previous.option_type,
                    atm_strike=previous.atm_strike,
                    multiplier=previous.multiplier,
                    trading_symbol=previous.trading_symbol,
                    symbol_token=previous.symbol_token,
                    rr=previous.rr,
                    score=previous.score,
                    reasons=list(previous.reasons or []) + [f"{reason_text} at {exit_px:.2f}"],
                    spot_stop=previous.spot_stop,
                    spot_target=previous.spot_target,
                    signal_timestamp=prev_ts,
                    current_price=exit_px,
                    pnl=calc_pnl,
                    status="EXIT SIGNAL",
                    action="EXIT",
                    exit_reason=exit_reason,
                )
                setattr(exit_candidate, "exit_timestamp", exit_ts.replace(tzinfo=None))
                setattr(exit_candidate, "exit_price", float(exit_px))
                exits.append(exit_candidate)
                self._last_opposite_exit_signal_time[dedupe_key] = exit_ts

        if not exits:
            return 0

        for exit_candidate in exits:
            self._refresh_trade_candidates(
                [exit_candidate],
                self.config.get("indices", {}).get(exit_candidate.instrument, {}),
            )
            self._latest_exit_candidates[exit_candidate.instrument] = [exit_candidate]
        self._remember_trade_candidates(exits)
        self.log_event(f"Force EOD exit applied to {len(exits)} manual signal row(s) at 15:18 IST", "warning")
        return len(exits)

    def _close_superseded_session_entries(self, candidates: List[TradeCandidate]) -> None:
        """In manual mode, a fresh index signal closes older active manual rows for clean P&L."""
        if not candidates or getattr(self, "mode", "") == "HISTORICAL":
            return

        exits: List[TradeCandidate] = []
        for candidate in candidates:
            if getattr(candidate, "action", "ENTRY") == "EXIT":
                continue
            signal_ts = getattr(candidate, "signal_timestamp", None)
            if not signal_ts:
                continue
            for previous in self._active_session_entry_candidates(candidate.instrument):
                prev_ts = getattr(previous, "signal_timestamp", None)
                if not prev_ts or signal_ts <= prev_ts:
                    continue
                if self._candidate_history_key(previous) == self._candidate_history_key(candidate):
                    continue

                dedupe_key = f"SUPERSEDE_{candidate.instrument}_{previous.timeframe}_{previous.direction}_{signal_ts.isoformat()}"
                if self._last_opposite_exit_signal_time.get(dedupe_key) == signal_ts:
                    continue

                # check if an exit candidate already exists in today's ledger!
                store = getattr(self, "_session_trade_candidates", {}) or {}
                book = store.get(candidate.instrument, {})
                hist_key = self._candidate_history_key(previous)
                if hist_key in book and getattr(book[hist_key], "action", "") == "EXIT":
                    continue

                # Calculate immediate exit price and P&L
                if previous.inst_type == "OPT":
                    proposed_exit_px = self._get_live_premium_cached(previous) or previous.current_price or previous.price
                else:
                    proposed_exit_px = candidate.price
                exit_px, exit_reason, reason_text = self._candidate_exit_resolution(
                    previous,
                    float(proposed_exit_px),
                    "SUPERSEDED_SIGNAL",
                    f"Superseded by {candidate.direction} {candidate.timeframe} signal",
                )
                calc_pnl = self._candidate_net_pnl(previous, exit_px)

                exit_candidate = TradeCandidate(
                    instrument=previous.instrument,
                    direction=previous.direction,
                    price=previous.price,
                    stop=previous.stop,
                    target=0.0,
                    lots=previous.lots,
                    lot_size=previous.lot_size,
                    grade=previous.grade,
                    confidence=previous.confidence,
                    timeframe=previous.timeframe,
                    inst_type=previous.inst_type,
                    option_type=previous.option_type,
                    atm_strike=previous.atm_strike,
                    multiplier=previous.multiplier,
                    trading_symbol=previous.trading_symbol,
                    symbol_token=previous.symbol_token,
                    rr=previous.rr,
                    score=previous.score,
                    reasons=list(previous.reasons or []) + [f"{reason_text} at {signal_ts:%H:%M}"],
                    spot_stop=previous.spot_stop,
                    spot_target=previous.spot_target,
                    signal_timestamp=prev_ts,
                    current_price=exit_px,
                    pnl=calc_pnl,
                    status="EXIT SIGNAL",
                    action="EXIT",
                    exit_reason=exit_reason,
                )
                setattr(exit_candidate, "exit_timestamp", signal_ts)
                setattr(exit_candidate, "exit_price", float(exit_px))
                exits.append(exit_candidate)
                self._last_opposite_exit_signal_time[dedupe_key] = signal_ts

        if exits:
            for exit_candidate in exits:
                self._refresh_trade_candidates([exit_candidate], self.config.get("indices", {}).get(exit_candidate.instrument, {}))
                self._latest_exit_candidates[exit_candidate.instrument] = [exit_candidate]
            self._remember_trade_candidates(exits)

    def _get_session_trade_candidates(self, instrument: str) -> List[TradeCandidate]:
        self._ensure_session_candidate_day()
        store = getattr(self, "_session_trade_candidates", {})
        book = store.get(instrument, {})
        return sorted(
            book.values(),
            key=lambda c: getattr(c, "signal_timestamp", None) or datetime.min,
            reverse=True,
        )

    def _build_dashboard_trade_payload(self) -> Dict[str, Any]:
        """Return trade-manager rows plus the persisted manual/live signal ledger."""
        payload = self.trades.get_dashboard_payload(
            is_historical=(self.mode == "HISTORICAL"),
            backtest_days=self.backtest_days,
            inst_pref=getattr(self, "inst_pref", "AUTO"),
        )
        
        def inst_pref_match(c) -> bool:
            pref = (getattr(self, "inst_pref", "AUTO") or "AUTO").upper()
            if pref in ("AUTO", "HYBRID"): return True
            inst_type = getattr(c, "inst_type", "FUT") if not isinstance(c, dict) else c.get("inst_type", "FUT")
            if pref == "FUT" and inst_type == "FUT": return True
            if pref == "OPT" and inst_type in ("OPT", "CE", "PE"): return True
            return False

        if self.mode != "HISTORICAL":
            signal_rows = []
            instruments = list(getattr(self, "active_indices", []) or [])
            for instrument in (getattr(self, "_session_trade_candidates", {}) or {}).keys():
                if instrument not in instruments:
                    instruments.append(instrument)
            for instrument in instruments:
                for candidate in self._get_session_trade_candidates(instrument):
                    if inst_pref_match(candidate):
                        self._normalize_loaded_exit_candidate(candidate)
                        signal_rows.append(self._serialize_trade_candidate(candidate))
            payload["signals"] = signal_rows
            has_trade_rows = bool(payload.get("open") or payload.get("closed"))
            if signal_rows and not has_trade_rows:
                exit_rows = [r for r in signal_rows if r.get("action") == "EXIT"]
                exit_keys = {
                    (
                        r.get("instrument"),
                        r.get("timeframe"),
                        r.get("direction"),
                        r.get("timestamp"),
                    )
                    for r in exit_rows
                }
                active_rows = [
                    r for r in signal_rows
                    if r.get("action") == "ENTRY"
                    and (
                        r.get("instrument"),
                        r.get("timeframe"),
                        r.get("direction"),
                        r.get("timestamp"),
                    ) not in exit_keys
                ]
                accounting_rows = exit_rows + active_rows
                pnls = [float(r.get("pnl") or 0.0) for r in accounting_rows]
                wins = len([p for p in pnls if p > 0])
                losses = len([p for p in pnls if p < 0])
                gross_win = sum(p for p in pnls if p > 0)
                gross_loss = abs(sum(p for p in pnls if p < 0))
                payload["summary"] = {
                    **(payload.get("summary") or {}),
                    "daily_pnl": round(sum(pnls), 2),
                    "fut_pnl": round(sum(float(r.get("pnl") or 0.0) for r in accounting_rows if r.get("inst_type") == "FUT"), 2),
                    "opt_pnl": round(sum(float(r.get("pnl") or 0.0) for r in accounting_rows if r.get("inst_type") == "OPT"), 2),
                    "total_trades": len(accounting_rows),
                    "open_count": len(active_rows),
                    "wins": wins,
                    "losses": losses,
                    "win_rate": round((wins / len(accounting_rows)) * 100, 1) if accounting_rows else 0.0,
                    "profit_factor": round(gross_win / gross_loss, 2) if gross_loss > 0 else (99.99 if gross_win > 0 else 1.0),
                }
        else:
            session_rows = []
            for instrument in (getattr(self, "_session_trade_candidates", {}) or {}).keys():
                for candidate in self._get_session_trade_candidates(instrument):
                    if inst_pref_match(candidate):
                        session_rows.append(self._serialize_trade_candidate(candidate))

            filtered_rows = []
            for rows in (getattr(self, "_latest_filtered_candidates", {}) or {}).values():
                for candidate in rows:
                    if inst_pref_match(candidate):
                        filtered_rows.append(self._serialize_trade_candidate(candidate))
            
            if session_rows:
                # In HISTORICAL mode, merge ENTRY and EXIT rows to show complete trades
                # Only show ENTRY rows with exit info merged from matching EXIT rows
                entry_rows = [r for r in session_rows if r.get("action") == "ENTRY"]
                exit_rows = [r for r in session_rows if r.get("action") == "EXIT"]
                
                # Build exit lookup by (instrument, timeframe, direction, timestamp)
                exit_lookup = {
                    (
                        r.get("instrument"),
                        r.get("timeframe"),
                        r.get("direction"),
                        r.get("timestamp"),
                    ): r
                    for r in exit_rows
                }
                
                # Active rows are entry rows that do not have a corresponding exit
                active_keys = set(exit_lookup.keys())
                active_rows = [
                    entry for entry in entry_rows
                    if (
                        entry.get("instrument"),
                        entry.get("timeframe"),
                        entry.get("direction"),
                        entry.get("timestamp"),
                    ) not in active_keys
                ]
                
                # Merge exit info into entry rows
                merged_rows = []
                for entry in entry_rows:
                    key = (
                        entry.get("instrument"),
                        entry.get("timeframe"),
                        entry.get("direction"),
                        entry.get("timestamp"),
                    )
                    exit_row = exit_lookup.get(key)
                    if exit_row:
                        # Merge exit information
                        entry["exit_price"] = exit_row.get("exit_price", 0.0)
                        entry["exit_timestamp"] = exit_row.get("exit_timestamp", "")
                        entry["exit_reason"] = exit_row.get("exit_reason", "")
                        entry["pnl"] = exit_row.get("pnl", 0.0)
                        entry["current_price"] = exit_row.get("exit_price", entry.get("current_price"))
                        entry["status"] = "EXIT SIGNAL"
                        entry["action"] = "EXIT"
                        entry["is_exit"] = True
                    merged_rows.append(entry)
                
                # --- CRITICAL BUG FIX FOR MULTI-DAY BACKTESTS ---
                # Also convert and append simulated trades from closed/open payload lists
                # whose dates are not already covered by the loaded session_rows.
                # This ensures multi-day backtests show ALL signals/trades, not just last day's!
                session_dates = set()
                for r in merged_rows:
                    ts_str = r.get("timestamp") or r.get("signal_timestamp")
                    if ts_str:
                        try:
                            session_dates.add(datetime.fromisoformat(str(ts_str)).date())
                        except Exception:
                            pass

                closed_trades = payload.get("closed", [])
                open_trades = payload.get("open", [])
                
                simulated_signals = []
                for row in open_trades + closed_trades:
                    # Filter for simulated trades (starting with H_ or EOD_)
                    is_sim = str(row.get("id", "")).startswith("H_") or str(row.get("id", "")).startswith("EOD_")
                    if not is_sim:
                        continue
                    
                    entry_ts_sec = row.get("entry_timestamp", 0)
                    entry_date = None
                    entry_iso = ""
                    if entry_ts_sec:
                        try:
                            entry_dt = datetime.fromtimestamp(entry_ts_sec, tz=IST)
                            entry_date = entry_dt.date()
                            entry_iso = entry_dt.isoformat()
                        except Exception:
                            pass
                    
                    # If this simulated trade's date is already covered by a session file, skip it
                    if entry_date and entry_date in session_dates:
                        continue
                        
                    exit_ts_sec = row.get("exit_timestamp", 0)
                    exit_iso = ""
                    if exit_ts_sec:
                        try:
                            exit_iso = datetime.fromtimestamp(exit_ts_sec, tz=IST).isoformat()
                        except Exception:
                            pass

                    is_closed = row.get("status") == "CLOSED" or row.get("exit_price", 0.0) > 0
                    
                    simulated_signals.append({
                        "instrument": row.get("instrument", ""),
                        "direction": row.get("direction", ""),
                        "price": row.get("entry_price", 0.0),
                        "current_price": row.get("exit_price", 0.0) or row.get("current_price", 0.0),
                        "pnl": row.get("pnl", 0.0),
                        "stop": row.get("trailing_stop", 0.0),
                        "target": row.get("target", 0.0),
                        "lots": row.get("lots", 1),
                        "lot_size": row.get("lot_size", 1),
                        "grade": row.get("grade", ""),
                        "confidence": row.get("confidence", 0.0),
                        "timeframe": row.get("timeframe", ""),
                        "inst_type": row.get("inst_type", "FUT"),
                        "option_type": row.get("option_type", ""),
                        "atm_strike": row.get("atm_strike", 0.0),
                        "timestamp": entry_iso or row.get("entry_time", ""),
                        "entry_date": row.get("entry_date", ""),
                        "exit_price": row.get("exit_price", 0.0),
                        "status": "EXIT SIGNAL" if is_closed else "TRADE SIGNAL",
                        "action": "EXIT" if is_closed else "ENTRY",
                        "exit_reason": row.get("exit_reason", ""),
                        "exit_timestamp": exit_iso or row.get("exit_time", ""),
                        "rr": row.get("rr_ratio", 1.5),
                        "trading_symbol": "",
                        "symbol_token": "",
                        "exec_type": row.get("exec_type", "A"),
                    })
                
                all_signals = merged_rows + simulated_signals
                all_signals = sorted(all_signals, key=lambda x: x.get("timestamp") or "", reverse=True)
                payload["signals"] = all_signals
                accounting_rows = all_signals
                
                # Recalculate summary metrics for the combined list of signals
                pnls = [float(r.get("pnl") or 0.0) for r in accounting_rows]
                wins = len([p for p in pnls if p > 0])
                losses = len([p for p in pnls if p < 0])
                gross_win = sum(p for p in pnls if p > 0)
                gross_loss = abs(sum(p for p in pnls if p < 0))
                
                open_count = len([r for r in accounting_rows if r.get("action") == "ENTRY"])
                
                payload["summary"] = {
                    **(payload.get("summary") or {}),
                    "daily_pnl": round(sum(pnls), 2),
                    "fut_pnl": round(sum(float(r.get("pnl") or 0.0) for r in accounting_rows if r.get("inst_type") == "FUT"), 2),
                    "opt_pnl": round(sum(float(r.get("pnl") or 0.0) for r in accounting_rows if r.get("inst_type") == "OPT"), 2),
                    "total_trades": len(accounting_rows),
                    "open_count": open_count,
                    "wins": wins,
                    "losses": losses,
                    "win_rate": round((wins / len(accounting_rows)) * 100, 1) if accounting_rows else 0.0,
                    "profit_factor": round(gross_win / gross_loss, 2) if gross_loss > 0 else (99.99 if gross_win > 0 else 1.0),
                }
            else:
                # No real session candidates — fall back to reconstructed closed trades
                closed_trades = payload.get("closed", [])
                open_trades = payload.get("open", [])
                all_trades = open_trades + closed_trades
                
                all_trades = sorted(all_trades, key=lambda x: x.get("entry_timestamp", 0) or x.get("entry_time", 0), reverse=True)
                closed_count = len(closed_trades)
                
                hist_signal_rows = []
                for row in all_trades:
                    entry_ts_sec = row.get("entry_timestamp", 0)
                    entry_iso = ""
                    if entry_ts_sec:
                        try:
                            entry_iso = datetime.fromtimestamp(entry_ts_sec, tz=IST).isoformat()
                        except Exception:
                            pass
                    
                    exit_ts_sec = row.get("exit_timestamp", 0)
                    exit_iso = ""
                    if exit_ts_sec:
                        try:
                            exit_iso = datetime.fromtimestamp(exit_ts_sec, tz=IST).isoformat()
                        except Exception:
                            pass
                    
                    is_closed = row.get("status") == "CLOSED" or row.get("exit_price", 0.0) > 0
                    
                    hist_signal_rows.append({
                        "instrument": row.get("instrument", ""),
                        "direction": row.get("direction", ""),
                        "price": row.get("entry_price", 0.0),
                        "current_price": row.get("exit_price", 0.0) or row.get("current_price", 0.0),
                        "pnl": row.get("pnl", 0.0),
                        "stop": row.get("trailing_stop", 0.0),
                        "target": row.get("target", 0.0),
                        "lots": row.get("lots", 1),
                        "lot_size": row.get("lot_size", 1),
                        "grade": row.get("grade", ""),
                        "confidence": row.get("confidence", 0.0),
                        "timeframe": row.get("timeframe", ""),
                        "inst_type": row.get("inst_type", "FUT"),
                        "option_type": row.get("option_type", ""),
                        "atm_strike": row.get("atm_strike", 0.0),
                        "timestamp": entry_iso or row.get("entry_time", ""),
                        "entry_date": row.get("entry_date", ""),
                        "exit_price": row.get("exit_price", 0.0),
                        "status": "EXIT SIGNAL" if is_closed else "TRADE SIGNAL",
                        "action": "EXIT" if is_closed else "ENTRY",
                        "exit_reason": row.get("exit_reason", ""),
                        "exit_timestamp": exit_iso or row.get("exit_time", ""),
                        "rr": row.get("rr_ratio", 1.5),
                        "trading_symbol": "",
                        "symbol_token": "",
                        "exec_type": row.get("exec_type", "A"),
                    })
                self._cached_hist_signals = hist_signal_rows
                self._cached_hist_closed_count = closed_count
                if not hist_signal_rows and filtered_rows:
                    filtered_rows = sorted(filtered_rows, key=lambda x: x.get("timestamp") or "", reverse=True)[:50]
                    payload["signals"] = filtered_rows
                    payload["summary"] = {
                        **(payload.get("summary") or {}),
                        "total_trades": 0,
                        "open_count": 0,
                    }
                else:
                    payload["signals"] = self._cached_hist_signals
        return payload

    def _handle_opposite_signal_exit(self, instrument: str, signal, timeframe: str) -> bool:
        """Exit/notify on raw opposite UT signals before entry filters can block them."""
        if signal.signal_type not in ("BUY", "SELL"):
            return False

        signal_ts = signal.timestamp
        new_direction = "LONG" if signal.signal_type == "BUY" else "SHORT"
        handled = False

        for tid, trade in list(self.trades.open_trades.items()):
            if trade.instrument != instrument or trade.status != "OPEN":
                continue
            is_opposite = (
                (trade.direction == "LONG" and signal.signal_type == "SELL") or
                (trade.direction == "SHORT" and signal.signal_type == "BUY")
            )
            if not is_opposite:
                continue

            dedupe_key = f"OPEN_{tid}_{timeframe}_{signal_ts.isoformat()}"
            if self._last_opposite_exit_signal_time.get(dedupe_key) == signal_ts:
                continue

            exit_price = getattr(trade, "current_price", 0.0) or signal.price
            if getattr(trade, "inst_type", "") == "OPT":
                exit_price = self._get_live_premium_cached(trade) or exit_price

            self._close_and_record(tid, exit_price, "OPPOSITE_SIGNAL")
            self._last_opposite_exit_signal_time[dedupe_key] = signal_ts
            self.log_event(f"EXIT {trade.direction} {instrument} ({timeframe} opposite {signal.signal_type})", "trade")
            self._notify(
                f"EXIT {trade.direction} {instrument} @ {exit_price:.2f} | Opposite {signal.signal_type} signal {timeframe}",
                "sell",
            )
            handled = True

        previous_candidates = list(self._latest_trade_candidates.get(instrument, []))
        remembered_keys = {self._candidate_history_key(c) for c in previous_candidates}
        for candidate in self._active_session_entry_candidates(instrument):
            key = self._candidate_history_key(candidate)
            if key not in remembered_keys:
                previous_candidates.append(candidate)
                remembered_keys.add(key)
        for previous in previous_candidates:
            if getattr(previous, "action", "ENTRY") == "EXIT":
                continue
            if previous.direction == new_direction:
                continue
            prev_ts = getattr(previous, "signal_timestamp", None)
            if prev_ts and signal_ts <= prev_ts:
                continue

            dedupe_key = f"MANUAL_{instrument}_{previous.timeframe}_{previous.direction}_{signal_ts.isoformat()}"
            if self._last_opposite_exit_signal_time.get(dedupe_key) == signal_ts:
                continue

            # check if an exit candidate already exists in today's ledger!
            store = getattr(self, "_session_trade_candidates", {}) or {}
            book = store.get(instrument, {})
            hist_key = self._candidate_history_key(previous)
            if hist_key in book and getattr(book[hist_key], "action", "") == "EXIT":
                # Exit has already been processed for this candidate. Do not overwrite/recalculate!
                continue

            # Calculate immediate exit price and P&L
            if previous.inst_type == "OPT":
                proposed_exit_px = self._get_live_premium_cached(previous) or previous.current_price or previous.price
            else:
                proposed_exit_px = signal.price
            exit_px, exit_reason, reason_text = self._candidate_exit_resolution(
                previous,
                float(proposed_exit_px),
                "OPPOSITE_SIGNAL",
                f"Opposite {signal.signal_type} signal",
            )
            calc_pnl = self._candidate_net_pnl(previous, exit_px)

            exit_candidate = TradeCandidate(
                instrument=previous.instrument,
                direction=previous.direction,
                price=previous.price,
                stop=previous.stop,
                target=0.0,
                lots=previous.lots,
                lot_size=previous.lot_size,
                grade=previous.grade,
                confidence=previous.confidence,
                timeframe=previous.timeframe,
                inst_type=previous.inst_type,
                option_type=previous.option_type,
                atm_strike=previous.atm_strike,
                multiplier=previous.multiplier,
                trading_symbol=previous.trading_symbol,
                symbol_token=previous.symbol_token,
                rr=previous.rr,
                score=previous.score,
                reasons=list(previous.reasons or []) + [f"{reason_text} at {signal_ts:%H:%M}"],
                spot_stop=previous.spot_stop,
                spot_target=previous.spot_target,
                signal_timestamp=prev_ts or signal_ts,
                current_price=exit_px,
                pnl=calc_pnl,
                status="EXIT SIGNAL",
                action="EXIT",
                exit_reason=exit_reason,
            )
            setattr(exit_candidate, "exit_timestamp", signal_ts)
            setattr(exit_candidate, "exit_price", float(exit_px))

            self._refresh_trade_candidates([exit_candidate], self.config.get("indices", {}).get(instrument, {}))
            self._latest_exit_candidates[instrument] = [exit_candidate]
            self._remember_trade_candidates([exit_candidate])
            self._last_opposite_exit_signal_time[dedupe_key] = signal_ts
            self.log_event(
                f"Manual EXIT {previous.direction} {instrument} ({previous.timeframe} closed by {exit_reason})",
                "trade",
            )
            self._notify(
                f"MANUAL EXIT {previous.direction} {instrument} @ {exit_candidate.current_price:.2f} | {previous.timeframe} closed by {exit_reason}",
                "sell",
            )
            handled = True

        return handled

    def _passes_timeframe_entry_policy(self, candidate: TradeCandidate) -> bool:
        """Return whether a candidate may become a fresh live/manual entry."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return True

        policy = str(getattr(get_settings(), "ut_timeframe_entry_policy", "PRIMARY_15") or "PRIMARY_15").upper()
        timeframe = str(getattr(candidate, "timeframe", "") or "")
        if policy == "PRIMARY_15" and timeframe == "5min":
            logger.info(
                f"Live Gate: blocked {candidate.instrument} {timeframe} {candidate.direction}; "
                "15M MAIN policy treats 5min as timing/exit-only."
            )
            return False
        return True

    def _passes_live_trade_ready_gate(self, candidate: TradeCandidate) -> bool:
        """Final live/manual gate before a signal is allowed into the session ledger."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return True

        grade_rank = self._grade_rank(getattr(candidate, "grade", ""))
        confidence = float(getattr(candidate, "confidence", 0.0) or 0.0)
        regime = str(self.latest_regimes.get(candidate.instrument, "UNKNOWN") or "UNKNOWN").upper()
        is_choppy = regime in {"CHOPPY", "SIDEWAYS", "MEAN_REVERTING", "RANGING", "VOLATILE", "UNKNOWN"}
        timeframe = str(getattr(candidate, "timeframe", "") or "")
        inst_type = str(getattr(candidate, "inst_type", "") or "")

        if not self._passes_timeframe_entry_policy(candidate):
            return False

        # 5min is a timing/exit timeframe in live mode. It must be exceptional to become a trade signal.
        if timeframe == "5min":
            min_conf = float(getattr(get_settings(), "ut_5min_option_min_confidence", 0.90) or 0.90)
            if grade_rank < 3 and confidence < min_conf:
                logger.info(
                    f"Live Gate: blocked {candidate.instrument} {timeframe} {candidate.direction}; "
                    f"5min setup is not A/A+ or very high-confidence ({candidate.grade}, {confidence:.0%})."
                )
                return False
            if is_choppy and not ((grade_rank >= 3 and confidence >= 0.72) or confidence >= min_conf):
                logger.info(
                    f"Live Gate: blocked {candidate.instrument} {timeframe} {candidate.direction}; "
                    f"choppy regime requires A/A+ with >=72% confidence, or very high confidence."
                )
                return False

        # Options decay badly in range-bound sessions, so choppy options need A/A+ or very high confidence.
        if is_choppy and inst_type == "OPT" and not ((grade_rank >= 3 and confidence >= 0.72) or confidence >= 0.90):
            logger.info(
                f"Live Gate: blocked {candidate.instrument} {inst_type} {candidate.direction}; "
                f"range/choppy regime allows only A/A+ option setups or very high confidence."
            )
            return False

        # Never promote weak B-grade live/manual rows in choppy markets unless confidence is exceptional.
        if is_choppy and grade_rank < 3 and confidence < 0.90:
            logger.info(
                f"Live Gate: blocked {candidate.instrument} {candidate.direction}; "
                f"grade {candidate.grade} is too weak for {regime}."
            )
            return False

        return True

    def _get_candidate_ltp_cached(self, candidate: TradeCandidate) -> Optional[float]:
        """Use in-memory candle prices for display rows; broker REST is a throttled fallback."""
        candle_price = self.candles.get_latest_price(candidate.instrument)
        if candle_price and candle_price > 0:
            return float(candle_price)

        if not (candidate.trading_symbol and candidate.symbol_token):
            return None

        ttl = 10.0
        key = f"{candidate.instrument}:{candidate.symbol_token}"
        now = time.time()
        cached = self._candidate_ltp_cache.get(key)
        if cached and now - cached.get("time", 0.0) <= ttl:
            return cached.get("price")

        exchange = "BFO" if candidate.instrument == "SENSEX" else "NFO"
        try:
            price = self.data.get_ltp(exchange, candidate.trading_symbol, candidate.symbol_token)
            if price and price > 0:
                self._candidate_ltp_cache[key] = {"price": float(price), "time": now}
                return float(price)
        except Exception as e:
            logger.debug(f"Candidate futures price refresh failed for {candidate.trading_symbol}: {e}")
        return cached.get("price") if cached else None

    def _refresh_trade_candidates(self, candidates: List[TradeCandidate], cfg: Dict):
        """Refresh live candidate prices so manual-mode signals do not freeze.
        In HISTORICAL mode, skip refreshing - session candidates already have correct prices."""
        if not candidates:
            return
        
        # In HISTORICAL mode, don't refresh prices - they're already correct from session files
        if getattr(self, "mode", "") == "HISTORICAL":
            return

        for candidate in candidates:
            if getattr(candidate, "action", "ENTRY") == "EXIT":
                frozen_exit_price = float(getattr(candidate, "exit_price", 0.0) or 0.0)
                if frozen_exit_price > 0:
                    candidate.current_price = frozen_exit_price
                    continue

            current = None
            if candidate.inst_type == "OPT":
                current = self._get_live_premium_cached(candidate)
            elif candidate.trading_symbol and candidate.symbol_token:
                current = self._get_candidate_ltp_cached(candidate)

            if not current or current <= 0:
                current = self.candles.get_latest_price(candidate.instrument)

            if not current or current <= 0:
                current = candidate.current_price or candidate.price

            candidate.current_price = float(current)
            candidate.pnl = self._candidate_net_pnl(candidate, candidate.current_price)
            if getattr(candidate, "action", "ENTRY") == "EXIT":
                setattr(candidate, "exit_price", float(candidate.current_price or candidate.price or 0.0))

    def _refresh_session_signal_payloads(self, results: Dict) -> None:
        """Rebuild dashboard signal rows after the final trade-ready gate has run."""
        self._prune_session_trade_candidates_for_live_gate()
        instruments = results.get("instruments", {}) if isinstance(results, dict) else {}
        for name, ui_data in instruments.items():
            if not isinstance(ui_data, dict):
                continue
            display_candidates = self._get_session_trade_candidates(name)
            cfg = self.config.get("indices", {}).get(name, {})
            self._refresh_trade_candidates(display_candidates, cfg)
            
            # Check for SL/TP breaches on manual session candidates
            self._check_session_candidates_breaches(name)
            
            # Fetch the final display candidates list (including any newly created exits)
            final_display_candidates = self._get_session_trade_candidates(name)
            self._refresh_trade_candidates(final_display_candidates, cfg)
            
            ui_data["trade_candidates"] = [self._serialize_trade_candidate(c) for c in final_display_candidates]

    def _check_session_candidates_breaches(self, instrument: str) -> None:
        """Check active manual/live session candidates for stoploss/target breaches and exit them."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return
            
        # Block simulated exits for manual mode to prevent Ghost Signals illusion on UI
        if not getattr(self, "auto_mode", True):
            return

        active_entries = self._active_session_entry_candidates(instrument)
        if not active_entries:
            return

        latest_time = self.candles.get_max_timestamp()
        if latest_time is not None:
            if getattr(latest_time, "tzinfo", None) is not None:
                latest_time = latest_time.astimezone(IST).replace(tzinfo=None)
            else:
                latest_time = latest_time.replace(tzinfo=None)
        else:
            latest_time = datetime.now(IST).replace(tzinfo=None)

        exits_to_create = []
        for previous in active_entries:
            current_price = previous.current_price or previous.price
            if not current_price or current_price <= 0:
                continue

            triggered = False
            exit_px = current_price
            exit_reason = ""
            reason_text = ""

            if self._candidate_stop_hit(previous, current_price):
                triggered = True
                exit_px = previous.stop
                exit_reason = "STOP_HIT"
                reason_text = "Stoploss hit"
            elif self._candidate_target_hit(previous, current_price):
                triggered = True
                exit_px = previous.target
                exit_reason = "TARGET_HIT"
                reason_text = "Target hit"

            if triggered:
                calc_pnl = self._candidate_net_pnl(previous, exit_px)

                exit_candidate = TradeCandidate(
                    instrument=previous.instrument,
                    direction=previous.direction,
                    price=previous.price,
                    stop=previous.stop,
                    target=previous.target,
                    lots=previous.lots,
                    lot_size=previous.lot_size,
                    grade=previous.grade,
                    confidence=previous.confidence,
                    timeframe=previous.timeframe,
                    inst_type=previous.inst_type,
                    option_type=previous.option_type,
                    atm_strike=previous.atm_strike,
                    multiplier=previous.multiplier,
                    trading_symbol=previous.trading_symbol,
                    symbol_token=previous.symbol_token,
                    rr=previous.rr,
                    score=previous.score,
                    reasons=list(previous.reasons or []) + [f"{reason_text} at {exit_px:.2f}"],
                    spot_stop=previous.spot_stop,
                    spot_target=previous.spot_target,
                    signal_timestamp=previous.signal_timestamp,
                    current_price=exit_px,
                    pnl=calc_pnl,
                    status="EXIT SIGNAL",
                    action="EXIT",
                    exit_reason=exit_reason,
                )
                setattr(exit_candidate, "exit_timestamp", latest_time)
                setattr(exit_candidate, "exit_price", float(exit_px))
                exits_to_create.append(exit_candidate)

                self.log_event(
                    f"Manual candidate SL/TP breach for {previous.instrument} {previous.direction} ({previous.timeframe}): {reason_text} @ {exit_px:.2f}",
                    "warning"
                )
                self._notify(
                    f"MANUAL EXIT {previous.direction} {previous.instrument} ({previous.timeframe}): {reason_text} @ {exit_px:.2f}",
                    "sell"
                )

        if exits_to_create:
            self._remember_trade_candidates(exits_to_create)
            for exit_candidate in exits_to_create:
                self._latest_exit_candidates[exit_candidate.instrument] = [exit_candidate]

    def _prune_session_trade_candidates_for_live_gate(self) -> None:
        """Remove pre-gate/weak rows left in today's manual ledger from older builds."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return

        store = getattr(self, "_session_trade_candidates", {}) or {}
        changed = False
        for instrument, book in list(store.items()):
            removed_entries = set()
            for key, candidate in list(book.items()):
                action = getattr(candidate, "action", "ENTRY")
                if action == "EXIT":
                    continue
                if action == "NO_ENTRY":
                    book.pop(key, None)
                    changed = True
                    continue
                if self._passes_live_trade_ready_gate(candidate):
                    continue
                entry_ts = getattr(candidate, "signal_timestamp", None)
                removed_entries.add((
                    candidate.instrument,
                    candidate.timeframe,
                    candidate.direction,
                    candidate.trading_symbol or "",
                    entry_ts.isoformat(timespec="seconds") if entry_ts else "",
                ))
                book.pop(key, None)
                changed = True

            if removed_entries:
                for key, candidate in list(book.items()):
                    if getattr(candidate, "action", "ENTRY") != "EXIT":
                        continue
                    entry_ts = getattr(candidate, "signal_timestamp", None)
                    entry_key = (
                        candidate.instrument,
                        candidate.timeframe,
                        candidate.direction,
                        candidate.trading_symbol or "",
                        entry_ts.isoformat(timespec="seconds") if entry_ts else "",
                    )
                    if entry_key in removed_entries:
                        book.pop(key, None)
                        changed = True

            if not book:
                store.pop(instrument, None)
                changed = True

        if changed:
            self._persist_session_trade_candidates()

    def _grade_rank(self, grade: str) -> int:
        base = str(grade or "C").split()[0]
        return {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4, "Recovered": 4}.get(base, 0)

    def _is_aplus_setup(self, item) -> bool:
        return self._grade_rank(getattr(item, "grade", "")) >= 4 or float(getattr(item, "confidence", 0.0) or 0.0) >= 0.90

    def _force_candidate_instrument_type(self, candidate: TradeCandidate, target_type: str) -> bool:
        """Convert a same-index cross-TF candidate to the complementary instrument type."""
        target_type = (target_type or "").upper()
        if target_type == candidate.inst_type:
            return True

        spot = self.candles.get_latest_price(candidate.instrument) or getattr(candidate, "entry_spot", 0.0) or candidate.price
        rr = max(1.1, float(candidate.rr or 1.5))

        if target_type == "FUT":
            info = self.market_info.get(candidate.instrument, {})
            symbol = info.get("current_fut", "")
            token = info.get("current_fut_token", "")
            if not symbol or not token:
                logger.warning(f"Concurrency Guard: cannot convert {candidate.instrument} to FUT; missing futures token.")
                return False

            exchange = "BFO" if candidate.instrument == "SENSEX" else "NFO"
            fut_price = 0.0
            try:
                fut_price = float(self.data.get_ltp(exchange, symbol, token) or 0.0)
            except Exception as e:
                logger.debug(f"FUT conversion LTP fallback for {symbol}: {e}")
            if fut_price <= 0:
                fut_price = float(spot or candidate.price)

            risk = max(1.0, fut_price * (self.futures_sl_pct / 100.0))
            candidate.inst_type = "FUT"
            candidate.option_type = ""
            candidate.atm_strike = 0.0
            candidate.multiplier = 1.0
            candidate.trading_symbol = symbol
            candidate.symbol_token = token
            candidate.price = fut_price
            candidate.current_price = fut_price
            if candidate.direction == "LONG":
                candidate.stop = fut_price - risk
                candidate.target = fut_price + (risk * rr)
            else:
                candidate.stop = fut_price + risk
                candidate.target = fut_price - (risk * rr)
            candidate.reasons = list(candidate.reasons or []) + ["Concurrency Guard forced FUT complement"]
            return True

        if target_type == "OPT":
            strike_interval = self.config.get("indices", {}).get(candidate.instrument, {}).get("strike_interval", 50)
            strike = round(float(spot or candidate.price) / strike_interval) * strike_interval
            option_type = "CE" if candidate.direction == "LONG" else "PE"
            opt_info = self.data.get_option_token(candidate.instrument, strike, option_type)
            if not opt_info:
                logger.warning(f"Concurrency Guard: cannot convert {candidate.instrument} to OPT; missing option token.")
                return False

            premium = 0.0
            exchange = "BFO" if candidate.instrument == "SENSEX" else "NFO"
            try:
                premium = float(self.data.get_ltp(exchange, opt_info["symbol"], opt_info["token"]) or 0.0)
            except Exception as e:
                logger.debug(f"OPT conversion LTP fallback for {opt_info.get('symbol')}: {e}")
            if premium <= 0:
                premium = max(1.0, float(spot or candidate.price) * 0.012)

            risk = max(0.5, premium * (self.options_sl_pct / 100.0))
            candidate.inst_type = "OPT"
            candidate.option_type = option_type
            candidate.atm_strike = strike
            candidate.multiplier = 0.5 if option_type == "CE" else -0.5
            candidate.trading_symbol = opt_info["symbol"]
            candidate.symbol_token = opt_info["token"]
            candidate.price = premium
            candidate.current_price = premium
            candidate.stop = premium - risk
            candidate.target = premium + (risk * rr)
            candidate.reasons = list(candidate.reasons or []) + ["Concurrency Guard forced OPT complement"]
            return True

        return False

    def _prepare_candidate_for_concurrency(self, candidate: TradeCandidate) -> bool:
        """Hard guard: one index can only add a cross-TF trade as the opposite instrument type."""
        if not getattr(get_settings(), "ut_concurrency_guard", True):
            return True

        base_inst = candidate.instrument.split()[0]
        existing = [t for t in self.trades.open_trades.values() if t.instrument.split()[0] == base_inst]
        if not existing:
            # Check manual/live session candidate signals as well for concurrency matching
            session_entries = self._active_session_entry_candidates(candidate.instrument, as_of=candidate.signal_timestamp)
            if session_entries:
                existing = session_entries

        if not existing:
            return True
        if len(existing) >= 2:
            logger.info(f"Concurrency Guard: blocked {candidate.instrument}; max 2 concurrent trades per index.")
            return False

        existing_trade = existing[0]
        if getattr(existing_trade, "timeframe", "") == candidate.timeframe:
            logger.info(f"Concurrency Guard: blocked {candidate.instrument}; same timeframe already active.")
            return False

        existing_rank = self._grade_rank(getattr(existing_trade, "grade", ""))
        candidate_rank = self._grade_rank(candidate.grade)
        if candidate_rank <= existing_rank and not self._is_aplus_setup(candidate):
            logger.info(f"Concurrency Guard: blocked {candidate.instrument}; cross-TF setup is not higher grade.")
            return False

        target_type = "OPT" if getattr(existing_trade, "inst_type", "FUT") == "FUT" else "FUT"
        if candidate.inst_type != target_type:
            if (self.inst_pref or "AUTO").upper() in {"FUT", "OPT"} and (self.inst_pref or "AUTO").upper() != target_type:
                logger.info(f"Concurrency Guard: blocked {candidate.instrument}; fixed preference prevents {target_type} complement.")
                return False
            return self._force_candidate_instrument_type(candidate, target_type)
        return True

    def _passes_correlated_index_guard(self, candidate: TradeCandidate, direction: str) -> bool:
        """Hard guard: at most two same-direction index trades unless all three are A+."""
        if not getattr(get_settings(), "ut_concurrency_guard", True):
            return True

        same_dir = [t for t in self.trades.open_trades.values() if t.direction == direction]
        if len(same_dir) < 2:
            return True

        all_aplus = all(self._is_aplus_setup(t) for t in same_dir) and self._is_aplus_setup(candidate)
        if all_aplus:
            logger.info(f"Concurrency Guard: allowing 3rd correlated {direction} only because all setups are A+.")
            return True

        logger.info(f"Concurrency Guard: blocked {candidate.instrument} {direction}; best-2 correlated index limit reached.")
        return False

    async def _process_instrument_async(self, name: str, cfg: Dict) -> Tuple[str, Dict]:
        """Async version of instrument processing for ultra-low latency"""
        try:
            # 1. Fetch Price & Update Candle State (I/O Bound)
            token = cfg.get("token", "")
            exchange = cfg.get("exchange", "NSE")

            # Fetch LTP directly from SmartAPI (Fast)
            spot = self.data.get_ltp(exchange, name, token)
            if spot and spot > 0:
                self.candles.update_latest_price(name, spot)

            # 2. Run Analysis Pipeline (Using cached data)
            ui_data, candidate = self._process_instrument(name, cfg)
            return name, (ui_data, candidate)
        except Exception as e:
            logger.error(f"â Œ Async error for {name}: {e}")
            return name, ({}, [])

    # â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â•
    # BEST SIGNAL SELECTION â€” across 5m and 15m TFs
    # â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â•
    # ====================================================================
    # LIVE/HISTORICAL PARITY HELPERS
    # --------------------------------------------------------------------
    # The historical backfill path used to call _grade_signal with
    # hardcoded confluence=0.5, intel=0.0 and a date-only VIX key (which
    # never matched the minute-level vix_history.json). That made backtest
    # grades drift from live for the same setup. These helpers reproduce
    # the live grading inputs as faithfully as the available data allows:
    #   * intel.analyze() runs on candles sliced at the signal timestamp
    #     (volume/regime/order-flow always recomputable; OI/PCR/Greeks
    #     degrade gracefully when historical option chain is unavailable
    #     -- exactly like the live aggregator's None-chain path)
    #   * confluence is reconstructed from each TF's signals up to ts
    #     using the same MTF weights as MultiTimeframeEngine
    #   * VIX uses the same minute key shape as the live path with a
    #     same-day fallback so older signals get a real reading instead of
    #     the 15.0 sentinel.
    # ====================================================================
    def _get_historical_vix(self, ts) -> float:
        """Resolve VIX at a historical timestamp.

        1) Exact minute key '%Y-%m-%d %H:%M:00' (matches vix_history.json)
        2) Latest minute on same date <= ts; else earliest of that date
        3) Fallback 15.0
        """
        if not isinstance(self._vix_data, dict) or not self._vix_data:
            return 15.0
        try:
            minute_key = ts.strftime("%Y-%m-%d %H:%M:00")
        except Exception:
            return 15.0
        v = self._vix_data.get(minute_key)
        if v is not None:
            try:
                vf = float(v)
                if vf > 0:
                    return vf
            except Exception:
                pass
        date_prefix = ts.strftime("%Y-%m-%d ")
        try:
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
            same_day = [(k, val) for k, val in self._vix_data.items() if isinstance(k, str) and k.startswith(date_prefix)]
            if same_day:
                before = [(k, val) for k, val in same_day if k <= ts_str]
                if before:
                    _, val = max(before, key=lambda kv: kv[0])
                else:
                    _, val = min(same_day, key=lambda kv: kv[0])
                vf = float(val)
                if vf > 0:
                    return vf
        except Exception:
            pass
        return 15.0

    def _compute_historical_intel_at(self, instrument: str, ts, tf: str = "5min"):
        """Recompute intelligence score and regime at a historical timestamp.

        Slices cached candles to ``ts`` and runs the same
        ``IntelligenceAggregator.analyze`` used by the live path with
        ``options_chain=None``. Returns ``(intel_score in [-1,1], regime_str)``.
        On any failure returns ``(0.0, 'UNKNOWN')``.
        """
        try:
            df_5m = self.candles.get_candles(instrument, "5min")
            df_1m = self.candles.get_candles(instrument, "1min")
            if df_5m is None or df_5m.empty:
                return 0.0, "UNKNOWN"
            sub_5m = df_5m[df_5m.index <= ts]
            sub_1m = None
            if df_1m is not None and not df_1m.empty:
                sub_1m = df_1m[df_1m.index <= ts]
                if sub_1m.empty:
                    sub_1m = None
            if sub_5m.empty:
                return 0.0, "UNKNOWN"
            spot = float(sub_5m["close"].iloc[-1])
            try:
                o = float(sub_5m["open"].iloc[-1])
                pct = ((spot - o) / o) * 100.0 if o > 0 else 0.0
            except Exception:
                pct = 0.0
            intel_res = self.intel.analyze(
                instrument=instrument,
                timeframe=tf,
                candle_df=sub_5m,
                candle_1min_df=sub_1m,
                options_chain=None,
                spot_price=spot,
                strike_interval=50,
                days_to_expiry=7,
                price_change_pct=pct,
                chain_quality=None,
            )
            agg = intel_res.get("aggregate", {}) if isinstance(intel_res, dict) else {}
            intel_score = float(agg.get("score", 0.0)) / 100.0
            intel_score = max(-1.0, min(1.0, intel_score))
            regime = (intel_res.get("regime", {}) or {}).get("regime", "UNKNOWN")
            return intel_score, regime
        except Exception as exc:
            logger.debug(f"_compute_historical_intel_at failed for {instrument} @ {ts}: {exc}")
            return 0.0, "UNKNOWN"

    def _compute_historical_confluence_at(self, mtf_result, ts) -> float:
        """Reconstruct MTF confluence score at a historical timestamp.

        For each TF, finds the most recent signal flip <= ``ts`` and applies
        the same weights as ``MultiTimeframeEngine._compute_confluence``.
        Returns float in [-1.0, +1.0].
        """
        if mtf_result is None:
            return 0.0
        tf_weights = getattr(self.mtf, "tf_weights", {"1min": 0.15, "5min": 0.40, "15min": 0.45})

        def _naive(t):
            try:
                return t.replace(tzinfo=None) if getattr(t, "tzinfo", None) else t
            except Exception:
                return t

        naive_ts = _naive(ts)
        score = 0.0
        total_weight = 0.0
        for tf, weight in tf_weights.items():
            if tf == "1min":
                res = getattr(mtf_result, "results_1min", None)
            elif tf == "5min":
                res = getattr(mtf_result, "results_5min", None)
            elif tf == "15min":
                res = getattr(mtf_result, "results_15min", None)
            else:
                res = None
            if not res:
                continue
            sigs = res.get("signals", []) or []
            prior = [s for s in sigs if _naive(s.timestamp) <= naive_ts]
            if not prior:
                continue
            latest = max(prior, key=lambda s: _naive(s.timestamp))
            pos = 1 if latest.signal_type == "BUY" else (-1 if latest.signal_type == "SELL" else 0)
            score += pos * weight
            total_weight += weight
        if total_weight > 0:
            score = score / total_weight
        return round(max(-1.0, min(1.0, score)), 3)

    def _resolve_instrument_type(
        self,
        instrument: str,
        grade: str,
        regime: str,
        intel_score: float = 0.0,
        signal_score: float = 0.0,
        confidence: float = 0.0,
        adx_value: float = 0.0,
        atr_value: float = 0.0,
        price: float = 0.0,
        signal_time: Optional[datetime] = None,
        iv_percentile: float = 50.0,
        existing_type: Optional[str] = None,
    ) -> str:
        """Resolve FUT/OPT once so backtest, recovery, and live use the same Hybrid rules."""
        pref = (self.inst_pref or "AUTO").upper()
        if pref == "FUT":
            return "FUT"
        if pref == "OPT":
            return "OPT"

        if existing_type in ("FUT", "OPT"):
            return "OPT" if existing_type == "FUT" else "FUT"

        grade = grade or ""
        regime = regime or "UNKNOWN"
        adx = float(adx_value or 0.0)
        atr_pct = (float(atr_value or 0.0) / float(price or 1.0)) * 100.0 if price else 0.0
        score = float(signal_score or 0.0)
        conf = float(confidence or 0.0)
        intel = abs(float(intel_score or 0.0))
        ivp = float(iv_percentile or 50.0)

        is_choppy = regime in {"CHOPPY", "SIDEWAYS", "MEAN_REVERTING", "RANGING", "VOLATILE", "UNKNOWN"}

        # UT1 Grade Rules:
        # Options allowed on B+, A, A+
        # Futures allowed on B, B+, A, A+
        high_grade = grade in {"A", "A+", "B+"}
        strong_momentum = high_grade and (conf >= 0.62 or score >= 0.45 or intel >= 55.0 or adx >= 28.0)
        high_iv = ivp >= 75.0
        high_spot_vol = atr_pct >= 0.65

        is_late_session = False
        is_0dte = False
        if signal_time is not None:
            is_late_session = signal_time.time() >= dtime(14, 0)
            try:
                is_0dte = self.expiry.is_expiry_day(instrument, signal_time.date())
            except Exception:
                is_0dte = False

        if is_0dte and is_late_session and not strong_momentum:
            return "FUT"
        if high_iv or high_spot_vol or is_choppy:
            return "FUT"
        if strong_momentum:
            return "OPT"
        return "FUT"

    def _process_best_signal(
        self, instrument, mtf_result, intel_result, intel_score, regime,
        lots, lot_size, cfg, spot, atm_strike,
    ) -> List[TradeCandidate]:
        settings = get_settings()
        if getattr(self.risk_manager, "daily_loss_breached", False):
            return []

        # Collect NEW signals from both signal TFs
        candidates_raw = []
        # â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â•
        # UNIFIED CHRONOLOGICAL HISTORICAL BACKFILL
        # â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â• â•
        backfill_key = f"{instrument}_backfill"
        if backfill_key not in self._last_signal_time:
            self._last_signal_time[backfill_key] = datetime.now()

            # Load last N days for Historical mode, or 1 day for Live modes (Warm Startup)
            is_live = "REAL" in self.mode.upper()
            if (self.mode == "HISTORICAL" or is_live):
                settings = get_settings()

                # Fetch 5min candles to determine trading dates for lookback
                candles_df = self.candles.get_candles(instrument, "5min")
                if candles_df is not None and not candles_df.empty:
                    data_lookback = 45 # Use up to 45 trading days for indicator warm-up
                    trade_lookback = self.backtest_days

                    # Extract unique trading dates
                    active_dates = sorted(list(set(candles_df.index.date)))
                    if active_dates:
                        trade_lookback = int(self.backtest_days)
                        trade_cutoff = active_dates[-min(len(active_dates), trade_lookback)]
                        data_cutoff = active_dates[-min(len(active_dates), data_lookback)]

                        logger.info(f"ðŸ“Š BACKFILL START: {instrument} | Days: {trade_lookback} | Cutoff: {trade_cutoff}")

                        # Collect all signal pairs from both timeframes
                        all_pairs = []
                        for backfill_tf in ["5min", "15min"]:
                            res = mtf_result.results_5min if backfill_tf == "5min" else mtf_result.results_15min
                            if res is None:
                                continue
                            tf_sigs = res.get("signals", [])
                            if not tf_sigs:
                                continue

                            # Early session gate: check if candle close time >= 09:18
                            tf_sigs = [
                                s for s in tf_sigs
                                if (s.timestamp + timedelta(minutes=5 if backfill_tf == "5min" else 15)).time() >= dtime(9, 18)
                            ]
                            if not tf_sigs:
                                continue

                            def get_naive_ts(s):
                                return s.timestamp.replace(tzinfo=None) if s.timestamp.tzinfo else s.timestamp

                            sorted_sigs = sorted([s for s in tf_sigs if s.instrument == instrument and s.timestamp.date() >= data_cutoff], key=get_naive_ts)

                            # Initialize live lock timestamp for each timeframe so they only process future live ticks
                            if sorted_sigs:
                                self._last_signal_time[f"{instrument}_{backfill_tf}"] = sorted_sigs[-1].timestamp
                            else:
                                self._last_signal_time[f"{instrument}_{backfill_tf}"] = datetime.min

                            # Create regular pairs
                            for idx in range(len(sorted_sigs) - 1):
                                s1, s2 = sorted_sigs[idx], sorted_sigs[idx+1]
                                if s1.timestamp.date() >= trade_cutoff and s2.timestamp.date() >= trade_cutoff and s1.signal_type != s2.signal_type:
                                    all_pairs.append((s1, s2, backfill_tf))

                            # Create synthetic EOD close if applicable
                            if sorted_sigs:
                                last_s = sorted_sigs[-1]
                                # Only if last_s is from a previous day
                                if last_s.timestamp.date() >= trade_cutoff:
                                    today = datetime.now(IST).date()
                                    if last_s.timestamp.date() < today or self.mode == "HISTORICAL":
                                        all_pairs.append((last_s, None, backfill_tf))

                        # Sort all pairs chronologically across all timeframes by entry time
                        def get_pair_naive_ts(p):
                            return p[0].timestamp.replace(tzinfo=None) if p[0].timestamp.tzinfo else p[0].timestamp

                        all_pairs = sorted(all_pairs, key=get_pair_naive_ts)

                        # Process pairs in absolute chronological order!
                        for s1, s2, tf in all_pairs:
                            # â”€â”€ Dynamic Regime-Aware Quality Gate â”€â”€
                            sub_df = candles_df[candles_df.index <= s1.timestamp]
                            hist_regime_res = self.intel.regime.detect(sub_df, instrument, "5min")
                            hist_regime = hist_regime_res.get("regime", "UNKNOWN")

                            # Live/historical parity (mirrors signal_processor.py):
                            # real VIX (minute-key with same-day fallback) + per-bar
                            # confluence + per-bar intel score (intel.analyze on
                            # candles sliced at s1.timestamp, options_chain=None).
                            hist_vix = self._get_historical_vix(s1.timestamp)
                            hist_intel_score, hist_intel_regime = self._compute_historical_intel_at(
                                instrument, s1.timestamp, tf
                            )
                            if hist_intel_regime and hist_intel_regime != "UNKNOWN":
                                hist_regime = hist_intel_regime
                            hist_confluence = self._compute_historical_confluence_at(mtf_result, s1.timestamp)

                            hist_grade, hist_conf, hist_reasons = self.signals._grade_signal(
                                s1, hist_confluence, hist_intel_score, regime=hist_regime, vix_value=hist_vix
                            )

                            sim_inst = self._resolve_instrument_type(
                                instrument=instrument,
                                grade=hist_grade,
                                regime=hist_regime,
                                intel_score=intel_score,
                                signal_score=hist_conf,
                                confidence=hist_conf,
                                adx_value=getattr(s1, "adx_value", 0.0),
                                atr_value=getattr(s1, "atr_value", 0.0),
                                price=s1.price,
                                signal_time=s1.timestamp,
                            )

                            grade_hierarchy = {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4}
                            base_grade = hist_grade
                            sig_rank = grade_hierarchy.get(base_grade, 0)

                            grade_pref = getattr(settings, "signal_grade_preference", "auto")
                            is_choppy = hist_regime in {"CHOPPY", "SIDEWAYS", "MEAN_REVERTING", "RANGING", "VOLATILE", "UNKNOWN"}
                            direction = "LONG" if s1.signal_type == "BUY" else "SHORT"

                            # -- Aligned Quality Gates (mirror _passes_live_trade_ready_gate) --

                            # 5min is a timing/exit TF; must be exceptional to become a trade signal
                            if tf == "5min":
                                min_conf_5m = float(getattr(settings, "ut_5min_option_min_confidence", 0.90) or 0.90)
                                if sig_rank < 3 and hist_conf < min_conf_5m:
                                    self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, f"5min gate needs A grade or {min_conf_5m:.0%} confidence", sim_inst, lot_size, atm_strike)
                                    continue
                                if is_choppy and not ((sig_rank >= 3 and hist_conf >= 0.72) or hist_conf >= min_conf_5m):
                                    self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, "5min choppy market gate", sim_inst, lot_size, atm_strike)
                                    continue

                            # Options decay in choppy regimes - need A/A+ or very high confidence
                            if is_choppy and sim_inst == "OPT" and not ((sig_rank >= 3 and hist_conf >= 0.72) or hist_conf >= 0.90):
                                self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, "Options blocked in choppy regime", sim_inst, lot_size, atm_strike)
                                continue

                            # Block weak grades in choppy markets unless confidence is exceptional
                            if is_choppy and sig_rank < 3 and hist_conf < 0.90:
                                self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, "Weak grade in choppy regime", sim_inst, lot_size, atm_strike)
                                continue

                            # Dynamic Confidence Filter
                            min_confidence = 0.58 if not is_choppy else 0.65
                            if hist_conf < min_confidence:
                                self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, f"Confidence below {min_confidence:.0%}", sim_inst, lot_size, atm_strike)
                                continue

                            # 3. Grade Preference Filter
                            if grade_pref == "B":
                                # Futures allow B (1), Options require B+ (2)
                                min_rank = 1 if sim_inst == "FUT" else 2
                            elif grade_pref == "B+":
                                min_rank = 2
                            elif grade_pref == "A":
                                min_rank = 3
                            else: # auto
                                min_rank = 3 if is_choppy else 2

                            if sig_rank < min_rank:
                                self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, f"Grade rank below required {min_rank}", sim_inst, lot_size, atm_strike)
                                continue

                            # â”€â”€ Historical Concurrency Guard â”€â”€
                            is_overlapping = False
                            if getattr(settings, "ut_concurrency_guard", True):
                                for ct in self.trades.closed_trades:
                                    if ct.instrument.split()[0] == instrument.split()[0]:
                                        ct_entry = ct.entry_time.replace(tzinfo=None) if (ct.entry_time and getattr(ct.entry_time, "tzinfo", None)) else (ct.entry_time or datetime.min)
                                        ct_exit = ct.exit_time.replace(tzinfo=None) if (ct.exit_time and getattr(ct.exit_time, "tzinfo", None)) else (ct.exit_time or datetime.max)
                                        s_ts = s1.timestamp.replace(tzinfo=None) if (s1.timestamp and getattr(s1.timestamp, "tzinfo", None)) else (s1.timestamp or datetime.min)

                                        if s2 is not None:
                                            cand_exit = s2.timestamp.replace(tzinfo=None) if (s2.timestamp and getattr(s2.timestamp, "tzinfo", None)) else (s2.timestamp or datetime.max)
                                        else:
                                            cand_exit = datetime.combine(s1.timestamp.date(), dtime(15, 20))

                                        if max(ct_entry, s_ts) < min(ct_exit, cand_exit):
                                            is_overlapping = True
                                            break

                            if is_overlapping:
                                self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, "Skipped by concurrency guard", sim_inst, lot_size, atm_strike)
                                continue

                            # Check for existing manual exits before applying EOD close
                            if s2 is None:
                                already_exited = False
                                book = getattr(self, "_session_trade_candidates", {}).get(instrument, {})
                                for c in book.values():
                                    if getattr(c, "action", "ENTRY") == "EXIT" and c.timeframe == tf and c.direction == ("LONG" if s1.signal_type == "BUY" else "SHORT"):
                                        if getattr(c, "signal_timestamp", None) == s1.timestamp:
                                            already_exited = True
                                            break
                                if already_exited:
                                    self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, "Already has matching exit", sim_inst, lot_size, atm_strike)
                                    continue

                            # â• â• â•  HISTORICAL INTELLIGENCE (Respect User Preference) â• â• â•
                            sim_option_type = ("CE" if direction == "LONG" else "PE") if sim_inst == "OPT" else ""
                            sim_multiplier = 0.5 if sim_inst == "OPT" else 1.0

                            # â• â• â•  HISTORICAL RISK SIZING â• â• â•
                            if sim_inst == "FUT":
                                hist_cap = self.capital_fut
                                hist_risk = self.risk_fut_pct
                            else:
                                hist_cap = self.capital_opt
                                hist_risk = self.risk_opt_pct

                            risk_amount = hist_cap * (hist_risk / 100.0)
                            risk_amount = min(risk_amount, getattr(settings, "max_trade_loss_abs", 10000.0))
                            index_stop_distance = s1.stop_distance

                            if sim_inst == "FUT":
                                risk_amount = (self.capital_fut * self.risk_fut_pct / 100)
                                max_allowed_sl = s1.price * (self.futures_sl_pct / 100.0)
                                if index_stop_distance > (max_allowed_sl * 1.5):
                                    self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, "Stop distance above futures hard cap", sim_inst, lot_size, atm_strike)
                                    continue
                                index_stop_distance = min(index_stop_distance, max_allowed_sl)
                            else:
                                risk_amount = (self.capital_opt * self.risk_opt_pct / 100)

                            unit_risk = max(1.0, index_stop_distance) * sim_multiplier

                            if sim_inst == "FUT":
                                hist_lots = self.user_lots_fut.get(instrument, 1)
                            else:
                                max_units = int(risk_amount / unit_risk)
                                user_target = self.user_lots.get(instrument, 1)
                                hist_lots = min(user_target, max(1, int(max_units / lot_size)))

                            qty = hist_lots * lot_size
                            entry_premium = s1.price * 0.012 if sim_inst == "OPT" else s1.price

                            if sim_inst == "FUT":
                                max_sl_dist = entry_premium * (self.futures_sl_pct / 100.0)
                                sl_price = s1.trailing_stop
                                if direction == "LONG":
                                    sl_price = max(sl_price, entry_premium - max_sl_dist)
                                else:
                                    sl_price = min(sl_price, entry_premium + max_sl_dist)
                            else:
                                # Options are premium-long instruments (CE/PE buy). Keep SL below entry.
                                max_sl_dist = entry_premium * (self.options_sl_pct / 100.0)
                                natural_sl_dist = abs(index_stop_distance * sim_multiplier)
                                effective_sl_dist = max(0.05, min(natural_sl_dist, max_sl_dist))
                                sl_price = max(0.05, entry_premium - effective_sl_dist)

                            sim_charges = 0.0

                            # Determine EOD hard square-off time limit (15:18 PM on entry day)
                            day_end_dt = datetime.combine(s1.timestamp.date(), dtime(15, 20))
                            is_overnight = (s2 is None) or (s2.timestamp.replace(tzinfo=None) if s2.timestamp.tzinfo else s2.timestamp) > day_end_dt

                            if is_overnight:
                                raw_exit_spot = s1.price # fallback
                                candles_for_tf = self.candles.get_candles(instrument, tf)
                                if candles_for_tf is not None and not candles_for_tf.empty:
                                    day_candles = candles_for_tf[candles_for_tf.index.date == s1.timestamp.date()]
                                    day_candles = day_candles[day_candles.index.time <= dtime(15, 20)]
                                    if not day_candles.empty:
                                        raw_exit_spot = day_candles['close'].iloc[-1]

                                if sim_inst == "OPT":
                                    spot_diff = raw_exit_spot - s1.price if direction == "LONG" else s1.price - raw_exit_spot
                                    raw_exit_premium = entry_premium + (spot_diff * sim_multiplier)
                                    time_held_hours = (day_end_dt - s1.timestamp).total_seconds() / 3600.0
                                    if time_held_hours > 1.0 and abs(spot_diff) < (s1.atr_value * 0.5):
                                        time_held_hours = 1.0
                                        spot_diff = 0
                                        raw_exit_spot = s1.price
                                    if time_held_hours > 0:
                                        dte = self.expiry.get_dte(instrument, s1.timestamp.date())
                                        actual_dte = dte + 7 if dte <= 1 else dte
                                        theta_rate = 0.05 if actual_dte == 0 else (0.02 if actual_dte == 1 else 0.005)
                                        theta_decay = entry_premium * theta_rate * time_held_hours
                                        if spot_diff > 0:
                                            theta_decay *= 0.5
                                        raw_# exit_premium -= theta_decay
                                    raw_# exit_premium -= (entry_premium * 0.002)
                                    raw_exit_premium = max(0.05, raw_exit_premium)
                                else:
                                    raw_exit_premium = raw_exit_spot

                                exit_premium = raw_exit_premium
                                is_sl_hit = False
                                if sim_inst == "FUT":
                                    if direction == "LONG" and raw_exit_spot < sl_price:
                                        exit_premium = sl_price
                                        is_sl_hit = True
                                    elif direction == "SHORT" and raw_exit_spot > sl_price:
                                        exit_premium = sl_price
                                        is_sl_hit = True
                                else:
                                    if raw_exit_premium < sl_price:
                                        exit_premium = sl_price
                                        is_sl_hit = True

                                hist_exit_reason = "SL HIT" if is_sl_hit else "SESSION_END"
                                ex_time = IST.localize(day_end_dt)
                            else:
                                raw_exit_spot = s2.price
                                if sim_inst == "OPT":
                                    spot_diff = raw_exit_spot - s1.price if direction == "LONG" else s1.price - raw_exit_spot
                                    raw_exit_premium = entry_premium + (spot_diff * sim_multiplier)
                                    time_held_hours = (s2.timestamp - s1.timestamp).total_seconds() / 3600.0
                                    if time_held_hours > 1.0 and abs(spot_diff) < (s1.atr_value * 0.5):
                                        time_held_hours = 1.0
                                        spot_diff = 0
                                        raw_exit_spot = s1.price
                                    if time_held_hours > 0:
                                        dte = self.expiry.get_dte(instrument, s1.timestamp.date())
                                        actual_dte = dte + 7 if dte <= 1 else dte
                                        theta_rate = 0.05 if actual_dte == 0 else (0.02 if actual_dte == 1 else 0.005)
                                        theta_decay = entry_premium * theta_rate * time_held_hours
                                        if spot_diff > 0:
                                            theta_decay *= 0.5
                                        raw_# exit_premium -= theta_decay
                                    raw_# exit_premium -= (entry_premium * 0.002)
                                    raw_exit_premium = max(0.05, raw_exit_premium)
                                else:
                                    raw_exit_premium = raw_exit_spot

                                exit_premium = raw_exit_premium
                                is_sl_hit = False
                                if sim_inst == "FUT":
                                    if direction == "LONG" and raw_exit_spot < sl_price:
                                        exit_premium = sl_price
                                        is_sl_hit = True
                                    elif direction == "SHORT" and raw_exit_spot > sl_price:
                                        exit_premium = sl_price
                                        is_sl_hit = True
                                else:
                                    if raw_exit_premium < sl_price:
                                        exit_premium = sl_price
                                        is_sl_hit = True

                                hist_exit_reason = "SL HIT" if is_sl_hit else "SESSION_END"
                                ex_time = IST.localize(s2.timestamp) if s2.timestamp.tzinfo is None else s2.timestamp

                            # â• â• â•  CALCULATE P&L â• â• â•
                            if sim_inst == "OPT":
                                gross_pnl = (exit_premium - entry_premium) * qty
                            else:
                                if direction == "LONG":
                                    gross_pnl = (exit_premium - entry_premium) * qty
                                else:
                                    gross_pnl = (entry_premium - exit_premium) * qty

                            net_pnl = gross_pnl - sim_charges
                            hist_rr = round(0.5 + (hist_conf * 2.0), 2)

                            if s2 is not None:
                                hist_id = f"H_{instrument}_{tf}_{s1.timestamp.strftime('%Y%m%d%H%M%S')}"
                                display_grade = f"{hist_grade} (Hist)"
                            else:
                                hist_id = f"EOD_{instrument}_{tf}_{s1.timestamp.strftime('%m%d%H%M')}"
                                display_grade = f"{hist_grade} (EOD, Hist)"

                            if any(ct.id == hist_id for ct in self.trades.closed_trades):
                                continue

                            if direction == "LONG":
                                hist_target = s1.price + (index_stop_distance * hist_rr)
                            else:
                                hist_target = s1.price - (index_stop_distance * hist_rr)
                            if sim_inst == "OPT":
                                risk_per_unit = max(0.5, entry_premium - sl_price)
                                hist_target = entry_premium + (risk_per_unit * hist_rr)

                            from trading.trade_manager import Trade
                            e_time = IST.localize(s1.timestamp) if s1.timestamp.tzinfo is None else s1.timestamp
                            t = Trade(
                                id=hist_id, instrument=instrument, timeframe=tf,
                                direction=direction, entry_price=entry_premium, entry_time=e_time,
                                trailing_stop=(sl_price if sim_inst == "OPT" else s1.trailing_stop),
                                current_stop=(sl_price if sim_inst == "OPT" else s1.trailing_stop),
                                lots=hist_lots, lot_size=lot_size, grade=display_grade, confidence=hist_conf,
                                inst_type=sim_inst, option_type=sim_option_type, atm_strike=atm_strike,
                                rr_ratio=hist_rr, target=hist_target,
                                status="CLOSED", exit_price=exit_premium, exit_time=ex_time,
                                pnl=net_pnl, charges=sim_charges, exit_reason=hist_exit_reason,
                                instrument_multiplier=sim_multiplier, entry_spot=s1.price,
                            )
                            if self.mode == "HISTORICAL":
                                self.trades.closed_trades.append(t)
                            else:
                                logger.info(
                                    f"Skipping simulated backfill trade {hist_id} in live mode; "
                                    "live dashboard ledger only accepts real post-gate session rows."
                                )
                            if s2 is None:
                                logger.debug(f"ðŸ“Š Synthetic EOD close: {hist_id} | PnL: â‚¹{net_pnl:,.0f}")

                        # â• â• â•  STATE RECOVERY FOR TODAY'S LIVE SESSION â• â• â•
                        for backfill_tf in ["5min", "15min"]:
                            res = mtf_result.results_5min if backfill_tf == "5min" else mtf_result.results_15min
                            if res is None:
                                continue
                            tf_sigs = res.get("signals", [])
                            if not tf_sigs:
                                continue
                            sorted_sigs = sorted([s for s in tf_sigs if s.instrument == instrument and s.timestamp.date() >= data_cutoff], key=get_naive_ts)
                            if sorted_sigs:
                                last_s = sorted_sigs[-1]
                                today = datetime.now(IST).date()

                # Sizing estimation for Options
                est_premium = spot * 0.012
                lot_cost = est_premium * lot_size

                # User-defined: 1 Lakh position capital allocation per trade
                pos_cap_limit = 100000.0
                cap_based_lots = int(pos_cap_limit / max(1, lot_cost))
                user_target = self.user_lots.get(instrument, 1)

                # Take whichever is lower to protect margin, but ensure at least 1 lot
                actual_lots = max(1, min(user_target, cap_based_lots))

                # Options: Minimum of Technical Stop and Premium-based Hard Cap
                max_premium_sl_pts = est_premium * (self.options_sl_pct / 100.0)
                # Convert premium stop to index points using the instrument multiplier (delta)
                hard_cap_index_sl = max_premium_sl_pts / max(0.1, abs(instrument_multiplier))

                import config.settings as conf_settings
                if getattr(conf_settings.get_settings(), "sl_mode", "NATURAL") == "NATURAL":
                    index_stop_distance = best_sig.stop_distance
                else:
                    index_stop_distance = min(best_sig.stop_distance, hard_cap_index_sl)

            # â• â•  RR and Direction â• â• 
            # Dynamic RR based on confidence: 60% conf -> 1.70 RR, 80% conf -> 2.10 RR
            final_rr = max(1.10, round(0.5 + (best_conf * 2.0), 2))
            direction = "LONG" if best_sig.signal_type == "BUY" else "SHORT"

            # Target/Stop calculation based on Index Spot
            if direction == "LONG":
                entry_stop = spot - index_stop_distance
                target = spot + (index_stop_distance * final_rr)
            else: # SHORT
                entry_stop = spot + index_stop_distance
                target = spot - (index_stop_distance * final_rr)

            # â• â•  Order Resolution â• â• 
            trading_symbol = ""
            symbol_token = ""
            if inst_type == "FUT":
                info = self.market_info.get(instrument, {})
                trading_symbol = info.get("current_fut", "")
                symbol_token = info.get("current_fut_token", "")
            else:
                opt_info = self.data.get_option_token(instrument, strike_local, option_type_local)
                if opt_info:
                    trading_symbol = opt_info['symbol']
                    symbol_token = opt_info['token']

            if symbol_token:
                # â• â•  INTELLIGENT STRIKE SCORING (ALPHA SCORE) â• â• 
                # This determines the winner if BOTH mode is selected.
                # Factors: Delta (Speed), OI (Support), Volume (Liquidity)
                strike_alpha = (abs(instrument_multiplier) * 0.4) # Delta 40%

                # Fetch OI/Vol for this specific strike if available
                # (Falling back to general intel if strike-specific not found)
                oi_score = intel_result.get("oi", {}).get("score", 50) / 100.0
                vol_score = intel_result.get("volume", {}).get("score", 50) / 100.0

                # ITM Bonus: Higher delta usually leads to 'best gains'
                itm_bonus = 0.15 if (strike_local == itm_strike_val and inst_type == "OPT") else 0.0

                final_strike_score = strike_alpha + (oi_score * 0.3) + (vol_score * 0.3) + itm_bonus

                # â• â•  LIVE PREMIUM FETCH (Fixes BUG-02) â• â• 
                live_trade_price = spot
                if inst_type == "OPT":
                    live_premium = self.data.get_ltp(cfg.get("option_exchange", "NFO"), trading_symbol, symbol_token)
                    live_trade_price = live_premium if live_premium and live_premium > 0 else (spot * 0.012)

                results_list.append(TradeCandidate(
                    instrument=instrument, direction=direction, price=live_trade_price,
                    stop=entry_stop, target=target, lots=actual_lots, lot_size=lot_size, grade=best_grade,
                    confidence=best_conf, timeframe=best_tf, inst_type=inst_type, option_type=option_type_local,
                    atm_strike=strike_local, multiplier=instrument_multiplier, trading_symbol=trading_symbol,
                    symbol_token=symbol_token, rr=round(final_rr, 2), score=final_strike_score,
                    reasons=best_reasons, signal_timestamp=best_sig.timestamp
                ))

        if not results_list:
            return []

        # â• â• â•  COMPETITIVE WINNER SELECTION â• â• â• 
        # If BOTH mode, pick the strike with highest Alpha Score.
        # Otherwise, the list already contains only 1 entry.
        results_list.sort(key=lambda x: x.score, reverse=True)
        winner = results_list[0]

        if strike_selection == "BOTH" and len(results_list) > 1:
            other = results_list[1]
            self.log_event(f"ðŸ † Competitive Face-off: {winner.atm_strike} (Score: {winner.score:.2f}) BEATS {other.atm_strike}", "trade")
            logger.info(f"ðŸ † Competitive Strike Face-off: {winner.atm_strike} (Score: {winner.score:.2f}) BEAT {other.atm_strike} (Score: {other.score:.2f})")
        else:
            self.log_event(f"ðŸŽ¯ Selected Strike: {winner.atm_strike} ({winner.inst_type})", "trade")

        self.log_event(f"ðŸŽ¯ Potential {winner.direction} Signal for {winner.instrument} ({winner.timeframe}) | Score: {winner.score:.2f}", "trade")
        logger.info(
            f"ðŸ“‹ Candidate Resolution: {best_sig.signal_type} -> {winner.direction} {winner.inst_type} "
            f"{winner.option_type} {winner.trading_symbol or winner.instrument} "
            f"@ â‚¹{winner.price:.2f} | Strike: {winner.atm_strike} | RR: {winner.rr}"
        )
        return [winner]

    async def _coordinate_and_execute(self, candidates: List[TradeCandidate], is_warmup: bool = False):
        """
        Correlation Filter:
        If multiple indices have same direction, only take the 1-2 best.
        Only take 3rd if conviction is extraordinarily high (>90%).
        """
        accepted_candidates: List[TradeCandidate] = []

        if self.mode != "HISTORICAL" and not is_warmup:
            settings = get_settings()
            session_now = self._session_squareoff_clock()
            if session_now.time() >= dtime(15, 20):
                if self.scan_count % 25 == 0:
                    logger.info("Execution Gate: hard square-off window active; no fresh manual/live entries after 15:18 IST.")
                self._pending_live_signals.clear()
                return accepted_candidates

            if session_now.time() >= dtime(15, 15):
                if self.scan_count % 25 == 0:
                    logger.info("Execution Gate: no fresh entries after 15:15 IST.")
                self._pending_live_signals.clear()
                return accepted_candidates
        # ── Live Anti-Repaint Stabilization Buffer ──
        if self.mode != "HISTORICAL" and not is_warmup:
            now = datetime.now()
            matured_candidates = []

            # Step 1: Add new candidates to the buffer if not already present
            for c in candidates:
                # Timeframe-Specific entry gate check
                c_time = session_now.time()
                limit_time = dtime(15, 0) if c.timeframe == "15min" else dtime(15, 15)
                if c_time >= limit_time:
                    logger.info(f"🚫 [LIVE GATE] Skipping buffering for {c.instrument} {c.timeframe} {c.direction} at {c_time} due to entry policy ({limit_time})")
                    continue

                key = f"{c.instrument}_{c.timeframe}_{c.direction}"
                if key not in self._pending_live_signals:
                    # Find corresponding signal from cached results to get its exact timestamp
                    sig_timestamp = None
                    cached_key = f"{c.instrument}_{c.timeframe}"
                    cached_res = self._cached_results.get(cached_key)
                    if cached_res and cached_res.get("signals"):
                        target_type = "BUY" if c.direction == "LONG" else "SELL"
                        matching_sigs = [s for s in cached_res["signals"] if s.signal_type == target_type]
                        if matching_sigs:
                            sig_timestamp = matching_sigs[-1].timestamp

                    self._pending_live_signals[key] = {
                        "timestamp": now,
                        "candidate": c,
                        "sig_timestamp": sig_timestamp
                    }
                    logger.info(f"â³ [STABILIZATION] Buffering {c.direction} signal for {c.instrument} {c.timeframe} for 15s...")
                    self.log_event(f"â³ Buffering {c.direction} signal for {c.instrument} {c.timeframe}...", "system")

            # Step 2: Check which buffered signals have matured (held for >= 15 seconds)
            keys_to_remove = []
            for key, data in list(self._pending_live_signals.items()):
                c = data["candidate"]
                sig_timestamp = data.get("sig_timestamp")

                # A signal is still present if it remains in the latest cached results of the engine
                still_present = False
                cached_key = f"{c.instrument}_{c.timeframe}"
                cached_res = self._cached_results.get(cached_key)
                if cached_res and cached_res.get("signals"):
                    target_type = "BUY" if c.direction == "LONG" else "SELL"
                    for s in cached_res["signals"]:
                        if s.signal_type == target_type:
                            if sig_timestamp:
                                if s.timestamp == sig_timestamp:
                                    still_present = True
                                    break
                            else:
                                still_present = True
                                break

                if not still_present:
                    logger.warning(f"ðŸ’¨ [STABILIZATION] Signal for {key} repainted/disappeared. Discarding.")
                    self.log_event(f"ðŸ’¨ Signal for {key} repainted/disappeared", "warning")
                    keys_to_remove.append(key)
                    dedup_key = f"{c.instrument}_{c.timeframe}"
                    self._last_live_signal_candle_time.pop(dedup_key, None)
                    continue

                elapsed = (now - data["timestamp"]).total_seconds()
                max_pending_seconds = 45.0
                if elapsed > max_pending_seconds:
                    logger.warning(
                        f"[STABILIZATION] Signal for {key} expired after {elapsed:.1f}s "
                        "without a stable scan cadence. Discarding stale candidate."
                    )
                    self.log_event(f"Stale buffered signal discarded: {key}", "warning")
                    keys_to_remove.append(key)
                    dedup_key = f"{c.instrument}_{c.timeframe}"
                    self._last_live_signal_candle_time.pop(dedup_key, None)
                    continue

                if elapsed >= 15.0:
                    c_time = session_now.time()
                    limit_time = dtime(15, 0) if c.timeframe == "15min" else dtime(15, 15)
                    if c_time >= limit_time:
                        logger.info(f"🚫 [LIVE GATE] Discarding matured signal for {key} due to entry policy ({limit_time})")
                        keys_to_remove.append(key)
                        dedup_key = f"{c.instrument}_{c.timeframe}"
                        self._last_live_signal_candle_time.pop(dedup_key, None)
                        continue

                    logger.success(f"✅ [STABILIZATION] Signal for {key} matured after {elapsed:.1f}s. Proceeding to execute.")
                    matured_candidates.append(c)
                    keys_to_remove.append(key)
                    if sig_timestamp:
                        dedup_key = f"{c.instrument}_{c.timeframe}"
                        self._last_signal_time[dedup_key] = max(self._last_signal_time.get(dedup_key, datetime.min), sig_timestamp)

            for key in keys_to_remove:
                self._pending_live_signals.pop(key, None)

            candidates = matured_candidates
            if not candidates:
                return accepted_candidates

        # â”€â”€ Institutional Execution Gate â”€â”€
        if not self.data.is_market_open() and not is_warmup:
            # Don't place new trades if market is closed (except in Historical mode)
            if self.mode != "HISTORICAL":
                if self.scan_count % 100 == 0:
                    logger.debug("â³ Execution Gate: Market is closed. Analysis only.")
                return accepted_candidates

        # Group candidates by direction
        by_dir = {"LONG": [], "SHORT": []}
        for c in candidates:
            by_dir[c.direction].append(c)

        for direction, signals in by_dir.items():
            if not signals: continue

            # Sort by Score/Confidence
            signals.sort(key=lambda x: x.confidence, reverse=True)
            if getattr(get_settings(), "ut_concurrency_guard", True) and len(signals) >= 3:
                if not all(self._is_aplus_setup(sig) for sig in signals):
                    skipped = signals[2:]
                    signals = signals[:2]
                    for sig in skipped:
                        logger.info(
                            f"Concurrency Guard: batch-blocked {sig.instrument} {direction}; "
                            "only best 2 correlated same-direction index signals are allowed."
                        )
            open_count = len(self.trades.open_trades)

            # â•â•â• CROSS-INSTRUMENT CORRELATION GUARD â•â•â•
            # Rules:
            # 1. Max 2 index trades concurrently.
            # 2. Max 3 ONLY if all are A+ setups.
            # 3. Avoid same-direction trades unless setup is very strong.

            for i, sig in enumerate(signals):
                total_open = len(self.trades.open_trades)

                # â”€â”€ Grade Preference Filter â”€â”€
                settings = get_settings()
                grade_pref = getattr(settings, "signal_grade_preference", "auto")

                grade_hierarchy = {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4}
                base_grade = sig.grade.split()[0] if isinstance(sig.grade, str) else "C"
                sig_rank = grade_hierarchy.get(base_grade, 0)

                # Dynamic Regime-Aware minimum grade preference
                current_regime = self.latest_regimes.get(sig.instrument, "UNKNOWN")
                is_choppy = current_regime in {"CHOPPY", "SIDEWAYS", "MEAN_REVERTING", "RANGING", "VOLATILE", "UNKNOWN"}

                if grade_pref == "B":
                    # Futures allow B (Rank 1), Options require B+ (Rank 2)
                    min_rank = 1 if sig.inst_type == "FUT" else 2
                elif grade_pref == "B+":
                    min_rank = 2
                elif grade_pref == "A":
                    min_rank = 3
                else: # auto
                    # Dynamic upgrade to A/A+ under chops, baseline is B+
                    min_rank = 3 if is_choppy else 2

                if sig_rank < min_rank and "Recovered" not in sig.grade and "EOD" not in sig.grade:
                    reason_str = ", ".join(sig.reasons) if sig.reasons else "Low confidence"
                    rejection_reason = f"Low Grade: {sig.grade}" if sig_rank < 1 else f"Below Grade Pref ({grade_pref})"
                    self.log_event(f"ðŸš« Signal Rejected: {sig.instrument} {sig.direction} ({sig.grade}) - {rejection_reason} | Reasons: {reason_str}", "trade")
                    logger.info(f"ðŸš« Signal Rejected: {sig.instrument} {sig.direction} ({sig.grade}) - {rejection_reason} | Reasons: {reason_str}")
                    continue

                if not self._passes_live_trade_ready_gate(sig):
                    continue

                # Per-Instrument Safety (Cross-TF Complementary Rule)
                if not self._prepare_candidate_for_concurrency(sig):
                    continue

                # Cross-index correlation guard: max best 2 same-direction index trades unless all are A+.
                can_take = self._passes_correlated_index_guard(sig, direction)

                if can_take:
                    # â”€â”€â”€ Manual Signal Gate â”€â”€â”€
                    if not self.auto_mode and not is_warmup:
                        self._close_superseded_session_entries([sig])
                        self._latest_trade_candidates[sig.instrument] = [sig]
                        setattr(sig, "accepted_by_gate", True)
                        self._remember_trade_candidates([sig])
                        accepted_candidates.append(sig)
                        if i == 0: # Only notify for the primary candidate
                            logger.info(f"ðŸ“¢ MANUAL SIGNAL: {sig.direction} {sig.instrument} @ {sig.price} (Auto Mode: Manual)")
                            self.log_event(f"ðŸŽ¯ Manual {sig.direction} Signal: {sig.instrument} @ {sig.price:.2f}", "trade")
                            self._notify(f"ðŸŽ¯ MANUAL SIGNAL: {sig.direction} {sig.instrument} @ {sig.price:.2f}. Auto-execution disabled.", "info")
                        continue # Don't take the trade in manual mode

                    # TradeManager.open_trade is the single broker execution path.
                    actual_spot = self.candles.get_latest_price(sig.instrument)
                    
                    # --- Premium-Led / Dual-Chart Sync Integration ---
                    exec_price = sig.price
                    exec_stop = sig.stop
                    exec_target = sig.target
                    spot_stop = sig.stop
                    spot_target = sig.target
                    
                    if getattr(sig, "inst_type", "FUT") == "OPT":
                        real_premium = self._get_live_premium_cached(sig)
                        if real_premium and real_premium > 0:
                            exec_price = real_premium
                            opt_sl_pct = getattr(self.trades, "options_sl_pct", 15.0) / 100.0
                            # For options, we only ever BUY (CE or PE), so the premium must drop to hit SL
                            exec_stop = exec_price * (1 - opt_sl_pct)
                            
                            target_move = abs(sig.target - sig.price) * getattr(sig, "multiplier", 1.0)
                            exec_target = exec_price + target_move if target_move > 0 else 0.0

                    trade = self.trades.open_trade(
                        instrument=sig.instrument, timeframe=sig.timeframe, direction=sig.direction,
                        price=exec_price, trailing_stop=exec_stop,
                        lots=sig.lots, lot_size=sig.lot_size, grade=sig.grade,
                        atm_strike=sig.atm_strike, option_type=sig.option_type,
                        target=exec_target,
                        rr_ratio=sig.rr,
                        confidence=sig.confidence,
                        instrument_multiplier=sig.multiplier,
                        trading_symbol=sig.trading_symbol,
                        symbol_token=sig.symbol_token,
                        inst_type=sig.inst_type,
                        exec_type="A",
                        entry_spot=actual_spot,
                        spot_stop=spot_stop,
                        spot_target=spot_target,
                        signal_time=getattr(sig, "signal_timestamp", None)
                    )

                    if trade:
                        trade.is_live = (self.mode == "REAL")

                    if trade:
                        self._close_superseded_session_entries([sig])
                        self._latest_trade_candidates[sig.instrument] = [sig]
                        setattr(sig, "accepted_by_gate", True)
                        self._remember_trade_candidates([sig])
                        accepted_candidates.append(sig)
                        open_count += 1

                        self._notify(
                            f"{'ðŸŸ¢' if sig.direction == 'LONG' else 'ðŸ”´'} {sig.direction} {sig.instrument} "
                            f"@ {sig.price:.2f} | Conf: {sig.confidence:.0%} | TF: {sig.timeframe}",
                            "buy" if sig.direction == "LONG" else "sell"
                        )
                else:
                    logger.info(f"â³ Waitlisting correlated trade: {sig.instrument} {sig.direction} (Current {direction} Exposure: {open_count})")

    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
    # CHART BUILDER â€” uses CACHED results (zero re-processing)
    # â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
        return accepted_candidates

    def _build_chart_from_cache(self, instrument: str, force: bool = False) -> Dict:
        chart = {}
        active_tf = getattr(self, "active_chart_tf", "5min")
        active_inst = getattr(self, "active_chart_instrument", "NIFTY")

        if getattr(self, "_chart_metadata_cache", None) is None:
            self._chart_metadata_cache = {}

        for tf in ["1min", "5min", "15min"]:
            key = f"{instrument}_{tf}"

            # Optimization: Only build chart for the active instrument and timeframe!
            if instrument != active_inst or tf != active_tf:
                continue

            result = self._cached_results.get(key)
            df = self.candles.get_candles(instrument, tf)

            if df is None or len(df) == 0 or result is None:
                continue

            last_row = df.iloc[-1]
            last_ts = df.index[-1]
            sig_count = len(result.get("signals", []))
            open_count = len(self.trades.open_trades) if hasattr(self, "trades") else 0
            
            fingerprint = (len(df), last_ts, float(last_row['close']), float(last_row['volume']) if 'volume' in last_row else 0, sig_count, open_count)
            
            cache_attr = f"chart_cache_{instrument}_{tf}"
            if not force and self._chart_metadata_cache.get(key) == fingerprint and hasattr(self, cache_attr):
                chart[tf] = getattr(self, cache_attr).get("data", {})
                continue
            
            self._chart_metadata_cache[key] = fingerprint

            volume_sum = float(df["volume"].sum()) if "volume" in df.columns else 0.0
            if len(df) <= 5 or volume_sum <= 0:
                disk_path = Path("data_store") / "candles" / f"{instrument}_{tf}.csv"
                if disk_path.exists():
                    try:
                        disk_df = pd.read_csv(disk_path, parse_dates=["timestamp"])
                        disk_volume_sum = float(disk_df["volume"].sum()) if "volume" in disk_df.columns else 0.0
                        if not disk_df.empty and (len(disk_df) > len(df) or disk_volume_sum > volume_sum):
                            disk_df = disk_df.set_index("timestamp").sort_index()
                            disk_df.columns = [c.lower() for c in disk_df.columns]
                            df = disk_df
                            self.candles.update_candles(instrument, disk_df, tf)
                    except Exception as e:
                        logger.debug(f"Chart disk hydration skipped for {key}: {e}")

            # Optimization: REAL mode only needs a compact live chart window; historical/backtest keeps depth.
            chart_limit = 300 if self.mode == "REAL" else 1000
            df = df.tail(chart_limit)
            if "trailing_stop" in result:
                result["trailing_stop"] = result["trailing_stop"][-chart_limit:]
            if "trailing_stop_colors" in result:
                result["trailing_stop_colors"] = result["trailing_stop_colors"][-chart_limit:]

            def chart_epoch_seconds(value) -> int:
                ts = pd.Timestamp(value)
                if ts.tzinfo is None:
                    ts = ts.tz_localize(IST)
                else:
                    ts = ts.tz_convert(IST)
                return int(ts.timestamp())

            # UNIX timestamp conversion for chart display.
            # Candle timestamps are stored as naive IST; convert them to true epoch seconds.
            if isinstance(df.index, pd.DatetimeIndex):
                timestamps = [chart_epoch_seconds(ts) for ts in df.index]
            else:
                timestamps = (df.index.astype('int64') // 10**9).tolist() if hasattr(df.index, 'astype') else list(range(len(df)))
            opens = df['open'].round(2).tolist()
            highs = df['high'].round(2).tolist()
            lows = df['low'].round(2).tolist()
            closes = df['close'].round(2).tolist()
            volumes = df['volume'].astype(int).tolist()

            candles_list = [
                {"time": int(t), "open": float(o), "high": float(h), "low": float(l), "close": float(c), "volume": int(v)}
                for t, o, h, l, c, v in zip(timestamps, opens, highs, lows, closes, volumes)
            ]

            ts_data = result.get("trailing_stop", [])
            ts_colors = result.get("trailing_stop_colors", [])
            logger.info(f"ðŸ“ˆ Chart Build: {instrument} {tf} -> Candles: {len(candles_list)}, TS Points: {len(ts_data)}")
            # Build ts_line efficiently without per-element Python loop
            n = len(candles_list)
            last_val = ts_data[-1] if ts_data else 0
            last_color = ts_colors[-1] if ts_colors else "gray"
            padded_vals = list(ts_data) + [last_val] * max(0, n - len(ts_data))
            padded_colors = list(ts_colors) + [last_color] * max(0, n - len(ts_colors))
            ts_line = [
                {"time": candles_list[i]["time"], "value": round(float(padded_vals[i]), 2), "color": padded_colors[i]}
                for i in range(n)
            ]

            # Signal markers
            markers = []

            # Use signal timestamp directly instead of bar_index (Fixes markers disappearing on sliced data)
            for sig in result.get("signals", []):
                try:
                    ts_obj = sig.timestamp
                    sig_time = chart_epoch_seconds(ts_obj)
                except Exception:
                    sig_time = int(sig.timestamp.timestamp())
                risk_pts = round(sig.stop_distance, 2)

                if sig.signal_type == "BUY":
                    tgt = round(sig.price + sig.stop_distance * 1.5, 2)
                    label = f"BUY @ {sig.price:.0f}"
                    markers.append({
                        "time": sig_time,
                        "position": "belowBar", "color": "#22c55e",
                        "shape": "arrowUp", "text": label,
                    })
                else:
                    tgt = round(sig.price - sig.stop_distance * 1.5, 2)
                    label = f"SELL @ {sig.price:.0f}"
                    markers.append({
                        "time": sig_time,
                        "position": "aboveBar", "color": "#ef4444",
                        "shape": "arrowDown", "text": label,
                    })

            # Truncate lists safely to max 1000 candles
            sliced_candles = candles_list[-1000:]
            sliced_ts = ts_line[-1000:]

            # Filter markers to only those that fall within our VISIBLE candle data range
            if sliced_candles:
                first_ts = sliced_candles[0]["time"]
                markers = [m for m in markers if m["time"] >= first_ts]

            # Optimization: intel_history is not used by the dashboard chart, so keep it empty to save bandwidth
            res = {
                "candles": sliced_candles,
                "trailing_stop": sliced_ts,
                "markers": markers[-100:],
                "state": result.get("state", {}),
                "intel_history": {}
            }
            if len(res["candles"]) > 0:
                logger.info(f"Sending chart data for {key}: first_ts={res['candles'][0]['time']}, last_ts={res['candles'][-1]['time']}")
            chart[tf] = res
            # Save to optimization cache (required for get_latest_results)
            setattr(self, f"chart_cache_{instrument}_{tf}", {"data": res})
        return chart

    def get_latest_results(self, include_full_charts: bool = False) -> Dict:
        """Return the latest dashboard payload, optionally rehydrating full chart history."""
        if not include_full_charts or not self.latest_results:
            return self.latest_results

        active_inst = getattr(self, "active_chart_instrument", "NIFTY")
        active_tf = getattr(self, "active_chart_tf", "5min")

        snapshot = dict(self.latest_results)
        instruments = {}
        for name, ui_data in snapshot.get("instruments", {}).items():
            if not isinstance(ui_data, dict):
                instruments[name] = ui_data
                continue

            hydrated_ui = dict(ui_data)
            chart = dict(hydrated_ui.get("chart", {}))
            for tf in ["1min", "5min", "15min"]:
                if name == active_inst and tf == active_tf:
                    cached = getattr(self, f"chart_cache_{name}_{tf}", None)
                    if cached and cached.get("data"):
                        chart[tf] = cached["data"]
                else:
                    chart[tf] = {
                        "candles": [],
                        "trailing_stop": [],
                        "markers": [],
                        "state": {},
                        "intel_history": {}
                    }
            hydrated_ui["chart"] = chart
            instruments[name] = hydrated_ui

        snapshot["instruments"] = instruments
        return snapshot

    def _close_and_record(self, tid: str, exit_price: float, reason: str, exit_time: Optional[datetime] = None):
        """Wraps trade closure to track over-trading metrics"""
        trade = self.trades.open_trades.get(tid)
        if not trade: return

        instrument = trade.instrument
        # Close the trade
        self.trades.close_trade(tid, exit_price, reason, exit_time=exit_time)

        # Track outcome
        closed_trade = next((t for t in self.trades.closed_trades if t.id == tid), None)
        if closed_trade:
            # Increment daily trades
            self._trades_today[instrument] = self._trades_today.get(instrument, 0) + 1

            # Track losses (negative PnL)
            if closed_trade.pnl < 0:
                self._losses_today[instrument] = self._losses_today.get(instrument, 0) + 1
            else:
                # Reset consecutive losses on a win
                self._losses_today[instrument] = 0

            # Record exit time for cooldown
            self._last_exit_time[instrument] = exit_time if exit_time else datetime.now()
            logger.info(f"ðŸ“Š {instrument} Tracker: Daily Trades={self._trades_today[instrument]}, Consecutive Losses={self._losses_today[instrument]}")

    def _update_active_trades(self):
        for tid, trade in list(self.trades.open_trades.items()):
            price = self.candles.get_latest_price(trade.instrument)
            if price is None or price <= 0: continue
            real_premium = None
            if getattr(trade, "inst_type", "FUT") == "OPT":
                real_premium = self._get_live_premium_cached(trade)

            regime = self.latest_regimes.get(trade.instrument, "UNKNOWN")

            # â”€â”€ Intelligence-Based Early Exit (Patch) â”€â”€
            from config.settings import get_settings
            settings = get_settings()
            if getattr(settings, "ut_intel_early_exit", False):
                intel_result = getattr(self, "_intel_cache", {}).get(trade.instrument)
                if intel_result:
                    agg = intel_result.get("aggregate", {})
                    score = agg.get("score", 0.0)

                    if trade.direction == "LONG" and score < -0.4:
                        logger.info(f"ðŸš¨ Intelligence Early Exit for {trade.instrument} LONG: Score={score}")
                        self.trades.close_trade(tid, trade.current_price, "INTEL_FLIP")
                        continue
                    elif trade.direction == "SHORT" and score > 0.4:
                        logger.info(f"ðŸš¨ Intelligence Early Exit for {trade.instrument} SHORT: Score={score}")
                        self.trades.close_trade(tid, trade.current_price, "INTEL_FLIP")
                        continue

            # â”€â”€ Smart Trailing Patch â”€â”€
            if getattr(settings, "ut_smart_trailing", False):
                key_tf = f"{trade.instrument}_{trade.timeframe}"
            else:
                key_tf = f"{trade.instrument}_1min"

            engine_tf = self.mtf.engines.get(key_tf)
            if not engine_tf: continue

            state_tf = engine_tf.get_state(key_tf)
            raw_ts = state_tf.trailing_stop
            new_stop = trade.current_stop

            # â”€â”€ Adaptive Logic based on Market Regime â”€â”€
            # 1. TRENDING (Normal ATR Trailing)
            if regime in ["TRENDING", "STRONG_TREND"]:
                if raw_ts > 0:
                    # Only move stop in favorable direction
                    if trade.direction == "LONG":
                        new_stop = max(new_stop, raw_ts)
                    else:
                        new_stop = min(new_stop, raw_ts)

            # 2. VOLATILE / REVERSAL / CHOPPY (Tight Trailing)
            elif regime in ["VOLATILE", "CHOPPY", "MEAN_REVERTING"]:
                df_1m = self.candles.get_candles(trade.instrument, "1min")
                if df_1m is not None and len(df_1m) > 2:
                    last_low = df_1m['low'].iloc[-2]
                    last_high = df_1m['high'].iloc[-2]

                    if trade.direction == "LONG":
                        # Trail by last candle's low (Tight)
                        new_stop = max(new_stop, last_low)
                    else:
                        # Trail by last candle's high (Tight)
                        new_stop = min(new_stop, last_high)

            # 3. PROFIT PROTECTOR (Lock-in at 1:1 RR)
            # Use entry_spot for options so we compare Spot with Spot
            ref_entry = trade.entry_spot if trade.inst_type == "OPT" else trade.entry_price
            risk_dist = abs(ref_entry - trade.trailing_stop)
            current_profit_pts = (price - ref_entry) if trade.direction == "LONG" else (ref_entry - price)

            if current_profit_pts > risk_dist:
                buffer = ref_entry * 0.0005 # Tighten buffer (0.05%)
                old_stop = new_stop
                if trade.direction == "LONG":
                    new_stop = max(new_stop, ref_entry + buffer)
                else:
                    new_stop = min(new_stop, ref_entry - buffer)

                if new_stop != old_stop:
                    self.log_event(f"ðŸ›¡ï¸ Profit Protector: Locking BE for {trade.instrument}", "trade")

            # â”€â”€ 4. TARGET TRAILING & RUNNER MODE (T1 -> T2 -> T3) â”€â”€
            if trade.target > 0 or getattr(trade, 'runner_mode', False):
                # Use entry_spot for options so we compare Spot with Spot
                ref_entry = trade.entry_spot if trade.inst_type == "OPT" else trade.entry_price

                # Check if T1 was already hit and we are in runner mode
                is_runner = getattr(trade, 'runner_mode', False)

                if not is_runner and trade.target > 0:
                    # Logic to activate Runner Mode at T1 (or 90% of T1)
                    target_distance = abs(trade.target - ref_entry)
                    threshold_t1 = target_distance * 0.95 # Activate slightly before T1 for safety
                    current_move = abs(price - ref_entry)

                    # Verify we are moving in the correct direction (towards target)
                    is_correct_direction = (trade.direction == "LONG" and price > ref_entry) or \
                                         (trade.direction == "SHORT" and price < ref_entry)

                    if is_correct_direction and current_move >= threshold_t1:
                        trade.runner_mode = True
                        trade.target = 0 # Remove hard target to let it run to T2/T3
                        self.log_event(f"ðŸš€ TARGET 1 HIT for {trade.instrument}. Entering Institutional Runner Mode (90% Gain Lock).", "trade")
                        logger.success(f"ðŸ”¥ Runner Mode Active: {trade.instrument}. Hard target removed, locking 90% gains.")

                # â”€â”€ Runner Mode Trailing (90% Gain Lock) â”€â”€
                if getattr(trade, 'runner_mode', False):
                    # Calculate current gain in points
                    # For options, we use the spot move * multiplier to estimate gain pts if real premium is stale
                    if trade.inst_type == "OPT":
                        prem = real_premium if (real_premium and real_premium > 0) else trade.current_price
                        current_gain_pts = max(0, (prem - trade.entry_price))
                    else:
                        current_gain_pts = max(0, abs(price - trade.entry_price))

                    # Lock 90% of current gain points
                    locked_gain_pts = current_gain_pts * 0.90

                    if trade.inst_type == "OPT":
                        # Convert locked gain points back to spot stop for the trade manager
                        multiplier = abs(trade.instrument_multiplier) or 0.5
                        # Locked Price = Entry Price + Locked Gain
                        locked_price_prem = trade.entry_price + locked_gain_pts
                        # Convert to spot: Spot = Entry Spot + (Gain / Multiplier)
                        spot_gain = locked_gain_pts / multiplier
                        new_runner_stop = (trade.entry_spot + spot_gain) if trade.direction == "LONG" else (trade.entry_spot - spot_gain)
                    else:
                        new_runner_stop = (trade.entry_price + locked_gain_pts) if trade.direction == "LONG" else (trade.entry_price - locked_gain_pts)

                    # Update stop if the new locked gain is higher (trailing)
                    if trade.direction == "LONG":
                        new_stop = max(new_stop, new_runner_stop)
                    else:
                        new_stop = min(new_stop, new_runner_stop)

            # â”€â”€ 5. SMART PROFIT LOCK & REVERSAL GUARD â”€â”€
            # Calculate absolute PnL in Rupees for institutional exit rules
            current_pnl_rs = 0.0
            entry_val = trade.entry_price
            qty = getattr(trade, "quantity", getattr(trade, "qty", 0))
            if trade.inst_type == "OPT":
                prem = real_premium if (real_premium and real_premium > 0) else trade.current_price
                current_pnl_rs = (prem - entry_val) * qty
                current_gain_pct = (current_pnl_rs / (entry_val * qty)) * 100.0 if entry_val > 0 and qty > 0 else 0
            else:
                current_pnl_rs = (price - entry_val) * qty if trade.direction == "LONG" else (entry_val - price) * qty
                current_gain_pct = (current_pnl_rs / (entry_val * qty)) * 100.0 if entry_val > 0 and qty > 0 else 0

            # Update peak PnL in the trade object
            trade.peak_pnl = max(getattr(trade, 'peak_pnl', 0.0), current_pnl_rs)
            peak_gain_pct = (trade.peak_pnl / (entry_val * qty)) * 100.0 if entry_val > 0 and qty > 0 else 0
            latest_time = self.candles.get_max_timestamp()

            # Rule 1: High-Profit Protection (Peak >= Rs.1000)
            if trade.peak_pnl >= 1000.0:
                # If profit drops below 65% of peak OR falls below Rs.450 minimum floor
                if current_pnl_rs < (trade.peak_pnl * 0.65) or current_pnl_rs < 450.0:
                    logger.warning(f"ðŸš¨ Smart Profit Lock Hit for {trade.instrument}: Peak â‚¹{trade.peak_pnl:.0f}, Current â‚¹{current_pnl_rs:.0f}")
                    self._close_and_record(tid, real_premium if (trade.inst_type == "OPT" and real_premium and real_premium > 0) else price, "SMART_PROFIT_LOCK", exit_time=latest_time)
                    self.log_event(f"ðŸ’° Profit Locked: {trade.instrument} (Retention Guard Hit @ â‚¹{current_pnl_rs:.0f})", "trade")
                    continue

            # Rule 2: Low-Gain Protection (If gain was > 10% but reversed)
            # Ensures we exit before loss or BE with minimum gains
            if peak_gain_pct >= 10.0:
                # If gain drops below 40% of peak or falls near BE (+2% floor)
                if current_gain_pct < (peak_gain_pct * 0.40) or current_gain_pct < 2.0:
                    logger.warning(f"ðŸš¨ Low-Gain Protection Hit for {trade.instrument}: Peak {peak_gain_pct:.1f}%, Current {current_gain_pct:.1f}%")
                    self._close_and_record(tid, real_premium if (trade.inst_type == "OPT" and real_premium and real_premium > 0) else price, "LOW_GAIN_PROTECT", exit_time=latest_time)
                    continue

            # Rule 3: Major Win Protection (Peak >= Rs.3000)
            if trade.peak_pnl >= 3000.0:
                # Tighter trailing for major wins (75% retention)
                if current_pnl_rs < (trade.peak_pnl * 0.75):
                    logger.warning(f"ðŸ† Major Win Guard for {trade.instrument}: Peak â‚¹{trade.peak_pnl:.0f} -> Exit @ â‚¹{current_pnl_rs:.0f}")
                    self._close_and_record(tid, real_premium if (trade.inst_type == "OPT" and real_premium and real_premium > 0) else price, "MAJOR_WIN_GUARD", exit_time=latest_time)
                    continue

            # Rule 4: Stagnation Profit Lock (Profit >= Rs.300, held 15-20m, stalled)
            if trade.entry_time and current_pnl_rs >= 300.0:
                time_elapsed = (latest_time - trade.entry_time.replace(tzinfo=None)).total_seconds() / 60.0 if latest_time else 0
                # Exit if held > 15m and current profit is < 85% of peak (stalled)
                if time_elapsed >= 15.0 and current_pnl_rs < (trade.peak_pnl * 0.85):
                    logger.info(f"â³ Stagnation Exit: {trade.instrument} held {time_elapsed:.0f}m, PnL â‚¹{current_pnl_rs:.0f}")
                    self._close_and_record(tid, real_premium if (trade.inst_type == "OPT" and real_premium and real_premium > 0) else price, "STAGNATION_EXIT", exit_time=latest_time)
                    continue

            latest_time = self.candles.get_max_timestamp()

            # â”€â”€ Live Trade Preservation Patch â”€â”€
            if getattr(trade, 'is_live', False) and self.mode == "HISTORICAL":
                triggered = False
                reason = ""
                if trade.direction == "LONG":
                    if price <= new_stop:
                        triggered = True
                        reason = "TRAILING_STOP"
                    elif price >= trade.target and trade.target > 0:
                        triggered = True
                        reason = "TARGET_HIT"
                else:
                    if price >= new_stop:
                        triggered = True
                        reason = "TRAILING_STOP"
                    elif price <= trade.target and trade.target > 0:
                        triggered = True
                        reason = "TARGET_HIT"

                if triggered:
                    msg = f"ðŸš¨ ALERT: Live Trade {trade.instrument} requires EXIT ({reason}) at {price:.2f}!"
                    logger.warning(msg)
                    if self.on_notification:
                        self.on_notification(msg, "warning")

                # Update price and stop in memory so UI shows it, but do NOT close it!
                trade.current_price = price
                trade.current_stop = new_stop
                continue

            self.trades.update_trade(tid, price, new_stop, current_time=latest_time, real_premium=real_premium)
            if tid not in self.trades.open_trades:
                # Trade was closed by update_trade (SL or Target)
                # Find it in closed_trades to record metrics
                closed_trade = next((t for t in self.trades.closed_trades if t.id == tid), None)
                if closed_trade:
                    self._trades_today[trade.instrument] = self._trades_today.get(trade.instrument, 0) + 1
                    if closed_trade.pnl < 0:
                        self._losses_today[trade.instrument] = self._losses_today.get(trade.instrument, 0) + 1
                    else:
                        self._losses_today[trade.instrument] = 0
                    self._last_exit_time[trade.instrument] = latest_time if latest_time else datetime.now()
                    logger.info(f"ðŸ“Š {trade.instrument} Tracker (SL/TP): Daily Trades={self._trades_today[trade.instrument]}, Consecutive Losses={self._losses_today[trade.instrument]}")

                self.log_event(f"ðŸ›‘ Trade Closed: {trade.instrument} Stop Hit @ {price:.2f}", "trade")
                self._notify(f"ðŸ›‘ STOP HIT {trade.direction} {trade.instrument} @ {price:.2f}", "sell")



    def _notify(self, message, msg_type="info"):
        if self.on_notification:
            try: self.on_notification(message, msg_type)
            except: pass

    def stop(self):
        self.is_running = False
        logger.info("Scanner stopped")

    def update_fyers_token(self, auth_code: str) -> Dict[str, Any]:
        """Update Fyers token and restart history sync if successful"""
        result = self.data.update_fyers_token_from_auth_code(auth_code)
        if result.get("status") == "ok":
            # Force a full history fetch to use the new Fyers token immediately
            self.log_event("âœ… Fyers Token Updated: Restarting Data Sync", "success")
            asyncio.create_task(self._perform_full_recalculation())
        return result
