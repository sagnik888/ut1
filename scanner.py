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
import re
import csv
import ctypes
import os
import shutil
import subprocess
import threading
import pandas as pd
import numpy as np
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Dict, Optional, Callable, List, Tuple, Any
from loguru import logger
from data.sqlite_db import db

from config.settings import get_settings
from data.market_data import MarketDataProvider
from data.candle_builder import CandleBuilder
from data.warm_memory import WarmMemoryStore, dataframe_to_records, records_to_dataframe
from engine.multi_timeframe import MultiTimeframeEngine
from engine.signal_manager import SignalManager
from intelligence.intelligence_aggregator import IntelligenceAggregator
from intelligence.memory import IntelligenceMemory
from intelligence.snapshot_store import MarketIntelSnapshotStore
from trading.trade_manager import Trade, TradeManager, IST
from trading.performance_tracker import PerformanceTracker
from engine.risk_manager import RiskManager
from engine.signal_processor import SignalProcessor, TradeCandidate
from engine.recalculation_queue import RecalculationQueue
from engine.scanner_dashboard import ScannerDashboardCache
from engine.recalculation_worker import ProcessRecalculationWorker, candle_rows_from_manifest
from engine.settings_persistence import schedule_settings_save, settings_persistence_status
from engine.intelligence_score import normalize_intelligence_score
from intelligence.outcome_calibration import OutcomeCalibrationStore
from engine.notification_manager import get_notification_manager


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
    CORE_CORRELATED_INDICES = {"NIFTY", "BANKNIFTY", "SENSEX"}
    MIDCAP_CORRELATED_INDEX = "MIDCPNIFTY"

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
        self.trades.ui_logger = self.log_event
        self.perf = performance
        self.config = instruments_config
        self.on_update = on_update
        self.on_notification = on_notification

        # â• â•  SUBSCRIPTION MODEL FOR CHARTS â• â•
        # Default to NIFTY 5min to reduce initial payload size
        self.active_chart_instrument = "NIFTY"
        self.active_chart_tf = "5min"
        self.chart_stream_enabled = True

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
        self.user_lots: Dict[str, int] = {"NIFTY": 1, "BANKNIFTY": 1, "SENSEX": 1, "MIDCPNIFTY": 1}
        self.user_lots_fut: Dict[str, int] = {"NIFTY": 1, "BANKNIFTY": 1, "SENSEX": 1, "MIDCPNIFTY": 1}
        self.capital_fut = settings.capital_fut
        self.capital_opt = settings.capital_opt
        self.capital_total = getattr(settings, "capital_total", 500000.0)
        self.risk_fut_pct = settings.risk_fut_pct
        self.risk_opt_pct = settings.risk_opt_pct
        self.futures_sl_pct = settings.futures_sl_pct
        self.options_sl_pct = settings.options_sl_pct
        # Every restart begins with the latest session only. A larger window is
        # loaded only after the user explicitly changes the running setting.
        self.backtest_days = 1
        self.auto_mode = False
        self.inst_pref = getattr(settings, "inst_pref", "AUTO")
        self.max_trades_per_index = int(getattr(settings, "max_trades_per_index", 5))
        self.max_consecutive_losses = int(getattr(settings, "max_consecutive_losses", 3))
        self.index_cooldown_minutes = float(getattr(settings, "index_cooldown_minutes", 4.0))

        self.intel_memory = IntelligenceMemory()
        self.intel_snapshots = MarketIntelSnapshotStore()
        self.latest_results: Dict = {}
        self.dashboard_cache = ScannerDashboardCache()
        self.recalculation_queue = RecalculationQueue(
            self._perform_full_recalculation_impl,
            debounce_seconds=float(getattr(settings, "settings_recalculation_debounce_seconds", 8.0) or 8.0),
        )
        self.recalculation_worker = ProcessRecalculationWorker(
            timeout_seconds=float(getattr(settings, "recalculation_worker_timeout_seconds", 120.0) or 120.0)
        )
        self.outcome_calibration = OutcomeCalibrationStore()
        self._recalculation_status: Dict[str, Any] = {
            "status": "idle",
            "reason": "",
            "started_at": None,
            "finished_at": None,
            "worker": {},
        }
        self._last_signal_time: Dict[str, datetime] = {}
        self._last_signal_identity: Dict[str, str] = {}
        self._last_live_signal_candle_time: Dict[str, datetime] = {}
        self._live_signal_recheck_eligible: Dict[str, str] = {}
        self._live_signal_recheck_at: Dict[str, datetime] = {}
        self._last_data_fetch: Dict[str, float] = {}
        self._api_semaphore = asyncio.Semaphore(1) # Background history semaphore (serialized to prevent rate limits)
        self._ltp_semaphore = asyncio.Semaphore(5) # High-priority price semaphore (LTP)
        self._ltp_rate_limiter = TokenBucket(capacity=3, fill_rate=2.0)
        self._state_lock = asyncio.Lock()
        self._initial_setup_task = None
        self._initial_setup_completed = False
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
            circuit_breaker_slippage_bps=float(
                getattr(settings, "circuit_breaker_slippage_bps", 10.0) or 10.0
            ),
        )

        # ðŸ§  Initialize Signal Processor
        self.signal_processor = SignalProcessor(self)
        self.latest_regimes: Dict[str, str] = {} # For adaptive trailing
        self._intel_cache: Dict[str, Dict] = {}
        self._latest_trade_candidates: Dict[str, List[TradeCandidate]] = {}
        self._latest_exit_candidates: Dict[str, List[TradeCandidate]] = {}
        self._latest_filtered_candidates: Dict[str, List[TradeCandidate]] = {}
        self._diagnostics: Dict[str, Any] = {
            "rejects": {},
            "option_history": {"attempts": 0, "hits": 0, "misses": 0, "synthetic": 0, "fallback_to_fut": 0},
            "latency": {"last_ms": 0, "high_latency_events": 0},
            "stabilization": {
                "buffered": 0,
                "waiting_stability": 0,
                "waiting_candle_close": 0,
                "discarded_repaint": 0,
                "discarded_future": 0,
                "discarded_stale": 0,
                "matured": 0,
            },
            "timeouts": {"skipped_inflight": 0},
            "repaint_guard": {"checked": 0, "aborted": 0, "passed": 0},
            "exit_reasons": {},
            "instrument_selection": {"OPT": 0, "FUT": 0},
            "source_fallback": {"delayed_entry_blocked": 0},
        }
        self._system_perf_last_alert: Dict[str, float] = {}
        self._system_perf_snapshot: Dict[str, Any] = {}
        self._live_processing_futures: Dict[str, asyncio.Future] = {}
        self._live_processing_started: Dict[str, float] = {}
        self._live_inflight_skip_counts: Dict[str, int] = {}
        self._live_inflight_last_alert: Dict[str, float] = {}
        self._history_rate_limiter = TokenBucket(capacity=1, fill_rate=0.7)
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
                    
                    saved_entry_ts = getattr(entry_cand, "entry_timestamp", None)
                    if saved_entry_ts:
                        entry_time = (
                            saved_entry_ts
                            if isinstance(saved_entry_ts, datetime)
                            else datetime.fromisoformat(str(saved_entry_ts))
                        )
                    else:
                        entry_time = signal_ts
                    if entry_time.tzinfo is None:
                        entry_time = IST.localize(entry_time)
                    else:
                        entry_time = entry_time.astimezone(IST)
                    
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
                        pnl = self._candidate_net_pnl(exit_cand, exit_price) if exit_price > 0 else 0.0
                    
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
                        charges=(
                            float(get_settings().fut_cost)
                            if getattr(entry_cand, 'inst_type') == 'FUT'
                            else float(get_settings().opt_cost)
                        ),
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
        self._last_runtime_cache_prune = 0.0
        self._intel_save_inflight = False
        self.active_indices = settings.active_indices
        self.system_power = "ON"
        
        from data.data_manager import DataManager
        self.data_manager = DataManager(self.data)
        
        self.is_calculating = False
        self.is_warmup = False
        self.simulation_id = int(time.time())
        self._diagnostic_unique_events = set()
        self._diagnostics["simulation"] = {
            "id": self.simulation_id,
            "backtest_days": int(self.backtest_days),
            "started_at": datetime.now(IST).isoformat(),
        }
        self._daily_reset_done = False
        self._daily_maintenance_done = False
        self._morning_refresh_inflight = False
        self._morning_refresh_last_date = None
        self._pending_live_signals = {}

        # â• â• â•  CACHED PROCESS RESULTS (avoid double-processing) â• â• â•
        self._cached_results: Dict[str, Dict] = {}
        self.warm_memory = WarmMemoryStore()
        self._warm_memory_loaded = False
        self._recovery_checkpoint_count = 0
        self._last_recovery_checkpoint_at = None
        self._last_recovery_checkpoint_error = ""

        # â• â• â•  LOGS â• â• â•
        import collections
        self.activity_log = collections.deque(maxlen=2000)
        self._max_log_size = 50
        self.log_event("UT1 System Initialized", "system")

        # â”€â”€ Over-trading Guards (New) â”€â”€
        self._trades_today = {} # {instrument: count}
        self._losses_today = {} # {instrument: count}
        self._last_exit_time = {} # {instrument: timestamp}
        self._counted_entry_keys = set()
        self._last_reset_date = None
        self._session_rollover_date = (
            now_ist_init.date()
            if now_ist_init.time() >= dtime(9, 0)
            else (now_ist_init - timedelta(days=1)).date()
        )

        # â• â• â•  PRE-MARKET ANALYSIS (AngelOne Master Integration) â• â• â•
        from engine.expiry_manager import expiry_manager
        self.expiry = expiry_manager
        self.market_info = self.expiry.pre_market_check()
        self._restore_warm_memory()

    def _reset_daily_counters(self):
        """Resets daily trading counters at 09:15 AM IST"""
        today = datetime.now(IST).date()
        if getattr(self, "_last_reset_date", None) != today:
            self._trades_today = {}
            self._losses_today = {}
            self._last_exit_time = {}
            self._counted_entry_keys = set()
            self._last_reset_date = today
            closed_today = []
            for trade in list(getattr(self.trades, "closed_trades", []) or []):
                entry_time = getattr(trade, "entry_time", None)
                if entry_time and entry_time.date() == today:
                    instrument = str(getattr(trade, "instrument", "") or "").split()[0]
                    key = str(getattr(trade, "id", "") or "")
                    if instrument and key:
                        self._counted_entry_keys.add(key)
                        self._trades_today[instrument] = self._trades_today.get(instrument, 0) + 1
                    closed_today.append(trade)
            for trade in list(getattr(self.trades, "open_trades", {}).values()):
                entry_time = getattr(trade, "entry_time", None)
                if entry_time and entry_time.date() == today:
                    instrument = str(getattr(trade, "instrument", "") or "").split()[0]
                    key = str(getattr(trade, "id", "") or "")
                    if instrument and key and key not in self._counted_entry_keys:
                        self._counted_entry_keys.add(key)
                        self._trades_today[instrument] = self._trades_today.get(instrument, 0) + 1
            def exit_sort_key(row):
                exit_time = getattr(row, "exit_time", None)
                if not exit_time:
                    return 0.0
                if exit_time.tzinfo is None:
                    exit_time = IST.localize(exit_time)
                return exit_time.timestamp()

            for trade in sorted(closed_today, key=exit_sort_key):
                instrument = str(getattr(trade, "instrument", "") or "").split()[0]
                if float(getattr(trade, "pnl", 0.0) or 0.0) < 0:
                    self._losses_today[instrument] = self._losses_today.get(instrument, 0) + 1
                else:
                    self._losses_today[instrument] = 0
                exit_time = getattr(trade, "exit_time", None)
                if exit_time:
                    self._last_exit_time[instrument] = exit_time
            logger.info("ðŸ“… Daily over-trading counters reset.")

    def _rollover_session_if_needed(self, now_ist: datetime) -> bool:
        """Run session rollover once per date after 09:00, including late startups."""
        if now_ist.time() < dtime(9, 0):
            return False
        session_date = now_ist.date()
        if getattr(self, "_session_rollover_date", None) == session_date:
            return False

        reset_session = getattr(self.trades, "reset_session", None)
        if not callable(reset_session):
            self._session_rollover_date = session_date
            return False
        retained = reset_session(session_date)
        reset_risk = getattr(getattr(self, "risk_manager", None), "reset_for_session", None)
        if callable(reset_risk):
            reset_risk(session_date)
        session_day = session_date.isoformat()
        is_same_session_day = getattr(self, "_session_candidate_day", "") == session_day
        self._session_candidate_day = session_day
        if not is_same_session_day:
            self._session_trade_candidates.clear()
            self._persist_session_trade_candidates()
        self._pending_live_signals.clear()
        self._last_opposite_exit_signal_time.clear()
        self._last_intel_fetch.clear()
        self._chain_cache.clear()
        self._chain_quality_cache.clear()
        self._last_chain_fetch.clear()
        self._premium_cache.clear()
        self._candidate_ltp_cache.clear()
        self._candidate_process_cache.clear()
        self._cached_results.clear()
        try:
            self.market_info = self.expiry.pre_market_check()
        except Exception as exc:
            logger.error(f"Session rollover expiry refresh failed: {exc}")
        try:
            self.trades.reconcile_broker_positions()
        except Exception as exc:
            logger.error(f"Session rollover broker reconciliation failed: {exc}")
        self._session_rollover_date = session_date
        self._daily_reset_done = True
        logger.info(
            f"Session rollover complete for {session_date}; "
            f"active retained={retained['active_retained']}."
        )
        return True

    def _record_accepted_entry(self, candidate: TradeCandidate, trade=None) -> None:
        """Count each accepted live/manual entry exactly once."""
        self._reset_daily_counters()
        key = str(getattr(trade, "id", "") or self._candidate_history_key(candidate))
        if key in self._counted_entry_keys:
            return
        self._counted_entry_keys.add(key)
        instrument = str(candidate.instrument or "").split()[0]
        self._trades_today[instrument] = self._trades_today.get(instrument, 0) + 1

    def _mark_candidate_accepted_time(
        self,
        candidate: TradeCandidate,
        accepted_at: Optional[datetime] = None,
    ) -> None:
        """Separate dashboard entry time from the UTBot source-candle timestamp."""
        if not candidate:
            return
        source_ts = getattr(candidate, "signal_timestamp", None)
        if source_ts is not None and not getattr(candidate, "source_signal_timestamp", None):
            setattr(candidate, "source_signal_timestamp", source_ts)
        if getattr(candidate, "entry_timestamp", None):
            return
        accepted_at = accepted_at or datetime.now(IST)
        if getattr(accepted_at, "tzinfo", None) is not None:
            accepted_at = accepted_at.astimezone(IST).replace(tzinfo=None)
        setattr(candidate, "entry_timestamp", accepted_at)

    @staticmethod
    def _mtf_has_forming_signal(mtf_result, now: Optional[datetime] = None) -> bool:
        """Decision-cache results are unsafe while the latest signal candle is forming."""
        now = now or datetime.now(IST)
        if getattr(now, "tzinfo", None) is not None:
            now = now.astimezone(IST).replace(tzinfo=None)
        for timeframe, attr in (("5min", "results_5min"), ("15min", "results_15min")):
            result = getattr(mtf_result, attr, None) or {}
            signals = result.get("signals", []) or []
            if not signals:
                continue
            signal_ts = getattr(signals[-1], "timestamp", None)
            if signal_ts is None:
                continue
            if getattr(signal_ts, "tzinfo", None) is not None:
                signal_ts = signal_ts.astimezone(IST).replace(tzinfo=None)
            minutes = 5 if timeframe == "5min" else 15
            if now < signal_ts + timedelta(minutes=minutes):
                return True
        return False

    async def _handle_broker_entry_result(
        self,
        candidate: TradeCandidate,
        trade,
        accepted: bool,
    ) -> None:
        """Promote a pending signal only after broker acknowledgement."""
        if accepted:
            if getattr(candidate, "accepted_by_gate", False):
                await self._publish_trade_payload_now()
                return
            self._close_superseded_session_entries([candidate])
            self._latest_trade_candidates[candidate.instrument] = [candidate]
            setattr(candidate, "accepted_by_gate", True)
            self._mark_candidate_accepted_time(candidate, getattr(trade, "entry_time", None))
            candidate.status = "TRADE SIGNAL"
            self._remember_trade_candidates([candidate])
            self._record_accepted_entry(candidate, trade)
            
            # Launch repaint tracker for live non-warmup entries
            if self.mode != "HISTORICAL" and not getattr(self, "is_warmup", False):
                asyncio.create_task(self._track_repaint_until_candle_close(trade, candidate))
            
            self._notify(
                f"{candidate.direction} {candidate.instrument} @ {candidate.price:.2f} "
                f"| Broker acknowledged | TF: {candidate.timeframe}",
                "buy" if candidate.direction == "LONG" else "sell",
            )
        else:
            setattr(candidate, "accepted_by_gate", False)
            candidate.status = "REJECTED - BROKER"
            self._forget_signal_display_rows(candidate)
            self.log_event(
                f"Broker rejected {candidate.instrument} {candidate.direction}; "
                "signal was not counted as an accepted trade.",
                "trade",
            )
        await self._publish_trade_payload_now()

    def _can_trade_instrument(self, instrument: str, signal_time: datetime) -> Tuple[bool, str]:
        """Checks if we should allow another trade for this instrument today"""
        self._reset_daily_counters()

        # Rule 1: Max accepted entries per index (prevent over-churn)
        count = self._trades_today.get(instrument, 0)
        if count >= self.max_trades_per_index:
            return False, f"Max daily trades ({self.max_trades_per_index}) reached"

        # Rule 2: Max Consecutive Losses (Prevent death spirals)
        losses = self._losses_today.get(instrument, 0)
        if losses >= self.max_consecutive_losses:
            return False, f"Index HALTED: {self.max_consecutive_losses} consecutive losses"

        # Rule 3: Cooldown Period (Prevent revenge trading)
        last_exit = self._last_exit_time.get(instrument)
        if last_exit and self.index_cooldown_minutes > 0:
            # For historical simulation, use signal timestamp; for live, use now
            diff = (signal_time - last_exit.replace(tzinfo=None)).total_seconds() / 60.0
            if diff < self.index_cooldown_minutes:
                return False, f"Cooldown active: {self.index_cooldown_minutes - diff:.0f}m remaining"

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
            "fut_cost": getattr(get_settings(), "fut_cost", 200.0),
            "opt_cost": getattr(get_settings(), "opt_cost", 80.0),
            "backtest_days": self.backtest_days,
            "auto_mode": self.auto_mode,
            "inst_pref": self.inst_pref,
            "active_indices": self.active_indices,
            "strike_selection": getattr(get_settings(), "option_strike_selection", "BOTH"),
            "sl_mode": getattr(get_settings(), "sl_mode", "NATURAL"),
            "grade_preference": getattr(get_settings(), "signal_grade_preference", "auto"),
            "ut_regime_adaptation": getattr(get_settings(), "ut_regime_adaptation", False),
            "ut_concurrency_guard": getattr(get_settings(), "ut_concurrency_guard", True),
            "ut_no_entry_after": getattr(get_settings(), "ut_no_entry_after", "15:00"),
            "ut_5min_no_entry_after": getattr(get_settings(), "ut_5min_no_entry_after", "15:15"),
            "ut_force_exit_time": getattr(get_settings(), "ut_force_exit_time", "15:25"),
            "trade_product_type": getattr(get_settings(), "trade_product_type", "CARRYFORWARD"),
            "intelligence_cache_ttl_seconds": getattr(get_settings(), "intelligence_cache_ttl_seconds", 30.0),
            "history_cache_ttl_seconds": getattr(get_settings(), "history_cache_ttl_seconds", 60.0),
            "ut_option_history_mode": getattr(get_settings(), "ut_option_history_mode", "fetch_or_synthetic"),
            "ut_backtest_more_results": getattr(get_settings(), "ut_backtest_more_results", True),
            "ut_timeframe_entry_policy": getattr(get_settings(), "ut_timeframe_entry_policy", "PRIMARY_15"),
            "ut_5min_option_min_confidence": getattr(get_settings(), "ut_5min_option_min_confidence", 0.70),
            "ut_5min_loss_cooldown_minutes": getattr(get_settings(), "ut_5min_loss_cooldown_minutes", 45),
            "live_filter_leniency_pct": getattr(get_settings(), "live_filter_leniency_pct", 0.15),
            "live_choppy_gate_confidence": getattr(get_settings(), "live_choppy_gate_confidence", 0.78),
            "live_signal_stabilization_seconds": getattr(get_settings(), "live_signal_stabilization_seconds", 20.0),
            "live_restart_recovery_grace_seconds": getattr(get_settings(), "live_restart_recovery_grace_seconds", 120),
            "max_trades_per_index": getattr(
                self, "max_trades_per_index", getattr(get_settings(), "max_trades_per_index", 5)
            ),
            "max_consecutive_losses": getattr(
                self, "max_consecutive_losses", getattr(get_settings(), "max_consecutive_losses", 3)
            ),
            "index_cooldown_minutes": getattr(
                self, "index_cooldown_minutes", getattr(get_settings(), "index_cooldown_minutes", 4.0)
            ),
            "chart_stream_enabled": bool(getattr(self, "chart_stream_enabled", True)),
            "ut_preset": getattr(get_settings(), "ut_preset", "AGGRESSIVE")
        }

    def _update_dashboard_cache(self, payload: Dict[str, Any]) -> None:
        cache = getattr(self, "dashboard_cache", None)
        if cache is not None:
            cache.update(payload)

    def _publish_config_snapshot(self) -> None:
        snapshot = dict(getattr(self, "latest_results", {}) or {})
        snapshot["mode"] = self.mode
        snapshot["config"] = self.get_broadcast_config()
        snapshot["timestamp"] = datetime.now(IST).isoformat()
        self.latest_results = snapshot
        self._update_dashboard_cache(snapshot)

    def set_chart_stream_enabled(self, enabled: bool) -> bool:
        """Toggle dashboard chart payload generation without affecting trading."""
        self.chart_stream_enabled = bool(enabled)
        if not self.chart_stream_enabled:
                self.latest_results = self.get_latest_results(include_full_charts=False) or self.latest_results
                self._update_dashboard_cache(self.latest_results)
        logger.info(f"Chart stream: {'ENABLED' if self.chart_stream_enabled else 'DISABLED'}")
        return self.chart_stream_enabled

    def allows_historical_trade_creation(self) -> bool:
        """Only offline historical/backtest mode may create simulated trade rows."""
        return str(getattr(self, "mode", "") or "").upper() == "HISTORICAL"

    def historical_session_cutoff_date(self, now: Optional[datetime] = None):
        """Last session date allowed in historical mode.

        Today's live session becomes historical only after the market handoff
        time. Before that, historical views/backfills stay on completed data.
        """
        now_ist = now or datetime.now(IST)
        if getattr(now_ist, "tzinfo", None) is None:
            now_ist = IST.localize(now_ist)
        else:
            now_ist = now_ist.astimezone(IST)

        handoff = dtime(15, 30)
        session_day = datetime.fromisoformat(str(self._get_current_session_day())).date()
        # Overnight, _get_current_session_day already resolves to the previous
        # completed market session. Do not subtract another trading day.
        if now_ist.date() > session_day or now_ist.time() >= handoff:
            return session_day

        try:
            dates = [
                datetime.fromisoformat(str(day)).date()
                for day in db.list_session_signal_dates()
                if datetime.fromisoformat(str(day)).date() < session_day
            ]
            if dates:
                return dates[-1]
        except Exception:
            pass
        return session_day - timedelta(days=1)

    def _warm_memory_allowed(self) -> bool:
        return str(getattr(self, "mode", "") or "").upper() == "REAL"

    def _warm_memory_cutoff(self) -> datetime:
        now_fn = getattr(self.warm_memory, "_now", None)
        now = now_fn() if callable(now_fn) else datetime.now(IST)
        return now - getattr(self.warm_memory, "ttl", timedelta(minutes=15))

    def _build_warm_memory_payload(self) -> Dict[str, Any]:
        cutoff = self._warm_memory_cutoff()
        candles: Dict[str, Dict[str, list]] = {}
        for instrument in list(getattr(self, "active_indices", []) or []):
            tf_map = {}
            for tf in ("1min", "5min", "15min"):
                rows = dataframe_to_records(self.candles.get_candles(instrument, tf), cutoff)
                if rows:
                    tf_map[tf] = rows
            if tf_map:
                candles[instrument] = tf_map

        session_candidates: Dict[str, list] = {}
        for instrument, book in (getattr(self, "_session_trade_candidates", {}) or {}).items():
            rows = []
            for candidate in (book or {}).values():
                if not self._candidate_matches_live_session_day(candidate):
                    continue
                row = self._serialize_trade_candidate(candidate)
                if str(row.get("id") or "").startswith(("H_", "EOD_")):
                    continue
                rows.append(row)
            if rows:
                session_candidates[instrument] = rows[-150:]

        pending_live_signals = []
        for key, item in (getattr(self, "_pending_live_signals", {}) or {}).items():
            if not isinstance(item, dict):
                continue
            candidate = item.get("candidate")
            buffered_at = item.get("timestamp")
            if candidate is None or buffered_at is None:
                continue
            row = self._serialize_trade_candidate(candidate)
            row["source_signal_type"] = str(
                getattr(candidate, "source_signal_type", "")
                or ("BUY" if getattr(candidate, "direction", "") == "LONG" else "SELL")
            )
            sig_timestamp = item.get("sig_timestamp") or getattr(candidate, "signal_timestamp", None)
            pending_live_signals.append({
                "key": str(key),
                "buffered_at": buffered_at.isoformat(),
                "sig_timestamp": sig_timestamp.isoformat() if sig_timestamp is not None else "",
                "candidate": row,
            })

        return {
            "mode": self.mode,
            "session_day": getattr(self, "_session_candidate_day", self._get_current_session_day()),
            "active_indices": list(getattr(self, "active_indices", []) or []),
            "candles": candles,
            "latest_prices": dict(getattr(self.candles, "_latest_prices", {}) or {}),
            "session_candidates": session_candidates,
            "pending_live_signals": pending_live_signals,
            "latest_results": self.latest_results or {},
            "open_trade_ids": list(getattr(self.trades, "open_trades", {}) or {}),
        }

    def _save_warm_memory(self, force: bool = False) -> bool:
        if not getattr(self, "warm_memory", None) or not self._warm_memory_allowed():
            return False
        try:
            if self.warm_memory.save(self._build_warm_memory_payload(), force=force):
                logger.debug("Warm memory checkpoint saved.")
                return True
        except Exception as exc:
            logger.debug(f"Warm memory checkpoint skipped: {exc}")
        return False

    def _restore_warm_memory(self) -> None:
        if not getattr(self, "warm_memory", None) or not self._warm_memory_allowed():
            return
        payload = self.warm_memory.load()
        if not payload:
            return
        if payload.get("session_day") != getattr(self, "_session_candidate_day", self._get_current_session_day()):
            return
        restored_candles = 0
        for instrument, tf_map in (payload.get("candles") or {}).items():
            if instrument not in set(getattr(self, "active_indices", []) or []):
                continue
            for tf, records in (tf_map or {}).items():
                df = records_to_dataframe(records or [])
                if df.empty:
                    continue
                self.candles.update_candles(instrument, df, tf)
                restored_candles += len(df)

        for instrument, price in (payload.get("latest_prices") or {}).items():
            try:
                self.candles.update_latest_price(instrument, float(price))
            except Exception:
                pass

        restored_candidates = 0
        for instrument, rows in (payload.get("session_candidates") or {}).items():
            for row in rows or []:
                candidate = self._candidate_from_payload(row)
                if not candidate or not self._candidate_matches_live_session_day(candidate):
                    continue
                if str(getattr(candidate, "action", "ENTRY") or "ENTRY").upper() == "NO_ENTRY":
                    continue
                book = self._session_trade_candidates.setdefault(instrument, {})
                book[self._candidate_history_key(candidate)] = candidate
                restored_candidates += 1

        restored_pending = 0
        for item in payload.get("pending_live_signals") or []:
            if not isinstance(item, dict):
                continue
            candidate = self._candidate_from_payload(item.get("candidate") or {})
            if not candidate or not self._candidate_matches_live_session_day(candidate):
                continue
            try:
                buffered_at = datetime.fromisoformat(str(item.get("buffered_at") or ""))
                sig_timestamp = datetime.fromisoformat(str(item.get("sig_timestamp") or ""))
            except (TypeError, ValueError):
                continue
            if buffered_at.tzinfo is not None:
                buffered_at = buffered_at.astimezone(IST).replace(tzinfo=None)
            if sig_timestamp.tzinfo is not None:
                sig_timestamp = sig_timestamp.astimezone(IST).replace(tzinfo=None)
            expected_key = f"{candidate.instrument}_{candidate.timeframe}_{candidate.direction}"
            key = str(item.get("key") or expected_key)
            if key != expected_key:
                continue
            source_signal_type = str(
                (item.get("candidate") or {}).get("source_signal_type")
                or ("BUY" if candidate.direction == "LONG" else "SELL")
            )
            setattr(candidate, "source_signal_type", source_signal_type)
            self._pending_live_signals[key] = {
                "timestamp": buffered_at,
                "candidate": candidate,
                "sig_timestamp": sig_timestamp,
                "recovered": True,
            }
            restored_pending += 1

        latest = payload.get("latest_results") or {}
        if isinstance(latest, dict) and latest.get("mode") == self.mode:
            self.latest_results = latest
            self._update_dashboard_cache(latest)

        self._warm_memory_loaded = True
        logger.info(
            f"Warm memory restored: {restored_candles} candle row(s), "
            f"{restored_candidates} live signal row(s), "
            f"{restored_pending} pending stabilization buffer(s)."
        )

    @staticmethod
    def _normalize_signal_grade_preference(value) -> str:
        """Keep UI/API/env grade settings on one canonical vocabulary."""
        raw = str(value or "auto").strip().upper().replace(" ", "")
        aliases = {
            "": "auto",
            "AUTO": "auto",
            "SMART": "auto",
            "B": "B",
            "B+": "B+",
            "BPLUS": "B+",
            "B_PLUS": "B+",
            "A": "A",
            "A+": "A+",
            "APLUS": "A+",
            "A_PLUS": "A+",
        }
        return aliases.get(raw, "auto")

    def configure(self, capital_total=None, capital_fut=None, capital_opt=None, risk_fut_pct=None, risk_opt_pct=None,
                  lots=None, lots_fut=None, mode=None, reset=False, futures_sl_pct=None, options_sl_pct=None,
                  fut_cost=None, opt_cost=None, backtest_days=None, auto_mode=None, inst_pref=None, strike_selection=None, active_indices=None,
                  grade_preference=None, concurrency_guard=None, dynamic_risk_enabled=None,
                  dynamic_risk_hybrid=None, confirm_real_mode=False, real_mode_verification=None, ut_preset=None,
                  timeframe_entry_policy=None, max_trades_per_index=None, max_consecutive_losses=None,
                  index_cooldown_minutes=None, **kwargs):
        from config.settings import get_settings
        settings = get_settings()

        def real_mode_confirmed() -> bool:
            verification_text = str(real_mode_verification or kwargs.get("real_mode_verification", "")).strip().upper()
            return verification_text in {"YES", "REAL"}

        if mode == "REAL" and getattr(self, "mode", None) != "REAL":
            if not real_mode_confirmed():
                logger.warning("REAL mode blocked: explicit one-step confirmation is required.")
                if self.on_notification:
                    self.on_notification("REAL mode blocked. Confirm YES to switch to REAL.", "error")
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
            strike_selection = str(strike_selection or "BOTH").upper()
            if strike_selection not in {"ATM", "ITM", "BOTH"}:
                strike_selection = "BOTH"
            if getattr(settings, "option_strike_selection", "BOTH") != strike_selection:
                settings.option_strike_selection = strike_selection
                needs_full_refresh = True
                logger.info(f"Strike Selection updated: {strike_selection}")

        if grade_preference is not None:
            grade_preference = self._normalize_signal_grade_preference(grade_preference)
            if getattr(settings, "signal_grade_preference", "auto") != grade_preference:
                settings.signal_grade_preference = grade_preference
                needs_full_refresh = True
                logger.info(f"Signal Grade Preference updated: {grade_preference}")

        if ut_preset is not None:
            preset = str(ut_preset or "AGGRESSIVE").upper()
            if getattr(settings, "ut_preset", "AGGRESSIVE") != preset:
                settings.ut_preset = preset
                if hasattr(self.mtf, "apply_engine_params"):
                    self.mtf.apply_engine_params(settings.get_ut_engine_params())
                needs_full_refresh = True
                logger.info(f"UT Bot Preset updated: {preset}")

        if timeframe_entry_policy is not None:
            policy = str(timeframe_entry_policy or "PRIMARY_15").upper()
            if policy not in {"PRIMARY_15", "INCLUDE_5MIN"}:
                policy = "PRIMARY_15"
            if getattr(settings, "ut_timeframe_entry_policy", "PRIMARY_15") != policy:
                settings.ut_timeframe_entry_policy = policy
                needs_full_refresh = True
                logger.info(f"Timeframe entry policy updated: {policy}")

        if max_trades_per_index is not None:
            value = max(1, min(20, int(max_trades_per_index)))
            self.max_trades_per_index = value
            settings.max_trades_per_index = value
        if max_consecutive_losses is not None:
            value = max(1, min(10, int(max_consecutive_losses)))
            self.max_consecutive_losses = value
            settings.max_consecutive_losses = value
        if index_cooldown_minutes is not None:
            value = max(0.0, min(120.0, float(index_cooldown_minutes)))
            self.index_cooldown_minutes = value
            settings.index_cooldown_minutes = value

        if capital_total is not None:
            new_capital_total = float(capital_total)
            if new_capital_total != self.capital_total:
                self.capital_total = new_capital_total
                settings.capital_total = new_capital_total
                needs_full_refresh = True
                logger.info(f"Total Capital updated: {self.capital_total:,.0f}")

        if auto_mode is not None:
            wants_auto = bool(auto_mode)
            if wants_auto and getattr(self, "mode", None) == "REAL":
                verification_text = str(real_mode_verification or kwargs.get("real_mode_verification", "")).strip().upper()
                if not confirm_real_mode or verification_text not in {"YES", "REAL"}:
                    logger.warning("REAL auto-execution blocked: explicit verification is required.")
                    if self.on_notification:
                        self.on_notification("REAL auto-execution blocked. Confirm to enable Auto Mode in REAL.", "error")
                    return False
            self.auto_mode = bool(auto_mode)
            logger.info(f"ðŸ”„ Auto Mode: {'ENABLED' if self.auto_mode else 'DISABLED'}")

        if inst_pref and inst_pref != self.inst_pref:
            self.inst_pref = inst_pref
            settings.inst_pref = inst_pref
            needs_full_refresh = True
            logger.info(f"ðŸ”„ Instrument Preference changed to: {self.inst_pref}")

        if backtest_days is not None and int(backtest_days) != self.backtest_days:
            self.backtest_days = int(backtest_days)
            settings.default_backtest_days = self.backtest_days
            needs_full_refresh = True
            logger.info(f"ðŸ”„ Backtest window changed to: {self.backtest_days} days")
            self.simulation_id = int(time.time()) # Update sim ID to force full broadcast
            self._hist_candidates_loaded = False  # Force reload of session candidates

        if capital_fut is not None and float(capital_fut) != self.capital_fut:
            self.capital_fut = float(capital_fut)
            settings.capital_fut = self.capital_fut
            needs_full_refresh = True
        if capital_opt is not None and float(capital_opt) != self.capital_opt:
            self.capital_opt = float(capital_opt)
            settings.capital_opt = self.capital_opt
            needs_full_refresh = True
        if risk_fut_pct is not None and float(risk_fut_pct) != self.risk_fut_pct:
            self.risk_fut_pct = float(risk_fut_pct)
            settings.risk_fut_pct = self.risk_fut_pct
            needs_full_refresh = True
        if risk_opt_pct is not None and float(risk_opt_pct) != self.risk_opt_pct:
            self.risk_opt_pct = float(risk_opt_pct)
            settings.risk_opt_pct = self.risk_opt_pct
            needs_full_refresh = True
            
        if fut_cost is not None and fut_cost != getattr(settings, "fut_cost", 200):
            settings.fut_cost = float(fut_cost)
            self.fut_cost = float(fut_cost)
            needs_full_refresh = True
        if opt_cost is not None and opt_cost != getattr(settings, "opt_cost", 80):
            settings.opt_cost = float(opt_cost)
            self.opt_cost = float(opt_cost)
            needs_full_refresh = True

        if lots and lots != self.user_lots:
            self.user_lots.update(lots)
            needs_full_refresh = True

        if lots_fut and lots_fut != self.user_lots_fut:
            self.user_lots_fut.update(lots_fut)
            needs_full_refresh = True

        if futures_sl_pct is not None and float(futures_sl_pct) != self.futures_sl_pct:
            self.futures_sl_pct = float(futures_sl_pct)
            settings.futures_sl_pct = self.futures_sl_pct
            needs_full_refresh = True
        if options_sl_pct is not None and float(options_sl_pct) != self.options_sl_pct:
            self.options_sl_pct = float(options_sl_pct)
            settings.options_sl_pct = self.options_sl_pct
            needs_full_refresh = True

        # ——— SYNC TO TRADE MANAGER ———
        self.trades.update_risk_settings(self.futures_sl_pct, self.options_sl_pct)

        # ——— SYNC TO RISK MANAGER (Circuit Breaker) ———
        self.risk_manager.update_settings(
            max_daily_loss_pct=self.max_daily_loss_pct,
            risk_fut_pct=self.risk_fut_pct,
            risk_opt_pct=self.risk_opt_pct,
            capital_fut=self.capital_fut,
            capital_opt=self.capital_opt,
            capital_total=self.capital_total,
            circuit_breaker_slippage_bps=float(
                getattr(settings, "circuit_breaker_slippage_bps", 10.0) or 10.0
            ),
        )

        if active_indices is not None and active_indices != self.active_indices:
            self.active_indices = active_indices
            settings.active_indices = active_indices
            needs_full_refresh = True
            logger.info(f"ðŸ”„ Active Indices updated: {self.active_indices}")

        if mode:
            # Mode Map: HISTORICAL, REAL
            old_mode = self.mode

            self.mode = mode
            self.trades.mode = mode
            settings.trading_mode = mode
            logger.info(f"ðŸ”„ Mode switched: {old_mode} âž” {mode}")

            # If switching TO or FROM Historical, we need a full state purge
            if mode == "HISTORICAL" or old_mode == "HISTORICAL":
                needs_full_refresh = True
                self._hist_candidates_loaded = False

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
            if hasattr(self, "_last_signal_identity"):
                self._last_signal_identity.clear()
            if hasattr(self, "_last_live_signal_candle_time"):
                self._last_live_signal_candle_time.clear()
            if hasattr(self, "_pending_live_signals"):
                self._pending_live_signals.clear()

            # Reset scan count to trigger initial logic in next cycle
            self.scan_count = 0
            if self.mode == "REAL":
                self.latest_results = {
                    **(self.latest_results or {}),
                    "mode": self.mode,
                    "config": self.get_broadcast_config(),
                    "trades": self._build_dashboard_trade_payload(),
                    "timestamp": datetime.now(IST).isoformat(),
                }
                self._update_dashboard_cache(self.latest_results)
            else:
                self.queue_full_recalculation("reset")
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
            if hasattr(self, "_last_signal_identity"):
                self._last_signal_identity.clear()
            if hasattr(self, "_last_live_signal_candle_time"):
                self._last_live_signal_candle_time.clear()
            if hasattr(self, "_pending_live_signals"):
                self._pending_live_signals.clear()

            # Reload session candidates from the correct path for the new mode/settings
            self._load_session_trade_candidates()

            self.scan_count = 0
            if self.mode == "REAL":
                self.latest_results = {
                    **(self.latest_results or {}),
                    "mode": self.mode,
                    "config": self.get_broadcast_config(),
                    "trades": self._build_dashboard_trade_payload(),
                    "timestamp": datetime.now(IST).isoformat(),
                }
                self._update_dashboard_cache(self.latest_results)
            else:
                self.queue_full_recalculation("settings-refresh")

        self._publish_config_snapshot()

        # Persist the updated settings off the hot path; rapid UI edits coalesce.
        schedule_settings_save(settings)

        return True

    def _diag_inc(self, category: str, key: str, amount: int = 1) -> None:
        diagnostics = getattr(self, "_diagnostics", None)
        if diagnostics is None:
            self._diagnostics = {}
            diagnostics = self._diagnostics
        bucket = diagnostics.setdefault(category, {})
        bucket[key] = int(bucket.get(key, 0) or 0) + amount

    def _diag_inc_unique(self, category: str, key: str, identity: str) -> None:
        unique_events = getattr(self, "_diagnostic_unique_events", None)
        if unique_events is None:
            self._diagnostic_unique_events = set()
            unique_events = self._diagnostic_unique_events
        event_key = (str(category), str(key), str(identity))
        if event_key in unique_events:
            return
        unique_events.add(event_key)
        self._diag_inc(category, key)

    def _reset_simulation_diagnostics(self) -> None:
        """Start strategy counters for the newly requested backtest window."""
        previous = getattr(self, "_diagnostics", {}) or {}
        previous_latency = previous.get("latency", {}) or {}
        self._diagnostic_unique_events = set()
        self._diagnostics = {
            "rejects": {},
            "option_history": {"attempts": 0, "hits": 0, "misses": 0, "synthetic": 0, "fallback_to_fut": 0},
            "latency": {
                "last_ms": 0,
                "high_latency_events": int(previous_latency.get("high_latency_events", 0) or 0),
            },
            "stabilization": {
                "buffered": 0,
                "waiting_stability": 0,
                "waiting_candle_close": 0,
                "discarded_repaint": 0,
                "discarded_future": 0,
                "discarded_stale": 0,
                "matured": 0,
            },
            "timeouts": {"skipped_inflight": 0},
            "repaint_guard": {"checked": 0, "aborted": 0, "passed": 0},
            "exit_reasons": {},
            "instrument_selection": {"OPT": 0, "FUT": 0},
            "source_fallback": {"delayed_entry_blocked": 0},
            "simulation": {
                "id": int(getattr(self, "simulation_id", 0) or 0),
                "backtest_days": int(getattr(self, "backtest_days", 0) or 0),
                "started_at": datetime.now(IST).isoformat(),
            },
        }

    def _get_system_metrics_payload(self) -> Dict[str, Any]:
        """Return cached local hardware/process metrics for the UT1 state panel."""
        now = time.time()
        cached = getattr(self, "_system_metrics_cache", None)
        if isinstance(cached, dict) and now - float(cached.get("_sampled_at", 0.0) or 0.0) < 5.0:
            return {k: v for k, v in cached.items() if k != "_sampled_at"}

        def process_rss_mb() -> float:
            try:
                import psutil
                return round(float(psutil.Process(os.getpid()).memory_info().rss) / (1024 * 1024), 1)
            except Exception:
                pass
            if os.name == "nt":
                try:
                    proc = subprocess.run(
                        ["tasklist", "/FI", f"PID eq {os.getpid()}", "/FO", "CSV", "/NH"],
                        capture_output=True,
                        text=True,
                        timeout=0.8,
                        creationflags=subprocess.CREATE_NO_WINDOW,
                    )
                    row = (proc.stdout or "").splitlines()[0] if proc.returncode == 0 and proc.stdout else ""
                    if row:
                        parts = [part.strip().strip('"') for part in row.split('","')]
                        mem_text = parts[-1] if parts else ""
                        mem_kb = float("".join(ch for ch in mem_text if ch.isdigit()) or 0.0)
                        if mem_kb > 0:
                            return round(mem_kb / 1024.0, 1)
                except Exception:
                    pass
            if os.name != "nt":
                return 0.0
            try:
                class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
                    _fields_ = [
                        ("cb", ctypes.c_ulong),
                        ("PageFaultCount", ctypes.c_ulong),
                        ("PeakWorkingSetSize", ctypes.c_size_t),
                        ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t),
                        ("PeakPagefileUsage", ctypes.c_size_t),
                    ]

                counters = PROCESS_MEMORY_COUNTERS()
                counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS)
                handle = ctypes.windll.kernel32.GetCurrentProcess()
                ok = ctypes.windll.psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb)
                return round(float(counters.WorkingSetSize) / (1024 * 1024), 1) if ok else 0.0
            except Exception:
                return 0.0

        def system_memory() -> Dict[str, float]:
            if os.name != "nt":
                return {"total_mb": 0.0, "used_mb": 0.0, "used_pct": 0.0}
            try:
                class MEMORYSTATUSEX(ctypes.Structure):
                    _fields_ = [
                        ("dwLength", ctypes.c_ulong),
                        ("dwMemoryLoad", ctypes.c_ulong),
                        ("ullTotalPhys", ctypes.c_ulonglong),
                        ("ullAvailPhys", ctypes.c_ulonglong),
                        ("ullTotalPageFile", ctypes.c_ulonglong),
                        ("ullAvailPageFile", ctypes.c_ulonglong),
                        ("ullTotalVirtual", ctypes.c_ulonglong),
                        ("ullAvailVirtual", ctypes.c_ulonglong),
                        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                    ]

                stat = MEMORYSTATUSEX()
                stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
                ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
                total = float(stat.ullTotalPhys)
                used = total - float(stat.ullAvailPhys)
                return {
                    "total_mb": round(total / (1024 * 1024), 0),
                    "used_mb": round(used / (1024 * 1024), 0),
                    "used_pct": round(float(stat.dwMemoryLoad), 1),
                }
            except Exception:
                return {"total_mb": 0.0, "used_mb": 0.0, "used_pct": 0.0}

        def process_cpu_pct() -> float:
            cpu_now = time.process_time()
            previous = getattr(self, "_system_cpu_sample", None)
            self._system_cpu_sample = (now, cpu_now)
            if not previous:
                return 0.0
            prev_wall, prev_cpu = previous
            wall_delta = max(0.001, now - float(prev_wall or now))
            return round(max(0.0, ((cpu_now - float(prev_cpu or cpu_now)) / wall_delta) * 100.0), 1)

        def disk_metrics() -> Dict[str, float]:
            try:
                usage = shutil.disk_usage(Path.cwd())
                used = usage.total - usage.free
                return {
                    "total_gb": round(usage.total / (1024 ** 3), 1),
                    "free_gb": round(usage.free / (1024 ** 3), 1),
                    "used_pct": round((used / usage.total) * 100.0, 1) if usage.total else 0.0,
                }
            except Exception:
                return {"total_gb": 0.0, "free_gb": 0.0, "used_pct": 0.0}

        def gpu_metrics() -> Dict[str, Any]:
            gpu_cache = getattr(self, "_system_gpu_cache", None)
            if isinstance(gpu_cache, dict) and now - float(gpu_cache.get("_sampled_at", 0.0) or 0.0) < 20.0:
                return {k: v for k, v in gpu_cache.items() if k != "_sampled_at"}
            try:
                from engine.gpu_accelerator import get_gpu_accelerator
                accel_payload = get_gpu_accelerator().status()
                bench_payload = get_gpu_accelerator().benchmark(n=1_000_000)
            except Exception:
                accel_payload = {
                    "compute_available": False,
                    "compute_backend": "none",
                    "acceleration_mode": "telemetry_only",
                    "error": "",
                }
                bench_payload = {"available": False, "backend": "none"}

            payload: Dict[str, Any] = {
                "available": False,
                "label": "N/A",
                "used_mb": 0.0,
                "total_mb": 0.0,
                "used_pct": 0.0,
                **accel_payload,
                "benchmark": bench_payload,
            }
            try:
                proc = subprocess.run(
                    [
                        "nvidia-smi",
                        "--query-gpu=name,memory.used,memory.total,utilization.gpu",
                        "--format=csv,noheader,nounits",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=0.8,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
                )
                first = (proc.stdout or "").splitlines()[0].strip() if proc.returncode == 0 and proc.stdout else ""
                if first:
                    parts = [p.strip() for p in first.split(",")]
                    used = float(parts[1]) if len(parts) > 1 else 0.0
                    total = float(parts[2]) if len(parts) > 2 else 0.0
                    payload = {
                        "available": True,
                        "label": parts[0] if parts else "GPU",
                        "used_mb": round(used, 0),
                        "total_mb": round(total, 0),
                        "used_pct": round((used / total) * 100.0, 1) if total > 0 else 0.0,
                        "util_pct": round(float(parts[3]), 1) if len(parts) > 3 and parts[3] else 0.0,
                        **accel_payload,
                        "benchmark": bench_payload,
                    }
            except Exception:
                pass
            self._system_gpu_cache = {**payload, "_sampled_at": now}
            return payload

        payload = {
            "pid": os.getpid(),
            "process_ram_mb": process_rss_mb(),
            "process_cpu_pct": process_cpu_pct(),
            "threads": threading.active_count(),
            "ram": system_memory(),
            "disk": disk_metrics(),
            "gpu": gpu_metrics(),
        }
        self._system_metrics_cache = {**payload, "_sampled_at": now}
        return payload

    def _get_diagnostics_payload(self) -> Dict[str, Any]:
        diagnostics = getattr(self, "_diagnostics", {}) or {}
        rejects = diagnostics.get("rejects", {}) or {}

        # ── Data Freshness: age of latest candle per instrument ──
        data_freshness = {}
        now_fresh = datetime.now(IST)
        for inst_name in list(getattr(self, "active_indices", [])):
            try:
                df = self.candles.get_candles(inst_name, "5min")
                if df is not None and len(df) > 0:
                    last_ts = df.index[-1]
                    if getattr(last_ts, "tzinfo", None) is None:
                        last_ts = last_ts.replace(tzinfo=IST)
                    else:
                        last_ts = last_ts.astimezone(IST)
                    age_s = (now_fresh - last_ts).total_seconds()
                    data_freshness[inst_name] = round(age_s, 0)
                else:
                    data_freshness[inst_name] = -1
            except Exception:
                data_freshness[inst_name] = -1

        # ── Session Stats: signals generated vs trades taken ──
        session_candidates = getattr(self, "_session_trade_candidates", {}) or {}
        total_entries = 0
        total_exits = 0
        for inst_book in list(session_candidates.values()):
            for cand in list(inst_book.values()):
                action = getattr(cand, "action", "ENTRY")
                if action == "EXIT":
                    total_exits += 1
                elif action == "ENTRY":
                    total_entries += 1

        # ── Active trade peak PnL ──
        active_peak = {}
        for inst_name, book in list(session_candidates.items()):
            for cand in list(book.values()):
                if getattr(cand, "action", "") == "ENTRY" and not getattr(cand, "is_exit", False):
                    peak = float(getattr(cand, "peak_pnl", 0.0) or 0.0)
                    current = float(getattr(cand, "pnl", 0.0) or 0.0)
                    active_peak[inst_name] = {"peak": round(peak, 0), "current": round(current, 0)}

        return {
            "rejects": dict(sorted(rejects.items(), key=lambda item: item[1], reverse=True)[:12]),
            "option_history": diagnostics.get("option_history", {}),
            "scanner_freshness": self._scanner_freshness_payload(),
            "recalculation": self._recalculation_status_payload(),
            "settings_persistence": settings_persistence_status(),
            "latency": {
                **(diagnostics.get("latency", {}) or {}),
                "last_ms": self.latest_results.get("latency", 0) if isinstance(self.latest_results, dict) else 0,
                "scan_count": self.scan_count,
            },
            "stabilization": {
                **(diagnostics.get("stabilization", {}) or {}),
                "pending": len(getattr(self, "_pending_live_signals", {}) or {}),
            },
            "recovery_checkpoint": {
                "enabled": bool(getattr(self, "warm_memory", None) and self._warm_memory_allowed()),
                "count": int(getattr(self, "_recovery_checkpoint_count", 0) or 0),
                "last_at": getattr(self, "_last_recovery_checkpoint_at", None),
                "last_error": getattr(self, "_last_recovery_checkpoint_error", ""),
                "path": str(getattr(getattr(self, "warm_memory", None), "path", "")),
                "interval_seconds": float(getattr(getattr(self, "warm_memory", None), "save_interval_seconds", 0.0) or 0.0),
                "ttl_minutes": int(getattr(getattr(self, "warm_memory", None), "ttl", timedelta()).total_seconds() // 60),
            },
            "timeouts": diagnostics.get("timeouts", {}),
            "sources": self.data.get_source_health(),
            "mode": self.mode,
            "repaint_guard": diagnostics.get("repaint_guard", {}),
            "exit_reasons": diagnostics.get("exit_reasons", {}),
            "instrument_selection": diagnostics.get("instrument_selection", {}),
            "source_fallback": diagnostics.get("source_fallback", {}),
            "simulation": diagnostics.get("simulation", {}),
            "data_freshness": data_freshness,
            "session_stats": {
                "entries": total_entries,
                "exits": total_exits,
            },
            "active_peak": active_peak,
            "system_metrics": self._get_system_metrics_payload(),
            "system_performance": self._system_performance_payload(),
        }

    def _system_performance_payload(self, latency_ms: Optional[float] = None) -> Dict[str, Any]:
        """Summarise runtime health for the dashboard SYS tape and diagnostics."""
        diagnostics = getattr(self, "_diagnostics", {}) or {}
        latency_diag = diagnostics.get("latency", {}) or {}
        last_latency = float(
            latency_ms
            if latency_ms is not None
            else latency_diag.get("last_ms")
            or (self.latest_results.get("latency", 0) if isinstance(self.latest_results, dict) else 0)
            or 0
        )
        freshness = self._scanner_freshness_payload()
        recalculation = self._recalculation_status_payload()
        metrics = self._get_system_metrics_payload()
        sources = {}
        try:
            sources = self.data.get_source_health()
        except Exception:
            sources = {}

        issues: List[Dict[str, Any]] = []

        def add_issue(key: str, label: str, severity: str = "warn", value: Any = None) -> None:
            issues.append({"key": key, "label": label, "severity": severity, "value": value})

        if freshness.get("is_stale"):
            add_issue("scanner_stale", "Scanner stale", "critical", freshness.get("age_seconds"))
        if recalculation.get("status") not in (None, "", "idle"):
            add_issue("recalculation_running", f"Recalculation {recalculation.get('status')}", "info")
        if last_latency >= 5000:
            add_issue("scan_latency_high", "Scan latency high", "warn", round(last_latency, 0))
        if bool(sources.get("broker_degraded")):
            add_issue("broker_degraded", "Broker degraded", "warn")
        if bool(sources.get("all_brokers_unavailable")):
            add_issue("brokers_down", "All brokers unavailable", "critical")

        ram_pct = float(((metrics.get("ram") or {}).get("used_pct")) or 0.0)
        disk_pct = float(((metrics.get("disk") or {}).get("used_pct")) or 0.0)
        cpu_pct = float(metrics.get("process_cpu_pct") or 0.0)
        if ram_pct >= 88.0:
            add_issue("ram_pressure", "System RAM pressure", "warn", round(ram_pct, 1))
        if disk_pct >= 92.0:
            add_issue("disk_pressure", "Storage pressure", "warn", round(disk_pct, 1))
        if cpu_pct >= 85.0:
            add_issue("process_cpu_pressure", "UT1 CPU pressure", "warn", round(cpu_pct, 1))

        status = "OK"
        if any(issue["severity"] == "critical" for issue in issues):
            status = "CRITICAL"
        elif any(issue["severity"] == "warn" for issue in issues):
            status = "WARN"
        elif any(issue["severity"] == "info" for issue in issues):
            status = "BUSY"

        payload = {
            "status": status,
            "issues": issues[:8],
            "latency_ms": round(last_latency, 0),
            "scan_count": int(getattr(self, "scan_count", 0) or 0),
            "scanner_stale": bool(freshness.get("is_stale")),
            "recalculation_status": recalculation.get("status", "idle"),
            "sampled_at": datetime.now(IST).isoformat(),
        }
        self._system_perf_snapshot = payload
        return payload

    def _publish_system_performance_alerts(self, latency_ms: Optional[float] = None) -> None:
        """Rate-limit system-performance findings into the activity SYS tape."""
        payload = self._system_performance_payload(latency_ms=latency_ms)
        now_ts = time.time()
        last_alert = getattr(self, "_system_perf_last_alert", {})
        for issue in payload.get("issues", []):
            if issue.get("severity") == "info":
                continue
            key = str(issue.get("key") or issue.get("label") or "system_perf")
            raw_value = issue.get("value")
            value = float(raw_value or 0.0)
            if key == "scanner_stale" and value < 30.0:
                continue
            if key == "scan_latency_high" and value < 30000.0:
                continue
            if key == "process_cpu_pressure" and value < 95.0:
                continue
            if now_ts - float(last_alert.get(key, 0.0) or 0.0) < 60.0:
                continue
            last_alert[key] = now_ts
            suffix = f" ({raw_value})" if raw_value not in (None, "") else ""
            self.log_event(f"SYS PERF: {issue.get('label')}{suffix}", "system")
        self._system_perf_last_alert = last_alert

    def _scanner_freshness_payload(self) -> Dict[str, Any]:
        settings = get_settings()
        threshold = max(1.0, float(getattr(settings, "scanner_stale_after_seconds", 5.0) or 5.0))
        last_scan = getattr(self, "last_scan_time", None)
        age = None
        if last_scan is not None:
            try:
                age = max(0.0, (datetime.now(IST) - last_scan).total_seconds())
            except Exception:
                age = None
        cache_age_ms = None
        cache = getattr(self, "dashboard_cache", None)
        if cache is not None and hasattr(cache, "cache_age_ms"):
            cache_age_ms = cache.cache_age_ms()
        return {
            "last_scan": last_scan.isoformat() if last_scan else None,
            "age_seconds": round(age, 2) if age is not None else None,
            "stale_after_seconds": threshold,
            "is_stale": bool(age is not None and age > threshold),
            "is_calculating": bool(getattr(self, "is_calculating", False)),
            "calculation_lock": bool(getattr(self, "_calculation_lock", False)),
            "cache_age_ms": cache_age_ms,
        }

    def _recalculation_status_payload(self) -> Dict[str, Any]:
        status = dict(getattr(self, "_recalculation_status", {}) or {})
        queue = getattr(self, "recalculation_queue", None)
        if queue is not None:
            status["queue_status"] = getattr(queue, "last_status", "unknown")
            status["queue_reason"] = getattr(queue, "last_reason", "")
            status["queue_superseded_count"] = int(getattr(queue, "superseded_count", 0) or 0)
        return status

    def queue_full_recalculation(self, reason: str = "manual") -> None:
        """Request a background recalculation without blocking the caller."""
        try:
            queue = getattr(self, "recalculation_queue", None)
            scanner_loop = getattr(self, "_event_loop", None)
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                current_loop = None
            if queue is not None:
                coro = queue.request(reason)
                if scanner_loop is not None and scanner_loop.is_running() and scanner_loop is not current_loop:
                    asyncio.run_coroutine_threadsafe(coro, scanner_loop)
                else:
                    asyncio.create_task(coro)
            else:
                coro = self._perform_full_recalculation()
                if scanner_loop is not None and scanner_loop.is_running() and scanner_loop is not current_loop:
                    asyncio.run_coroutine_threadsafe(coro, scanner_loop)
                else:
                    asyncio.create_task(coro)
        except RuntimeError:
            logger.warning("Unable to queue recalculation: no active event loop")

    async def _perform_full_recalculation(self):
        """Compatibility path: direct awaits still run the recalculation now."""
        return await self._perform_full_recalculation_impl()

    def _set_recalculation_status(self, status: str, reason: str = "", **extra) -> None:
        payload = dict(getattr(self, "_recalculation_status", {}) or {})
        payload.update({"status": status, "reason": reason or payload.get("reason", ""), **extra})
        if status in {"worker_running", "applying", "running"}:
            payload.setdefault("started_at", datetime.now(IST).isoformat())
            payload["finished_at"] = None
        if status in {"idle", "failed", "timeout"}:
            payload["finished_at"] = datetime.now(IST).isoformat()
        self._recalculation_status = payload

    async def _run_recalculation_worker(self, indices: Dict[str, Any]) -> Dict[str, Any]:
        settings = get_settings()
        if not getattr(settings, "recalculation_worker_enabled", True):
            return {"status": "disabled", "files": [], "errors": []}
        worker = getattr(self, "recalculation_worker", None) or ProcessRecalculationWorker(
            timeout_seconds=float(getattr(settings, "recalculation_worker_timeout_seconds", 120.0) or 120.0)
        )
        job = {
            "backtest_days": int(getattr(self, "backtest_days", 1) or 1),
            "indices": indices,
            "timeframes": ["1min", "5min", "15min"],
        }
        loop = asyncio.get_running_loop()
        self._set_recalculation_status("worker_running", "historical-cache-warm")
        return await loop.run_in_executor(None, lambda: worker.run(job))

    def _load_worker_candles(self, worker_result: Dict[str, Any]) -> int:
        loaded = 0
        for item in worker_result.get("files", []) or []:
            try:
                path = Path(item.get("path", ""))
                if not path.exists():
                    continue
                df = pd.read_csv(path, index_col=0, parse_dates=True)
                if df is None or df.empty:
                    continue
                self.candles.update_candles(item.get("instrument", ""), df, item.get("timeframe", "1min"))
                loaded += len(df)
            except Exception as exc:
                logger.debug(f"Worker candle load skipped: {exc}")
        return loaded

    async def _perform_full_recalculation_impl(self):
        """Sequential sequence to ensure re-simulation uses NEW data"""
        if getattr(self, "_calculation_lock", False):
            logger.warning("ðŸš« Re-simulation delayed: Calculation lock active.")
            settings = get_settings()
            live_wait = max(
                2.0,
                float(getattr(settings, "live_calculation_lock_timeout_seconds", 12.0) or 12.0),
            )
            wait_budget = max(75.0, live_wait) if self.mode == "HISTORICAL" else live_wait
            deadline = time.monotonic() + wait_budget
            while getattr(self, "_calculation_lock", False) and time.monotonic() < deadline:
                await asyncio.sleep(0.25)
            if getattr(self, "_calculation_lock", False):
                logger.error(
                    f"Re-simulation skipped after waiting {wait_budget:.1f}s; "
                    "the active calculation remains owner of the lock."
                )
                return False

        self._calculation_lock = True
        self._lock_time = time.time()
        started_at = time.monotonic()
        active_label = ", ".join(getattr(self, "active_indices", []) or []) or "none"
        self.log_event(
            f"Backtest started: {self.backtest_days} day(s), indices={active_label}",
            "system",
        )
        all_indices = self.config.get("indices", {})
        indices = {k: v for k, v in all_indices.items() if k in self.active_indices}
        worker_result: Dict[str, Any] = {"status": "not_started", "files": [], "errors": []}
        if self.mode == "HISTORICAL":
            try:
                worker_result = await self._run_recalculation_worker(indices)
            except Exception as exc:
                worker_result = {"status": "error", "files": [], "errors": [str(exc)]}
                logger.warning(f"Process recalculation worker failed; falling back in-process: {exc}")
        self._set_recalculation_status("applying", "apply-worker-result", worker=worker_result)
        loaded_rows = 0
        try:
            self._reset_simulation_diagnostics()
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
                            signal_ts = getattr(cand, 'signal_timestamp', None)
                            signal_key = signal_ts.isoformat() if signal_ts else ""
                            match_key = (
                                getattr(cand, 'instrument', instrument),
                                getattr(cand, 'direction', 'LONG'),
                                getattr(cand, 'timeframe', '5min'),
                                signal_key
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
                        
                        saved_entry_ts = getattr(entry_cand, "entry_timestamp", None)
                        if saved_entry_ts:
                            entry_time = (
                                saved_entry_ts
                                if isinstance(saved_entry_ts, datetime)
                                else datetime.fromisoformat(str(saved_entry_ts))
                            )
                        else:
                            entry_time = signal_ts
                        if entry_time.tzinfo is None:
                            entry_time = IST.localize(entry_time)
                        else:
                            entry_time = entry_time.astimezone(IST)
                        
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
                            pnl = self._candidate_net_pnl(exit_cand, exit_price) if exit_price > 0 else 0.0
                        
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
                            charges=(
                                float(get_settings().fut_cost)
                                if getattr(entry_cand, 'inst_type') == 'FUT'
                                else float(get_settings().opt_cost)
                            ),
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

            if worker_result.get("status") == "ok" and candle_rows_from_manifest(worker_result.get("files", [])) > 0:
                loaded_rows = self._load_worker_candles(worker_result)
                logger.info(f"Loaded {loaded_rows} candle row(s) from process worker cache.")
            if loaded_rows <= 0:
                logger.info("Process worker cache unavailable; using in-process history fetch fallback.")
                await self._fetch_all_data(indices, force=True)

            logger.info("ðŸ”„ RE-SIMULATION SEQUENCE: Recalculating signals and trades...")
            self._last_signal_time.clear() # Clear here ONLY after data is ready
            self._last_signal_identity.clear()
            self.simulation_id = int(time.time())
            # The next _scan_cycle will now pick up the new days_back and empty last_signal_time
            logger.info("ðŸ§¹ System state RESET with FORCED history fetch.")
            elapsed = time.monotonic() - started_at
            self.log_event(
                f"Backtest finished: {self.backtest_days} day(s) recalculated in {elapsed:.1f}s",
                "success",
            )
            self._set_recalculation_status(
                "idle",
                "complete",
                worker=worker_result,
                loaded_rows=loaded_rows,
                duration_seconds=round(elapsed, 2),
            )
            return True
        except Exception as exc:
            elapsed = time.monotonic() - started_at
            self.log_event(
                f"Backtest failed after {elapsed:.1f}s: {exc}",
                "error",
            )
            self._set_recalculation_status(
                "failed",
                "exception",
                worker=worker_result,
                error=str(exc),
                duration_seconds=round(elapsed, 2),
            )
            raise
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
        was_warmup = getattr(self, "is_warmup", False)
        self.is_warmup = True
        try:
            # 1. Build the selected chart first, then hydrate the rest in the
            # background. This avoids startup stalls where 9 history calls block
            # the dashboard before the user can see anything useful.
            active_inst = getattr(self, "active_chart_instrument", "NIFTY")
            active_tf = getattr(self, "active_chart_tf", "5min")
            active_cfg = indices.get(active_inst)
            startup_days = 1 if self.mode == "REAL" else None
            if active_cfg:
                await self._fetch_all_data(
                    {active_inst: active_cfg},
                    force=True,
                    timeframes=[active_tf],
                    days_back_override=startup_days,
                )
            await self._fetch_all_data(
                indices,
                force=False,
                days_back_override=startup_days,
            )

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

        finally:
            self._initial_setup_completed = True
            self.is_warmup = was_warmup
            self._initial_setup_task = None

    def _schedule_initial_setup(self, indices: Dict) -> bool:
        """Schedule startup hydration once, even when settings reset scan_count."""
        if self._initial_setup_completed:
            return False
        task = self._initial_setup_task
        if task is not None and not task.done():
            return False
        self._initial_setup_task = asyncio.create_task(self._background_initial_setup(indices))
        return True

    def _is_trading_day(self, now_ist: datetime) -> bool:
        """Return True for Mon-Fri NSE trading days, excluding configured holidays."""
        if now_ist.weekday() >= 5:
            return False
        try:
            if hasattr(self.data, "is_market_holiday") and self.data.is_market_holiday(now_ist):
                return False
        except Exception:
            pass
        return True

    def _schedule_morning_market_refresh(self, indices: Dict, now_ist: datetime) -> None:
        """Launch the full market-session refresh once per trading day."""
        session_day = now_ist.date().isoformat()
        if getattr(self, "_morning_refresh_inflight", False):
            logger.info("Morning market refresh already running; skipping duplicate schedule.")
            return
        if getattr(self, "_morning_refresh_last_date", None) == session_day:
            logger.info("Morning market refresh already completed for this session.")
            return

        self._morning_refresh_inflight = True
        try:
            asyncio.create_task(self._morning_market_refresh(indices, session_day))
        except RuntimeError:
            self._morning_refresh_inflight = False
            logger.error("Morning market refresh could not be scheduled; no running event loop.")

    def _ensure_market_session_refresh(self, indices: Dict, now_ist: datetime) -> bool:
        """Ensure live-session data is warmed before signal processing."""
        session_day = now_ist.date().isoformat()
        if getattr(self, "_morning_refresh_last_date", None) == session_day:
            return False
        if getattr(self, "_morning_refresh_inflight", False):
            return True
        if not self._is_trading_day(now_ist):
            return False

        try:
            is_live_market = bool(self.data.is_market_open(now_ist))
        except TypeError:
            is_live_market = bool(self.data.is_market_open())
        except Exception:
            is_live_market = False

        if is_live_market:
            self.log_event("Market session warmup required before live scanning; starting refresh.", "data")
            self._schedule_morning_market_refresh(indices, now_ist)
            return True

        return False

    async def _morning_market_refresh(self, indices: Dict, session_day: str) -> None:
        """Refresh market-critical data once at live market start."""
        started = time.time()
        summary = {
            "index_quotes": 0,
            "vix": False,
            "futures": 0,
            "option_tokens": 0,
            "option_ltp": 0,
            "chains": 0,
            "intel": 0,
        }
        try:
            active_indices = {k: v for k, v in (indices or {}).items() if k in self.active_indices}
            if not active_indices:
                active_indices = {k: v for k, v in self.config.get("indices", {}).items() if k in self.active_indices}

            self.log_event("Morning refresh: warming index, VIX, futures, options, expiry, and intel data", "data")

            try:
                self.market_info = self.expiry.pre_market_check()
                summary["expiry"] = True
            except Exception as exc:
                summary["expiry"] = False
                logger.error(f"Morning refresh expiry/rollover check failed: {exc}")

            try:
                await self._fetch_all_data(active_indices, force=True, timeframes=["1min", "5min", "15min"])
                self._last_full_history_refresh = time.time()
                summary["history"] = True
            except Exception as exc:
                summary["history"] = False
                logger.error(f"Morning refresh history fetch failed: {exc}")

            loop = asyncio.get_event_loop()
            session_date = datetime.fromisoformat(session_day).date()
            parallelism = max(
                1,
                int(getattr(get_settings(), "morning_refresh_parallelism", 3) or 3),
            )
            refresh_semaphore = asyncio.Semaphore(parallelism)

            async def bounded_to_thread(func, *args):
                async with refresh_semaphore:
                    return await asyncio.to_thread(func, *args)

            async def prewarm_index(name, cfg):
                quote_task = bounded_to_thread(self.data.get_index_quote, name)
                previous_close_task = bounded_to_thread(self.data.get_previous_close, name)
                chain_task = bounded_to_thread(
                        self._get_option_chain_cached,
                        name,
                        cfg,
                        True,
                        True,
                    )
                quote, previous_close, chain_result = await asyncio.gather(
                    quote_task,
                    previous_close_task,
                    chain_task,
                    return_exceptions=True,
                )
                return name, {
                    "quote": quote,
                    "previous_close": previous_close,
                    "chain_result": chain_result,
                }

            warmed = await asyncio.gather(
                *(prewarm_index(name, cfg) for name, cfg in active_indices.items())
            )
            prewarmed = dict(warmed)
            vix_task = asyncio.create_task(
                asyncio.to_thread(self.data.get_ltp, "NSE", "INDIAVIX", "99926017")
            )
            for name, cfg in active_indices.items():
                token = cfg.get("token", "")
                exchange = cfg.get("exchange", "NSE")
                spot = 0.0

                try:
                    quote = prewarmed.get(name, {}).get("quote")
                    if isinstance(quote, Exception):
                        raise quote
                    spot = float((quote or {}).get("ltp", 0.0) or 0.0)
                    if spot > 0:
                        self.candles.update_latest_price(name, spot)
                        summary["index_quotes"] += 1
                except Exception as exc:
                    logger.debug(f"Morning refresh index quote failed for {name}: {exc}")

                if spot <= 0 and token:
                    try:
                        spot = await loop.run_in_executor(
                            None,
                            lambda n=name, t=token, e=exchange: self.data.get_latest_price_by_token(t, n, e),
                        )
                        if spot and spot > 0:
                            self.candles.update_latest_price(name, float(spot))
                            summary["index_quotes"] += 1
                    except Exception as exc:
                        logger.debug(f"Morning refresh fallback index LTP failed for {name}: {exc}")

                try:
                    previous_close = prewarmed.get(name, {}).get("previous_close")
                    if isinstance(previous_close, Exception):
                        raise previous_close
                except Exception as exc:
                    logger.debug(f"Morning refresh previous close failed for {name}: {exc}")

                info = (self.market_info or {}).get(name, {}) if isinstance(self.market_info, dict) else {}
                fut_symbol = info.get("current_fut") or ""
                fut_token = info.get("current_fut_token") or ""
                if fut_symbol and fut_token:
                    fut_exchange = "BFO" if name == "SENSEX" else "NFO"
                    try:
                        fut_ltp = await loop.run_in_executor(
                            None,
                            lambda e=fut_exchange, s=fut_symbol, t=fut_token: self.data.get_ltp(e, s, t),
                        )
                        if fut_ltp and fut_ltp > 0:
                            summary["futures"] += 1
                    except Exception as exc:
                        logger.debug(f"Morning refresh futures LTP failed for {name}: {exc}")

                if spot and spot > 0:
                    strike_interval = cfg.get("strike_interval", 50)
                    atm = round(float(spot) / strike_interval) * strike_interval
                    opt_exchange = cfg.get("option_exchange", "BFO" if name == "SENSEX" else "NFO")
                    for opt_type in ("CE", "PE"):
                        try:
                            opt_info = self.data.get_option_token(name, atm, opt_type, session_date)
                            if not opt_info:
                                opt_info = self.data.get_option_token(name, atm, opt_type)
                            if opt_info:
                                summary["option_tokens"] += 1
                                opt_ltp = await loop.run_in_executor(
                                    None,
                                    lambda e=opt_exchange, s=opt_info.get("symbol", ""), t=opt_info.get("token", ""): self.data.get_ltp(e, s, t),
                                )
                                if opt_ltp and opt_ltp > 0:
                                    token_key = str(opt_info.get("token", ""))
                                    self._premium_cache[token_key] = {"price": float(opt_ltp), "time": time.time()}
                                    self._premium_cache[f"{name}-{opt_type}"] = {"price": float(opt_ltp), "time": time.time()}
                                    summary["option_ltp"] += 1
                        except Exception as exc:
                            logger.debug(f"Morning refresh option {name} {opt_type} failed: {exc}")

                try:
                    chain_result = prewarmed.get(name, {}).get("chain_result")
                    if isinstance(chain_result, Exception):
                        raise chain_result
                    chain, quality = chain_result
                    if chain is not None and not chain.empty:
                        summary["chains"] += 1

                    candle_5min = self.candles.get_candles(name, "5min")
                    candle_1min = self.candles.get_candles(name, "1min")
                    latest_spot = float(spot or self.candles.get_latest_price(name) or 0.0)
                    if latest_spot > 0 and candle_5min is not None and not candle_5min.empty:
                        dte = 7
                        try:
                            dte = max(0, self.expiry.get_dte(name, session_date))
                        except Exception:
                            pass
                        strike_interval = cfg.get("strike_interval", 50)
                        intel_res = await loop.run_in_executor(
                            None,
                            lambda n=name, c5=candle_5min, c1=candle_1min, ch=chain, sp=latest_spot, q=quality, d=dte, si=strike_interval: self.intel.analyze(
                                instrument=n,
                                timeframe="5min",
                                candle_df=c5,
                                candle_1min_df=c1,
                                options_chain=ch,
                                spot_price=sp,
                                strike_interval=si,
                                days_to_expiry=d,
                                price_change_pct=0,
                                chain_quality=q,
                            ),
                        )
                        self._intel_cache[name] = intel_res
                        if not hasattr(self, "_intel_cache_time"):
                            self._intel_cache_time = {}
                        self._intel_cache_time[name] = time.time()
                        self._last_intel_fetch[name] = time.time()
                        summary["intel"] += 1
                except Exception as exc:
                    logger.debug(f"Morning refresh option chain/intel failed for {name}: {exc}")

            try:
                vix = await vix_task
                summary["vix"] = bool(vix and vix > 0)
                if summary["vix"]:
                    self._premium_cache["INDIAVIX"] = {"price": float(vix), "time": time.time()}
            except Exception as exc:
                logger.debug(f"Morning refresh VIX failed: {exc}")

            self._morning_refresh_last_date = session_day
            elapsed = time.time() - started
            msg = (
                "Morning refresh complete: "
                f"idx={summary['index_quotes']}/{len(active_indices)}, "
                f"fut={summary['futures']}, opt_tokens={summary['option_tokens']}, "
                f"opt_ltp={summary['option_ltp']}, chains={summary['chains']}, "
                f"intel={summary['intel']}, vix={'OK' if summary['vix'] else 'MISS'}, "
                f"{elapsed:.1f}s"
            )
            logger.info(msg)
            self.log_event(msg, "success")
        except Exception as exc:
            logger.error(f"Morning market refresh failed: {exc}")
            self.log_event(f"Morning refresh failed: {exc}", "error")
        finally:
            self._morning_refresh_inflight = False

    async def run(self):
        self.is_running = True
        self._event_loop = asyncio.get_running_loop()
        logger.info(f"ðŸš€ Scanner started â€” {len(self.active_indices)} active indices Ã— 3 TFs")

        # â• â• â•  Launch High-Priority Background Workers â• â• â•
        asyncio.create_task(self._background_ltp_worker())
        asyncio.create_task(self._background_intel_worker())
        asyncio.create_task(self._background_history_worker())
        asyncio.create_task(self._background_hft_worker())
        asyncio.create_task(self._background_recovery_checkpoint_worker())
        asyncio.create_task(self._background_signal_report_worker())
        asyncio.create_task(self._smart_health_monitor_loop())

        while self.is_running:
            try:
                # â• â• â•  CONTINUOUS SCANNING (Calculation Only - Fast Path) â• â• â•
                await self._scan_cycle()
                await asyncio.sleep(self.scan_interval)
            except Exception as e:
                import traceback
                logger.error(f"Scanner error: {e}\n{traceback.format_exc()}")
                backoff = max(
                    0.25,
                    float(getattr(get_settings(), "scanner_exception_backoff_seconds", 1.0) or 1.0),
                )
                await asyncio.sleep(backoff)

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

    async def _background_recovery_checkpoint_worker(self):
        """Persist a rolling crash-recovery checkpoint independent of scan completion."""
        while self.is_running:
            try:
                saved = self._save_warm_memory()
                if saved:
                    self._persist_session_trade_candidates()
                    save_state = getattr(getattr(self, "trades", None), "save_state", None)
                    if callable(save_state):
                        await asyncio.to_thread(save_state)
                    self._recovery_checkpoint_count = int(getattr(self, "_recovery_checkpoint_count", 0) or 0) + 1
                    self._last_recovery_checkpoint_at = datetime.now(IST).isoformat()
                    self._last_recovery_checkpoint_error = ""
                await asyncio.sleep(5)
            except Exception as exc:
                self._last_recovery_checkpoint_error = str(exc)
                logger.debug(f"Recovery checkpoint worker skipped: {exc}")
                await asyncio.sleep(5)

    async def _background_signal_report_worker(self):
        """Refresh today's missed/rejected signal report without blocking scans."""
        while self.is_running:
            try:
                await asyncio.to_thread(self.generate_daily_signal_report)
            except Exception as exc:
                logger.debug(f"Daily signal report refresh skipped: {exc}")
            await asyncio.sleep(300)

    async def _background_ltp_worker(self):
        """Constant high-priority LTP polling utilizing 85% of SmartAPI throughput"""
        while self.is_running:
            try:
                if not self.data.is_market_open() and not self.trades.open_trades:
                    await asyncio.sleep(5)
                    continue
                indices = {k: v for k, v in self.config.get("indices", {}).items() if k in self.active_indices}
                tasks = []
                for name, cfg in indices.items():
                    token = cfg.get("token")
                    if token:
                        tasks.append(self._fetch_ltp_raw(name, token, cfg.get("exchange", "NSE")))

                if tasks:
                    await asyncio.gather(*tasks)

                # Stagger to utilize ~15 pings/sec (Safe under 20/sec limit)
                await asyncio.sleep(0.2)
            except Exception as e:
                logger.debug(f"LTP Worker error: {e}")
                await asyncio.sleep(1)

    async def _fetch_ltp_raw(self, name, token, exchange="NSE"):
        try:
            loop = asyncio.get_event_loop()
            await self._ltp_rate_limiter.consume(1)
            async with self._ltp_semaphore:
                live_price = await loop.run_in_executor(
                    None,
                    lambda: self.data.get_latest_price_by_token(
                        token=token, symbol=name,
                        exchange=exchange
                    )
                )
                if live_price and live_price > 0:
                    self.candles.update_latest_price(name, live_price)
        except Exception as e: logger.debug(f"Ignored exception: {e}")

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
                            depth_context = self._get_depth_context(name, cfg)
                            chain_quality = {**chain_quality, "depth": depth_context}
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
                            self._record_bar_aligned_intelligence(name, options_chain, intel_res, chain_quality, depth_context)
                    except Exception as e: logger.debug(f"Ignored exception: {e}")
                    await asyncio.sleep(5) # Stagger between instruments
                await asyncio.sleep(25) # Main interval
            except Exception as e:
                await asyncio.sleep(5)

    async def _background_history_worker(self):
        """Background OHLCV sync. In REAL mode keep scans light and refresh deep candles separately."""
        while self.is_running:
            try:
                indices = {k: v for k, v in self.config.get("indices", {}).items() if k in self.active_indices}
                if self.mode == "HISTORICAL":
                    # Historical simulations fetch their required window during recalculation.
                    # Re-polling deep history every minute after that competes with scan cycles
                    # and has caused multi-second dashboard stalls.
                    await asyncio.sleep(max(300, self._data_fetch_interval))
                    continue
                if self.mode == "REAL":
                    await self._fetch_all_data(indices, timeframes=["1min"])
                    last_full = getattr(self, "_last_full_history_refresh", 0.0)
                    if time.time() - last_full >= 300.0:
                        await self._fetch_all_data(indices, timeframes=["5min", "15min"])
                        self._last_full_history_refresh = time.time()
                else:
                    await self._fetch_all_data(indices)
                await asyncio.sleep(self._data_fetch_interval)
            except Exception as e:

                logger.debug(f"Caught bare exception: {e}")
                await asyncio.sleep(10)


    async def _smart_health_monitor_loop(self):
        """Periodically emits system health and market regime logs."""
        import asyncio
        from datetime import datetime
        import time
        from config.settings import get_settings
        
        last_log_time = 0.0
        
        while getattr(self, "is_running", True):
            now = datetime.now(IST)
            # Market hours: 09:15 to 15:30 IST
            try:
                start_time = datetime.strptime("09:15", "%H:%M").time()
                end_time = datetime.strptime("15:30", "%H:%M").time()
                is_live = start_time <= now.time() <= end_time
            except Exception:
                is_live = False
                
            interval = 1800 if is_live else 7200 # 30 mins live, 2 hrs off-market
            
            current_time = time.time()
            if current_time - last_log_time >= interval:
                # 1. Regime State
                regimes = getattr(self, "latest_regimes", {})
                regime_str = ", ".join([f"{k}: {v}" for k, v in list(regimes.items())[:3]]) if regimes else "UNKNOWN"
                
                # 2. ADX Momentum
                adx_str = "Neutral"
                try:
                    if getattr(self, "_cached_results", None):
                        adxs = []
                        for k, v in self._cached_results.items():
                            if v and "adx" in v:
                                adxs.append(v["adx"])
                        if adxs:
                            avg_adx = sum(adxs) / len(adxs)
                            adx_str = f"Strong ({avg_adx:.1f})" if avg_adx >= 25 else (f"Building ({avg_adx:.1f})" if avg_adx >= 20 else f"Weak ({avg_adx:.1f})")
                except Exception:
                    pass
                    
                # 3. Trade Capacity
                open_trades_count = len(self.trade_manager.open_trades) if getattr(self, "trade_manager", None) and hasattr(self.trade_manager, "open_trades") else 0
                max_trades = getattr(get_settings(), "max_concurrent_positions", 4)
                
                # 4. Data Health (latency)
                ltp_health = "OK"
                last_ltp = getattr(self.data, "_last_ltp_time", 0.0) if hasattr(self, "data") else 0.0
                if current_time - last_ltp > 60:
                    ltp_health = "STALE"
                
                log_msg = f"System Health: {regime_str} | ADX: {adx_str} | Capacity: {open_trades_count}/{max_trades} | Data: {ltp_health}"
                self.log_event(log_msg, "system")
                last_log_time = current_time
                
            await asyncio.sleep(60) # Wake up every minute to check if interval has passed

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
            "time": datetime.now(IST).strftime("%H:%M:%S"),
            "msg": f"{icon} {message}",
            "type": type
        })
        self._persist_rejected_signal_event(message, type)

    def _persist_rejected_signal_event(self, message: str, event_type: str = "trade") -> None:
        """Persist signal rejection/block events for the daily missed-signal report."""
        if getattr(self, "mode", "") == "HISTORICAL" or getattr(self, "is_warmup", False):
            return
        text = str(message or "").strip()
        lowered = text.lower()
        decision_terms = (
            "signal rejected",
            "entry blocked",
            "signal blocked",
            "discarded signal",
            "signal discarded",
            "skipping buffering",
            "discarding matured signal",
            "repainted during",
            "repaint guard",
            "stale buffered signal",
            "future-timestamp signal",
            "signal waiting",
            "signal ignored",
            "concurrency guard: blocked",
            "live gate: blocked",
        )
        if not any(term in lowered for term in decision_terms):
            return

        now = datetime.now(IST)
        instrument = next(
            (
                name
                for name in ("MIDCPNIFTY", "BANKNIFTY", "SENSEX", "NIFTY")
                if re.search(rf"\b{name}\b", text, flags=re.IGNORECASE)
            ),
            "",
        )
        tf_match = re.search(r"\b(1min|5min|15min)\b", text, flags=re.IGNORECASE)
        direction_match = re.search(r"\b(LONG|SHORT|BUY|SELL)\b", text, flags=re.IGNORECASE)
        status = "REJECTED" if "reject" in lowered or "blocked" in lowered else "MISSED"
        reason = text.rsplit(" - ", 1)[-1] if " - " in text else text
        event_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{now.date().isoformat()}|{instrument}|{tf_match.group(1) if tf_match else ''}|"
                f"{direction_match.group(1) if direction_match else ''}|{text}",
            )
        )
        try:
            db.save_signal_decision(
                {
                    "id": event_id,
                    "date": now.date().isoformat(),
                    "timestamp": now.isoformat(),
                    "instrument": instrument,
                    "timeframe": tf_match.group(1).lower() if tf_match else "",
                    "direction": direction_match.group(1).upper() if direction_match else "",
                    "status": status,
                    "reason": reason,
                    "message": text,
                    "source": f"activity:{event_type}",
                }
            )
        except Exception as exc:
            logger.debug(f"Signal decision persistence skipped: {exc}")

    def log_signal_decision_once(self, key: str, message: str, type: str = "trade"):
        """Log live signal decisions without spamming the activity panel every scan."""
        seen = getattr(self, "_activity_signal_decision_keys", None)
        if seen is None:
            self._activity_signal_decision_keys = set()
            seen = self._activity_signal_decision_keys
        if key in seen:
            return
        seen.add(key)
        if len(seen) > 1000:
            self._activity_signal_decision_keys = set(list(seen)[-500:])
        self.log_event(message, type)

    def _record_live_inflight_skip(self, instrument: str) -> None:
        """Raise a visible warning when live scans repeatedly skip an instrument."""
        counts = getattr(self, "_live_inflight_skip_counts", None)
        if counts is None:
            self._live_inflight_skip_counts = {}
            counts = self._live_inflight_skip_counts
        counts[instrument] = counts.get(instrument, 0) + 1

        if counts[instrument] < 3:
            return

        now = time.time()
        last_alerts = getattr(self, "_live_inflight_last_alert", None)
        if last_alerts is None:
            self._live_inflight_last_alert = {}
            last_alerts = self._live_inflight_last_alert
        if now - last_alerts.get(instrument, 0.0) < 60.0:
            return

        last_alerts[instrument] = now
        msg = (
            f"Live scan backlog: {instrument} skipped {counts[instrument]} times "
            "because the previous processing task is still running. Reversal detection may be delayed."
        )
        logger.warning(msg)
        self.log_event(msg, "warning")
        self._notify(msg, "warning")

    def _clear_live_inflight_skip(self, instrument: str) -> None:
        counts = getattr(self, "_live_inflight_skip_counts", None)
        if isinstance(counts, dict):
            counts.pop(instrument, None)

    def _record_live_processing_timeout(self, instrument: str, timeout_seconds: float) -> None:
        now = time.time()
        last_alerts = getattr(self, "_live_inflight_last_alert", None)
        if last_alerts is None:
            self._live_inflight_last_alert = {}
            last_alerts = self._live_inflight_last_alert
        key = f"{instrument}:timeout"
        if now - last_alerts.get(key, 0.0) < 60.0:
            return

        last_alerts[key] = now
        msg = (
            f"Live scan timeout: {instrument} did not finish within {timeout_seconds:.1f}s. "
            "Signal and reversal detection may be stale until processing catches up."
        )
        logger.warning(msg)
        self.log_event(msg, "warning")
        self._notify(msg, "warning")

    def _build_live_fallback_result(self, name: str, cfg: Dict, reason: str) -> Tuple[Dict, List[TradeCandidate]]:
        """Keep live monitoring alive when the full analysis worker is stale."""
        try:
            self._check_session_candidates_breaches(name)
        except Exception as exc:
            logger.debug(f"Fallback session breach check failed for {name}: {exc}")

        display_candidates = self._get_session_trade_candidates(name)
        if not display_candidates:
            display_candidates = (
                self._latest_exit_candidates.get(name, [])
                + self._latest_trade_candidates.get(name, [])
            )
        try:
            self._refresh_trade_candidates(display_candidates, cfg)
        except Exception as exc:
            logger.debug(f"Fallback candidate refresh failed for {name}: {exc}")

        prev_ui = {}
        try:
            prev_ui = ((self.latest_results or {}).get("instruments") or {}).get(name) or {}
        except Exception:
            prev_ui = {}

        ui_data = dict(prev_ui) if isinstance(prev_ui, dict) else {}
        spot = self.candles.get_latest_price(name)
        if (not spot or spot <= 0) and isinstance(prev_ui, dict):
            spot = float(prev_ui.get("spot_price") or prev_ui.get("ltp") or 0.0)

        ui_data.setdefault("mtf", {})
        ui_data.setdefault("intelligence", {})
        ui_data.setdefault("chart", {})
        ui_data["ltp"] = float(spot or 0.0)
        ui_data["spot_price"] = float(spot or 0.0)
        ui_data["trade_candidates"] = [self._serialize_trade_candidate(c) for c in display_candidates]
        ui_data["fallback_active"] = True
        ui_data["fallback_reason"] = reason
        ui_data["quote_source"] = "fallback_cache"
        ui_data["error"] = reason
        return ui_data, []

    def _live_utbot_signal_still_present(self, candidate: TradeCandidate, sig_timestamp) -> bool:
        """True when the UTBot signal with the same timestamp still matches this candidate."""
        cached_res = self._cached_results.get(f"{candidate.instrument}_{candidate.timeframe}") or {}
        signals = list(cached_res.get("signals") or [])
        if not signals or sig_timestamp is None:
            return False

        target_type = getattr(candidate, "source_signal_type", None) or ("BUY" if candidate.direction == "LONG" else "SELL")
        # Repaint stabilization is identity-only: original timestamp + type.
        # Price can move during the stabilization window and is handled later
        # by candidate selection/execution pricing, not repaint validation.
        match_sig = None
        for s in signals:
            s_ts = getattr(s, "timestamp", None)
            if s_ts:
                t1 = s_ts.replace(tzinfo=None)
                t2 = sig_timestamp.replace(tzinfo=None)
                if abs((t1 - t2).total_seconds()) <= 1.0:
                    match_sig = s
                    break

        if not match_sig:
            # The signal at this timestamp has disappeared (repainted)
            return False

        # Verify the type/direction still matches. Price is intentionally ignored.
        if getattr(match_sig, "signal_type", None) != target_type:
            return False

        return True

    def _live_signal_candle_has_closed(
        self,
        candidate: TradeCandidate,
        sig_timestamp,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return whether the source candle has closed for repaint tracking."""
        if sig_timestamp is None:
            return False

        tf_minutes = {"1min": 1, "5min": 5, "15min": 15}.get(
            str(getattr(candidate, "timeframe", "") or ""),
            5,
        )

        sig_time = sig_timestamp
        if getattr(sig_time, "tzinfo", None) is not None:
            sig_time = sig_time.astimezone(IST).replace(tzinfo=None)

        if now is None:
            now = self._session_squareoff_clock()
        if getattr(now, "tzinfo", None) is not None:
            now = now.astimezone(IST).replace(tzinfo=None)

        return now >= (sig_time + timedelta(minutes=tf_minutes))

    def _calculation_lock_timeout(self) -> float:
        live_timeout = max(
            2.0,
            float(
                getattr(
                    get_settings(),
                    "live_calculation_lock_timeout_seconds",
                    12.0,
                )
                or 12.0
            ),
        )
        if self.mode == "HISTORICAL" or getattr(self, "is_warmup", False):
            return max(75.0, live_timeout)
        return live_timeout

    def _closed_market_idle_allowed(self) -> bool:
        """Avoid recomputing unchanged charts when REAL mode has nothing active."""
        if getattr(self, "mode", "") != "REAL":
            return False
        if not getattr(self, "_initial_setup_completed", False):
            return False
        if getattr(getattr(self, "trades", None), "open_trades", {}):
            return False
        for instrument in (getattr(self, "_session_trade_candidates", {}) or {}):
            if self._active_session_entry_candidates(instrument):
                return False
        return True

    def _prune_runtime_caches(self, force: bool = False) -> int:
        """Physically evict expired fast-path entries instead of waiting for daily reset."""
        now = time.time()
        interval = max(
            5.0,
            float(
                getattr(
                    get_settings(),
                    "runtime_cache_prune_interval_seconds",
                    60.0,
                )
                or 60.0
            ),
        )
        if not force and now - getattr(self, "_last_runtime_cache_prune", 0.0) < interval:
            return 0
        self._last_runtime_cache_prune = now

        removed = 0
        timed_caches = (
            (getattr(self, "_premium_cache", {}), max(10.0, interval * 2.0)),
            (getattr(self, "_candidate_ltp_cache", {}), max(10.0, interval * 2.0)),
            (getattr(self, "_candidate_process_cache", {}), max(30.0, interval * 2.0)),
        )
        for cache, max_age in timed_caches:
            for key, value in list(cache.items()):
                stamp = value.get("time", 0.0) if isinstance(value, dict) else 0.0
                if stamp <= 0 or now - stamp > max_age:
                    cache.pop(key, None)
                    removed += 1

        option_cache = getattr(getattr(self, "signal_processor", None), "_option_history_cache", None)
        if isinstance(option_cache, dict) and len(option_cache) > 500:
            overflow = len(option_cache) - 500
            for key in list(option_cache)[:overflow]:
                option_cache.pop(key, None)
                removed += 1
        return removed

    async def _scan_cycle(self):
        # ═══ LOCK WATCHDOG (Must be at the very top to break deadlocks) ═══
        if getattr(self, "_calculation_lock", False):
            if (
                hasattr(self, "_lock_time")
                and time.time() - self._lock_time > self._calculation_lock_timeout()
            ):
                logger.error(
                    f"Calculation lock exceeded {self._calculation_lock_timeout():.1f}s. "
                    "Releasing the stale scan gate."
                )
                self._calculation_lock = False
            else:
                # logger.warning("🚫 Scan cycle blocked: Calculation lock active.")
                return

        self.scan_count += 1
        t0 = time.time()
        self.risk_manager.check_circuit_breaker()
        results = {"timestamp": datetime.now(IST).isoformat(), "instruments": {}, "activity_log": self.activity_log}

        # Filter indices based on active_indices setting
        all_indices = self.config.get("indices", {})
        indices = {k: v for k, v in all_indices.items() if k in self.active_indices}

        if self.scan_count == 1 and not self._initial_setup_completed:
            self.log_event("📥 Initializing deep history fetch & warm startup in background...", "system")
            # Run the heavy initialization in the background to prevent screen freeze
            self.is_warmup = True
            self._schedule_initial_setup(indices)

        # ══ DAILY SESSION RESET (At 09:00 AM IST) ══
        cycle_is_warmup = getattr(self, "is_warmup", False)

        now_ist = datetime.now(IST)
        self._prune_runtime_caches()
        self._rollover_session_if_needed(now_ist)
        if False and now_ist.hour == 9 and now_ist.minute == 0:
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

        if self.mode == "REAL" and not cycle_is_warmup and self._ensure_market_session_refresh(indices, now_ist):
            self.is_calculating = False
            return

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
            self._update_dashboard_cache(results)
            self._save_warm_memory()
            if self.on_update:
                try: await self.on_update(results)
                except Exception as e:
                    logger.error(f"Failed to broadcast update: {e}")
            return

        # Market Status Tagging
        market_status = "OPEN" if is_market_open else "CLOSED"
        results["market_status"] = market_status

        if not is_market_open and self._closed_market_idle_allowed():
            previous = self.latest_results if isinstance(self.latest_results, dict) else {}
            results.update({
                "instruments": dict(previous.get("instruments") or {}),
                "system_power": self.system_power,
                "is_calculating": False,
                "mode": self.mode,
                "gateway_status": self.data.get_source_health(),
                "config": self.get_broadcast_config(),
                "trades": self._build_dashboard_trade_payload(),
                "scan_count": self.scan_count,
                "latency": round((time.time() - t0) * 1000),
                "timestamp": datetime.now(IST).isoformat(),
                "simulation_id": self.simulation_id,
            })
            self.latest_results = results
            self._update_dashboard_cache(results)
            self.last_scan_time = datetime.now(IST)
            self.is_calculating = False
            if self.on_update and time.time() - getattr(self, "_last_broadcast_time", 0) >= 1.0:
                self._last_broadcast_time = time.time()
                await self.on_update(results)
            return

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
                    timeout_seconds = 60.0 if (self.mode == "HISTORICAL" or cycle_is_warmup) else float(getattr(get_settings(), "live_index_timeout_seconds", 6.0) or 6.0)
                    existing_future = getattr(self, "_live_processing_futures", {}).get(name)
                    if (
                        self.mode != "HISTORICAL"
                        and not cycle_is_warmup
                        and existing_future is not None
                        and not existing_future.done()
                    ):
                        self._diag_inc("timeouts", f"{name}_skipped_inflight")
                        self._diag_inc("timeouts", "skipped_inflight")
                        logger.warning(f"Skipping {name}: previous processing task is still running.")
                        self._record_live_inflight_skip(name)
                        return name, self._build_live_fallback_result(name, cfg, "Previous processing task still running")

                    spot = self.candles.get_latest_price(name)
                    if spot <= 0:
                        spot = await asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda: self.data.get_ltp(cfg.get("exchange", "NSE"), name, cfg.get("token", "")),
                        )
                        if spot and spot > 0:
                            self.candles.update_latest_price(name, spot)

                    loop = asyncio.get_event_loop()
                    try:
                        processing_future = loop.run_in_executor(None, lambda: self._process_instrument(name, cfg))
                        if self.mode != "HISTORICAL" and not cycle_is_warmup:
                            self._live_processing_futures[name] = processing_future
                            self._live_processing_started[name] = time.time()

                            def _clear_live_future(done_future, inst=name):
                                if getattr(self, "_live_processing_futures", {}).get(inst) is done_future:
                                    self._live_processing_futures.pop(inst, None)
                                    self._live_processing_started.pop(inst, None)

                            processing_future.add_done_callback(_clear_live_future)

                        ui_data, candidate = await asyncio.wait_for(
                            asyncio.shield(processing_future),
                            timeout=timeout_seconds,
                        )
                        if self.mode != "HISTORICAL" and not cycle_is_warmup:
                            self._clear_live_inflight_skip(name)
                        return name, (ui_data, candidate)
                    except asyncio.TimeoutError:
                        self._diag_inc("timeouts", name)
                        logger.error(f"Timeout processing {name}! Signal generation might be hanging.")
                        if self.mode != "HISTORICAL" and not cycle_is_warmup:
                            self._record_live_processing_timeout(name, timeout_seconds)
                        return name, self._build_live_fallback_result(name, cfg, "Timeout")
                    except Exception as e:
                        logger.error(f"Error processing {name}: {e}")
                        return name, ({"error": str(e)}, [])
                except Exception as e:
                    logger.error(f"Outer error processing {name}: {e}")
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
                        msg = (
                            f"Ignoring stale REAL-mode signal: {candidate.instrument} "
                            f"{candidate.direction} {candidate.timeframe} @ {signal_ts}. "
                            "It was no longer fresh when received."
                        )
                        logger.warning(msg)
                        ts_key = signal_ts.isoformat(timespec="seconds") if signal_ts else ""
                        self.log_signal_decision_once(
                            f"{candidate.instrument}|{candidate.timeframe}|{candidate.direction}|{ts_key}|stale-real",
                            msg,
                            "trade",
                        )
                        continue
                    fresh_candidates.append(candidate)
                candidates = fresh_candidates

            if candidates or self._pending_live_signals:
                if self.mode != "HISTORICAL":
                    display_only_candidates = [
                        c for c in candidates
                        if getattr(c, "action", "ENTRY") == "NO_ENTRY" or (not self.data.is_market_open() and not getattr(self, "is_warmup", False))
                    ]
                    if display_only_candidates:
                        for display_candidate in display_only_candidates:
                            self._remember_filtered_candidate(display_candidate)
                    candidates = [
                        c for c in candidates
                        if getattr(c, "action", "ENTRY") != "NO_ENTRY" and (self.data.is_market_open() or getattr(self, "is_warmup", False))
                    ]
                await self._coordinate_and_execute(candidates)
            self._refresh_session_signal_payloads(results)

            # Update active trades (PnL and Trailing Stops)
            self._update_active_trades()

            elapsed = time.time() - t0
            latency_ms = round(elapsed * 1000)
            self._diagnostics.setdefault("latency", {})["last_ms"] = latency_ms
            if latency_ms >= 5000 or self.scan_count % 20 == 0:
                self._publish_system_performance_alerts(latency_ms=latency_ms)

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
                    if not getattr(self, "chart_stream_enabled", True):
                        ui_data["chart"] = {}
                    elif not is_full_chart_needed:
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
                "diagnostics": self._get_diagnostics_payload(),
                "trades": self._build_dashboard_trade_payload(),
                "scan_count": self.scan_count,
                "latency": latency_ms,
                "timestamp": datetime.now(IST).isoformat(),
                "simulation_id": self.simulation_id
            })
            self.latest_results = results
            self._update_dashboard_cache(results)
            self.last_scan_time = datetime.now(IST)
            self._last_sent_sim_id = self.simulation_id
            self._save_warm_memory()

            if latency_ms > 10000 and not cycle_is_warmup:
                self._diag_inc("latency", "high_latency_events")
                logger.warning(f"⚠️ High Latency: {latency_ms}ms")
                
            if latency_ms > 30000 and self.scan_count > 2 and not getattr(self, "is_warmup", False):
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
    async def _fetch_all_data(self, indices: Dict, force=False, timeframes: Optional[List[str]] = None, days_back_override: Optional[int] = None):
        """Fetch data for all instruments, only when cache expired"""
        now = time.time()
        tasks = []
        selected_timeframes = timeframes or ["1min", "5min", "15min"]
        for name, cfg in indices.items():
            for tf in selected_timeframes:
                cache_key = f"{name}_{tf}"
                last = self._last_data_fetch.get(cache_key, 0)
                if force or now - last >= self._data_fetch_interval:
                    tasks.append(
                        self._fetch_one(
                            name,
                            cfg,
                            tf,
                            days_back_override=days_back_override,
                        )
                    )
                    self._last_data_fetch[cache_key] = now

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_one(
        self,
        name,
        cfg,
        tf,
        days_back_override: Optional[int] = None,
    ):
        """Fetch one instrument-timeframe from Yahoo (in thread pool)"""
        loop = asyncio.get_event_loop()
        # Use instance-specific settings (reflects UI changes)
        user_days = (
            int(days_back_override)
            if days_back_override is not None
            else getattr(self, "backtest_days", 2)
        )

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
                await self._history_rate_limiter.consume(1)
                # Keep background history below broker burst limits while live scans run.
                await asyncio.sleep(0.75)
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
        today = datetime.now(IST).date()
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
        expiry_aligned = bool(meta.get("expiry_aligned", meta.get("source") != "fyers"))
        if meta.get("source") == "fyers" and not expiry_aligned:
            score = min(score, 50)
        return {
            "instrument": name,
            "source": meta.get("source", "unknown" if strike_count else "none"),
            "strike_count": strike_count,
            "age_seconds": round(age, 1),
            "score": score,
            "fallback": fallback,
            "stale": False,
            "active_expiry": meta.get("active_expiry", ""),
            "expiry_aligned": expiry_aligned,
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

    def _get_depth_context(self, name: str, cfg: Dict) -> Dict[str, Any]:
        getter = getattr(self.data, "get_order_book_snapshot", None)
        if not callable(getter):
            return {"ofr": 1.0, "source": "unavailable", "usable": False}
        try:
            depth = getter(cfg.get("exchange", "NSE"), name, str(cfg.get("token", "")))
            if isinstance(depth, dict) and depth:
                return depth
        except Exception as exc:
            logger.debug(f"Depth/OFR fetch skipped for {name}: {exc}")
        return {"ofr": 1.0, "source": "unavailable", "usable": False}

    def _record_bar_aligned_intelligence(
        self,
        name: str,
        options_chain: Optional[pd.DataFrame],
        intel_result: Dict[str, Any],
        chain_quality: Dict[str, Any],
        depth_context: Dict[str, Any],
    ) -> None:
        try:
            self.intel_snapshots.push_snapshot(
                name,
                options_chain,
                intel_result=intel_result,
                depth=depth_context,
                quality=chain_quality,
            )
            for tf in ("1min", "5min", "15min"):
                tf_df = self.candles.get_candles(name, tf)
                if tf_df is not None and not tf_df.empty:
                    self.intel_snapshots.freeze_bar(name, tf, tf_df.index[-1])
        except Exception as exc:
            logger.debug(f"Bar-aligned intelligence snapshot skipped for {name}: {exc}")

    def _intel_reversal_reason(self, direction: str, intel_result: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(intel_result, dict):
            return None
        pcr = intel_result.get("pcr", {}) if isinstance(intel_result.get("pcr"), dict) else {}
        oi = intel_result.get("oi", {}) if isinstance(intel_result.get("oi"), dict) else {}
        flow = intel_result.get("order_flow", {}) if isinstance(intel_result.get("order_flow"), dict) else {}

        pcr_signal = str(pcr.get("contrarian_signal") or pcr.get("signal") or "NEUTRAL").upper()
        oi_signal = str(oi.get("signal") or "NEUTRAL").upper()
        flow_signal = str(flow.get("signal") or "NEUTRAL").upper()
        pcr_value = float(pcr.get("primary_pcr") or pcr.get("pcr_oi") or 0.0)
        flow_ratio = float(flow.get("ratio") or flow.get("buy_sell_ratio") or 1.0)

        bearish_votes = sum(
            [
                pcr_signal in {"BEARISH", "STRONG_BEARISH"},
                oi_signal == "BEARISH",
                flow_signal == "BEARISH" or flow_ratio <= 0.85,
            ]
        )
        bullish_votes = sum(
            [
                pcr_signal in {"BULLISH", "STRONG_BULLISH"},
                oi_signal == "BULLISH",
                flow_signal == "BULLISH" or flow_ratio >= 1.15,
            ]
        )

        direction = str(direction or "").upper()
        # Relaxed threshold: require 3 votes (full consensus) instead of 2 to prevent premature exits
        if direction == "LONG" and bearish_votes >= 3:
            return f"OI_PCR_REVERSAL bearish votes={bearish_votes} PCR={pcr_value:.2f} OFR={flow_ratio:.2f}"
        if direction == "SHORT" and bullish_votes >= 3:
            return f"OI_PCR_REVERSAL bullish votes={bullish_votes} PCR={pcr_value:.2f} OFR={flow_ratio:.2f}"
        return None

    def _get_live_premium_cached(self, trade) -> Optional[float]:
        if not getattr(trade, "symbol_token", "") or not getattr(trade, "trading_symbol", ""):
            return None
        settings = get_settings()
        ttl = float(getattr(settings, "option_premium_cache_ttl_seconds", 2.0))
        key = trade.symbol_token
        now = time.time()
        cached = self._premium_cache.get(key)
        if cached and now - cached.get("time", 0.0) <= ttl:
            cached_price = float(cached.get("price") or 0.0)
            if self._option_premium_quote_plausible(trade, cached_price):
                return cached_price
            self._premium_cache.pop(key, None)
        exchange = "BFO" if "SENSEX" in trade.trading_symbol.upper() else "NFO"
        try:
            price = self.data.get_ltp(exchange, trade.trading_symbol, trade.symbol_token)
            if price and price > 0:
                price = float(price)
                if self._option_premium_quote_plausible(trade, price):
                    self._premium_cache[key] = {"price": price, "time": now}
                    return price
                self._log_bad_option_premium_once(trade, price, "live premium fetch")
        except Exception as e:
            logger.debug(f"Premium fetch failed for {trade.trading_symbol}: {e}")
        fallback = float(cached.get("price") or 0.0) if cached else 0.0
        return fallback if fallback > 0 and self._option_premium_quote_plausible(trade, fallback) else None

    def _option_premium_quote_plausible(self, trade, premium: float) -> bool:
        """Reject option quotes that are clearly underlying/index prices."""
        try:
            premium = float(premium or 0.0)
        except (TypeError, ValueError):
            return False
        if premium <= 0:
            return False
        inst_type = str(getattr(trade, "inst_type", "") or "").upper()
        symbol = str(getattr(trade, "trading_symbol", "") or "").upper()
        if inst_type != "OPT" and "CE" not in symbol and "PE" not in symbol:
            return True

        entry = float(getattr(trade, "entry_price", getattr(trade, "price", 0.0)) or 0.0)
        target = float(getattr(trade, "target", 0.0) or 0.0)
        spot = (
            float(self.candles.get_latest_price(getattr(trade, "instrument", "")) or 0.0)
            if hasattr(self, "candles")
            else 0.0
        )
        if spot <= 0:
            spot = float(getattr(trade, "entry_spot", 0.0) or 0.0)
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
            bounds.append((intrinsic or 0.0) + spot * 0.05)
            bounds.append(spot * 0.12)
        upper_bound = max(bounds)
        return premium <= upper_bound

    def _sanitize_option_mark(self, trade, premium: float, spot: float, source: str) -> float:
        """Keep every option mark in premium space before P&L or exit logic sees it."""
        premium = float(premium or 0.0)
        if self._option_premium_quote_plausible(trade, premium):
            return premium

        self._log_bad_option_premium_once(trade, premium, source)
        entry = float(getattr(trade, "entry_price", getattr(trade, "price", 0.0)) or 0.0)
        entry_spot = float(getattr(trade, "entry_spot", 0.0) or 0.0)
        multiplier = float(getattr(trade, "instrument_multiplier", 0.0) or 0.0)
        if entry > 0 and entry_spot > 0 and spot > 0 and multiplier != 0:
            fallback = max(0.05, entry + ((float(spot) - entry_spot) * multiplier))
            if self._option_premium_quote_plausible(trade, fallback):
                return fallback
        return max(0.05, entry)

    def _allow_inferred_signal_recovery(self) -> bool:
        """Actual trades recover from SQLite/broker state, never from a chart arrow."""
        return False

    def _log_bad_option_premium_once(self, trade, premium: float, source: str) -> None:
        key = (
            f"{getattr(trade, 'instrument', '')}|{getattr(trade, 'trading_symbol', '')}|"
            f"{getattr(trade, 'symbol_token', '')}|{int(float(premium or 0.0))}|bad-option-premium"
        )
        self.log_signal_decision_once(
            key,
            (
                f"Bad option LTP ignored: {getattr(trade, 'instrument', '')} "
                f"{getattr(trade, 'trading_symbol', '')} returned {float(premium):.2f} from {source}; "
                "treated as instrument mismatch/underlying-price contamination"
            ),
            "warning",
        )

    def _get_live_fut_ltp_cached(self, trade) -> Optional[float]:
        if not getattr(trade, "symbol_token", "") or not getattr(trade, "trading_symbol", ""):
            return None
        ttl = 2.0
        key = f"FUT:{trade.symbol_token}"
        now = time.time()
        cached = self._candidate_ltp_cache.get(key)
        if cached and now - cached.get("time", 0.0) <= ttl:
            return cached.get("price")

        exchange = "BFO" if "SENSEX" in trade.trading_symbol.upper() else "NFO"
        try:
            price = self.data.get_ltp(exchange, trade.trading_symbol, trade.symbol_token)
            if price and price > 0:
                self._candidate_ltp_cache[key] = {"price": float(price), "time": now}
                return float(price)
        except Exception as e:
            logger.debug(f"FUT LTP fetch failed for {trade.trading_symbol}: {e}")
        return cached.get("price") if cached else None

    def _execution_levels(self, candidate: TradeCandidate) -> Tuple[float, float, float]:
        """Resolve contract-space entry, stop, and target levels for execution."""
        entry = float(candidate.price or 0.0)
        stop = float(candidate.stop or 0.0)
        target = float(candidate.target or 0.0)
        inst_type = str(getattr(candidate, "inst_type", "FUT") or "FUT").upper()

        if inst_type == "OPT":
            live_premium = self._get_live_premium_cached(candidate)
            if not live_premium or live_premium <= 0:
                return entry, stop, target
            entry = float(live_premium)
            stop = entry * (1 - (float(getattr(self.trades, "options_sl_pct", 15.0)) / 100.0))
            target_move = abs(float(candidate.target or 0.0) - float(candidate.price or 0.0))
            target_move *= float(getattr(candidate, "multiplier", 1.0) or 1.0)
            target = entry + target_move if target_move > 0 else 0.0
            return entry, stop, target

        live_future = self._get_live_fut_ltp_cached(candidate)
        if not live_future or live_future <= 0:
            return entry, stop, target

        risk_distance = abs(float(candidate.price or 0.0) - float(candidate.stop or 0.0))
        reward_distance = abs(float(candidate.target or 0.0) - float(candidate.price or 0.0))
        entry = float(live_future)
        if str(candidate.direction or "LONG").upper() == "SHORT":
            stop = entry + risk_distance
            target = entry - reward_distance if reward_distance > 0 else 0.0
        else:
            stop = entry - risk_distance
            target = entry + reward_distance if reward_distance > 0 else 0.0
        return entry, stop, target

    @staticmethod
    def _fut_price_from_spot_level(trade, spot_level: float) -> float:
        """Convert a spot-index level into this trade's futures price space."""
        if not spot_level or spot_level <= 0:
            return float(spot_level or 0.0)
        entry_spot = float(getattr(trade, "entry_spot", 0.0) or 0.0)
        entry_price = float(getattr(trade, "entry_price", getattr(trade, "price", 0.0)) or 0.0)
        if entry_spot > 0 and entry_price > 0:
            return entry_price + (float(spot_level) - entry_spot)
        return float(spot_level)

    @staticmethod
    def _candidate_price_from_spot_level(candidate: TradeCandidate, spot_level: float) -> float:
        """Map an index level into a candidate's contract-price space."""
        if not spot_level or spot_level <= 0:
            return 0.0
        if str(getattr(candidate, "inst_type", "FUT") or "FUT").upper() != "OPT":
            return Scanner._fut_price_from_spot_level(candidate, spot_level)

        entry_spot = float(getattr(candidate, "entry_spot", 0.0) or 0.0)
        entry_price = float(getattr(candidate, "price", 0.0) or 0.0)
        if entry_spot <= 0 or entry_price <= 0:
            return 0.0
        multiplier = float(getattr(candidate, "multiplier", 0.5) or 0.5)
        return entry_price + ((float(spot_level) - entry_spot) * multiplier)

    @staticmethod
    def _candidate_trailing_stop_is_valid(
        candidate: TradeCandidate,
        stop_level: float,
        current_price: float,
    ) -> bool:
        """Reject mapped stops that land beyond the live contract price."""
        if stop_level <= 0 or current_price <= 0:
            return False
        if str(getattr(candidate, "direction", "LONG") or "LONG").upper() == "SHORT":
            return stop_level >= current_price
        return stop_level <= current_price

    def _live_analysis_window(self, df: Optional[pd.DataFrame], timeframe: str) -> Optional[pd.DataFrame]:
        """Keep scans focused on the active window plus indicator warmup bars."""
        if df is None or df.empty:
            return df
        try:
            if self.mode == "HISTORICAL":
                active_dates = sorted(set(df.index.date))
                if not active_dates:
                    return df
                trade_days = max(1, int(getattr(self, "backtest_days", 1) or 1))
                trade_cutoff = active_dates[-min(len(active_dates), trade_days)]
                trade_df = df[pd.Series(df.index.date, index=df.index) >= trade_cutoff]
                if trade_df.empty:
                    return df.tail({"1min": 720, "5min": 240, "15min": 120}.get(timeframe, 240))
                warmup_bars = {"1min": 180, "5min": 60, "15min": 40}.get(timeframe, 60)
                warmup_df = df[df.index < trade_df.index[0]].tail(warmup_bars)
                window = pd.concat([warmup_df, trade_df])
                return window[~window.index.duplicated(keep="last")].sort_index()

            if self.mode != "REAL":
                return df

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
        market_quote = {}
        if hasattr(self.data, "get_index_quote"):
            try:
                market_quote = self.data.get_index_quote(name) or {}
            except Exception as exc:
                logger.debug(f"Index quote fetch failed for {name}: {exc}")

        quote_spot = float(market_quote.get("ltp", 0.0) or 0.0)
        if quote_spot > 0:
            self.candles.update_latest_price(name, quote_spot)

        spot = quote_spot if quote_spot > 0 else self.candles.get_latest_price(name)
        if spot <= 0:
            logger.warning(f"âš ï¸  Spot price for {name} is {spot}. Skipping scan.")
            return {"error": f"Invalid spot price: {spot}"}, []

        # â• â•  Daily Change (Optimized: Use Cache) â• â•
        quote_change = float(market_quote.get("change", 0.0) or 0.0)
        quote_change_pct = float(market_quote.get("change_pct", 0.0) or 0.0)
        if quote_change or quote_change_pct:
            change_points = quote_change
            change_pct = quote_change_pct
        else:
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
        live_spot = quote_spot if quote_spot > 0 else self.candles.get_latest_price(name)
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
        depth_context = self._get_depth_context(name, cfg)
        chain_quality = {**chain_quality, "depth": depth_context}
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
        self._record_bar_aligned_intelligence(name, chain, intel_result, chain_quality, depth_context)

        # â• â• â•  Record Intelligence Memory â• â• â•
        scan_ts = time.time()
        self.intel_memory.record(name, scan_ts, intel_result)
        if self.scan_count % 300 == 0: self._schedule_intel_memory_save() # Periodically persist

        intel_score = normalize_intelligence_score(
            intel_result.get("aggregate", {}).get("score", 0)
        )
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
        has_forming_signal = self._mtf_has_forming_signal(mtf_result)
        if (
            self.mode == "REAL"
            and not has_forming_signal
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
            if self.mode == "REAL" and not has_forming_signal:
                self._candidate_process_cache[name] = {
                    "key": decision_key,
                    "time": now,
                    "candidates": candidates,
                }
        source_snapshot = self.data.get_market_source_snapshot(name)
        candidates = self._filter_live_candidates_by_source(name, candidates, source_snapshot)
        display_candidates = self._get_session_trade_candidates(name)
        if not display_candidates:
            display_candidates = (
                self._latest_exit_candidates.get(name, [])
                + self._latest_trade_candidates.get(name, [])
            )
        self._refresh_trade_candidates(display_candidates, cfg)
        # Use first candidate for RR display if any
        primary_candidate = candidates[0] if candidates else None

        # â• â• â•  Build chart from CACHED results â€” NO re-processing â• â• â•
        chart_data = self._build_chart_from_cache(name) if getattr(self, "chart_stream_enabled", True) else {}

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
            "quote_source": market_quote.get("source", "unknown"),
            "market_source": source_snapshot,
            "spot_price": spot,
        }

        # â• â•  BROADCAST OPTIMIZATION â• â•
        # If this is the currently selected instrument in the dashboard, we MUST include chart data.
        # Otherwise, we strip it to save 90% of bandwidth.
        # Note: The main loop in _scan_cycle handles stripping based on is_full_chart_needed.

        return ui_data, candidates

    def _filter_live_candidates_by_source(
        self,
        instrument: str,
        candidates: List[TradeCandidate],
        source_snapshot: Dict[str, Any],
    ) -> List[TradeCandidate]:
        """Delayed/context-only data may manage exits but cannot authorize live entries."""
        if self.mode != "REAL" or bool(source_snapshot.get("entry_eligible")):
            return candidates

        allowed = []
        for candidate in candidates:
            action = str(getattr(candidate, "action", "ENTRY") or "ENTRY").upper()
            if action != "ENTRY":
                allowed.append(candidate)
                continue
            signal_ts = getattr(candidate, "signal_timestamp", None)
            identity = "|".join(
                [
                    instrument,
                    str(getattr(candidate, "timeframe", "")),
                    str(signal_ts),
                    str(getattr(candidate, "direction", "")),
                ]
            )
            self._diag_inc_unique("source_fallback", "delayed_entry_blocked", identity)
            logger.warning(
                "Blocked live entry for %s because no entry-eligible broker source is fresh; "
                "delayed data remains context-only.",
                instrument,
            )
        return allowed

    def _serialize_trade_candidate(self, candidate: TradeCandidate) -> Dict:
        """Serialize final post-filter trade candidates for the dashboard signal panel."""
        is_option = str(getattr(candidate, "inst_type", "") or "").upper() == "OPT"
        option_type = str(getattr(candidate, "option_type", "") or "").upper()
        candidate_id = str(getattr(candidate, "trade_id", "") or getattr(candidate, "id", "") or "")
        if not candidate_id:
            candidate_id = f"SESSION|{self._candidate_history_key(candidate)}"
            setattr(candidate, "trade_id", candidate_id)
        signal_ts = getattr(candidate, "signal_timestamp", None)
        entry_ts = getattr(candidate, "entry_timestamp", None) or signal_ts
        lifecycle_id = self._candidate_lifecycle_key(candidate)
        return {
            "id": candidate_id,
            "lifecycle_id": lifecycle_id,
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
            "peak_pnl": float(getattr(candidate, "peak_pnl", 0.0) or 0.0),
            "max_drawdown": float(getattr(candidate, "max_drawdown", 0.0) or 0.0),
            "runner_mode": bool(getattr(candidate, "runner_mode", False)),
            "initial_stop": float(getattr(candidate, "initial_stop", getattr(candidate, "stop", 0.0)) or 0.0),
            "entry_spot": float(getattr(candidate, "entry_spot", 0.0) or 0.0),
            "scalp_lock_mode": bool(getattr(candidate, "scalp_lock_mode", False)),
            "location_risk": str(getattr(candidate, "location_risk", "") or ""),
            "location_position_pct": float(getattr(candidate, "location_position_pct", 0.0) or 0.0),
            "accepted_by_gate": bool(getattr(candidate, "accepted_by_gate", False)),
            "exit_reason": getattr(candidate, "exit_reason", ""),
            "is_exit": getattr(candidate, "action", "ENTRY") == "EXIT",
            "signal_timestamp": (
                signal_ts.isoformat()
                if signal_ts is not None
                else ""
            ),
            "source_signal_timestamp": (
                signal_ts.isoformat()
                if signal_ts is not None
                else ""
            ),
            "entry_timestamp": (
                entry_ts.isoformat()
                if entry_ts is not None
                else ""
            ),
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
                entry_ts.isoformat()
                if entry_ts is not None
                else ""
            ),
        }

    def _is_premium_long_candidate(self, candidate: TradeCandidate) -> bool:
        return str(getattr(candidate, "inst_type", "") or "").upper() == "OPT"

    def _candidate_gross_pnl(self, candidate: TradeCandidate, exit_px: float) -> float:
        if getattr(candidate, "action", "ENTRY") == "NO_ENTRY":
            return 0.0
        qty = int(getattr(candidate, "lots", 1) or 1) * int(getattr(candidate, "lot_size", 1) or 1)
        mult = getattr(candidate, "multiplier", 1.0) if getattr(candidate, "inst_type", "FUT") != "OPT" else 1.0
        if getattr(self, "_is_premium_long_candidate", lambda x: False)(candidate):
            return (exit_px - candidate.price) * qty * mult
        if candidate.direction == "LONG":
            return (exit_px - candidate.price) * qty * mult
        return (candidate.price - exit_px) * qty * mult

    def _candidate_net_pnl(self, candidate: TradeCandidate, exit_px: float) -> float:
        if getattr(candidate, "action", "ENTRY") == "NO_ENTRY":
            return 0.0
        settings = get_settings()
        from engine.trade_accounting import estimate_trade_charges
        qty = int(getattr(candidate, "lots", 1)) * int(getattr(candidate, "lot_size", 1))
        mult = getattr(candidate, "multiplier", 1.0)
        charges = estimate_trade_charges(
            candidate.price,
            exit_px,
            qty,
            "OPT" if self._is_premium_long_candidate(candidate) else "FUT",
            settings,
            mult,
        )
        return round(self._candidate_gross_pnl(candidate, exit_px) - charges, 2)

    def _profit_lock_floor(
        self,
        entry_price: float,
        direction: str,
        qty: int,
        peak_pnl: float,
        scalp_mode: bool = False,
        inst_mult: float = 1.0,
    ) -> Tuple[float, str, float]:
        """Convert peak P&L retention into a protective price floor/ceiling."""
        min_peak = 500.0 if scalp_mode else 1000.0
        if entry_price <= 0 or qty <= 0 or peak_pnl < min_peak or inst_mult <= 0:
            return 0.0, "", 0.0

        lock_ratio = 0.75 if peak_pnl >= 3000.0 else (0.70 if scalp_mode else 0.65)
        reason = "MAJOR_WIN_GUARD" if peak_pnl >= 3000.0 else "SMART_PROFIT_LOCK"
        locked_points = (peak_pnl * lock_ratio) / (qty * inst_mult)
        if str(direction).upper() == "SHORT":
            return float(entry_price) - locked_points, reason, lock_ratio
        return float(entry_price) + locked_points, reason, lock_ratio

    @staticmethod
    def _timeframe_seconds(timeframe: str) -> int:
        tf = str(timeframe or "").lower()
        if tf.endswith("min"):
            try:
                return max(60, int(tf[:-3]) * 60)
            except Exception:
                return 300
        return 300

    def _option_breathing_distance(
        self,
        trade_like,
        regime: str = "",
        entry_price: Optional[float] = None,
        initial_stop: Optional[float] = None,
    ) -> float:
        """Minimum premium-space breathing room before option stops may tighten."""
        if str(getattr(trade_like, "inst_type", "") or "").upper() != "OPT":
            return 0.0
        entry = float(entry_price if entry_price is not None else getattr(trade_like, "price", getattr(trade_like, "entry_price", 0.0)) or 0.0)
        if entry <= 0:
            return 0.0

        raw_initial = initial_stop
        if raw_initial is None:
            raw_initial = getattr(
                trade_like,
                "initial_stop",
                getattr(trade_like, "trailing_stop", getattr(trade_like, "stop", 0.0)),
            )
        try:
            initial = float(raw_initial or 0.0)
        except Exception:
            initial = 0.0

        if initial > 0 and abs(entry - initial) > 1e-9:
            initial_risk = abs(entry - initial)
        else:
            try:
                sl_pct = float(getattr(self, "options_sl_pct", getattr(get_settings(), "options_sl_pct", 15.0)) or 15.0)
            except Exception:
                sl_pct = 15.0
            initial_risk = max(entry * (sl_pct / 100.0), entry * 0.08)

        absolute_floor = 8.0 if entry >= 100.0 else max(3.0, entry * 0.04)
        floor = max(entry * 0.0125, initial_risk * 0.20, absolute_floor)
        if str(regime or "").upper() in {"VOLATILE", "CHOPPY", "MEAN_REVERTING", "UNKNOWN"}:
            floor *= 1.25

        cap = max(entry * 0.03, min(initial_risk * 0.75, entry * 0.08))
        return round(max(0.05, min(floor, cap)), 2)

    def _apply_option_stop_breathing_room(
        self,
        trade_like,
        proposed_stop: float,
        latest_time: Optional[datetime] = None,
        regime: str = "",
    ) -> Tuple[float, float, bool]:
        """Clamp premature option stop tightening so normal premium noise is not treated as failure."""
        if str(getattr(trade_like, "inst_type", "") or "").upper() != "OPT":
            return float(proposed_stop or 0.0), 0.0, False
        stop = float(proposed_stop or 0.0)
        entry = float(getattr(trade_like, "price", getattr(trade_like, "entry_price", 0.0)) or 0.0)
        if entry <= 0 or stop <= 0:
            return stop, 0.0, False

        breathing = self._option_breathing_distance(trade_like, regime=regime, entry_price=entry)
        if breathing <= 0:
            return stop, breathing, False

        qty = int(getattr(trade_like, "lots", 1) or 1) * int(getattr(trade_like, "lot_size", 1) or 1)
        qty = max(1, qty)
        peak_pnl = max(0.0, float(getattr(trade_like, "peak_pnl", 0.0) or 0.0))
        peak_points = peak_pnl / qty

        entry_time = getattr(trade_like, "signal_timestamp", getattr(trade_like, "entry_time", None))
        age_seconds = None
        if latest_time is not None and entry_time is not None:
            try:
                ref = entry_time.astimezone(IST).replace(tzinfo=None) if getattr(entry_time, "tzinfo", None) is not None else entry_time
                now = latest_time.astimezone(IST).replace(tzinfo=None) if getattr(latest_time, "tzinfo", None) is not None else latest_time
                age_seconds = max(0.0, (now - ref).total_seconds())
            except Exception:
                age_seconds = None

        maturity_seconds = self._timeframe_seconds(getattr(trade_like, "timeframe", "5min"))
        signal_matured = age_seconds is not None and age_seconds >= maturity_seconds
        proved_move = peak_points >= breathing * 1.8
        max_early_stop = entry - breathing

        if stop > max_early_stop and not (signal_matured and proved_move):
            return max(0.05, max_early_stop), breathing, True
        return stop, breathing, False

    def _should_defer_option_intel_exit(
        self,
        trade_like,
        current_price: float,
        latest_time: Optional[datetime],
        reason_text: str,
        regime: str = "",
    ) -> Tuple[bool, str]:
        """Debounce intelligence exits for fresh option signals inside normal premium noise."""
        if str(getattr(trade_like, "inst_type", "") or "").upper() != "OPT":
            return False, ""

        entry = float(getattr(trade_like, "price", getattr(trade_like, "entry_price", 0.0)) or 0.0)
        current = float(current_price or 0.0)
        if entry <= 0 or current <= 0:
            return False, ""

        entry_time = getattr(trade_like, "signal_timestamp", getattr(trade_like, "entry_time", None))
        if latest_time is None or entry_time is None:
            return False, ""
        try:
            ref = entry_time.astimezone(IST).replace(tzinfo=None) if getattr(entry_time, "tzinfo", None) is not None else entry_time
            now = latest_time.astimezone(IST).replace(tzinfo=None) if getattr(latest_time, "tzinfo", None) is not None else latest_time
            age_seconds = max(0.0, (now - ref).total_seconds())
        except Exception:
            return False, ""

        breathing = self._option_breathing_distance(trade_like, regime=regime, entry_price=entry)
        adverse_points = max(0.0, entry - current)
        severe_adverse = adverse_points >= breathing * 1.25 if breathing > 0 else False
        min_hold_seconds = self._timeframe_seconds(getattr(trade_like, "timeframe", "5min"))

        votes = 0
        match = re.search(r"votes=(\d+)", str(reason_text or ""))
        if match:
            try:
                votes = int(match.group(1))
            except Exception:
                votes = 0
        severe_votes = votes >= 3 and adverse_points >= breathing * 0.75 if breathing > 0 else votes >= 3

        if age_seconds < min_hold_seconds and not severe_adverse and not severe_votes:
            return True, (
                f"within {age_seconds:.0f}s of {getattr(trade_like, 'timeframe', '5min')} signal "
                f"and adverse {adverse_points:.2f} < breathing {breathing:.2f}"
            )
        return False, ""

    def _profit_guard_reversal_context(
        self,
        trade_like,
        current_price: float,
        latest_time: Optional[datetime],
    ) -> Dict[str, Any]:
        """Read recent 1m price action to distinguish pullback from fast reversal."""
        context = {
            "has_context": False,
            "fast_reversal": False,
            "trend_supportive": False,
            "reason": "no 1m context",
        }
        try:
            df = self.candles.get_candles(getattr(trade_like, "instrument", ""), "1min")
        except Exception:
            return context
        if df is None or len(df) < 4:
            return context

        work = df.copy()
        if not isinstance(work.index, pd.DatetimeIndex):
            if "timestamp" not in work.columns:
                return context
            work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")
            work = work.dropna(subset=["timestamp"]).set_index("timestamp")
        if work.empty:
            return context
        work = work.sort_index()
        if latest_time is not None:
            latest_ts = pd.Timestamp(latest_time)
            if latest_ts.tzinfo is not None:
                latest_ts = latest_ts.tz_convert(IST).tz_localize(None)
            work = work[work.index <= latest_ts]
        if len(work) < 4:
            return context

        tail = work.tail(12)
        opens = tail["open"].astype(float)
        highs = tail["high"].astype(float)
        lows = tail["low"].astype(float)
        closes = tail["close"].astype(float)
        ranges = (highs - lows).abs()
        median_range = float(ranges.iloc[:-1].median() if len(ranges) > 1 else ranges.iloc[-1])
        if not math.isfinite(median_range) or median_range <= 0:
            median_range = max(float(current_price) * 0.0002, 1.0)

        last_open = float(opens.iloc[-1])
        last_close = float(closes.iloc[-1])
        prev_close = float(closes.iloc[-2])
        direction = str(getattr(trade_like, "direction", "") or "").upper()

        if direction == "SHORT":
            adverse_body = max(0.0, last_close - last_open)
            adverse_close_move = max(0.0, last_close - prev_close)
        else:
            adverse_body = max(0.0, last_open - last_close)
            adverse_close_move = max(0.0, prev_close - last_close)

        fast_body_threshold = max(median_range * 1.20, float(current_price) * 0.00025)
        fast_close_threshold = max(median_range * 1.50, float(current_price) * 0.00035)
        fast_reversal = adverse_body >= fast_body_threshold or adverse_close_move >= fast_close_threshold

        ema9 = float(closes.ewm(span=9, adjust=False).mean().iloc[-1])
        ema21 = float(closes.ewm(span=21, adjust=False).mean().iloc[-1])
        slope = float(closes.iloc[-1] - closes.iloc[max(0, len(closes) - 4)])
        if direction == "SHORT":
            trend_votes = sum((float(current_price) <= ema9, ema9 <= ema21, slope <= 0))
        else:
            trend_votes = sum((float(current_price) >= ema9, ema9 >= ema21, slope >= 0))
        trend_supportive = trend_votes >= 1

        context.update(
            {
                "has_context": True,
                "fast_reversal": bool(fast_reversal),
                "trend_supportive": bool(trend_supportive),
                "trend_votes": int(trend_votes),
                "reason": (
                    f"fast={bool(fast_reversal)} trend_supportive={bool(trend_supportive)} "
                    f"trend_votes={trend_votes}/3 "
                    f"body={adverse_body:.2f}/{fast_body_threshold:.2f} "
                    f"close_move={adverse_close_move:.2f}/{fast_close_threshold:.2f}"
                ),
                "median_range": median_range,
            }
        )
        return context

    def _should_exit_major_win_guard(
        self,
        trade_like,
        current_price: float,
        peak_pnl: float,
        current_pnl: float,
        latest_time: Optional[datetime],
    ) -> Tuple[bool, str]:
        """Major-win guard exits on real reversal, not normal same-trend breathing."""
        if peak_pnl <= 0:
            return True, "invalid peak"
        retention = current_pnl / peak_pnl
        if retention <= 0.60:
            return True, f"hard giveback floor hit ({retention:.0%} of peak)"

        context = self._profit_guard_reversal_context(trade_like, current_price, latest_time)
        if not context.get("has_context"):
            return True, context.get("reason", "no context")
        if context.get("fast_reversal"):
            return True, f"fast adverse reversal ({context.get('reason')})"
        if not context.get("trend_supportive"):
            return True, f"trend no longer supportive ({context.get('reason')})"
        return False, f"same-trend pullback deferred ({retention:.0%} of peak; {context.get('reason')})"

    def _stagnation_exit_decision(
        self,
        trade_like,
        current_pnl: float,
        peak_pnl: float,
        latest_time: Optional[datetime],
        regime: str = "",
    ) -> Tuple[bool, str]:
        """Scale stagnation exits by initial risk, index noise, and current trend."""
        if latest_time is None or peak_pnl <= 0 or current_pnl <= 0:
            return False, "not a profitable stagnation candidate"

        entry_time = getattr(trade_like, "entry_time", None) or getattr(trade_like, "signal_timestamp", None)
        if entry_time is None:
            return False, "missing entry time"
        try:
            entry_check = (
                entry_time.astimezone(IST).replace(tzinfo=None)
                if getattr(entry_time, "tzinfo", None) is not None
                else entry_time
            )
            now_check = (
                latest_time.astimezone(IST).replace(tzinfo=None)
                if getattr(latest_time, "tzinfo", None) is not None
                else latest_time
            )
            age_minutes = max(0.0, (now_check - entry_check).total_seconds() / 60.0)
        except Exception:
            return False, "invalid entry time"

        settings = get_settings()
        base_minutes = max(1.0, float(getattr(settings, "trade_stagnation_minutes", 20.0) or 20.0))
        max_extra = max(0.0, float(getattr(settings, "trade_stagnation_max_extra_minutes", 10.0) or 10.0))

        entry = float(getattr(trade_like, "price", getattr(trade_like, "entry_price", 0.0)) or 0.0)
        initial_stop = float(
            getattr(
                trade_like,
                "initial_stop",
                getattr(trade_like, "trailing_stop", getattr(trade_like, "stop", 0.0)),
            )
            or 0.0
        )
        qty = max(
            1,
            int(getattr(trade_like, "lots", 1) or 1)
            * int(getattr(trade_like, "lot_size", 1) or 1),
        )
        initial_risk_rs = abs(entry - initial_stop) * qty if entry > 0 and initial_stop > 0 else 0.0
        min_peak_r = max(0.0, float(getattr(settings, "trade_stagnation_min_peak_r", 0.50) or 0.50))
        min_peak_rs = max(300.0, initial_risk_rs * min_peak_r)
        if peak_pnl < min_peak_rs:
            return False, f"peak Rs.{peak_pnl:.0f} below meaningful threshold Rs.{min_peak_rs:.0f}"

        current_price = float(
            getattr(
                trade_like,
                "current_price",
                getattr(trade_like, "price", getattr(trade_like, "entry_price", 0.0)),
            )
            or 0.0
        )
        context = self._profit_guard_reversal_context(trade_like, current_price, latest_time)
        median_range = float(context.get("median_range", 0.0) or 0.0)
        baseline_bps = {
            "NIFTY": 4.0,
            "BANKNIFTY": 5.0,
            "SENSEX": 4.0,
            "MIDCPNIFTY": 5.0,
        }.get(str(getattr(trade_like, "instrument", "") or "").upper(), 4.5)
        observed_bps = (median_range / current_price) * 10000.0 if median_range > 0 and current_price > 0 else baseline_bps
        volatility_ratio = max(0.5, min(3.0, observed_bps / baseline_bps))
        extra_minutes = max(0.0, min(max_extra, (volatility_ratio - 1.0) * 5.0))
        if str(regime or "").upper() in {"VOLATILE", "CHOPPY", "MEAN_REVERTING"}:
            extra_minutes = max(extra_minutes, min(max_extra, 5.0))
        required_minutes = base_minutes + extra_minutes
        if age_minutes < required_minutes:
            return False, f"age {age_minutes:.1f}m below volatility-adjusted {required_minutes:.1f}m"

        retention_floor = max(0.68, min(0.85, 0.85 - max(0.0, volatility_ratio - 1.0) * 0.10))
        retention = current_pnl / peak_pnl
        if retention >= retention_floor:
            return False, f"retention {retention:.0%} remains above {retention_floor:.0%}"
        if context.get("has_context") and int(context.get("trend_votes", 0) or 0) >= 2 and not context.get("fast_reversal"):
            return False, f"trend still supportive despite {retention:.0%} retention"
        return True, (
            f"age {age_minutes:.1f}m/{required_minutes:.1f}m, retention {retention:.0%} "
            f"< {retention_floor:.0%}, volatility {volatility_ratio:.2f}x; "
            f"{context.get('reason', 'no trend context')}"
        )

    def _candidate_candle_extreme_price(self, candidate: TradeCandidate, latest_time: Optional[datetime]) -> float:
        """Estimate the best reached trade price from 1m candle extremes since entry."""
        if getattr(self, "_is_premium_long_candidate", lambda x: False)(candidate):
            return 0.0
        entry_spot = float(getattr(candidate, "entry_spot", getattr(candidate, "price", 0.0)) or 0.0)
        if entry_spot <= 0:
            return 0.0
        try:
            df = self.candles.get_candles(candidate.instrument, "1min")
        except Exception:
            return 0.0
        if df is None or len(df) == 0:
            return 0.0
        work = df.copy()
        if not isinstance(work.index, pd.DatetimeIndex):
            if "timestamp" not in work.columns:
                return 0.0
            work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")
            work = work.dropna(subset=["timestamp"]).set_index("timestamp")
        if work.empty:
            return 0.0
        work = work.sort_index()
        entry_ts = getattr(candidate, "signal_timestamp", None)
        if entry_ts is not None:
            entry_ts = pd.Timestamp(entry_ts)
            if entry_ts.tzinfo is not None:
                entry_ts = entry_ts.tz_convert(IST).tz_localize(None)
            work = work[work.index >= entry_ts]
        if latest_time is not None:
            latest_ts = pd.Timestamp(latest_time)
            if latest_ts.tzinfo is not None:
                latest_ts = latest_ts.tz_convert(IST).tz_localize(None)
            work = work[work.index <= latest_ts]
        if work.empty:
            return 0.0
        if str(getattr(candidate, "direction", "") or "").upper() == "SHORT":
            if "low" not in work.columns:
                return 0.0
            favorable_spot = float(work["low"].astype(float).min())
        else:
            if "high" not in work.columns:
                return 0.0
            favorable_spot = float(work["high"].astype(float).max())
        if favorable_spot <= 0:
            return 0.0
        return float(candidate.price) + (favorable_spot - entry_spot)

    def _trade_candle_extreme_price(self, trade, latest_time: Optional[datetime]) -> float:
        """Estimate best reached active trade price from 1m candle extremes since entry."""
        inst_type = str(getattr(trade, "inst_type", "FUT") or "FUT").upper()
        entry_price = float(getattr(trade, "entry_price", 0.0) or 0.0)
        if entry_price <= 0:
            return 0.0
        try:
            df = self.candles.get_candles(trade.instrument, "1min")
        except Exception:
            return 0.0
        if df is None or len(df) == 0:
            return 0.0
        work = df.copy()
        if not isinstance(work.index, pd.DatetimeIndex):
            if "timestamp" not in work.columns:
                return 0.0
            work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")
            work = work.dropna(subset=["timestamp"]).set_index("timestamp")
        if work.empty:
            return 0.0
        work = work.sort_index()
        entry_ts = getattr(trade, "entry_time", None)
        if entry_ts is not None:
            entry_ts = pd.Timestamp(entry_ts)
            if entry_ts.tzinfo is not None:
                entry_ts = entry_ts.tz_convert(IST).tz_localize(None)
            
            # ─────────────────────────────────────────────────────────────
            # FIX: Exclude the entry minute candle entirely from the peak!
            # ─────────────────────────────────────────────────────────────
            entry_minute = entry_ts.replace(second=0, microsecond=0)
            work = work[work.index > entry_minute]
            
        if latest_time is not None:
            latest_ts = pd.Timestamp(latest_time)
            if latest_ts.tzinfo is not None:
                latest_ts = latest_ts.tz_convert(IST).tz_localize(None)
            work = work[work.index <= latest_ts]
        if work.empty:
            return 0.0
        direction = str(getattr(trade, "direction", "") or "").upper()
        if direction == "SHORT":
            if "low" not in work.columns:
                return 0.0
            favorable_spot = float(work["low"].astype(float).min())
        else:
            if "high" not in work.columns:
                return 0.0
            favorable_spot = float(work["high"].astype(float).max())
        if favorable_spot <= 0:
            return 0.0
        if inst_type == "OPT":
            multiplier = float(getattr(trade, "instrument_multiplier", 0.0) or 0.0)
            entry_spot = float(getattr(trade, "entry_spot", 0.0) or 0.0)
            if entry_spot <= 0 or multiplier == 0:
                return 0.0
            return max(1.0, entry_price + ((favorable_spot - entry_spot) * multiplier))
        return self._fut_price_from_spot_level(trade, favorable_spot)

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
        """Repair persisted live/session rows created by older builds before accounting them."""
        changed = False
        entry = float(getattr(candidate, "price", 0.0) or 0.0)
        if entry <= 0:
            return False

        inst_type = str(getattr(candidate, "inst_type", "") or "").upper()
        direction = str(getattr(candidate, "direction", "") or "").upper()
        rr = max(0.1, float(getattr(candidate, "rr", 1.5) or 1.5))
        old_stop = float(getattr(candidate, "stop", 0.0) or 0.0)
        old_target = float(getattr(candidate, "target", 0.0) or 0.0)
        runner_mode = bool(getattr(candidate, "runner_mode", False))

        if inst_type == "FUT":
            max_dist = entry * (float(getattr(self, "futures_sl_pct", 0.2) or 0.2) / 100.0)
            if direction == "LONG":
                repaired_stop = max(old_stop or (entry - max_dist), entry - max_dist)
                repaired_target = old_target if repaired_stop >= entry else entry + ((entry - repaired_stop) * rr)
            else:
                repaired_stop = min(old_stop or (entry + max_dist), entry + max_dist)
                repaired_target = old_target if repaired_stop <= entry else entry - ((repaired_stop - entry) * rr)
        elif inst_type == "OPT":
            max_dist = entry * (float(getattr(self, "options_sl_pct", 15.0) or 15.0) / 100.0)
            repaired_stop = max(0.05, max(old_stop or (entry - max_dist), entry - max_dist))
            repaired_target = old_target if repaired_stop >= entry else entry + ((entry - repaired_stop) * rr)
        else:
            repaired_stop = old_stop
            repaired_target = old_target

        if runner_mode:
            repaired_target = 0.0

        if repaired_stop > 0 and abs(repaired_stop - old_stop) > 1e-9:
            candidate.stop = float(repaired_stop)
            changed = True
        if runner_mode and old_target > 0:
            candidate.target = 0.0
            changed = True
        elif repaired_target > 0 and (old_target <= 0 or abs(repaired_target - old_target) > 1e-9):
            candidate.target = float(repaired_target)
            changed = True

        if str(getattr(candidate, "action", "ENTRY") or "").upper() != "EXIT":
            return changed

        observed_price = float(
            getattr(candidate, "exit_price", 0.0)
            or getattr(candidate, "current_price", 0.0)
            or getattr(candidate, "price", 0.0)
            or 0.0
        )
        if observed_price <= 0:
            return changed

        original_reason = str(getattr(candidate, "exit_reason", "") or "EXIT_SIGNAL").upper()
        reason_text = original_reason.replace("_", " ").title()
        exit_reason = str(getattr(candidate, "exit_reason", "") or "EXIT_SIGNAL")
        exit_px = observed_price
        proactive_reasons = {
            "SMART_PROFIT_LOCK",
            "LOW_GAIN_PROTECT",
            "MAJOR_WIN_GUARD",
            "STAGNATION_EXIT",
            "INTEL_FLIP",
        }
        if original_reason in proactive_reasons:
            exit_px = observed_price
        elif original_reason in {"STOP_HIT", "TRAILING_STOP", "SL HIT", "SL_HIT"}:
            exit_px = float(candidate.stop)
            exit_reason = "STOP_HIT" if original_reason != "SL HIT" else "SL HIT"
            reason_text = "Stoploss hit"
        elif original_reason == "TARGET_HIT":
            exit_px = float(candidate.target)
            reason_text = "Target hit"
        else:
            exit_px, exit_reason, reason_text = self._candidate_exit_resolution(
                candidate,
                observed_price,
                exit_reason,
                reason_text,
            )

        new_pnl = self._candidate_net_pnl(candidate, float(exit_px))
        if (
            abs(float(exit_px) - observed_price) > 1e-9
            or abs(float(getattr(candidate, "pnl", 0.0) or 0.0) - new_pnl) > 1e-9
            or getattr(candidate, "exit_reason", "") != exit_reason
        ):
            changed = True

        candidate.current_price = float(exit_px)
        candidate.pnl = new_pnl
        candidate.exit_reason = exit_reason
        setattr(candidate, "exit_price", float(exit_px))
        if changed:
            reasons = list(getattr(candidate, "reasons", []) or [])
            repair_reason = f"{reason_text} reconciled from persisted exit {observed_price:.2f}"
            if repair_reason not in reasons:
                candidate.reasons = reasons + [repair_reason]
        return changed

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

    def _candidate_lifecycle_key(self, candidate: TradeCandidate) -> str:
        """Pair one accepted entry with its later exit without using execution time."""
        source_ts = (
            getattr(candidate, "source_signal_timestamp", None)
            or getattr(candidate, "signal_timestamp", None)
        )
        if source_ts is None:
            source_ts = datetime.min
        elif getattr(source_ts, "tzinfo", None) is not None:
            source_ts = source_ts.astimezone(IST).replace(tzinfo=None)
        return "|".join([
            str(getattr(candidate, "instrument", "") or ""),
            str(getattr(candidate, "timeframe", "") or ""),
            str(getattr(candidate, "direction", "") or ""),
            source_ts.isoformat(),
        ])

    def _inherit_exit_entry_metadata(
        self,
        exit_candidate: TradeCandidate,
        entry_candidate: Optional[TradeCandidate],
    ) -> None:
        """Keep source-candle and accepted-entry clocks intact on an exit row."""
        if not exit_candidate or not entry_candidate:
            return
        source_ts = (
            getattr(entry_candidate, "source_signal_timestamp", None)
            or getattr(entry_candidate, "signal_timestamp", None)
        )
        if source_ts is not None:
            setattr(exit_candidate, "source_signal_timestamp", source_ts)
            exit_candidate.signal_timestamp = source_ts
        entry_ts = getattr(entry_candidate, "entry_timestamp", None)
        if entry_ts is not None:
            setattr(exit_candidate, "entry_timestamp", entry_ts)

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

    def _latest_session_candidate_file(self, require_candidates: bool = False) -> Optional[Path]:
        """Most recent persisted session ledger, used for weekend/holiday display."""
        try:
            files = sorted(
                [f for f in self._session_candidate_dir.glob("*.json") if f.name != "historical.json"],
                key=lambda x: x.name,
            )
            if require_candidates:
                files = [
                    f for f in files
                    if len(json.loads(f.read_text(encoding="utf-8")).get("candidates", []) or []) > 0
                ]
            return files[-1] if files else None
        except Exception:
            return None

    def _candidate_from_payload(self, payload: Dict[str, Any]) -> Optional[TradeCandidate]:
        try:
            raw_ts = (
                payload.get("signal_timestamp")
                or payload.get("source_signal_timestamp")
                or payload.get("timestamp")
            )
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
                multiplier=float(payload.get("multiplier") or 1.0),
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
            setattr(candidate, "peak_pnl", float(payload.get("peak_pnl") or 0.0))
            setattr(candidate, "max_drawdown", float(payload.get("max_drawdown") or 0.0))
            setattr(candidate, "trade_id", str(payload.get("id") or payload.get("trade_id") or ""))
            setattr(candidate, "runner_mode", bool(payload.get("runner_mode") or False))
            setattr(candidate, "initial_stop", float(payload.get("initial_stop") or candidate.stop or 0.0))
            setattr(candidate, "entry_spot", float(payload.get("entry_spot") or 0.0))
            setattr(candidate, "scalp_lock_mode", bool(payload.get("scalp_lock_mode") or False))
            setattr(candidate, "location_risk", str(payload.get("location_risk") or ""))
            setattr(candidate, "location_position_pct", float(payload.get("location_position_pct") or 0.0))
            setattr(candidate, "accepted_by_gate", bool(payload.get("accepted_by_gate") or False))
            raw_entry_ts = payload.get("entry_timestamp") or payload.get("timestamp")
            if raw_entry_ts:
                try:
                    entry_ts = datetime.fromisoformat(str(raw_entry_ts))
                    if entry_ts.tzinfo is not None:
                        entry_ts = entry_ts.astimezone(IST).replace(tzinfo=None)
                    setattr(candidate, "entry_timestamp", entry_ts)
                except Exception:
                    pass
            if signal_ts is not None:
                setattr(candidate, "source_signal_timestamp", signal_ts)
            setattr(
                candidate,
                "source_signal_type",
                str(
                    payload.get("source_signal_type")
                    or ("BUY" if candidate.direction == "LONG" else "SELL")
                ),
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

    def _candidate_valid_for_live_restore(self, candidate: TradeCandidate) -> bool:
        """Strict startup quarantine for stale REAL-mode signal rows."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return True
        if not self._candidate_matches_live_session_day(candidate):
            return False
        action = str(getattr(candidate, "action", "ENTRY") or "ENTRY").upper()
        if action == "NO_ENTRY":
            return False
        if action == "EXIT":
            return True
        if bool(getattr(candidate, "accepted_by_gate", False)):
            return True
        signal_ts = getattr(candidate, "signal_timestamp", None)
        if not signal_ts:
            return False
        if not self._live_signal_candle_has_closed(candidate, signal_ts, self._session_squareoff_clock()):
            return False
        if not self._signal_in_restart_recovery_window(signal_ts, getattr(candidate, "timeframe", "5min")):
            return False
        return True

    def _signal_in_restart_recovery_window(
        self,
        signal_ts,
        timeframe: str,
        now: Optional[datetime] = None,
    ) -> bool:
        """Allow only the most recent just-closed candle after a live restart."""
        if getattr(self, "mode", "") == "HISTORICAL" or signal_ts is None:
            return False

        started_at = getattr(self, "_process_started_at", None)
        if not started_at:
            return False

        sig_check = signal_ts.astimezone(IST).replace(tzinfo=None) if getattr(signal_ts, "tzinfo", None) is not None else signal_ts
        start_check = started_at.astimezone(IST).replace(tzinfo=None) if getattr(started_at, "tzinfo", None) is not None else started_at
        now = now or self._session_squareoff_clock()
        now_check = now.astimezone(IST).replace(tzinfo=None) if getattr(now, "tzinfo", None) is not None else now

        expires_at = self._live_signal_processing_deadline(signal_ts, timeframe)
        if expires_at is None:
            return False

        # A pre-start signal is recoverable only if the app restarted before
        # the recovery window expired, and the current scan is still inside it.
        if sig_check >= start_check:
            return now_check <= expires_at
        return start_check <= expires_at and now_check <= expires_at

    def _live_signal_processing_deadline(
        self,
        signal_ts,
        timeframe: str,
    ) -> Optional[datetime]:
        """Deadline for admitting a finalized live signal into stabilization."""
        if signal_ts is None:
            return None
        sig_check = signal_ts.astimezone(IST).replace(tzinfo=None) if getattr(signal_ts, "tzinfo", None) is not None else signal_ts
        started_at = getattr(self, "_process_started_at", None)
        start_check = (
            started_at.astimezone(IST).replace(tzinfo=None)
            if getattr(started_at, "tzinfo", None) is not None
            else started_at
        )
        tf_minutes = {"1min": 1, "5min": 5, "15min": 15}.get(str(timeframe or ""), 5)
        grace_seconds = max(0.0, float(getattr(get_settings(), "live_restart_recovery_grace_seconds", 120.0) or 120.0))
        close_time = sig_check + timedelta(minutes=tf_minutes)
        timeframe_name = str(timeframe or "")
        if timeframe_name in {"5min", "15min"} and start_check is not None and start_check <= close_time:
            grace_setting = (
                "live_signal_post_close_grace_seconds_15min"
                if timeframe_name == "15min"
                else "live_signal_post_close_grace_seconds_5min"
            )
            grace_seconds = max(
                grace_seconds,
                float(
                    getattr(
                        get_settings(),
                        grace_setting,
                        300.0,
                    )
                    or 300.0
                ),
            )
        return close_time + timedelta(seconds=grace_seconds)

    def _load_session_trade_candidates(self) -> None:
        """Load session candidates from SQLite on startup to prevent dashboard amnesia."""
        desired_session_day = (
            self.historical_session_cutoff_date().isoformat()
            if getattr(self, "mode", "") == "HISTORICAL"
            else getattr(self, "_session_candidate_day", self._get_current_session_day())
        )
        if (
            getattr(self, "mode", "") == "HISTORICAL"
            and getattr(self, "_hist_candidates_loaded", False)
            and getattr(self, "_loaded_session_candidate_day", "") == desired_session_day
        ):
            return
        
        self._session_trade_candidates = {}
        session_day = desired_session_day
        self._loaded_session_candidate_day = session_day
        
        try:
            payloads = db.load_session_signals(session_day)
            count = 0
            for item in payloads:
                candidate = self._candidate_from_payload(item)
                if not candidate or not candidate.instrument:
                    continue
                if not self._candidate_valid_for_live_restore(candidate):
                    logger.info(
                        f"Quarantined stale session candidate on startup: "
                        f"{candidate.instrument} {candidate.timeframe} {candidate.direction} "
                        f"{getattr(candidate, 'signal_timestamp', None)}"
                    )
                    continue
                self._normalize_loaded_exit_candidate(candidate)
                book = self._session_trade_candidates.setdefault(candidate.instrument, {})
                book[self._candidate_history_key(candidate)] = candidate
                count += 1
                
            if count:
                logger.info(f"Restored {count} session signal row(s) from SQLite for {session_day}")
        except Exception as exc:
            logger.warning(f"Could not restore session signal ledger from DB: {exc}")
        
        # Mark as loaded in HISTORICAL mode
        if getattr(self, "mode", "") == "HISTORICAL":
            self._hist_candidates_loaded = True

    def _loaded_session_is_current_day(self) -> bool:
        return (
            getattr(self, "_loaded_session_candidate_day", getattr(self, "_session_candidate_day", "")) 
            == getattr(self, "_session_candidate_day", self._get_current_session_day())
        )

    def _persist_session_trade_candidates(self) -> None:
        """Persist today's dashboard signal ledger to SQLite.
        In HISTORICAL mode, does nothing."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return
        
        try:
            rows = []
            for book in getattr(self, "_session_trade_candidates", {}).values():
                for candidate in book.values():
                    self._normalize_loaded_exit_candidate(candidate)
                    rows.append(self._serialize_trade_candidate(candidate))
            
            session_day = getattr(self, "_session_candidate_day", self._get_current_session_day())
            
            # Sort chronologically before insert
            rows.sort(key=lambda r: r.get("timestamp") or "", reverse=True)
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                db.save_session_signals(session_day, rows)
                return

            self._session_persist_pending = (session_day, rows)
            task = getattr(self, "_session_persist_task", None)
            if task is None or task.done():
                self._session_persist_task = loop.create_task(
                    self._flush_session_trade_candidates()
                )
            
        except Exception as exc:
            logger.debug(f"Failed to persist session trade candidates to DB: {exc}")

    async def _flush_session_trade_candidates(self) -> None:
        """Coalesce ledger updates and perform SQLite writes outside the event loop."""
        while True:
            pending = getattr(self, "_session_persist_pending", None)
            if pending is None:
                return
            self._session_persist_pending = None
            session_day, rows = pending
            try:
                await asyncio.to_thread(db.save_session_signals, session_day, rows)
            except Exception as exc:
                logger.debug(f"Failed to persist session trade candidates to DB: {exc}")

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
            desired_session_day = self.historical_session_cutoff_date().isoformat()
            # In HISTORICAL mode, load once per completed-session cutoff.
            if (
                not getattr(self, "_hist_candidates_loaded", False)
                or getattr(self, "_loaded_session_candidate_day", "") != desired_session_day
            ):
                self._load_session_trade_candidates()
                self._hist_candidates_loaded = True
            return
        today = self._get_current_session_day()
        if today == getattr(self, "_session_candidate_day", today):
            loaded_day = getattr(self, "_loaded_session_candidate_day", None)
            if loaded_day is not None and loaded_day != today:
                self._load_session_trade_candidates()
            return
        logger.info(f"Session candidate day rolling over from {self._session_candidate_day} to {today}")
        self._session_candidate_day = today
        self._hist_candidates_loaded = False
        self._load_session_trade_candidates()

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

    async def _publish_trade_payload_now(self) -> None:
        """Publish an accepted entry without waiting for the scan-cycle epilogue."""
        snapshot = dict(getattr(self, "latest_results", {}) or {})
        snapshot["trades"] = self._build_dashboard_trade_payload()
        snapshot["timestamp"] = datetime.now(IST).isoformat()
        self.latest_results = snapshot
        self._update_dashboard_cache(snapshot)
        if self.on_update:
            try:
                await self.on_update(snapshot)
            except Exception as exc:
                logger.error(f"Failed to publish accepted signal immediately: {exc}")

    def _candidate_matches_live_session_day(self, candidate: TradeCandidate) -> bool:
        """REAL/manual dashboard rows must belong to the active session day."""
        if getattr(self, "mode", "") != "REAL":
            return True
        trade_id = str(getattr(candidate, "trade_id", "") or getattr(candidate, "id", "") or "")
        if trade_id.startswith(("H_", "EOD_")):
            return False
        if not getattr(self, "_process_started_at", None):
            return True
        signal_ts = getattr(candidate, "signal_timestamp", None)
        if not signal_ts:
            return True
        if getattr(signal_ts, "tzinfo", None) is not None:
            signal_day = signal_ts.astimezone(IST).date().isoformat()
        else:
            signal_day = signal_ts.date().isoformat()
        session_day = getattr(self, "_session_candidate_day", self._get_current_session_day())
        return signal_day == session_day

    def _remember_filtered_candidate(self, candidate: TradeCandidate) -> None:
        """Keep recent display-only rejected rows visible without making them executable."""
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

    def _forget_signal_display_rows(self, candidate: TradeCandidate) -> None:
        """Remove display-only rows tied to a live signal that later repainted or expired."""
        if not candidate or not getattr(candidate, "instrument", ""):
            return
        key = self._candidate_history_key(candidate)
        for attr in ("_latest_filtered_candidates", "_latest_trade_candidates"):
            store = getattr(self, attr, None)
            if not isinstance(store, dict):
                continue
            rows = list(store.get(candidate.instrument, []) or [])
            rows = [row for row in rows if self._candidate_history_key(row) != key]
            if rows:
                store[candidate.instrument] = rows
            else:
                store.pop(candidate.instrument, None)

        # Also clean up from session history so it doesn't linger as a ghost
        session_store = getattr(self, "_session_trade_candidates", {})
        if candidate.instrument in session_store:
            book = session_store[candidate.instrument]
            if key in book:
                book.pop(key)
                if not book:
                    session_store.pop(candidate.instrument)
                self._persist_session_trade_candidates()

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
            signal_ts = getattr(signal, "timestamp", None)
            identity = "|".join(
                [
                    str(instrument),
                    str(timeframe),
                    str(signal_ts),
                    str(direction),
                    str(inst_type),
                    str(reason or "Unknown"),
                ]
            )
            self._diag_inc_unique("rejects", str(reason or "Unknown"), identity)
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

    @staticmethod
    def _configured_session_time(setting_name: str, default: str) -> dtime:
        label = str(getattr(get_settings(), setting_name, default) or default)
        return datetime.strptime(label, "%H:%M").time()

    def _entry_cutoff_allows_candidate(self, candidate: TradeCandidate, session_now: datetime) -> bool:
        """Allow a candle that closed by cutoff to finish its stabilization window."""
        timeframe = str(getattr(candidate, "timeframe", "") or "5min")
        cutoff = self._configured_session_time(
            "ut_no_entry_after" if timeframe == "15min" else "ut_5min_no_entry_after",
            "15:00" if timeframe == "15min" else "15:15",
        )
        signal_ts = getattr(candidate, "signal_timestamp", None)
        if signal_ts is None:
            return session_now.time() < cutoff
        if getattr(signal_ts, "tzinfo", None) is not None:
            signal_ts = signal_ts.astimezone(IST).replace(tzinfo=None)
        tf_minutes = {"1min": 1, "5min": 5, "15min": 15}.get(timeframe, 5)
        candle_close = signal_ts + timedelta(minutes=tf_minutes)
        cutoff_dt = datetime.combine(candle_close.date(), cutoff)
        if candle_close > cutoff_dt:
            return False
        stable_seconds = (
            float(getattr(get_settings(), "live_signal_stabilization_seconds_15min", 30.0) or 30.0)
            if timeframe == "15min"
            else float(getattr(get_settings(), "live_signal_stabilization_seconds", 25.0) or 25.0)
        )
        now_naive = session_now.astimezone(IST).replace(tzinfo=None) if session_now.tzinfo else session_now
        return now_naive <= cutoff_dt + timedelta(seconds=stable_seconds + 10.0)

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
        if signal_ts >= started_at:
            return False
        return not self._signal_in_restart_recovery_window(signal_ts, getattr(candidate, "timeframe", "5min"))

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
        """Close manual/live signal-ledger rows at the configured hard square-off."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return 0

        now = now or self._session_squareoff_clock()
        if now.tzinfo is None:
            now = IST.localize(now)
        else:
            now = now.astimezone(IST)

        trigger_time = self._configured_session_time("ut_force_exit_time", "15:25")
        exit_time = trigger_time
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
                    f"Hard square-off at {trigger_time.strftime('%H:%M')} IST",
                )
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
                    signal_timestamp=prev_ts,
                    current_price=exit_px,
                    pnl=calc_pnl,
                    status="EXIT SIGNAL",
                    action="EXIT",
                    exit_reason=exit_reason,
                )
                setattr(exit_candidate, "exit_timestamp", exit_ts.replace(tzinfo=None))
                setattr(exit_candidate, "exit_price", float(exit_px))
                self._inherit_exit_entry_metadata(exit_candidate, previous)
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
        self.log_event(
            f"Force EOD exit applied to {len(exits)} manual signal row(s) at {trigger_time.strftime('%H:%M')} IST",
            "warning",
        )
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
                self._inherit_exit_entry_metadata(exit_candidate, previous)
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
        for candidate in book.values():
            self._normalize_loaded_exit_candidate(candidate)
        entries_by_lifecycle = {
            self._candidate_lifecycle_key(candidate): candidate
            for candidate in book.values()
            if getattr(candidate, "action", "ENTRY") == "ENTRY"
        }
        repaired = False
        for candidate in book.values():
            if getattr(candidate, "action", "ENTRY") != "EXIT":
                continue
            entry = entries_by_lifecycle.get(self._candidate_lifecycle_key(candidate))
            old_entry_ts = getattr(candidate, "entry_timestamp", None)
            self._inherit_exit_entry_metadata(candidate, entry)
            if getattr(candidate, "entry_timestamp", None) != old_entry_ts:
                repaired = True
        if repaired:
            self._persist_session_trade_candidates()
        rows = [
            candidate for candidate in book.values()
            if self._candidate_matches_live_session_day(candidate)
        ]
        return sorted(
            rows,
            key=lambda c: (
                getattr(c, "entry_timestamp", None)
                or getattr(c, "signal_timestamp", None)
                or datetime.min
            ),
            reverse=True,
        )

    def _build_dashboard_trade_payload(self) -> Dict[str, Any]:
        """Return trade-manager rows plus the persisted manual/live signal ledger."""
        payload = self.trades.get_dashboard_payload(
            is_historical=(self.mode == "HISTORICAL"),
            backtest_days=self.backtest_days,
            inst_pref=getattr(self, "inst_pref", "AUTO"),
        )
        payload["meta"] = {
            "mode": self.mode,
            "open_rows": len(payload.get("open", []) or []),
            "closed_rows": len(payload.get("closed", []) or []),
            "restored_signal_rows": 0,
            "source": "trade_manager",
        }
        
        def inst_pref_match(c) -> bool:
            pref = (getattr(self, "inst_pref", "AUTO") or "AUTO").upper()
            if pref in ("AUTO", "HYBRID"): return True
            inst_type = getattr(c, "inst_type", "FUT") if not isinstance(c, dict) else c.get("inst_type", "FUT")
            if pref == "FUT" and inst_type == "FUT": return True
            if pref == "OPT" and inst_type in ("OPT", "CE", "PE"): return True
            return False

        def row_id(row: Dict[str, Any]) -> str:
            return str(row.get("id") or row.get("trade_id") or "")

        def is_simulated_history_row(row: Dict[str, Any]) -> bool:
            return row_id(row).startswith(("H_", "EOD_"))

        def row_session_date(row: Dict[str, Any]):
            try:
                ts_value = row.get("entry_timestamp") or 0
                if ts_value:
                    return datetime.fromtimestamp(float(ts_value), tz=IST).date()
            except Exception:
                pass
            for key in ("timestamp", "signal_timestamp", "entry_time"):
                raw = row.get(key)
                if not raw:
                    continue
                try:
                    parsed = datetime.fromisoformat(str(raw))
                    if parsed.tzinfo is not None:
                        parsed = parsed.astimezone(IST).replace(tzinfo=None)
                    return parsed.date()
                except Exception:
                    continue
            return None

        def real_payload_row_allowed(row: Dict[str, Any]) -> bool:
            if is_simulated_history_row(row):
                return False
            day = row_session_date(row)
            if day is None:
                return True
            session_day = datetime.fromisoformat(str(self._get_current_session_day())).date()
            return day == session_day

        def historical_payload_row_allowed(row: Dict[str, Any]) -> bool:
            day = row_session_date(row)
            if day is None:
                return True
            return day <= self.historical_session_cutoff_date()

        def row_event_seconds(row: Dict[str, Any], prefer_exit: bool = False) -> float:
            keys = (
                ("exit_timestamp", "entry_timestamp", "timestamp", "signal_timestamp", "entry_time")
                if prefer_exit
                else ("entry_timestamp", "timestamp", "signal_timestamp", "exit_timestamp", "entry_time")
            )
            for key in keys:
                raw = row.get(key)
                if raw in (None, ""):
                    continue
                try:
                    value = float(raw)
                    return value / 1000.0 if value > 1e12 else value
                except (TypeError, ValueError):
                    pass
                try:
                    parsed = datetime.fromisoformat(str(raw))
                    if parsed.tzinfo is None:
                        parsed = IST.localize(parsed)
                    else:
                        parsed = parsed.astimezone(IST)
                    return parsed.timestamp()
                except (TypeError, ValueError):
                    continue
            return 0.0

        def reconcile_display_row(row: Dict[str, Any]) -> Dict[str, Any]:
            """Final accounting guard for legacy rows before dashboard display."""
            row = dict(row)
            inst_type = str(row.get("inst_type") or "FUT").upper()
            direction = str(row.get("direction") or "LONG").upper()
            entry_px = float(row.get("entry_price") or row.get("price") or 0.0)
            if entry_px <= 0:
                return row
            stop = float(row.get("trailing_stop") or row.get("stop") or 0.0)
            target = float(row.get("target") or 0.0)
            exit_reason = str(row.get("exit_reason") or "").upper()
            exit_px = float(row.get("exit_price") or row.get("current_price") or 0.0)
            if exit_reason in {"TARGET_HIT"} and target > 0:
                exit_px = target
            elif exit_reason in {"STOP_HIT", "TRAILING_STOP", "SL HIT", "SL_HIT"} and stop > 0:
                exit_px = stop
            if exit_px <= 0:
                return row
            qty = int(row.get("lots") or 1) * int(row.get("lot_size") or 1)
            settings = get_settings()
            charges = float(settings.fut_cost) if inst_type == "FUT" else float(settings.opt_cost)
            if inst_type == "OPT" or direction == "LONG":
                gross = (exit_px - entry_px) * qty
            else:
                gross = (entry_px - exit_px) * qty
            row["exit_price"] = exit_px
            row["current_price"] = exit_px
            row["pnl"] = round(gross - charges, 2)
            return row

        payload["closed"] = [reconcile_display_row(r) for r in payload.get("closed", []) or []]
        if self.mode == "REAL":
            payload["open"] = [r for r in payload.get("open", []) or [] if real_payload_row_allowed(r)]
            payload["closed"] = [r for r in payload.get("closed", []) or [] if real_payload_row_allowed(r)]
        elif self.mode == "HISTORICAL":
            payload["open"] = [r for r in payload.get("open", []) or [] if historical_payload_row_allowed(r)]
            payload["closed"] = [r for r in payload.get("closed", []) or [] if historical_payload_row_allowed(r)]

        def rebuild_display_accounting() -> None:
            """Keep headline metrics identical to the final reconciled dashboard rows."""
            closed_rows = list(payload.get("closed", []) or [])
            open_rows = list(payload.get("open", []) or [])
            closed_pnls = [float(row.get("pnl") or 0.0) for row in closed_rows]
            open_pnls = [float(row.get("pnl") or 0.0) for row in open_rows]
            total_pnl = sum(closed_pnls) + sum(open_pnls)
            vector_backend = "cpu"
            try:
                from engine.gpu_accelerator import get_gpu_accelerator
                vector_stats = get_gpu_accelerator().vector_stats(closed_pnls)
                vector_backend = vector_stats.backend
                wins = vector_stats.wins
                losses = vector_stats.losses
            except Exception:
                wins = sum(1 for value in closed_pnls if value > 0)
                losses = len(closed_pnls) - wins
            profits = sum(value for value in closed_pnls if value > 0)
            losses_abs = abs(sum(value for value in closed_pnls if value < 0))
            profit_factor = profits / losses_abs if losses_abs > 0 else (profits if profits > 0 else 1.0)

            sharpe = 0.0
            if len(closed_pnls) > 2:
                try:
                    std = float(vector_stats.std)  # type: ignore[name-defined]
                    mean = float(vector_stats.mean)  # type: ignore[name-defined]
                except Exception:
                    pnl_array = np.asarray(closed_pnls, dtype=float)
                    std = float(np.std(pnl_array))
                    mean = float(np.mean(pnl_array))
                sharpe = float(mean / std * np.sqrt(252)) if std > 0 else 0.0

            chronological = sorted(
                closed_rows,
                key=row_event_seconds,
            )
            try:
                chronological_pnls = [float(row.get("pnl") or 0.0) for row in chronological]
                max_drawdown = get_gpu_accelerator().vector_stats(chronological_pnls).max_drawdown
            except Exception:
                running = 0.0
                peak = 0.0
                max_drawdown = 0.0
                for row in chronological:
                    running += float(row.get("pnl") or 0.0)
                    peak = max(peak, running)
                    max_drawdown = max(max_drawdown, peak - running)

            def parse_event_seconds(row: Dict[str, Any], prefer_exit: bool = False) -> float:
                keys = ("exit_timestamp", "entry_timestamp") if prefer_exit else ("entry_timestamp", "exit_timestamp")
                for key in keys:
                    raw = row.get(key)
                    if raw in (None, ""):
                        continue
                    try:
                        value = float(raw)
                        return value / 1000.0 if value > 1e12 else value
                    except Exception:
                        pass
                    try:
                        parsed = datetime.fromisoformat(str(raw))
                        if parsed.tzinfo is None:
                            parsed = IST.localize(parsed)
                        else:
                            parsed = parsed.astimezone(IST)
                        return parsed.timestamp()
                    except Exception:
                        continue
                return 0.0

            capital_total = float(getattr(self, "capital_total", 0.0) or getattr(get_settings(), "capital_total", 0.0) or 0.0)
            capital_fut = float(getattr(self, "capital_fut", 0.0) or getattr(get_settings(), "capital_fut", 0.0) or 0.0)
            capital_opt = float(getattr(self, "capital_opt", 0.0) or getattr(get_settings(), "capital_opt", 0.0) or 0.0)

            equity_curve = []
            running_equity = 0.0
            peak_equity = 0.0
            closed_for_equity = sorted(closed_rows, key=lambda row: parse_event_seconds(row, prefer_exit=True))
            for row in closed_for_equity:
                running_equity += float(row.get("pnl") or 0.0)
                peak_equity = max(peak_equity, running_equity)
                drawdown = peak_equity - running_equity
                event_ts = parse_event_seconds(row, prefer_exit=True) or parse_event_seconds(row)
                equity_curve.append({
                    "time": int(event_ts) if event_ts else len(equity_curve),
                    "equity": round(running_equity, 2),
                    "drawdown": round(drawdown, 2),
                    "drawdown_pct": round((drawdown / capital_total) * 100.0, 2) if capital_total > 0 else 0.0,
                    "pnl": round(float(row.get("pnl") or 0.0), 2),
                    "trade_id": row.get("id", ""),
                })
            open_unrealized = sum(float(row.get("pnl") or 0.0) for row in open_rows)
            current_equity = running_equity + open_unrealized
            current_peak = max(peak_equity, current_equity)
            current_drawdown = max(0.0, current_peak - current_equity)
            if open_rows:
                equity_curve.append({
                    "time": int(time.time()),
                    "equity": round(current_equity, 2),
                    "drawdown": round(current_drawdown, 2),
                    "drawdown_pct": round((current_drawdown / capital_total) * 100.0, 2) if capital_total > 0 else 0.0,
                    "pnl": round(open_unrealized, 2),
                    "trade_id": "OPEN_UNREALIZED",
                })

            def trade_allocation(row: Dict[str, Any]) -> float:
                qty = int(row.get("lots") or 0) * int(row.get("lot_size") or 0)
                entry = float(row.get("entry_price") or 0.0)
                return max(0.0, entry * qty)

            active_allocation = sum(trade_allocation(row) for row in open_rows)

            def compact_trade(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
                if not row:
                    return {}
                return {
                    "id": row.get("id", ""),
                    "instrument": row.get("instrument", "--"),
                    "inst_type": row.get("inst_type", "--"),
                    "timeframe": row.get("timeframe", "--"),
                    "direction": row.get("direction", "--"),
                    "pnl": round(float(row.get("pnl") or 0.0), 2),
                    "entry_time": row.get("entry_time", "--"),
                    "exit_time": row.get("exit_time", "--"),
                    "exit_reason": row.get("exit_reason", "--"),
                }

            def bucketize_detail(rows: List[Dict[str, Any]], key_fn: Callable[[Dict[str, Any]], str]) -> Dict[str, Any]:
                buckets: Dict[str, Dict[str, Any]] = {}
                for row in rows:
                    name = key_fn(row) or "--"
                    value = float(row.get("pnl") or 0.0)
                    bucket = buckets.setdefault(name, {"count": 0, "pnl": 0.0, "wins": 0, "losses": 0})
                    bucket["count"] += 1
                    bucket["pnl"] += value
                    if value > 0:
                        bucket["wins"] += 1
                    elif value < 0:
                        bucket["losses"] += 1
                for bucket in buckets.values():
                    count = int(bucket["count"] or 0)
                    bucket["pnl"] = round(float(bucket["pnl"] or 0.0), 2)
                    bucket["win_rate"] = round((bucket["wins"] / count) * 100.0, 1) if count else 0.0
                    bucket["avg_pnl"] = round(bucket["pnl"] / count, 2) if count else 0.0
                return dict(sorted(buckets.items(), key=lambda item: abs(item[1]["pnl"]), reverse=True))

            def period_totals(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
                today = datetime.now(IST).date()
                week_start = today - timedelta(days=today.weekday())
                month_start = today.replace(day=1)
                totals = {"daily": 0.0, "weekly": 0.0, "monthly": 0.0}
                for row in rows:
                    ts = parse_event_seconds(row, prefer_exit=True) or parse_event_seconds(row)
                    if not ts:
                        continue
                    day = datetime.fromtimestamp(ts, tz=IST).date()
                    value = float(row.get("pnl") or 0.0)
                    if day == today:
                        totals["daily"] += value
                    if day >= week_start:
                        totals["weekly"] += value
                    if day >= month_start:
                        totals["monthly"] += value
                return {key: round(value, 2) for key, value in totals.items()}

            hold_minutes = []
            for row in closed_rows:
                entry_ts = parse_event_seconds(row)
                exit_ts = parse_event_seconds(row, prefer_exit=True)
                if entry_ts and exit_ts and exit_ts > entry_ts:
                    hold_minutes.append((exit_ts - entry_ts) / 60.0)
            avg_hold = sum(hold_minutes) / len(hold_minutes) if hold_minutes else 0.0

            largest_winner = max(closed_rows, key=lambda row: float(row.get("pnl") or 0.0), default=None)
            largest_loser = min(closed_rows, key=lambda row: float(row.get("pnl") or 0.0), default=None)
            max_dd_pct = round((max_drawdown / capital_total) * 100.0, 2) if capital_total > 0 else 0.0
            current_dd_pct = round((current_drawdown / capital_total) * 100.0, 2) if capital_total > 0 else 0.0

            monetary = {
                "capital": {
                    "configured_demat_capital": round(capital_total, 2),
                    "futures_allocation": round(capital_fut, 2),
                    "options_allocation": round(capital_opt, 2),
                    "active_trade_allocation": round(active_allocation, 2),
                    "free_configured_capital": round(max(0.0, capital_total - active_allocation), 2),
                    "source": "configured_settings",
                },
                "headline": {
                    "win_rate": round((wins / len(closed_rows) * 100.0) if closed_rows else 0.0, 1),
                    "profit_factor": round(profit_factor, 2),
                    "sharpe_ratio": round(sharpe, 2),
                    "max_drawdown": round(max_drawdown, 2),
                    "max_drawdown_pct": max_dd_pct,
                    "current_drawdown": round(current_drawdown, 2),
                    "current_drawdown_pct": current_dd_pct,
                    "avg_hold_minutes": round(avg_hold, 1),
                },
                "period_pnl": period_totals(closed_rows),
                "largest_winner": compact_trade(largest_winner if largest_winner and float(largest_winner.get("pnl") or 0.0) > 0 else None),
                "largest_loser": compact_trade(largest_loser if largest_loser and float(largest_loser.get("pnl") or 0.0) < 0 else None),
                "breakdowns": {
                    "index": bucketize_detail(closed_rows, lambda row: str(row.get("instrument") or "--").split()[0]),
                    "instrument_type": bucketize_detail(closed_rows, lambda row: str(row.get("inst_type") or "--").upper()),
                    "timeframe": bucketize_detail(closed_rows, lambda row: str(row.get("timeframe") or "--")),
                },
                "live_positions": [
                    {
                        **compact_trade(row),
                        "allocation": round(trade_allocation(row), 2),
                        "target": round(float(row.get("target") or 0.0), 2),
                        "stop": round(float(row.get("trailing_stop") or 0.0), 2),
                        "rr_ratio": round(float(row.get("rr_ratio") or 0.0), 2),
                    }
                    for row in open_rows
                ],
            }

            def is_option(row: Dict[str, Any]) -> bool:
                return str(row.get("inst_type") or "FUT").upper() == "OPT"

            payload["summary"] = {
                "daily_pnl": round(total_pnl, 2),
                "fut_pnl": round(sum(float(row.get("pnl") or 0.0) for row in closed_rows + open_rows if not is_option(row)), 2),
                "opt_pnl": round(sum(float(row.get("pnl") or 0.0) for row in closed_rows + open_rows if is_option(row)), 2),
                "total_trades": len(closed_rows) + len(open_rows),
                "open_count": len(open_rows),
                "wins": wins,
                "losses": losses,
                "win_rate": (wins / len(closed_rows) * 100.0) if closed_rows else 0.0,
                "profit_factor": profit_factor,
                "sharpe_ratio": sharpe,
                "max_drawdown": max_drawdown,
                "vector_backend": vector_backend,
            }
            payload["equity_curve"] = equity_curve[-1000:]
            payload["monetary"] = monetary

            def bucketize(key: str) -> Dict[str, Any]:
                buckets: Dict[str, Dict[str, Any]] = {}
                for row in closed_rows:
                    name = str(row.get(key) or "--")
                    if key == "grade":
                        name = name.split()[0]
                    if key == "instrument":
                        name = name.split()[0]
                    bucket = buckets.setdefault(name, {"count": 0, "pnl": 0.0, "wins": 0, "losses": 0})
                    value = float(row.get("pnl") or 0.0)
                    bucket["count"] += 1
                    bucket["pnl"] += value
                    bucket["wins" if value > 0 else "losses"] += 1
                for bucket in buckets.values():
                    bucket["pnl"] = round(bucket["pnl"], 0)
                    bucket["win_rate"] = round(bucket["wins"] / bucket["count"] * 100.0, 1) if bucket["count"] else 0.0
                return dict(sorted(buckets.items(), key=lambda item: abs(item[1]["pnl"]), reverse=True))

            payload["analytics"] = {
                "by_timeframe": bucketize("timeframe"),
                "by_grade": bucketize("grade"),
                "by_instrument": bucketize("instrument"),
                "by_type": bucketize("inst_type"),
                "by_exit_reason": bucketize("exit_reason"),
                "open_by_type": payload.get("analytics", {}).get("open_by_type", {}),
            }

        rebuild_display_accounting()

        if self.mode != "HISTORICAL":
            signal_rows = []
            if self._loaded_session_is_current_day():
                instruments = list(getattr(self, "active_indices", []) or [])
                for instrument in (getattr(self, "_session_trade_candidates", {}) or {}).keys():
                    if instrument not in instruments:
                        instruments.append(instrument)
                session_day = getattr(self, "_session_candidate_day", self._get_current_session_day())
                for instrument in instruments:
                    for candidate in self._get_session_trade_candidates(instrument):
                        candidate_ts = getattr(candidate, "signal_timestamp", None)
                        candidate_day = candidate_ts.date().isoformat() if candidate_ts else session_day
                        if candidate_day != session_day:
                            continue
                        if inst_pref_match(candidate):
                            self._normalize_loaded_exit_candidate(candidate)
                            signal_rows.append(self._serialize_trade_candidate(candidate))
            # Separate active from exited signals so we don't show 'ghost' (closed) signals
            active_signals = []
            exit_keys = {
                r.get("lifecycle_id")
                for r in signal_rows if r.get("action") == "EXIT"
            }
            for r in signal_rows:
                if r.get("action") == "ENTRY":
                    key = r.get("lifecycle_id")
                    if key not in exit_keys:
                        active_signals.append(r)
            
            payload["signals"] = active_signals
            payload["meta"] = {
                **payload.get("meta", {}),
                "restored_signal_rows": len(active_signals),
                "source": "restored_session_signals" if active_signals and not (payload.get("open") or payload.get("closed")) else "mixed",
            }
            has_trade_rows = bool(payload.get("open") or payload.get("closed"))
            if signal_rows and not has_trade_rows:
                exit_rows = [r for r in signal_rows if r.get("action") == "EXIT"]
                exit_keys = {r.get("lifecycle_id") for r in exit_rows}
                active_rows = [
                    r for r in signal_rows
                    if r.get("action") == "ENTRY"
                    and r.get("lifecycle_id") not in exit_keys
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
                    "fut_pnl": round(sum(float(r.get("pnl") or 0.0) for r in accounting_rows if not (str(r.get("inst_type") or "").upper() == "OPT" or "CE" in str(r.get("trading_symbol") or "") or "PE" in str(r.get("trading_symbol") or "") or "CE" in str(r.get("instrument") or "") or "PE" in str(r.get("instrument") or ""))), 2),
                    "opt_pnl": round(sum(float(r.get("pnl") or 0.0) for r in accounting_rows if (str(r.get("inst_type") or "").upper() == "OPT" or "CE" in str(r.get("trading_symbol") or "") or "PE" in str(r.get("trading_symbol") or "") or "CE" in str(r.get("instrument") or "") or "PE" in str(r.get("instrument") or ""))), 2),
                    "total_trades": len(accounting_rows),
                    "open_count": len(active_rows),
                    "wins": wins,
                    "losses": losses,
                    "win_rate": round((wins / len(accounting_rows)) * 100, 1) if accounting_rows else 0.0,
                    "profit_factor": round(gross_win / gross_loss, 2) if gross_loss > 0 else (99.99 if gross_win > 0 else 1.0),
                }
                original_open = payload.get("open", [])
                original_closed = payload.get("closed", [])
                try:
                    payload["open"] = active_rows
                    payload["closed"] = exit_rows
                    rebuild_display_accounting()
                finally:
                    payload["open"] = original_open
                    payload["closed"] = original_closed
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

                def close_historical_entry_row(entry: Dict[str, Any]) -> Dict[str, Any]:
                    row = dict(entry)
                    entry_px = float(row.get("price") or 0.0)
                    exit_px = float(row.get("current_price") or row.get("exit_price") or entry_px or 0.0)
                    qty = int(row.get("lots") or 1) * int(row.get("lot_size") or 1)
                    inst_type = str(row.get("inst_type") or "FUT").upper()
                    direction = str(row.get("direction") or "LONG").upper()
                    if inst_type == "OPT" or direction == "LONG":
                        gross = (exit_px - entry_px) * qty
                    else:
                        gross = (entry_px - exit_px) * qty
                    settings = get_settings()
                    charges = float(settings.opt_cost) if inst_type == "OPT" else float(settings.fut_cost)

                    exit_ts = ""
                    raw_ts = row.get("timestamp") or row.get("signal_timestamp")
                    if raw_ts:
                        try:
                            entry_dt = datetime.fromisoformat(str(raw_ts))
                            if entry_dt.tzinfo is None:
                                entry_dt = IST.localize(entry_dt)
                            else:
                                entry_dt = entry_dt.astimezone(IST)
                            force_exit = self._configured_session_time("ut_force_exit_time", "15:25")
                            exit_ts = IST.localize(datetime.combine(entry_dt.date(), force_exit)).isoformat()
                        except Exception:
                            exit_ts = "15:25:00"

                    row.update({
                        "current_price": exit_px,
                        "exit_price": exit_px,
                        "status": "EXIT SIGNAL",
                        "action": "EXIT",
                        "is_exit": True,
                        "exit_reason": "SESSION_END",
                        "exit_timestamp": exit_ts or "15:25:00",
                        "pnl": round(gross - charges, 2),
                    })
                    return row
                
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
                    exit_row = reconcile_display_row(exit_lookup.get(key)) if exit_lookup.get(key) else None
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
                    else:
                        entry = close_historical_entry_row(entry)
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
                
                all_signals = [reconcile_display_row(r) for r in (merged_rows + simulated_signals)]
                all_signals = sorted(all_signals, key=lambda x: x.get("timestamp") or "", reverse=True)
                payload["signals"] = all_signals
                payload["meta"] = {
                    **payload.get("meta", {}),
                    "restored_signal_rows": len(merged_rows),
                    "simulated_signal_rows": len(simulated_signals),
                    "source": "historical_session_plus_simulation",
                }
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
                    "fut_pnl": round(sum(float(r.get("pnl") or 0.0) for r in accounting_rows if not (str(r.get("inst_type") or "").upper() == "OPT" or "CE" in str(r.get("trading_symbol") or "") or "PE" in str(r.get("trading_symbol") or "") or "CE" in str(r.get("instrument") or "") or "PE" in str(r.get("instrument") or ""))), 2),
                    "opt_pnl": round(sum(float(r.get("pnl") or 0.0) for r in accounting_rows if (str(r.get("inst_type") or "").upper() == "OPT" or "CE" in str(r.get("trading_symbol") or "") or "PE" in str(r.get("trading_symbol") or "") or "CE" in str(r.get("instrument") or "") or "PE" in str(r.get("instrument") or ""))), 2),
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
                
                all_trades = sorted(all_trades, key=row_event_seconds, reverse=True)
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
                    payload["meta"] = {
                        **payload.get("meta", {}),
                        "filtered_signal_rows": len(filtered_rows),
                        "source": "filtered_candidates",
                    }
                    payload["summary"] = {
                        **(payload.get("summary") or {}),
                        "total_trades": 0,
                        "open_count": 0,
                    }
                else:
                    payload["signals"] = self._cached_hist_signals
                    payload["meta"] = {
                        **payload.get("meta", {}),
                        "simulated_signal_rows": len(self._cached_hist_signals),
                        "source": "historical_trade_manager",
                    }
        return payload

    def _handle_opposite_signal_exit(self, instrument: str, signal, timeframe: str) -> bool:
        """Exit/notify on raw opposite UT signals before entry filters can block them."""
        if signal.signal_type not in ("BUY", "SELL"):
            return False

        signal_ts = signal.timestamp
        handled = False
        latest_rows = list(getattr(self, "_latest_trade_candidates", {}).get(instrument, []) or [])
        session_rows = self._active_session_entry_candidates(instrument, timeframe=timeframe, as_of=signal_ts)
        candidates_by_key: Dict[str, TradeCandidate] = {}
        for candidate in latest_rows + session_rows:
            candidates_by_key[self._candidate_history_key(candidate)] = candidate

        for previous in candidates_by_key.values():
            if previous.timeframe != timeframe:
                continue
            if getattr(previous, "action", "ENTRY") in ("EXIT", "NO_ENTRY"):
                continue
            prev_ts = getattr(previous, "signal_timestamp", None)
            if not prev_ts or signal_ts <= prev_ts:
                continue
            is_opposite = (
                (previous.direction == "LONG" and signal.signal_type == "SELL") or
                (previous.direction == "SHORT" and signal.signal_type == "BUY")
            )
            if not is_opposite:
                continue

            dedupe_key = f"MANUAL_{instrument}_{previous.timeframe}_{previous.direction}_{prev_ts.isoformat()}"
            if self._last_opposite_exit_signal_time.get(dedupe_key) == signal_ts:
                continue

            existing_exit = any(
                getattr(row, "action", "ENTRY") == "EXIT"
                and getattr(row, "timeframe", "") == previous.timeframe
                and getattr(row, "direction", "") == previous.direction
                and getattr(row, "signal_timestamp", None) == prev_ts
                for row in self._get_session_trade_candidates(instrument)
            )
            if existing_exit:
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
            self._inherit_exit_entry_metadata(exit_candidate, previous)

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

    def _market_location_entry_gate(self, candidate: TradeCandidate) -> Tuple[bool, str]:
        """Classify live entries that are badly located inside the intraday structure."""
        settings = get_settings()
        if not bool(getattr(settings, "ut_market_location_gate", True)):
            return True, ""
        if getattr(self, "mode", "") == "HISTORICAL":
            return True, ""

        try:
            df = self.candles.get_candles(candidate.instrument, "1min")
        except Exception as exc:
            logger.debug(f"Market location gate skipped for {candidate.instrument}: candle read failed: {exc}")
            return True, ""
        if df is None or len(df) < 12:
            return True, ""

        work = df.copy()
        if not isinstance(work.index, pd.DatetimeIndex):
            if "timestamp" not in work.columns:
                return True, ""
            work["timestamp"] = pd.to_datetime(work["timestamp"], errors="coerce")
            work = work.dropna(subset=["timestamp"]).set_index("timestamp")
        if work.empty or "close" not in work.columns:
            return True, ""

        work = work.sort_index()
        signal_ts = getattr(candidate, "signal_timestamp", None)
        if signal_ts is not None:
            signal_ts = pd.Timestamp(signal_ts)
            if signal_ts.tzinfo is not None:
                signal_ts = signal_ts.tz_convert(IST).tz_localize(None)
            work = work[work.index <= signal_ts]
        if len(work) < 12:
            return True, ""

        try:
            closes = work["close"].astype(float)
            highs = work["high"].astype(float) if "high" in work.columns else closes
            lows = work["low"].astype(float) if "low" in work.columns else closes
            opens = work["open"].astype(float) if "open" in work.columns else closes
        except Exception:
            return True, ""

        close = float(closes.iloc[-1])
        if close <= 0:
            return True, ""
        setattr(candidate, "entry_spot", close)
        day_high = float(highs.max())
        day_low = float(lows.min())
        day_range = max(day_high - day_low, close * 0.0001)
        position_pct = (close - day_low) / day_range
        ema9 = float(closes.ewm(span=9, adjust=False).mean().iloc[-1])
        ema21 = float(closes.ewm(span=21, adjust=False).mean().iloc[-1])
        prior = float(closes.iloc[-6]) if len(closes) >= 6 else float(closes.iloc[0])
        impulse_pct = ((close - prior) / close) * 100.0
        last_open = float(opens.iloc[-1])
        prev_close = float(closes.iloc[-2]) if len(closes) >= 2 else close
        direction = str(getattr(candidate, "direction", "") or "").upper()
        timeframe = str(getattr(candidate, "timeframe", "") or "")

        bullish_reversal = (
            position_pct <= 0.38
            and close > ema9
            and close > last_open
            and close > prev_close
        )
        bearish_reversal = (
            position_pct >= 0.62
            and close < ema9
            and close < last_open
            and close < prev_close
        )
        long_structure_ok = close >= ema21 and ema9 >= ema21
        short_structure_ok = close <= ema21 and ema9 <= ema21
        risk_reason = ""

        if direction == "LONG":
            if not long_structure_ok and not bullish_reversal:
                risk_reason = (
                    f"Market location risk LONG: spot {close:.2f} below/against EMA structure "
                    f"(EMA9 {ema9:.2f}, EMA21 {ema21:.2f}, day-pos {position_pct:.0%})"
                )
            if timeframe == "5min" and position_pct >= 0.78 and impulse_pct >= 0.18 and not bullish_reversal:
                risk_reason = (
                    f"Market location risk LONG chase: day-pos {position_pct:.0%}, "
                    f"last-5m impulse {impulse_pct:.2f}%"
                )
        elif direction == "SHORT":
            if not short_structure_ok and not bearish_reversal:
                risk_reason = (
                    f"Market location risk SHORT: spot {close:.2f} above/against EMA structure "
                    f"(EMA9 {ema9:.2f}, EMA21 {ema21:.2f}, day-pos {position_pct:.0%})"
                )
            if timeframe == "5min" and position_pct <= 0.22 and impulse_pct <= -0.18 and not bearish_reversal:
                risk_reason = (
                    f"Market location risk SHORT chase: day-pos {position_pct:.0%}, "
                    f"last-5m impulse {impulse_pct:.2f}%"
                )

        if risk_reason:
            setattr(candidate, "scalp_lock_mode", True)
            setattr(candidate, "location_risk", risk_reason)
            setattr(candidate, "location_position_pct", float(position_pct))
            return True, risk_reason

        setattr(candidate, "scalp_lock_mode", False)
        return True, ""

    def _candidate_15m_trend_agrees(self, candidate: TradeCandidate) -> bool:
        """Return whether the live 15m position supports this candidate's direction."""
        stored = getattr(candidate, "trend_15m_agrees", None)
        if stored is not None:
            return bool(stored)
        expected = 1 if str(getattr(candidate, "direction", "") or "").upper() == "LONG" else -1
        key = f"{getattr(candidate, 'instrument', '')}_15min"
        try:
            engine = getattr(getattr(self, "mtf", None), "engines", {}).get(key)
            state = engine.get_state(key) if engine else None
            position = int(getattr(state, "position", 0) or 0)
            agrees = position == expected
            setattr(candidate, "trend_15m_agrees", agrees)
            return agrees
        except Exception:
            return False

    def _passes_candidate_grade_preference(self, candidate: TradeCandidate) -> Tuple[bool, str]:
        """Apply grade preference without undoing an explicit 5m/15m agreement."""
        settings = get_settings()
        grade_pref = self._normalize_signal_grade_preference(
            getattr(settings, "signal_grade_preference", "auto")
        )
        grade_hierarchy = {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4}
        raw_grade = str(getattr(candidate, "grade", "") or "")
        base_grade = raw_grade.split()[0] if raw_grade else "C"
        sig_rank = grade_hierarchy.get(base_grade, 0)

        if str(getattr(candidate, "timeframe", "") or "") == "5min" and self._candidate_15m_trend_agrees(candidate):
            return True, "15min trend agreement override"

        current_regime = str(self.latest_regimes.get(candidate.instrument, "UNKNOWN") or "UNKNOWN").upper()
        regime_adaptation = bool(getattr(settings, "ut_regime_adaptation", True))
        is_choppy = regime_adaptation and current_regime in {
            "CHOPPY",
            "SIDEWAYS",
            "MEAN_REVERTING",
            "RANGING",
            "VOLATILE",
            "UNKNOWN",
        }
        if grade_pref == "B":
            min_rank = 1 if candidate.inst_type == "FUT" else 2
        elif grade_pref == "B+":
            min_rank = 2
        elif grade_pref == "A":
            min_rank = 3
        elif grade_pref == "A+":
            min_rank = 4
        else:
            min_rank = 3 if is_choppy else 2

        if sig_rank >= min_rank or "Recovered" in raw_grade or "EOD" in raw_grade:
            return True, ""
        reason = f"Low Grade: {raw_grade}" if sig_rank < 1 else f"Below Grade Pref ({grade_pref})"
        return False, reason

    def _passes_live_trade_ready_gate(self, candidate: TradeCandidate) -> bool:
        """Final live/manual gate before a signal is allowed into the session ledger."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return True

        settings = get_settings()
        grade_rank = SignalProcessor._grade_rank(getattr(candidate, "grade", ""))
        confidence = float(getattr(candidate, "confidence", 0.0) or 0.0)
        regime = str(self.latest_regimes.get(candidate.instrument, "UNKNOWN") or "UNKNOWN").upper()
        # Intraday Momentum Override
        adx_val = getattr(candidate, "adx_value", 0.0)
        momentum_threshold = float(getattr(settings, "momentum_override_threshold", 25.0) or 25.0)
        momentum_override = float(adx_val or 0.0) >= momentum_threshold
        choppy_regimes = {"CHOPPY", "SIDEWAYS", "MEAN_REVERTING", "RANGING", "VOLATILE", "UNKNOWN"}
        regime_adaptation = bool(getattr(settings, "ut_regime_adaptation", True))
        is_choppy = regime_adaptation and (regime in choppy_regimes) and not momentum_override
        if momentum_override and regime_adaptation and (regime in choppy_regimes):
            logger.info(f"🚀 [OVERRIDE] Live Choppy regime bypassed due to strong ADX ({adx_val:.1f} >= {momentum_threshold})")
        timeframe = str(getattr(candidate, "timeframe", "") or "")
        inst_type = str(getattr(candidate, "inst_type", "") or "")

        if not self._passes_timeframe_entry_policy(candidate):
            return False

        if regime == "UNKNOWN" and getattr(candidate, "grade", "") != "A+":
            logger.info(f"Live Gate: blocked {candidate.instrument} {timeframe} {candidate.direction}; UNKNOWN regime strictly requires A+ grade.")
            return False

        location_ok, location_reason = self._market_location_entry_gate(candidate)
        if location_reason:
            try:
                candidate.reasons = list(candidate.reasons or []) + [location_reason, "Scalp lock mode: protect first profit fast"]
            except Exception:
                pass
            logger.info(
                f"Live Gate: allowing {candidate.instrument} {timeframe} {candidate.direction} in scalp-lock mode; "
                f"{location_reason}"
            )

        # 5min is a timing/exit timeframe in live mode. It must be exceptional to become a trade signal.
        if timeframe == "5min":
            min_conf = SignalProcessor._relaxed_threshold(
                float(getattr(settings, "ut_5min_option_min_confidence", 0.70) or 0.70),
                settings,
            )
            trend_15m_agrees = self._candidate_15m_trend_agrees(candidate)
            grade_conf_ok = grade_rank >= 3 and confidence >= min_conf
            if not (grade_conf_ok or trend_15m_agrees):
                logger.info(
                    f"Live Gate: blocked {candidate.instrument} {timeframe} {candidate.direction}; "
                    f"needs A/A+ with >= {min_conf:.0%}, or 15min trend agreement "
                    f"({candidate.grade}, {confidence:.0%}, agree={trend_15m_agrees})."
                )
                return False
            if is_choppy and not (grade_conf_ok or trend_15m_agrees):
                logger.info(
                    f"Live Gate: blocked {candidate.instrument} {timeframe} {candidate.direction}; "
                    f"choppy regime needs A/A+ with >= {min_conf:.0%}, or 15min trend agreement."
                )
                return False

        # Options decay badly in range-bound sessions, so choppy options need A/A+ or very high confidence.
        if is_choppy and inst_type == "OPT" and not ((grade_rank >= 3 and confidence >= SignalProcessor._relaxed_threshold(0.72, settings)) or confidence >= SignalProcessor._relaxed_threshold(SignalProcessor._choppy_confidence_gate(settings), settings)):
            logger.info(
                f"Live Gate: blocked {candidate.instrument} {inst_type} {candidate.direction}; "
                f"range/choppy regime allows only A/A+ option setups or very high confidence."
            )
            return False

        # Never promote weak B-grade live/manual rows in choppy markets unless confidence is exceptional.
        # Baseline parity before leniency: confidence >= 0.72 for A/A+ choppy setups.
        # Baseline parity before leniency: confidence >= 0.72 or exceptional confidence bypasses choppy options.
        if is_choppy and grade_rank < 3 and confidence < SignalProcessor._relaxed_threshold(SignalProcessor._choppy_confidence_gate(settings), settings):
            logger.info(
                f"Live Gate: blocked {candidate.instrument} {candidate.direction}; "
                f"grade {candidate.grade} is too weak for {regime}."
            )
            return False

        return True

    def _get_live_data_health_label(self, now_ts: Optional[float] = None, is_live: bool = True) -> str:
        """Classify live feed freshness for dashboard health reporting."""
        if not is_live:
            return "HISTORICAL"
        now_ts = float(now_ts if now_ts is not None else time.time())

        last_ltp_time = float(getattr(getattr(self, "data", None), "_last_ltp_time", 0.0) or 0.0)
        if last_ltp_time > 0 and now_ts - last_ltp_time <= 60.0:
            return "OK"

        candles = getattr(self, "candles", None)
        status = {}
        latest = None
        try:
            status = candles.get_status() if candles and hasattr(candles, "get_status") else {}
        except Exception:
            status = {}
        try:
            latest = candles.get_max_timestamp() if candles and hasattr(candles, "get_max_timestamp") else None
        except Exception:
            latest = None

        has_candles = any(
            int(count or 0) > 0
            for tf_counts in (status or {}).values()
            for count in (tf_counts or {}).values()
        )
        if has_candles and latest is not None:
            latest_ts = latest.timestamp() if hasattr(latest, "timestamp") else 0.0
            if latest_ts and now_ts - latest_ts <= 120.0:
                return "OK"

        if not has_candles and last_ltp_time <= 0:
            return "WARMING"
        return "STALE"

    def _get_candidate_ltp_cached(self, candidate: TradeCandidate) -> Optional[float]:
        """Use in-memory candle prices for display rows; broker REST is a throttled fallback."""
        ttl = 10.0
        key = f"{candidate.instrument}:{candidate.symbol_token}"
        now = time.time()
        cached = self._candidate_ltp_cache.get(key)
        if cached and now - cached.get("time", 0.0) <= ttl:
            return cached.get("price")

        prefer_contract_quote = (
            str(getattr(candidate, "inst_type", "") or "").upper() == "FUT"
            and bool(candidate.trading_symbol and candidate.symbol_token)
        )
        exchange = "BFO" if candidate.instrument == "SENSEX" else "NFO"
        if prefer_contract_quote:
            try:
                price = self.data.get_ltp(exchange, candidate.trading_symbol, candidate.symbol_token)
                if price and price > 0:
                    self._candidate_ltp_cache[key] = {"price": float(price), "time": now}
                    return float(price)
            except Exception as e:
                logger.debug(f"Candidate futures price refresh failed for {candidate.trading_symbol}: {e}")

        candle_price = self.candles.get_latest_price(candidate.instrument)
        if candle_price and candle_price > 0:
            return float(candle_price)

        if not (candidate.trading_symbol and candidate.symbol_token):
            return None

        if not prefer_contract_quote:
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
        
        if getattr(self, "mode", "") != "HISTORICAL" and not self._loaded_session_is_current_day():
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

            if candidate.inst_type != "OPT" and (not current or current <= 0):
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
        """Serialize session exit evaluation across scan and fallback publishers."""
        lock = getattr(self, "_session_breach_lock", None)
        if lock is None:
            lock = threading.RLock()
            self._session_breach_lock = lock
        with lock:
            self._check_session_candidates_breaches_locked(instrument)

    def _check_session_candidates_breaches_locked(self, instrument: str) -> None:
        """Apply live trade-management parity to manual/session signal rows."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return

        if not self._loaded_session_is_current_day():
            return

        active_entries = self._active_session_entry_candidates(instrument)
        if not active_entries:
            return

        latest_time = (
            datetime.now(IST).replace(tzinfo=None)
            if getattr(self, "mode", "") != "HISTORICAL"
            else self.candles.get_max_timestamp()
        )
        if latest_time is not None:
            if getattr(latest_time, "tzinfo", None) is not None:
                latest_time = latest_time.astimezone(IST).replace(tzinfo=None)
            else:
                latest_time = latest_time.replace(tzinfo=None)
        else:
            latest_time = datetime.now(IST).replace(tzinfo=None)

        exits_to_create = []
        changed = False

        def create_exit(previous: TradeCandidate, exit_px: float, exit_reason: str, reason_text: str) -> None:
            nonlocal changed
            self._diag_inc("exit_reasons", exit_reason)
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
                reasons=list(previous.reasons or []) + [f"{reason_text} at {latest_time:%H:%M:%S}"],
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
            self._inherit_exit_entry_metadata(exit_candidate, previous)
            setattr(exit_candidate, "peak_pnl", float(getattr(previous, "peak_pnl", 0.0) or 0.0))
            setattr(exit_candidate, "max_drawdown", float(getattr(previous, "max_drawdown", 0.0) or 0.0))
            setattr(exit_candidate, "entry_spot", float(getattr(previous, "entry_spot", 0.0) or 0.0))
            setattr(exit_candidate, "scalp_lock_mode", bool(getattr(previous, "scalp_lock_mode", False)))
            setattr(exit_candidate, "location_risk", str(getattr(previous, "location_risk", "") or ""))
            setattr(exit_candidate, "location_position_pct", float(getattr(previous, "location_position_pct", 0.0) or 0.0))
            exits_to_create.append(exit_candidate)
            changed = True

            self.log_event(
                f"Manual/session exit for {previous.instrument} {previous.direction} ({previous.timeframe}): {reason_text} @ {exit_px:.2f}",
                "warning",
            )
            self._notify(
                f"MANUAL EXIT {previous.direction} {previous.instrument} ({previous.timeframe}): {reason_text} @ {exit_px:.2f}",
                "sell",
            )

        for previous in active_entries:
            current_price = previous.current_price or previous.price
            if not current_price or current_price <= 0:
                continue

            triggered = False
            exit_px = current_price
            exit_reason = ""
            reason_text = ""

            if not hasattr(previous, "initial_stop"):
                setattr(previous, "initial_stop", float(previous.spot_stop or previous.stop or 0.0))

            settings = get_settings()
            regime = str(
                getattr(self, "latest_regimes", {}).get(previous.instrument, "UNKNOWN") or "UNKNOWN"
            ).upper()

            # Intelligence early exit: manual rows get the same directional flip guard,
            # but only as a session exit/alert, never a broker action.
            if getattr(settings, "ut_intel_early_exit", False):
                intel_result = getattr(self, "_intel_cache", {}).get(previous.instrument)
                if intel_result:
                    reversal_reason = self._intel_reversal_reason(previous.direction, intel_result)
                    if reversal_reason:
                        defer_exit, defer_text = self._should_defer_option_intel_exit(
                            previous,
                            float(current_price),
                            latest_time,
                            reversal_reason,
                            regime=regime,
                        )
                        if defer_exit:
                            marker = f"{latest_time:%H:%M}:{reversal_reason}"
                            if getattr(previous, "_last_deferred_intel_exit", "") != marker:
                                setattr(previous, "_last_deferred_intel_exit", marker)
                                self.log_event(
                                    f"Deferred intelligence exit for {previous.instrument} {previous.timeframe}: {reversal_reason}; {defer_text}.",
                                    "info",
                                )
                        else:
                            create_exit(previous, float(current_price), "OI_PCR_REVERSAL", reversal_reason)
                            continue
                    score = (intel_result.get("aggregate", {}) or {}).get("score", 0.0)
                    if previous.direction == "LONG" and float(score or 0.0) < -40.0:
                        reason = f"Intelligence flip score {score:.1f}"
                        defer_exit, defer_text = self._should_defer_option_intel_exit(
                            previous,
                            float(current_price),
                            latest_time,
                            reason,
                            regime=regime,
                        )
                        if defer_exit:
                            marker = f"{latest_time:%H:%M}:{reason}"
                            if getattr(previous, "_last_deferred_intel_exit", "") != marker:
                                setattr(previous, "_last_deferred_intel_exit", marker)
                                self.log_event(
                                    f"Deferred intelligence exit for {previous.instrument} {previous.timeframe}: {reason}; {defer_text}.",
                                    "info",
                                )
                        else:
                            create_exit(previous, float(current_price), "INTEL_FLIP", reason)
                            continue
                    if previous.direction == "SHORT" and float(score or 0.0) > 40.0:
                        reason = f"Intelligence flip score {score:.1f}"
                        defer_exit, defer_text = self._should_defer_option_intel_exit(
                            previous,
                            float(current_price),
                            latest_time,
                            reason,
                            regime=regime,
                        )
                        if defer_exit:
                            marker = f"{latest_time:%H:%M}:{reason}"
                            if getattr(previous, "_last_deferred_intel_exit", "") != marker:
                                setattr(previous, "_last_deferred_intel_exit", marker)
                                self.log_event(
                                    f"Deferred intelligence exit for {previous.instrument} {previous.timeframe}: {reason}; {defer_text}.",
                                    "info",
                                )
                        else:
                            create_exit(previous, float(current_price), "INTEL_FLIP", reason)
                            continue

            # Smart trailing parity for manual/session rows.
            new_stop = float(previous.stop or 0.0)
            
            def map_spot_to_inst(spot_val: float) -> float:
                return self._candidate_price_from_spot_level(previous, spot_val)

            if getattr(settings, "ut_smart_trailing", False):
                key_tf = f"{previous.instrument}_{previous.timeframe}"
            else:
                key_tf = f"{previous.instrument}_1min"
            engine_tf = getattr(getattr(self, "mtf", None), "engines", {}).get(key_tf)
            if engine_tf:
                try:
                    state_tf = engine_tf.get_state(key_tf)
                    raw_ts = float(getattr(state_tf, "trailing_stop", 0.0) or 0.0)
                    if regime in {"TRENDING", "STRONG_TREND"} and raw_ts > 0:
                        mapped_ts = map_spot_to_inst(raw_ts)
                        if (
                            self._candidate_trailing_stop_is_valid(previous, mapped_ts, current_price)
                            and previous.direction == "LONG"
                        ):
                            new_stop = max(new_stop, mapped_ts)
                        elif self._candidate_trailing_stop_is_valid(previous, mapped_ts, current_price):
                            new_stop = min(new_stop, mapped_ts)
                except Exception:
                    pass

            if regime in {"VOLATILE", "CHOPPY", "MEAN_REVERTING", "UNKNOWN"}:
                try:
                    df_1m = self.candles.get_candles(previous.instrument, "1min")
                    if df_1m is not None and len(df_1m) > 2:
                        if previous.direction == "LONG":
                            mapped_ts = map_spot_to_inst(float(df_1m["low"].iloc[-2]))
                            if self._candidate_trailing_stop_is_valid(previous, mapped_ts, current_price):
                                new_stop = max(new_stop, mapped_ts)
                        else:
                            mapped_ts = map_spot_to_inst(float(df_1m["high"].iloc[-2]))
                            if self._candidate_trailing_stop_is_valid(previous, mapped_ts, current_price):
                                new_stop = min(new_stop, mapped_ts)
                except Exception:
                    pass

            initial_stop = float(getattr(previous, "initial_stop", previous.stop) or previous.stop or 0.0)
            risk_dist = abs(float(previous.price) - initial_stop)
            current_profit_pts = (
                float(current_price) - float(previous.price)
                if previous.direction == "LONG"
                else float(previous.price) - float(current_price)
            )
            if risk_dist > 0 and current_profit_pts > risk_dist:
                buffer = float(previous.price) * 0.0005
                if previous.direction == "LONG":
                    new_stop = max(new_stop, float(previous.price) + buffer)
                else:
                    new_stop = min(new_stop, float(previous.price) - buffer)

            if previous.target > 0 or getattr(previous, "runner_mode", False):
                rr_settings = get_settings()
                runner_unlock_ratio = min(1.0, max(0.50, float(getattr(rr_settings, "dynamic_rr_runner_unlock_ratio", 0.95) or 0.95)))
                runner_lock_pct = min(0.98, max(0.25, float(getattr(rr_settings, "dynamic_rr_runner_lock_pct", 0.90) or 0.90)))
                is_runner = bool(getattr(previous, "runner_mode", False))
                if not is_runner and previous.target > 0:
                    target_distance = abs(float(previous.target) - float(previous.price))
                    current_move = abs(float(current_price) - float(previous.price))
                    is_correct_direction = (
                        (previous.direction == "LONG" and current_price > previous.price)
                        or (previous.direction == "SHORT" and current_price < previous.price)
                    )
                    if is_correct_direction and target_distance > 0 and current_move >= target_distance * runner_unlock_ratio:
                        setattr(previous, "runner_mode", True)
                        previous.target = 0.0
                        changed = True
                        self.log_event(
                            f"Target 1 reached for manual/session {previous.instrument}; runner lock enabled.",
                            "trade",
                        )

                if getattr(previous, "runner_mode", False):
                    current_gain_pts = max(0.0, abs(float(current_price) - float(previous.price)))
                    locked_gain_pts = current_gain_pts * runner_lock_pct
                    new_runner_stop = (
                        float(previous.price) + locked_gain_pts
                        if previous.direction == "LONG"
                        else float(previous.price) - locked_gain_pts
                    )
                    if previous.direction == "LONG":
                        new_stop = max(new_stop, new_runner_stop)
                    else:
                        new_stop = min(new_stop, new_runner_stop)

            if new_stop > 0:
                adjusted_stop, breathing, clamped = self._apply_option_stop_breathing_room(
                    previous,
                    new_stop,
                    latest_time=latest_time,
                    regime=regime,
                )
                if clamped:
                    marker = f"{latest_time:%H:%M}:{adjusted_stop:.2f}"
                    if getattr(previous, "_last_breathing_stop_log", "") != marker:
                        setattr(previous, "_last_breathing_stop_log", marker)
                        self.log_event(
                            f"Option breathing room kept {previous.instrument} {previous.timeframe} stop at {adjusted_stop:.2f} "
                            f"(floor {breathing:.2f}) instead of {new_stop:.2f}.",
                            "info",
                        )
                    new_stop = adjusted_stop

            if new_stop > 0 and abs(new_stop - float(previous.stop or 0.0)) > 1e-9:
                previous.stop = float(new_stop)
                changed = True

            current_pnl_rs = self._candidate_gross_pnl(previous, float(current_price))
            previous.pnl = self._candidate_net_pnl(previous, float(current_price))
            candle_extreme_price = self._candidate_candle_extreme_price(previous, latest_time)
            candle_extreme_pnl = (
                self._candidate_gross_pnl(previous, candle_extreme_price)
                if candle_extreme_price > 0
                else 0.0
            )
            previous_peak = max(float(getattr(previous, "peak_pnl", 0.0) or 0.0), current_pnl_rs, candle_extreme_pnl)
            if abs(previous_peak - float(getattr(previous, "peak_pnl", 0.0) or 0.0)) > 1e-9:
                setattr(previous, "peak_pnl", previous_peak)
                changed = True
            qty = int(previous.lots or 1) * int(previous.lot_size or 1)
            c_mult = getattr(previous, "multiplier", 1.0) if getattr(previous, "inst_type", "FUT") != "OPT" else 1.0
            lock_stop, lock_reason, lock_ratio = self._profit_lock_floor(
                float(previous.price or 0.0),
                previous.direction,
                qty,
                previous_peak,
                bool(getattr(previous, "scalp_lock_mode", False)),
                inst_mult=c_mult,
            )
            if (
                lock_reason == "MAJOR_WIN_GUARD"
                and str(getattr(previous, "inst_type", "FUT") or "FUT").upper() == "FUT"
            ):
                should_exit_major, guard_context = self._should_exit_major_win_guard(
                    previous,
                    float(current_price),
                    previous_peak,
                    current_pnl_rs,
                    latest_time,
                )
                if not should_exit_major:
                    lock_stop = 0.0
                    lock_reason = ""
                    marker = f"{latest_time:%H:%M}:{previous_peak:.0f}:{current_pnl_rs:.0f}"
                    if getattr(previous, "_last_major_win_defer_log", "") != marker:
                        setattr(previous, "_last_major_win_defer_log", marker)
                        self.log_event(
                            f"Deferred major-win lock for {previous.instrument} {previous.timeframe}: {guard_context}.",
                            "info",
                        )
            if lock_stop > 0:
                old_stop = float(previous.stop or 0.0)
                if previous.direction == "LONG":
                    new_stop = max(float(previous.stop or 0.0), lock_stop)
                else:
                    new_stop = min(float(previous.stop or lock_stop), lock_stop)
                new_stop, breathing, clamped = self._apply_option_stop_breathing_room(
                    previous,
                    new_stop,
                    latest_time=latest_time,
                    regime=regime,
                )
                if clamped:
                    marker = f"{latest_time:%H:%M}:{new_stop:.2f}"
                    if getattr(previous, "_last_breathing_stop_log", "") != marker:
                        setattr(previous, "_last_breathing_stop_log", marker)
                        self.log_event(
                            f"Option breathing room delayed profit-lock stop for {previous.instrument} {previous.timeframe}; "
                            f"floor {breathing:.2f}.",
                            "info",
                        )
                if abs(new_stop - old_stop) > 1e-9:
                    previous.stop = float(new_stop)
                    changed = True
            previous_dd = max(
                float(getattr(previous, "max_drawdown", 0.0) or 0.0),
                previous_peak - current_pnl_rs,
            )
            if abs(previous_dd - float(getattr(previous, "max_drawdown", 0.0) or 0.0)) > 1e-9:
                setattr(previous, "max_drawdown", previous_dd)
                changed = True

            entry_value = abs(float(previous.price or 0.0)) * int(previous.lots or 1) * int(previous.lot_size or 1)
            peak_gain_pct = (previous_peak / entry_value) * 100.0 if entry_value > 0 else 0.0
            current_gain_pct = (current_pnl_rs / entry_value) * 100.0 if entry_value > 0 else 0.0

            if lock_reason and self._candidate_stop_hit(previous, current_price):
                locked_pnl = self._candidate_gross_pnl(previous, float(previous.stop or current_price))
                create_exit(
                    previous,
                    float(previous.stop or current_price),
                    lock_reason,
                    f"Profit lock stop hit ({lock_ratio:.0%} of peak Rs.{previous_peak:.0f}, locked Rs.{locked_pnl:.0f})",
                )
                continue

            if peak_gain_pct >= 10.0 and (current_gain_pct < peak_gain_pct * 0.40 or current_gain_pct < 2.0):
                create_exit(previous, float(current_price), "LOW_GAIN_PROTECT", f"Low-gain protection peak {peak_gain_pct:.1f}%, current {current_gain_pct:.1f}%")
                continue

            if previous_peak >= 3000.0 and current_pnl_rs < previous_peak * 0.75:
                should_exit, guard_context = (
                    self._should_exit_major_win_guard(
                        previous,
                        float(current_price),
                        previous_peak,
                        current_pnl_rs,
                        latest_time,
                    )
                    if str(getattr(previous, "inst_type", "FUT") or "FUT").upper() == "FUT"
                    else (True, "option major-win guard")
                )
                if should_exit:
                    exit_floor = float(lock_stop or current_price)
                    create_exit(
                        previous,
                        exit_floor,
                        "MAJOR_WIN_GUARD",
                        f"Major win guard peak Rs.{previous_peak:.0f}, current Rs.{current_pnl_rs:.0f}; {guard_context}",
                    )
                    continue
                marker = f"{latest_time:%H:%M}:{previous_peak:.0f}:{current_pnl_rs:.0f}"
                if getattr(previous, "_last_major_win_defer_log", "") != marker:
                    setattr(previous, "_last_major_win_defer_log", marker)
                    self.log_event(
                        f"Deferred major-win exit for {previous.instrument} {previous.timeframe}: {guard_context}.",
                        "info",
                    )

            scalp_mode = bool(getattr(previous, "scalp_lock_mode", False))
            smart_min_peak = 500.0 if scalp_mode else 1000.0
            smart_ratio = 0.70 if scalp_mode else 0.65
            if smart_min_peak <= previous_peak < 3000.0 and (current_pnl_rs < previous_peak * smart_ratio or current_pnl_rs < 450.0):
                exit_floor = float(lock_stop or current_price)
                create_exit(previous, exit_floor, "SMART_PROFIT_LOCK", f"Smart profit lock peak Rs.{previous_peak:.0f}, current Rs.{current_pnl_rs:.0f}")
                continue

            should_stagnate, stagnation_context = self._stagnation_exit_decision(
                previous,
                current_pnl_rs,
                previous_peak,
                latest_time,
                regime=regime,
            )
            if should_stagnate:
                create_exit(previous, float(current_price), "STAGNATION_EXIT", stagnation_context)
                continue

            # ── REPAINT GUARD EXIT ──────────────────────────────────────
            # If the source candle has fully closed, verify the UT Bot
            # signal that originally triggered this trade is still present.
            # For 15min trades, we also check periodically (every 2 mins)
            # while the candle is still forming to abort earlier.
            sig_ts = getattr(previous, "signal_timestamp", None)
            repaint_checked = bool(getattr(previous, "_repaint_guard_checked", False))
            if sig_ts and not repaint_checked:
                is_candle_closed = self._live_signal_candle_has_closed(previous, sig_ts, latest_time)
                
                # Intra-candle and Edge-case handling for 15min and 5min timeframes
                if previous.timeframe in ("15min", "5min"):
                    check_ts = sig_ts
                    if getattr(check_ts, "tzinfo", None) is not None:
                        check_ts = check_ts.astimezone(IST).replace(tzinfo=None)
                        
                    tf_mins = 15 if previous.timeframe == "15min" else 5
                    candle_end = check_ts + timedelta(minutes=tf_mins)
                    
                    # 1. Edge Case: Signal generated in last 1 min of candle
                    is_last_minute = getattr(previous, "_is_last_minute_entry", None)
                    if is_last_minute is None:
                        # If the time we first start monitoring it is >= candle_end - 1min
                        # FIX H2: use check_ts instead of latest_time
                        is_last_minute = check_ts >= (candle_end - timedelta(minutes=1))
                        setattr(previous, "_is_last_minute_entry", is_last_minute)
                        
                    if is_last_minute:
                        # Extend the "closed" definition by 1 minute into the next candle
                        is_candle_closed = latest_time >= (candle_end + timedelta(minutes=1))
                        
                    # 2. Periodic 1-minute checks
                    if not is_candle_closed:
                        next_check = getattr(previous, "_next_repaint_check", None)
                        if next_check is None:
                            # Start clock from the first time we monitor it (post-buffer)
                            next_check = latest_time + timedelta(minutes=1)
                            setattr(previous, "_next_repaint_check", next_check)
                            
                        if latest_time >= next_check:
                            still_present = self._live_utbot_signal_still_present(previous, sig_ts)
                            self._diag_inc("repaint_guard", "checked")
                            if not still_present:
                                self._diag_inc("repaint_guard", "aborted")
                                logger.warning(
                                    f"🚨 [INTRA-CANDLE REPAINT ABORT] Signal for {previous.instrument} {previous.direction} "
                                    f"({previous.timeframe}) at {sig_ts} repainted during 1-minute check. Force-exiting."
                                )
                                create_exit(previous, float(current_price), "REPAINT_ABORT",
                                            f"Signal repainted intra-candle (1m check)")
                                continue
                                
                            self._diag_inc("repaint_guard", "passed")
                            setattr(previous, "_next_repaint_check", next_check + timedelta(minutes=1))
                            changed = True

                # Final close check
                if is_candle_closed:
                    still_present = self._live_utbot_signal_still_present(previous, sig_ts)
                    setattr(previous, "_repaint_guard_checked", True)
                    setattr(previous, "_next_repaint_check", None)  # FIX H1
                    changed = True  # persist the flag so we don't re-check
                    self._diag_inc("repaint_guard", "checked")
                    if not still_present:
                        self._diag_inc("repaint_guard", "aborted")
                        logger.warning(
                            f"🚨 [REPAINT ABORT] Signal for {previous.instrument} {previous.direction} "
                            f"({previous.timeframe}) at {sig_ts} repainted after candle close. Force-exiting."
                        )
                        create_exit(previous, float(current_price), "REPAINT_ABORT",
                                    f"Signal repainted after candle close")
                        continue
                    self._diag_inc("repaint_guard", "passed")
            # ── END REPAINT GUARD ──────────────────────────────────────

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
                create_exit(previous, float(exit_px), exit_reason, reason_text)

        if exits_to_create:
            self._remember_trade_candidates(exits_to_create)
            for exit_candidate in exits_to_create:
                self._latest_exit_candidates[exit_candidate.instrument] = [exit_candidate]
            changed = True
            
        if changed:
            self._persist_session_trade_candidates()

    def _prune_session_trade_candidates_for_live_gate(self) -> None:
        """Remove pre-gate/weak rows left in today's manual ledger from older builds."""
        if getattr(self, "mode", "") == "HISTORICAL":
            return

        if not self._loaded_session_is_current_day():
            return

        store = getattr(self, "_session_trade_candidates", {}) or {}
        changed = False
        for instrument, book in list(store.items()):
            removed_entries = set()
            for key, candidate in list(book.items()):
                if not self._candidate_matches_live_session_day(candidate):
                    book.pop(key, None)
                    changed = True
                    continue
                action = getattr(candidate, "action", "ENTRY")
                if action == "EXIT":
                    continue
                if action == "NO_ENTRY":
                    book.pop(key, None)
                    changed = True
                    continue
                if bool(getattr(candidate, "accepted_by_gate", False)):
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

    def _is_aplus_setup(self, item) -> bool:
        return SignalProcessor._grade_rank(getattr(item, "grade", "")) >= 4 or float(getattr(item, "confidence", 0.0) or 0.0) >= 0.90

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

        existing_rank = SignalProcessor._grade_rank(getattr(existing_trade, "grade", ""))
        candidate_rank = SignalProcessor._grade_rank(candidate.grade)
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
        """Do not pre-drop valid signals merely because other indices fired together."""
        return True

    def _correlated_index_bucket(self, instrument: str) -> str:
        base = str(instrument or "").split()[0].upper()
        if base == self.MIDCAP_CORRELATED_INDEX:
            return "MIDCAP"
        return "CORE"

    def _correlated_same_direction_counts(self, direction: str) -> Tuple[int, int]:
        """Count active same-direction exposures, including manual session candidates."""
        core_seen = set()
        midcap_seen = set()

        def add_exposure(obj):
            if getattr(obj, "direction", None) != direction:
                return
            instrument = str(getattr(obj, "instrument", "") or "").split()[0].upper()
            timeframe = str(getattr(obj, "timeframe", "") or "")
            key = (instrument, timeframe, str(getattr(obj, "inst_type", "") or ""))
            if self._correlated_index_bucket(instrument) == "MIDCAP":
                midcap_seen.add(key)
            else:
                core_seen.add(key)

        for trade in getattr(self.trades, "open_trades", {}).values():
            add_exposure(trade)

        if getattr(self, "mode", "") != "HISTORICAL":
            for instrument in getattr(self, "active_indices", []) or []:
                for row in self._active_session_entry_candidates(instrument):
                    add_exposure(row)

        return len(core_seen), len(midcap_seen)

    def _select_correlated_batch(self, signals: List[TradeCandidate], direction: str) -> Tuple[List[TradeCandidate], List[TradeCandidate]]:
        """Preserve every simultaneous signal for independent eligibility checks."""
        return list(signals), []

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
            intel_score = normalize_intelligence_score(agg.get("score", 0.0))
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

    @staticmethod
    def _historical_15m_trend_agrees(mtf_result, ts, direction: str) -> bool:
        """Resolve the latest 15m signal state available at a historical timestamp."""
        result = getattr(mtf_result, "results_15min", None) or {}
        signals = result.get("signals", []) or []
        if not signals:
            return False
        naive_ts = ts.replace(tzinfo=None) if getattr(ts, "tzinfo", None) else ts
        prior = []
        for signal in signals:
            signal_ts = signal.timestamp.replace(tzinfo=None) if getattr(signal.timestamp, "tzinfo", None) else signal.timestamp
            if signal_ts <= naive_ts:
                prior.append(signal)
        if not prior:
            return False
        latest = max(
            prior,
            key=lambda signal: signal.timestamp.replace(tzinfo=None)
            if getattr(signal.timestamp, "tzinfo", None)
            else signal.timestamp,
        )
        expected = "BUY" if str(direction or "").upper() == "LONG" else "SELL"
        return str(getattr(latest, "signal_type", "") or "").upper() == expected

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

        high_grade = grade in {"A", "A+", "B+"}
        strong_momentum = high_grade and (conf >= 0.62 or score >= 0.45 or intel >= 0.55 or adx >= 28.0)
        high_iv = ivp >= 75.0
        high_spot_vol = atr_pct >= 0.65
        option_quality_ok = False
        try:
            option_quality_ok = self.signal_processor._option_grade_allowed(
                grade,
                conf,
                regime,
                adx_value=adx,
                intel_score=intel_score,
                signal_score=score,
            )
        except Exception:
            option_quality_ok = grade in {"A", "A+"} and conf >= 0.70
        more_results = (
            getattr(get_settings(), "ut_backtest_more_results", True)
            and getattr(self, "mode", "") == "HISTORICAL"
        )

        def choose(inst_type: str, reason: str) -> str:
            if getattr(self, "mode", "") != "HISTORICAL":
                logger.info(
                    f"Instrument Type Decision: {instrument} {grade} {conf:.0%} "
                    f"regime={regime} adx={adx:.1f} atr%={atr_pct:.2f} ivp={ivp:.1f} "
                    f"-> {inst_type} ({reason})"
                )
            return inst_type

        is_late_session = False
        is_0dte = False
        if signal_time is not None:
            is_late_session = signal_time.time() >= dtime(14, 0)
            try:
                is_0dte = self.expiry.is_expiry_day(instrument, signal_time.date())
            except Exception:
                is_0dte = False

        if is_0dte and is_late_session and not strong_momentum:
            return choose("FUT", "0DTE late-session without strong momentum")
        if high_iv:
            return choose("FUT", "high IV; avoid inflated option premium")
        if strong_momentum and option_quality_ok and not high_spot_vol:
            return choose("OPT", "strong momentum passed option-quality gate")
        if more_results and strong_momentum and not high_spot_vol:
            return choose("OPT", "historical exploratory strong momentum")
        if high_spot_vol:
            return choose("FUT", "high spot volatility; prefer linear futures tracking")
        if is_choppy:
            return choose("FUT", "choppy/volatile regime without option-quality confirmation")
        if strong_momentum:
            return choose("OPT", "clean strong momentum")
        return choose("FUT", "default AUTO preference for ordinary setup")

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
            self._last_signal_time[backfill_key] = datetime.now(IST).replace(tzinfo=None)

            # Load last N days for Historical mode, or 1 day for Live modes (Warm Startup)
            is_live = "REAL" in self.mode.upper()
            if (self.mode == "HISTORICAL" or is_live):
                settings = get_settings()

                # Fetch 5min candles to determine trading dates for lookback
                candles_df = self.candles.get_candles(instrument, "5min")
                if candles_df is not None and not candles_df.empty:
                    data_lookback = 45 # Use up to 45 trading days for indicator warm-up
                    trade_lookback = 1 if is_live else self.backtest_days

                    # Extract unique trading dates
                    active_dates = sorted(list(set(candles_df.index.date)))
                    if active_dates:
                        trade_lookback = 1 if is_live else int(self.backtest_days)
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
                                intel_score=hist_intel_score,
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

                            grade_pref = self._normalize_signal_grade_preference(getattr(settings, "signal_grade_preference", "auto"))
                            is_choppy = hist_regime in {"CHOPPY", "SIDEWAYS", "MEAN_REVERTING", "RANGING", "VOLATILE", "UNKNOWN"}
                            direction = "LONG" if s1.signal_type == "BUY" else "SHORT"

                            # -- Aligned Quality Gates (mirror _passes_live_trade_ready_gate) --

                            # 5min is a timing/exit TF; must be exceptional to become a trade signal
                            trend_15m_agrees = False
                            if tf == "5min":
                                min_conf_5m = float(getattr(settings, "ut_5min_option_min_confidence", 0.70) or 0.70)
                                trend_15m_agrees = self._historical_15m_trend_agrees(mtf_result, s1.timestamp, direction)
                                grade_conf_ok = sig_rank >= 3 and hist_conf >= min_conf_5m
                                if not (grade_conf_ok or trend_15m_agrees):
                                    self._record_historical_reject(
                                        s1,
                                        instrument,
                                        tf,
                                        direction,
                                        hist_grade,
                                        hist_conf,
                                        f"5min gate needs A/A+ with {min_conf_5m:.0%} or 15min trend agreement",
                                        sim_inst,
                                        lot_size,
                                        atm_strike,
                                    )
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
                            elif grade_pref == "A+":
                                min_rank = 4
                            else: # auto
                                min_rank = 3 if is_choppy else 2

                            if sig_rank < min_rank and not (tf == "5min" and trend_15m_agrees):
                                self._record_historical_reject(s1, instrument, tf, direction, hist_grade, hist_conf, f"Grade rank below required {min_rank}", sim_inst, lot_size, atm_strike)
                                continue

                            # Historical concurrency is applied after all workers finish.
                            # Concurrent historical workers must not filter against a
                            # partially built shared list. TradeManager enforces the
                            # configured concurrency policy in a stable final pass.

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
                                base_lots = self.user_lots_fut.get(instrument, 1)
                            else:
                                max_units = int(risk_amount / unit_risk)
                                user_target = self.user_lots.get(instrument, 1)
                                base_lots = min(user_target, max(1, int(max_units / lot_size)))

                            if hist_intel_score >= 0.8:
                                intel_lots = int(base_lots * 0.70)
                            elif hist_intel_score >= 0.4:
                                intel_lots = int(base_lots * 0.55)
                            elif hist_intel_score >= 0.0:
                                intel_lots = int(base_lots * 0.35)
                            else:
                                intel_lots = 1

                            regime_upper = hist_intel_regime.upper() if hist_intel_regime else ""
                            if regime_upper in {"RANGING", "CHOPPY"}:
                                intel_lots = int(intel_lots * 0.50)
                            elif regime_upper == "EXTREME":
                                intel_lots = 1

                            hist_lots = max(1, intel_lots)

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

                            settings = get_settings()
                            sim_charges = (
                                float(settings.opt_cost)
                                if sim_inst == "OPT"
                                else float(settings.fut_cost)
                            )

                            force_exit_time = self._configured_session_time("ut_force_exit_time", "15:25")
                            day_end_dt = datetime.combine(s1.timestamp.date(), force_exit_time)
                            is_overnight = (s2 is None) or (s2.timestamp.replace(tzinfo=None) if s2.timestamp.tzinfo else s2.timestamp) > day_end_dt

                            if is_overnight:
                                raw_exit_spot = s1.price # fallback
                                candles_for_tf = self.candles.get_candles(instrument, tf)
                                if candles_for_tf is not None and not candles_for_tf.empty:
                                    day_candles = candles_for_tf[candles_for_tf.index.date == s1.timestamp.date()]
                                    day_candles = day_candles[day_candles.index.time <= force_exit_time]
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
                                        raw_exit_premium -= theta_decay
                                    raw_exit_premium -= (entry_premium * 0.002)
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
                                        raw_exit_premium -= theta_decay
                                    raw_exit_premium -= (entry_premium * 0.002)
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
                            hist_rr = SignalProcessor._dynamic_rr(hist_grade, hist_conf)

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
                                trailing_stop=sl_price,
                                current_stop=sl_price,
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
                
                if inst_type == "FUT":
                    base_lots = self.user_lots_fut.get(instrument, 1)
                else:
                    lot_cost = est_premium * lot_size
                    pos_cap_limit = 100000.0
                    cap_based_lots = int(pos_cap_limit / max(1, lot_cost))
                    user_target = self.user_lots.get(instrument, 1)
                    base_lots = max(1, min(user_target, cap_based_lots))

                # --- Dynamic Intelligence Multiplier ---
                if intel_score >= 0.8:
                    intel_lots = int(base_lots * 0.70)
                elif intel_score >= 0.4:
                    intel_lots = int(base_lots * 0.55)
                elif intel_score >= 0.0:
                    intel_lots = int(base_lots * 0.35)
                else:
                    intel_lots = 1
                
                # --- Regime Penalty ---
                regime_upper = regime.upper() if regime else ""
                if regime_upper in {"RANGING", "CHOPPY"}:
                    intel_lots = int(intel_lots * 0.50)
                elif regime_upper == "EXTREME":
                    intel_lots = 1
                
                actual_lots = max(1, intel_lots)

                index_stop_distance = SignalProcessor._option_index_stop_distance(
                    natural_stop_distance=best_sig.stop_distance,
                    est_premium=est_premium,
                    options_sl_pct=self.options_sl_pct,
                    instrument_multiplier=instrument_multiplier,
                    settings=get_settings(),
                )

            # â• â•  RR and Direction â• â• 
            # Dynamic RR: confidence anchors the target, while grade/intel add bounded runway.
            final_rr = SignalProcessor._dynamic_rr(best_grade, best_conf, intel_score)
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

                # ── LIVE PREMIUM FETCH (Fixes BUG-02) ──
                live_trade_price = spot
                if inst_type == "OPT":
                    live_premium = self.data.get_ltp(cfg.get("option_exchange", "NFO"), trading_symbol, symbol_token)
                    live_trade_price = live_premium if live_premium and live_premium > 0 else (spot * 0.012)
                elif inst_type == "FUT":
                    live_fut = self.data.get_ltp(cfg.get("exchange", "NFO"), trading_symbol, symbol_token)
                    live_trade_price = live_fut if live_fut and live_fut > 0 else spot

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

    async def _track_repaint_until_candle_close(self, trade, candidate):
        """Continuously monitors if a signal repaints before its source candle closes."""
        try:
            if not getattr(trade, "id", None) or trade.status != "OPEN":
                return
                
            sig_timestamp = getattr(candidate, "signal_timestamp", None)
            if not sig_timestamp:
                return
                
            tf_minutes = {"1min": 1, "5min": 5, "15min": 15}.get(str(getattr(candidate, "timeframe", "") or ""), 5)
            # Find the time when the candle closes
            sig_time_naive = sig_timestamp.replace(tzinfo=None) if sig_timestamp.tzinfo else sig_timestamp
            candle_close_time = sig_time_naive + timedelta(minutes=tf_minutes)
            
            logger.info(f"🔍 Repaint Tracker Active for {trade.id} until candle closes at {candle_close_time.time()}")
            
            while trade.status == "OPEN":
                await asyncio.sleep(60.0) # Check every 1 minute
                
                now = datetime.now(IST).replace(tzinfo=None)
                if now >= candle_close_time:
                    logger.info(f"✅ Repaint Tracker complete for {trade.id} - candle securely closed.")
                    break
                    
                # Check if signal is still present
                still_present = self._live_utbot_signal_still_present(candidate, sig_timestamp)
                if not still_present:
                    logger.warning(f"🚨 REPAINT ABORT: Signal vanished for {trade.id}! Exiting immediately.")
                    self.log_event(f"🚨 REPAINT ABORT: Signal vanished for {candidate.instrument}. Exiting.", "error")
                    # Force Exit
                    price = self.candles.get_latest_price(candidate.instrument) or getattr(trade, "current_price", trade.entry_price)
                    self.trades.close_trade(trade.id, float(price), "REPAINT_ABORT")
                    break
        except Exception as e:
            logger.error(f"Error in repaint tracker for {getattr(trade, 'id', 'unknown')}: {e}")

    async def _coordinate_and_execute(self, candidates: List[TradeCandidate], is_warmup: bool = False):
        """
        Coordinate every matured signal through independent eligibility checks.
        Same-time signals are never pre-dropped by a cross-index quota.
        """
        accepted_candidates: List[TradeCandidate] = []

        if self.mode != "HISTORICAL" and not is_warmup:
            settings = get_settings()
            session_now = self._session_squareoff_clock()
            force_exit_time = self._configured_session_time("ut_force_exit_time", "15:25")
            final_entry_time = self._configured_session_time("ut_5min_no_entry_after", "15:15")
            if session_now.time() >= force_exit_time:
                if self.scan_count % 25 == 0:
                    logger.info(
                        f"Execution Gate: hard square-off window active after {force_exit_time.strftime('%H:%M')} IST."
                    )
                self._pending_live_signals.clear()
                return accepted_candidates

            if session_now.time() >= final_entry_time:
                pending_candidates = [
                    item.get("candidate")
                    for item in self._pending_live_signals.values()
                    if isinstance(item, dict) and item.get("candidate") is not None
                ]
                cutoff_eligible = any(
                    self._entry_cutoff_allows_candidate(candidate, session_now)
                    for candidate in list(candidates) + pending_candidates
                )
                if not cutoff_eligible:
                    if self.scan_count % 25 == 0:
                        logger.info(
                            f"Execution Gate: no eligible candle closed by "
                            f"{final_entry_time.strftime('%H:%M')} IST."
                        )
                    self._pending_live_signals.clear()
                    return accepted_candidates
        # ── Live Anti-Repaint Stabilization Buffer ──
        if self.mode != "HISTORICAL" and not is_warmup:
            now = datetime.now(IST).replace(tzinfo=None)
            matured_candidates = []
            pending_changed = False

            # Step 1: Add new candidates to the buffer if not already present
            for c in candidates:
                if getattr(c, "action", "ENTRY") != "ENTRY":
                    continue
                # Timeframe-Specific entry gate check
                c_time = session_now.time()
                limit_time = (
                    self._configured_session_time("ut_no_entry_after", "15:00")
                    if c.timeframe == "15min"
                    else self._configured_session_time("ut_5min_no_entry_after", "15:15")
                )
                if not self._entry_cutoff_allows_candidate(c, session_now):
                    logger.info(f"🚫 [LIVE GATE] Skipping buffering for {c.instrument} {c.timeframe} {c.direction} at {c_time} due to entry policy ({limit_time})")
                    continue

                key = f"{c.instrument}_{c.timeframe}_{c.direction}"
                if key not in self._pending_live_signals:
                    # Track the exact intrabar UTBot signal. This lets realtime
                    # signals form and repaint, but only mature if the same
                    # signal is still present after the stabilization delay.
                    sig_timestamp = getattr(c, "signal_timestamp", None)
                    cached_key = f"{c.instrument}_{c.timeframe}"
                    cached_res = self._cached_results.get(cached_key)
                    if sig_timestamp is None and cached_res and cached_res.get("signals"):
                        target_type = "BUY" if c.direction == "LONG" else "SELL"
                        matching_sigs = [s for s in cached_res["signals"] if s.signal_type == target_type]
                        if matching_sigs:
                            sig_timestamp = matching_sigs[-1].timestamp

                    self._pending_live_signals[key] = {
                        "timestamp": now,
                        "candidate": c,
                        "sig_timestamp": sig_timestamp
                    }
                    pending_changed = True
                    self._diag_inc("stabilization", "buffered")
                    stable_seconds = (
                        float(getattr(settings, "live_signal_stabilization_seconds_15min", 20.0) or 20.0)
                        if c.timeframe == "15min"
                        else float(getattr(settings, "live_signal_stabilization_seconds", 20.0) or 20.0)
                    )
                    logger.info(f"[STABILIZATION] Buffering {c.direction} signal for {c.instrument} {c.timeframe} for {stable_seconds:.0f}s...")
                    self.log_event(f"Buffering {c.direction} signal for {c.instrument} {c.timeframe} for {stable_seconds:.0f}s...", "system")

            if pending_changed:
                # A signal may mature before the normal 15-second checkpoint.
                # Persist immediately so a restart resumes this exact timer.
                self._save_warm_memory(force=True)

            # Step 2: Check which buffered signals have matured (held for >= 20 seconds)
            keys_to_remove = []
            for key, data in list(self._pending_live_signals.items()):
                c = data["candidate"]
                sig_timestamp = data.get("sig_timestamp")
                elapsed = (now - data["timestamp"]).total_seconds()
                tf_minutes = {"1min": 1, "5min": 5, "15min": 15}.get(str(getattr(c, "timeframe", "") or ""), 5)
                stable_seconds = (
                    float(getattr(settings, "live_signal_stabilization_seconds_15min", 20.0) or 20.0)
                    if c.timeframe == "15min"
                    else float(getattr(settings, "live_signal_stabilization_seconds", 20.0) or 20.0)
                )
                max_pending_seconds = (tf_minutes * 60.0) + 90.0
                if sig_timestamp:
                    sig_time_check = sig_timestamp
                    if getattr(sig_time_check, "tzinfo", None) is not None:
                        sig_time_check = sig_time_check.astimezone(IST).replace(tzinfo=None)
                    clock_check = session_now
                    if getattr(clock_check, "tzinfo", None) is not None:
                        clock_check = clock_check.astimezone(IST).replace(tzinfo=None)
                    if sig_time_check > (clock_check + timedelta(seconds=5)):
                        logger.warning(
                            f"[STABILIZATION] Signal for {key} has future timestamp {sig_time_check} "
                            f"relative to scanner clock {clock_check}. Discarding."
                        )
                        self.log_event(f"Future-timestamp signal discarded: {key}", "warning")
                        self._diag_inc("stabilization", "discarded_future")
                        self._forget_signal_display_rows(c)
                        keys_to_remove.append(key)
                        dedup_key = f"{c.instrument}_{c.timeframe}"
                        self._last_live_signal_candle_time.pop(dedup_key, None)
                        continue

                if elapsed > max_pending_seconds:
                    logger.warning(
                        f"[STABILIZATION] Signal for {key} expired after {elapsed:.1f}s "
                        "without a stable scan cadence. Discarding stale candidate."
                    )
                    self.log_event(f"Stale buffered signal discarded: {key}", "warning")
                    self._diag_inc("stabilization", "discarded_stale")
                    self._forget_signal_display_rows(c)
                    keys_to_remove.append(key)
                    dedup_key = f"{c.instrument}_{c.timeframe}"
                    self._last_live_signal_candle_time.pop(dedup_key, None)
                    continue

                cached_key = f"{c.instrument}_{c.timeframe}"
                if data.get("recovered") and cached_key not in self._cached_results:
                    # Startup may restore the checkpoint before the first fresh
                    # indicator pass. Wait for that pass instead of declaring
                    # the restored arrow repainted from an empty cache.
                    self._diag_inc("stabilization", "waiting_fresh_scan_after_restart")
                    continue

                still_present = self._live_utbot_signal_still_present(c, sig_timestamp)

                if not still_present:
                    self._diag_inc("stabilization", "discarded_repaint")
                    logger.warning(
                        f"[STABILIZATION] Rejected {c.instrument} {c.timeframe} {c.direction}: "
                        f"UTBot arrow vanished after {elapsed:.1f}s."
                    )
                    self.log_event(
                        f"Signal rejected: {c.instrument} {c.timeframe} {c.direction} "
                        f"repainted during the {stable_seconds:.0f}s stabilization window.",
                        "warning",
                    )
                    self._forget_signal_display_rows(c)
                    keys_to_remove.append(key)
                    dedup_key = f"{c.instrument}_{c.timeframe}"
                    identity = f"{sig_timestamp.replace(tzinfo=None).isoformat()}|{getattr(c, 'source_signal_type', '')}" if sig_timestamp else ""
                    last_signal_identity = getattr(self, "_last_signal_identity", {})
                    if last_signal_identity.get(dedup_key) == identity:
                        last_signal_identity.pop(dedup_key, None)
                        self._last_signal_time[dedup_key] = sig_timestamp.replace(tzinfo=None) - timedelta(microseconds=1)
                    self._candidate_process_cache.pop(c.instrument, None)
                    raw_key = (
                        f"{c.instrument}|{c.timeframe}|{getattr(c, 'source_signal_type', '')}|"
                        f"{sig_timestamp.replace(tzinfo=None).isoformat(timespec='seconds')}"
                        if sig_timestamp
                        else ""
                    )
                    if raw_key:
                        getattr(self, "_raw_utbot_activity_keys", set()).discard(raw_key)
                    continue

                if data.pop("recovered", False):
                    logger.info(
                        f"[STABILIZATION] Resumed {c.instrument} {c.timeframe} {c.direction} "
                        f"after restart with {elapsed:.1f}s already elapsed."
                    )
                    self.log_event(
                        f"Resumed signal buffer after restart: {c.instrument} {c.timeframe} "
                        f"{c.direction}, {elapsed:.1f}s already stable.",
                        "system",
                    )

                if elapsed < stable_seconds:
                    self._diag_inc("stabilization", "waiting_stability")
                    continue

                if elapsed >= stable_seconds:
                    c_time = session_now.time()
                    limit_time = (
                        self._configured_session_time("ut_no_entry_after", "15:00")
                        if c.timeframe == "15min"
                        else self._configured_session_time("ut_5min_no_entry_after", "15:15")
                    )
                    if not self._entry_cutoff_allows_candidate(c, session_now):
                        logger.info(f"🚫 [LIVE GATE] Discarding matured signal for {key} due to entry policy ({limit_time})")
                        keys_to_remove.append(key)
                        dedup_key = f"{c.instrument}_{c.timeframe}"
                        self._last_live_signal_candle_time.pop(dedup_key, None)
                        continue

                    logger.success(f"✅ [STABILIZATION] Signal for {key} matured after {elapsed:.1f}s. Proceeding to execute.")
                    self._diag_inc("stabilization", "matured")
                    self.log_event(
                        f"UTBot {c.direction} {c.instrument} {c.timeframe} stayed valid for {elapsed:.1f}s. Running filters.",
                        "trade",
                    )
                    matured_candidates.append(c)
                    keys_to_remove.append(key)
                    if sig_timestamp:
                        dedup_key = f"{c.instrument}_{c.timeframe}"
                        self._last_signal_time[dedup_key] = max(self._last_signal_time.get(dedup_key, datetime.min), sig_timestamp)

            for key in keys_to_remove:
                self._pending_live_signals.pop(key, None)
            if keys_to_remove:
                # Prevent a rejected or matured buffer from being resurrected
                # if another restart happens before the periodic checkpoint.
                self._save_warm_memory(force=True)

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
            open_count = len(self.trades.open_trades)

            # â•â•â• CROSS-INSTRUMENT CORRELATION GUARD â•â•â•
            # Rules:
            # 1. Max 2 concurrent trades per index.
            # 2. Every separate index receives its own complete eligibility pass.

            for i, sig in enumerate(signals):
                total_open = len(self.trades.open_trades)

                # â”€â”€ Grade Preference Filter â”€â”€
                settings = get_settings()
                grade_pref = self._normalize_signal_grade_preference(getattr(settings, "signal_grade_preference", "auto"))

                grade_hierarchy = {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4}
                base_grade = sig.grade.split()[0] if isinstance(sig.grade, str) else "C"
                sig_rank = grade_hierarchy.get(base_grade, 0)

                # Dynamic Regime-Aware minimum grade preference
                current_regime = self.latest_regimes.get(sig.instrument, "UNKNOWN")
                regime_adaptation = bool(getattr(settings, "ut_regime_adaptation", True))
                is_choppy = regime_adaptation and current_regime in {"CHOPPY", "SIDEWAYS", "MEAN_REVERTING", "RANGING", "VOLATILE", "UNKNOWN"}

                if grade_pref == "B":
                    # Futures allow B (Rank 1), Options require B+ (Rank 2)
                    min_rank = 1 if sig.inst_type == "FUT" else 2
                elif grade_pref == "B+":
                    min_rank = 2
                elif grade_pref == "A":
                    min_rank = 3
                elif grade_pref == "A+":
                    min_rank = 4
                else: # auto
                    # Dynamic upgrade to A/A+ under chops, baseline is B+
                    min_rank = 3 if is_choppy else 2

                grade_allowed, grade_preference_reason = self._passes_candidate_grade_preference(sig)
                if not grade_allowed:
                    reason_str = ", ".join(sig.reasons) if sig.reasons else "Low confidence"
                    rejection_reason = grade_preference_reason
                    self.log_event(f"ðŸš« Signal Rejected: {sig.instrument} {sig.direction} ({sig.grade}) - {rejection_reason} | Reasons: {reason_str}", "trade")
                    logger.info(f"ðŸš« Signal Rejected: {sig.instrument} {sig.direction} ({sig.grade}) - {rejection_reason} | Reasons: {reason_str}")
                    continue

                signal_time = getattr(sig, "signal_timestamp", None) or datetime.now(IST).replace(tzinfo=None)
                if getattr(signal_time, "tzinfo", None) is not None:
                    signal_time = signal_time.astimezone(IST).replace(tzinfo=None)
                allowed, limit_reason = self._can_trade_instrument(sig.instrument, signal_time)
                if not allowed:
                    self.log_signal_decision_once(
                        f"{sig.instrument}|{sig.timeframe}|{sig.direction}|{signal_time.isoformat()}|trade-limit",
                        f"Signal rejected: {sig.instrument} {sig.direction} - {limit_reason}",
                        "trade",
                    )
                    continue

                if not self._passes_live_trade_ready_gate(sig):
                    ts = getattr(sig, "signal_timestamp", None)
                    ts_key = ts.isoformat(timespec="seconds") if ts else ""
                    self.log_signal_decision_once(
                        f"{sig.instrument}|{sig.timeframe}|{sig.direction}|{ts_key}|live-ready",
                        f"Signal rejected: {sig.instrument} {sig.direction} {sig.timeframe} - live trade readiness gate blocked it",
                        "trade",
                    )
                    continue

                # Per-Instrument Safety (Cross-TF Complementary Rule)
                if not self._prepare_candidate_for_concurrency(sig):
                    ts = getattr(sig, "signal_timestamp", None)
                    ts_key = ts.isoformat(timespec="seconds") if ts else ""
                    self.log_signal_decision_once(
                        f"{sig.instrument}|{sig.timeframe}|{sig.direction}|{ts_key}|concurrency",
                        f"Signal rejected: {sig.instrument} {sig.direction} {sig.timeframe} - concurrency guard blocked it",
                        "trade",
                    )
                    continue

                # Every index receives an independent pass; only same-index overlap is restricted.
                can_take = self._passes_correlated_index_guard(sig, direction)

                if can_take:
                    # â”€â”€â”€ Manual Signal Gate â”€â”€â”€
                    if not self.auto_mode and not is_warmup:
                        self._close_superseded_session_entries([sig])
                        self._latest_trade_candidates[sig.instrument] = [sig]
                        setattr(sig, "accepted_by_gate", True)
                        self._mark_candidate_accepted_time(sig, datetime.now(IST))
                        self._remember_trade_candidates([sig])
                        accepted_candidates.append(sig)
                        self._record_accepted_entry(sig)
                        await self._publish_trade_payload_now()
                        if i == 0: # Only notify for the primary candidate
                            logger.info(f"📣 MANUAL SIGNAL: {sig.direction} {sig.instrument} @ {sig.price} (Auto Mode: Manual)")
                            self.log_event(f"🎯 Manual {sig.direction} Signal: {sig.instrument} @ {sig.price:.2f}", "trade")
                            self._notify(f"🎯 MANUAL SIGNAL: {sig.direction} {sig.instrument} @ {sig.price:.2f}. Auto-execution disabled.", "info")
                            
                            try:
                                nm = get_notification_manager(asyncio.get_running_loop())
                                
                                def make_executor(candidate_sig, is_ghost=False):
                                    def do_execute():
                                        exec_price, exec_stop, exec_target = self._execution_levels(candidate_sig)
                                        actual_spot = self.candles.get_latest_price(candidate_sig.instrument)
                                        
                                        def on_execution_result(executed_trade, accepted, candidate=candidate_sig):
                                            asyncio.run_coroutine_threadsafe(
                                                self._handle_broker_entry_result(candidate, executed_trade, accepted),
                                                asyncio.get_running_loop(),
                                            )

                                        trade = self.trades.open_trade(
                                            instrument=candidate_sig.instrument, timeframe=candidate_sig.timeframe, direction=candidate_sig.direction,
                                            price=exec_price, trailing_stop=exec_stop,
                                            lots=candidate_sig.lots, lot_size=candidate_sig.lot_size, grade=candidate_sig.grade,
                                            atm_strike=candidate_sig.atm_strike, option_type=candidate_sig.option_type,
                                            target=exec_target,
                                            rr_ratio=candidate_sig.rr,
                                            confidence=candidate_sig.confidence,
                                            instrument_multiplier=candidate_sig.multiplier,
                                            trading_symbol=candidate_sig.trading_symbol,
                                            symbol_token=candidate_sig.symbol_token,
                                            inst_type=candidate_sig.inst_type,
                                            exec_type="A",
                                            entry_spot=actual_spot,
                                            spot_stop=candidate_sig.stop,
                                            spot_target=candidate_sig.target,
                                            is_explosive_bypass=bool(getattr(candidate_sig, "is_explosive_bypass", False)),
                                            is_recovery=False,
                                            is_ghost=is_ghost,
                                            entry_time=datetime.now(IST),
                                            signal_time=getattr(candidate_sig, "signal_timestamp", None),
                                            on_execution_result=on_execution_result,
                                        )
                                        if trade:
                                            trade.is_live = (self.mode == "REAL")
                                            candidate_sig.status = "ORDER PENDING" if not is_ghost else "GHOST WATCH"
                                            asyncio.create_task(self._publish_trade_payload_now())
                                            
                                    return do_execute
                                
                                nm.send_trade_notification(
                                    signal_type="BUY" if sig.inst_type == "OPT" else ("BUY" if sig.direction == "LONG" else "SELL"),
                                    instrument=sig.instrument,
                                    entry_price=sig.price,
                                    target_price=sig.target,
                                    sl_price=sig.stop,
                                    rr_ratio=sig.rr,
                                    grade=sig.grade,
                                    confidence=getattr(sig, 'confidence', 0.0) * 100,
                                    timeframe=sig.timeframe,
                                    on_execute=make_executor(sig, is_ghost=False),
                                    on_watch=make_executor(sig, is_ghost=True)
                                )
                            except Exception as e:
                                logger.error(f"Failed to dispatch toast notification: {e}")
                                
                        continue # Don't take the trade in manual mode

                    # TradeManager.open_trade is the single broker execution path.
                    actual_spot = self.candles.get_latest_price(sig.instrument)
                    
                    sig_ts = getattr(sig, "signal_timestamp", None)
                    sig_ts_str = sig_ts.strftime('%H:%M:%S') if sig_ts else "--:--:--"
                    exec_ts_str = datetime.now(IST).strftime('%H:%M:%S')
                    logger.info(f"⏱️ Exact Execution Trigger: Signal={sig_ts_str} + 20s Buffer + Latency -> Entry={exec_ts_str}")
                    
                    # Resolve entries in the traded contract's price space.
                    exec_price, exec_stop, exec_target = self._execution_levels(sig)
                    spot_stop = sig.stop
                    spot_target = sig.target

                    execution_loop = asyncio.get_running_loop()

                    def on_execution_result(executed_trade, accepted, candidate=sig):
                        asyncio.run_coroutine_threadsafe(
                            self._handle_broker_entry_result(candidate, executed_trade, accepted),
                            execution_loop,
                        )

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
                        is_recovery=getattr(self, "is_warmup", False),
                        entry_time=datetime.now(IST),
                        signal_time=getattr(sig, "signal_timestamp", None),
                        on_execution_result=on_execution_result,
                    )

                    if trade:
                        trade.is_live = (self.mode == "REAL")
                        setattr(sig, "accepted_by_gate", False)
                        self._mark_candidate_accepted_time(sig, getattr(trade, "entry_time", None))
                        sig.status = "ORDER PENDING"
                        await self._publish_trade_payload_now()
                        open_count += 1
                        continue

                    if trade:
                        self._close_superseded_session_entries([sig])
                        self._latest_trade_candidates[sig.instrument] = [sig]
                        setattr(sig, "accepted_by_gate", True)
                        self._mark_candidate_accepted_time(sig, getattr(trade, "entry_time", None))
                        self._remember_trade_candidates([sig])
                        accepted_candidates.append(sig)
                        self._record_accepted_entry(sig, trade)
                        await self._publish_trade_payload_now()
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
        if not getattr(self, "chart_stream_enabled", True):
            return {}

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

            if df is None or len(df) == 0:
                continue
            if result is None:
                result = {}

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
            if len(ts_data) == 0:
                ts_line = []
            else:
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

            intel_history = self.intel_snapshots.history(
                instrument,
                tf,
                times=[c["time"] for c in sliced_candles],
                limit=len(sliced_candles),
            )
            res = {
                "candles": sliced_candles,
                "trailing_stop": sliced_ts,
                "markers": markers[-100:],
                "state": result.get("state", {}),
                "intel_history": intel_history,
            }
            if len(res["candles"]) > 0:
                logger.info(f"Sending chart data for {key}: first_ts={res['candles'][0]['time']}, last_ts={res['candles'][-1]['time']}")
            chart[tf] = res
            # Save to optimization cache (required for get_latest_results and
            # low-latency chart REST refreshes).
            setattr(
                self,
                f"chart_cache_{instrument}_{tf}",
                {"data": res, "simulation_id": self.simulation_id, "built_at": time.time()},
            )
        return chart

    def get_chart_snapshot(self, instrument: str = "NIFTY", timeframe: str = "5min") -> Dict[str, Any]:
        """Switch and return the active chart payload for dashboard tab changes."""
        valid_instruments = set(self.config.get("indices", {}).keys())
        instrument = (instrument or "NIFTY").upper()
        if instrument not in valid_instruments:
            instrument = "NIFTY"

        timeframe = timeframe if timeframe in {"1min", "5min", "15min"} else "5min"
        self.active_chart_instrument = instrument
        self.active_chart_tf = timeframe

        if not getattr(self, "chart_stream_enabled", True):
            return {
                "status": "ok",
                "instrument": instrument,
                "tf": timeframe,
                "chart": {},
                "chart_enabled": False,
                "simulation_id": self.simulation_id,
                "timestamp": datetime.now(IST).isoformat(),
            }

        cached = getattr(self, f"chart_cache_{instrument}_{timeframe}", None)
        cache_is_current = (
            isinstance(cached, dict)
            and cached.get("data")
            and int(cached.get("simulation_id") or 0) == int(getattr(self, "simulation_id", 0) or 0)
        )

        if cache_is_current:
            chart = {timeframe: cached["data"]}
        else:
            chart = self._build_chart_from_cache(instrument, force=True)
            if not chart.get(timeframe):
                cached = getattr(self, f"chart_cache_{instrument}_{timeframe}", None)
                if cached and cached.get("data"):
                    chart[timeframe] = cached["data"]

        return {
            "status": "ok",
            "instrument": instrument,
            "tf": timeframe,
            "chart": chart,
            "chart_enabled": True,
            "simulation_id": self.simulation_id,
            "timestamp": datetime.now(IST).isoformat(),
        }

    def get_latest_results(self, include_full_charts: bool = False) -> Dict:
        """Return the latest dashboard payload, optionally rehydrating full chart history."""
        if not include_full_charts or not self.latest_results:
            snapshot = dict(self.latest_results or {})
            if not getattr(self, "chart_stream_enabled", True) and snapshot.get("instruments"):
                instruments = {}
                for name, ui_data in snapshot.get("instruments", {}).items():
                    if isinstance(ui_data, dict):
                        hydrated_ui = dict(ui_data)
                        hydrated_ui["chart"] = {}
                        instruments[name] = hydrated_ui
                    else:
                        instruments[name] = ui_data
                snapshot["instruments"] = instruments
            return snapshot

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

    def generate_daily_signal_report(self, date_str: Optional[str] = None) -> Dict[str, Any]:
        """Build and save a daily missed/rejected signal report from SQLite and runtime logs."""
        date_str = str(date_str or datetime.now(IST).date().isoformat())
        report_dir = Path("data_store") / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)

        decisions = list(db.load_signal_decisions(date_str))
        session_rows = list(db.load_session_signals(date_str))
        seen_messages = {
            str(item.get("message") or "").strip()
            for item in decisions
            if str(item.get("message") or "").strip()
        }
        match_terms = (
            "signal rejected",
            "entry blocked",
            "signal blocked",
            "discarded",
            "repainted during",
            "repaint guard",
            "signal waiting",
            "signal ignored",
            "concurrency guard: blocked",
            "live gate: blocked",
            "ghost guard",
        )

        def add_log_decision(raw_message: str, timestamp: str, source: str) -> None:
            message = str(raw_message or "").strip()
            lowered = message.lower()
            if not message or not any(term in lowered for term in match_terms):
                return
            if message in seen_messages:
                return
            seen_messages.add(message)
            instrument = next(
                (
                    name
                    for name in ("MIDCPNIFTY", "BANKNIFTY", "SENSEX", "NIFTY")
                    if re.search(rf"\b{name}\b", message, flags=re.IGNORECASE)
                ),
                "",
            )
            tf_match = re.search(r"\b(1min|5min|15min)\b", message, flags=re.IGNORECASE)
            direction_match = re.search(r"\b(LONG|SHORT|BUY|SELL)\b", message, flags=re.IGNORECASE)
            decisions.append(
                {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{date_str}|{source}|{message}")),
                    "date": date_str,
                    "timestamp": timestamp,
                    "instrument": instrument,
                    "timeframe": tf_match.group(1).lower() if tf_match else "",
                    "direction": direction_match.group(1).upper() if direction_match else "",
                    "status": "REJECTED" if "reject" in lowered or "blocked" in lowered else "MISSED",
                    "reason": message.rsplit(" - ", 1)[-1] if " - " in message else message,
                    "message": message,
                    "source": source,
                }
            )

        for item in list(getattr(self, "activity_log", []) or []):
            raw = re.sub(r"^[^\w\[]+\s*", "", str(item.get("msg") or ""))
            add_log_decision(
                raw,
                f"{date_str}T{str(item.get('time') or '00:00:00')}",
                "activity_log",
            )

        target_day = datetime.fromisoformat(date_str).date()
        for log_path in sorted(Path(".").glob("runtime*.err.log")):
            try:
                if datetime.fromtimestamp(log_path.stat().st_mtime, tz=IST).date() != target_day:
                    continue
                with log_path.open("r", encoding="utf-8", errors="ignore") as handle:
                    handle.seek(0, 2)
                    size = handle.tell()
                    handle.seek(max(0, size - 4_000_000))
                    for line in handle:
                        lowered = line.lower()
                        if not any(term in lowered for term in match_terms):
                            continue
                        time_match = re.search(r"\b(\d{2}:\d{2}:\d{2})\b", line)
                        message = line.split("|", 2)[-1].strip()
                        add_log_decision(
                            message,
                            f"{date_str}T{time_match.group(1) if time_match else '00:00:00'}",
                            f"log:{log_path.name}",
                        )
            except Exception as exc:
                logger.debug(f"Signal report log scan skipped for {log_path}: {exc}")

        decisions.sort(key=lambda item: str(item.get("timestamp") or ""))
        reason_counts: Dict[str, int] = {}
        instrument_counts: Dict[str, int] = {}
        status_counts: Dict[str, int] = {}
        for item in decisions:
            reason = str(item.get("reason") or "Unknown")
            instrument = str(item.get("instrument") or "UNKNOWN")
            status = str(item.get("status") or "REJECTED")
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            instrument_counts[instrument] = instrument_counts.get(instrument, 0) + 1
            status_counts[status] = status_counts.get(status, 0) + 1

        accepted_rows = [
            row
            for row in session_rows
            if str(row.get("action") or "ENTRY").upper() == "ENTRY"
            and bool(row.get("accepted_by_gate"))
        ]
        exit_rows = [row for row in session_rows if str(row.get("action") or "").upper() == "EXIT"]
        report = {
            "date": date_str,
            "generated_at": datetime.now(IST).isoformat(),
            "summary": {
                "missed_or_rejected": len(decisions),
                "accepted_signals": len(accepted_rows),
                "exits": len(exit_rows),
                "by_status": dict(sorted(status_counts.items())),
                "by_instrument": dict(sorted(instrument_counts.items())),
                "top_reasons": dict(
                    sorted(reason_counts.items(), key=lambda item: item[1], reverse=True)[:25]
                ),
            },
            "decisions": decisions,
            "accepted_signals": accepted_rows,
            "exits": exit_rows,
        }

        json_path = report_dir / f"missed_rejected_signals_{date_str}.json"
        csv_path = report_dir / f"missed_rejected_signals_{date_str}.csv"
        json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        with csv_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "timestamp",
                    "instrument",
                    "timeframe",
                    "direction",
                    "status",
                    "reason",
                    "source",
                    "message",
                ],
            )
            writer.writeheader()
            for item in decisions:
                writer.writerow({key: item.get(key, "") for key in writer.fieldnames})

        report["files"] = {"json": str(json_path), "csv": str(csv_path)}
        return report

    def _close_and_record(self, tid: str, exit_price: float, reason: str, exit_time: Optional[datetime] = None):
        """Wraps trade closure to track over-trading metrics"""
        self._reset_daily_counters()
        trade = self.trades.open_trades.get(tid)
        if not trade: return
        if str(getattr(trade, "execution_status", "") or "").upper() == "RECOVERY_REQUIRED":
            logger.critical(f"Skipped strategy close for recovery-required trade {tid}; broker reconciliation required.")
            return

        instrument = str(trade.instrument or "").split()[0]

        # ─── Manual Exit Gate ───
        if not self.auto_mode and reason not in ("STALE_SESSION_END",):
            try:
                nm = get_notification_manager(asyncio.get_running_loop())
                def make_exit_executor(t_id, e_price, e_reason, e_time):
                    def do_exit():
                        self._close_and_record_internal(t_id, e_price, e_reason, e_time)
                    return do_exit
                
                profit = (exit_price - trade.entry_price) if trade.direction == "LONG" else (trade.entry_price - exit_price)
                profit = profit * trade.quantity()
                
                nm.send_trade_notification(
                    signal_type="EXIT",
                    instrument=trade.instrument,
                    entry_price=exit_price,
                    target_price=trade.target,
                    sl_price=trade.trailing_stop,
                    rr_ratio=trade.rr_ratio,
                    profit=profit,
                    exit_reason=reason,
                    on_execute=make_exit_executor(tid, exit_price, reason, exit_time)
                )
                logger.info(f"📣 MANUAL EXIT SIGNAL: {trade.instrument} @ {exit_price:.2f} (Reason: {reason})")
            except Exception as e:
                logger.error(f"Failed to dispatch manual exit toast: {e}")
            return
            
        self._close_and_record_internal(tid, exit_price, reason, exit_time)

    def _close_and_record_internal(self, tid: str, exit_price: float, reason: str, exit_time: Optional[datetime] = None):
        """Internal closure after manual gate"""
        self._reset_daily_counters()
        trade = self.trades.open_trades.get(tid)
        if not trade: return
        instrument = str(trade.instrument or "").split()[0]
        
        # Close the trade
        self.trades.close_trade(tid, exit_price, reason, exit_time=exit_time)

        # Track outcome
        closed_trade = next((t for t in self.trades.closed_trades if t.id == tid), None)
        if closed_trade:
            # Track losses (negative PnL)
            if closed_trade.pnl < 0:
                self._losses_today[instrument] = self._losses_today.get(instrument, 0) + 1
            else:
                # Reset consecutive losses on a win
                self._losses_today[instrument] = 0

            # Record exit time for cooldown
            self._last_exit_time[instrument] = exit_time if exit_time else datetime.now(IST).replace(tzinfo=None)
            self._trades_today.setdefault(instrument, 0)
            logger.info(f"ðŸ“Š {instrument} Tracker: Daily Trades={self._trades_today[instrument]}, Consecutive Losses={self._losses_today[instrument]}")

    def _update_active_trades(self):
        self._reset_daily_counters()
        for tid, trade in list(self.trades.open_trades.items()):
            if str(getattr(trade, "execution_status", "") or "").upper() == "RECOVERY_REQUIRED":
                continue
            price = self.candles.get_latest_price(trade.instrument)
            if price is None or price <= 0: continue
            real_premium = None
            real_fut_price = None
            is_fut_trade = getattr(trade, "inst_type", "FUT") == "FUT"
            if getattr(trade, "inst_type", "FUT") == "OPT":
                real_premium = self._get_live_premium_cached(trade)
            elif is_fut_trade:
                real_fut_price = self._get_live_fut_ltp_cached(trade)
            
            # Update current_price
            trade_price = float(price)
            if is_fut_trade:
                if real_fut_price and real_fut_price > 0:
                    trade_price = float(real_fut_price)
                elif getattr(trade, "entry_spot", 0.0) > 0:
                    trade_price = self._fut_price_from_spot_level(trade, float(price))
                trade.current_price = trade_price
            elif getattr(trade, "inst_type", "FUT") == "OPT":
                if real_premium and real_premium > 0:
                    trade.current_price = float(real_premium)
                elif getattr(trade, "entry_spot", 0.0) > 0:
                    spot_change = (float(price) - trade.entry_spot) if trade.direction == "LONG" else (trade.entry_spot - float(price))
                    premium_move = spot_change * getattr(trade, "instrument_multiplier", 0.5)
                    trade.current_price = max(0.05, trade.entry_price + premium_move)
                else:
                    trade.current_price = trade.entry_price

                trade.current_price = self._sanitize_option_mark(
                    trade,
                    trade.current_price,
                    float(price),
                    "active trade mark",
                )

            opt_val = trade.current_price if getattr(trade, "inst_type", "FUT") == "OPT" else trade_price

            # Define conversion helpers for this trade
            def prem_to_spot(prem_val):
                if getattr(trade, "inst_type", "FUT") != "OPT":
                    return prem_val
                mult = abs(getattr(trade, "instrument_multiplier", 0.5)) or 0.5
                spot_dist = (trade.entry_price - prem_val) / mult
                if trade.direction == "LONG":
                    return trade.entry_spot - spot_dist
                else:
                    return trade.entry_spot + spot_dist

            def spot_to_prem(spot_val):
                if getattr(trade, "inst_type", "FUT") != "OPT":
                    return spot_val
                mult = getattr(trade, "instrument_multiplier", 0.5)
                spot_change = spot_val - trade.entry_spot
                return trade.entry_price + (spot_change * mult)

            regime = self.latest_regimes.get(trade.instrument, "UNKNOWN")
            latest_time = (
                datetime.now(IST).replace(tzinfo=None)
                if getattr(self, "mode", "") != "HISTORICAL"
                else self.candles.get_max_timestamp()
            )

            # ── Intelligence-Based Early Exit (Patch) ──
            from config.settings import get_settings
            settings = get_settings()
            if getattr(settings, "ut_intel_early_exit", False):
                intel_result = getattr(self, "_intel_cache", {}).get(trade.instrument)
                if intel_result:
                    reversal_reason = self._intel_reversal_reason(trade.direction, intel_result)
                    if reversal_reason:
                        defer_exit, defer_text = self._should_defer_option_intel_exit(
                            trade,
                            opt_val,
                            latest_time,
                            reversal_reason,
                            regime=regime,
                        )
                        if defer_exit:
                            self.log_event(
                                f"Deferred intelligence exit for {trade.instrument} {trade.timeframe}: {reversal_reason}; {defer_text}.",
                                "info",
                            )
                        else:
                            logger.info(f"🚨 OI/PCR Reversal Exit for {trade.instrument} {trade.direction}: {reversal_reason}")
                            self.trades.close_trade(tid, opt_val, "OI_PCR_REVERSAL")
                            continue
                    agg = intel_result.get("aggregate", {})
                    score = agg.get("score", 0.0)

                    if trade.direction == "LONG" and score < -40.0:
                        reason = f"Intelligence flip score {score:.1f}"
                        defer_exit, defer_text = self._should_defer_option_intel_exit(
                            trade,
                            opt_val,
                            latest_time,
                            reason,
                            regime=regime,
                        )
                        if defer_exit:
                            self.log_event(
                                f"Deferred intelligence exit for {trade.instrument} {trade.timeframe}: {reason}; {defer_text}.",
                                "info",
                            )
                        else:
                            logger.info(f"🚨 Intelligence Early Exit for {trade.instrument} LONG: Score={score}")
                            self.trades.close_trade(tid, opt_val, "INTEL_FLIP")
                            continue
                    elif trade.direction == "SHORT" and score > 40.0:
                        reason = f"Intelligence flip score {score:.1f}"
                        defer_exit, defer_text = self._should_defer_option_intel_exit(
                            trade,
                            opt_val,
                            latest_time,
                            reason,
                            regime=regime,
                        )
                        if defer_exit:
                            self.log_event(
                                f"Deferred intelligence exit for {trade.instrument} {trade.timeframe}: {reason}; {defer_text}.",
                                "info",
                            )
                        else:
                            logger.info(f"🚨 Intelligence Early Exit for {trade.instrument} SHORT: Score={score}")
                            self.trades.close_trade(tid, opt_val, "INTEL_FLIP")
                            continue

            # ── Smart Trailing Patch ──
            if getattr(settings, "ut_smart_trailing", False):
                key_tf = f"{trade.instrument}_{trade.timeframe}"
            else:
                key_tf = f"{trade.instrument}_1min"

            engine_tf = self.mtf.engines.get(key_tf)
            latest_time = self.candles.get_max_timestamp()
            if not engine_tf:
                logger.warning(f"Active trade guard running without UT engine state for {trade.instrument}; trailing skipped, P&L lock still active.")

            state_tf = engine_tf.get_state(key_tf) if engine_tf else None
            raw_ts = state_tf.trailing_stop if state_tf else 0.0

            # Initialize new_stop as spot index level
            if getattr(trade, "inst_type", "FUT") == "OPT":
                new_stop = prem_to_spot(trade.current_stop)
            else:
                new_stop = trade.current_stop

            # ── Adaptive Logic based on Market Regime ──
            # 1. TRENDING (Normal ATR Trailing)
            if regime in ["TRENDING", "STRONG_TREND"]:
                if raw_ts > 0:
                    trade_raw_ts = self._fut_price_from_spot_level(trade, raw_ts) if is_fut_trade else raw_ts
                    # Only move stop in favorable direction
                    if trade.direction == "LONG":
                        new_stop = max(new_stop, trade_raw_ts)
                    else:
                        new_stop = min(new_stop, trade_raw_ts)

            # 2. VOLATILE / REVERSAL / CHOPPY (Tight Trailing)
            elif regime in ["VOLATILE", "CHOPPY", "MEAN_REVERTING", "UNKNOWN"]:
                df_1m = self.candles.get_candles(trade.instrument, "1min")
                if df_1m is not None and len(df_1m) > 2:
                    last_low = df_1m['low'].iloc[-2]
                    last_high = df_1m['high'].iloc[-2]

                    if trade.direction == "LONG":
                        # Trail by last candle's low (Tight)
                        trade_last_low = self._fut_price_from_spot_level(trade, last_low) if is_fut_trade else last_low
                        new_stop = max(new_stop, trade_last_low)
                    else:
                        # Trail by last candle's high (Tight)
                        trade_last_high = self._fut_price_from_spot_level(trade, last_high) if is_fut_trade else last_high
                        new_stop = min(new_stop, trade_last_high)

            # 3. PROFIT PROTECTOR (Lock-in at 1:1 RR)
            # Use entry_spot for options so we compare Spot with Spot
            ref_entry = trade.entry_spot if trade.inst_type == "OPT" else trade.entry_price
            risk_dist = abs(ref_entry - prem_to_spot(trade.trailing_stop)) if trade.inst_type == "OPT" else abs(ref_entry - trade.trailing_stop)
            ref_current = price if trade.inst_type == "OPT" else trade_price
            current_profit_pts = (ref_current - ref_entry) if trade.direction == "LONG" else (ref_entry - ref_current)

            if current_profit_pts > risk_dist:
                buffer = ref_entry * 0.0005 # Tighten buffer (0.05%)
                old_stop = new_stop
                if trade.direction == "LONG":
                    new_stop = max(new_stop, ref_entry + buffer)
                else:
                    new_stop = min(new_stop, ref_entry - buffer)

                if new_stop != old_stop:
                    self.log_event(f"🛡️ Profit Protector: Locking BE for {trade.instrument}", "trade")

            # ── 4. TARGET TRAILING & RUNNER MODE (T1 -> T2 -> T3) ──
            if trade.target > 0 or getattr(trade, 'runner_mode', False):
                rr_settings = get_settings()
                runner_unlock_ratio = min(1.0, max(0.50, float(getattr(rr_settings, "dynamic_rr_runner_unlock_ratio", 0.95) or 0.95)))
                runner_lock_pct = min(0.98, max(0.25, float(getattr(rr_settings, "dynamic_rr_runner_lock_pct", 0.90) or 0.90)))
                # Use entry_spot for options so we compare Spot with Spot
                ref_entry = trade.entry_spot if trade.inst_type == "OPT" else trade.entry_price

                # Check if T1 was already hit and we are in runner mode
                is_runner = getattr(trade, 'runner_mode', False)

                if not is_runner and trade.target > 0:
                    # Logic to activate Runner Mode at T1 (or 90% of T1)
                    if trade.inst_type == "OPT":
                        target_distance = abs(trade.target - trade.entry_price)
                        threshold_t1 = target_distance * runner_unlock_ratio
                        current_move = abs(trade.current_price - trade.entry_price)
                        is_correct_direction = trade.current_price > trade.entry_price
                    else:
                        target_distance = abs(trade.target - ref_entry)
                        threshold_t1 = target_distance * runner_unlock_ratio
                        target_current = price if trade.inst_type == "OPT" else trade_price
                        current_move = abs(target_current - ref_entry)
                        is_correct_direction = (trade.direction == "LONG" and target_current > ref_entry) or                                              (trade.direction == "SHORT" and target_current < ref_entry)

                    if is_correct_direction and current_move >= threshold_t1:
                        trade.runner_mode = True
                        trade.target = 0 # Remove hard target to let it run to T2/T3
                        lock_label = f"{runner_lock_pct * 100:.0f}%"
                        self.log_event(f"🚀 TARGET 1 HIT for {trade.instrument}. Entering Institutional Runner Mode ({lock_label} Gain Lock).", "trade")
                        logger.success(f"🔥 Runner Mode Active: {trade.instrument}. Hard target removed, locking {lock_label} gains.")

                # ── Runner Mode Trailing (90% Gain Lock) ──
                if getattr(trade, 'runner_mode', False):
                    # Calculate current gain in points
                    if trade.inst_type == "OPT":
                        current_gain_pts = max(0, (trade.current_price - trade.entry_price))
                    else:
                        current_gain_pts = max(0, abs(trade_price - trade.entry_price))

                    # Lock 90% of current gain points
                    locked_gain_pts = current_gain_pts * runner_lock_pct

                    if trade.inst_type == "OPT":
                        multiplier = abs(trade.instrument_multiplier) or 0.5
                        # Convert to spot: Spot = Entry Spot + (Gain / Multiplier) if Call, else Entry Spot - (Gain / Multiplier)
                        spot_gain = locked_gain_pts / multiplier
                        new_runner_stop = (trade.entry_spot + spot_gain) if trade.direction == "LONG" else (trade.entry_spot - spot_gain)
                    else:
                        new_runner_stop = (trade.entry_price + locked_gain_pts) if trade.direction == "LONG" else (trade.entry_price - locked_gain_pts)

                    # Update stop if the new locked gain is higher (trailing)
                    if trade.direction == "LONG":
                        new_stop = max(new_stop, new_runner_stop)
                    else:
                        new_stop = min(new_stop, new_runner_stop)

            # ── 5. SMART PROFIT LOCK & REVERSAL GUARD ──
            # Calculate absolute PnL in Rupees for institutional exit rules
            current_pnl_rs = 0.0
            entry_val = trade.entry_price
            qty = getattr(trade, "quantity", getattr(trade, "qty", 0))
            inst_mult = getattr(trade, 'instrument_multiplier', 1.0)
            if trade.inst_type == "OPT":
                current_pnl_rs = (trade.current_price - entry_val) * qty
                current_gain_pct = (current_pnl_rs / (entry_val * qty)) * 100.0 if entry_val > 0 and qty > 0 else 0
            else:
                current_pnl_rs = (trade_price - entry_val) * qty if trade.direction == "LONG" else (entry_val - trade_price) * qty
                current_pnl_rs *= inst_mult
                current_gain_pct = (current_pnl_rs / (entry_val * qty * inst_mult)) * 100.0 if entry_val > 0 and qty > 0 and inst_mult > 0 else 0

            candle_extreme_price = self._trade_candle_extreme_price(trade, latest_time)
            if candle_extreme_price > 0:
                if trade.inst_type == "OPT":
                    # _trade_candle_extreme_price already returns premium space for options.
                    prem_extreme = float(candle_extreme_price)
                    prem_extreme = self._sanitize_option_mark(
                        trade,
                        prem_extreme,
                        float(price),
                        "intrabar peak reconstruction",
                    )
                    candle_extreme_pnl = (prem_extreme - entry_val) * qty
                else:
                    candle_extreme_pnl = (
                        (candle_extreme_price - entry_val) * qty
                        if trade.direction == "LONG"
                        else (entry_val - candle_extreme_price) * qty
                    )
                    candle_extreme_pnl *= inst_mult
            else:
                candle_extreme_pnl = 0.0

            # Update peak PnL from sampled LTP plus intrabar candle extremes.
            inst_mult_peak = getattr(trade, 'instrument_multiplier', 1.0) if getattr(trade, 'inst_type', 'FUT') != 'OPT' else 1.0
            trade.peak_pnl = max(getattr(trade, 'peak_pnl', 0.0), current_pnl_rs, candle_extreme_pnl)
            peak_gain_pct = (trade.peak_pnl / (entry_val * qty * inst_mult_peak)) * 100.0 if entry_val > 0 and qty > 0 and inst_mult_peak > 0 else 0

            lock_dir = "LONG" if trade.inst_type == "OPT" else trade.direction
            lock_stop, lock_reason, lock_ratio = self._profit_lock_floor(entry_val, lock_dir, int(qty or 0), trade.peak_pnl, inst_mult=inst_mult_peak)
            if lock_reason == "MAJOR_WIN_GUARD" and trade.inst_type == "FUT":
                should_exit_major, guard_context = self._should_exit_major_win_guard(
                    trade,
                    float(trade_price),
                    trade.peak_pnl,
                    current_pnl_rs,
                    latest_time,
                )
                if not should_exit_major:
                    lock_stop = 0.0
                    lock_reason = ""
                    self.log_event(
                        f"Deferred major-win lock for {trade.instrument} {trade.timeframe}: {guard_context}.",
                        "info",
                    )
            
            lock_stop_spot = prem_to_spot(lock_stop) if lock_stop > 0 else 0.0
            if lock_stop_spot > 0:
                if trade.direction == "LONG":
                    new_stop = max(new_stop, lock_stop_spot)
                else:
                    new_stop = min(new_stop, lock_stop_spot)

            if trade.inst_type == "OPT":
                proposed_prem_stop = spot_to_prem(new_stop)
                adjusted_prem_stop, breathing, clamped = self._apply_option_stop_breathing_room(
                    trade,
                    proposed_prem_stop,
                    latest_time=latest_time,
                    regime=regime,
                )
                if clamped:
                    self.log_event(
                        f"Option breathing room kept live {trade.instrument} {trade.timeframe} stop at {adjusted_prem_stop:.2f} "
                        f"(floor {breathing:.2f}) instead of {proposed_prem_stop:.2f}.",
                        "info",
                    )
                    new_stop = prem_to_spot(adjusted_prem_stop)

            if lock_reason:
                lock_hit_price = price if trade.inst_type == "OPT" else trade_price
                lock_hit = (trade.direction == "LONG" and lock_hit_price <= new_stop) or (trade.direction == "SHORT" and lock_hit_price >= new_stop)
                if lock_hit:
                    exit_val = spot_to_prem(new_stop) if trade.inst_type == "OPT" else new_stop
                    if trade.inst_type == "OPT":
                        locked_pnl = (exit_val - entry_val) * qty
                    else:
                        locked_pnl = (exit_val - entry_val) * qty if trade.direction == "LONG" else (entry_val - exit_val) * qty
                    logger.warning(
                        f"Profit Lock Stop Hit for {trade.instrument}: Peak Rs.{trade.peak_pnl:.0f}, "
                        f"Locked {lock_ratio:.0%} Rs.{locked_pnl:.0f}"
                    )
                    self._close_and_record(tid, exit_val, lock_reason, exit_time=latest_time)
                    continue

            # Rule 1: High-Profit Protection (Peak >= Rs.1000)
            if 1000.0 <= trade.peak_pnl < 3000.0:
                # If profit drops below 65% of peak OR falls below Rs.450 minimum floor
                if current_pnl_rs < (trade.peak_pnl * 0.65) or current_pnl_rs < 450.0:
                    logger.warning(f"🚨 Smart Profit Lock Hit for {trade.instrument}: Peak ₹{trade.peak_pnl:.0f}, Current ₹{current_pnl_rs:.0f}")
                    self._close_and_record(tid, lock_stop if lock_stop > 0 else opt_val, "SMART_PROFIT_LOCK", exit_time=latest_time)
                    self.log_event(f"💰 Profit Locked: {trade.instrument} (Retention Guard Hit @ ₹{current_pnl_rs:.0f})", "trade")
                    continue

            # Rule 2: Low-Gain Protection (If gain was > 10% but reversed)
            if peak_gain_pct >= 10.0:
                # If gain drops below 40% of peak or falls near BE (+2% floor)
                if current_gain_pct < (peak_gain_pct * 0.40) or current_gain_pct < 2.0:
                    logger.warning(f"🚨 Low-Gain Protection Hit for {trade.instrument}: Peak {peak_gain_pct:.1f}%, Current {current_gain_pct:.1f}%")
                    self._close_and_record(tid, opt_val, "LOW_GAIN_PROTECT", exit_time=latest_time)
                    continue

            # Rule 3: Major Win Protection (Peak >= Rs.3000)
            if trade.peak_pnl >= 3000.0:
                if current_pnl_rs < (trade.peak_pnl * 0.75):
                    should_exit, guard_context = (
                        self._should_exit_major_win_guard(
                            trade,
                            float(trade_price),
                            trade.peak_pnl,
                            current_pnl_rs,
                            latest_time,
                        )
                        if str(getattr(trade, "inst_type", "FUT") or "FUT").upper() == "FUT"
                        else (True, "option major-win guard")
                    )
                    if should_exit:
                        logger.warning(
                            f"Major Win Guard for {trade.instrument}: Peak Rs.{trade.peak_pnl:.0f} "
                            f"-> Exit @ Rs.{current_pnl_rs:.0f}; {guard_context}"
                        )
                        self._close_and_record(
                            tid,
                            lock_stop if lock_stop > 0 else opt_val,
                            "MAJOR_WIN_GUARD",
                            exit_time=latest_time,
                        )
                        continue
                    self.log_event(
                        f"Deferred major-win exit for {trade.instrument} {trade.timeframe}: {guard_context}.",
                        "info",
                    )

            # Rule 4: Risk- and volatility-normalized stagnation protection.
            should_stagnate, stagnation_context = self._stagnation_exit_decision(
                trade,
                current_pnl_rs,
                trade.peak_pnl,
                latest_time,
                regime=regime,
            )
            if should_stagnate:
                logger.info(f"Stagnation Exit: {trade.instrument}; {stagnation_context}")
                self._close_and_record(tid, opt_val, "STAGNATION_EXIT", exit_time=latest_time)
                continue

            if False and trade.entry_time and current_pnl_rs >= 300.0:
                time_elapsed = (latest_time - trade.entry_time.replace(tzinfo=None)).total_seconds() / 60.0 if latest_time else 0
                # Exit if held > 20m and current profit is < 85% of peak (stalled)
                stagnation_minutes = max(
                    1.0,
                    float(getattr(settings, "trade_stagnation_minutes", 20.0) or 20.0),
                )
                if time_elapsed >= stagnation_minutes and current_pnl_rs < (trade.peak_pnl * 0.85):
                    logger.info(f"⏳ Stagnation Exit: {trade.instrument} held {time_elapsed:.0f}m, PnL ₹{current_pnl_rs:.0f}")
                    self._close_and_record(tid, opt_val, "STAGNATION_EXIT", exit_time=latest_time)
                    continue

            latest_time = self.candles.get_max_timestamp()

            # ── Live Trade Preservation Patch ──
            if getattr(trade, 'is_live', False) and self.mode == "HISTORICAL":
                triggered = False
                reason = ""
                if trade.inst_type == "OPT":
                    if trade.current_price <= trade.current_stop:
                        triggered = True
                        reason = "TRAILING_STOP"
                    elif trade.target > 0 and trade.current_price >= trade.target:
                        triggered = True
                        reason = "TARGET_HIT"
                else:
                    stop_check_price = trade_price
                    if trade.direction == "LONG":
                        if stop_check_price <= new_stop:
                            triggered = True
                            reason = "TRAILING_STOP"
                        elif stop_check_price >= trade.target and trade.target > 0:
                            triggered = True
                            reason = "TARGET_HIT"
                    else:
                        if stop_check_price >= new_stop:
                            triggered = True
                            reason = "TRAILING_STOP"
                        elif stop_check_price <= trade.target and trade.target > 0:
                            triggered = True
                            reason = "TARGET_HIT"

                if triggered:
                    msg = f"🚨 ALERT: Live Trade {trade.instrument} requires EXIT ({reason}) at {trade.current_price:.2f}!"
                    logger.warning(msg)
                    if self.on_notification:
                        self.on_notification(msg, "warning")

                # Update price and stop in memory so UI shows it, but do NOT close it!
                if trade.inst_type == "OPT":
                    pass
                else:
                    trade.current_stop = new_stop
                continue

            self.trades.update_trade(tid, price, new_stop, current_time=latest_time, real_premium=real_premium, real_fut_price=real_fut_price)
            if tid not in self.trades.open_trades:
                # Trade was closed by update_trade (SL or Target)
                # Find it in closed_trades to record metrics
                closed_trade = next((t for t in self.trades.closed_trades if t.id == tid), None)
                if closed_trade:
                    if closed_trade.pnl < 0:
                        self._losses_today[trade.instrument] = self._losses_today.get(trade.instrument, 0) + 1
                    else:
                        self._losses_today[trade.instrument] = 0
                    self._last_exit_time[trade.instrument] = latest_time if latest_time else datetime.now(IST).replace(tzinfo=None)
                    self._trades_today.setdefault(trade.instrument, 0)
                    logger.info(f"📊 {trade.instrument} Tracker (SL/TP): Daily Trades={self._trades_today[trade.instrument]}, Consecutive Losses={self._losses_today[trade.instrument]}")

                self.log_event(f"🛑 Trade Closed: {trade.instrument} Stop Hit @ {price:.2f}", "trade")
                self._notify(f"🛑 STOP HIT {trade.direction} {trade.instrument} @ {price:.2f}", "sell")

    def _notify(self, message, msg_type="info"):
        if self.on_notification:
            try: self.on_notification(message, msg_type)
            except Exception as e: logger.debug(f"Ignored exception: {e}")

    def stop(self):
        self._save_warm_memory(force=True)
        try:
            self.generate_daily_signal_report()
        except Exception as exc:
            logger.debug(f"Final daily signal report save skipped: {exc}")
        self.is_running = False
        logger.info("Scanner stopped")

    def update_fyers_token(self, auth_code: str) -> Dict[str, Any]:
        """Update Fyers token and restart history sync if successful"""
        result = self.data.update_fyers_token_from_auth_code(auth_code)
        if result.get("status") == "ok":
            # Force a full history fetch to use the new Fyers token immediately
            self.log_event("âœ… Fyers Token Updated: Restarting Data Sync", "success")
            self.queue_full_recalculation("fyers-token-update")
        return result
