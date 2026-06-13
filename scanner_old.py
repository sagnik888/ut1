"""
Scanner ΓÇö Continuous Multi-Instrument Multi-Timeframe Scanner
ΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉ
v3.0 ΓÇö UT1 Intelligent Scanning Engine
"""

import asyncio
import time
import math
import uuid
import pandas as pd
from datetime import datetime, time as dtime, timedelta
from typing import Dict, Optional, Callable, List, Tuple
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


from dataclasses import dataclass, field


@dataclass
class TradeCandidate:
    instrument: str
    direction: str
    price: float
    stop: float
    target: float
    lots: int
    lot_size: int
    grade: str
    confidence: float
    timeframe: str
    inst_type: str
    option_type: str
    atm_strike: float
    multiplier: float
    trading_symbol: str
    symbol_token: str
    rr: float = 1.5
    score: float = 0.0


class Scanner:
    """Continuous scanning engine ΓÇö optimized, best-signal selection"""

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
        self.perf = performance
        self.config = instruments_config
        self.on_update = on_update
        self.on_notification = on_notification

        self.is_running = False
        self.scan_interval = 1.0
        self.last_scan_time = None
        self.scan_count = 0
        self.mode = trading_mode # Master Mode Control
        self._state_file = "data_store/trade_state.json"
        
        # Risk settings (updated via configure)
        self.futures_sl_pct = 0.30
        self.options_sl_pct = 15.0
        
        # Load state on initialize from global settings
        settings = get_settings()
        self.user_lots: Dict[str, int] = {"NIFTY": 1, "BANKNIFTY": 1, "SENSEX": 1}
        self.capital_fut = settings.capital_fut
        self.capital_opt = settings.capital_opt
        self.capital_total = getattr(settings, "capital_total", 500000.0)
        self.risk_fut_pct = settings.risk_fut_pct
        self.risk_opt_pct = settings.risk_opt_pct
        self.futures_sl_pct = settings.futures_sl_pct
        self.options_sl_pct = settings.options_sl_pct
        self.backtest_days = settings.default_backtest_days
        self.auto_mode = False
        self.inst_pref = "AUTO" # Default to Intelligent Auto

        self.intel_memory = IntelligenceMemory()
        self.latest_results: Dict = {}
        self._last_signal_time: Dict[str, datetime] = {}
        self._last_data_fetch: Dict[str, float] = {}
        self._api_semaphore = asyncio.Semaphore(1) # Background history semaphore (serialized to prevent rate limits)
        self._ltp_semaphore = asyncio.Semaphore(5) # High-priority price semaphore (LTP)
        self._data_fetch_interval = 10
        self.max_daily_loss_pct = 3.0
        self._daily_loss_breached = False
        self.latest_regimes: Dict[str, str] = {} # For adaptive trailing
        self.active_indices = settings.active_indices
        self.system_power = "ON"
        self.is_calculating = False
        self.is_warmup = False
        self.simulation_id = int(time.time())
        self._daily_reset_done = False

        # ΓòÉΓòÉΓòÉ CACHED PROCESS RESULTS (avoid double-processing) ΓòÉΓòÉΓòÉ
        self._cached_results: Dict[str, Dict] = {}
        
        # ΓòÉΓòÉΓòÉ SYSTEM ACTIVITY LOG ΓòÉΓòÉΓòÉ
        self.activity_log: List[Dict] = []
        self._max_log_size = 50
        self.log_event("UT1 System Initialized", "system")
        
        # ΓòÉΓòÉΓòÉ PRE-MARKET ANALYSIS (AngelOne Master Integration) ΓòÉΓòÉΓòÉ
        from engine.expiry_manager import expiry_manager
        self.expiry = expiry_manager
        self.market_info = self.expiry.pre_market_check()

    def get_broadcast_config(self):
        """Standardized config payload for UI broadcasting"""
        return {
            "capital_total": self.capital_total,
            "capital_fut": self.capital_fut,
            "risk_fut_pct": self.risk_fut_pct,
            "capital_opt": self.capital_opt,
            "risk_opt_pct": self.risk_opt_pct,
            "lots": self.user_lots,
            "fut_sl": self.futures_sl_pct,
            "opt_sl": self.options_sl_pct,
            "backtest_days": self.backtest_days,
            "auto_mode": self.auto_mode,
            "inst_pref": self.inst_pref,
            "active_indices": self.active_indices,
            "strike_selection": getattr(get_settings(), "option_strike_selection", "BOTH")
        }

    def configure(self, capital_total=None, capital_fut=None, capital_opt=None, risk_fut_pct=None, risk_opt_pct=None, 
                  lots=None, mode=None, reset=False, futures_sl_pct=None, options_sl_pct=None, 
                  backtest_days=None, auto_mode=None, inst_pref=None, strike_selection=None, active_indices=None):
        from config.settings import get_settings
        settings = get_settings()
        
        needs_full_refresh = False
        
        if strike_selection is not None:
            settings.option_strike_selection = strike_selection
            logger.info(f"≡ƒÄ» Strike Selection updated: {strike_selection}")
        
        if capital_total is not None:
            self.capital_total = float(capital_total)
            logger.info(f"≡ƒÆ░ Total Capital updated: Γé╣{self.capital_total:,.0f}")
        
        if auto_mode is not None:
            self.auto_mode = bool(auto_mode)
            logger.info(f"≡ƒöä Auto Mode: {'ENABLED' if self.auto_mode else 'DISABLED'}")

        if inst_pref and inst_pref != self.inst_pref:
            self.inst_pref = inst_pref
            needs_full_refresh = True
            logger.info(f"≡ƒöä Instrument Preference changed to: {self.inst_pref}")
        
        if backtest_days and backtest_days != self.backtest_days:
            self.backtest_days = backtest_days
            needs_full_refresh = True
            logger.info(f"≡ƒöä Backtest window changed to: {backtest_days} days")

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
            
        if futures_sl_pct and futures_sl_pct != self.futures_sl_pct:
            self.futures_sl_pct = futures_sl_pct
            needs_full_refresh = True
        if options_sl_pct and options_sl_pct != self.options_sl_pct:
            self.options_sl_pct = options_sl_pct
            needs_full_refresh = True
            
        # ΓòÉΓòÉ SYNC TO TRADE MANAGER ΓòÉΓòÉ
        self.trades.update_risk_settings(self.futures_sl_pct, self.options_sl_pct)
        
        if active_indices is not None and active_indices != self.active_indices:
            self.active_indices = active_indices
            needs_full_refresh = True
            logger.info(f"≡ƒöä Active Indices updated: {self.active_indices}")

        if mode:
            # Mode Map: HISTORICAL, REAL
            old_mode = self.mode
            self.mode = mode
            logger.info(f"≡ƒöä Mode switched: {old_mode} Γ₧ö {mode}")
            
            # If switching TO or FROM Historical, we need a full state purge
            if mode == "HISTORICAL" or old_mode == "HISTORICAL":
                needs_full_refresh = True
            
            # ΓòÉΓòÉΓòÉ HOT-SWAP BROKER ΓòÉΓòÉΓòÉ
            if mode == "REAL":
                if hasattr(self.trades, "real_broker") and self.trades.real_broker:
                    self.trades.broker = self.trades.real_broker
                    logger.info("≡ƒôí Broker Hot-Swapped to: LIVE EXECUTION (AngelOne)")
                else:
                    logger.error("Γ¥î CRITICAL: Attempted REAL mode without Broker connection!")
                    self.mode = "HISTORICAL" # Safety fallback
                    self.trades.broker = getattr(self.trades, "mock_broker", self.trades.broker)
            else:
                # Historical Mode: Signals analysed from Real Historical Data
                self.trades.broker = getattr(self.trades, "mock_broker", self.trades.broker)
                logger.info(f"≡ƒôí Broker Mode: {mode} (Signal-Based Analysis)")
            
            self.log_event(f"System Mode: {mode}", "system")

        if reset or needs_full_refresh:
            self.trades.reset_pnl()
            self.perf.reset()
            # Clear internal math caches to force recalculation
            self._cached_results.clear()
            # self._last_signal_time.clear()  # MOVED to _perform_full_recalculation to avoid early clearing
            self.simulation_id = int(time.time()) # Signal UI that a re-sim happened
            
            if hasattr(self.mtf, "_result_cache"):
                self.mtf._result_cache.clear()
            if hasattr(self.mtf, "_data_hash"):
                self.mtf._data_hash.clear()
            
            # Reset scan count to trigger initial logic in next cycle
            self.scan_count = 0 
            # Force immediate history fetch and wait for it
            asyncio.create_task(self._perform_full_recalculation())

    async def _perform_full_recalculation(self):
        """Sequential sequence to ensure re-simulation uses NEW data"""
        logger.info("≡ƒöä RE-SIMULATION SEQUENCE: Fetching deep history...")
        # Filter indices based on active_indices setting
        all_indices = self.config.get("indices", {})
        indices = {k: v for k, v in all_indices.items() if k in self.active_indices}
        await self._fetch_all_data(indices, force=True)
        
        logger.info("≡ƒöä RE-SIMULATION SEQUENCE: Recalculating signals and trades...")
        self._last_signal_time.clear() # Clear here ONLY after data is ready
        self.simulation_id = int(time.time())
        # The next _scan_cycle will now pick up the new days_back and empty last_signal_time
        logger.info("≡ƒº╣ System state RESET with FORCED history fetch.")

    async def run(self):
        self.is_running = True
        logger.info(f"≡ƒÜÇ Scanner started ΓÇö {len(self.active_indices)} active indices ├ù 3 TFs")
        
        # ΓòÉΓòÉΓòÉ Launch High-Priority Background Workers ΓòÉΓòÉΓòÉ
        asyncio.create_task(self._background_ltp_worker())
        asyncio.create_task(self._background_intel_worker())
        asyncio.create_task(self._background_history_worker())
        
        while self.is_running:
            try:
                # ΓòÉΓòÉΓòÉ CONTINUOUS SCANNING (Calculation Only - Fast Path) ΓòÉΓòÉΓòÉ
                await self._scan_cycle()
                await asyncio.sleep(self.scan_interval)
            except Exception as e:
                import traceback
                logger.error(f"Scanner error: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)

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
                
                # Stagger to utilize ~15-18 pings/sec (85-90% of 20/sec limit)
                await asyncio.sleep(0.06) 
            except Exception as e:
                logger.debug(f"LTP Worker error: {e}")
                await asyncio.sleep(1)

    async def _fetch_ltp_raw(self, name, token):
        try:
            loop = asyncio.get_event_loop()
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
        """Background Options Intel Fetcher (PCR, OI, Greeks) ΓÇö Non-Blocking"""
        while self.is_running:
            try:
                indices = {k: v for k, v in self.config.get("indices", {}).items() if k in self.active_indices}
                for name, cfg in indices.items():
                    spot = self.candles.get_latest_price(name)
                    if spot <= 0: continue
                    
                    try:
                        options_chain = await asyncio.get_event_loop().run_in_executor(
                            None, lambda: self.data.get_option_chain(name, cfg.get("option_exchange", "NFO"))
                        )
                        if options_chain is not None:
                            # Pre-calculate intel in background
                            candle_5min = self.candles.get_candles(name, "5min")
                            candle_1min = self.candles.get_candles(name, "1min")
                            
                            # Cache the result for the main loop
                            self._intel_cache[name] = self.intel.analyze(
                                instrument=name, timeframe="5min", candle_df=candle_5min,
                                candle_1min_df=candle_1min, options_chain=options_chain, 
                                spot_price=spot, strike_interval=cfg.get("strike_interval", 50), 
                                days_to_expiry=7, price_change_pct=0 # Approx
                            )
                            self._last_intel_fetch[name] = time.time()
                    except: pass
                    await asyncio.sleep(5) # Stagger between instruments
                await asyncio.sleep(25) # Main interval
            except Exception as e:
                await asyncio.sleep(5)

    async def _background_history_worker(self):
        """Background OHLCV Sync utilizing 80% of permitted 3/sec history limit"""
        while self.is_running:
            try:
                indices = {k: v for k, v in self.config.get("indices", {}).items() if k in self.active_indices}
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
            "info": "Γä╣∩╕Å",
            "success": "Γ£à",
            "warning": "ΓÜá∩╕Å",
            "error": "Γ¥î",
            "trade": "≡ƒöö",
            "system": "ΓÜÖ∩╕Å",
            "data": "≡ƒôè",
            "intel": "≡ƒºá"
        }
        icon = icons.get(type, "≡ƒö╣")
        self.activity_log.insert(0, {
            "time": datetime.now().strftime("%H:%M:%S"),
            "msg": f"{icon} {message}",
            "type": type
        })
        if len(self.activity_log) > 100:
            self.activity_log.pop()

    async def _scan_cycle(self):
        self.scan_count += 1
        t0 = time.time()
        self._check_daily_loss_limit()
        results = {"timestamp": datetime.now().isoformat(), "instruments": {}, "activity_log": self.activity_log}

        # Filter indices based on active_indices setting
        all_indices = self.config.get("indices", {})
        indices = {k: v for k, v in all_indices.items() if k in self.active_indices}
        
        if self.scan_count == 1:
            self.log_event("≡ƒôÑ Initializing deep history fetch & warm startup...", "system")
            # Force initial fetch once
            await self._fetch_all_data(indices, force=True)

            # 2. SYNC INTELLIGENCE HISTORY (Zero-Skip Memory)
            await self._sync_intelligence_history(indices)

            # 2b. PRE-FETCH PREVIOUS CLOSES (Latency Fix)
            logger.info("≡ƒôí Pre-fetching previous closes for latency optimization...")
            for name in indices:
                self.data.get_previous_close(name)
            self.log_event("≡ƒôí Market Data Baselines (Prev Closes) Synchronized", "data")

            # 3. If we are in a live mode after market hours, re-simulate TODAY
            # This populates the Results table with today's performance
            if self.mode == "REAL":
                logger.info("≡ƒöÑ WARM STARTUP: Re-simulating today's activity for dashboard population...")
                # Temporarily enable calculation to backfill
                old_power = self.system_power
                self.system_power = "ON"
                self.is_warmup = True
                
                # Process all instruments once to trigger signal/trade generation from history
                tasks = [self._process_instrument_async(name, cfg) for name, cfg in indices.items()]
                trade_signals = await asyncio.gather(*tasks)
                
                # Force coordinate signals with warmup flag
                warmup_candidates = []
                for name, result_pair in trade_signals:
                    if isinstance(result_pair, tuple) and len(result_pair) == 2:
                        _, candidate = result_pair
                        if candidate:
                            if isinstance(candidate, list): warmup_candidates.extend(candidate)
                            else: warmup_candidates.append(candidate)
                
                if warmup_candidates:
                    # Coordination is handled by the core _process_best_signal recovery logic
                    pass

                self.system_power = old_power
                self.is_warmup = False
                self.log_event("≡ƒöÑ Warm Startup Complete: All index histories restored", "success")
                logger.info("≡ƒöÑ WARM STARTUP: All index histories restored.")
                
            pass
        
        # ΓòÉΓòÉ DAILY SESSION RESET (At 09:14 AM) ΓòÉΓòÉ
        now = datetime.now()
        if now.hour == 9 and now.minute == 14:
            if not getattr(self, "_daily_reset_done", False):
                self.log_event("≡ƒº╣ New Session Prep: Clearing yesterday's trades...", "system")
                self.trades.reset_pnl()
                self._daily_reset_done = True
                logger.info("≡ƒº╣ Pre-Session Refresh: TradeManager reset for the new day.")
        elif now.hour == 9 and now.minute == 16:
            self._daily_reset_done = False # Reset readiness flag for next session

        # ΓòÉΓòÉ PERIODIC OFFICIAL CANDLE SYNC (Every 60s) ΓòÉΓòÉ
        # Ensures chart is 100% accurate with broker even if polling missed a tick
        if self.scan_count % 60 == 0:
            asyncio.create_task(self._fetch_all_data(indices))

        # ΓòÉΓòÉ SYSTEM POWER & MARKET STATUS CHECK ΓòÉΓòÉ
        is_market_open = self.data.is_market_open()
        
        # EOD Square-off monitoring (Always active if trades exist)
        if self.trades.open_trades:
            # Use the latest market timestamp for session exit logic (prevents PC-clock corruption)
            latest_market_time = self.candles.get_max_timestamp()
            self.trades.check_session_end(current_time=latest_market_time)

        # Calculation Gate: Only blocked by SYSTEM POWER (not market hours)
        # This allows users to see signals/intelligence 24/7
        if self.system_power == "OFF":
            self.is_calculating = False
            results.update({
                "system_power": "OFF",
                "is_calculating": False,
                "market_status": "SYSTEM OFF",
                "mode": self.mode,
                "config": self.get_broadcast_config(),
                "trades": self.trades.get_dashboard_payload(is_historical=(self.mode == "HISTORICAL"))
            })
            self.latest_results = results
            if self.on_update:
                try: await self.on_update(results)
                except: pass
            return

        # Market Status Tagging
        market_status = "OPEN" if is_market_open else "CLOSED"
        results["market_status"] = market_status

        self.is_calculating = True
        
        async def process_task(name, cfg):
            try:
                # 1. Use the price already fetched in the parallel loop at start of _scan_cycle
                spot = self.candles.get_latest_price(name)
                
                # If for some reason it's missing, try a quick local fetch (should be rare)
                if spot <= 0:
                    spot = self.data.get_ltp(cfg.get("exchange", "NSE"), name, cfg.get("token", ""))
                    if spot > 0: self.candles.update_latest_price(name, spot)
                
                # 2. Run Analysis
                ui_data, candidate = self._process_instrument(name, cfg)
                return name, (ui_data, candidate)
            except Exception as e:
                logger.error(f"Γ¥î Error processing {name}: {e}")
                return name, ({"error": str(e)}, [])

        # ΓòÉΓòÉ Process each instrument in PARALLEL ΓòÉΓòÉ
        tasks = []
        for name in self.active_indices:
            cfg = self.config.get("indices", {}).get(name)
            if cfg:
                tasks.append(process_task(name, cfg))
                
        scan_results = await asyncio.gather(*tasks)
        
        # ΓòÉΓòÉ COORDINATE SIGNALS (Correlation Filter) ΓòÉΓòÉ
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
        
        if candidates:
            await self._coordinate_and_execute(candidates)

        self._update_active_trades()

        elapsed = time.time() - t0
        latency_ms = round(elapsed * 1000)
        
        # ΓòÉΓòÉΓòÉ Final Payload Construction (Dashboard-Unified Schema) ΓòÉΓòÉΓòÉ
        # Calculate performance and merge it into summary for UI direct-binding
        perf_stats = self.perf.calculate(self.trades.closed_trades)
        summary_data = self.trades.get_summary()
        summary_data.update(perf_stats)

        # Build intelligence map for all instruments
        intel_map = {}
        for name, ui_data in results.get("instruments", {}).items():
            if isinstance(ui_data, dict):
                intel_map[name] = ui_data.get("intelligence", {})

        # ΓòÉΓòÉ BROADCAST CONFIG (For UI Sync) ΓòÉΓòÉ
        config_data = self.get_broadcast_config()

        results.update({
            "system_power": self.system_power,
            "is_calculating": self.is_calculating,
            "mode": self.mode,
            "instruments": results.get("instruments", {}), # Explicitly Preserve
            "config": config_data,
            "intelligence": intel_map, # Transmit ALL intel at once
            "trades": self.trades.get_dashboard_payload(is_historical=(self.mode == "HISTORICAL")),
            "scan_count": self.scan_count,
            "latency": latency_ms,
            "timestamp": datetime.now().isoformat()
        })
        self.latest_results = results
        self.last_scan_time = datetime.now()
        self.is_calculating = False
        
        if latency_ms > 1500:
            logger.warning(f"ΓÜá∩╕Å High Latency: {latency_ms}ms")
        
        # ΓòÉΓòÉ DASHBOARD THROTTLE (High-Frequency Response) ΓòÉΓòÉ
        # Backend is now ultra-fast (<20ms); broadcasting every scan cycle is safe.
        should_broadcast = (
            self.scan_count == 1 or 
            len(candidates) > 0 or 
            (time.time() - getattr(self, "_last_broadcast_time", 0)) > 0.5
        )
        
        if self.on_update and should_broadcast:
            try: 
                await self.on_update(results)
                self._last_broadcast_time = time.time()
            except: pass

    # ΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉ
    # PARALLEL DATA FETCH ΓÇö major latency fix
    # ΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉ
    async def _fetch_all_data(self, indices: Dict, force=False):
        """Fetch data for all instruments, only when cache expired"""
        now = time.time()
        tasks = []
        for name, cfg in indices.items():
            for tf in ["1min", "5min", "15min"]:
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
        fetch_days = max(7, user_days * 3) 
        
        # Scaling & Limits: 
        # 1m: Max 7 days (Yahoo hard limit)
        # 5m/15m: Can go deeper (up to 60 days)
        if tf == "1min":
            final_fetch_days = min(7, fetch_days)
        else:
            final_fetch_days = min(59, fetch_days * (5 if tf == "5min" else 10))
        
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
        logger.info("≡ƒºá Syncing Intelligence History (9:15 AM to Now)...")
        today = datetime.now().date()
        
        for name, cfg in indices.items():
            df_5m = self.candles.get_candles(name, "5min")
            if df_5m is None or df_5m.empty: continue
            
            # Filter for today's session
            today_data = df_5m[df_5m.index.date == today]
            if today_data.empty: continue
            
            # Process in batches of 5min candles to build history
            for i in range(len(today_data)):
                # We can't get historical options chain, but we can reconstruct Technical Intelligence
                # This prevents 'skipping' the morning's momentum/volatility context
                sub_df = today_data.iloc[:i+1]
                ts = sub_df.index[-1].timestamp()
                
                # Check if already in memory
                if name in self.intel_memory.memory and ts in self.intel_memory.memory[name]["timestamps"]:
                    continue
                
                # Analyze Technical components only for history (Options chain is current-only)
                try:
                    intel = self.intel.analyze(
                        instrument=name, timeframe="5min", candle_df=sub_df,
                        options_chain=None, # No historical chain available
                        spot_price=sub_df['close'].iloc[-1],
                        price_change_pct=0 # Relative change
                    )
                    self.intel_memory.record(name, ts, intel)
                except: continue
        
        self.intel_memory.save()
        self.log_event(f"Γ£à Intelligence & History Sync Complete for all indices", "success")
        logger.info("Γ£à Intelligence Memory Synchronized")

    # ΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉ
    # PROCESS INSTRUMENT ΓÇö cache results, avoid double-compute
    # ΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉ
    def _process_instrument(self, name: str, cfg: Dict) -> Tuple[Dict, List[TradeCandidate]]:
        """Main Analysis Pipeline ΓÇö focused on speed and real-time accuracy"""
        lot_size = cfg.get("lot_size", 25)
        lots = self.user_lots.get(name, 1)
        strike_interval = cfg.get("strike_interval", 50)
        combined_cap = self.capital_fut + self.capital_opt

        # 1. Prioritize Broker LTP over cached candles to avoid stale data display
        spot = self.data.get_ltp(cfg.get("exchange", "NSE"), name, cfg.get("token", "")) or 0
        if spot <= 0:
            spot = self.candles.get_latest_price(name)
        
        # ΓòÉΓòÉ Daily Change (Optimized: Use Cache) ΓòÉΓòÉ
        if not hasattr(self, "_prev_close_cache"):
            self._prev_close_cache = {}
            
        prev_close = self._prev_close_cache.get(name, 0)
        if prev_close <= 0:
            prev_close = self.data.get_previous_close(name)
            self._prev_close_cache[name] = prev_close
            
        change_pct = ((spot - prev_close) / prev_close * 100) if prev_close > 0 else 0
        
        # 2. Update MTF and Analysis
        for tf in ["1min", "5min", "15min"]:
            data = self.candles.get_candles(name, tf)
            if data is not None:
                self.mtf.update_data(name, tf, data)

        # Multi-TF analysis
        mtf_result = self.mtf.process_instrument(name, lot_size, lots, combined_cap, self.risk_fut_pct)

        # Cache per-TF process results for chart builder (avoid re-processing)
        for tf_key in ["1min", "5min", "15min"]:
            key = f"{name}_{tf_key}"
            tf_result = getattr(mtf_result, f"results_{tf_key}", None)
            if tf_result:
                self._cached_results[key] = tf_result

        # ΓòÉΓòÉ INTELLIGENCE (Using Background Cache) ΓòÉΓòÉ
        if not hasattr(self, "_intel_cache"): self._intel_cache = {}
        intel_result = self._intel_cache.get(name, {})
        
        if not intel_result:
            # Fallback for very first scan (ensures schema compatibility)
            intel_result = {
                "pcr": {"pcr": 1.0, "signal": "NEUTRAL"},
                "oi": {"signal": "NEUTRAL", "cumulative_analysis": {}},
                "greeks": {},
                "volume": {"buy_sell_ratio": 1.0},
                "order_flow": {"ratio": 1.0},
                "regime": {"regime": "UNKNOWN"}
            }
        
        results = {"intelligence": intel_result} # Ensure results dict exists for return
        
        # ΓòÉΓòÉ MASTER SPOT RESOLUTION (Broker Priority Fix) ΓòÉΓòÉ
        candle_5min = self.candles.get_candles(name, "5min")

        # ΓòÉΓòÉ MASTER SPOT RESOLUTION (Broker Priority Fix) ΓòÉΓòÉ
        # Priority: Official Broker Spot -> Historical Fallback
        live_spot = self.candles.get_latest_price(name)
        hist_spot = candle_5min['close'].iloc[-1] if candle_5min is not None and not candle_5min.empty else 0
        
        # Use broker price if available, otherwise fallback to candle close
        spot = live_spot if (live_spot and live_spot > 0) else hist_spot
        
        # Debug Sync
        if self.scan_count % 10 == 0:
            logger.info(f"≡ƒöì Price Resolution for {name}: Broker={live_spot}, Hist={hist_spot} -> Final={spot}")
        
        # simulation_id is only set during actual re-simulations (see configure())

        # ΓòÉΓòÉΓòÉ Record Intelligence Memory ΓòÉΓòÉΓòÉ
        scan_ts = time.time()
        self.intel_memory.record(name, scan_ts, intel_result)
        if self.scan_count % 300 == 0: self.intel_memory.save() # Periodically persist

        intel_score = intel_result.get("aggregate", {}).get("score", 0)
        regime = intel_result.get("regime", {}).get("regime", "UNKNOWN")
        self.latest_regimes[name] = regime # Store for adaptive trailing

        # ΓòÉΓòÉΓòÉ ATM Strike Resolution ΓòÉΓòÉΓòÉ
        atm_strike = round(spot / strike_interval) * strike_interval if spot > 0 else 0

        # ΓòÉΓòÉΓòÉ BEST SIGNAL SELECTION across timeframes ΓòÉΓòÉΓòÉ
        candidates = self._process_best_signal(
            name, mtf_result, intel_result, intel_score, regime,
            lots, lot_size, cfg, spot, atm_strike,
        )
        # Use first candidate for RR display if any
        primary_candidate = candidates[0] if candidates else None

        # ΓòÉΓòÉΓòÉ Build chart from CACHED results ΓÇö NO re-processing ΓòÉΓòÉΓòÉ
        chart_data = self._build_chart_from_cache(name)

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
            "signals": self.signals.get_active_signals(),
            "simulation_id": self.simulation_id,
            "atm_strike": atm_strike,
            "ltp": spot,
            "change_pct": change_pct,
            "spot_price": spot,
        }
        return ui_data, candidates
        
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
            logger.error(f"Γ¥î Async error for {name}: {e}")
            return name, ({}, [])

    # ΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉ
    # BEST SIGNAL SELECTION ΓÇö across 5m and 15m TFs
    # ΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉ
    def _process_best_signal(
        self, instrument, mtf_result, intel_result, intel_score, regime,
        lots, lot_size, cfg, spot, atm_strike,
    ) -> List[TradeCandidate]:
        if self._daily_loss_breached:
            return []

        # Collect NEW signals from both signal TFs
        candidates_raw = []
        for tf in ["5min", "15min"]:
            result = mtf_result.results_5min if tf == "5min" else mtf_result.results_15min
            if result is None:
                if tf == "15min": logger.debug(f"ΓÜá∩╕Å No 15min results for {instrument} yet.")
                continue
            sigs = result.get("signals", [])
            if not sigs:
                continue
            dedup_key = f"{instrument}_{tf}"
            if dedup_key not in self._last_signal_time:
                # ΓòÉΓòÉΓòÉ Mode-Aware Historical Backfill (Atomic Lock) ΓòÉΓòÉΓòÉ
                # Immediately seed to prevent concurrent backfill from other tasks
                self._last_signal_time[dedup_key] = datetime.now() 
                # Load last N days for Historical mode, or 1 day for Live modes (Warm Startup)
                is_live = "REAL" in self.mode.upper()
                if (self.mode == "HISTORICAL" or is_live):
                    from config.settings import get_settings
                    settings = get_settings()
                    
                    # Fetch 5min candles to determine trading dates for lookback
                    candles_df = self.candles.get_candles(instrument, "5min")
                    if candles_df is None or candles_df.empty:
                        return []
                    
                    # Logic Fix: Data lookback vs Trade lookback
                    # We always need 2 days of data for 15min TF stability
                    data_lookback = max(2, self.backtest_days)
                    
                    # Trade lookback: In REAL mode we only want to see today's trades in the panel
                    # ΓòÉΓòÉ Recovery Logic (Even in Live Mode) ΓòÉΓòÉ
                    # We always look back 2 days to populate the dashboard/chart
                    # but we only ALLOW live entries if it's a NEW signal.
                    trade_lookback = self.backtest_days
                    trade_cutoff = (datetime.now() - timedelta(days=trade_lookback)).date()
                    
                    # ΓòÉΓòÉΓòÉ Trading Day Awareness ΓòÉΓòÉΓòÉ
                    # Extract unique trading dates from the fetched candle data
                    active_dates = sorted(list(set(candles_df.index.date)))
                    if not active_dates: return []
                    
                    # Trade Cutoff (for display/logging in closed_trades)
                    trade_cutoff = active_dates[-min(len(active_dates), trade_lookback)]
                    
                    # Data Cutoff (for signal calculation)
                    data_cutoff = active_dates[-min(len(active_dates), data_lookback)]
                    
                    # Filter signals for this TF by data_cutoff and sort chronologically (oldest first)
                    # Optimization: Normalize to naive to avoid compare errors if mixed data sources exist
                    def get_naive_ts(s):
                        return s.timestamp.replace(tzinfo=None) if s.timestamp.tzinfo else s.timestamp
                        
                    sigs = sorted([s for s in sigs if s.timestamp.date() >= data_cutoff], key=get_naive_ts)
                    
                    # Process signals for this TF
                    for i in range(len(sigs) - 1):
                        s1, s2 = sigs[i], sigs[i+1]
                        
                        # ΓöÇΓöÇ Grade Filter: Evaluate what we would trade ΓöÇΓöÇ
                        # Pass neutral 0.0 intelligence to avoid artificially biasing historical trades
                        hist_grade, hist_conf, _ = self.signals._grade_signal(s1, 0.5, 0.0)
                        
                        # Skip C grade signals to keep dashboard clean
                        if hist_grade == "C":
                            continue
                            
                        # ΓöÇΓöÇ Historical Concurrency Guard ΓöÇΓöÇ
                        is_overlapping = False
                        for ct in self.trades.closed_trades:
                            if ct.instrument.split()[0] == instrument.split()[0]:
                                ct_entry = ct.entry_time.replace(tzinfo=None) if ct.entry_time.tzinfo else ct.entry_time
                                ct_exit = ct.exit_time.replace(tzinfo=None) if ct.exit_time.tzinfo else ct.exit_time
                                s_ts = s1.timestamp.replace(tzinfo=None) if s1.timestamp.tzinfo else s1.timestamp
                                if ct_entry <= s_ts < ct_exit:
                                    is_overlapping = True
                                    break
                        
                        if is_overlapping:
                            continue
                            
                        if s1.timestamp.date() >= trade_cutoff and s2.timestamp.date() >= trade_cutoff and s1.signal_type != s2.signal_type:
                            # ΓòÉΓòÉΓòÉ HISTORICAL INTELLIGENCE (Respect User Preference) ΓòÉΓòÉΓòÉ
                            direction = "LONG" if s1.signal_type == "BUY" else "SHORT"
                            
                            if self.inst_pref == "FUT":
                                sim_inst = "FUT"
                                sim_option_type = ""
                                sim_multiplier = 1.0
                            elif self.inst_pref == "OPT":
                                sim_inst = "OPT"
                                sim_option_type = "CE" if direction == "LONG" else "PE"
                                sim_multiplier = 0.5
                            else: # AUTO
                                is_trending = s1.adx_value >= 20.0 if hasattr(s1, 'adx_value') and s1.adx_value else True
                                hist_volatility_pct = (s1.atr_value / s1.price) * 100 if s1.price > 0 else 0
                                is_high_vol = hist_volatility_pct > 0.65 # Increased from 0.45 to allow more option buying
                                
                                # Allow options on highly confident signals even if ADX is lagging
                                if hist_grade in ["A", "A+"] or (is_trending and not is_high_vol):
                                    sim_inst = "OPT"
                                    sim_option_type = "CE" if direction == "LONG" else "PE"
                                    sim_multiplier = 0.5
                                else:
                                    sim_inst = "FUT"
                                    sim_option_type = ""
                                    sim_multiplier = 1.0
                            
                            # ΓòÉΓòÉΓòÉ HISTORICAL RISK SIZING ΓòÉΓòÉΓòÉ
                            if sim_inst == "FUT":
                                hist_cap = self.capital_fut
                                hist_risk = self.risk_fut_pct
                            else:
                                hist_cap = self.capital_opt
                                hist_risk = self.risk_opt_pct
                            
                            risk_amount = hist_cap * (hist_risk / 100.0)
                            
                            # Global safety cap
                            risk_amount = min(risk_amount, getattr(settings, "max_trade_loss_abs", 10000.0))
                            
                            index_stop_distance = s1.stop_distance
                            
                            # Volatility Filter for Futures
                            if sim_inst == "FUT":
                                risk_amount = (self.capital_fut * self.risk_fut_pct / 100)
                                max_allowed_sl = s1.price * (self.futures_sl_pct / 100.0)
                                if index_stop_distance > max_allowed_sl:
                                    continue # Skip high vol trade in history
                            else:
                                risk_amount = (self.capital_opt * self.risk_opt_pct / 100)
                            
                            unit_risk = max(1.0, index_stop_distance) * sim_multiplier
                            
                            # Fixed 1 Lot for Futures, Risk-based for Options
                            if sim_inst == "FUT":
                                hist_lots = 1 # HARDCAP
                            else:
                                max_units = int(risk_amount / unit_risk)
                                # Cap by user preference
                                user_target = self.user_lots.get(instrument, 1)
                                hist_lots = min(user_target, max(1, int(max_units / lot_size)))
                            
                            qty = hist_lots * lot_size
                            
                            # ΓòÉΓòÉΓòÉ VIRTUAL PREMIUM CALCULATION (With Stop Loss Protection) ΓòÉΓòÉΓòÉ
                            entry_premium = s1.price * 0.012 if sim_inst == "OPT" else s1.price
                            
                            # ΓöÇΓöÇ 1. Calculate Maximum Allowed SL (Risk Management) ΓöÇΓöÇ
                            if sim_inst == "FUT":
                                # Max 0.35% Spot SL as per user rule
                                max_sl_dist = entry_premium * (self.futures_sl_pct / 100.0)
                                sl_price = s1.trailing_stop
                                # If trailing stop is worse than 0.35%, cap it
                                if direction == "LONG":
                                    sl_price = max(sl_price, entry_premium - max_sl_dist)
                                else:
                                    sl_price = min(sl_price, entry_premium + max_sl_dist)
                            else:
                                # Max 10% Premium SL for Options
                                max_sl_dist = entry_premium * (self.options_sl_pct / 100.0)
                                sl_price = entry_premium - max_sl_dist # Options only BUY (Long)
                            
                            # ΓöÇΓöÇ 2. Determine Exit Premium ΓöÇΓöÇ
                            raw_exit_spot = s2.price
                            
                            if sim_inst == "OPT":
                                spot_diff = raw_exit_spot - s1.price if direction == "LONG" else s1.price - raw_exit_spot
                                raw_exit_premium = entry_premium + (spot_diff * sim_multiplier)
                            else:
                                raw_exit_premium = raw_exit_spot
                            
                            # ΓöÇΓöÇ 3. Stop Loss Detection ΓöÇΓöÇ
                            is_sl_hit = False
                            if sim_inst == "FUT":
                                if direction == "LONG" and raw_exit_spot < sl_price:
                                    exit_premium = sl_price
                                    is_sl_hit = True
                                elif direction == "SHORT" and raw_exit_spot > sl_price:
                                    exit_premium = sl_price
                                    is_sl_hit = True
                                else:
                                    exit_premium = raw_exit_premium
                            else:
                                if raw_exit_premium < sl_price:
                                    exit_premium = sl_price
                                    is_sl_hit = True
                                else:
                                    exit_premium = raw_exit_premium

                            # ΓòÉΓòÉΓòÉ CALCULATE P&L ΓòÉΓòÉΓòÉ
                            if sim_inst == "OPT":
                                gross_pnl = (exit_premium - entry_premium) * qty
                            else:
                                if direction == "LONG":
                                    gross_pnl = (exit_premium - entry_premium) * qty
                                else:
                                    gross_pnl = (entry_premium - exit_premium) * qty
                            
                            sim_charges = 80.0 if sim_inst == "OPT" else 200.0
                            net_pnl = gross_pnl - sim_charges
                            
                            # Update exit reason if SL was hit
                            hist_exit_reason = "SL HIT" if is_sl_hit else "HISTORICAL"
                            
                            # Estimate historical RR based on grade
                            hist_rr = 1.25 if hist_grade == "C" else (2.0 if hist_grade == "A" else 1.5)
                            
                            # ΓöÇΓöÇ Deterministic Historical ID ΓöÇΓöÇ
                            hist_id = f"H_{instrument}_{tf}_{s1.timestamp.strftime('%H%M%S')}"
                            
                            # Check for duplicates in closed_trades
                            if any(ct.id == hist_id for ct in self.trades.closed_trades):
                                continue
                                
                            # Calculate target at entry time (no look-ahead bias)
                            # Trade.target MUST be in SPOT terms to match Live Signal structure
                            if direction == "LONG":
                                hist_target = s1.price + (index_stop_distance * hist_rr)
                            else:
                                hist_target = s1.price - (index_stop_distance * hist_rr)
                            
                            from trading.trade_manager import Trade
                            e_time = IST.localize(s1.timestamp) if s1.timestamp.tzinfo is None else s1.timestamp
                            ex_time = IST.localize(s2.timestamp) if s2.timestamp.tzinfo is None else s2.timestamp
                            t = Trade(
                                id=hist_id, instrument=instrument, timeframe=tf,
                                direction=direction, entry_price=entry_premium, entry_time=e_time,
                                trailing_stop=s1.trailing_stop, current_stop=s1.trailing_stop,
                                lots=hist_lots, lot_size=lot_size, grade=f"{hist_grade} (Hist)", confidence=hist_conf,
                                inst_type=sim_inst, option_type=sim_option_type, atm_strike=atm_strike,
                                rr_ratio=hist_rr, target=hist_target,
                                status="CLOSED", exit_price=exit_premium, exit_time=ex_time,
                                pnl=net_pnl, charges=sim_charges, exit_reason=hist_exit_reason,
                                instrument_multiplier=sim_multiplier, entry_spot=s1.price
                            )
                            self.trades.closed_trades.append(t)

                    # ΓòÉΓòÉΓòÉ 3. STATE RECOVERY: Re-open the latest signal if still active ΓòÉΓòÉΓòÉ
                    # CRITICAL FIX: Only recover signals from TODAY's session.
                    # A signal from a previous day (e.g. 07 May 14:30) would have been 
                    # squared off at session end (15:30). It is NOT a live position.
                    if sigs:
                        last_s = sigs[-1]
                        today = datetime.now().date()
                        signal_is_from_today = last_s.timestamp.date() == today
                        if signal_is_from_today:
                            direction = "LONG" if last_s.signal_type == "BUY" else "SHORT"
                            logger.info(f"≡ƒöä Recovering Active {direction} state for {instrument} from {last_s.timestamp}")
                            if not self.is_warmup:
                                self.log_event(f"≡ƒöä Recovered {direction} signal for {instrument} from {last_s.timestamp.strftime('%H:%M')}", "trade")
                            
                            # ΓöÇΓöÇ Recovery Cross-TF Guard ΓöÇΓöÇ
                            existing = [t for t in self.trades.open_trades.values() if t.instrument == instrument]
                            
                            # Determine recovery instrument type based on AUTO/FUT/OPT preference
                            # In AUTO mode, Grade A/A+ signals are forced to Options
                            rec_inst_type = "FUT"
                            if self.inst_pref == "AUTO":
                                rec_inst_type = "OPT" # Assume high grade for recovery unless specified
                            elif self.inst_pref == "OPT":
                                rec_inst_type = "OPT"
                                
                            if existing:
                                # Switch type if another TF is already open for this instrument
                                existing_type = getattr(existing[0], 'inst_type', 'FUT')
                                rec_inst_type = "OPT" if existing_type == "FUT" else "FUT"
                            
                            # Calculate atm_strike for recovery if it's an Option
                            rec_atm_strike = 0.0
                            if rec_inst_type == "OPT":
                                strike_interval = cfg.get("strike_interval", 50)
                                rec_atm_strike = round(last_s.price / strike_interval) * strike_interval

                            self.trades.open_trade(
                                instrument=instrument, timeframe=tf, direction=direction,
                                price=last_s.price * 0.012 if rec_inst_type == "OPT" else last_s.price,
                                trailing_stop=last_s.trailing_stop,
                                target=last_s.price + (abs(last_s.price - last_s.trailing_stop) * 1.5) if direction == "LONG" else last_s.price - (abs(last_s.price - last_s.trailing_stop) * 1.5),
                                lots=1, lot_size=lot_size, grade="Recovery (Hist)",
                                confidence=min(0.95, last_s.adx_value/50.0) if hasattr(last_s, 'adx_value') else 0.7,
                                instrument_multiplier=0.5 if rec_inst_type == "OPT" else 1.0,
                                option_type=("CE" if direction == "LONG" else "PE") if rec_inst_type == "OPT" else "",
                                atm_strike=rec_atm_strike,
                                trading_symbol=last_s.trading_symbol if hasattr(last_s, 'trading_symbol') else "",
                                symbol_token=last_s.symbol_token if hasattr(last_s, 'symbol_token') else "",
                                inst_type=rec_inst_type,
                                entry_spot=last_s.price, 
                                is_recovery=True
                            )
                            if self.trades.open_trades:
                                active_tid = list(self.trades.open_trades.keys())[-1]
                                ts = pd.Timestamp(last_s.timestamp)
                                if ts.tzinfo is not None:
                                    active_trade.entry_time = ts.tz_convert(IST).tz_localize(None).to_pydatetime()
                                else:
                                    active_trade.entry_time = ts.to_pydatetime()
                                
                                # ΓòÉΓòÉ Recovery Initialization (Institutional Calibration) ΓòÉΓòÉ
                                # For recovered options, we must estimate synthetic state
                                if rec_inst_type == "OPT":
                                    # Entry premium estimate (1.2% of spot)
                                    active_trade.entry_price = last_s.price * 0.012
                                    active_trade.current_price = active_trade.entry_price
                                    # Signed delta for synthetic moves
                                    active_trade.instrument_multiplier = 0.5 if direction == "LONG" else -0.5
                                    # For recovery trades, ensure the stop is relative to the entry premium if possible,
                                    # but the system uses Index Stops (e.g. last_s.trailing_stop)
                                    active_trade.current_stop = last_s.trailing_stop if last_s.trailing_stop > 100 else (last_s.price * 0.995)
                                else:
                                    active_trade.entry_price = last_s.price
                                    active_trade.current_price = last_s.price
                                    active_trade.current_stop = last_s.trailing_stop if last_s.trailing_stop > 100 else (last_s.price * 0.998)
                        elif last_s.timestamp.date() >= trade_cutoff:
                            # ΓòÉΓòÉΓòÉ SYNTHETIC SESSION-END CLOSE ΓòÉΓòÉΓòÉ
                            # Signal from a previous day ΓÇö create a closed trade at session end price
                            
                            # ΓöÇΓöÇ Grade Filter: Evaluate what we would trade ΓöÇΓöÇ
                            eod_grade, eod_conf, _ = self.signals._grade_signal(last_s, 0.5, 0.0)
                            if eod_grade == "C":
                                continue
                                
                            # ΓöÇΓöÇ Historical Concurrency Guard ΓöÇΓöÇ
                            is_overlapping = False
                            for ct in self.trades.closed_trades:
                                if ct.instrument.split()[0] == instrument.split()[0]:
                                    ct_entry = ct.entry_time.replace(tzinfo=None) if ct.entry_time.tzinfo else ct.entry_time
                                    ct_exit = ct.exit_time.replace(tzinfo=None) if ct.exit_time.tzinfo else ct.exit_time
                                    s_ts = last_s.timestamp.replace(tzinfo=None) if last_s.timestamp.tzinfo else last_s.timestamp
                                    if ct_entry <= s_ts < ct_exit:
                                        is_overlapping = True
                                        break
                            
                            if is_overlapping:
                                continue

                            direction = "LONG" if last_s.signal_type == "BUY" else "SHORT"
                            sig_date = last_s.timestamp.date()
                            
                            # Find the closing price on that date from candle data
                            candles_for_tf = self.candles.get_candles(instrument, tf)
                            eod_price = last_s.price  # fallback
                            if candles_for_tf is not None and not candles_for_tf.empty:
                                day_candles = candles_for_tf[candles_for_tf.index.date == sig_date]
                                if not day_candles.empty:
                                    eod_price = day_candles['close'].iloc[-1]
                            
                            eod_id = f"EOD_{instrument}_{tf}_{last_s.timestamp.strftime('%m%d%H%M')}"
                            if not any(ct.id == eod_id for ct in self.trades.closed_trades):
                                if direction == "LONG":
                                    eod_pnl = (eod_price - last_s.price) * lot_size
                                else:
                                    eod_pnl = (last_s.price - eod_price) * lot_size
                                eod_pnl -= 200.0  # Simulated charges
                                
                                eod_exit_time = datetime.combine(sig_date, dtime(15, 30))
                                e_time = IST.localize(last_s.timestamp) if last_s.timestamp.tzinfo is None else last_s.timestamp
                                ex_time = IST.localize(eod_exit_time) if eod_exit_time.tzinfo is None else eod_exit_time
                                
                                from trading.trade_manager import Trade
                                t = Trade(
                                    id=eod_id, instrument=instrument, timeframe=tf,
                                    direction=direction, entry_price=last_s.price, entry_time=e_time,
                                    trailing_stop=last_s.trailing_stop, current_stop=last_s.trailing_stop,
                                    lots=1, lot_size=lot_size, grade=f"{eod_grade} (EOD, Hist)", confidence=eod_conf,
                                    inst_type="FUT", rr_ratio=1.5,
                                    target=last_s.price + (abs(last_s.price - last_s.trailing_stop) * 1.5) if direction == "LONG" else last_s.price - (abs(last_s.price - last_s.trailing_stop) * 1.5),
                                    status="CLOSED", exit_price=eod_price, exit_time=ex_time,
                                    pnl=eod_pnl, charges=200.0, exit_reason="SESSION_END",
                                    instrument_multiplier=1.0, entry_spot=last_s.price
                                )
                                self.trades.closed_trades.append(t)
                                logger.debug(f"≡ƒôè Synthetic EOD close: {eod_id} | PnL: Γé╣{eod_pnl:,.0f}")

                # Update lock with precise last signal time
                if sigs:
                    self._last_signal_time[dedup_key] = sigs[-1].timestamp
                else:
                    self._last_signal_time[dedup_key] = datetime.min
                
            last_time = self._last_signal_time[dedup_key]
            for sig in sigs:
                # Naive comparison to avoid TZ errors
                s_ts = sig.timestamp.replace(tzinfo=None) if sig.timestamp.tzinfo else sig.timestamp
                l_ts = last_time.replace(tzinfo=None) if last_time.tzinfo else last_time
                
                if s_ts > l_ts:
                    # Calculate Grade for prioritization
                    grade, conf, _ = self.signals._grade_signal(sig, mtf_result.confluence_score, intel_score, regime)
                    candidates_raw.append({
                        "sig": sig, 
                        "tf": tf, 
                        "grade": grade, 
                        "confidence": conf
                    })
                    self._last_signal_time[dedup_key] = max(self._last_signal_time[dedup_key], sig.timestamp)

        if not candidates_raw:
            return []

        # ΓòÉΓòÉΓòÉ SCORE each candidate and pick the BEST ΓòÉΓòÉΓòÉ
        # Rule: Priority to 15min unless 5min has higher confidence/score
        scored = []
        for c in candidates_raw:
            sig, tf, grade, confidence = c["sig"], c["tf"], c["grade"], c["confidence"]
            
            # Simple scoring: Confidence is the primary metric
            score = confidence
            
            # Timeframe Priority Logic:
            # 15min is priority. 5min only wins if its score is HIGHER.
            # We add a tiny epsilon to 15min to break ties in its favor.
            if tf == "15min":
                score += 0.0001
                
            scored.append((score, sig, tf, grade, confidence))

        # Sort by final score
        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_sig, best_tf, best_grade, best_conf = scored[0]

        # ΓòÉΓòÉΓòÉ EXIT before ENTRY / UPGRADE ΓòÉΓòÉΓòÉ
        for tid, trade in list(self.trades.open_trades.items()):
            if trade.instrument == instrument and trade.status == "OPEN":
                # 1. Opposite signal (Exit)
                is_opposite = (
                    (trade.direction == "LONG" and best_sig.signal_type == "SELL") or
                    (trade.direction == "SHORT" and best_sig.signal_type == "BUY")
                )
                if is_opposite:
                    self.trades.close_trade(tid, best_sig.price, "OPPOSITE_SIGNAL")
                    self.log_event(f"≡ƒöä EXIT {trade.direction} {instrument} (Opposite Signal)", "trade")
                    self._notify(
                        f"≡ƒöä EXIT {trade.direction} {instrument} @ {best_sig.price:.2f} | "
                        f"PnL: Γé╣{trade.pnl:+,.0f}", "sell"
                    )
                    break # Allow potential flip entry
                
                # 2. Timeframe Upgrade (Same direction)
                if best_tf == "15min" and trade.timeframe == "5min":
                    trade.timeframe = "15min"
                    trade.grade = best_grade
                    trade.confidence = best_conf
                    logger.info(f"≡ƒÜÇ UPGRADED {instrument} trade to 15-min status (Confidence: {best_conf:.2f})")
                    return [] # Metadata updated, don't open new trade
                
                # 3. Same direction, no upgrade: skip
                return []

        graded = self.signals.process_signal(best_sig, mtf_result.confluence_score, intel_score, regime)
        if not graded or not graded.is_actionable:
            return []

        if graded.confidence < 0.55:
            logger.info(f"ΓÅ│ Skipping {instrument} {best_sig.signal_type}: Low Confidence ({graded.confidence:.0%})")
            return []

        can_open, _ = self.trades.can_open_trade()
        if not can_open:
            return []

        iv_percentile = intel_result.get("greeks", {}).get("iv_percentile", 50.0)
        is_high_iv = iv_percentile > 80.0
        is_choppy = regime in ["CHOPPY", "SIDEWAYS", "MEAN_REVERTING", "UNKNOWN"]
        strong_momentum = abs(intel_score) > 60.0 or best_score > 0.4
        
        if self.inst_pref == "FUT":
            inst_type = "FUT"
        elif self.inst_pref == "OPT":
            inst_type = "OPT"
        else: # AUTO
            # Force Options for A/A+ grades for capital efficiency on trending moves
            if graded.grade in ["A", "A+"] and not is_choppy:
                inst_type = "OPT"
            elif is_high_iv or is_choppy or not strong_momentum:
                inst_type = "FUT"
            else:
                inst_type = "OPT"

        # ΓòÉΓòÉΓòÉ Cross-Timeframe Monopoly Guard ΓòÉΓòÉΓòÉ
        existing_index_trades = [t for t in self.trades.open_trades.values() if t.instrument == instrument]
        if existing_index_trades:
            # Check if this is a DIFFERENT timeframe
            is_different_tf = all(t.timeframe != timeframe for t in existing_index_trades)
            is_high_grade = graded.grade in ["A", "A+"]
            
            can_add = False
            if is_different_tf and is_high_grade and len(existing_index_trades) == 1:
                # Force switch type (FUT -> OPT or vice versa)
                existing_trade = existing_index_trades[0]
                existing_type = getattr(existing_trade, 'inst_type', 'FUT')
                
                # If first was FUT, second MUST be OPT. If first was OPT, second MUST be FUT.
                inst_type = "OPT" if existing_type == "FUT" else "FUT"
                can_add = True
                logger.info(f"≡ƒîƒ Cross-TF Confluence ({timeframe}): Adding {inst_type} to complement {existing_trade.timeframe} {existing_type}")
            
            if not can_add:
                logger.info(f"≡ƒÜ½ Monopoly Guard: {instrument} already has an active {existing_index_trades[0].timeframe} trade. Skipping {timeframe}.")
                return []

        # ΓöÇΓöÇ Strike List Generation (ATM/ITM/BOTH) ΓöÇΓöÇ
        from config.settings import get_settings
        settings = get_settings()
        
        strike_selection = getattr(settings, "option_strike_selection", "ATM")
        strike_interval = cfg.get("strike_interval", 50)
        
        option_type = "CE" if best_sig.signal_type == "BUY" else "PE"
        
        # Calculate Strikes
        atm_strike_val = round(spot / strike_interval) * strike_interval if spot > 0 else 0
        itm_strike_val = (atm_strike_val - strike_interval) if option_type == "CE" else (atm_strike_val + strike_interval)
        
        target_strikes = []
        if inst_type == "FUT":
            target_strikes = [0.0]
        else:
            if strike_selection == "ATM": target_strikes = [atm_strike_val]
            elif strike_selection == "ITM": target_strikes = [itm_strike_val]
            else: target_strikes = [atm_strike_val, itm_strike_val] # BOTH

        results_list = []
        for strike in target_strikes:
            if inst_type == "FUT":
                option_type_local = ""
                strike_local = 0.0
                instrument_multiplier = 1.0
            else:
                option_type_local = option_type
                strike_local = strike
                if option_type_local == "CE":
                    # CE Delta is Positive
                    instrument_multiplier = intel_result.get("greeks", {}).get("call", {}).get("delta", 0.5)
                else:
                    # PE Delta is Negative
                    instrument_multiplier = intel_result.get("greeks", {}).get("put", {}).get("delta", -0.5)
                    if instrument_multiplier > 0: instrument_multiplier = -instrument_multiplier
                
                if abs(instrument_multiplier) < 0.1:
                    instrument_multiplier = 0.5 if option_type_local == "CE" else -0.5

            # ΓòÉΓòÉΓòÉ DYNAMIC RISK SIZING (0-2% FUT, 0-8% OPT) ΓòÉΓòÉΓòÉ
            # ΓòÉΓòÉ RR and Direction (Must be calculated first for risk sizing) ΓòÉΓòÉ
            final_rr = max(1.25, 1.5 + ({"A": 0.5, "B": 0.0, "C": -0.25}.get(graded.grade, 0.0)) + (intel_score / 200.0))
            direction = "LONG" if best_sig.signal_type == "BUY" else "SHORT"
            
            # Quality Factor: 0.0 to 1.0 based on Confidence + Grade + RR Ratio
            quality_factor = min(1.0, (best_conf + (0.1 if best_grade in ["A", "A+"] else 0.0)))
            rr_factor = 1.0 if final_rr >= 1.5 else (final_rr / 1.5)
            total_multiplier = quality_factor * rr_factor
            
            if inst_type == "FUT":
                # Dynamic FUT Risk based on base_fut_risk (User setting, e.g. 2% or 0.3%)
                # User stated: "FUT Risk Per session/day is 2%, FUT Max SL every trade is 0.3%"
                # The sizing risk should be capped by the "Max SL every trade" (futures_sl_pct) 
                # but technically this determines how many lots. Since lots=1 is hardcapped, 
                # this is mostly academic for Futures, but we keep logic consistent.
                base_fut_risk = self.risk_fut_pct
                dynamic_fut_risk = max(0.1, min(base_fut_risk, base_fut_risk * total_multiplier))
                risk_amt = self.capital_fut * (dynamic_fut_risk / 100.0)
                
                # Use user-defined futures_sl_pct as a hard cap on the stop distance
                max_allowed_sl_pts = spot * (self.futures_sl_pct / 100.0)
                
                # The stop distance is the minimum of the technical (ATR) stop and the hard-cap SL
                index_stop_distance = min(best_sig.stop_distance, max_allowed_sl_pts)
                
                # If the technical stop is much wider than the allowed stop, we still use the allowed stop
                # but we skip if it's excessively wide (optional: kept L1310 logic as a safeguard)
                if best_sig.stop_distance > (max_allowed_sl_pts * 1.5): # Safeguard: Skip if technical stop is >1.5x user stop
                     logger.warning(f"≡ƒÜ½ Technical Stop ({best_sig.stop_distance:.1f}) too wide for user limit ({max_allowed_sl_pts:.1f}). Skipping.")
                     return []
                
                # Standard Lot Calculation (Stay at 1 lot by default)
                actual_lots = 1
            else:
                # Dynamic OPT Risk: 1.0% to 8.0% of OPT Capital
                base_opt_risk = self.risk_opt_pct
                dynamic_opt_risk = max(1.0, min(base_opt_risk, base_opt_risk * total_multiplier))
                risk_amt = self.capital_opt * (dynamic_opt_risk / 100.0)
                
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
                
                # Final stop is the tighter of the two
                index_stop_distance = min(best_sig.stop_distance, hard_cap_index_sl)

            # ΓòÉΓòÉ RR and Direction ΓòÉΓòÉ
            final_rr = max(1.25, 1.5 + ({"A": 0.5, "B": 0.0, "C": -0.25}.get(graded.grade, 0.0)) + (intel_score / 200.0))
            direction = "LONG" if best_sig.signal_type == "BUY" else "SHORT"
            
            # Target/Stop calculation based on Index Spot
            if direction == "LONG":
                entry_stop = spot - index_stop_distance
                target = spot + (index_stop_distance * final_rr)
            else: # SHORT
                entry_stop = spot + index_stop_distance
                target = spot - (index_stop_distance * final_rr)

            # ΓòÉΓòÉ Order Resolution ΓòÉΓòÉ
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
                # ΓòÉΓòÉ INTELLIGENT STRIKE SCORING (ALPHA SCORE) ΓòÉΓòÉ
                # This determines the winner if BOTH mode is selected.
                # Factors: Delta (Speed), OI (Support), Volume (Liquidity)
                strike_alpha = (instrument_multiplier * 0.4) # Delta 40%
                
                # Fetch OI/Vol for this specific strike if available
                # (Falling back to general intel if strike-specific not found)
                oi_score = intel_result.get("oi", {}).get("score", 50) / 100.0
                vol_score = intel_result.get("volume", {}).get("score", 50) / 100.0
                
                # ITM Bonus: Higher delta usually leads to 'best gains'
                itm_bonus = 0.15 if (strike_local == itm_strike_val and inst_type == "OPT") else 0.0
                
                final_strike_score = strike_alpha + (oi_score * 0.3) + (vol_score * 0.3) + itm_bonus

                # ΓòÉΓòÉ LIVE PREMIUM FETCH (Fixes BUG-02) ΓòÉΓòÉ
                live_trade_price = spot
                if inst_type == "OPT":
                    live_premium = self.data.get_ltp(cfg.get("exchange", "NFO"), trading_symbol, symbol_token)
                    live_trade_price = live_premium if live_premium > 0 else (spot * 0.012)
                
                results_list.append(TradeCandidate(
                    instrument=instrument, direction=direction, price=live_trade_price,
                    stop=entry_stop, target=target, lots=actual_lots, lot_size=lot_size, grade=best_grade,
                    confidence=best_conf, timeframe=best_tf, inst_type=inst_type, option_type=option_type_local,
                    atm_strike=strike_local, multiplier=instrument_multiplier, trading_symbol=trading_symbol,
                    symbol_token=symbol_token, rr=round(final_rr, 2), score=final_strike_score
                ))

        if not results_list:
            return []

        # ΓòÉΓòÉΓòÉ COMPETITIVE WINNER SELECTION ΓòÉΓòÉΓòÉ
        # If BOTH mode, pick the strike with highest Alpha Score.
        # Otherwise, the list already contains only 1 entry.
        results_list.sort(key=lambda x: x.score, reverse=True)
        winner = results_list[0]
        
        if strike_selection == "BOTH" and len(results_list) > 1:
            other = results_list[1]
            self.log_event(f"≡ƒÅå Competitive Face-off: {winner.atm_strike} (Score: {winner.score:.2f}) BEATS {other.atm_strike}", "trade")
            logger.info(f"≡ƒÅå Competitive Strike Face-off: {winner.atm_strike} (Score: {winner.score:.2f}) BEAT {other.atm_strike} (Score: {other.score:.2f})")
        else:
            self.log_event(f"≡ƒÄ» Selected Strike: {winner.atm_strike} ({winner.inst_type})", "trade")
            
        self.log_event(f"≡ƒÄ» Potential {winner.direction} Signal for {winner.instrument} ({winner.timeframe}) | Score: {winner.score:.2f}", "trade")
        return [winner]

    async def _coordinate_and_execute(self, candidates: List[TradeCandidate], is_warmup: bool = False):
        """
        Correlation Filter: 
        If multiple indices have same direction, only take the 1-2 best.
        Only take 3rd if conviction is extraordinarily high (>90%).
        """
        # ΓöÇΓöÇ Institutional Execution Gate ΓöÇΓöÇ
        if not self.data.is_market_open() and not is_warmup:
            # Don't place new trades if market is closed (except in Historical mode)
            if self.mode != "HISTORICAL":
                if self.scan_count % 100 == 0:
                    logger.debug("ΓÅ│ Execution Gate: Market is closed. Analysis only.")
                return

        # Group candidates by direction
        by_dir = {"LONG": [], "SHORT": []}
        for c in candidates:
            by_dir[c.direction].append(c)
        
        for direction, signals in by_dir.items():
            if not signals: continue
            
            # Sort by Score/Confidence
            signals.sort(key=lambda x: x.confidence, reverse=True)
            
            # ΓòÉΓòÉΓòÉ CROSS-INSTRUMENT CORRELATION GUARD ΓòÉΓòÉΓòÉ
            # Rules: 
            # 1. Max 2 index trades concurrently.
            # 2. Max 3 ONLY if all are A+ setups.
            # 3. Avoid same-direction trades unless setup is very strong.
            
            for i, sig in enumerate(signals):
                total_open = len(self.trades.open_trades)
                # ΓöÇΓöÇ Per-Instrument Safety (Cross-TF Complementary Rule) ΓöÇΓöÇ
                base_sig_inst = sig.instrument.split()[0]
                existing_index_trades = [t for t in self.trades.open_trades.values() if t.instrument.split()[0] == base_sig_inst]
                
                if len(existing_index_trades) >= 2:
                    continue # Max 2 concurrent trades per index
                    
                if existing_index_trades:
                    # Allow second trade ONLY if:
                    # 1. Complementary type (FUT vs OPT)
                    # 2. Very High Grade (A+)
                    is_comp_type = all(t.inst_type != sig.inst_type for t in existing_index_trades)
                    is_high_grade = sig.grade in ["A+", "Recovered"] or getattr(sig, 'confidence', 0.0) >= 0.90
                    
                    if not (is_comp_type and is_high_grade):
                        continue # Guard block validly triggered

                # ΓöÇΓöÇ Correlation Logic ΓöÇΓöÇ
                can_take = False
                
                # Check for similar trades (same direction)
                same_dir_exists = any(t.direction == direction for t in self.trades.open_trades.values())
                
                if total_open == 0:
                    can_take = True
                elif total_open == 1:
                    # If taking a second trade in the SAME direction, require high confidence
                    if same_dir_exists:
                        if sig.confidence >= 0.75: # Grade A or better
                            can_take = True
                    else:
                        can_take = True # Divergent directions are okay
                elif total_open == 2:
                    # Allow 3rd only if ALL (existing + new) are A+
                    all_aplus = all(t.confidence >= 0.90 for t in self.trades.open_trades.values())
                    if sig.confidence >= 0.90 and all_aplus:
                        can_take = True
                        logger.info(f"≡ƒÆÄ ULTIMATE CONFLUENCE: Taking 3rd index trade ({sig.instrument})")
                
                if can_take:
                    # ΓöÇΓöÇΓöÇ Manual Signal Gate ΓöÇΓöÇΓöÇ
                    if not self.auto_mode and not is_warmup:
                        if i == 0: # Only notify for the primary candidate
                            logger.info(f"≡ƒôó MANUAL SIGNAL: {sig.direction} {sig.instrument} @ {sig.price} (Auto Mode: Manual)")
                            self.log_event(f"≡ƒÄ» Manual {sig.direction} Signal: {sig.instrument} @ {sig.price:.2f}", "trade")
                            self._notify(f"≡ƒÄ» MANUAL SIGNAL: {sig.direction} {sig.instrument} @ {sig.price:.2f}. Auto-execution disabled.", "info")
                        continue # Don't take the trade in manual mode

                    # ΓòÉΓòÉΓòÉ ASYNC REAL BROKER ENTRY (Latency Audit Fix) ΓòÉΓòÉΓòÉ
                    actual_spot = self.candles.get_latest_price(sig.instrument)
                    trade = self.trades.open_trade(
                        instrument=sig.instrument, timeframe=sig.timeframe, direction=sig.direction,
                        price=sig.price, trailing_stop=sig.stop,
                        lots=sig.lots, lot_size=sig.lot_size, grade=sig.grade,
                        atm_strike=sig.atm_strike, option_type=sig.option_type,
                        target=sig.target, 
                        rr_ratio=sig.rr,
                        confidence=sig.confidence,
                        instrument_multiplier=sig.multiplier,
                        trading_symbol=sig.trading_symbol,
                        symbol_token=sig.symbol_token,
                        inst_type=sig.inst_type,
                        exec_type="A",
                        entry_spot=actual_spot
                    )
                    
                    if trade and self.trades.broker and sig.trading_symbol and sig.symbol_token:
                        def on_order_placed(order_id):
                            if order_id:
                                trade.broker_order_id = order_id
                                logger.success(f"Γ£à Broker Order ID Attached: {order_id}")
                                self.save_state()
                            else:
                                logger.error(f"Γ¥î Async Broker failure for {sig.trading_symbol}")
                        
                        self.trades.broker.place_order_async(
                            symbol=sig.trading_symbol,
                            token=sig.symbol_token,
                            qty=trade.quantity,
                            side=sig.direction,
                            callback=on_order_placed
                        )

                    if trade:
                        open_count += 1
                        
                        self._notify(
                            f"{'≡ƒƒó' if sig.direction == 'LONG' else '≡ƒö┤'} {sig.direction} {sig.instrument} "
                            f"@ {sig.price:.2f} | Conf: {sig.confidence:.0%} | TF: {sig.timeframe}",
                            "buy" if sig.direction == "LONG" else "sell"
                        )
                else:
                    logger.info(f"ΓÅ│ Waitlisting correlated trade: {sig.instrument} {sig.direction} (Current {direction} Exposure: {open_count})")

    # ΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉ
    # CHART BUILDER ΓÇö uses CACHED results (zero re-processing)
    # ΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉΓòÉ
    def _build_chart_from_cache(self, instrument: str) -> Dict:
        chart = {}
        for tf in ["1min", "5min", "15min"]:
            key = f"{instrument}_{tf}"
            result = self._cached_results.get(key)
            df = self.candles.get_candles(instrument, tf)
            
            if df is None or len(df) == 0 or result is None:
                continue

            # Optimization: Re-use cached chart structure if possible
            cache_id = f"chart_cache_{key}"
            if hasattr(self, cache_id):
                cached = getattr(self, cache_id)
                if cached['len'] == len(df) and cached['last'] == df.index[-1]:
                    chart[tf] = cached['data']
                    continue

            # UNIX timestamp conversion for chart display
            # Candle timestamps are naive-IST. astype('int64') interprets them as UTC epoch.
            # The chart's toISOString() formatter then displays the UTC time ΓÇö which coincidentally
            # matches the IST wall-clock time since the input was naive-IST treated as UTC.
            timestamps = (df.index.astype('int64') // 10**9).tolist() if hasattr(df.index, 'astype') else range(len(df))
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
            ts_line = []
            for i in range(min(len(ts_data), len(candles_list))):
                ts_line.append({
                    "time": candles_list[i]["time"],
                    "value": round(float(ts_data[i]), 2),
                    "color": ts_colors[i] if i < len(ts_colors) else "gray",
                })

            # Signal markers
            markers = []
            lot_size = self.config.get("indices", {}).get(instrument, {}).get("lot_size", 25)
            lots = self.user_lots.get(instrument, 1)
            qty = lots * lot_size
            strike_interval = self.config.get("indices", {}).get(instrument, {}).get("strike_interval", 50)
            spot = self.candles.get_latest_price(instrument) or 0
            atm = round(spot / strike_interval) * strike_interval if spot > 0 and strike_interval > 0 else 0

            for sig in result.get("signals", []):
                if sig.bar_index < len(candles_list):
                    risk_pts = round(sig.stop_distance, 2)
                    if sig.signal_type == "BUY":
                        tgt = round(sig.price + sig.stop_distance * 1.5, 2)
                        label = (
                            f"Γû▓ ENTRY: {sig.price:.2f}\n"
                            f"Stop: {sig.trailing_stop:.2f}\n"
                            f"Risk: {risk_pts} pts\n"
                            f"Size: {qty} units"
                        )
                        markers.append({
                            "time": candles_list[sig.bar_index]["time"],
                            "position": "belowBar", "color": "#22c55e",
                            "shape": "arrowUp", "text": label,
                        })
                    else:
                        tgt = round(sig.price - sig.stop_distance * 1.5, 2)
                        label = (
                            f"Γû╝ EXIT: {sig.price:.2f}\n"
                            f"Stop: {sig.trailing_stop:.2f}\n"
                            f"Risk: {risk_pts} pts"
                        )
                        markers.append({
                            "time": candles_list[sig.bar_index]["time"],
                            "position": "aboveBar", "color": "#ef4444",
                            "shape": "arrowDown", "text": label,
                        })

            # Optimization: Only send the last 500 points of intelligence history to match chart
            intel_raw = self.intel_memory.get_history(instrument)
            intel_history = {k: v[-500:] if isinstance(v, list) else v for k, v in intel_raw.items()}
            
            res = {
                "candles": candles_list[-500:],
                "trailing_stop": ts_line[-500:],
                "markers": markers[-100:],
                "state": result.get("state", {}),
                "intel_history": intel_history
            }
            chart[tf] = res
            # Save to optimization cache
            setattr(self, cache_id, {'len': len(df), 'last': df.index[-1], 'data': res})
        return chart

    def _update_active_trades(self):
        for tid, trade in list(self.trades.open_trades.items()):
            price = self.candles.get_latest_price(trade.instrument)
            if price is None: continue
            
            regime = self.latest_regimes.get(trade.instrument, "UNKNOWN")
            key_1m = f"{trade.instrument}_1min"
            engine_1m = self.mtf.engines.get(key_1m)
            
            if not engine_1m: continue
            
            state_1m = engine_1m.get_state(key_1m)
            raw_ts = state_1m.trailing_stop
            new_stop = trade.current_stop
            
            # ΓöÇΓöÇ Adaptive Logic based on Market Regime ΓöÇΓöÇ
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
                    self.log_event(f"≡ƒ¢í∩╕Å Profit Protector: Locking BE for {trade.instrument}", "trade")

            latest_time = self.candles.get_max_timestamp()
            self.trades.update_trade(tid, price, new_stop, current_time=latest_time)
            if tid not in self.trades.open_trades:
                self.log_event(f"≡ƒ¢æ Trade Closed: {trade.instrument} Stop Hit @ {price:.2f}", "trade")
                self._notify(f"≡ƒ¢æ STOP HIT {trade.direction} {trade.instrument} @ {price:.2f}", "sell")

    def _check_daily_loss_limit(self):
        """Hard stop if daily loss exceeds limit (Segment Specific)"""
        if self.capital_total <= 0: return
        
        today = datetime.now().date()
        
        # Calculate Realized + Unrealized per segment
        fut_pnl = 0.0
        opt_pnl = 0.0
        
        # Realized
        for t in self.trades.closed_trades:
            if t.entry_time.date() == today:
                if t.inst_type == "FUT": fut_pnl += t.pnl
                else: opt_pnl += t.pnl
                
        # Unrealized
        for t in self.trades.open_trades.values():
            if t.entry_time.date() == today:
                if t.inst_type == "FUT": fut_pnl += t.unrealized_pnl
                else: opt_pnl += t.unrealized_pnl
        
        fut_limit = -(self.capital_fut * (self.risk_fut_pct / 100.0))
        opt_limit = -(self.capital_opt * (self.options_sl_pct / 100.0))
        
        if fut_pnl < fut_limit and not getattr(self, "_fut_loss_breached", False):
            self._fut_loss_breached = True
            dlp = abs(fut_pnl / self.capital_fut * 100)
            logger.warning(f"≡ƒÜ¿ FUT DAILY LOSS LIMIT BREACHED: {dlp:.1f}%")
            self.log_event(f"≡ƒÜ¿ FUT Trading Halted: {dlp:.1f}% Loss", "error")
            for tid, t in list(self.trades.open_trades.items()):
                if t.inst_type == "FUT":
                    self.trades.close_trade(tid, t.current_price, "FUT_DAILY_LOSS_LIMIT")
                    
        if opt_pnl < opt_limit and not getattr(self, "_opt_loss_breached", False):
            self._opt_loss_breached = True
            dlp = abs(opt_pnl / self.capital_opt * 100)
            logger.warning(f"≡ƒÜ¿ OPT DAILY LOSS LIMIT BREACHED: {dlp:.1f}%")
            self.log_event(f"≡ƒÜ¿ OPT Trading Halted: {dlp:.1f}% Loss", "error")
            for tid, t in list(self.trades.open_trades.items()):
                if t.inst_type == "OPT":
                    self.trades.close_trade(tid, t.current_price, "OPT_DAILY_LOSS_LIMIT")

    def _notify(self, message, msg_type="info"):
        if self.on_notification:
            try: self.on_notification(message, msg_type)
            except: pass

    def stop(self):
        self.is_running = False
        logger.info("Scanner stopped")
