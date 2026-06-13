"""
UT1 Index Trading System — Configuration
Pydantic-based settings with .env support
"""

import json
import dotenv
from pathlib import Path
from typing import Literal, Optional, Dict, Any
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


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


class Settings(BaseSettings):
    """Main application settings loaded from .env"""

    # ─── AngelOne API ────────────────────────────────────────
    angelone_api_key: str = ""
    angelone_client_id: str = ""
    angelone_password: str = ""
    angelone_totp_secret: str = ""
    angelone_secret_key: str = ""

    # ─── Fyers API ───────────────────────────────────────────
    fyers_app_id: str = "6P37DKRJG6-100"
    fyers_secret_key: str = ""
    fyers_login_id: str = ""
    fyers_password: str = ""
    fyers_redirect_uri: str = "http://127.0.0.1:7000/"

    # ─── Trading Mode ────────────────────────────────────────
    trading_mode: Literal["HISTORICAL", "REAL"] = "HISTORICAL"
    active_indices: list[str] = ["NIFTY", "BANKNIFTY", "SENSEX", "MIDCPNIFTY"]

    # ─── Risk Settings ───────────────────────────────────────
    max_concurrent_positions: int = 4
    max_daily_loss_pct: float = 3.0
    capital_total: float = 1100000.0   # Total Demat Capital (Base for Daily Loss)
    
    # Futures Specific
    capital_fut: float = 750000.0   # Shared futures capital across active indices
    risk_fut_pct: float = 1.5        # FUT Daily Max Loss Limit (%)
    futures_sl_pct: float = 0.15     # Max SL for Futures per trade (%)
    fut_force_fixed_lots: bool = False # Always trade 1 lot for Futures
    fut_cost: float = 200.0         # Transaction cost per Futures trade
    
    # Options Specific
    capital_opt: float = 300000.0   # Shared options capital across active indices
    risk_opt_pct: float = 12.0       # Max SL for Options per trade (%)
    options_sl_pct: float = 15.0    # OPT Daily Max Loss Limit (%)
    opt_cost: float = 80.0          # Transaction cost per Options trade
    option_strike_selection: Literal["ATM", "ITM", "BOTH"] = "BOTH"
    inst_pref: Literal["FUT", "OPT", "AUTO"] = "AUTO"
    sl_mode: Literal["NATURAL", "HARDCODED"] = "NATURAL"
    signal_grade_preference: Literal["auto", "B", "B+", "A", "A+"] = "A"
    ut_timeframe_entry_policy: Literal["PRIMARY_15", "INCLUDE_5MIN"] = "INCLUDE_5MIN"
    ut_5min_option_min_confidence: float = 0.60
    ut_5min_loss_cooldown_minutes: int = 45
    
    max_trade_loss_abs: float = 15000.0 # Hard cap on absolute loss per trade
    dynamic_rr_min: float = 1.6 # Minimum initial RR for qualified trades
    dynamic_rr_max: float = 12.0 # Quality-scaled initial RR ceiling; runner mode can capture larger option moves
    dynamic_rr_runner_unlock_ratio: float = 0.95 # T1 progress needed before removing hard target
    dynamic_rr_runner_lock_pct: float = 0.90 # Runner-mode trailing lock on open gains

    # ─── Dashboard & Backtest ────────────────────────────────
    dashboard_port: int = 7000
    dashboard_host: str = "127.0.0.1"
    dashboard_auth_token: str = ""
    dashboard_allowed_origins: list[str] = []
    default_backtest_days: int = 1 # Default backtest window to 1 day as requested
    settings_recalculation_debounce_seconds: float = 8.0
    settings_persistence_debounce_seconds: float = 0.75
    recalculation_worker_enabled: bool = True
    recalculation_worker_timeout_seconds: float = 120.0
    scanner_stale_after_seconds: float = 5.0
    api_default_trade_limit: int = 100
    api_max_trade_limit: int = 2000
    api_default_chart_bars: int = 300
    api_max_chart_bars: int = 1000
    outcome_calibration_enabled: bool = True
    production_safe_mode_required: bool = True

    # ─── UT Bot Core Settings ────────────────────────────────
    ut_preset: str = "AGGRESSIVE"
    ut_atr_period: int = 10
    ut_atr_multiplier: float = 1.0
    ut_use_heikin_ashi: bool = False
    ut_signal_mode: Literal["realtime", "confirmed"] = "realtime"
    ut_adx_filter: bool = True      # ADX is informational, not a signal blocker
    ut_adx_period: int = 14
    ut_adx_threshold: float = 25.0
    ut_strict_adx: bool = False      # Keep UT marker timing aligned with TradingView; ADX is graded later
    ut_smart_trailing: bool = True   # Trailing stop follows the trade's timeframe
    ut_regime_adaptation: bool = True # Enable dynamic regime adaptation for choppy markets
    ut_concurrency_guard: bool = True # Enable concurrency guard to prevent overlapping index trades
    ut_fractal_confluence: bool = True # Allow 1m entries if 5m trend aligns
    ut_session_filter: bool = True   # Enable session filtering in the engine
    ut_intel_early_exit: bool = True # Enable intelligence-based early exits
    ut_dynamic_confidence: bool = True # Enable dynamic confidence threshold
    ut_backtest_more_results: bool = True # Loosen historical-only gates for exploratory backtests
    ut_session_start: str = "09:18"
    ut_session_end: str = "15:25"
    ut_no_entry_after: str = "15:00" # Block fresh 15min entries at/after this IST time
    ut_5min_no_entry_after: str = "15:15" # Qualified 5min entries have a later cutoff
    ut_force_exit_time: str = "15:25"
    trade_product_type: Literal["CARRYFORWARD"] = "CARRYFORWARD"
    live_filter_leniency_pct: float = 0.0 # Keep live gates strict; loosen only during deliberate diagnostics
    intelligence_cache_ttl_seconds: float = 90.0 # Option-chain cache TTL for live intelligence
    option_premium_cache_ttl_seconds: float = 2.0 # Open option premium refresh TTL
    history_cache_ttl_seconds: float = 120.0 # Broker history cache TTL for candles/volume
    live_index_timeout_seconds: float = 6.0 # Per-index live scan budget before skipping that index for the cycle
    live_calculation_lock_timeout_seconds: float = 12.0 # Live lock recovery budget; historical work gets a larger derived budget
    scanner_exception_backoff_seconds: float = 1.0 # Fast recovery after an unexpected scanner-loop exception
    morning_refresh_parallelism: int = 3 # Bound broker pressure while warming independent indices
    runtime_cache_prune_interval_seconds: float = 60.0 # Physically evict expired quote/decision cache entries
    trade_state_checkpoint_seconds: float = 1.0 # Coalesce mark-to-market persistence while keeping crash recovery fresh
    trade_stagnation_minutes: float = 20.0 # Minimum profitable hold before stagnation exit can trigger
    trade_stagnation_max_extra_minutes: float = 10.0 # Extra breathing time for volatile indices/regimes
    trade_stagnation_min_peak_r: float = 0.50 # Require meaningful peak profit relative to initial risk
    circuit_breaker_slippage_bps: float = 10.0 # Conservative projected exit slippage for open-position loss checks
    position_sizing_min_atr_fraction: float = 0.10 # Stop-distance floor used for sizing only
    position_sizing_min_price_fraction: float = 0.00005 # Absolute fallback when ATR is unavailable
    live_signal_stabilization_seconds: float = 20.0 # Short anti-flicker window before live entry filtering
    live_signal_stabilization_seconds_15min: float = 20.0 # Stabilization buffer for 15min timeframe
    live_intrabar_signal_entries: bool = True # Detect forming-candle UTBot arrows before candle close
    repaint_check_interval_minutes: int = 2 # Periodic interval for repaint guard checking
    last_minute_extension_seconds: int = 60 # Check last minute signal entry extension
    live_restart_recovery_grace_seconds: float = 120.0 # Recover only the most recent just-closed candle after restart
    live_signal_post_close_grace_seconds_5min: float = 300.0 # Let an already-running scanner finish slow 5min refreshes
    live_signal_post_close_grace_seconds_15min: float = 300.0 # Let an already-running scanner finish slow 15min refreshes
    live_choppy_gate_confidence: float = 0.90 # Choppy/ranging setups need exceptional confidence
    momentum_override_threshold: float = 25.0 # ADX threshold to bypass choppy market block
    max_trades_per_index: int = 5
    max_consecutive_losses: int = 3
    index_cooldown_minutes: float = 4.0
    ut_option_history_mode: Literal["stored_or_synthetic", "fetch_or_synthetic", "fetch_or_skip"] = "fetch_or_synthetic"
    pcr_near_strikes: int = 5 # Strikes on either side of ATM for near-money PCR

    # ─── Logging ─────────────────────────────────────────────
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_path: str = "./logs"

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        case_sensitive=False,
        extra='ignore'
    )

    @field_validator('max_daily_loss_pct')
    @classmethod
    def validate_max_daily_loss(cls, v: float) -> float:
        if not 0 < v <= 10:
            raise ValueError('max_daily_loss_pct must be between 0 and 10')
        return v


    def get_ut_engine_params(self) -> dict:
        preset = getattr(self, "ut_preset", "AGGRESSIVE").upper()
        kv = 1.0
        atr = 10
        sig_mode = self.ut_signal_mode
        if preset == "BALANCED":
            kv = 1.3
            atr = 10
            sig_mode = "confirmed"
        elif preset == "CONSERVATIVE":
            kv = 1.6
            atr = 14
            sig_mode = "confirmed"
            
        return {
            "key_value": kv,
            "atr_period": atr,
            "regime_adaptation": _setting_enabled(getattr(self, "ut_regime_adaptation", True), True),
            "use_heikin_ashi": self.ut_use_heikin_ashi,
            "signal_mode": sig_mode,
            "adx_filter": self.ut_adx_filter,
            "adx_period": self.ut_adx_period,
            "adx_threshold": self.ut_adx_threshold,
            "strict_adx": self.ut_strict_adx,
            "session_filter": self.ut_session_filter,
            "session_start": self.ut_session_start,
            "session_end": self.ut_session_end
        }
        

    def save_to_env(self):
        """Save current settings to .env file"""
        env_file = Path('.env')
        if not env_file.exists():
            env_file.touch()
            
        # Serialize fields using pydantic's model_dump
        data = self.model_dump()
        for key, val in data.items():
            # Handle list/dict serialization (e.g. active_indices)
            if isinstance(val, (list, dict)):
                str_val = json.dumps(val)
            else:
                str_val = str(val)
            dotenv.set_key('.env', key.upper(), str_val)

    def get_instruments(self) -> Dict[str, Any]:
        """Load instrument configuration from instruments.json"""
        config_path = Path(__file__).parent / "instruments.json"
        with open(config_path, 'r') as f:
            return json.load(f)

    def ensure_directories(self):
        """Create required directories"""
        dirs = [self.log_path, "data_store", "data_store/candles"]
        for d in dirs:
            Path(d).mkdir(parents=True, exist_ok=True)


# Singleton instance
_settings = None


def get_settings() -> Settings:
    """Get or create global settings instance"""
    global _settings
    if _settings is None:
        _settings = Settings()
        _settings.ensure_directories()
    return _settings
