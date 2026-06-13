import pandas as pd
from datetime import datetime, time as dtime, timedelta
from typing import Dict, List, Tuple, Optional
from loguru import logger
from dataclasses import dataclass, field
from config.settings import get_settings
from engine.intelligence_score import normalize_intelligence_score
from engine.trade_accounting import estimate_trade_charges
from engine.signal_manager import GradedSignal
from trading.trade_manager import IST, Trade


def _entry_cutoff(settings, timeframe: str):
    """Return the configured no-entry label and time for a timeframe."""
    if str(timeframe) == "5min":
        label = str(getattr(settings, "ut_5min_no_entry_after", "15:15") or "15:15")
    else:
        label = str(getattr(settings, "ut_no_entry_after", "15:00") or "15:00")
    return label, datetime.strptime(label, "%H:%M").time()


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
    reasons: list[str] = field(default_factory=list)
    spot_stop: float = 0.0
    spot_target: float = 0.0
    signal_timestamp: Optional[datetime] = None
    current_price: float = 0.0
    pnl: float = 0.0
    status: str = "TRADE SIGNAL"
    action: str = "ENTRY"
    exit_reason: str = ""
    adx_value: float = 0.0
    is_explosive_bypass: bool = False
    

def _estimate_opt_premium(spot_price: float, scanner=None, instrument: str = "", trade_date=None) -> float:
    """Dynamic option premium estimation based on VIX"""
    base_pct = 0.012
    vix_val = 15.0
    if scanner and trade_date is not None and hasattr(scanner, "_vix_data"):
        try:
            day = str(trade_date)[:10]
            day_values = [
                float(v)
                for k, v in getattr(scanner, "_vix_data", {}).items()
                if str(k).startswith(day) and float(v or 0.0) > 0
            ]
            if day_values:
                vix_val = sum(day_values) / len(day_values)
        except Exception:
            pass
    elif scanner and hasattr(scanner, "data"):
        try:
            live_vix = scanner.data.get_latest_price("INDIA VIX")
            if live_vix and live_vix > 0:
                vix_val = live_vix
        except Exception:
            pass

    dte_boost = 1.0
    if scanner and instrument and trade_date is not None and hasattr(scanner, "expiry"):
        try:
            dte = max(0.0, float(scanner.expiry.get_dte(instrument, trade_date)))
            dte_boost = max(0.75, min(1.35, (max(dte, 1.0) / 3.0) ** 0.25))
        except Exception:
            pass

    # Premium roughly scales with VIX. At VIX 15, pct is 1.2%
    scaler = max(10.0, vix_val) / 15.0
    return spot_price * base_pct * scaler * dte_boost

class SignalProcessor:
    """
    Handles signal candidate selection, scoring, and prioritization.
    Extracted from Scanner to reduce file size.
    """
    def __init__(self, scanner):
        self.scanner = scanner
        self._option_history_cache: Dict[Tuple[str, str], pd.DataFrame] = {}

    @staticmethod
    def _live_signal_candle_is_closed(signal_ts: datetime, timeframe: str, now: datetime) -> bool:
        """Return whether a signal's source candle has finalized."""
        tf_minutes = {"1min": 1, "5min": 5, "15min": 15}.get(str(timeframe or ""), 5)
        signal_check = signal_ts.replace(tzinfo=None) if getattr(signal_ts, "tzinfo", None) else signal_ts
        now_check = now.replace(tzinfo=None) if getattr(now, "tzinfo", None) else now
        return now_check >= signal_check + timedelta(minutes=tf_minutes)

    @staticmethod
    def _live_signal_is_new(
        signal_ts: datetime,
        signal_type: str,
        last_ts: datetime,
        last_identity: str,
    ) -> bool:
        """Allow a forming candle to emit again when its BUY/SELL identity changes."""
        identity = f"{signal_ts.isoformat()}|{signal_type}"
        return signal_ts > last_ts or (signal_ts == last_ts and identity != last_identity)

    @classmethod
    def _live_signal_recheck_due(
        cls,
        signal_ts: datetime,
        timeframe: str,
        now: datetime,
        eligible: bool,
        pending: bool,
        last_recheck: datetime,
    ) -> bool:
        """Re-evaluate a dynamically rejected arrow while its candle is still forming."""
        if not eligible or pending or cls._live_signal_candle_is_closed(signal_ts, timeframe, now):
            return False
        now_check = now.replace(tzinfo=None) if getattr(now, "tzinfo", None) else now
        last_check = (
            last_recheck.replace(tzinfo=None)
            if getattr(last_recheck, "tzinfo", None)
            else last_recheck
        )
        return (now_check - last_check).total_seconds() >= 1.0

    @staticmethod
    def _contract_ltp(scanner, exchange: str, symbol: str, token: str) -> float:
        """Use the same quote abstraction for option and futures contracts."""
        if not scanner or not getattr(scanner, "data", None) or not symbol or not token:
            return 0.0
        try:
            price = scanner.data.get_ltp(exchange, symbol, token)
            return float(price or 0.0)
        except Exception as exc:
            logger.debug(f"Contract LTP fetch failed for {symbol}: {exc}")
            return 0.0

    @staticmethod
    def _grade_rank(grade: str) -> int:
        base = str(grade or "C").split()[0]
        return {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4, "Recovered": 4}.get(base, 0)

    @staticmethod
    def _is_choppy_regime(regime: str) -> bool:
        return str(regime or "UNKNOWN").upper() in {
            "CHOPPY",
            "SIDEWAYS",
            "MEAN_REVERTING",
            "RANGING",
            "RANGEBOUND",
            "VOLATILE",
            "UNKNOWN",
        }

    @staticmethod
    def _relaxed_threshold(value: float, settings) -> float:
        leniency = min(max(float(getattr(settings, "live_filter_leniency_pct", 0.15) or 0.0), 0.0), 0.20)
        return float(value or 0.0) * (1.0 - leniency)

    @staticmethod
    def _choppy_confidence_gate(settings) -> float:
        return float(getattr(settings, "live_choppy_gate_confidence", 0.84) or 0.84)

    @staticmethod
    def _dynamic_rr(grade: str, confidence: float, intel_score: float = 0.0, max_rr: Optional[float] = None) -> float:
        """Quality-scaled initial RR; exceptional moves are handled by runner mode."""
        conf = min(max(float(confidence or 0.0), 0.0), 1.0)
        min_rr = 1.6
        if max_rr is None:
            try:
                settings = get_settings()
                min_rr = float(getattr(settings, "dynamic_rr_min", 1.6) or 1.6)
                max_rr = float(getattr(settings, "dynamic_rr_max", 12.0) or 12.0)
            except Exception:
                max_rr = 12.0
        max_rr = max(1.60, float(max_rr or 12.0))
        min_rr = min(max(1.60, float(min_rr or 1.6)), max_rr)
        base_grade = str(grade or "C").split()[0]
        grade_bonus = {
            "A+": 1.15,
            "A": 0.75,
            "B+": 0.35,
            "B": 0.0,
            "C": -0.20,
            "Recovered": 0.75,
        }.get(base_grade, 0.0)
        confidence_runway = max(0.0, conf - 0.55) * 3.2
        intel_adj = min(max(normalize_intelligence_score(intel_score) / 1.20, -0.40), 1.25)
        rr = min_rr + confidence_runway + grade_bonus + intel_adj
        return round(max(min_rr, min(max_rr, rr)), 2)

    @staticmethod
    def _option_index_stop_distance(
        natural_stop_distance: float,
        est_premium: float,
        options_sl_pct: float,
        instrument_multiplier: float,
        settings,
    ) -> float:
        """Convert option premium stop policy into index points."""
        natural = float(natural_stop_distance or 0.0)
        if str(getattr(settings, "sl_mode", "NATURAL") or "NATURAL").upper() == "NATURAL":
            return natural
        premium_risk = float(est_premium or 0.0) * (float(options_sl_pct or 0.0) / 100.0)
        mult = max(0.1, abs(float(instrument_multiplier or 0.0)))
        hardcoded = premium_risk / mult
        return min(natural, hardcoded) if natural > 0 else hardcoded

    def _option_grade_allowed(
        self,
        grade: str,
        confidence: float,
        regime: str,
        adx_value: float = 0.0,
        intel_score: float = 0.0,
        signal_score: float = 0.0,
    ) -> bool:
        """Shared option grade gate for live, recovery, and historical rows."""
        rank = self._grade_rank(grade)
        conf = float(confidence or 0.0)
        settings = get_settings()
        more_results = (
            getattr(settings, "ut_backtest_more_results", True)
            and getattr(getattr(self, "scanner", None), "mode", "") == "HISTORICAL"
        )
        if self._is_choppy_regime(regime):
            if more_results:
                return rank >= 2 or conf >= self._relaxed_threshold(0.62, settings)
            return rank >= 3 or conf >= self._relaxed_threshold(0.90, settings)

        high_volume_or_trend = (
            str(regime or "").upper() in {"TRENDING", "STRONG_TREND", "BREAKOUT", "MOMENTUM"}
            or float(adx_value or 0.0) >= 22.0
            or abs(normalize_intelligence_score(intel_score)) >= 0.50
            or float(signal_score or 0.0) >= 0.55
            or conf >= 0.55
        )
        return rank >= 3 or (rank >= 2 and high_volume_or_trend)

    def _passes_quality_gate(
        self,
        tf: str,
        grade: str,
        confidence: float,
        regime: str,
        inst_type: str,
        settings,
        adx_value: float = 0.0,
        trend_15m_agrees: bool = False,
        intel_score: float = 0.0,
    ) -> bool:
        """Mirror scanner's _passes_live_trade_ready_gate for historical backfill."""
        grade_rank = self._grade_rank(grade)
        
        # Intraday Momentum Override
        momentum_override = False
        momentum_threshold = float(getattr(settings, "momentum_override_threshold", 25.0) or 25.0)
        if adx_value >= momentum_threshold:
            momentum_override = True
            
        explosive_move = momentum_override or (abs(normalize_intelligence_score(intel_score)) >= 0.70)
            
        regime_adaptation = bool(getattr(settings, "ut_regime_adaptation", True))
        is_choppy = regime_adaptation and self._is_choppy_regime(regime) and not momentum_override
        if explosive_move and self._is_choppy_regime(regime):
            logger.info(f"🚀 [OVERRIDE] Choppy regime bypassed due to strong ADX ({adx_value:.1f} >= {momentum_threshold})")
        more_results = (
            getattr(settings, "ut_backtest_more_results", True)
            and getattr(getattr(self, "scanner", None), "mode", "") == "HISTORICAL"
        )

        # Timeframe Entry Policy Filter
        policy = str(getattr(settings, "ut_timeframe_entry_policy", "PRIMARY_15") or "PRIMARY_15").upper()
        if policy == "PRIMARY_15" and tf == "5min":
            return False

        # 5min is a timing timeframe; require either strong grade/confidence or 15min agreement.
        if tf == "5min":
            min_conf = self._relaxed_threshold(float(getattr(settings, "ut_5min_option_min_confidence", 0.60) or 0.60), settings)
            if more_results:
                min_conf = min(min_conf, self._relaxed_threshold(0.72, settings))
            grade_conf_ok = grade_rank >= 3 and confidence >= min_conf

            if not (grade_conf_ok or trend_15m_agrees or explosive_move):
                return False
            if is_choppy and not (grade_conf_ok or trend_15m_agrees or explosive_move):
                return False

        # Options decay badly in range-bound sessions, so choppy options need A/A+ or very high confidence.
        if more_results and is_choppy and inst_type == "OPT" and not explosive_move and not ((grade_rank >= 2 and confidence >= self._relaxed_threshold(0.58, settings)) or confidence >= self._relaxed_threshold(0.72, settings)):
            return False
        if not more_results and is_choppy and inst_type == "OPT" and not explosive_move and not ((grade_rank >= 3 and confidence >= self._relaxed_threshold(0.72, settings)) or confidence >= self._relaxed_threshold(0.90, settings)):
            return False

        # Never promote weak B-grade live/manual rows in choppy markets unless confidence is exceptional.
        # Baseline parity before leniency: confidence >= 0.72 for A/A+ choppy setups.
        # Baseline parity before leniency: confidence >= 0.72 or exceptional confidence bypasses choppy options.
        # Baseline parity before leniency: is_choppy and grade_rank < 3 and confidence < 0.90.
        if more_results and is_choppy and grade_rank < 1 and confidence < self._relaxed_threshold(0.62, settings):
            return False
        if not more_results and is_choppy and grade_rank < 3 and confidence < self._relaxed_threshold(self._choppy_confidence_gate(settings), settings):
            return False

        return True

    @staticmethod
    def _trend_15m_agrees_at(mtf_result, signal_ts, signal_type: str) -> bool:
        result = getattr(mtf_result, "results_15min", None) or {}
        signals = result.get("signals", []) or []
        if not signals:
            state = result.get("state", {}) or {}
            position = int(state.get("position") or 0)
            expected = 1 if str(signal_type or "").upper() == "BUY" else -1
            return position == expected

        target_ts = signal_ts.replace(tzinfo=None) if getattr(signal_ts, "tzinfo", None) else signal_ts
        prior = []
        for signal in signals:
            ts = signal.timestamp.replace(tzinfo=None) if getattr(signal.timestamp, "tzinfo", None) else signal.timestamp
            if target_ts is None or ts <= target_ts:
                prior.append(signal)
        if not prior:
            return False
        latest = max(
            prior,
            key=lambda signal: signal.timestamp.replace(tzinfo=None)
            if getattr(signal.timestamp, "tzinfo", None)
            else signal.timestamp,
        )
        return str(getattr(latest, "signal_type", "") or "").upper() == str(signal_type or "").upper()

    def _is_5min_impulse_reversal(
        self,
        sig,
        mtf_result,
        graded_confidence: float,
        settings,
    ) -> bool:
        """Allow a 5m choppy reversal only when the flip is a real impulse."""
        candle = getattr(sig, "raw_candle", {}) or {}
        try:
            open_px = float(candle.get("open") or 0.0)
            high_px = float(candle.get("high") or 0.0)
            low_px = float(candle.get("low") or 0.0)
            close_px = float(candle.get("close") or getattr(sig, "price", 0.0) or 0.0)
            atr = max(float(getattr(sig, "atr_value", 0.0) or 0.0), 1e-9)
            body_atr = abs(close_px - open_px) / atr
            range_atr = abs(high_px - low_px) / atr
            close_near_extreme = (
                close_px >= high_px - (0.25 * atr)
                if getattr(sig, "signal_type", "") == "BUY"
                else close_px <= low_px + (0.25 * atr)
            )
        except Exception:
            return False

        adx_value = float(getattr(sig, "adx_value", 0.0) or 0.0)
        conf = float(graded_confidence or 0.0)
        strong_adx = adx_value >= 28.0 and conf >= self._relaxed_threshold(self._choppy_confidence_gate(settings), settings)
        impulse_body = body_atr >= 0.80 and range_atr >= 1.05 and close_near_extreme
        extreme_range = range_atr >= 1.45 and close_near_extreme and conf >= self._relaxed_threshold(0.78, settings)

        volume_confirmed = False
        try:
            candles = (getattr(mtf_result, "results_5min", None) or {}).get("candles") or []
            idx = int(getattr(sig, "bar_index", -1) or -1)
            if candles and 0 <= idx < len(candles):
                current_vol = float(candles[idx].get("volume") or 0.0)
                lookback = candles[max(0, idx - 12):idx]
                vols = [float(row.get("volume") or 0.0) for row in lookback if float(row.get("volume") or 0.0) > 0]
                avg_vol = sum(vols) / len(vols) if vols else 0.0
                volume_confirmed = avg_vol > 0 and current_vol >= (1.35 * avg_vol)
        except Exception:
            volume_confirmed = False

        return strong_adx or ((impulse_body or extreme_range) and (volume_confirmed or conf >= self._relaxed_threshold(0.82, settings)))

    def _historical_option_strike(self, instrument: str, direction: str, price: float, cfg: Dict) -> float:
        interval = cfg.get("strike_interval", 50)
        atm = round(float(price) / interval) * interval
        selection = getattr(get_settings(), "option_strike_selection", "ITM")
        option_type = "CE" if direction == "LONG" else "PE"
        if selection in ("ITM", "BOTH"):
            return atm - interval if option_type == "CE" else atm + interval
        return atm

    @staticmethod
    def _filter_option_history_date(df: pd.DataFrame, trade_date) -> pd.DataFrame:
        if df is None or df.empty or trade_date is None:
            return pd.DataFrame() if df is None else df
        try:
            target_day = pd.Timestamp(trade_date).date()
            work = df.copy()
            if getattr(work.index, "tz", None) is not None:
                work.index = work.index.tz_convert(IST).tz_localize(None)
            day_rows = work[work.index.date == target_day]
            return day_rows.sort_index()
        except Exception:
            return pd.DataFrame()

    def _load_option_candles(self, scanner, instrument: str, strike: float, option_type: str, interval: str, trade_date) -> Tuple[pd.DataFrame, str, str]:
        opt_info = scanner.data.get_option_token(instrument, strike, option_type, trade_date=trade_date)
        if not opt_info:
            return pd.DataFrame(), "", ""
        symbol = str(opt_info.get("symbol", ""))
        token = str(opt_info.get("token", ""))
        exchange = str(opt_info.get("exch_seg") or ("BFO" if instrument == "SENSEX" else "NFO"))
        trade_day = pd.Timestamp(trade_date).date() if trade_date is not None else None
        cache_key = (symbol, interval, trade_day.isoformat() if trade_day else "")
        diag = getattr(scanner, "_diag_inc", None)
        if cache_key in self._option_history_cache:
            if diag:
                diag("option_history", "hits" if not self._option_history_cache[cache_key].empty else "misses")
            return self._option_history_cache[cache_key], symbol, token
        from pathlib import Path
        cache_path = Path("data_store/candles") / f"{symbol}_{interval}.csv"
        if cache_path.exists():
            try:
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True).sort_index()
                day_df = self._filter_option_history_date(df, trade_day)
                if not day_df.empty:
                    self._option_history_cache[cache_key] = day_df
                    if diag:
                        diag("option_history", "hits")
                    return day_df, symbol, token
                logger.info(f"Option history cache for {symbol} has no rows for {trade_day}; fetching/using configured fallback.")
            except Exception:
                pass
        mode = getattr(get_settings(), "ut_option_history_mode", "fetch_or_skip")
        if mode in ("stored_or_synthetic", "fetch_or_skip") and trade_day and trade_day < datetime.now(IST).date():
            self._option_history_cache[cache_key] = pd.DataFrame()
            if diag:
                diag("option_history", "misses")
            return pd.DataFrame(), symbol, token
        if mode == "stored_or_synthetic":
            self._option_history_cache[cache_key] = pd.DataFrame()
            if diag:
                diag("option_history", "synthetic")
            return pd.DataFrame(), symbol, token
        days_back = max(15, int(getattr(scanner, "backtest_days", 7) or 7) * 3)
        if diag:
            diag("option_history", "attempts")
        df = scanner.data.get_historical_candles(
            token,
            exchange,
            interval,
            days_back=days_back,
            instrument_name=symbol,
        )
        if df is not None and not df.empty:
            df = df.sort_index()
            df = self._filter_option_history_date(df, trade_day)
            if diag and not df.empty:
                diag("option_history", "hits")
        else:
            df = pd.DataFrame()
        if df.empty:
            if diag:
                diag("option_history", "misses")
        self._option_history_cache[cache_key] = df
        return df, symbol, token

    def _simulate_actual_option_exit(
        self,
        option_df: pd.DataFrame,
        start_ts: datetime,
        end_ts: datetime,
        sl_price: float,
        rr: float,
        natural_sl_distance: Optional[float] = None,
        max_sl_pct: float = 0.15,
    ) -> Optional[Dict]:
        if option_df is None or option_df.empty:
            return None
        start = start_ts.replace(tzinfo=None) if start_ts.tzinfo else start_ts
        end = end_ts.replace(tzinfo=None) if end_ts.tzinfo else end_ts
        force_exit_time = datetime.strptime(
            str(getattr(get_settings(), "ut_force_exit_time", "15:25") or "15:25"),
            "%H:%M",
        ).time()
        session_end = datetime.combine(start.date(), force_exit_time)
        end = min(end, session_end)
        window = option_df.loc[(option_df.index >= start) & (option_df.index <= end)]
        if window.empty:
            return None
        entry_premium = float(window.iloc[0]["close"])
        if entry_premium <= 0:
            return None
        risk_from_price = entry_premium - float(sl_price)
        max_sl_dist = max(0.5, entry_premium * float(max_sl_pct or 0.15))
        if natural_sl_distance is not None and float(natural_sl_distance or 0) > 0:
            risk = min(float(natural_sl_distance), max_sl_dist)
        elif risk_from_price >= max(0.5, entry_premium * 0.01):
            risk = min(risk_from_price, max_sl_dist)
        else:
            # The pre-estimated stop can be stale once real option candles supply the entry.
            # Re-anchor to a meaningful premium risk before checking SL/target hits.
            risk = max_sl_dist
        risk = max(0.5, risk)
        sl_price = max(0.05, entry_premium - risk)
        target_price = entry_premium + (risk * rr)
        exit_price = float(window.iloc[-1]["close"])
        exit_time = window.index[-1]
        exit_reason = "HISTORICAL"
        for ts, row in window.iloc[1:].iterrows():
            low = float(row.get("low", row.get("close", 0.0)) or 0.0)
            high = float(row.get("high", row.get("close", 0.0)) or 0.0)
            op = float(row.get("open", row.get("close", 0.0)) or 0.0)
            
            if low <= sl_price and high >= target_price:
                dist_to_sl = abs(op - sl_price)
                dist_to_tgt = abs(op - target_price)
                if dist_to_sl < dist_to_tgt:
                    exit_price = float(sl_price)
                    exit_reason = "SL HIT"
                else:
                    exit_price = float(target_price)
                    exit_reason = "TARGET_HIT"
                exit_time = ts
                break
            elif low <= sl_price:
                exit_price = float(sl_price)
                exit_time = ts
                exit_reason = "SL HIT"
                break
            elif high >= target_price:
                exit_price = float(target_price)
                exit_time = ts
                exit_reason = "TARGET_HIT"
                break
        return {
            "entry": entry_premium,
            "exit": max(0.05, exit_price),
            "stop": sl_price,
            "target": target_price,
            "exit_time": exit_time,
            "reason": exit_reason,
        }
    def _select_historical_option_candidate(
        self,
        scanner,
        instrument: str,
        direction: str,
        price: float,
        trailing_stop: float,
        index_stop_distance: float,
        rr: float,
        cfg: Dict,
        start_ts: datetime,
        end_ts: datetime,
    ) -> Optional[Dict]:
        """
        Evaluate candidate option strikes (ATM and/or ITM) chronologically against intraday candles
        and return the selected option details.
        """
        from config.settings import get_settings
        interval = cfg.get("strike_interval", 50)
        atm_strike = round(float(price) / interval) * interval
        option_type = "CE" if direction == "LONG" else "PE"
        itm_strike = (atm_strike - interval) if option_type == "CE" else (atm_strike + interval)
        
        selection = getattr(get_settings(), "option_strike_selection", "ATM")
        target_strikes = []
        if selection == "ATM":
            target_strikes = [atm_strike]
        elif selection == "ITM":
            target_strikes = [itm_strike]
        else:  # BOTH
            # Evaluate both ATM and ITM. ITM is preferred/safer, so evaluate it first
            target_strikes = [itm_strike, atm_strike]
            
        for strike in target_strikes:
            option_df, trading_symbol, symbol_token = self._load_option_candles(
                scanner, instrument, strike, option_type, "1min", start_ts.date()
            )
            if option_df.empty:
                continue
                
            # Estimate entry/stop/target/exit using _simulate_actual_option_exit
            # Estimate entry premium (1.2% of spot)
            est_entry = _estimate_opt_premium(price, scanner)
            spot_stop_dist = abs(float(price) - float(trailing_stop))
            sim_mult = 0.5
            natural_sl_premium_dist = abs(spot_stop_dist * sim_mult)
            options_sl_pct = getattr(scanner, "options_sl_pct", 15.0)
            max_sl_dist = est_entry * (options_sl_pct / 100.0)
            effective_sl_dist = max(0.05, min(natural_sl_premium_dist, max_sl_dist))
            opt_sl_price = max(0.05, est_entry - effective_sl_dist)
            
            actual_exit = self._simulate_actual_option_exit(
                option_df=option_df,
                start_ts=start_ts,
                end_ts=end_ts,
                sl_price=opt_sl_price,
                rr=rr,
                natural_sl_distance=natural_sl_premium_dist,
                max_sl_pct=options_sl_pct / 100.0,
            )
            if actual_exit:
                return {
                    "strike": strike,
                    "trading_symbol": trading_symbol,
                    "symbol_token": symbol_token,
                    "option_df": option_df,
                    "entry": actual_exit["entry"],
                    "stop": actual_exit["stop"],
                    "exit": actual_exit["exit"],
                    "target": actual_exit["target"],
                    "exit_time": actual_exit["exit_time"],
                    "reason": actual_exit["reason"] if actual_exit["reason"] != "HISTORICAL" else "SESSION_END",
                }
        return None

    @staticmethod
    def _hybrid_backtest_dual_futures_enabled(scanner) -> bool:
        pref = str(getattr(scanner, "inst_pref", "AUTO") or "AUTO").upper()
        return SignalProcessor._historical_trade_creation_allowed(scanner) and pref in {"AUTO", "HYBRID"}

    @staticmethod
    def _historical_trade_creation_allowed(scanner) -> bool:
        guard = getattr(scanner, "allows_historical_trade_creation", None)
        if callable(guard):
            return bool(guard())
        return str(getattr(scanner, "mode", "") or "").upper() == "HISTORICAL"

    @staticmethod
    def _historical_signal_date_allowed(scanner, signal_ts) -> bool:
        if not SignalProcessor._historical_trade_creation_allowed(scanner):
            return False
        cutoff_getter = getattr(scanner, "historical_session_cutoff_date", None)
        if not callable(cutoff_getter) or signal_ts is None:
            return True
        try:
            return signal_ts.date() <= cutoff_getter()
        except Exception:
            return True

    def _build_historical_futures_shadow_trade(
        self,
        scanner,
        trade_id: str,
        instrument: str,
        timeframe: str,
        direction: str,
        signal,
        exit_spot: float,
        exit_time,
        grade: str,
        confidence: float,
        rr: float,
        lot_size: int,
        display_suffix: str,
        default_exit_reason: str,
    ) -> Optional[Trade]:
        entry = float(getattr(signal, "price", 0.0) or 0.0)
        if entry <= 0:
            return None
        trailing_stop = float(getattr(signal, "trailing_stop", entry) or entry)
        max_sl_dist = entry * (float(getattr(scanner, "futures_sl_pct", 0.30) or 0.30) / 100.0)
        if direction == "LONG":
            stop = max(trailing_stop, entry - max_sl_dist)
            target = entry + ((entry - stop) * rr)
            resolved_exit = float(exit_spot or entry)
            exit_reason = default_exit_reason
            if resolved_exit <= stop:
                resolved_exit = stop
                exit_reason = "SL HIT"
            gross_pnl = (resolved_exit - entry) * (int(getattr(scanner, "user_lots_fut", {}).get(instrument, 1) or 1) * int(lot_size or 1))
        else:
            stop = min(trailing_stop, entry + max_sl_dist)
            target = entry - ((stop - entry) * rr)
            resolved_exit = float(exit_spot or entry)
            exit_reason = default_exit_reason
            if resolved_exit >= stop:
                resolved_exit = stop
                exit_reason = "SL HIT"
            gross_pnl = (entry - resolved_exit) * (int(getattr(scanner, "user_lots_fut", {}).get(instrument, 1) or 1) * int(lot_size or 1))

        lots = int(getattr(scanner, "user_lots_fut", {}).get(instrument, 1) or 1)
        settings = get_settings()
        charges = estimate_trade_charges(
            entry,
            resolved_exit,
            lots * int(lot_size or 1),
            "FUT",
            settings,
        )
        entry_time = getattr(signal, "timestamp", None)
        e_time = IST.localize(entry_time) if (entry_time and getattr(entry_time, "tzinfo", None) is None) else entry_time
        ex_time = IST.localize(exit_time) if (exit_time and getattr(exit_time, "tzinfo", None) is None) else exit_time
        return Trade(
            id=trade_id,
            instrument=instrument,
            timeframe=timeframe,
            direction=direction,
            entry_price=entry,
            entry_time=e_time,
            trailing_stop=stop,
            current_stop=stop,
            lots=lots,
            lot_size=lot_size,
            grade=f"{grade} ({display_suffix}, Fut Shadow)",
            confidence=confidence,
            inst_type="FUT",
            option_type="",
            atm_strike=0.0,
            rr_ratio=rr,
            target=target,
            status="CLOSED",
            exit_price=resolved_exit,
            exit_time=ex_time,
            pnl=gross_pnl - charges,
            charges=charges,
            exit_reason=exit_reason,
            instrument_multiplier=1.0,
            entry_spot=entry,
            spot_stop=trailing_stop,
            spot_target=target,
            trading_symbol="FUT_SHADOW",
            symbol_token="",
        )

    def process_best_signal(
        self, instrument, mtf_result, intel_result, intel_score, regime,
        lots, lot_size, cfg, spot, atm_strike,
    ) -> List[TradeCandidate]:
        
        # Access Scanner properties via self.scanner
        scanner = self.scanner
        settings = get_settings()

        def naive_ts(value):
            if value is None:
                return datetime.min
            return value.replace(tzinfo=None) if getattr(value, "tzinfo", None) else value

        if not scanner.risk_manager.can_trade("ANY"):
            return []

        # Collect NEW signals from both signal TFs
        candidates_raw = []
        for tf in ["5min", "15min"]:
            sigs = [] # Master Fix: Clear sigs to prevent stale leak from previous iterations
            result = mtf_result.results_5min if tf == "5min" else mtf_result.results_15min
            if result is None:
                if tf == "15min": logger.debug(f"⚠️ No 15min results for {instrument} yet.")
                continue
            sigs = [s for s in result.get("signals", []) if s.instrument == instrument]
            logger.debug(f"🔍 [AUDIT-SIGS] {instrument} {tf}: found {len(sigs)} signals")
            if not sigs:
                continue
            dedup_key = f"{instrument}_{tf}"
            is_live = scanner.mode.upper() != "HISTORICAL"
            if dedup_key not in scanner._last_signal_time:
                # ═══ Mode-Aware Historical Backfill (Atomic Lock) ═══
                # Immediately seed to prevent concurrent backfill from other tasks
                scanner._last_signal_time[dedup_key] = naive_ts(datetime.now(IST))
                # Load last N days for Historical mode, or 1 day for Live modes (Warm Startup)
                if (scanner.mode == "HISTORICAL" or is_live):
                    # Fetch 5min candles to determine trading dates for lookback
                    candles_df = scanner.candles.get_candles(instrument, "5min")
                    if candles_df is None or candles_df.empty:
                        return []
                    
                    # Logic Fix: Data lookback vs Trade lookback
                    data_lookback = 1 if is_live else 45 # Live mode only replays the current session.
                    
                    # Trade lookback: The actual window the user wants to see/test
                    trade_lookback = 1 if is_live else scanner.backtest_days
                    
                    # ═══ Trading Day Awareness ═══
                    active_dates = sorted(list(set(candles_df.index.date)))
                    logger.debug(f"📅 Active dates in candles for {instrument}: {active_dates}")
                    if not active_dates: return []
                    
                    if is_live:
                        # Live/Paper mode replays the active trading session. After midnight but before
                        # 09:15 IST, that session is still the previous market day.
                        session_day_raw = (
                            scanner._get_current_session_day()
                            if hasattr(scanner, "_get_current_session_day")
                            else datetime.now(IST).date().isoformat()
                        )
                        session_day = datetime.fromisoformat(str(session_day_raw)).date()
                        trade_cutoff = session_day
                        data_cutoff = session_day
                    else:
                        # Trade Cutoff (for display/logging in closed_trades)
                        if trade_lookback <= 1 and hasattr(scanner, "_get_current_session_day"):
                            session_day = datetime.fromisoformat(str(scanner._get_current_session_day())).date()
                            session_dates = [d for d in active_dates if d <= session_day]
                            trade_cutoff = session_dates[-1] if session_dates else active_dates[-1]
                        else:
                            trade_cutoff = active_dates[-min(len(active_dates), trade_lookback)]
                        # Data Cutoff (for signal calculation)
                        data_cutoff = active_dates[-min(len(active_dates), data_lookback)]
                    
                    # Filter signals for this TF by data_cutoff and sort chronologically (oldest first)
                    def get_naive_ts(s):
                        return s.timestamp.replace(tzinfo=None) if s.timestamp.tzinfo else s.timestamp
                        
                    sigs = sorted([s for s in sigs if s.instrument == instrument and s.timestamp.date() >= data_cutoff], key=get_naive_ts)
                    
                    # Process signals for this TF
                    for i in range(len(sigs) - 1):
                        s1, s2 = sigs[i], sigs[i+1]
                        # Earlier rows are indicator warm-up only. They must not inflate
                        # diagnostics for the selected backtest window.
                        if s1.timestamp.date() < trade_cutoff:
                            continue
                        
                        # â”€â”€ Dynamic Regime-Aware Quality Gate â”€â”€
                        sub_df = candles_df[candles_df.index <= s1.timestamp]
                        hist_regime_res = scanner.intel.regime.detect(sub_df, instrument, "5min")
                        hist_regime = hist_regime_res.get("regime", "UNKNOWN")
                        
                        hist_vix = scanner._get_historical_vix(s1.timestamp)
                        hist_intel_score, hist_intel_regime = scanner._compute_historical_intel_at(
                            instrument,
                            s1.timestamp,
                            tf,
                        )
                        if hist_intel_regime and hist_intel_regime != "UNKNOWN":
                            hist_regime = hist_intel_regime
                        hist_confluence = scanner._compute_historical_confluence_at(
                            mtf_result,
                            s1.timestamp,
                        )

                        hist_grade, hist_conf, hist_reasons = scanner.signals._grade_signal(
                            s1,
                            hist_confluence,
                            hist_intel_score,
                            regime=hist_regime,
                            vix_value=hist_vix,
                        )
                        direction = "LONG" if s1.signal_type == "BUY" else "SHORT"

                        def record_reject(reason: str, inst_type: str = "FUT", strike: float = 0.0, option_type: str = "") -> None:
                            recorder = getattr(scanner, "_record_historical_reject", None)
                            if recorder:
                                recorder(s1, instrument, tf, direction, hist_grade, hist_conf, reason, inst_type, lot_size, strike, option_type)
                            emit_once = getattr(scanner, "log_signal_decision_once", None)
                            if callable(emit_once):
                                emit_once(
                                    f"hist|{instrument}|{tf}|{s1.signal_type}|{s1.timestamp.isoformat(timespec='seconds')}|{inst_type}|{reason}",
                                    (
                                        f"Signal rejected: {instrument} {s1.signal_type} {tf} "
                                        f"@ {float(getattr(s1, 'price', 0.0) or 0.0):.2f} - {reason}"
                                    ),
                                    "trade",
                                )
                        
                        logger.debug(f"ðŸ” [AUDIT] Signal {i} for {instrument} at {s1.timestamp}: Grade={hist_grade}, Conf={hist_conf:.2f}, Regime={hist_regime}, Type={s1.signal_type}")
                        
                        more_results = (
                            getattr(settings, "ut_backtest_more_results", True)
                            and scanner.mode == "HISTORICAL"
                        )

                        if hist_grade == "C" and not (more_results and hist_conf >= self._relaxed_threshold(0.62, settings) and not self._is_choppy_regime(hist_regime)):
                            record_reject("C grade blocked")
                            logger.info(f"â­ï¸ [AUDIT] Skipping C grade signal at {s1.timestamp}")
                            continue
                            
                        # â”€â”€ Determine Instrument Type via Hybrid Rules â”€â”€
                        direction = "LONG" if s1.signal_type == "BUY" else "SHORT"
                        
                        sim_inst = scanner._resolve_instrument_type(
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
                        diag_inc_unique = getattr(scanner, "_diag_inc_unique", None)
                        if diag_inc_unique:
                            diag_inc_unique(
                                "instrument_selection",
                                sim_inst,
                                "|".join([instrument, tf, str(s1.timestamp), str(s1.signal_type)]),
                            )
                        
                        grade_hierarchy = {"C": 0, "B": 1, "B+": 2, "A": 3, "A+": 4}
                        base_grade = hist_grade
                        sig_rank = grade_hierarchy.get(base_grade, 0)
                        
                        grade_pref = getattr(settings, "signal_grade_preference", "auto")
                        adx_val = getattr(s1, "adx_value", 0.0)
                        momentum_override = float(adx_val or 0.0) >= float(getattr(settings, "momentum_override_threshold", 25.0) or 25.0)
                        is_choppy = self._is_choppy_regime(hist_regime) and not momentum_override
                        
                        # 1. Block B/B+ in choppy markets entirely
                        if is_choppy and base_grade in ["B", "B+"] and not (more_results and (base_grade == "B+" or hist_conf >= self._relaxed_threshold(0.58, settings))):
                            record_reject("Choppy market blocked", sim_inst)
                            logger.info(f"⛔ Skipping choppy-blocked signal {s1.signal_type} {base_grade} at {s1.timestamp}")
                            continue
                            
                        # 2. Dynamic Confidence Filter
                        base_min_confidence = (0.50 if not is_choppy else 0.55) if more_results else (0.58 if not is_choppy else 0.65)
                        min_confidence = self._relaxed_threshold(base_min_confidence, settings)
                        if hist_conf < min_confidence:
                            record_reject(f"Confidence below {min_confidence:.0%}", sim_inst)
                            logger.info(f"â­ï¸ Skipping low-confidence signal {s1.signal_type} {base_grade} ({hist_conf} < {min_confidence}) at {s1.timestamp}")
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
                            record_reject(f"Grade rank below required {min_rank}", sim_inst)
                            logger.info(f"🚫 Skipping grade-preference-blocked signal {s1.signal_type} {base_grade} (rank {sig_rank} < min {min_rank}) at {s1.timestamp}")
                            continue
                            
                        # Aligned Quality Gates (mirror _passes_live_trade_ready_gate)
                        if not self._passes_quality_gate(
                            tf,
                            base_grade,
                            hist_conf,
                            hist_regime,
                            sim_inst,
                            settings,
                            getattr(s1, "adx_value", 0.0),
                            trend_15m_agrees=self._trend_15m_agrees_at(
                                mtf_result,
                                s1.timestamp,
                                s1.signal_type,
                            ),
                        ):
                            record_reject("Quality gate blocked", sim_inst)
                            logger.info(f"⭐ Skipping {instrument} {tf} signal due to quality gate at {s1.timestamp}")
                            continue
                            
                        if hist_regime == "UNKNOWN" and base_grade != "A+":
                            record_reject("UNKNOWN regime requires A+", sim_inst)
                            logger.info(f"⛔ Skipping {instrument} {tf} signal: UNKNOWN regime requires A+ at {s1.timestamp}")
                            continue
                            
                        # ── No-Entry After Gate ──
                        try:
                            no_entry_after, gate_time = _entry_cutoff(settings, tf)
                            s1_time = s1.timestamp.replace(tzinfo=None).time() if s1.timestamp.tzinfo else s1.timestamp.time()
                            if s1_time >= gate_time:
                                record_reject(f"After no-entry gate {no_entry_after}", sim_inst)
                                logger.info(f"🚫 [AUDIT] Skipping backfill entry at {s1.timestamp} due to ut_no_entry_after ({no_entry_after})")
                                continue
                        except Exception as e:
                            pass
                            
                        # Historical concurrency is applied after all workers finish.
                        if sim_inst == "OPT":
                            option_pref = (getattr(scanner, "inst_pref", "AUTO") or "AUTO").upper()
                            option_quality_ok = self._option_grade_allowed(
                                base_grade,
                                hist_conf,
                                hist_regime,
                                adx_value=getattr(s1, "adx_value", 0.0),
                                intel_score=hist_intel_score,
                                signal_score=hist_conf,
                            )
                            if not option_quality_ok:
                                if option_pref == "OPT":
                                    record_reject("Option premium quality gate blocked", sim_inst)
                                    logger.info(f"Skipping option signal below premium quality gate at {s1.timestamp}")
                                    continue
                                logger.info(
                                    f"Falling back to FUT for {instrument} {tf} at {s1.timestamp}: "
                                    f"option gate failed but AUTO/HYBRID should preserve the futures setup."
                                )
                                sim_inst = "FUT"

                        # Historical workers run concurrently. Filtering against the
                        # partially built shared trade list makes replay results depend
                        # on task completion order. TradeManager applies the configured
                        # concurrency policy deterministically after generation.
                        if s1.timestamp.date() >= trade_cutoff and s2.timestamp.date() >= trade_cutoff and s1.signal_type != s2.signal_type:
                            force_exit_time = datetime.strptime(
                                str(getattr(settings, "ut_force_exit_time", "15:25") or "15:25"),
                                "%H:%M",
                            ).time()
                            entry_ts = s1.timestamp.replace(tzinfo=None) if s1.timestamp.tzinfo else s1.timestamp
                            next_signal_ts = s2.timestamp.replace(tzinfo=None) if s2.timestamp.tzinfo else s2.timestamp
                            session_exit_ts = datetime.combine(entry_ts.date(), force_exit_time)
                            crosses_session = (
                                next_signal_ts.date() != entry_ts.date()
                                or next_signal_ts > session_exit_ts
                            )
                            effective_exit_ts = session_exit_ts if crosses_session else next_signal_ts

                            if sim_inst == "FUT":
                                sim_option_type = ""
                                sim_multiplier = 1.0
                                hist_strike = 0.0
                            else:
                                sim_option_type = "CE" if direction == "LONG" else "PE"
                                sim_multiplier = 0.5
                                hist_strike = self._historical_option_strike(instrument, direction, s1.price, cfg)

                            if sim_inst == "FUT":
                                hist_cap = scanner.capital_fut
                                hist_risk = scanner.risk_fut_pct
                            else:
                                hist_cap = scanner.capital_opt
                                hist_risk = scanner.risk_opt_pct
                            
                            risk_amount = hist_cap * (hist_risk / 100.0)
                            risk_amount = min(risk_amount, getattr(settings, "max_trade_loss_abs", 10000.0))
                            
                            index_stop_distance = s1.stop_distance
                            unit_risk = max(1.0, index_stop_distance) * sim_multiplier
                            
                            if sim_inst == "FUT":
                                hist_lots = getattr(scanner, "user_lots_fut", {}).get(instrument, 1)
                            else:
                                max_units = int(risk_amount / unit_risk)
                                user_target = scanner.user_lots.get(instrument, 1)
                                hist_lots = min(user_target, max(1, int(max_units / lot_size)))
                            
                            if hist_regime == "UNKNOWN":
                                hist_lots = max(1, hist_lots // 2)

                            qty = hist_lots * lot_size
                            
                            actual_option = None
                            trading_symbol = ""
                            symbol_token = ""
                            if sim_inst == "OPT":
                                option_df, trading_symbol, symbol_token = self._load_option_candles(
                                    scanner, instrument, hist_strike, sim_option_type, "1min", s1.timestamp.date()
                                )
                                if option_df.empty and getattr(settings, "ut_option_history_mode", "fetch_or_skip") == "fetch_or_skip":
                                    option_pref = (getattr(scanner, "inst_pref", "AUTO") or "AUTO").upper()
                                    if option_pref == "OPT":
                                        logger.info(f"Skipping {instrument} {sim_option_type} {hist_strike} at {s1.timestamp}: no real option candles.")
                                        continue
                                    logger.info(
                                        f"Falling back to FUT for {instrument} {tf} at {s1.timestamp}: "
                                        "no real option candles in AUTO/HYBRID."
                                    )
                                    diag = getattr(scanner, "_diag_inc", None)
                                    if diag:
                                        diag("option_history", "fallback_to_fut")
                                    sim_inst = "FUT"
                                    sim_option_type = ""
                                    sim_multiplier = 1.0
                                    hist_strike = 0.0
                                    hist_lots = getattr(scanner, "user_lots_fut", {}).get(instrument, 1)
                                    if hist_regime == "UNKNOWN":
                                        hist_lots = max(1, hist_lots // 2)
                                    qty = hist_lots * lot_size
                                    entry_premium = s1.price
                                else:
                                    entry_premium = _estimate_opt_premium(s1.price, scanner)
                            else:
                                entry_premium = s1.price
                            
                            if sim_inst == "FUT":
                                sl_price = s1.trailing_stop
                                max_sl_dist = entry_premium * (scanner.futures_sl_pct / 100.0)
                                if direction == "LONG":
                                    sl_price = max(sl_price, entry_premium - max_sl_dist)
                                else:
                                    sl_price = min(sl_price, entry_premium + max_sl_dist)
                                index_stop_distance = max(1.0, abs(entry_premium - sl_price))
                            else:
                                # Options are always long-premium instruments (CE/PE buy). Keep SL below entry.
                                # Use absolute spot stop distance mapped by multiplier, capped by option SL%.
                                spot_stop_dist = abs(float(s1.price) - float(s1.trailing_stop))
                                natural_sl_dist = abs(spot_stop_dist * sim_multiplier)
                                max_sl_dist = entry_premium * (scanner.options_sl_pct / 100.0)
                                effective_sl_dist = max(0.05, min(natural_sl_dist, max_sl_dist))
                                sl_price = max(0.05, entry_premium - effective_sl_dist)
                            
                            raw_exit_spot = s2.price
                            if crosses_session:
                                raw_exit_spot = s1.price
                                day_candles = candles_df[
                                    (candles_df.index.date == entry_ts.date())
                                    & (candles_df.index.time <= force_exit_time)
                                ]
                                if not day_candles.empty:
                                    raw_exit_spot = float(day_candles.iloc[-1]["close"])
                            
                            if sim_inst == "OPT":
                                natural_sl_distance = abs(index_stop_distance * sim_multiplier)
                                hist_rr = self._dynamic_rr(hist_grade, hist_conf)
                                actual_option = self._simulate_actual_option_exit(
                                    option_df if "option_df" in locals() else pd.DataFrame(),
                                    s1.timestamp,
                                    effective_exit_ts,
                                    sl_price,
                                    hist_rr,
                                    natural_sl_distance=natural_sl_distance,
                                    max_sl_pct=scanner.options_sl_pct / 100.0,
                                )
                                if actual_option:
                                    entry_premium = actual_option["entry"]
                                    sl_price = actual_option["stop"]
                                    raw_exit_premium = actual_option["exit"]
                                    exit_premium = raw_exit_premium
                                    exit_time = actual_option["exit_time"]
                                    is_sl_hit = actual_option["reason"] == "SL HIT"
                                else:
                                    spot_diff = raw_exit_spot - s1.price if direction == "LONG" else s1.price - raw_exit_spot
                                    raw_exit_premium = entry_premium + (spot_diff * sim_multiplier)
                                    
                                    time_held_hours = (effective_exit_ts - entry_ts).total_seconds() / 3600.0
                                    if time_held_hours > 1.0 and abs(spot_diff) < (s1.atr_value * 0.5):
                                        time_held_hours = 1.0
                                        spot_diff = 0
                                        raw_exit_spot = s1.price
                                        
                                    if time_held_hours > 0:
                                        dte = scanner.expiry.get_dte(instrument, s1.timestamp.date())
                                        # Allow 0 or 1 to exist to apply high expiry day decay if not rolled over
                                        actual_dte = max(0, dte)
                                        theta_rate = 0.05 if actual_dte == 0 else (0.02 if actual_dte == 1 else 0.005)
                                        theta_decay = entry_premium * theta_rate * time_held_hours
                                        if spot_diff > 0:
                                            theta_decay *= 0.5 
                                        raw_exit_premium -= theta_decay
                                        
                                    raw_exit_premium -= (entry_premium * 0.002)
                                    raw_exit_premium = max(0.05, raw_exit_premium)
                            else:
                                raw_exit_premium = raw_exit_spot
                            
                            is_sl_hit = bool(actual_option and actual_option["reason"] == "SL HIT")
                            exit_premium = raw_exit_premium
                            exit_time = actual_option["exit_time"] if actual_option else effective_exit_ts
                            
                            candles_1min = scanner.candles.get_candles(instrument, "1min")
                            
                            if actual_option is None and candles_1min is not None and not candles_1min.empty:
                                try:
                                    s1_ts = s1.timestamp.replace(tzinfo=None) if s1.timestamp.tzinfo else s1.timestamp
                                    between_candles = candles_1min.loc[s1_ts:effective_exit_ts]
                                    
                                    if not between_candles.empty:
                                        if sim_inst == "FUT":
                                            if direction == "LONG":
                                                hit_rows = between_candles[between_candles['low'] < sl_price]
                                                if not hit_rows.empty:
                                                    is_sl_hit = True
                                                    exit_premium = sl_price
                                                    exit_time = hit_rows.index[0]
                                            else:
                                                hit_rows = between_candles[between_candles['high'] > sl_price]
                                                if not hit_rows.empty:
                                                    is_sl_hit = True
                                                    exit_premium = sl_price
                                                    exit_time = hit_rows.index[0]
                                        else:
                                            if direction == "LONG":
                                                all_premiums = entry_premium + ((between_candles['low'] - s1.price) * sim_multiplier)
                                                hit_rows = between_candles[all_premiums < sl_price]
                                                if not hit_rows.empty:
                                                    is_sl_hit = True
                                                    exit_premium = sl_price
                                                    exit_time = hit_rows.index[0]
                                            else:
                                                all_premiums = entry_premium + ((s1.price - between_candles['high']) * sim_multiplier)
                                                hit_rows = between_candles[all_premiums < sl_price]
                                                if not hit_rows.empty:
                                                    is_sl_hit = True
                                                    exit_premium = sl_price
                                                    exit_time = hit_rows.index[0]
                                except Exception as e:
                                    logger.warning(f"⚠️ Granular SL check failed: {e}")
                                    
                            if not is_sl_hit:
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

                            if sim_inst == "OPT":
                                gross_pnl = (exit_premium - entry_premium) * qty
                            else:
                                if direction == "LONG":
                                    gross_pnl = (exit_premium - entry_premium) * qty
                                else:
                                    gross_pnl = (entry_premium - exit_premium) * qty
                            
                            sim_charges = estimate_trade_charges(
                                entry_premium,
                                exit_premium,
                                qty,
                                sim_inst,
                                settings,
                            )
                            net_pnl = gross_pnl - sim_charges
                            
                            if actual_option:
                                hist_exit_reason = actual_option["reason"]
                                if crosses_session and hist_exit_reason == "HISTORICAL":
                                    hist_exit_reason = "SESSION_END"
                            else:
                                hist_exit_reason = "SL HIT" if is_sl_hit else (
                                    "SESSION_END" if crosses_session else "HISTORICAL"
                                )
                            hist_rr = self._dynamic_rr(hist_grade, hist_conf)
                            hist_id = f"H_{instrument}_{tf}_{s1.timestamp.strftime('%Y%m%d%H%M%S')}"

                            if not self._historical_signal_date_allowed(scanner, s1.timestamp):
                                logger.info(
                                    f"Skipping in-progress session historical trade before 15:30 handoff: {hist_id}"
                                )
                                continue
                            
                            if any(ct.id == hist_id for ct in scanner.trades.closed_trades):
                                continue
                                
                            if entry_premium <= 0:
                                logger.warning(f"Skipping simulated candidate {hist_id} due to invalid entry premium: {entry_premium}")
                                continue
                                
                            if sim_inst == "OPT":
                                hist_target = actual_option["target"] if actual_option else entry_premium + ((entry_premium - sl_price) * hist_rr)
                            elif direction == "LONG":
                                hist_target = s1.price + (index_stop_distance * hist_rr)
                            else:
                                hist_target = s1.price - (index_stop_distance * hist_rr)
                            
                            e_time = IST.localize(s1.timestamp) if (s1.timestamp and getattr(s1.timestamp, "tzinfo", None) is None) else s1.timestamp
                            ex_time = IST.localize(exit_time) if (exit_time and getattr(exit_time, "tzinfo", None) is None) else exit_time
                            t = Trade(
                                id=hist_id, instrument=instrument, timeframe=tf,
                                direction=direction, entry_price=entry_premium, entry_time=e_time,
                                trailing_stop=sl_price,
                                current_stop=sl_price,
                                lots=hist_lots, lot_size=lot_size, grade=f"{hist_grade} (Hist)", confidence=hist_conf,
                                inst_type=sim_inst, option_type=sim_option_type, atm_strike=hist_strike,
                                rr_ratio=hist_rr, target=hist_target,
                                status="CLOSED", exit_price=exit_premium, exit_time=ex_time,
                                pnl=net_pnl, charges=sim_charges, exit_reason=hist_exit_reason,
                                instrument_multiplier=sim_multiplier, entry_spot=s1.price,
                                spot_stop=s1.trailing_stop,
                                spot_target=(s1.price + (index_stop_distance * hist_rr)) if direction == "LONG" else (s1.price - (index_stop_distance * hist_rr)),
                                trading_symbol=trading_symbol, symbol_token=symbol_token,
                            )
                            
                            live_session_day_raw = (
                                scanner._get_current_session_day()
                                if hasattr(scanner, "_get_current_session_day")
                                else datetime.now(IST).date().isoformat()
                            )
                            live_session_day = datetime.fromisoformat(str(live_session_day_raw)).date()
                            if is_live and s1.timestamp.date() == live_session_day:
                                logger.info(
                                    f"Skipping simulated backfill signal {hist_id} in live mode; "
                                    "live dashboard ledger only accepts real post-gate session rows."
                                )
                                continue
                            if is_live and s1.timestamp.date() == live_session_day:
                                exit_candidate = TradeCandidate(
                                    instrument=instrument,
                                    direction=direction,
                                    price=entry_premium,
                                    stop=sl_price,
                                    target=hist_target,
                                    lots=hist_lots,
                                    lot_size=lot_size,
                                    grade=hist_grade,
                                    confidence=hist_conf,
                                    timeframe=tf,
                                    inst_type=sim_inst,
                                    option_type=sim_option_type,
                                    atm_strike=hist_strike,
                                    multiplier=sim_multiplier,
                                    trading_symbol=trading_symbol,
                                    symbol_token=symbol_token,
                                    rr=hist_rr,
                                    score=hist_conf,
                                    reasons=list(hist_reasons or []) + [f"Signal closed by {hist_exit_reason}"],
                                    spot_stop=s1.trailing_stop,
                                    spot_target=(s1.price + (index_stop_distance * hist_rr)) if direction == "LONG" else (s1.price - (index_stop_distance * hist_rr)),
                                    signal_timestamp=s1.timestamp,
                                    current_price=exit_premium,
                                    pnl=net_pnl,
                                    status="EXIT SIGNAL",
                                    action="EXIT",
                                    exit_reason=hist_exit_reason,
                                )
                                setattr(exit_candidate, "exit_timestamp", exit_time)
                                setattr(exit_candidate, "exit_price", exit_premium)
                                scanner._remember_trade_candidates([exit_candidate])
                                logger.info(f"⛔ Skipping adding historical trade {hist_id} to closed_trades in live mode.")
                            elif self._historical_trade_creation_allowed(scanner):
                                scanner.trades.closed_trades.append(t)
                                if sim_inst == "OPT" and self._hybrid_backtest_dual_futures_enabled(scanner):
                                    shadow_id = f"{hist_id}_FUT"
                                    if not any(ct.id == shadow_id for ct in scanner.trades.closed_trades):
                                        shadow = self._build_historical_futures_shadow_trade(
                                            scanner=scanner,
                                            trade_id=shadow_id,
                                            instrument=instrument,
                                            timeframe=tf,
                                            direction=direction,
                                            signal=s1,
                                            exit_spot=raw_exit_spot,
                                            exit_time=exit_time,
                                            grade=hist_grade,
                                            confidence=hist_conf,
                                            rr=hist_rr,
                                            lot_size=lot_size,
                                            display_suffix="Hist",
                                            default_exit_reason=("SL HIT" if is_sl_hit else "HISTORICAL"),
                                        )
                                        if shadow:
                                            scanner.trades.closed_trades.append(shadow)
                            else:
                                logger.info(
                                    f"Blocked historical trade creation outside HISTORICAL mode: {hist_id}"
                                )

                # CRITICAL FIX: Only recover signals from TODAY's session.
                    # A signal from a previous day (e.g. 07 May 14:30) would have been 
                    # squared off at session end (15:30). It is NOT a live position.
                    # Master Fix: We DO want to recover trades if the bot crashes during a live/paper session!
                    allow_inferred_recovery = getattr(
                        scanner,
                        "_allow_inferred_signal_recovery",
                        lambda: False,
                    )
                    if is_live and sigs and bool(allow_inferred_recovery()):
                        logger.info(f"Running live recovery for {instrument} {tf}...")
                        last_s = sigs[-1]
                        session_day_raw = (
                            scanner._get_current_session_day()
                            if hasattr(scanner, "_get_current_session_day")
                            else datetime.now(IST).date().isoformat()
                        )
                        session_day = datetime.fromisoformat(str(session_day_raw)).date()
                        try:
                            now_ist = datetime.now(IST)
                            sess_end_str = get_settings().ut_session_end
                            sess_hr, sess_min = map(int, sess_end_str.split(':'))
                            kill_threshold = dtime(sess_hr, sess_min)
                            
                            market_session_active = (
                                now_ist.date() == session_day
                                and now_ist.time() < kill_threshold
                                and bool(scanner.data.is_market_open())
                            )
                        except Exception:
                            now_ist = datetime.now(IST)
                            sess_end_str = get_settings().ut_session_end
                            sess_hr, sess_min = map(int, sess_end_str.split(':'))
                            market_session_active = now_ist.date() == session_day and now_ist.time() < dtime(sess_hr, sess_min)
                        signal_is_from_today = (
                            (scanner.mode == "REAL")
                            and last_s.timestamp.date() == session_day
                            and market_session_active
                        )
                        
                        tf_minutes = {"1min": 1, "5min": 5, "15min": 15}.get(tf, 5)
                        candle_close_time = naive_ts(last_s.timestamp) + timedelta(minutes=tf_minutes)
                        now_naive = naive_ts(datetime.now(IST))
                        
                        recovery_window_ok = True
                        recovery_window_fn = getattr(scanner, "_signal_in_restart_recovery_window", None)
                        if callable(recovery_window_fn):
                            recovery_window_ok = bool(recovery_window_fn(last_s.timestamp, tf))

                        if signal_is_from_today and now_naive >= candle_close_time and recovery_window_ok:
                            direction = "LONG" if last_s.signal_type == "BUY" else "SHORT"
                            logger.info(f"🔄 Recovering Active {direction} state for {instrument} from {last_s.timestamp}")
                            if not scanner.is_warmup:
                                scanner.log_event(f"🔄 Recovered {direction} signal for {instrument} from {last_s.timestamp.strftime('%H:%M')}", "trade")
                            
                            # ── Recovery Cross-TF Guard ──
                            existing = [t for t in scanner.trades.open_trades.values() if t.instrument == instrument]
                            
                            # Check if signal was already processed and closed
                            existing_closed = [
                                t for t in scanner.trades.closed_trades 
                                if t.instrument == instrument and 
                                getattr(t, 'entry_time', None) and
                                (t.entry_time.strftime('%Y%m%d%H%M') == last_s.timestamp.strftime('%Y%m%d%H%M') if isinstance(t.entry_time, datetime) else str(t.entry_time) == last_s.timestamp.strftime('%d %b %H:%M:%S'))
                                and "REPAINT" not in str(getattr(t, "exit_reason", "")).upper()
                            ]
                            if existing_closed:
                                logger.info(f"⛔ Signal for {instrument} at {last_s.timestamp} already processed and closed. Skipping recovery.")
                                continue
                            
                            logger.info(f"🔍 [AUDIT] Recovery logic running for {instrument} {tf}. last_s={last_s.timestamp} {last_s.signal_type}. scanner.inst_pref: {scanner.inst_pref}")
                            
                            # Data Sanity Check: Detect swapped prices!
                            if instrument == "SENSEX" and last_s.price < 50000:
                                logger.error(f"🚨 [AUDIT] SENSEX signal has NIFTY price: {last_s.price}. Skipping recovery.")
                                continue
                            if instrument == "NIFTY" and last_s.price > 50000:
                                logger.error(f"🚨 [AUDIT] NIFTY signal has SENSEX price: {last_s.price}. Skipping recovery.")
                                continue

                            # Determine recovery instrument type based on AUTO/FUT/OPT preference
                            # In AUTO mode, Grade A/A+ signals are forced to Options
                            rec_inst_type = "FUT"


                            if scanner.inst_pref == "AUTO":
                                rec_inst_type = "OPT" # Assume high grade for recovery unless specified
                            elif scanner.inst_pref == "OPT":
                                rec_inst_type = "OPT"
                                
                            if existing:
                                logger.info(f"🚷 Recovery Guard: Already have an open trade for {instrument}. Skipping recovery.")
                                continue

                            
                            try:
                                live_regime = regime
                            except Exception:
                                live_regime = "UNKNOWN"
                            try:
                                live_confluence = float(getattr(mtf_result, "confluence_score", 0.5) or 0.5)
                            except Exception:
                                live_confluence = 0.5
                            try:
                                live_intel_score = float(intel_score or 0.0)
                            except Exception:
                                live_intel_score = 0.0
                            try:
                                rec_vix_key = last_s.timestamp.strftime("%Y-%m-%d %H:%M:00")
                                rec_vix = scanner._vix_data.get(rec_vix_key)
                                if rec_vix is None:
                                    rec_vix = scanner.data.get_ltp("NSE", "INDIAVIX", "99926017") or 15.0
                            except Exception:
                                rec_vix = 15.0
                            try:
                                rec_grade, rec_conf, rec_reasons = scanner.signals._grade_signal(
                                    last_s,
                                    live_confluence,
                                    live_intel_score,
                                    live_regime,
                                    vix_value=rec_vix,
                                )
                            except Exception as exc:
                                logger.warning(f"Recovery quality grading failed for {instrument} {tf}: {exc}")
                                rec_grade, rec_conf, rec_reasons = "C", 0.0, ["Recovery grading failed"]

                            if not self._passes_quality_gate(
                                tf,
                                rec_grade,
                                rec_conf,
                                live_regime,
                                rec_inst_type,
                                get_settings(),
                                adx_value=float(getattr(last_s, "adx_value", 0.0) or 0.0),
                                trend_15m_agrees=self._trend_15m_agrees_at(
                                    mtf_result,
                                    last_s.timestamp,
                                    last_s.signal_type,
                                ),
                            ):
                                logger.info(
                                    f"Recovery Guard: {instrument} {tf} {last_s.signal_type} at {last_s.timestamp} "
                                    f"rejected by live quality gate (grade={rec_grade}, conf={rec_conf:.3f}, regime={live_regime})."
                                )
                                continue

                            # Fail-safe: Respect instrument preference!
                            if scanner.inst_pref == "FUT" and rec_inst_type == "OPT":
                                logger.warning(f"⚠️ Recovery Guard: Forced to OPT but preference is FUT. Overriding to FUT.")
                                rec_inst_type = "FUT"

                            # Calculate atm_strike for recovery if it's an Option
                            rec_atm_strike = 0.0
                            if rec_inst_type == "OPT":

                                strike_interval = cfg.get("strike_interval", 50)
                                rec_atm_strike = round(last_s.price / strike_interval) * strike_interval

                            rec_lots = getattr(scanner, "user_lots_fut", {}).get(instrument, 1) if rec_inst_type == "FUT" else getattr(scanner, "user_lots", {}).get(instrument, 1)
                            rec_entry = _estimate_opt_premium(last_s.price, scanner) if rec_inst_type == "OPT" else last_s.price
                            if rec_inst_type == "OPT":
                                rec_risk = max(0.5, rec_entry * (scanner.options_sl_pct / 100.0))
                                rec_stop = max(0.05, rec_entry - rec_risk)
                                rec_target = rec_entry + (rec_risk * 1.5)
                            else:
                                rec_risk = max(1.0, rec_entry * (scanner.futures_sl_pct / 100.0))
                                if direction == "LONG":
                                    rec_stop = max(last_s.trailing_stop, rec_entry - rec_risk)
                                    rec_target = rec_entry + ((rec_entry - rec_stop) * 1.5)
                                else:
                                    rec_stop = min(last_s.trailing_stop, rec_entry + rec_risk)
                                    rec_target = rec_entry - ((rec_stop - rec_entry) * 1.5)
                            scanner.trades.open_trade(
                                instrument=instrument, timeframe=tf, direction=direction,
                                price=rec_entry,
                                trailing_stop=rec_stop,
                                target=rec_target,
                                lots=rec_lots, lot_size=lot_size, grade=rec_grade,
                                confidence=rec_conf,
                                entry_time=last_s.timestamp,
                                instrument_multiplier=0.5 if rec_inst_type == "OPT" else 1.0,
                                option_type=("CE" if direction == "LONG" else "PE") if rec_inst_type == "OPT" else "",
                                atm_strike=rec_atm_strike,
                                trading_symbol=last_s.trading_symbol if hasattr(last_s, 'trading_symbol') else "",
                                symbol_token=last_s.symbol_token if hasattr(last_s, 'symbol_token') else "",
                                inst_type=rec_inst_type,
                                entry_spot=last_s.price, 
                                is_explosive_bypass=bool(
                                    float(getattr(last_s, "adx_value", 0.0) or 0.0)
                                    >= float(getattr(settings, "momentum_override_threshold", 25.0) or 25.0)
                                ),
                                is_recovery=True
                            )
                            if scanner.trades.open_trades:
                                active_tid = list(scanner.trades.open_trades.keys())[-1]
                                active_trade = scanner.trades.open_trades[active_tid]
                                ts = pd.Timestamp(last_s.timestamp)
                                if ts.tzinfo is not None:
                                    active_trade.entry_time = ts.tz_convert(IST).tz_localize(None).to_pydatetime()
                                else:
                                    active_trade.entry_time = ts.to_pydatetime()
                                
                                # ══ Recovery Initialization (Institutional Calibration) ══
                                # For recovered options, we must estimate synthetic state
                                if rec_inst_type == "OPT":
                                    # Entry premium estimate (1.2% of spot)
                                    active_trade.entry_price = _estimate_opt_premium(last_s.price, scanner)
                                    active_trade.current_price = active_trade.entry_price
                                    # Signed delta for synthetic moves
                                    active_trade.instrument_multiplier = 0.5 if direction == "LONG" else -0.5
                                    active_trade.current_stop = rec_stop
                                    active_trade.target = rec_target
                                else:
                                    active_trade.entry_price = last_s.price
                                    active_trade.current_price = last_s.price
                                    active_trade.current_stop = rec_stop
                                    active_trade.target = rec_target
                        elif last_s.timestamp.date() >= trade_cutoff:
                            # ═══ SYNTHETIC SESSION-END CLOSE ═══
                            # Signal from a previous day — create a closed trade at session end price
                            
                            # ── Grade Filter: Evaluate what we would trade ──
                            eod_regime = "UNKNOWN"
                            try:
                                eod_df = scanner.candles.get_candles(instrument, "5min")
                                if eod_df is not None and not eod_df.empty:
                                    eod_regime_res = scanner.intel.regime.detect(
                                        eod_df[eod_df.index <= last_s.timestamp],
                                        instrument,
                                        "5min",
                                    )
                                    eod_regime = eod_regime_res.get("regime", "UNKNOWN")
                            except Exception as exc:
                                logger.debug(f"EOD option regime fallback for {instrument}: {exc}")

                            eod_grade, eod_conf, _ = scanner.signals._grade_signal(last_s, 0.5, 0.0, regime=eod_regime)
                            if eod_grade == "C":
                                continue
                                
                            # Historical concurrency is applied after all workers finish.
                            # Check for existing manual exits before applying EOD close
                            already_exited = False
                            book = getattr(scanner, "_session_trade_candidates", {}).get(instrument, {})
                            for c in book.values():
                                if getattr(c, "action", "ENTRY") == "EXIT" and c.timeframe == tf and c.direction == ("LONG" if last_s.signal_type == "BUY" else "SHORT"):
                                    if getattr(c, "signal_timestamp", None) == last_s.timestamp:
                                        already_exited = True
                                        break
                            if already_exited:
                                continue

                            direction = "LONG" if last_s.signal_type == "BUY" else "SHORT"
                            sig_date = last_s.timestamp.date()
                            
                            # Find the closing price on that date from candle data
                            candles_for_tf = scanner.candles.get_candles(instrument, tf)
                            eod_price = last_s.price  # fallback
                            if candles_for_tf is not None and not candles_for_tf.empty:
                                day_candles = candles_for_tf[candles_for_tf.index.date == sig_date]
                                force_exit_time = datetime.strptime(
                                    str(getattr(settings, "ut_force_exit_time", "15:25") or "15:25"),
                                    "%H:%M",
                                ).time()
                                day_candles = day_candles[day_candles.index.time <= force_exit_time]
                                if not day_candles.empty:
                                    eod_price = day_candles['close'].iloc[-1]
                            
                            eod_id = f"EOD_{instrument}_{tf}_{last_s.timestamp.strftime('%m%d%H%M')}"
                            if not self._historical_signal_date_allowed(scanner, last_s.timestamp):
                                logger.info(
                                    f"Skipping in-progress session historical EOD trade before 15:30 handoff: {eod_id}"
                                )
                                continue
                            if not any(ct.id == eod_id for ct in scanner.trades.closed_trades):
                                eod_exit_time = datetime.combine(sig_date, force_exit_time)
                                e_time = IST.localize(last_s.timestamp) if last_s.timestamp.tzinfo is None else last_s.timestamp
                                ex_time = IST.localize(eod_exit_time) if eod_exit_time.tzinfo is None else eod_exit_time
                                
                                
                                # ═══ EOD Mode-Aware Intelligence ═══
                                if scanner.inst_pref == "FUT":
                                    sim_inst = "FUT"
                                    sim_option_type = ""
                                    sim_multiplier = 1.0
                                    entry_premium = last_s.price
                                    exit_premium = eod_price
                                elif scanner.inst_pref == "OPT":
                                    sim_inst = "OPT"
                                    sim_option_type = "CE" if direction == "LONG" else "PE"
                                    sim_multiplier = 0.5
                                    entry_premium = _estimate_opt_premium(last_s.price, scanner)
                                    spot_diff = eod_price - last_s.price if direction == "LONG" else last_s.price - eod_price
                                    exit_premium = entry_premium + (spot_diff * sim_multiplier)
                                else: # AUTO
                                    eod_option_allowed = self._option_grade_allowed(
                                        eod_grade,
                                        eod_conf,
                                        eod_regime,
                                        adx_value=getattr(last_s, "adx_value", 0.0),
                                        intel_score=intel_score,
                                        signal_score=eod_conf,
                                    )
                                    sim_inst = "OPT" if eod_option_allowed else "FUT"
                                    sim_option_type = ("CE" if direction == "LONG" else "PE") if sim_inst == "OPT" else ""
                                    sim_multiplier = 0.5 if sim_inst == "OPT" else 1.0
                                    if sim_inst == "OPT":
                                        entry_premium = _estimate_opt_premium(last_s.price, scanner)
                                        spot_diff = eod_price - last_s.price if direction == "LONG" else last_s.price - eod_price
                                        exit_premium = entry_premium + (spot_diff * sim_multiplier)
                                    else:
                                        entry_premium = last_s.price
                                        exit_premium = eod_price

                                sim_atm_strike = 0.0
                                trading_symbol = ""
                                symbol_token = ""
                                eod_exit_reason = "SESSION_END"
                                eod_stop = last_s.trailing_stop
                                if sim_inst == "FUT":
                                    max_fut_sl_dist = entry_premium * (scanner.futures_sl_pct / 100.0)
                                    if direction == "LONG":
                                        eod_stop = max(last_s.trailing_stop, entry_premium - max_fut_sl_dist)
                                        eod_target = entry_premium + ((entry_premium - eod_stop) * 1.5)
                                    else:
                                        eod_stop = min(last_s.trailing_stop, entry_premium + max_fut_sl_dist)
                                        eod_target = entry_premium - ((eod_stop - entry_premium) * 1.5)
                                else:
                                    eod_target = last_s.price + (abs(last_s.price - last_s.trailing_stop) * 1.5) if direction == "LONG" else last_s.price - (abs(last_s.price - last_s.trailing_stop) * 1.5)

                                if sim_inst == "OPT":
                                    option_pref = (getattr(scanner, "inst_pref", "AUTO") or "AUTO").upper()
                                    _, no_entry_after = _entry_cutoff(settings, tf)
                                    eod_option_quality_ok = (
                                        self._option_grade_allowed(
                                            eod_grade,
                                            eod_conf,
                                            eod_regime,
                                            adx_value=getattr(last_s, "adx_value", 0.0),
                                            intel_score=intel_score,
                                            signal_score=eod_conf,
                                        )
                                        and last_s.timestamp.time() <= no_entry_after
                                    )
                                    if not eod_option_quality_ok:
                                        if option_pref == "OPT":
                                            logger.info(f"Skipping EOD option below premium quality gate at {last_s.timestamp}")
                                            continue
                                        logger.info(
                                            f"Falling back to FUT for EOD {instrument} {tf} at {last_s.timestamp}: "
                                            f"option gate failed but AUTO/HYBRID should preserve the futures setup."
                                        )
                                        sim_inst = "FUT"
                                        sim_option_type = ""
                                        sim_multiplier = 1.0
                                        entry_premium = last_s.price
                                        exit_premium = eod_price
                                        max_fut_sl_dist = entry_premium * (scanner.futures_sl_pct / 100.0)
                                        if direction == "LONG":
                                            eod_stop = max(last_s.trailing_stop, entry_premium - max_fut_sl_dist)
                                            eod_target = entry_premium + ((entry_premium - eod_stop) * 1.5)
                                        else:
                                            eod_stop = min(last_s.trailing_stop, entry_premium + max_fut_sl_dist)
                                            eod_target = entry_premium - ((eod_stop - entry_premium) * 1.5)

                                if sim_inst == "OPT":
                                    sim_atm_strike = self._historical_option_strike(instrument, direction, last_s.price, cfg)
                                    option_df, trading_symbol, symbol_token = self._load_option_candles(
                                        scanner, instrument, sim_atm_strike, sim_option_type, "1min", sig_date
                                    )
                                    history_mode = getattr(settings, "ut_option_history_mode", "fetch_or_skip")
                                    if option_df.empty and history_mode == "fetch_or_skip":
                                        option_pref = (getattr(scanner, "inst_pref", "AUTO") or "AUTO").upper()
                                        if option_pref == "OPT":
                                            logger.info(f"Skipping EOD option {instrument} {sim_option_type} {sim_atm_strike} at {last_s.timestamp}: no real option candles.")
                                            continue
                                        logger.info(
                                            f"Falling back to FUT for EOD {instrument} {tf} at {last_s.timestamp}: "
                                            "no real option candles in AUTO/HYBRID."
                                        )
                                        sim_inst = "FUT"
                                        sim_option_type = ""
                                        sim_multiplier = 1.0
                                        entry_premium = last_s.price
                                        exit_premium = eod_price
                                        max_fut_sl_dist = entry_premium * (scanner.futures_sl_pct / 100.0)
                                        if direction == "LONG":
                                            eod_stop = max(last_s.trailing_stop, entry_premium - max_fut_sl_dist)
                                            eod_target = entry_premium + ((entry_premium - eod_stop) * 1.5)
                                        else:
                                            eod_stop = min(last_s.trailing_stop, entry_premium + max_fut_sl_dist)
                                            eod_target = entry_premium - ((eod_stop - entry_premium) * 1.5)

                                    opt_sl_price = max(
                                        0.05,
                                        entry_premium - max(0.05, entry_premium * (scanner.options_sl_pct / 100.0)),
                                    )
                                    if sim_inst == "OPT" and not option_df.empty:
                                        est_entry = _estimate_opt_premium(last_s.price, scanner)
                                        spot_stop_dist = abs(float(last_s.price) - float(last_s.trailing_stop))
                                        natural_sl_premium_dist = abs(spot_stop_dist * sim_multiplier)
                                        max_sl_dist = est_entry * (scanner.options_sl_pct / 100.0)
                                        effective_sl_dist = max(0.05, min(natural_sl_premium_dist, max_sl_dist))
                                        opt_sl_price = max(0.05, est_entry - effective_sl_dist)
                                        actual_eod = self._simulate_actual_option_exit(
                                            option_df,
                                            last_s.timestamp,
                                            eod_exit_time,
                                            opt_sl_price,
                                            1.5,
                                            natural_sl_distance=natural_sl_premium_dist,
                                            max_sl_pct=scanner.options_sl_pct / 100.0,
                                        )
                                        if actual_eod:
                                            entry_premium = actual_eod["entry"]
                                            opt_sl_price = actual_eod["stop"]
                                            exit_premium = actual_eod["exit"]
                                            eod_target = actual_eod["target"]
                                            eod_exit_time = actual_eod["exit_time"]
                                            ex_time = IST.localize(eod_exit_time) if (eod_exit_time and getattr(eod_exit_time, "tzinfo", None) is None) else eod_exit_time
                                            eod_exit_reason = actual_eod["reason"] if actual_eod["reason"] != "HISTORICAL" else "SESSION_END"
                                        else:
                                            fallback_risk = max(0.5, est_entry - opt_sl_price)
                                            eod_target = est_entry + (fallback_risk * 1.5)
                                    else:
                                        fallback_risk = max(0.5, entry_premium - opt_sl_price)
                                        eod_target = entry_premium + (fallback_risk * 1.5)

                                # Check quality gate
                                if not self._passes_quality_gate(
                                    tf,
                                    eod_grade,
                                    eod_conf,
                                    eod_regime,
                                    sim_inst,
                                    settings,
                                    trend_15m_agrees=self._trend_15m_agrees_at(
                                        mtf_result,
                                        last_s.timestamp,
                                        last_s.signal_type,
                                    ),
                                ):
                                    logger.info(f"⭐️ Skipping EOD {instrument} {tf} signal due to quality gate at {last_s.timestamp}")
                                    continue

                                eod_lots = getattr(scanner, "user_lots_fut", {}).get(instrument, 1) if sim_inst == "FUT" else getattr(scanner, "user_lots", {}).get(instrument, 1)
                                sim_charges = estimate_trade_charges(
                                    last_s.price if sim_inst == "FUT" else entry_premium,
                                    exit_premium,
                                    eod_lots * lot_size,
                                    sim_inst,
                                    settings,
                                )
                                if sim_inst == "FUT":
                                    if direction == "LONG" and eod_price <= eod_stop:
                                        exit_premium = eod_stop
                                        eod_exit_reason = "SL HIT"
                                    elif direction == "SHORT" and eod_price >= eod_stop:
                                        exit_premium = eod_stop
                                        eod_exit_reason = "SL HIT"
                                    if direction == "LONG":
                                        eod_pnl = (exit_premium - last_s.price) * (eod_lots * lot_size)
                                    else:
                                        eod_pnl = (last_s.price - exit_premium) * (eod_lots * lot_size)
                                    eod_pnl -= sim_charges
                                else:
                                    eod_pnl = (exit_premium - entry_premium) * (eod_lots * lot_size)
                                    eod_pnl -= sim_charges

                                t = Trade(
                                    id=eod_id, instrument=instrument, timeframe=tf,
                                    direction=direction, entry_price=entry_premium, entry_time=e_time,
                                    trailing_stop=(opt_sl_price if sim_inst == "OPT" else eod_stop),
                                    current_stop=(opt_sl_price if sim_inst == "OPT" else eod_stop),
                                    lots=eod_lots, lot_size=lot_size, grade=f"{eod_grade} (EOD, Hist)", confidence=eod_conf,
                                    inst_type=sim_inst, option_type=sim_option_type, rr_ratio=1.5,
                                    target=eod_target,
                                    status="CLOSED", exit_price=exit_premium, exit_time=ex_time,
                                    pnl=eod_pnl, charges=sim_charges, exit_reason=eod_exit_reason,
                                    instrument_multiplier=sim_multiplier, entry_spot=last_s.price,
                                    spot_stop=last_s.trailing_stop,
                                    spot_target=last_s.price + (abs(last_s.price - last_s.trailing_stop) * 1.5) if direction == "LONG" else last_s.price - (abs(last_s.price - last_s.trailing_stop) * 1.5),
                                    atm_strike=sim_atm_strike,
                                    trading_symbol=trading_symbol, symbol_token=symbol_token,
                                )
                                live_session_day_raw = (
                                    scanner._get_current_session_day()
                                    if hasattr(scanner, "_get_current_session_day")
                                    else datetime.now(IST).date().isoformat()
                                )
                                live_session_day = datetime.fromisoformat(str(live_session_day_raw)).date()
                                if is_live and last_s.timestamp.date() == live_session_day:
                                    logger.info(
                                        f"Skipping simulated EOD backfill signal {eod_id} in live mode; "
                                        "live dashboard ledger only accepts real post-gate session rows."
                                    )
                                    continue
                                if is_live and last_s.timestamp.date() == live_session_day:
                                    exit_candidate = TradeCandidate(
                                        instrument=instrument,
                                        direction=direction,
                                        price=entry_premium,
                                        stop=(opt_sl_price if sim_inst == "OPT" else eod_stop),
                                        target=eod_target,
                                        lots=eod_lots,
                                        lot_size=lot_size,
                                        grade=eod_grade,
                                        confidence=eod_conf,
                                        timeframe=tf,
                                        inst_type=sim_inst,
                                        option_type=sim_option_type,
                                        atm_strike=sim_atm_strike,
                                        multiplier=sim_multiplier,
                                        trading_symbol=trading_symbol,
                                        symbol_token=symbol_token,
                                        rr=1.5,
                                        score=eod_conf,
                                        reasons=[f"Signal closed by {eod_exit_reason}"],
                                        spot_stop=last_s.trailing_stop,
                                        spot_target=(
                                            last_s.price + (abs(last_s.price - last_s.trailing_stop) * 1.5)
                                            if direction == "LONG"
                                            else last_s.price - (abs(last_s.price - last_s.trailing_stop) * 1.5)
                                        ),
                                        signal_timestamp=last_s.timestamp,
                                        current_price=exit_premium,
                                        pnl=eod_pnl,
                                        status="EXIT SIGNAL",
                                        action="EXIT",
                                        exit_reason=eod_exit_reason,
                                    )
                                    setattr(exit_candidate, "exit_timestamp", eod_exit_time)
                                    setattr(exit_candidate, "exit_price", exit_premium)
                                    scanner._remember_trade_candidates([exit_candidate])
                                elif self._historical_trade_creation_allowed(scanner):
                                    scanner.trades.closed_trades.append(t)
                                    if sim_inst == "OPT" and self._hybrid_backtest_dual_futures_enabled(scanner):
                                        shadow_id = f"{eod_id}_FUT"
                                        if not any(ct.id == shadow_id for ct in scanner.trades.closed_trades):
                                            shadow = self._build_historical_futures_shadow_trade(
                                                scanner=scanner,
                                                trade_id=shadow_id,
                                                instrument=instrument,
                                                timeframe=tf,
                                                direction=direction,
                                                signal=last_s,
                                                exit_spot=eod_price,
                                                exit_time=eod_exit_time,
                                                grade=eod_grade,
                                                confidence=eod_conf,
                                                rr=1.5,
                                                lot_size=lot_size,
                                                display_suffix="EOD, Hist",
                                                default_exit_reason=eod_exit_reason,
                                            )
                                            if shadow:
                                                scanner.trades.closed_trades.append(shadow)
                                else:
                                    logger.info(
                                        f"Blocked historical EOD trade creation outside HISTORICAL mode: {eod_id}"
                                    )

                                logger.debug(f"ðŸ“Š Synthetic EOD close: {eod_id} | PnL: â‚¹{eod_pnl:,.0f}")

                # Update lock with precise last signal time
                # CRITICAL FIX: Use the SECOND-TO-LAST signal's timestamp
                # The LAST signal may be unpaired (no exit yet) â€” if we set lock
                # to its timestamp, the live detector won't see it as "new"
                # (because s_ts > l_ts fails when they're equal).
                if len(sigs) >= 2:
                    scanner._last_signal_time[dedup_key] = naive_ts(sigs[-2].timestamp)
                elif sigs:
                    scanner._last_signal_time[dedup_key] = naive_ts(sigs[-1].timestamp - pd.Timedelta(seconds=1))
                else:
                    scanner._last_signal_time[dedup_key] = naive_ts(datetime.now(IST)) if is_live else datetime.min
                
            last_time = naive_ts(scanner._last_signal_time[dedup_key])
            
            for sig in sigs:
                s_ts = naive_ts(sig.timestamp)
                l_ts = last_time
                signal_identity = f"{s_ts.isoformat()}|{sig.signal_type}"
                last_identity = getattr(scanner, "_last_signal_identity", {}).get(dedup_key, "")
                now_naive = naive_ts(datetime.now(IST))
                tf_minutes = {"1min": 1, "5min": 5, "15min": 15}.get(tf, 5)
                pending_key = (
                    f"{instrument}_{tf}_"
                    f"{'LONG' if sig.signal_type == 'BUY' else 'SHORT'}"
                )
                recheck_eligible = (
                    getattr(scanner, "_live_signal_recheck_eligible", {}).get(dedup_key)
                    == signal_identity
                )
                last_recheck = getattr(scanner, "_live_signal_recheck_at", {}).get(
                    dedup_key,
                    datetime.min,
                )
                recheck_due = is_live and bool(
                    getattr(settings, "live_intrabar_signal_entries", True)
                ) and self._live_signal_recheck_due(
                    s_ts,
                    tf,
                    now_naive,
                    recheck_eligible,
                    pending_key in getattr(scanner, "_pending_live_signals", {}),
                    naive_ts(last_recheck),
                )

                is_new_signal = (
                    self._live_signal_is_new(s_ts, sig.signal_type, l_ts, last_identity)
                    if is_live
                    else s_ts > l_ts
                )
                is_new_signal = is_new_signal or recheck_due
                if is_new_signal:
                    if not hasattr(scanner, "_live_signal_recheck_at"):
                        scanner._live_signal_recheck_at = {}
                    scanner._live_signal_recheck_at[dedup_key] = now_naive
                    # ── Ghost Signal Age Guard (live mode only) ──
                    # Even though this signal is "new" per the dedup lock, in
                    # live/real mode we must verify its candle is actually
                    # current.  A signal from a candle that closed minutes ago
                    # is a historical-backfill echo, not a live signal.
                    if is_live:
                        deadline_fn = getattr(scanner, "_live_signal_processing_deadline", None)
                        deadline = (
                            deadline_fn(s_ts, tf)
                            if callable(deadline_fn)
                            else s_ts + timedelta(minutes=tf_minutes, seconds=90)
                        )
                        if deadline is not None and now_naive > deadline:
                            logger.warning(
                                f"👻 [GHOST GUARD] Skipping stale backfill signal "
                                f"{instrument} {tf} at {s_ts} "
                                f"(closed_age={(now_naive - (s_ts + timedelta(minutes=tf_minutes))).total_seconds():.0f}s). "
                                f"Not a live signal."
                            )
                            emit_once = getattr(scanner, "log_signal_decision_once", None)
                            if callable(emit_once):
                                emit_once(
                                    f"{instrument}|{tf}|{sig.signal_type}|{s_ts.isoformat(timespec='seconds')}|ghost",
                                    f"Signal ignored: {instrument} {sig.signal_type} {tf} @ {sig.price:.2f} - stale backfill echo, not a live signal",
                                    "trade",
                                )
                            # Still advance the dedup lock so we don't re-check
                            scanner._last_signal_time[dedup_key] = max(
                                naive_ts(scanner._last_signal_time[dedup_key]), s_ts
                            )
                            if not hasattr(scanner, "_last_signal_identity"):
                                scanner._last_signal_identity = {}
                            scanner._last_signal_identity[dedup_key] = signal_identity
                            getattr(scanner, "_live_signal_recheck_eligible", {}).pop(dedup_key, None)
                            continue

                        candle_close_time = s_ts + timedelta(minutes=tf_minutes)
                        intrabar_enabled = bool(getattr(settings, "live_intrabar_signal_entries", True))
                        if (
                            not intrabar_enabled
                            and not self._live_signal_candle_is_closed(s_ts, tf, now_naive)
                        ):
                            logger.debug(
                                f"[PARTIAL CANDLE] Deferring live signal processing for {instrument} {tf} at {s_ts}. "
                                f"Candle closes at {candle_close_time}."
                            )
                            emit_once = getattr(scanner, "log_signal_decision_once", None)
                            if callable(emit_once):
                                emit_once(
                                    f"{instrument}|{tf}|{sig.signal_type}|{s_ts.isoformat(timespec='seconds')}|partial",
                                    f"Signal waiting: {instrument} {sig.signal_type} {tf} @ {sig.price:.2f} - partial candle deferred until {candle_close_time.strftime('%H:%M:%S')}",
                                    "trade",
                                )
                            continue

                    no_entry_label, no_entry_after = _entry_cutoff(settings, tf)
                    is_after_no_entry = tf in ["5min", "15min"] and sig.timestamp.time() >= no_entry_after
                    ts_str = sig.timestamp.strftime("%Y-%m-%d %H:%M:00")
                    vix_value = scanner._vix_data.get(ts_str)
                    if vix_value is None:
                        vix_value = scanner.data.get_ltp("NSE", "INDIAVIX", "99926017") or 15.0

                    if is_live:
                        raw_key = f"{instrument}|{tf}|{sig.signal_type}|{s_ts.isoformat(timespec='seconds')}"
                        seen_raw = getattr(scanner, "_raw_utbot_activity_keys", None)
                        if seen_raw is None:
                            scanner._raw_utbot_activity_keys = set()
                            seen_raw = scanner._raw_utbot_activity_keys
                        if raw_key not in seen_raw:
                            seen_raw.add(raw_key)
                            if len(seen_raw) > 500:
                                scanner._raw_utbot_activity_keys = set(list(seen_raw)[-250:])
                            scanner.log_event(
                                f"UTBot raw {sig.signal_type} detected: {instrument} {tf} @ {sig.price:.2f}. Watching for repaint.",
                                "trade",
                            )
                        
                    grade, conf, reasons = scanner.signals._grade_signal(sig, mtf_result.confluence_score, intel_score, regime, vix_value=vix_value)
                    candidates_raw.append({
                        "sig": sig, 
                        "tf": tf, 
                        "grade": grade, 
                        "confidence": conf,
                        "reasons": reasons,
                        "after_no_entry": is_after_no_entry,
                        "after_no_entry_label": no_entry_label,
                        "dedup_key": dedup_key,
                        "signal_identity": signal_identity,
                    })
                    
                    # Fix: Always update the lock to prevent evaluating the same signal on every tick
                    scanner._last_signal_time[dedup_key] = max(naive_ts(scanner._last_signal_time.get(dedup_key, datetime.min)), s_ts)
                    if not hasattr(scanner, "_last_signal_identity"):
                        scanner._last_signal_identity = {}
                    scanner._last_signal_identity[dedup_key] = signal_identity
                    getattr(scanner, "_live_signal_recheck_eligible", {}).pop(dedup_key, None)

        # In HISTORICAL mode, we only run the backfill once and do not return live candidates
        if getattr(scanner, "mode", "").upper() == "HISTORICAL":
            return []

        if not candidates_raw:
            return []

        def _emit_signal_decision(raw: Dict, reason_msg: str, status_msg: str) -> None:
            sig = raw["sig"]
            tf = raw["tf"]
            decision_key = (
                f"{instrument}|{tf}|{sig.signal_type}|"
                f"{sig.timestamp.isoformat(timespec='seconds')}|{status_msg}|{reason_msg}"
            )
            logger.info(f"Signal decision: {instrument} {tf} {sig.signal_type} -> {status_msg}: {reason_msg}")
            emit_once = getattr(scanner, "log_signal_decision_once", None)
            message = (
                f"Signal rejected: {instrument} {sig.signal_type} {tf} @ {sig.price:.2f} - "
                f"{reason_msg}"
            )
            if callable(emit_once):
                emit_once(decision_key, message, "trade")
            else:
                scanner.log_event(message, "trade")
                
            diag_inc = getattr(scanner, "_diag_inc", None)
            if callable(diag_inc):
                diag_inc("rejects", reason_msg)

        def _build_rejected_from_raw(raw: Dict, reason_msg: str, status_msg: str) -> List[TradeCandidate]:
            _emit_signal_decision(raw, reason_msg, status_msg)
            sig = raw["sig"]
            tf = raw["tf"]
            grade = raw["grade"]
            confidence = raw["confidence"]
            reasons = raw["reasons"]
            if is_live and status_msg in {
                "REJECTED - Quality Gate",
                "REJECTED - 5min Quality",
                "REJECTED - Low Confidence",
                "REJECTED - 5min Choppy Confirmation",
                "REJECTED - Opt Gate",
            }:
                now_check = naive_ts(datetime.now(IST))
                tf_minutes = {"1min": 1, "5min": 5, "15min": 15}.get(tf, 5)
                if now_check < naive_ts(sig.timestamp) + timedelta(minutes=tf_minutes):
                    if not hasattr(scanner, "_live_signal_recheck_eligible"):
                        scanner._live_signal_recheck_eligible = {}
                    scanner._live_signal_recheck_eligible[
                        raw.get("dedup_key", f"{instrument}_{tf}")
                    ] = raw.get(
                        "signal_identity",
                        f"{naive_ts(sig.timestamp).isoformat()}|{sig.signal_type}",
                    )
            direction = "LONG" if sig.signal_type == "BUY" else "SHORT"
            rej = TradeCandidate(
                instrument=instrument,
                direction=direction,
                price=sig.price,
                stop=sig.trailing_stop,
                target=sig.price,
                lots=0,
                lot_size=0,
                grade=grade,
                confidence=confidence,
                timeframe=tf,
                inst_type="FUT",
                option_type="",
                atm_strike=0.0,
                multiplier=1.0,
                trading_symbol="",
                symbol_token="",
                rr=0.0,
                score=confidence,
                reasons=list(reasons or []) + [reason_msg],
                signal_timestamp=sig.timestamp,
                status=status_msg,
                action="NO_ENTRY"
            )
            return [rej]

        exit_handled = False
        for c in sorted(candidates_raw, key=lambda item: item["sig"].timestamp):
            if scanner._handle_opposite_signal_exit(instrument, c["sig"], c["tf"]):
                exit_handled = True

        # Live/manual mode must not flip into a fresh opposite entry in the same
        # index/timeframe until the existing signal/trade has been exited.
        if exit_handled and getattr(scanner, "mode", "") != "HISTORICAL":
            scanner.log_event(
                f"Entry blocked: {instrument} opposite signal first requires the existing {candidates_raw[-1]['tf']} signal/trade to exit",
                "trade",
            )
            return _build_rejected_from_raw(candidates_raw[-1], "Opposite signal must exit existing signal/trade first", "REJECTED - Opposite Signal Wait")

        entry_candidates_raw = list(candidates_raw)
        if getattr(scanner, "mode", "") != "HISTORICAL":
            tf_policy = str(getattr(settings, "ut_timeframe_entry_policy", "PRIMARY_15") or "PRIMARY_15").upper()
            if tf_policy == "PRIMARY_15":
                entry_candidates_raw = [c for c in entry_candidates_raw if c.get("tf") == "15min"]
                if not entry_candidates_raw:
                    scanner.log_event(
                        f"Entry blocked: {instrument} 5min signal is exit/timing-only while 15M MAIN policy is active",
                        "trade",
                    )
                    return _build_rejected_from_raw(candidates_raw[-1], "5min signal is exit/timing-only while 15M MAIN policy is active", "REJECTED - 15M MAIN Policy Blocked")

        scored = []
        for c in entry_candidates_raw:
            sig, tf, grade, confidence, reasons = c["sig"], c["tf"], c["grade"], c["confidence"], c["reasons"]
            score = confidence
            if tf == "15min":
                score += 0.0001
            scored.append((score, sig, tf, grade, confidence, reasons, c.get("after_no_entry", False)))

        # Fix: Sort primarily by timestamp to pick the LATEST signal.
        # If multiple signals have the same timestamp, pick the one with highest score.
        scored.sort(key=lambda x: (x[1].timestamp, x[0]), reverse=True)
        best_score, best_sig, best_tf, best_grade, best_conf, best_reasons, best_after_no_entry = scored[0]

        def _build_rejected_candidate(reason_msg: str, status_msg: str) -> List[TradeCandidate]:
            return _build_rejected_from_raw(
                {
                    "sig": best_sig,
                    "tf": best_tf,
                    "grade": best_grade,
                    "confidence": best_conf,
                    "reasons": best_reasons,
                },
                reason_msg,
                status_msg,
            )

        for tid, trade in list(scanner.trades.open_trades.items()):
            if trade.instrument == instrument and trade.status == "OPEN":
                if best_tf == "15min" and trade.timeframe == "5min":
                    trade.timeframe = "15min"
                    trade.grade = best_grade
                    trade.confidence = best_conf
                    logger.info(f"🚀 UPGRADED {instrument} trade to 15-min status")
                    return []
                _emit_signal_decision(
                    {
                        "sig": best_sig,
                        "tf": best_tf,
                        "grade": best_grade,
                        "confidence": best_conf,
                        "reasons": best_reasons,
                    },
                    f"Already has active {trade.timeframe} {trade.direction} trade {tid}",
                    "REJECTED - Existing Active Trade",
                )
                return []

        # Do not call process_signal here. In live mode this stage can run
        # before the 25s repaint guard has matured, and process_signal mutates
        # active_signals/dedup state. Keep grading stateless; the execution
        # lifecycle starts only after stabilization in scanner._coordinate_and_execute.
        graded = GradedSignal(
            signal=best_sig,
            grade=best_grade,
            confidence=best_conf,
            reasons=list(best_reasons or []),
            intelligence_score=intel_score,
            confluence_score=mtf_result.confluence_score,
            is_actionable=best_grade in ["A+", "A", "B+", "B"],
        )
        state_15m = (getattr(mtf_result, "results_15min", None) or {}).get("state", {}) or {}
        position_15m = int(state_15m.get("position") or 0)
        direction_position = 1 if best_sig.signal_type == "BUY" else -1
        trend_15m_agrees = position_15m == direction_position
        adx_value = float(getattr(best_sig, "adx_value", 0.0) or 0.0)
        momentum_threshold = float(getattr(settings, "momentum_override_threshold", 25.0) or 25.0)
        is_explosive_bypass = (
            adx_value >= momentum_threshold
            or abs(normalize_intelligence_score(intel_score)) >= 0.70
        )
        if not graded or not graded.is_actionable:
            msg = "Not actionable after filters"
            scanner.log_event(
                f"Signal Rejected: {instrument} {best_sig.signal_type} ({best_tf}) - {msg}",
                "trade",
            )
            return _build_rejected_candidate(msg, "REJECTED - Quality Gate")

        if best_tf == "5min":
            min_conf_5m = self._relaxed_threshold(
                float(getattr(settings, "ut_5min_option_min_confidence", 0.60) or 0.60),
                settings,
            )
            grade_conf_ok = self._grade_rank(graded.grade) >= 3 and graded.confidence >= min_conf_5m
            if not (grade_conf_ok or trend_15m_agrees or is_explosive_bypass):
                msg = (
                    f"5min gate needs A/A+ with >= {min_conf_5m:.0%}, "
                    f"or 15min trend agreement"
                    + ", or explosive move bypass"
                )
                scanner.log_event(
                    f"Signal Rejected: {instrument} {best_sig.signal_type} ({best_tf}) - {msg}",
                    "trade",
                )
                return _build_rejected_candidate(msg, "REJECTED - 5min Quality")
            
        min_confidence = 0.55
        if getattr(settings, "ut_dynamic_confidence", False):
            if regime in ["VOLATILE", "CHOPPY", "MEAN_REVERTING", "UNKNOWN"]:
                min_confidence = 0.30
                
        if graded.confidence < min_confidence and not (best_tf == "5min" and (trend_15m_agrees or is_explosive_bypass)):
            msg = f"{graded.grade} {graded.confidence:.0%} below {min_confidence:.0%} confidence gate"
            scanner.log_event(
                f"Signal Rejected: {instrument} {best_sig.signal_type} ({best_tf}) - {msg}",
                "trade",
            )
            return _build_rejected_candidate(msg, "REJECTED - Low Confidence")

        if best_tf == "5min" and self._is_choppy_regime(regime):
            impulse_exception = self._is_5min_impulse_reversal(best_sig, mtf_result, graded.confidence, settings)
            if not trend_15m_agrees and not impulse_exception and not is_explosive_bypass:
                msg = "5min choppy gate needs 15min agreement or impulse-reversal confirmation"
                scanner.log_event(
                    f"Signal Rejected: {instrument} {best_sig.signal_type} ({best_tf}) - {msg}",
                    "trade",
                )
                return _build_rejected_candidate(msg, "REJECTED - 5min Choppy Confirmation")

        if best_tf == "1min":
            iv_percentile = intel_result.get("greeks", {}).get("iv_percentile", 50.0)
            if graded.confidence >= 0.70 or normalize_intelligence_score(intel_score) > 0.60 or iv_percentile > 80:
                logger.info(f"🚀 High-Confidence 1min Spike detected for {instrument}!")
            else:
                return _build_rejected_candidate("1min signal lacks high confidence/momentum", "REJECTED - Weak 1min")

        can_open, _ = scanner.trades.can_open_trade()
        if not can_open:
            return _build_rejected_candidate("Max overall trades reached or capital depleted", "REJECTED - Max Trades")

        iv_percentile = intel_result.get("greeks", {}).get("iv_percentile", 50.0)
        is_high_iv = iv_percentile > 80.0
        is_choppy = self._is_choppy_regime(regime)
        strong_momentum = abs(normalize_intelligence_score(intel_score)) > 0.60 or best_score > 0.4
        
        trade_date = best_sig.timestamp.date()
        is_0dte = scanner.expiry.is_expiry_day(instrument, trade_date)
        is_late_session = best_sig.timestamp.time() >= datetime.strptime("14:00", "%H:%M").time()
        
        inst_type = scanner._resolve_instrument_type(
            instrument=instrument,
            grade=graded.grade,
            regime=regime,
            intel_score=intel_score,
            signal_score=best_score,
            confidence=graded.confidence,
            adx_value=getattr(best_sig, "adx_value", 0.0),
            atr_value=getattr(best_sig, "atr_value", 0.0),
            price=best_sig.price,
            signal_time=best_sig.timestamp,
            iv_percentile=iv_percentile,
        )
        if not scanner.risk_manager.can_trade(inst_type):
            return _build_rejected_candidate(
                f"{inst_type} circuit breaker is active",
                f"REJECTED - {inst_type} Risk Halt",
            )
        
            if inst_type == "OPT":
                option_pref = (getattr(scanner, "inst_pref", "AUTO") or "AUTO").upper()
                option_quality_ok = self._option_grade_allowed(
                graded.grade,
                graded.confidence,
                regime,
                adx_value=getattr(best_sig, "adx_value", 0.0),
                intel_score=intel_score,
                signal_score=best_score,
            )
            if not option_quality_ok:
                if option_pref == "OPT":
                    msg = "Below premium quality gate"
                    logger.info(f"Skipping option signal below premium quality gate for {instrument} at {best_sig.timestamp}")
                    return _build_rejected_candidate(msg, "REJECTED - Opt Gate")
                inst_type = "FUT"

                # â•â•â• Cross-Timeframe Monopoly Guard â•â•â•
        existing_index_trades = [t for t in scanner.trades.open_trades.values() if t.instrument == instrument]
        if existing_index_trades:
            # Check if this is a DIFFERENT best_tf
            is_different_tf = all(getattr(t, "timeframe", "") != best_tf for t in existing_index_trades)
            is_high_grade = graded.grade in ["A", "A+"]
            
            can_add = False
            if is_different_tf and is_high_grade and len(existing_index_trades) == 1:
                # Force switch type (FUT -> OPT or vice versa)
                existing_trade = existing_index_trades[0]
                existing_type = getattr(existing_trade, 'inst_type', 'FUT')
                
                # Respect instrument preference!
                if scanner.inst_pref == "FUT" and existing_type == "FUT":
                    msg = "Already have FUT trade and preference is FUT"
                    logger.info(f"🚫 Monopoly Guard: {msg}. Skipping.")
                    return _build_rejected_candidate(msg, "REJECTED - Monopoly")
                elif scanner.inst_pref == "OPT" and existing_type == "OPT":
                    msg = "Already have OPT trade and preference is OPT"
                    logger.info(f"🚫 Monopoly Guard: {msg}. Skipping.")
                    return _build_rejected_candidate(msg, "REJECTED - Monopoly")
                
                # If no preference conflict, force switch type
                inst_type = "OPT" if existing_type == "FUT" else "FUT"
                can_add = True
                logger.info(f"🌟 Cross-TF Confluence ({best_tf}): Adding {inst_type} to complement {getattr(existing_trade, 'timeframe', '')} {existing_type}")

            
            if not can_add:
                msg = f"Already has an active {getattr(existing_index_trades[0], 'timeframe', '')} trade"
                logger.info(f"🚫 Monopoly Guard: {instrument} {msg}. Skipping {best_tf}.")
                return _build_rejected_candidate(msg, "REJECTED - Monopoly")

        option_type = "CE" if best_sig.signal_type == "BUY" else "PE"
        strike_interval = cfg.get("strike_interval", 50)
        
        atm_strike_val = round(spot / strike_interval) * strike_interval if spot > 0 else 0
        itm_strike_val = (atm_strike_val - strike_interval) if option_type == "CE" else (atm_strike_val + strike_interval)
        
        strike_selection = getattr(settings, "option_strike_selection", "ATM")
        target_strikes = []
        if inst_type == "FUT":
            target_strikes = [0.0]
        else:
            if strike_selection == "ATM": target_strikes = [atm_strike_val]
            elif strike_selection == "ITM": target_strikes = [itm_strike_val]
            else: target_strikes = [atm_strike_val, itm_strike_val]

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
                    instrument_multiplier = intel_result.get("greeks", {}).get("call", {}).get("delta", 0.5)
                else:
                    instrument_multiplier = intel_result.get("greeks", {}).get("put", {}).get("delta", -0.5)
                    if instrument_multiplier > 0: instrument_multiplier = -instrument_multiplier
                
                if abs(instrument_multiplier) < 0.1:
                    instrument_multiplier = 0.5 if option_type_local == "CE" else -0.5

            final_rr = self._dynamic_rr(best_grade, best_conf, intel_score)
            direction = "LONG" if best_sig.signal_type == "BUY" else "SHORT"
            
            quality_factor = min(1.0, (best_conf + (0.1 if best_grade in ["A", "A+"] else 0.0)))
            rr_factor = 1.0 if final_rr >= 1.5 else (final_rr / 1.5)
            total_multiplier = quality_factor * rr_factor
            
            if inst_type == "FUT":
                base_fut_risk = scanner.risk_fut_pct
                dynamic_fut_risk = max(0.1, min(base_fut_risk, base_fut_risk * total_multiplier))
                risk_amt = scanner.capital_fut * (dynamic_fut_risk / 100.0)
                max_allowed_sl_pts = spot * (scanner.futures_sl_pct / 100.0)
                index_stop_distance = min(best_sig.stop_distance, max_allowed_sl_pts)
                
                if best_sig.stop_distance > (max_allowed_sl_pts * 1.5):
                     return []
                actual_lots = getattr(scanner, "user_lots_fut", {}).get(instrument, 1)
            else:
                base_opt_risk = scanner.risk_opt_pct
                dynamic_opt_risk = max(1.0, min(base_opt_risk, base_opt_risk * total_multiplier))
                risk_amt = scanner.capital_opt * (dynamic_opt_risk / 100.0)
                
                est_premium = _estimate_opt_premium(spot, getattr(self, "scanner", None))
                lot_cost = est_premium * lot_size
                pos_cap_limit = 100000.0
                cap_based_lots = int(pos_cap_limit / max(1, lot_cost))
                user_target = scanner.user_lots.get(instrument, 1)
                actual_lots = max(1, min(user_target, cap_based_lots))
                
                max_premium_sl_pts = est_premium * (scanner.options_sl_pct / 100.0)
                hard_cap_index_sl = max_premium_sl_pts / max(0.1, abs(instrument_multiplier))
                index_stop_distance = min(best_sig.stop_distance, hard_cap_index_sl)

            if regime == "UNKNOWN":
                actual_lots = max(1, actual_lots // 2)

            if direction == "LONG":
                entry_stop = spot - index_stop_distance
                target = spot + (index_stop_distance * final_rr)
            else:
                entry_stop = spot + index_stop_distance
                target = spot - (index_stop_distance * final_rr)

            trading_symbol = ""
            symbol_token = ""
            if inst_type == "FUT":
                info = scanner.market_info.get(instrument, {})
                trading_symbol = info.get("current_fut", "")
                symbol_token = info.get("current_fut_token", "")
            else:
                opt_info = scanner.data.get_option_token(instrument, strike_local, option_type_local)
                if opt_info:
                    trading_symbol = opt_info['symbol']
                    symbol_token = opt_info['token']

            if symbol_token:
                strike_alpha = (instrument_multiplier * 0.4)
                oi_score = intel_result.get("oi", {}).get("score", 50) / 100.0
                vol_score = intel_result.get("volume", {}).get("score", 50) / 100.0
                itm_bonus = 0.15 if (strike_local == itm_strike_val and inst_type == "OPT") else 0.0
                final_strike_score = strike_alpha + (oi_score * 0.3) + (vol_score * 0.3) + itm_bonus

                live_trade_price = spot
                exchange = "BFO" if instrument == "SENSEX" else "NFO"
                candidate_stop = entry_stop
                candidate_target = target
                
                if symbol_token:
                    try:
                        if inst_type == "OPT":
                            live_premium = self._contract_ltp(scanner, exchange, trading_symbol, symbol_token)
                            live_trade_price = live_premium if live_premium > 0 else _estimate_opt_premium(spot, getattr(self, "scanner", None))
                            max_sl_dist = live_trade_price * 0.10
                            candidate_stop = live_trade_price - max_sl_dist
                            candidate_target = live_trade_price + (max_sl_dist * final_rr)
                        else: # FUT
                            live_fut_ltp = self._contract_ltp(scanner, exchange, trading_symbol, symbol_token)
                            live_trade_price = live_fut_ltp if live_fut_ltp > 0 else spot
                            max_sl_dist = spot * 0.0025
                            if direction == "LONG":
                                candidate_stop = live_trade_price - max_sl_dist
                                candidate_target = live_trade_price + (max_sl_dist * final_rr)
                            else:
                                candidate_stop = live_trade_price + max_sl_dist
                                candidate_target = live_trade_price - (max_sl_dist * final_rr)
                    except Exception as e:
                        if inst_type == "OPT":
                            live_trade_price = _estimate_opt_premium(spot, getattr(self, "scanner", None))
                            candidate_stop = live_trade_price * 0.90
                            candidate_target = live_trade_price * 1.15
                        else:
                            live_trade_price = spot
                            if direction == "LONG":
                                candidate_stop = live_trade_price * 0.9975
                                candidate_target = live_trade_price * 1.005
                            else:
                                candidate_stop = live_trade_price * 1.0025
                                candidate_target = live_trade_price * 0.995
                
                no_entry_label, _ = _entry_cutoff(settings, best_tf)
                candidate_status = f"NO ENTRY - AFTER {no_entry_label}" if best_after_no_entry else "TRADE SIGNAL"
                candidate_action = "NO_ENTRY" if best_after_no_entry else "ENTRY"
                candidate_reasons = list(best_reasons or [])
                if best_after_no_entry:
                    candidate_reasons.append(f"After no-fresh-entry gate ({no_entry_label} IST)")

                if live_trade_price <= 0:
                    logger.warning(f"Skipping candidate for {instrument} due to invalid live price: {live_trade_price}")
                    continue

                candidate = TradeCandidate(
                    instrument=instrument, direction=direction, price=live_trade_price,
                    stop=candidate_stop, target=candidate_target, lots=actual_lots, lot_size=lot_size, grade=best_grade,
                    confidence=best_conf, timeframe=best_tf, inst_type=inst_type, option_type=option_type_local,
                    atm_strike=strike_local, multiplier=instrument_multiplier, trading_symbol=trading_symbol,
                    symbol_token=symbol_token, rr=round(final_rr, 2), score=final_strike_score,
                    reasons=candidate_reasons, signal_timestamp=best_sig.timestamp,
                    status=candidate_status, action=candidate_action
                )
                setattr(candidate, "source_signal_type", best_sig.signal_type)
                setattr(candidate, "source_signal_price", float(best_sig.price or 0.0))
                setattr(candidate, "source_signal_bar_index", int(getattr(best_sig, "bar_index", -1) or -1))
                setattr(candidate, "trend_15m_agrees", bool(trend_15m_agrees))
                candidate.is_explosive_bypass = bool(is_explosive_bypass)
                if candidate.is_explosive_bypass:
                    candidate.reasons = list(candidate.reasons or []) + [
                        f"Explosive bypass ADX={adx_value:.1f}, intel={normalize_intelligence_score(intel_score):.2f}"
                    ]
                results_list.append(candidate)
                logger.info(
                    f"Resolved candidate: {instrument} {best_tf} {best_sig.signal_type} -> "
                    f"{direction} {inst_type} {option_type_local or 'FUT'} {trading_symbol}"
                    + (" | no-entry display only" if best_after_no_entry else "")
                )

        if not results_list:
            return []

        results_list.sort(key=lambda x: x.score, reverse=True)
        winner = results_list[0]
        getattr(scanner, "_live_signal_recheck_eligible", {}).pop(
            f"{instrument}_{best_tf}",
            None,
        )
        
        scanner.log_event(f"ðŸŽ¯ Potential {winner.direction} Signal for {winner.instrument} ({winner.timeframe})", "trade")
        return [winner]
