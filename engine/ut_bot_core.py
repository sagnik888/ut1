"""
UT Bot Core Engine — Exact Pine Script → Python Translation
═══════════════════════════════════════════════════════════════

Faithfully implements the TradingView "UT Bot Pro — Daily Trader Edition"
indicator logic in Python with identical signal generation.

Core Logic:
1. ATR Trailing Stop calculation with state machine
2. Crossover-based position detection (bullFlip / bearFlip)
3. Signal generation with ADX and session filters
4. Risk overlay (stop distance, position sizing)

CRITICAL: Both position state and signals use IDENTICAL crossover
comparison (src[1] vs xATRTS[1] AND src vs xATRTS) to stay in sync.
"""

import numpy as np
import pandas as pd
from typing import Dict, Optional, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime, time
from numba import njit
from config.settings import get_settings

# ═══ NUMBA ACCELERATED LOOPS ═══
@njit
def _nb_ffill(arr):
    n = len(arr)
    out = np.zeros(n, dtype=np.int32)
    current = 0
    for i in range(n):
        if arr[i] != 0:
            current = arr[i]
        out[i] = current
    return out

@njit
def _nb_heikin_ashi(op, hi, lo, cl):
    n = len(op)
    ha_cl = (op + hi + lo + cl) / 4.0
    ha_op = np.empty(n)
    ha_op[0] = (op[0] + cl[0]) / 2.0
    for i in range(1, n):
        ha_op[i] = (ha_op[i-1] + ha_cl[i-1]) / 2.0
    
    ha_hi = np.empty(n)
    ha_lo = np.empty(n)
    for i in range(n):
        ha_hi[i] = max(ha_op[i], ha_cl[i], hi[i])
        ha_lo[i] = min(ha_op[i], ha_cl[i], lo[i])
    return ha_op, ha_hi, ha_lo, ha_cl

@njit
def _nb_trailing_stop(src, n_loss):
    n = len(src)
    ts = np.empty(n)
    ts[0] = src[0] - n_loss[0]
    for i in range(1, n):
        prev_ts = ts[i-1]
        curr_src = src[i]
        prev_src = src[i-1]
        curr_nloss = n_loss[i]
        if curr_src > prev_ts and prev_src > prev_ts:
            ts[i] = max(prev_ts, curr_src - curr_nloss)
        elif curr_src < prev_ts and prev_src < prev_ts:
            ts[i] = min(prev_ts, curr_src + curr_nloss)
        else:
            ts[i] = curr_src - curr_nloss if curr_src > prev_ts else curr_src + curr_nloss
    return ts


@dataclass
class UTBotSignal:
    """Represents a single UT Bot signal"""
    timestamp: datetime
    signal_type: str          # "BUY" or "SELL"
    price: float              # Entry/exit price
    trailing_stop: float      # Current trailing stop level
    stop_distance: float      # Distance to stop in points
    atr_value: float          # Current ATR value
    adx_value: float          # Current ADX value
    position_state: int       # 1=LONG, -1=SHORT, 0=FLAT
    bar_index: int            # Index in the dataframe
    instrument: str = ""
    timeframe: str = ""
    suggested_qty: int = 0    # Suggested position size
    raw_candle: Dict = field(default_factory=dict) # For manipulation analysis


@dataclass
class UTBotState:
    """Maintains running state of the UT Bot engine"""
    trailing_stop: float = 0.0
    position: int = 0              # 1=LONG, -1=SHORT, 0=FLAT
    last_entry_price: float = 0.0
    last_exit_price: float = 0.0
    last_entry_time: Optional[datetime] = None
    last_signal: Optional[UTBotSignal] = None
    signals_history: List[UTBotSignal] = field(default_factory=list)
    bar_count: int = 0


class UTBotEngine:
    """
    UT Bot Pro — Python Implementation

    Parameters mirror the TradingView indicator exactly:
    - key_value (ATR Multiplier): Higher = fewer signals, more reliable
    - atr_period: ATR calculation period
    - use_heikin_ashi: Use Heikin Ashi candles for source
    - signal_mode: "realtime" or "confirmed"
    - adx_filter: Enable ADX trend filter
    - adx_period: ADX calculation period
    - adx_threshold: Minimum ADX value for signal
    - session_filter: Enable session time filter
    - session_start/end: Trading session window (IST)
    """

    def __init__(
        self,
        key_value: float = 1.0,
        atr_period: int = 10,
        use_heikin_ashi: bool = False,
        signal_mode: str = "realtime",
        adx_filter: bool = True,
        adx_period: int = 14,
        adx_threshold: float = 25.0,
        strict_adx: bool = False,
        session_filter: bool = True,
        session_start: str = "09:20",
        session_end: str = "15:15",
        capital: float = 100000.0,
        risk_pct: float = 1.0,
    ):
        self.key_value = key_value
        self.atr_period = atr_period
        self.use_heikin_ashi = use_heikin_ashi
        self.signal_mode = signal_mode
        self.adx_filter = adx_filter
        self.adx_period = adx_period
        self.adx_threshold = adx_threshold
        self.strict_adx = strict_adx
        self.session_filter = session_filter
        self.session_start = self._parse_time(session_start)
        self.session_end = self._parse_time(session_end)
        self.capital = capital
        self.risk_pct = risk_pct

        # Running state per instrument-timeframe
        self._states: Dict[str, UTBotState] = {}

    def _parse_time(self, t: str) -> time:
        """Parse time string 'HH:MM' to time object"""
        parts = t.split(":")
        return time(int(parts[0]), int(parts[1]))

    def get_state(self, key: str) -> UTBotState:
        """Get or create state for instrument-timeframe key"""
        if key not in self._states:
            self._states[key] = UTBotState()
        return self._states[key]

    @staticmethod
    def _effective_sizing_stop(stop_distance: float, atr_value: float, price: float) -> float:
        """Apply a volatility/price floor for sizing without moving the actual stop."""
        settings = get_settings()
        atr_floor = max(0.0, float(atr_value or 0.0)) * max(
            0.0,
            float(getattr(settings, "position_sizing_min_atr_fraction", 0.10) or 0.10),
        )
        price_floor = abs(float(price or 0.0)) * max(
            0.0,
            float(getattr(settings, "position_sizing_min_price_fraction", 0.00005) or 0.00005),
        )
        return max(float(stop_distance or 0.0), atr_floor, price_floor, 0.01)

    # ═══════════════════════════════════════════════════════════
    # HEIKIN ASHI
    # ═══════════════════════════════════════════════════════════
    def _to_heikin_ashi(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert OHLCV to Heikin Ashi candles using Numba"""
        ha_op, ha_hi, ha_lo, ha_cl = _nb_heikin_ashi(
            df['open'].values, df['high'].values, 
            df['low'].values, df['close'].values
        )
        ha = df.copy()
        ha['open'] = ha_op
        ha['high'] = ha_hi
        ha['low'] = ha_lo
        ha['close'] = ha_cl
        return ha

    # ═══════════════════════════════════════════════════════════
    # ATR CALCULATION
    # ═══════════════════════════════════════════════════════════
    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 10) -> pd.Series:
        """
        Calculate Average True Range using Wilder's Smoothing (RMA).
        Standard TradingView ta.atr(10) equivalent.
        """
        high = df['high']
        low = df['low']
        close = df['close']

        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()

        tr = np.maximum(tr1, np.maximum(tr2, tr3))
        # Wilder's Smoothing (RMA) is an EMA with alpha = 1 / period
        atr = tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
        return atr

    # ═══════════════════════════════════════════════════════════
    # ADX CALCULATION (ta.dmi equivalent)
    # ═══════════════════════════════════════════════════════════
    @staticmethod
    def calculate_adx(df: pd.DataFrame, period: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        Calculate ADX with DI+ and DI-
        Returns: (di_plus, di_minus, adx)
        """
        high = df['high']
        low = df['low']
        close = df['close']

        # True Range
        tr1 = high - low
        tr2 = (high - close.shift(1)).abs()
        tr3 = (low - close.shift(1)).abs()
        tr = np.maximum(tr1, np.maximum(tr2, tr3))

        # Directional Movement
        up_move = high - high.shift(1)
        down_move = low.shift(1) - low

        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        # Smoothed (Wilder's smoothing = EMA with alpha=1/period)
        atr_smooth = tr.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
        plus_dm_smooth = pd.Series(plus_dm, index=df.index).ewm(
            alpha=1.0 / period, min_periods=period, adjust=False
        ).mean()
        minus_dm_smooth = pd.Series(minus_dm, index=df.index).ewm(
            alpha=1.0 / period, min_periods=period, adjust=False
        ).mean()

        # DI+ and DI-
        di_plus = 100 * plus_dm_smooth / atr_smooth
        di_minus = 100 * minus_dm_smooth / atr_smooth

        # DX and ADX
        di_sum = di_plus + di_minus
        di_sum = di_sum.replace(0, np.nan)
        dx = 100 * (di_plus - di_minus).abs() / di_sum
        adx = dx.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

        return di_plus, di_minus, adx

    # ═══════════════════════════════════════════════════════════
    # ATR TRAILING STOP — EXACT PINE SCRIPT LOGIC
    # ═══════════════════════════════════════════════════════════
    @staticmethod
    def calculate_trailing_stop(
        src: pd.Series,
        atr: pd.Series,
        key_value: float
    ) -> pd.Series:
        """Calculate ATR Trailing Stop using Numba accelerated loop"""
        n_loss = key_value * atr.values
        ts_values = _nb_trailing_stop(src.values, n_loss)
        return pd.Series(ts_values, index=src.index)

    # ═══════════════════════════════════════════════════════════
    # SESSION FILTER
    # ═══════════════════════════════════════════════════════════
    def _in_session(self, timestamp: datetime, timeframe: str = "") -> bool:
        """Check if timestamp is within trading session"""
        if not self.session_filter:
            return True
        t = timestamp.time() if isinstance(timestamp, datetime) else timestamp
        
        # Hardcap 15min trades at 15:00
        if timeframe == "15min":
            end_cutoff = time(15, 0)
        # 5min trades allowed until 15:15
        elif timeframe == "5min":
            end_cutoff = time(15, 15)
        else:
            end_cutoff = self.session_end
            
        return self.session_start <= t <= end_cutoff

    # ═══════════════════════════════════════════════════════════
    # FULL SIGNAL GENERATION
    # ═══════════════════════════════════════════════════════════
    def process(
        self,
        df: pd.DataFrame,
        instrument: str = "",
        timeframe: str = "",
        capital: Optional[float] = None,
        risk_pct: Optional[float] = None,
        lots: int = 1,
        lot_size: int = 25,
    ) -> Dict:
        """
        Process OHLCV data and generate signals.

        Args:
            df: DataFrame with columns [open, high, low, close, volume]
                and a datetime index or 'timestamp' column.
            instrument: e.g. "NIFTY"
            timeframe: e.g. "5min"
            capital: Override capital for this run
            risk_pct: Override risk % for this run
            lots: Number of lots (user defined)
            lot_size: Lot size for the instrument

        Returns:
            Dict with trailing stop data, signals, position state, risk info
        """
        if len(df) < self.atr_period + 2:
            return {"error": "Insufficient data", "signals": [], "state": None}

        cap = capital or self.capital
        rpct = risk_pct or self.risk_pct
        state_key = f"{instrument}_{timeframe}"
        state = self.get_state(state_key)

        # ── Source: optionally use Heikin Ashi ──
        work_df = self._to_heikin_ashi(df.copy()) if self.use_heikin_ashi else df.copy()
        src = work_df['close']

        # ── ATR ──
        atr = self.calculate_atr(work_df, self.atr_period)

        # ── Trailing Stop ──
        ts = self.calculate_trailing_stop(src, atr, self.key_value)

        # ── ADX Filter ──
        di_plus, di_minus, adx = self.calculate_adx(work_df, self.adx_period)
        is_trending = adx > self.adx_threshold if self.adx_filter else pd.Series(True, index=df.index)

        # ── Position State & Signals (Vectorized) ──
        # CRITICAL: Use identical crossover comparison for pos and signals
        src_arr = src.values
        ts_arr = ts.values

        # Crossover detection
        prev_src = src_arr[:-1]
        curr_src = src_arr[1:]
        prev_ts = ts_arr[:-1]
        curr_ts = ts_arr[1:]

        # Pine's crossover compares both current values after confirming the
        # previous source was on the opposite side of the previous stop.
        bull_flips = (prev_src < prev_ts) & (curr_src > curr_ts)
        bear_flips = (prev_src > prev_ts) & (curr_src < curr_ts)

        # Build positions array
        flips = np.zeros(len(df), dtype=np.int32)
        flips[1:][bull_flips] = 1
        flips[1:][bear_flips] = -1

        # Forward fill positions using Numba
        positions = _nb_ffill(flips)

        signals: List[UTBotSignal] = []
        
        # Only iterate over indices where a flip occurred
        flip_indices = np.where(flips != 0)[0]
        
        is_trending_arr = is_trending.values if isinstance(is_trending, pd.Series) else np.full(len(df), is_trending)
        atr_arr = atr.values
        adx_arr = adx.values
        
        open_arr = df['open'].values
        high_arr = df['high'].values
        low_arr = df['low'].values
        close_arr = df['close'].values
        
        timestamps = df.index
        cap_rpct = (cap * rpct / 100.0)

        for i in flip_indices:
            if self.signal_mode == "confirmed" and i == len(df) - 1:
                continue
            ts_val = timestamps[i] if isinstance(timestamps, pd.DatetimeIndex) else None
            in_session = self._in_session(ts_val, timeframe) if ts_val else True
            
            if not in_session:
                continue
                
            if self.strict_adx and not is_trending_arr[i]:
                continue

            curr_src_val = float(src_arr[i])
            curr_ts_val = float(ts_arr[i])
            stop_dist = abs(curr_src_val - curr_ts_val)
            
            raw_c = {
                "open": float(open_arr[i]),
                "high": float(high_arr[i]),
                "low": float(low_arr[i]),
                "close": float(close_arr[i])
            }
            
            adx_val = float(adx_arr[i])
            if np.isnan(adx_val):
                adx_val = 0.0

            if flips[i] == 1:
                base_qty = lots * lot_size
                if stop_dist > 0:
                    sizing_stop = self._effective_sizing_stop(stop_dist, atr_arr[i], curr_src_val)
                    raw_qty = int(cap_rpct / sizing_stop)
                    # Hard cap at 5x base qty to prevent uncapped sizing risk on tiny stop_dist
                    suggested_qty = min(raw_qty, base_qty * 5)
                else:
                    suggested_qty = base_qty
                
                # Ensure it's a multiple of lot_size
                suggested_qty = max(lot_size, (suggested_qty // lot_size) * lot_size)
                
                sig = UTBotSignal(
                    timestamp=ts_val or datetime.now(),
                    signal_type="BUY",
                    price=curr_src_val,
                    trailing_stop=curr_ts_val,
                    stop_distance=float(stop_dist),
                    atr_value=float(atr_arr[i]),
                    adx_value=adx_val,
                    position_state=1,
                    bar_index=int(i),
                    instrument=instrument,
                    timeframe=timeframe,
                    suggested_qty=suggested_qty,
                    raw_candle=raw_c
                )
                signals.append(sig)
            elif flips[i] == -1:
                base_qty = lots * lot_size
                if stop_dist > 0:
                    sizing_stop = self._effective_sizing_stop(stop_dist, atr_arr[i], curr_src_val)
                    raw_qty = int(cap_rpct / sizing_stop)
                    suggested_qty = min(raw_qty, base_qty * 5)
                else:
                    suggested_qty = base_qty
                
                suggested_qty = max(lot_size, (suggested_qty // lot_size) * lot_size)
                
                sig = UTBotSignal(
                    timestamp=ts_val or datetime.now(),
                    signal_type="SELL",
                    price=curr_src_val,
                    trailing_stop=curr_ts_val,
                    stop_distance=float(stop_dist),
                    atr_value=float(atr_arr[i]),
                    adx_value=adx_val,
                    position_state=-1,
                    bar_index=int(i),
                    instrument=instrument,
                    timeframe=timeframe,
                    suggested_qty=suggested_qty,
                    raw_candle=raw_c
                )
                signals.append(sig)

        # ── Update State ──
        last_idx = len(df) - 1
        state_idx = last_idx - 1 if self.signal_mode == "confirmed" else last_idx
        state.trailing_stop = float(ts.iloc[state_idx])
        state.position = int(positions[state_idx])
        state.bar_count = len(df)
        if signals:
            state.last_signal = signals[-1]
            if signals[-1].signal_type == "BUY":
                state.last_entry_price = signals[-1].price
                state.last_entry_time = signals[-1].timestamp
            else:
                state.last_exit_price = signals[-1].price
            existing = {
                (sig.timestamp, sig.signal_type, sig.instrument, sig.timeframe)
                for sig in state.signals_history
            }
            state.signals_history.extend(
                sig
                for sig in signals
                if (sig.timestamp, sig.signal_type, sig.instrument, sig.timeframe) not in existing
            )
            # Keep only last 200 signals in memory
            if len(state.signals_history) > 200:
                state.signals_history = state.signals_history[-200:]

        # ── Risk Calculation ──
        curr_price = float(src.iloc[last_idx])
        stop_dist = abs(curr_price - state.trailing_stop)
        risk_amt = cap * rpct / 100.0
        sizing_stop = self._effective_sizing_stop(stop_dist, atr_arr[last_idx], curr_price)
        pos_size = int(risk_amt / sizing_stop) if stop_dist > 0 else 0

        # ── Build color arrays for chart ──
        # Color based on trailing stop vs close price (like TradingView)
        # Green = stop below price (bullish), Red = stop above price (bearish)
        ts_colors = np.where(src_arr > ts_arr, "green", "red")

        return {
            "instrument": instrument,
            "timeframe": timeframe,
            "candles": len(df),
            "trailing_stop": ts.values.tolist(),
            "trailing_stop_colors": ts_colors.tolist(),
            "positions": positions.tolist(),
            "atr": atr.values.tolist(),
            "adx": adx.values.tolist(),
            "di_plus": di_plus.values.tolist(),
            "di_minus": di_minus.values.tolist(),
            "signals": signals,
            "state": {
                "position": state.position,
                "position_label": "LONG" if state.position == 1 else ("SHORT" if state.position == -1 else "FLAT"),
                "trailing_stop": state.trailing_stop,
                "last_entry_price": state.last_entry_price,
                "last_exit_price": state.last_exit_price,
                "stop_distance": stop_dist,
                "suggested_qty": pos_size,
                "current_price": curr_price,
                "atr_value": float(atr.iloc[last_idx]),
                "adx_value": float(adx.iloc[last_idx]) if not pd.isna(adx.iloc[last_idx]) else 0,
                "is_trending": bool(is_trending.iloc[last_idx]) if not pd.isna(is_trending.iloc[last_idx]) else False,
            },
        }
