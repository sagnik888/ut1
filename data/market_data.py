"""
Market Data Provider — Institutional Broker-Master Integration
═══════════════════════════════════════════════════════════════
Primary: AngelOne SmartAPI for 100% accurate Index Spot (LTT)
Secondary: Yahoo Finance for backfill & historical candles
"""

import time
import json
import threading
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta, time as dtime, date
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from loguru import logger
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
from data.market_calendar import load_nse_holidays

try:
    import pyotp
    from SmartApi import SmartConnect
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
    HAS_SMARTAPI = True
except ImportError:
    SmartConnect = None
    SmartWebSocketV2 = None
    HAS_SMARTAPI = False

YAHOO_TICKERS = {
    "NIFTY": "^NSEI",
    "BANKNIFTY": "^NSEBANK",
    "SENSEX": "^BSESN",
    "MIDCPNIFTY": "NIFTY_MID_SELECT.NS",
}
INDEX_SPOT_TOKENS = {
    "NIFTY": ("99926000", "NSE"),
    "BANKNIFTY": ("99926009", "NSE"),
    "SENSEX": ("99919000", "BSE"),
    "MIDCPNIFTY": ("99926074", "NSE"),
}
INDEX_SHORT_TOKENS = {
    "26000": "NIFTY",
    "26009": "BANKNIFTY",
    "19000": "SENSEX",
    "26074": "MIDCPNIFTY",
}
FYERS_INDEX_SYMBOLS = {
    "NIFTY": "NSE:NIFTY50-INDEX",
    "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
    "SENSEX": "BSE:SENSEX-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
}
FYERS_SYMBOL_TO_INDEX = {v: k for k, v in FYERS_INDEX_SYMBOLS.items()}
INDEX_STRIKE_INTERVALS = {"NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100, "MIDCPNIFTY": 25}
YAHOO_INTERVALS = {"1min": "1m", "5min": "5m", "15min": "15m"}
YAHOO_MAX_DAYS = {"1m": 7, "5m": 60, "15m": 60}

import functools

def retry_on_exception(retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Exponential backoff retry decorator"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            m_delay = delay
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == retries - 1: raise e
                    logger.warning(f"Retry {i+1}/{retries} for {func.__name__} after {m_delay}s: {e}")
                    time.sleep(m_delay)
                    m_delay *= backoff
            return func(*args, **kwargs)
        return wrapper
    return decorator

class MarketDataProvider:
    def __init__(
        self,
        api_key: str = "",
        client_id: str = "",
        password: str = "",
        totp_secret: str = "",
        start_streams: bool = True,
    ):
        self.api_key = api_key
        self.client_id = client_id
        self.password = password
        self.totp_secret = totp_secret
        self.start_streams = bool(start_streams)
        self.smart_api: Optional[SmartConnect] = None
        self.is_connected = False
        self._rest_lock = threading.Lock()
        self._ltp_cache: Dict[str, Dict] = {}
        
        # Versioned exchange calendar with optional data_store overrides.
        self.nse_holidays = load_nse_holidays()

        self._yf_cache: Dict[str, Dict] = {}
        self._cache_ttl = 5
        self._ltp_cache: Dict[str, Dict[str, Any]] = {}
        self._ws_cache: Dict[str, Dict[str, Any]] = {}
        self._active_angel_tokens = {1: set(["26000", "26009", "26017", "26074"]), 3: set(["19000"])}
        self._last_ltp_time: float = 0
        self._last_ws_tick: float = 0.0  # Compatibility aggregate; provider health uses separate clocks.
        self._last_angel_ws_tick: float = 0.0
        self._last_fyers_ws_tick: float = 0.0
        self._yahoo_request_count: int = 0
        self._yahoo_last_used_at: Optional[float] = None
        self._instruments_df: Optional[pd.DataFrame] = None
        self._inst_cache_path = Path("data_store/instruments.csv")
        self._option_chain_meta: Dict[str, Dict[str, Any]] = {}
        self._fyers_refresh_disabled = False
        self._fyers_auth_required = False
        self._fyers_auth_reason = ""
        self._fyers_token_updated_at = None
        self.fyers: Optional[fyersModel.FyersModel] = None
        self._load_fyers_token()

    def _load_fyers_token(self):
        try:
            token_path = Path("fyers_token.json")
            if not token_path.exists():
                logger.warning("⚠️ fyers_token.json not found")
                return

            with open(token_path, "r") as f:
                token_data = json.load(f)
                access_token = token_data.get("access_token")
                
            if access_token:
                from config.settings import get_settings
                settings = get_settings()
                self.fyers = fyersModel.FyersModel(client_id=settings.fyers_app_id, token=access_token, log_path="")
                
                # Verify token status
                profile = self.fyers.get_profile()
                if profile and profile.get('s') == 'error' and profile.get('code') in [-8, -17]:
                    logger.warning(f"🔄 Fyers Token Issue (Code {profile.get('code')}). Attempting refresh...")
                    if not self._refresh_fyers_token(token_data):
                        self.fyers = None
                        self._mark_fyers_auth_required("Fyers token expired and manual login is required")
                else:
                    self._clear_fyers_auth_required()
                    logger.info("✅ Fyers API Connected for Data!")
                    if self.start_streams:
                        self.start_fyers_websocket(access_token)
                    else:
                        logger.info("Fyers WebSocket skipped for history-only provider.")
            else:
                logger.warning("⚠️ Fyers access token missing in fyers_token.json")
                self._mark_fyers_auth_required("Fyers access token missing")
        except Exception as e:
            self.fyers = None
            self._mark_fyers_auth_required(f"Failed to load Fyers token: {e}")
            logger.warning(f"⚠️ Failed to load Fyers token: {e}")

    def start_fyers_websocket(self, access_token: str):
        """Start Fyers WebSocket in a separate thread"""
        self._fyers_ws_generation = getattr(self, "_fyers_ws_generation", 0) + 1
        ws_generation = self._fyers_ws_generation
        if hasattr(self, "_fyers_ws") and self._fyers_ws:
            try:
                self._fyers_ws.close()
            except: pass
            
        try:
            def on_message(msg):
                if isinstance(msg, list):
                    messages = msg
                else:
                    messages = [msg]
                    
                for m in messages:
                    symbol = m.get('symbol')
                    ltp = m.get('ltp')
                    oi = m.get('oi')
                    vol = m.get('vol_traded_today')
                    
                    if symbol and ltp:
                        last_ws_debug = getattr(self, "_last_ws_debug_log", {})
                        now_ts = time.time()
                        self._ws_cache[symbol] = {
                            'ltp': float(ltp),
                            'oi': int(oi) if oi else 0,
                            'vol': int(vol) if vol else 0,
                            'change': float(m.get('ch') or 0.0),
                            'change_pct': float(m.get('chp') or 0.0),
                            'time': now_ts,
                            'source': 'fyers_ws',
                        }
                        if now_ts - last_ws_debug.get(symbol, 0) >= 10:
                            logger.debug(f"Fyers WS Tick: {symbol} -> {ltp}")
                            last_ws_debug[symbol] = now_ts
                            self._last_ws_debug_log = last_ws_debug
                        
                        self._last_fyers_ws_tick = time.time()
                        self._last_ws_tick = self._last_fyers_ws_tick
                        
                        if hasattr(self, 'on_tick') and self.on_tick:
                            if symbol in FYERS_SYMBOL_TO_INDEX:
                                self.on_tick(FYERS_SYMBOL_TO_INDEX[symbol], float(ltp), int(vol) if vol else 0, datetime.now())

            def on_open():
                logger.info("🔌 Fyers WebSocket Connected")
                self._fyers_ws_connected = True
                self._last_fyers_ws_tick = time.time()
                self._last_ws_tick = self._last_fyers_ws_tick
                if not getattr(self, "_fyers_ws_auth_failed", False):
                    self._fyers_ws_retry_count = 0
                symbols = list(FYERS_INDEX_SYMBOLS.values())
                self._fyers_ws.subscribe(symbols=symbols)
                logger.info(f"Subscribed to Fyers symbols: {symbols}")

            def on_error(e):
                self._fyers_ws_connected = False
                err_text = str(e).lower()
                if (
                    "valid token" in err_text
                    or "invalid token" in err_text
                    or "'code': -300" in err_text
                    or '"code": -300' in err_text
                ):
                    self._fyers_ws_auth_failed = True
                    self._mark_fyers_auth_required("Fyers WebSocket token invalid; refresh Fyers token before reconnecting")
                    logger.error(f"Fyers WS auth failed; reconnect loop stopped: {e}")
                    try:
                        self._fyers_ws.close()
                    except Exception:
                        pass
                    return
                logger.error(f"Fyers WS Error: {e}")

            def on_close():
                self._fyers_ws_connected = False
                logger.warning("🔌 Fyers WebSocket Closed")

            self._fyers_ws = data_ws.FyersDataSocket(
                access_token=access_token,
                log_path="",
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            self._fyers_ws.on_open = on_open
            
            def _ws_runner():
                self._fyers_ws_retry_count = 0
                self._fyers_ws_auth_failed = False
                while (
                    getattr(self, "_fyers_ws_generation", 0) == ws_generation
                    and not getattr(self, "_fyers_ws_auth_failed", False)
                ):
                    try:
                        self._fyers_ws.connect()
                    except Exception as e:
                        if getattr(self, "_fyers_ws_generation", 0) != ws_generation:
                            break
                        logger.error(f"Fyers WS Runner error: {e}")

                    if (
                        getattr(self, "_fyers_ws_generation", 0) != ws_generation
                        or getattr(self, "_fyers_ws_auth_failed", False)
                    ):
                        break
                    
                    while (
                        getattr(self, "_fyers_ws_generation", 0) == ws_generation
                        and getattr(self, "_fyers_ws_connected", False)
                        and not getattr(self, "_fyers_ws_auth_failed", False)
                    ):
                        if self._last_fyers_ws_tick > 0 and (time.time() - self._last_fyers_ws_tick > 60):
                            if not self.is_market_open():
                                time.sleep(10)
                                continue
                            logger.warning("Fyers WS Silent Disconnect Detected (No ticks > 60s).")
                            try:
                                self._fyers_ws.close()
                            except: pass
                            break
                        time.sleep(1)

                    self._fyers_ws_retry_count += 1
                    backoff = min(60, 2 ** self._fyers_ws_retry_count)
                    if getattr(self, "_fyers_ws_auth_failed", False):
                        break
                    logger.warning(f"🔄 Fyers WS Reconnecting in {backoff}s...")
                    time.sleep(backoff)

                if getattr(self, "_fyers_ws_auth_failed", False):
                    logger.warning("Fyers WS reconnect paused until token refresh.")

            import threading
            t = threading.Thread(target=_ws_runner, daemon=True)
            t.start()
            logger.info("🚀 Fyers WebSocket Thread Started")
            
        except Exception as e:
            logger.error(f"Failed to start Fyers WebSocket: {e}")

    def _mark_fyers_auth_required(self, reason: str):
        self._fyers_auth_required = True
        self._fyers_auth_reason = reason

    def _clear_fyers_auth_required(self):
        self._fyers_auth_required = False
        self._fyers_auth_reason = ""

    def _refresh_fyers_token(self, token_data: Dict) -> bool:
        """Automatically refresh Fyers access token using refresh_token"""
        if self._fyers_refresh_disabled:
            return False
        try:
            from config.settings import get_settings
            settings = get_settings()
            refresh_token = token_data.get("refresh_token")
            
            if not refresh_token:
                logger.error("❌ Cannot refresh Fyers token: refresh_token missing")
                self._mark_fyers_auth_required("Fyers refresh token missing")
                return False

            if not settings.fyers_secret_key:
                logger.error("❌ Cannot refresh Fyers token: FYERS_SECRET_KEY missing in .env")
                self._mark_fyers_auth_required("FYERS_SECRET_KEY missing in .env")
                return False

            # Fyers refresh requires a SHA256 hash of app_id + ":" + secret_key
            import requests
            import hashlib
            app_id = settings.fyers_app_id
            secret = settings.fyers_secret_key
            app_id_hash = hashlib.sha256((app_id + ":" + secret).encode()).hexdigest()
            
            # Pin must also be hashed
            pin = settings.fyers_password
            pin_hash = hashlib.sha256(pin.encode()).hexdigest()
            
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "appIdHash": app_id_hash,
                "pin": pin_hash
            }
            
            url = "https://api-t1.fyers.in/api/v3/validate-refresh-token"
            response_raw = requests.post(url, json=payload, timeout=20)
            response = response_raw.json()
            if response and response.get('s') == 'ok' and response.get('access_token'):
                new_access_token = response.get('access_token')
                token_data['access_token'] = new_access_token
                
                # Update file
                with open("fyers_token.json", "w") as f:
                    json.dump(token_data, f)
                
                # Update current instance
                self.fyers = fyersModel.FyersModel(client_id=settings.fyers_app_id, token=new_access_token, log_path="")
                self._fyers_token_updated_at = datetime.now().isoformat()
                self._fyers_refresh_disabled = False
                self._clear_fyers_auth_required()
                logger.success("✅ Fyers Token Refreshed Successfully!")
                self.start_fyers_websocket(new_access_token)
                return True
            else:
                if response and response.get("code") == -16:
                    self._fyers_refresh_disabled = True
                    self._mark_fyers_auth_required("Fyers automatic refresh is disabled by broker policy. Manual login is required.")
                    logger.warning("Fyers token refresh is disabled by broker policy; using available cache/fallback data.")
                else:
                    self._mark_fyers_auth_required(f"Fyers token refresh failed: {response}")
                    logger.error(f"❌ Fyers Token Refresh Failed: {response}")
                return False
        except Exception as e:
            self._mark_fyers_auth_required(f"Error during Fyers token refresh: {e}")
            logger.error(f"❌ Error during Fyers token refresh: {e}")
            return False

    def get_fyers_auth_status(self) -> Dict[str, Any]:
        token_path = Path("fyers_token.json")
        token_mtime = None
        if token_path.exists():
            token_mtime = datetime.fromtimestamp(token_path.stat().st_mtime).isoformat()
        return {
            "connected": self.fyers is not None,
            "auth_required": self._fyers_auth_required,
            "reason": self._fyers_auth_reason,
            "refresh_disabled": self._fyers_refresh_disabled,
            "token_updated_at": self._fyers_token_updated_at or token_mtime,
        }

    def get_fyers_auth_url(self) -> str:
        from config.settings import get_settings
        settings = get_settings()
        session = fyersModel.SessionModel(
            client_id=settings.fyers_app_id,
            secret_key=settings.fyers_secret_key,
            redirect_uri=settings.fyers_redirect_uri,
            response_type="code",
            grant_type="authorization_code",
        )
        return session.generate_authcode()

    def update_fyers_token_from_auth_code(self, auth_code: str) -> Dict[str, Any]:
        """Complete manual Fyers login after the user pastes auth_code or the redirect URL."""
        from config.settings import get_settings
        settings = get_settings()
        code = (auth_code or "").strip()
        
        # Robust parsing for both full URL and raw code
        if "auth_code=" in code or "code=" in code:
            from urllib.parse import parse_qs, urlparse
            parsed = urlparse(code)
            query = parse_qs(parsed.query)
            if not query and parsed.fragment:
                query = parse_qs(parsed.fragment)
            code = query.get("auth_code", query.get("code", [code]))[0]
            
        if not code:
            return {"status": "error", "message": "Missing Fyers auth_code"}

        session = fyersModel.SessionModel(
            client_id=settings.fyers_app_id,
            secret_key=settings.fyers_secret_key,
            redirect_uri=settings.fyers_redirect_uri,
            response_type="code",
            grant_type="authorization_code",
        )
        session.set_token(code)
        response = session.generate_token()
        if not response or response.get("s") != "ok" or not response.get("access_token"):
            self._mark_fyers_auth_required(f"Manual Fyers token generation failed: {response}")
            return {"status": "error", "message": "Fyers token generation failed", "response": response}

        token_data = {
            "access_token": response.get("access_token"),
            "refresh_token": response.get("refresh_token"),
            "timestamp": datetime.now().isoformat(),
        }
        with open("fyers_token.json", "w") as f:
            json.dump(token_data, f, indent=2)

        self.fyers = fyersModel.FyersModel(client_id=settings.fyers_app_id, token=token_data["access_token"], log_path="")
        profile = self.fyers.get_profile()
        if profile and profile.get("s") == "error":
            self.fyers = None
            self._mark_fyers_auth_required(f"New Fyers token validation failed: {profile}")
            return {"status": "error", "message": "New Fyers token did not validate", "response": profile}

        self._fyers_token_updated_at = token_data["timestamp"]
        self._fyers_refresh_disabled = False
        self._clear_fyers_auth_required()
        logger.success("✅ Fyers manual authorization completed successfully.")
        if getattr(self, "start_streams", True):
            self.start_fyers_websocket(token_data["access_token"])
        return {"status": "ok", "message": "Fyers token updated", "profile": profile}

    def is_market_holiday(self, dt: datetime = None) -> bool:
        """Check if current or given date is an NSE market holiday"""
        dt = dt or datetime.now()
        date_str = dt.strftime("%Y-%m-%d")
        return date_str in self.nse_holidays

    def is_market_open(self, dt: datetime = None) -> bool:
        """Robust check for NSE Market Status (09:15 - 15:30, Mon-Fri, No Holidays)"""
        dt = dt or datetime.now()
        # 1. Weekend check
        if dt.weekday() >= 5: return False
        # 2. Holiday check
        if self.is_market_holiday(dt): return False
        # 3. Session time check (09:15 - 15:30)
        curr_time = dt.time()
        start_time = dtime(9, 15)
        end_time = dtime(15, 30)
        return start_time <= curr_time <= end_time

    def _connect_attempt(self) -> bool:
        if not HAS_SMARTAPI or not self.api_key:
            return False
            
        import threading
        if not hasattr(self, "_connect_lock"):
            self._connect_lock = threading.Lock()
            
        with self._connect_lock:
            # Check if another thread already connected while we were waiting
            if getattr(self, "is_connected", False) and getattr(self, "smart_api", None):
                # Optionally check if token is valid, but returning True is safe
                return True
                
            self.smart_api = SmartConnect(api_key=self.api_key)
            totp = pyotp.TOTP(self.totp_secret).now()
            data = self.smart_api.generateSession(self.client_id, self.password, totp)
            if not data.get('status'):
                raise Exception(f"Login failed: {data.get('message', 'Unknown error')}")
            
        self.is_connected = True
        logger.success(f"✅ AngelOne Connected: {self.client_id}")
        
        # Start WebSocket only for the live provider. History workers use REST only.
        if self.start_streams:
            jwt_token = data.get('data', {}).get('jwtToken')
            feed_token = self.smart_api.getfeedToken()
            self.start_websocket(jwt_token, feed_token)
        return True

    @retry_on_exception(retries=3, delay=1.0)
    def _connect_with_retry(self) -> bool:
        return self._connect_attempt()

    def connect(self) -> bool:
        try:
            return self._connect_with_retry()
        except Exception as e:
            logger.error(f"❌ AngelOne connection failed after retries: {e}")
            return False

    def start_websocket(self, jwt_token: str, feed_token: str):
        """Start AngelOne WebSocket in a separate thread"""
        if not SmartWebSocketV2:
            logger.warning("SmartWebSocketV2 not available")
            return
            
        try:
            self.sws = SmartWebSocketV2(jwt_token, self.api_key, self.client_id, feed_token)
            
            def on_data(ws, tick_data):
                token = tick_data.get('token')
                ltp = tick_data.get('last_traded_price')
                oi = tick_data.get('open_interest', 0)
                vol = tick_data.get('volume_trade_for_the_day', 0)
                
                if token and ltp:
                    now_ts = time.time()
                    self._last_angel_ws_tick = now_ts
                    self._last_ws_tick = now_ts
                    self._last_ltp_time = now_ts
                    price = float(ltp) / 100.0
                    # Store as dict instead of float
                    self._ws_cache[token] = {
                        'ltp': price,
                        'oi': int(oi),
                        'vol': int(vol),
                        'time': now_ts,
                        'source': 'angel_ws',
                    }
                    last_ws_debug = getattr(self, "_last_ws_debug_log", {})
                    if now_ts - last_ws_debug.get(token, 0) >= 10:
                        logger.debug(f"WS Tick: {token} -> {price} | OI: {oi} | Vol: {vol}")
                        last_ws_debug[token] = now_ts
                        self._last_ws_debug_log = last_ws_debug
                    
                    if hasattr(self, 'on_tick') and self.on_tick:
                        if token in INDEX_SHORT_TOKENS:
                            self.on_tick(INDEX_SHORT_TOKENS[token], price, int(vol), datetime.now())

            def on_open(ws):
                logger.info("🔌 AngelOne WebSocket Connected")
                self._angel_ws_retry_count = 0
                self._angel_ws_connected = True
                self._last_angel_ws_tick = time.time()
                self._last_ws_tick = self._last_angel_ws_tick
                
                token_list = [{"exchangeType": exch, "tokens": list(toks)} for exch, toks in self._active_angel_tokens.items() if toks]
                self.sws.subscribe(correlation_id="ut1_default", mode=2, token_list=token_list)
                logger.info(f"Subscribed to tokens (Mode 2): {token_list}")

            def on_error(ws, error):
                self._angel_ws_connected = False
                logger.error(f"WS Error: {error}")

            def on_close(ws):
                self._angel_ws_connected = False
                logger.warning("🔌 AngelOne WebSocket Closed")

            self.sws.on_data = on_data
            self.sws.on_open = on_open
            self.sws.on_error = on_error
            self.sws.on_close = on_close
            def _ws_runner():
                self._angel_ws_retry_count = 0
                
                def watchdog():
                    while True:
                        if self._last_angel_ws_tick > 0 and getattr(self, '_angel_ws_connected', False) and (time.time() - self._last_angel_ws_tick > 60):
                            if self.is_market_open():
                                logger.warning("AngelOne WS Silent Disconnect Detected (No ticks > 60s).")
                                try:
                                    if hasattr(self.sws, 'close'):
                                        self.sws.close()
                                except Exception:
                                    pass
                        time.sleep(10)
                
                import threading
                threading.Thread(target=watchdog, daemon=True).start()

                while True:
                    try:
                        self.sws.connect()
                    except Exception as e:
                        logger.error(f"AngelOne WS Runner error: {e}")
                    
                    self._angel_ws_retry_count += 1
                    backoff = min(60, 2 ** self._angel_ws_retry_count)
                    logger.warning(f"🔄 AngelOne WS Reconnecting in {backoff}s...")
                    time.sleep(backoff)

            import threading
            t = threading.Thread(target=_ws_runner, daemon=True)
            t.start()
            logger.info("🚀 WebSocket Thread Started")
            
        except Exception as e:
            logger.error(f"Failed to start WebSocket: {e}")

    def subscribe_tokens(self, tokens: List[str], exchange_type: int = 1):
        """Dynamically subscribe to more tokens"""
        self._active_angel_tokens.setdefault(exchange_type, set()).update(tokens)
        if hasattr(self, 'sws') and self.sws:
            try:
                # Mode 2 = Full Quote (LTP + OI + Vol)
                self.sws.subscribe(correlation_id="ut1_dynamic", mode=2, token_list=[{"exchangeType": exchange_type, "tokens": tokens}])
                logger.info(f"📡 AngelOne: Dynamically subscribed to {len(tokens)} tokens (Mode 2)")
            except Exception as e:
                logger.error(f"Failed to subscribe to tokens: {e}")

    def get_source_health(self) -> Dict[str, Any]:
        """Check each provider independently; Yahoo is an idle emergency source."""
        now = time.time()
        is_live = self.is_market_open()
        freshness_threshold = 45.0
        angel_tick = float(getattr(self, "_last_angel_ws_tick", 0.0) or 0.0)
        fyers_tick = float(getattr(self, "_last_fyers_ws_tick", 0.0) or 0.0)
        angel_age = None if angel_tick <= 0 else max(0.0, now - angel_tick)
        fyers_age = None if fyers_tick <= 0 else max(0.0, now - fyers_tick)

        angel_fresh = angel_age is not None and angel_age < freshness_threshold
        fyers_fresh = fyers_age is not None and fyers_age < freshness_threshold
        angel_health = bool(self.is_connected and (angel_fresh if is_live else True))
        fyers_available = self.fyers is not None and not self._fyers_auth_required
        fyers_health = bool(fyers_available and (fyers_fresh if is_live else True))

        return {
            "angel": angel_health,
            "angel_tick_age_seconds": round(angel_age, 2) if angel_age is not None else None,
            "fyers": fyers_health,
            "fyers_tick_age_seconds": round(fyers_age, 2) if fyers_age is not None else None,
            "broker_degraded": bool(is_live and not (angel_health and fyers_health)),
            "all_brokers_unavailable": bool(is_live and not angel_health and not fyers_health),
            "fyers_auth_required": self._fyers_auth_required,
            "fyers_auth_reason": self._fyers_auth_reason,
            "fyers_refresh_disabled": self._fyers_refresh_disabled,
            "yahoo": True,
            "yahoo_mode": "emergency_used" if self._yahoo_request_count else "idle_standby",
            "yahoo_request_count": int(self._yahoo_request_count),
            "yahoo_last_used_at": (
                datetime.fromtimestamp(self._yahoo_last_used_at).isoformat()
                if self._yahoo_last_used_at
                else None
            ),
        }

    def get_market_source_snapshot(self, symbol: str) -> Dict[str, Any]:
        """Return source latency metadata for live-vs-delayed signal weighting."""
        now = time.time()

        def _age_seconds(value: Any) -> Optional[float]:
            try:
                ts = value.get("time") if isinstance(value, dict) else None
                if isinstance(ts, datetime):
                    return max(0.0, (datetime.now() - ts).total_seconds())
                if ts:
                    return max(0.0, now - float(ts))
            except Exception:
                return None
            return None

        def _source_record(name: str, value: Any, latency_class: str, entry_eligible: bool) -> Dict[str, Any]:
            age = _age_seconds(value)
            return {
                "source": name,
                "latency_class": latency_class,
                "age_seconds": round(age, 2) if age is not None else None,
                "entry_eligible": bool(entry_eligible),
                "ltp": float(value.get("ltp", 0.0)) if isinstance(value, dict) else 0.0,
            }

        records: List[Dict[str, Any]] = []
        token_info = INDEX_SPOT_TOKENS.get(symbol)
        if token_info:
            token = token_info[0]
            short_token = token[3:] if token.startswith("999") else token
            for key in (token, short_token):
                cached = self._ws_cache.get(key)
                if isinstance(cached, dict):
                    source = str(cached.get("source") or "angel_ws")
                    age = _age_seconds(cached)
                    records.append(_source_record(source, cached, "REALTIME", age is not None and age <= 6.0))

        fyers_symbol = FYERS_INDEX_SYMBOLS.get(symbol)
        if fyers_symbol and isinstance(self._ws_cache.get(fyers_symbol), dict):
            cached = self._ws_cache[fyers_symbol]
            age = _age_seconds(cached)
            records.append(_source_record("fyers_ws", cached, "REALTIME", age is not None and age <= 6.0))

        cached_ltp = self._ltp_cache.get(symbol)
        if isinstance(cached_ltp, dict):
            source = str(cached_ltp.get("source") or "unknown")
            if source == "yahoo":
                records.append(_source_record("yahoo", cached_ltp, "DELAYED", False))
            elif source in {"angel", "fyers"}:
                age = _age_seconds(cached_ltp)
                records.append(_source_record(f"{source}_rest", cached_ltp, "NEAR_REALTIME", age is not None and age <= 15.0))

        if not any(r["source"] == "yahoo" for r in records):
            records.append({
                "source": "yahoo",
                "latency_class": "DELAYED",
                "age_seconds": None,
                "entry_eligible": False,
                "ltp": 0.0,
            })

        entry_sources = [r for r in records if r.get("entry_eligible")]
        best = min(
            records,
            key=lambda r: (
                0 if r.get("entry_eligible") else 1,
                999999.0 if r.get("age_seconds") is None else float(r.get("age_seconds") or 0.0),
            ),
        ) if records else {}

        return {
            "instrument": symbol,
            "sources": records,
            "freshest": best,
            "entry_eligible": bool(entry_sources),
            "context_only_sources": [r for r in records if not r.get("entry_eligible")],
        }

    # ═══════════════════════════════════════════════════════════
    # UNIFIED PRICE BRIDGE (The "Master" Lock)
    # ═══════════════════════════════════════════════════════════
    
    @retry_on_exception(retries=1, delay=0.1)
    def get_latest_price_by_token(self, token: str, symbol: str, exchange: str = "NSE") -> float:
        """The Master Source for Index Spot Prices — Weekend Precision Fix"""
        def _remember_ltp_time(val: Any) -> float:
            if isinstance(val, dict):
                self._last_ltp_time = float(val.get("time") or time.time())
                return val.get('ltp', 0.0)
            self._last_ltp_time = time.time()
            return val

        if symbol in INDEX_SPOT_TOKENS:
            candidates = []
            now_ts = time.time()
            short_token = token[3:] if token.startswith('999') else token
            fyers_symbol = FYERS_INDEX_SYMBOLS.get(symbol, symbol)
            for key in (token, short_token, fyers_symbol):
                cached = self._ws_cache.get(key)
                if isinstance(cached, dict) and float(cached.get("ltp", 0.0) or 0.0) > 0:
                    age = now_ts - float(cached.get("time") or 0.0)
                    if age <= 15.0:
                        candidates.append((age, cached))
            if candidates:
                candidates.sort(key=lambda item: item[0])
                return _remember_ltp_time(candidates[0][1])

        # 1. WEBSOCKET CACHE (Fastest)
        if token in self._ws_cache:
            return _remember_ltp_time(self._ws_cache[token])
            
        # Fallback for indices where token might be stored without '999' prefix in WS
        short_token = token[3:] if token.startswith('999') else token
        if short_token in self._ws_cache:
            return _remember_ltp_time(self._ws_cache[short_token])

        # Fallback for Fyers WebSocket cache (stored by symbol)
        fyers_symbol = FYERS_INDEX_SYMBOLS.get(symbol, symbol)
        
        if fyers_symbol in self._ws_cache:
            return _remember_ltp_time(self._ws_cache[fyers_symbol])
            
        # 2. BROKER LIVE FEED (REST Fallback - TIER 2: FYERS)
        if getattr(self, 'fyers', None):
            try:
                f_sym = FYERS_INDEX_SYMBOLS.get(symbol)
                if f_sym:
                    f_resp = self.fyers.quotes({"symbols": f_sym})
                    if f_resp and f_resp.get('s') == 'ok':
                        f_ltp = f_resp.get('d', [{}])[0].get('v', {}).get('lp', 0)
                        if f_ltp > 0:
                            self._last_ltp_time = time.time()
                            self._ltp_cache[symbol] = {'ltp': f_ltp, 'time': datetime.now(), 'source': 'fyers'}
                            return f_ltp
            except: pass

        # 3. BROKER LIVE FEED (REST Fallback - TIER 1: AngelOne)
        if self.is_connected and self.smart_api:
            # Rate limit REST calls to avoid exceeding access rate (max 2 per second)
            with self._rest_lock:
                # Double-check cache inside lock to prevent stampedes (e.g. 4 threads asking for INDIAVIX)
                cached = self._ltp_cache.get(symbol)
                if isinstance(cached, dict) and (datetime.now() - cached.get('time', datetime.min)).total_seconds() < 5.0:
                    return cached['ltp']

                now = time.time()
                last_rest_time = getattr(self, "_last_rest_time", 0)
                if now - last_rest_time < 0.5:
                    time.sleep(0.5 - (now - last_rest_time))
                    self._last_rest_time = time.time()
                else:
                    self._last_rest_time = now

            try:
                trading_symbol = "INDEX" if symbol in INDEX_SPOT_TOKENS else symbol
                resp = self.smart_api.ltpData(exchange, trading_symbol, token)
                
                # Check for Invalid Token error
                if not resp.get('status') and resp.get('errorCode') == 'AG8001':
                    logger.warning("🔄 AngelOne Session Expired (AG8001). Re-authenticating...")
                    if self.connect():
                        # Retry once after re-connecting
                        resp = self.smart_api.ltpData(exchange, trading_symbol, token)
                    else:
                        return self._ltp_cache.get(symbol, {}).get('ltp', 0.0)

                if resp.get('status') and resp.get('data'):
                    ltp = float(resp['data'].get('ltp', 0.0))
                    close_px = float(resp['data'].get('close', 0.0))
                    
                    if close_px > 0:
                        if not hasattr(self, "_prev_close_global_cache"):
                            self._prev_close_global_cache = {}
                        self._prev_close_global_cache[symbol] = close_px

                    if ltp > 0:
                        self._last_ltp_time = time.time()
                        self._ltp_cache[symbol] = {'ltp': ltp, 'time': datetime.now(), 'source': 'angel'}
                        return ltp
            except Exception as e:
                if "Invalid Token" in str(e) or "AG8001" in str(e):
                    logger.warning("🔄 AngelOne Exception: Invalid Token. Re-authenticating...")
                    self.connect()
                pass
        
        # 4. LAST RESORT: YAHOO (TIER 3 - 15min DELAYED)
        ticker = YAHOO_TICKERS.get(symbol)
        if ticker:
            try:
                self._yahoo_request_count += 1
                y_ticker = yf.Ticker(ticker)
                y_data = y_ticker.fast_info
                y_ltp = y_data.last_price
                if y_ltp > 0:
                    logger.warning(f"⚠️ EMERGENCY: Using 15min delayed Yahoo data for {symbol}")
                    self._last_ltp_time = time.time()
                    self._yahoo_last_used_at = self._last_ltp_time
                    self._ltp_cache[symbol] = {'ltp': y_ltp, 'time': datetime.now(), 'source': 'yahoo'}
                    return y_ltp
            except: pass

        # 5. INTERNAL CACHE FALLBACK
        return self._ltp_cache.get(symbol, {}).get('ltp', 0.0)

    def get_index_quote(self, symbol: str) -> Dict[str, Any]:
        """Return index LTP plus broker-provided change fields when available."""
        quote = {"ltp": 0.0, "change": 0.0, "change_pct": 0.0, "source": "unknown"}
        if symbol not in INDEX_SPOT_TOKENS:
            return quote

        now_ts = time.time()
        token, exchange = INDEX_SPOT_TOKENS.get(symbol, ("", "NSE"))
        short_token = token[3:] if token.startswith("999") else token
        fyers_symbol = FYERS_INDEX_SYMBOLS.get(symbol)

        candidates = []
        for key in (fyers_symbol, token, short_token):
            cached = self._ws_cache.get(key) if key else None
            if isinstance(cached, dict) and float(cached.get("ltp", 0.0) or 0.0) > 0:
                age = now_ts - float(cached.get("time") or 0.0)
                if age <= 15.0:
                    candidates.append((age, cached))
        if candidates:
            candidates.sort(key=lambda item: item[0])
            cached = candidates[0][1]
            quote.update({
                "ltp": float(cached.get("ltp", 0.0) or 0.0),
                "change": float(cached.get("change", 0.0) or 0.0),
                "change_pct": float(cached.get("change_pct", 0.0) or 0.0),
                "source": str(cached.get("source") or "ws"),
            })
            self._last_ltp_time = float(cached.get("time") or now_ts)
            # A live WebSocket LTP must never be replaced by a slower REST
            # quote merely because the WS payload omitted day-change fields.
            # The scanner uses LTP to build the forming candle and detect UT
            # crossovers, so waiting for/restoring an older REST value here
            # can delay an otherwise real-time intrabar signal.
            if not quote["change"] and not quote["change_pct"]:
                cached_close = float(
                    getattr(self, "_prev_close_global_cache", {}).get(symbol, 0.0)
                    or 0.0
                )
                if cached_close > 0:
                    quote["change"] = quote["ltp"] - cached_close
                    quote["change_pct"] = quote["change"] / cached_close * 100.0
            return quote

        if getattr(self, "fyers", None) and fyers_symbol:
            try:
                resp = self.fyers.quotes({"symbols": fyers_symbol})
                if resp and resp.get("s") == "ok":
                    values = (resp.get("d") or [{}])[0].get("v", {})
                    ltp = float(values.get("lp", 0.0) or 0.0)
                    if ltp > 0:
                        quote.update({
                            "ltp": ltp,
                            "change": float(values.get("ch", 0.0) or 0.0),
                            "change_pct": float(values.get("chp", 0.0) or 0.0),
                            "source": "fyers_rest",
                        })
                        self._last_ltp_time = now_ts
                        self._ltp_cache[symbol] = {"ltp": ltp, "time": datetime.now(), "source": "fyers"}
                        return quote
            except Exception as exc:
                logger.debug(f"Fyers quote fetch failed for {symbol}: {exc}")

        ltp = self.get_latest_price_by_token(token, symbol, exchange)
        quote["ltp"] = float(ltp or 0.0)
        cached_source = self._ltp_cache.get(symbol, {})
        quote["source"] = str(cached_source.get("source") or "fallback")
        prev_close = self.get_previous_close(symbol)
        if quote["ltp"] > 0 and prev_close > 0:
            quote["change"] = quote["ltp"] - prev_close
            quote["change_pct"] = quote["change"] / prev_close * 100.0
        return quote

    def get_ltp(self, exchange: str, symbol: str, token: str) -> Optional[float]:
        """Unified LTP method used by entire system - Always fetches from AngelOne"""
        price = self.get_latest_price_by_token(token, symbol, exchange)
        return price if price > 0 else None

    def get_order_book_snapshot(self, exchange: str, symbol: str, token: str) -> Dict[str, Any]:
        """
        Best-effort bid/ask depth snapshot for OFR.

        This intentionally uses cached feed fields only. If the broker payload
        does not expose depth, callers receive a neutral unavailable result and
        keep using candle-pressure order flow.
        """
        keys = [token, str(token), symbol, FYERS_INDEX_SYMBOLS.get(symbol, "")]
        for key in keys:
            cached = self._ws_cache.get(key) if key else None
            if not isinstance(cached, dict):
                continue
            depth = cached.get("depth") or cached.get("market_depth") or cached.get("best5") or cached.get("bidask")
            parsed = self._parse_depth_payload(depth, cached.get("source", "ws_depth"))
            if parsed.get("usable"):
                return parsed
            bid_qty = cached.get("bid_qty") or cached.get("bid_size") or cached.get("bidsize")
            ask_qty = cached.get("ask_qty") or cached.get("ask_size") or cached.get("asksize")
            parsed = self._depth_from_qty(bid_qty, ask_qty, cached.get("source", "ws_depth"))
            if parsed.get("usable"):
                return parsed
        return {"ofr": 1.0, "bid_qty": 0, "ask_qty": 0, "source": "unavailable", "usable": False}

    def _parse_depth_payload(self, depth: Any, source: str = "depth") -> Dict[str, Any]:
        if not depth:
            return {"usable": False}
        bids, asks = [], []
        if isinstance(depth, dict):
            bids = depth.get("buy") or depth.get("bids") or depth.get("bid") or []
            asks = depth.get("sell") or depth.get("asks") or depth.get("ask") or []
        elif isinstance(depth, list):
            for row in depth:
                if not isinstance(row, dict):
                    continue
                side = str(row.get("side") or row.get("type") or "").lower()
                if side.startswith("b"):
                    bids.append(row)
                elif side.startswith("a") or side.startswith("s"):
                    asks.append(row)
        bid_qty = self._sum_depth_qty(bids)
        ask_qty = self._sum_depth_qty(asks)
        return self._depth_from_qty(bid_qty, ask_qty, source)

    @staticmethod
    def _sum_depth_qty(rows: Any) -> float:
        if isinstance(rows, dict):
            rows = [rows]
        total = 0.0
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            qty = row.get("quantity", row.get("qty", row.get("size", row.get("volume", 0))))
            try:
                total += float(qty or 0)
            except Exception:
                pass
        return total

    @staticmethod
    def _depth_from_qty(bid_qty: Any, ask_qty: Any, source: str = "depth") -> Dict[str, Any]:
        try:
            bid = float(bid_qty or 0.0)
            ask = float(ask_qty or 0.0)
        except Exception:
            return {"usable": False}
        if bid <= 0 and ask <= 0:
            return {"usable": False}
        ofr = bid / ask if ask > 0 else (bid if bid > 0 else 1.0)
        return {
            "ofr": round(float(ofr), 3),
            "bid_qty": int(bid),
            "ask_qty": int(ask),
            "source": source,
            "usable": True,
        }

    def get_previous_close(self, symbol: str) -> float:
        """Fetch official closing price with persistent session caching"""
        if not hasattr(self, "_prev_close_global_cache"):
            self._prev_close_global_cache = {}
            
        if symbol in self._prev_close_global_cache:
            return self._prev_close_global_cache[symbol]

        # 1. BROKER LIVE FEED (Primary Source)
        if self.is_connected and self.smart_api:
            if symbol in INDEX_SPOT_TOKENS:
                token, exchange = INDEX_SPOT_TOKENS[symbol]
                try:
                    trading_symbol = "INDEX" if symbol in INDEX_SPOT_TOKENS else symbol
                    
                    with getattr(self, "_rest_lock", __import__("threading").Lock()):
                        now = time.time()
                        last_rest_time = getattr(self, "_last_rest_time", 0)
                        if now - last_rest_time < 0.5:
                            import time as _time
                            _time.sleep(0.5 - (now - last_rest_time))
                            self._last_rest_time = _time.time()
                        else:
                            self._last_rest_time = now

                    resp = self.smart_api.ltpData(exchange, trading_symbol, token)
                    if resp.get('status') and resp.get('data'):
                        close_price = float(resp['data'].get('close', 0.0))
                        if close_price > 0:
                            self._prev_close_global_cache[symbol] = close_price
                            return close_price
                except Exception as e:
                    logger.debug(f"AngelOne prev_close fetch error for {symbol}: {e}")

        return 0.0

    def get_latest_price(self, symbol: str) -> float:
        """Alias for convenience, attempts to find correct token"""
        if symbol in INDEX_SPOT_TOKENS:
            t, e = INDEX_SPOT_TOKENS[symbol]
            return self.get_latest_price_by_token(t, symbol, e)
        return self._ltp_cache.get(symbol, {}).get('ltp', 0.0)

    # ═══════════════════════════════════════════════════════════
    # HISTORICAL & OPTIONS
    # ═══════════════════════════════════════════════════════════

    def get_historical_candles_angel(self, token, exchange, interval, days_back=15) -> pd.DataFrame:
        """Fetch historical candles directly from AngelOne SmartApi"""
        if not self.is_connected or not self.smart_api:
            return pd.DataFrame()
            
        try:
            # Map interval to AngelOne format
            api_interval = {
                "1min": "ONE_MINUTE",
                "5min": "FIVE_MINUTE",
                "15min": "FIFTEEN_MINUTE"
            }.get(interval, "FIVE_MINUTE")
            
            to_date = datetime.now().strftime("%Y-%m-%d %H:%M")
            from_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d 09:15")
            
            params = {
                "exchange": exchange,
                "symboltoken": token,
                "interval": api_interval,
                "fromdate": from_date,
                "todate": to_date
            }
            
            # ── Institutional Grade: Direct Async-ready Fetch ──
            # (Removed time.sleep to eliminate 300ms bottleneck)
            resp = self.smart_api.getCandleData(params)
            
            # Check for Invalid Token error
            if not resp.get('status') and resp.get('errorCode') == 'AG8001':
                logger.warning("🔄 AngelOne History Session Expired (AG8001). Re-authenticating...")
                if self.connect():
                    resp = self.smart_api.getCandleData(params)
                else:
                    return pd.DataFrame()

            if resp.get('status') and resp.get('data'):
                df = pd.DataFrame(resp['data'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                
                # Force IST Normalization (Broker → IST Naive)
                # This ensures '03:45 UTC' becomes '09:15 Naive'
                if df.index.tz is not None:
                    df.index = df.index.tz_convert('Asia/Kolkata').tz_localize(None)
                
                return df
            else:
                err = resp.get('message', 'Unknown Error')
                if "exceeding access rate" in err.lower():
                    logger.warning(f"🚨 AngelOne Rate Limit Hit for {token}")
                else:
                    logger.debug(f"AngelOne Empty Response for {token}: {err}")
        except Exception as e:
            logger.error(f"❌ AngelOne History Fetch Error: {e}")
        return pd.DataFrame()

    def get_historical_candles(self, token, exchange, interval, days_back=15, instrument_name="") -> pd.DataFrame:
        """
        Unified Historical Data Fetcher — Strictly prioritized for AngelOne.
        """
        cache_dir = Path("data_store/candles")
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"{instrument_name}_{interval}.csv"
        df = pd.DataFrame()

        # 0. GLOBAL API CACHE CHECK (Applies to both Fyers and AngelOne)
        now = time.time()
        cache_key = f"hist_api_{instrument_name}_{interval}"
        if not hasattr(self, "_api_hist_cache"): self._api_hist_cache = {}
        if not hasattr(self, "_last_api_hist_fetch"): self._last_api_hist_fetch = {}
        
        from config.settings import get_settings
        history_ttl = float(getattr(get_settings(), "history_cache_ttl_seconds", 60.0))
        last_fetch = self._last_api_hist_fetch.get(cache_key, 0)
        if isinstance(last_fetch, datetime):
            last_fetch = last_fetch.timestamp()
        elif not isinstance(last_fetch, (int, float)):
            last_fetch = 0
        if now - last_fetch < history_ttl:
            cached_df = self._api_hist_cache.get(cache_key)
            if cached_df is not None:
                logger.debug(f"Using cached API history for {cache_key}")
                return cached_df

        # Historical option candles are heavily reused during backtests. Prefer the
        # local per-symbol CSV cache before touching broker APIs to avoid rate-limit
        # stalls and session-token noise for expired option contracts.
        is_option_symbol = bool(instrument_name) and str(instrument_name).upper().endswith(("CE", "PE"))
        if is_option_symbol and cache_file.exists():
            try:
                cached_df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                if not cached_df.empty:
                    cached_df = cached_df.sort_index()
                    self._api_hist_cache[cache_key] = cached_df
                    self._last_api_hist_fetch[cache_key] = now
                    logger.info(f"📦 Using Local Option History Cache for {instrument_name}")
                    return cached_df
            except Exception as exc:
                logger.debug(f"Option history cache read failed for {instrument_name}: {exc}")

        # 1. PRIMARY SOURCE FOR INDICES: FYERS (To get Volume which AngelOne lacks)
        if getattr(self, 'fyers', None) and instrument_name in FYERS_INDEX_SYMBOLS:
            try:
                fyers_symbol = FYERS_INDEX_SYMBOLS.get(instrument_name)
                
                if fyers_symbol:
                    logger.info(f"Prioritizing Fyers history for {fyers_symbol} to get volume...")
                    res = "1" if interval == "1min" else ("5" if interval == "5min" else "15")
                    range_from = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
                    range_to = datetime.now().strftime("%Y-%m-%d")
                    
                    data = {
                        "symbol": fyers_symbol,
                        "resolution": res,
                        "date_format": "1",
                        "range_from": range_from,
                        "range_to": range_to,
                        "cont_flag": "1"
                    }
                    response = self.fyers.history(data=data)
                    if response and response.get('s') == 'ok':
                        candles = response.get('candles', [])
                        if candles:
                            fyers_df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                            fyers_df['timestamp'] = pd.to_datetime(fyers_df['timestamp'], unit='s')
                            fyers_df['timestamp'] = fyers_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                            fyers_df.set_index('timestamp', inplace=True)
                            
                            if not fyers_df.empty:
                                df = fyers_df
                                df = self._save_and_merge_candles(df, cache_file)
                                # Cache update moved to bottom
            except Exception as e:
                logger.warning(f"⚠️ Fyers history fetch failed for {instrument_name}: {e}")

        # 2. FALLBACK/PRIMARY FOR OTHERS: BROKER (AngelOne)
        if df.empty and self.is_connected and self.smart_api and token:
            try:
                df = self.get_historical_candles_angel(token, exchange, interval, days_back)
                if not df.empty:
                    df = self._save_and_merge_candles(df, cache_file)
                    # Cache update moved to bottom
            except Exception as e:
                logger.warning(f"⚠️ AngelOne fetch failed for {instrument_name}: {e}")

        # 3. FALLBACK FOR OTHERS: FYERS (If AngelOne fails)
        if df.empty and getattr(self, 'fyers', None):
            fyers_symbol = FYERS_INDEX_SYMBOLS.get(instrument_name)
            
            if fyers_symbol:
                try:
                    logger.info(f"Fetching Fyers history for {fyers_symbol}...")
                    # Map interval to Fyers resolution
                    res = "1" if interval == "1min" else ("5" if interval == "5min" else "15")
                    
                    range_from = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
                    range_to = datetime.now().strftime("%Y-%m-%d")
                    
                    data = {
                        "symbol": fyers_symbol,
                        "resolution": res,
                        "date_format": "1",
                        "range_from": range_from,
                        "range_to": range_to,
                        "cont_flag": "1"
                    }
                    response = self.fyers.history(data=data)
                    if response and response.get('s') == 'ok':
                        candles = response.get('candles', [])
                        if candles:
                            fyers_df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                            fyers_df['timestamp'] = pd.to_datetime(fyers_df['timestamp'], unit='s')
                            fyers_df['timestamp'] = fyers_df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
                            fyers_df.set_index('timestamp', inplace=True)
                            
                            if not fyers_df.empty:
                                df = fyers_df
                                df = self._save_and_merge_candles(df, cache_file)
                except Exception as e:
                    logger.warning(f"⚠️ Fyers history fetch failed for {instrument_name}: {e}")

        # 2. SECONDARY: LOCAL CACHE (If Broker fails or Market is closed)
        if df.empty and cache_file.exists():
            try:
                cached_df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                if not cached_df.empty:
                    df = cached_df
                    logger.info(f"📦 Using Local Cache for {instrument_name}")
            except: pass

        # 3. TERTIARY: YAHOO FALLBACK
        # If no data or data is from a previous day during a weekday, fallback to Yahoo
        today = datetime.now().date()
        is_stale = False
        if not df.empty:
            last_timestamp = df.index[-1]
            last_date = last_timestamp.date()
            if last_date < today and self.is_market_open():
                is_stale = True
                logger.info(f"📆 Cache data for {instrument_name} is stale (Last: {last_date}). Triggering Yahoo fallback.")
            elif self.is_market_open() and last_date == today:
                now = datetime.now()
                try:
                    int_mins = int(interval.replace('min', ''))
                except:
                    int_mins = 5
                threshold_minutes = max(15, 3 * int_mins)
                if (now - last_timestamp).total_seconds() / 60 > threshold_minutes:
                    is_stale = True
                    logger.info(f"🕒 Cache data for {instrument_name} is stale (Last: {last_timestamp}). Triggering Yahoo fallback.")
                
        if df.empty or is_stale:
            ticker = YAHOO_TICKERS.get(instrument_name, "")
            if ticker:
                try:
                    self._yahoo_request_count += 1
                    logger.info(f"📡 Falling back to Yahoo for {instrument_name}...")
                    yahoo_df = yf.Ticker(ticker).history(period=f"{days_back}d", interval=YAHOO_INTERVALS.get(interval, "5m"))
                    if not yahoo_df.empty:
                        self._yahoo_last_used_at = time.time()
                        df = yahoo_df
                except Exception as e:
                    logger.error(f"❌ Yahoo Fetch Error for {instrument_name}: {e}")

        if df.empty:
            empty_df = pd.DataFrame()
            self._api_hist_cache[cache_key] = empty_df
            self._last_api_hist_fetch[cache_key] = now
            return empty_df
        
        # --- NORMALIZATION & SESSION FILTERING ---
        df.columns = [c.lower() for c in df.columns]
        if 'adj close' in df.columns: df['close'] = df['adj close']
        
        if df.index.tz is not None:
            df.index = df.index.tz_convert('Asia/Kolkata').tz_localize(None)
        else:
            if instrument_name in YAHOO_TICKERS and df.index[-1].hour < 6:
                df.index = df.index.tz_localize('UTC').tz_convert('Asia/Kolkata').tz_localize(None)

        df = df.between_time('09:15', '15:30')
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col not in df.columns: df[col] = df['close'] if col != 'volume' else 0
            
        final_df = df[['open', 'high', 'low', 'close', 'volume']].copy()
        
        # ALWAYS update cache timestamp to prevent rate-limit spam loops on failures
        self._api_hist_cache[cache_key] = final_df
        self._last_api_hist_fetch[cache_key] = now
        
        return final_df

    def _save_and_merge_candles(self, df: pd.DataFrame, cache_file: Path):
        """Merge new candles with existing cache to prevent data loss and fill gaps"""
        if df.empty:
            return
            
        try:
            if cache_file.exists():
                existing_df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                if not existing_df.empty:
                    for frame in (existing_df, df):
                        frame.columns = [c.lower() for c in frame.columns]
                    existing_df = existing_df[~existing_df.index.duplicated(keep='last')].sort_index()
                    df = df[~df.index.duplicated(keep='last')].sort_index()
                    if "volume" in existing_df.columns and "volume" in df.columns:
                        existing_vol = pd.to_numeric(existing_df["volume"], errors="coerce").fillna(0)
                        new_vol = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
                        shared_idx = df.index.intersection(existing_df.index)
                        preserve_idx = [
                            idx for idx in shared_idx
                            if new_vol.get(idx, 0) <= 0 and existing_vol.get(idx, 0) > 0
                        ]
                        if preserve_idx:
                            df.loc[preserve_idx, "volume"] = existing_vol.loc[preserve_idx]
                    # Drop duplicates keeping the LATEST data (important for updates!)
                    merged_df = pd.concat([existing_df, df])
                    merged_df = merged_df[~merged_df.index.duplicated(keep='last')]
                    merged_df.sort_index(inplace=True)
                    merged_df = merged_df.tail(5000)
                    compare_cols = [c for c in ['open', 'high', 'low', 'close', 'volume'] if c in existing_df.columns and c in merged_df.columns]
                    if (
                        compare_cols
                        and len(merged_df) == len(existing_df)
                        and len(merged_df.index.difference(existing_df.index)) == 0
                        and existing_df[compare_cols].tail(3).equals(merged_df[compare_cols].tail(3))
                    ):
                        logger.debug(f"Skipping unchanged candle cache write for {cache_file.name}")
                        return existing_df
                    df = merged_df
                    
            # Save the full merged dataset
            df.to_csv(cache_file)
            logger.info(f"💾 Merged and saved data to {cache_file.name} (Total candles: {len(df)})")
        except Exception as e:
            logger.error(f"❌ Failed to merge/save candles to {cache_file.name}: {e}")
            # Fallback to just saving the new data
            try:
                df.to_csv(cache_file)
            except: pass
        return df

    def get_option_chain(self, index_symbol: str, exchange: str = "NFO") -> Optional[pd.DataFrame]:
        """Fetch real options data using Fyers (Primary) or AngelOne (Fallback)"""
        now = time.time()
        cache_key = f"opt_chain_{index_symbol}"
        if not hasattr(self, "_opt_chain_cache"): self._opt_chain_cache = {}
        if not hasattr(self, "_last_opt_chain_fetch"): self._last_opt_chain_fetch = {}
        if not hasattr(self, "_last_opt_chain_error"): self._last_opt_chain_error = {}
        
        from config.settings import get_settings
        chain_ttl = max(90.0, float(getattr(get_settings(), "intelligence_cache_ttl_seconds", 90.0)))
        error_cooldown = max(chain_ttl, 180.0)

        # Cache option chain aggressively during live scans to prevent API spam.
        last_fetch = self._last_opt_chain_fetch.get(cache_key, 0)
        if now - last_fetch < chain_ttl:
            cached_data = self._opt_chain_cache.get(cache_key)
            if cached_data is not None:
                logger.debug(f"Using cached option chain for {index_symbol}")
                return cached_data
            else:
                # In-flight fetch by another thread, return empty to prevent queueing
                return pd.DataFrame()

        last_error = self._last_opt_chain_error.get(cache_key, 0)
        if now - last_error < error_cooldown:
            cached_data = self._opt_chain_cache.get(cache_key)
            if cached_data is not None:
                logger.info(f"Using stale option chain for {index_symbol} during error cooldown")
                return cached_data
            return pd.DataFrame()

        # Mark as in-flight immediately to prevent concurrent threads from piling up
        self._last_opt_chain_fetch[cache_key] = now

        # ══ TRY FYERS FIRST ══
        if self.fyers:
            fyers_symbol = FYERS_INDEX_SYMBOLS.get(index_symbol)
            
            if fyers_symbol:
                try:
                    logger.info(f"Fetching Fyers option chain for {fyers_symbol}...")
                    
                    # Try to center around ATM strike if we have spot price
                    spot = self.get_latest_price(index_symbol)
                    data = {"symbol": fyers_symbol, "strikecount": 30, "timestamp": ""}
                    
                    if spot > 0:
                        strike_interval = INDEX_STRIKE_INTERVALS.get(index_symbol, 50)
                        atm_strike = round(spot / strike_interval) * strike_interval
                        logger.info(f"Fyers option chain requested for {fyers_symbol} (ATM: {atm_strike})")
                    
                    response = self.fyers.optionchain(data=data)
                    logger.info(f"Fyers optionchain response status: {response.get('s') if response else 'None'}")

                    # Align intelligence with the exact active expiry used by execution.
                    if response and response.get("s") == "ok":
                        from engine.expiry_manager import expiry_manager
                        if not expiry_manager.expiries:
                            expiry_manager.pre_market_check()
                        expiry_key = (
                            "active_monthly"
                            if index_symbol in {"BANKNIFTY", "MIDCPNIFTY"}
                            else "active_weekly"
                        )
                        active_label = (
                            expiry_manager.expiries.get(index_symbol, {}).get(expiry_key)
                        )
                        expiry_rows = response.get("data", {}).get("expiryData", []) or []
                        active_timestamp = ""
                        active_date = ""
                        if active_label:
                            try:
                                active_date = datetime.strptime(
                                    active_label, "%d%b%Y"
                                ).strftime("%d-%m-%Y")
                            except ValueError:
                                active_date = ""
                        for expiry_row in expiry_rows:
                            if str(expiry_row.get("date") or "") == active_date:
                                active_timestamp = str(expiry_row.get("expiry") or "")
                                break
                        if active_timestamp:
                            data["timestamp"] = active_timestamp
                            response = self.fyers.optionchain(data=data)
                            logger.info(
                                f"Fyers option chain aligned to active expiry "
                                f"{active_label} for {index_symbol}."
                            )
                    
                    if response and response.get('s') == 'ok':
                        chain_data = response.get('data', {}).get('optionsChain', [])
                        logger.info(f"Fyers optionsChain length: {len(chain_data)}")
                        df = pd.DataFrame(chain_data)
                        # Filter out index row
                        df = df[df['strike_price'] > 0]
                        
                        ce_df = df[df['option_type'] == 'CE'].rename(columns={'oi': 'call_oi', 'volume': 'call_volume', 'strike_price': 'strike', 'oich': 'call_oi_change', 'ltp': 'call_ltp'})
                        pe_df = df[df['option_type'] == 'PE'].rename(columns={'oi': 'put_oi', 'volume': 'put_volume', 'strike_price': 'strike', 'oich': 'put_oi_change', 'ltp': 'put_ltp'})
                        
                        # Merge on strike
                        chain = pd.merge(ce_df[['strike', 'call_oi', 'call_volume', 'call_oi_change', 'call_ltp']], pe_df[['strike', 'put_oi', 'put_volume', 'put_oi_change', 'put_ltp']], on='strike', how='outer')
                        chain = chain.fillna(0)
                        self._option_chain_meta[index_symbol] = {
                            "source": "fyers",
                            "fetched_at": now,
                            "strike_count": int(len(chain)),
                            "fallback": False,
                            "active_expiry": active_label if "active_label" in locals() else "",
                            "expiry_aligned": bool(active_timestamp) if "active_timestamp" in locals() else False,
                        }
                        
                        logger.info(f"✅ Fyers Option Chain fetched successfully for {index_symbol}")
                        self._opt_chain_cache[cache_key] = chain
                        self._last_opt_chain_fetch[cache_key] = now
                        return chain
                    self._last_opt_chain_fetch[cache_key] = now
                    self._last_opt_chain_error[cache_key] = now
                except Exception as e:
                    self._last_opt_chain_fetch[cache_key] = now
                    self._last_opt_chain_error[cache_key] = now
                    logger.warning(f"⚠️ Fyers Option Chain fetch failed: {e}. Falling back to AngelOne...")

        # ══ FALLBACK TO ANGELONE (Single ATM Row - Like older versions) ══
        spot = self.get_latest_price(index_symbol)
        if spot <= 0:
            self._last_opt_chain_error[cache_key] = now
            return pd.DataFrame()
        
        interval = INDEX_STRIKE_INTERVALS.get(index_symbol, 50)
        atm = round(spot / interval) * interval
        
        from engine.expiry_manager import expiry_manager
        
        # Get tokens for ATM CE and PE using the same active rollover logic as trades.
        ce_token_dict = self.get_option_token(index_symbol, atm, "CE")
        pe_token_dict = self.get_option_token(index_symbol, atm, "PE")
        
        if not ce_token_dict or not pe_token_dict:
            logger.warning(f"Tokens not found for ATM {atm} on {index_symbol}.")
            self._last_opt_chain_error[cache_key] = now
            return pd.DataFrame()
            
        ce_token = ce_token_dict['token']
        pe_token = pe_token_dict['token']
        
        ce_ltp, ce_oi, ce_vol = 0.0, 100000, 0
        pe_ltp, pe_oi, pe_vol = 0.0, 80000, 0
        
        if self.is_connected and self.smart_api:
            try:
                # 1. Check WebSocket Cache First
                ce_cached = self._ws_cache.get(ce_token, {})
                pe_cached = self._ws_cache.get(pe_token, {})
                
                ce_ltp = ce_cached.get('ltp', 0.0)
                ce_oi = ce_cached.get('oi', 0)
                ce_vol = ce_cached.get('vol', 0)
                
                pe_ltp = pe_cached.get('ltp', 0.0)
                pe_oi = pe_cached.get('oi', 0)
                pe_vol = pe_cached.get('vol', 0)
                
                # 2. If Cache is empty, use REST Fallback
                if ce_ltp <= 0 or pe_ltp <= 0:
                    logger.debug(f"ATM Cache empty for {index_symbol}, fetching via REST")
                    exch_seg = ce_token_dict.get('exch_seg', 'NFO')
                    exch_type = 4 if exch_seg == 'BFO' else 2
                    self.subscribe_tokens([ce_token, pe_token], exchange_type=exch_type)
                    
                    # Fetch CE Market Data
                    with getattr(self, "_rest_lock", __import__("threading").Lock()):
                        now_req = time.time()
                        last_rest_time = getattr(self, "_last_rest_time", 0)
                        if now_req - last_rest_time < 0.5:
                            import time as _time
                            _time.sleep(0.5 - (now_req - last_rest_time))
                            self._last_rest_time = _time.time()
                        else:
                            self._last_rest_time = now_req
                        ce_resp = self.smart_api.getMarketData(mode="FULL", exchangeTokens={exch_seg: [ce_token]})
                    
                    # Handle Session Expiry (AG8001) for CE
                    if not ce_resp or (not ce_resp.get('status') and ce_resp.get('errorCode') == 'AG8001'):
                        logger.warning("🔄 Option Chain Session Expired (AG8001). Re-authenticating...")
                        if self.connect():
                            ce_resp = self.smart_api.getMarketData(mode="FULL", exchangeTokens={exch_seg: [ce_token]})
                        else:
                            raise Exception("Could not re-authenticate AngelOne")

                    ce_fetched = ce_resp.get('data', {}).get('fetched', [{}])[0] if ce_resp and ce_resp.get('status') else {}
                    ce_ltp = float(ce_fetched.get('ltp', 0.0))
                    ce_oi = int(ce_fetched.get('opnInterest', 100000))
                    ce_vol = int(ce_fetched.get('totTrdQty', 0))
                    
                    # Fetch PE Market Data
                    with getattr(self, "_rest_lock", __import__("threading").Lock()):
                        now_req = time.time()
                        last_rest_time = getattr(self, "_last_rest_time", 0)
                        if now_req - last_rest_time < 0.5:
                            import time as _time
                            _time.sleep(0.5 - (now_req - last_rest_time))
                            self._last_rest_time = _time.time()
                        else:
                            self._last_rest_time = now_req
                        pe_resp = self.smart_api.getMarketData(mode="FULL", exchangeTokens={exch_seg: [pe_token]})

                    # Handle Session Expiry (AG8001) for PE
                    if not pe_resp or (not pe_resp.get('status') and pe_resp.get('errorCode') == 'AG8001'):
                        if self.connect():
                            pe_resp = self.smart_api.getMarketData(mode="FULL", exchangeTokens={exch_seg: [pe_token]})
                    
                    pe_fetched = pe_resp.get('data', {}).get('fetched', [{}])[0] if pe_resp and pe_resp.get('status') else {}
                    pe_ltp = float(pe_fetched.get('ltp', 0.0))
                    pe_oi = int(pe_fetched.get('opnInterest', 80000))
                    pe_vol = int(pe_fetched.get('totTrdQty', 0))
                
            except Exception as e:
                logger.error(f"❌ Failed to fetch option Market Data: {e}")
                self._last_opt_chain_error[cache_key] = now
                if 'ce_ltp' not in locals() or ce_ltp <= 0:
                    ce_ltp, ce_oi, ce_vol = 0.0, 100000, 0
                    pe_ltp, pe_oi, pe_vol = 0.0, 80000, 0
                
        # Build chain with 1 ATM row
        rows = [{
            'strike': atm, 
            'call_oi': ce_oi, 
            'call_oi_change': 0,
            'call_volume': ce_vol,
            'call_ltp': ce_ltp,
            'put_oi': pe_oi, 
            'put_oi_change': 0,
            'put_volume': pe_vol,
            'put_ltp': pe_ltp,
            'call_iv': 15.0, 'put_iv': 15.0, 
            'call_gamma': 0.0004, 'put_gamma': 0.0004,
            'call_theta': -10.0, 'put_theta': -10.0, 
            'is_atm': True
        }]
        df = pd.DataFrame(rows)
        self._option_chain_meta[index_symbol] = {
            "source": "angelone_atm_fallback",
            "fetched_at": now,
            "strike_count": 1,
            "fallback": True,
        }
        
        # Cache the result so we don't REST-fetch again within 60s
        self._opt_chain_cache[cache_key] = df
        self._last_opt_chain_fetch[cache_key] = now
        
        logger.debug(f"✅ AngelOne Option Chain built for {index_symbol} (ATM={atm})")
        return df

    def fetch_instruments(self):
        from engine.expiry_manager import expiry_manager
        return expiry_manager.pre_market_check()

    def get_option_token(self, name: str, strike: float, option_type: str, trade_date: Optional[date] = None) -> Optional[Dict]:
        from engine.expiry_manager import expiry_manager

        # Expiry Selection Logic:
        # BANKNIFTY/MIDCPNIFTY: Monthly (active_monthly handles T-2 rollover)
        # NIFTY/SENSEX: Always use Weekly (active_weekly handles 2-day rollover)
        if name in {"BANKNIFTY", "MIDCPNIFTY"}:
            expiry_type = "active_monthly"
        else:
            expiry_type = "active_weekly"

        if trade_date is not None:
            dated = expiry_manager.get_token_for_date(name, strike, option_type, trade_date, expiry_type)
            if dated:
                return dated
            
        return expiry_manager.get_token(name, strike, option_type, expiry_type)
