"""
Broker Interface — Abstracting order execution
═══════════════════════════════════════════════════════════════
"""

from typing import Dict, Optional
from loguru import logger
try:
    from SmartApi import SmartConnect
    HAS_SMARTAPI = True
except ImportError:
    SmartConnect = None
    HAS_SMARTAPI = False


from concurrent.futures import ThreadPoolExecutor

class Broker:
    """Base Broker Interface"""
    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=5)

    def place_order(self, symbol: str, token: str, qty: int, side: str, product_type: str = "CARRYFORWARD", price: float = 0.0) -> Optional[str]:
        raise NotImplementedError
    
    def place_order_async(self, symbol: str, token: str, qty: int, side: str, product_type: str = "CARRYFORWARD", price: float = 0.0, callback=None):
        """Fire-and-forget async order placement"""
        def task():
            try:
                res = self.place_order(symbol, token, qty, side, product_type, price)
                if callback: callback(res)
                return res
            except Exception as e:
                logger.error(f"Async Order Task Error: {e}")
                return None
        return self._executor.submit(task)

    def close_order(self, symbol: str, token: str, qty: int, side: str, product_type: str = "CARRYFORWARD", price: float = 0.0) -> Optional[str]:
        raise NotImplementedError
        
    def get_positions(self) -> Optional[Dict]:
        """Fetch real-time position book from broker"""
        raise NotImplementedError

class SignalSimBroker(Broker):
    """Simulation Broker for Scanned Signals"""
    def __init__(self):
        super().__init__()

    def place_order(self, symbol: str, token: str, qty: int, side: str, product_type: str = "CARRYFORWARD", price: float = 0.0) -> Optional[str]:
        logger.debug(f"SIGNAL SIMULATION: {side} {qty} {symbol} (Real Data Analysed) | Price: {price}")
        return "SIM_ORD_" + symbol
    
    def close_order(self, symbol: str, token: str, qty: int, side: str, product_type: str = "CARRYFORWARD", price: float = 0.0) -> Optional[str]:
        logger.debug(f"SIGNAL SIMULATION CLOSE: {side} {qty} {symbol} | Price: {price}")
        return "SIM_EXIT_" + symbol
        
    def get_positions(self) -> Optional[Dict]:
        return {"data": []}

class SmartApiBroker(Broker):
    """AngelOne SmartAPI Broker"""
    def __init__(self, smart_api: SmartConnect):
        super().__init__()
        self.api = smart_api
        import threading
        self._order_lock = threading.Lock()

    def place_order(self, symbol: str, token: str, qty: int, side: str, product_type: str = "CARRYFORWARD", price: float = 0.0) -> Optional[str]:
        try:
            # ══ Dynamic Exchange Resolution ══
            # SENSEX Options/Futures live on BFO (BSE), NIFTY/BANKNIFTY on NFO (NSE)
            exchange = "BFO" if "SENSEX" in symbol.upper() else "NFO"
            
            # Use LIMIT execution for options if a price is specified to mitigate slippage
            is_option = "CE" in symbol.upper() or "PE" in symbol.upper()
            order_type = "LIMIT" if (price > 0.0 and is_option) else "MARKET"
            
            params = {
                "variety": "NORMAL",
                "tradingsymbol": symbol,
                "symboltoken": token,
                "transactiontype": side, # BUY or SELL
                "exchange": exchange,
                "ordertype": order_type,
                "producttype": product_type,
                "duration": "DAY",
                "quantity": str(qty)
            }
            if order_type == "LIMIT":
                params["price"] = str(round(price, 2))
                
            logger.info(f"PLACING REAL ORDER: {side} {qty} {symbol} on {exchange} | Type: {order_type} | Price: {price:.2f}")
            
            max_retries = 3
            base_delay = 0.5
            import time
            for attempt in range(max_retries):
                try:
                    with self._order_lock:
                        response = self.api.placeOrder(params)
                    
                    # Validate response
                    if isinstance(response, dict):
                        # Handle Rate Limits (HTTP 429 or specific error codes from Angel)
                        err_code = str(response.get("errorcode", ""))
                        if err_code == "AG8001" or err_code == "429":
                            logger.warning(f"Rate limited (429) on attempt {attempt+1}/{max_retries}. Retrying...")
                            time.sleep(base_delay * (2 ** attempt))
                            continue
                            
                        if response.get("status") is False or response.get("errorcode"):
                            logger.error(f"Broker rejected order: {response}")
                            return None
                        data = response.get("data", response)
                        if isinstance(data, dict):
                            order_id = str(data.get("orderid") or data.get("uniqueorderid") or data)
                        else:
                            order_id = str(data)
                        break # Success
                    else:
                        order_id = str(response)
                        break
                        
                except Exception as api_err:
                    err_msg = str(api_err).lower()
                    if "429" in err_msg or "rate limit" in err_msg:
                        logger.warning(f"Rate limited (429) exception on attempt {attempt+1}/{max_retries}. Retrying...")
                        time.sleep(base_delay * (2 ** attempt))
                        continue
                    else:
                        logger.error(f"Broker API error during placeOrder: {api_err}")
                        return None
            else:
                logger.error("Failed to place order after max retries due to rate limiting.")
                return None
                
            if not order_id or order_id == "None":
                return None
                
            logger.success(f"REAL ORDER PLACED: {order_id} ({exchange})")
            return order_id
        except Exception as e:
            logger.error(f"Order placement failed on {exchange if 'exchange' in locals() else 'Unknown'}: {e}")
            return None

    def close_order(self, symbol: str, token: str, qty: int, side: str, product_type: str = "CARRYFORWARD", price: float = 0.0) -> Optional[str]:
        # `side` is the desired exit transaction side, computed by TradeManager.
        return self.place_order(symbol, token, qty, side.upper(), product_type, price=0.0)

    def get_positions(self) -> Optional[Dict]:
        try:
            response = self.api.position()
            if isinstance(response, dict):
                if response.get("status") is False or response.get("errorcode"):
                    logger.error(f"Broker returned error for positions: {response}")
                    return None
            return response
        except Exception as e:
            logger.error(f"Failed to fetch position book: {e}")
            return None
