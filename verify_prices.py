import sys
import os
import pandas as pd
from datetime import datetime
from loguru import logger
from data.market_data import INDEX_STRIKE_INTERVALS, MarketDataProvider
from engine.expiry_manager import expiry_manager
from config.settings import get_settings

def verify_system_pricing():
    logger.info("\n" + "="*60)
    logger.info("🚀 INSTITUTIONAL PRICING VERIFICATION")
    logger.info("="*60)
    
    settings = get_settings()
    data = MarketDataProvider(
        api_key=settings.angelone_api_key,
        client_id=settings.angelone_client_id,
        password=settings.angelone_password,
        totp_secret=settings.angelone_totp_secret
    )
    
    if not data.connect():
        logger.info("❌ Failed to connect to AngelOne")
        return

    # 1. Initialize Expiry Manager
    expiry_manager.pre_market_check()
    
    # 2. Define verification targets
    targets = [
        # (Index, Strike, Type, ExpiryType, ExpectedCMP)
        ("NIFTY", 23600, "CE", "weekly", 222.0),
        ("NIFTY", 23400, "PE", "weekly", 137.95),
        ("NIFTY", 23600, "CE", "next_weekly", 325.05),
        ("BANKNIFTY", 53400, "CE", "monthly", 690.25),
        ("BANKNIFTY", 53500, "PE", "monthly", 659.0),
        ("SENSEX", 75200, "CE", "weekly", 422.55),
        ("SENSEX", 75400, "PE", "weekly", 532.95)
    ]
    
    logger.info(f"\n{'INSTRUMENT':<25} | {'EXPIRY':<12} | {'EXPECTED':<8} | {'ACTUAL':<8} | {'STATUS'}")
    logger.info("-" * 80)
    
    for idx, strike, opt_type, exp_type, expected in targets:
        # Resolve Token
        token_info = expiry_manager.get_token(idx, strike, opt_type, exp_type)
        if not token_info:
            logger.info(f"{idx} {strike}{opt_type} ({exp_type:<7}) | NOT FOUND")
            continue
            
        symbol = token_info['symbol']
        token = token_info['token']
        expiry = token_info['expiry']
        exch = "BFO" if idx == "SENSEX" else "NFO"
        
        # Fetch LTP
        actual = data.get_ltp(exch, symbol, token)
        actual = float(actual) if actual is not None else 0.0
        
        status = "✅ MATCH" if abs(actual - expected) < (expected * 0.05) else "⚠️ DIFF"
        if actual <= 0: status = "❌ OFFLINE"
        
        logger.info(f"{symbol:<25} | {expiry:<12} | {expected:<8.2f} | {actual:<8.2f} | {status}")

    # 3. Verify ATM Calculation
    logger.info("\n" + "="*60)
    logger.info("🎯 ATM / ITM CALCULATION LOGIC")
    logger.info("="*60)
    for idx in settings.active_indices:
        spot = data.get_latest_price(idx)
        interval = INDEX_STRIKE_INTERVALS.get(idx, 50)
        atm = round(spot / interval) * interval
        itm_ce = atm - interval
        itm_pe = atm + interval
        
        logger.info(f"{idx:<10} | Spot: {spot:<8.2f} | ATM: {atm:<8.0f} | ITM-CE: {itm_ce:<8.0f} | ITM-PE: {itm_pe:<8.0f}")

if __name__ == "__main__":
    verify_system_pricing()
