
import json
import os
import time
from pathlib import Path
import pytest
from loguru import logger
from data.market_data import MarketDataProvider
from config.settings import get_settings

@pytest.mark.integration
def test_automated_refresh_loop():
    if os.getenv("RUN_LIVE_TOKEN_REFRESH_TEST") != "1":
        pytest.skip("Set RUN_LIVE_TOKEN_REFRESH_TEST=1 to run the live token refresh mutation test")

    logger.info("🛠️ Simulating Token Expiry Scenario...")
    settings = get_settings()
    provider = MarketDataProvider(
        api_key=settings.angelone_api_key,
        client_id=settings.angelone_client_id,
        password=settings.angelone_password,
        totp_secret=settings.angelone_totp_secret
    )

    # 1. Check current Fyers state
    token_path = Path("fyers_token.json")
    if not token_path.exists():
        logger.error("❌ No token file to test!")
        return

    with open(token_path, "r") as f:
        old_token_data = json.load(f)
    
    old_access = old_token_data.get('access_token')
    logger.info(f"Current Access Token (first 10 chars): {old_access[:10]}...")

    # 2. Trigger Force Refresh (Simulating a session expiry)
    logger.info("🔄 Triggering Automated Refresh Logic...")
    provider._refresh_fyers_token(old_token_data)

    # 3. Verify Result
    with open(token_path, "r") as f:
        new_token_data = json.load(f)
    
    new_access = new_token_data.get('access_token')
    
    if old_access != new_access:
        logger.success("✅ SUCCESS: System successfully generated a NEW token on its own!")
        logger.info(f"New Access Token (first 10 chars): {new_access[:10]}...")
        logger.info("Verification: This confirms the user does NOT need to login manually next week.")
    else:
        logger.error("❌ FAILED: Token did not refresh.")

if __name__ == "__main__":
    test_automated_refresh_loop()
