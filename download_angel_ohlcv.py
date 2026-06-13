import os
import sys
sys.path.append('.')
from data.market_data import MarketDataProvider
from config.settings import get_settings
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger

def download_data():
    s = get_settings()
    dp = MarketDataProvider(
        api_key=s.angelone_api_key,
        client_id=s.angelone_client_id,
        password=s.angelone_password,
        totp_secret=s.angelone_totp_secret
    )
    
    if not dp.connect():
        logger.info("Failed to connect to AngelOne")
        return

    # Use Future tokens for real Volume/OI
    futures = [
        {"name": "NIFTY", "token": "66071", "exchange": "NFO"},
        {"name": "BANKNIFTY", "token": "66068", "exchange": "NFO"},
        {"name": "SENSEX", "token": "1105863", "exchange": "BFO"}
    ]
    
    to_date = datetime.now().strftime("%Y-%m-%d %H:%M")
    from_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d %H:%M")
    
    os.makedirs("data_store/candles", exist_ok=True)
    
    for fut in futures:
        logger.info(f"Downloading {fut['name']} Future ({fut['token']}) from {from_date}...")
        for tf_label, angel_tf in [("1min", "ONE_MINUTE"), ("5min", "FIVE_MINUTE"), ("15min", "FIFTEEN_MINUTE")]:
            try:
                params = {
                    "exchange": fut["exchange"],
                    "symboltoken": fut["token"],
                    "interval": angel_tf,
                    "fromdate": from_date,
                    "todate": to_date
                }
                res = dp.smart_api.getCandleData(params)
                if res.get('status'):
                    df = pd.DataFrame(res['data'])
                    if not df.empty:
                        df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                        # Save to CSV
                        filename = f"data_store/candles/{fut['name']}_{tf_label}.csv"
                        df.to_csv(filename, index=False)
                        logger.info(f"  [DONE] Saved {len(df)} candles to {filename}")
                    else:
                        logger.info(f"  [WARN] No data for {fut['name']} {tf_label}")
                else:
                    logger.info(f"  [ERROR] {fut['name']} {tf_label}: {res.get('message')}")
            except Exception as e:
                logger.info(f"  [EXCEPTION] {fut['name']} {tf_label}: {e}")

if __name__ == "__main__":
    download_data()
