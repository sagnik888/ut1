import yfinance as yf
import json
from datetime import datetime, timedelta

def download_vix():
    vix = yf.Ticker("^INDIAVIX")
    
    # Try to get 1m data for last 7 days
    try:
        hist = vix.history(period="7d", interval="1m")
        print("Downloaded 1m data")
    except Exception as e:
        print(f"Failed to download 1m data: {e}")
        try:
            hist = vix.history(period="7d", interval="5m")
            print("Downloaded 5m data")
        except Exception as e2:
            print(f"Failed to download 5m data: {e2}")
            hist = vix.history(period="30d", interval="1d")
            print("Downloaded daily data fallback")
        
    if hist.empty:
        print("No data downloaded!")
        return
        
    # Convert to a dict mapping timestamp string to VIX value
    data = {}
    for index, row in hist.iterrows():
        ts_str = index.strftime("%Y-%m-%d %H:%M:%S")
        data[ts_str] = float(row['Close'])
        
    import os
    os.makedirs("data_store", exist_ok=True)
    
    with open("data_store/vix_history.json", "w") as f:
        json.dump(data, f, indent=4)
        
    print(f"Saved {len(data)} data points to data_store/vix_history.json")

if __name__ == "__main__":
    download_vix()
