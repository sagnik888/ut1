"""
Data Manager — Automated daily data pruning and gap-filling.
Maintains history for real and historical backtesting needs.
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
from data.market_data import MarketDataProvider

class DataManager:
    def __init__(self, data_provider: MarketDataProvider):
        self.data = data_provider
        self.data_dir = Path("data_store")
        self.candles_dir = self.data_dir / "candles"
        self.intel_file = self.data_dir / "intelligence_history.json"
        
        # Retention Policies (Days)
        self.RETENTION_1MIN = 30
        self.RETENTION_5MIN = 30
        self.RETENTION_15MIN = 65
        self.RETENTION_INTEL = 65

    def run_daily_maintenance(self, indices: list = None):
        """Run daily sync and pruning for data stores."""
        logger.info("🧹 [DataManager] Starting daily maintenance and pruning...")
        
        # 1. Sync Missing Data (Gap Filling)
        if indices:
            self._sync_history(indices)

        # 2. Prune Candles
        self._prune_candles()
        
        # 3. Prune Intelligence JSONs
        self._prune_intelligence()

        logger.info("✨ [DataManager] Daily maintenance complete.")

    def _sync_history(self, indices: list):
        """Fetch latest history to fill gaps."""
        from config.settings import get_settings
        instruments = get_settings().get_instruments()
        
        try:
            for name in indices:
                # Get exact token from config (e.g. NIFTY -> 26000)
                token = ""
                for inst_name, inst_data in instruments.items():
                    if inst_name == name:
                        token = inst_data.get("symbol_token", "")
                        break
                        
                for tf, days in [("1min", 5), ("5min", 5), ("15min", 5)]:
                    logger.info(f"🔄 [DataManager] Syncing {name} {tf} history...")
                    self.data.get_historical_candles(token=token, exchange="NSE", interval=tf, days_back=days, instrument_name=name)
        except Exception as e:
            logger.error(f"❌ [DataManager] Error syncing history: {e}")

    def _prune_candles(self):
        """Delete rows older than retention limits from CSV files."""
        if not self.candles_dir.exists():
            return
            
        for file in self.candles_dir.glob("*.csv"):
            try:
                tf = file.stem.split("_")[-1]  # Expected format: NIFTY_5min.csv
                if tf == "1min":
                    retention = self.RETENTION_1MIN
                elif tf == "5min":
                    retention = self.RETENTION_5MIN
                elif tf == "15min":
                    retention = self.RETENTION_15MIN
                else:
                    retention = 30
                    
                cutoff_date = datetime.now() - timedelta(days=retention)
                
                df = pd.read_csv(file, index_col=0, parse_dates=True)
                if df.empty:
                    continue
                
                # Filter out old rows
                orig_len = len(df)
                df = df[df.index >= cutoff_date]
                new_len = len(df)
                
                if new_len < orig_len:
                    df.to_csv(file)
                    logger.info(f"🧹 [DataManager] Pruned {orig_len - new_len} old rows from {file.name} (Retaining {retention} days)")
            except Exception as e:
                logger.error(f"❌ [DataManager] Error pruning {file.name}: {e}")

    def _prune_intelligence(self):
        """Prune intelligence_history.json to keep only last 65 days."""
        if not self.intel_file.exists():
            return
            
        try:
            cutoff_date = datetime.now() - timedelta(days=self.RETENTION_INTEL)
            cutoff_iso = cutoff_date.isoformat()
            
            with open(self.intel_file, "r") as f:
                data = json.load(f)
                
            orig_size = 0
            new_size = 0
            
            for instrument, records in data.items():
                orig_size += len(records)
                # Assuming records is a list of dicts with a 'timestamp' key
                # Alternatively, intelligence history might be keyed by timestamp.
                if isinstance(records, dict):
                    # Keyed by timestamp string
                    data[instrument] = {k: v for k, v in records.items() if k >= cutoff_iso}
                    new_size += len(data[instrument])
                elif isinstance(records, list):
                    # List of dicts
                    data[instrument] = [r for r in records if r.get("timestamp", "") >= cutoff_iso]
                    new_size += len(data[instrument])
                    
            if new_size < orig_size:
                with open(self.intel_file, "w") as f:
                    json.dump(data, f)
                logger.info(f"🧹 [DataManager] Pruned intelligence history from {orig_size} to {new_size} records (Retaining {self.RETENTION_INTEL} days)")
        except Exception as e:
            logger.error(f"❌ [DataManager] Error pruning intelligence JSON: {e}")
