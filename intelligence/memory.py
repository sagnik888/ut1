import json
import os
import pandas as pd
import threading
import time
from typing import Dict, List, Any, Optional
from loguru import logger
from datetime import datetime

class IntelligenceMemory:
    """
    Persistent memory for market intelligence metrics.
    Ensures that OI, PCR, Volume Pressure, and Regime data are remembered
    throughout the session and survive restarts.
    """
    
    def __init__(self, storage_path: str = "data_store/intelligence_history.json"):
        self.storage_path = storage_path
        self.memory: Dict[str, Dict[str, List[Any]]] = {}
        self._max_points = 5000 # ~13 market days of 1min points
        self._lock = threading.RLock()
        self._cleanup_temp_files()
        self.load()


    def _cleanup_temp_files(self):
        try:
            dir_path = os.path.dirname(self.storage_path) or "."
            base_name = os.path.basename(self.storage_path)
            for f in os.listdir(dir_path):
                if f.startswith(base_name) and f.endswith(".tmp"):
                    try:
                        os.remove(os.path.join(dir_path, f))
                    except:
                        pass
        except Exception as e:
            logger.debug(f"Temp cleanup failed: {e}")
            
    def load(self):
        """Load history from disk"""
        with self._lock:
            if os.path.exists(self.storage_path):
                try:
                    with open(self.storage_path, 'r') as f:
                        self.memory = json.load(f)
                    self._compact_loaded_memory()
                    logger.info(f"🧠 Intelligence Memory loaded: {len(self.memory)} instruments")
                except Exception as e:
                    logger.error(f"Failed to load intelligence memory: {e}")
                    self.memory = {}
                    # Overwrite corrupted file with empty dict to fix it for next time!
                    self.save()

    @staticmethod
    def _compact_mapping(value: Any, allowed_keys: set) -> Dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        return {
            key: value.get(key)
            for key in allowed_keys
            if key in value
            and (isinstance(value.get(key), (str, int, float, bool)) or value.get(key) is None)
        }

    def _compact_ca_oi(self, value: Any) -> Dict[str, Any]:
        return self._compact_mapping(value, {
            "signal", "interpretation", "call_oi_change", "put_oi_change",
            "call_oi", "put_oi", "total_call_oi", "total_put_oi",
        })

    def _compact_greeks(self, value: Any) -> Dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        compact = self._compact_mapping(value, {"iv_percentile", "iv_source", "iv_skew"})
        for side in ("call", "put"):
            side_data = self._compact_mapping(
                value.get(side),
                {"delta", "gamma", "theta", "vega", "iv"},
            )
            if side_data:
                compact[side] = side_data
        return compact

    def _compact_loaded_memory(self) -> None:
        """Migrate legacy full-chain snapshots into bounded scalar history."""
        for instrument, values in list(self.memory.items()):
            if not isinstance(values, dict):
                self.memory[instrument] = {}
                continue
            point_count = len(values.get("timestamps", []))
            for key in list(values):
                if isinstance(values[key], list):
                    values[key] = values[key][-self._max_points:]
            values["ca_oi"] = [
                self._compact_ca_oi(item) for item in values.get("ca_oi", [])
            ]
            values["greeks"] = [
                self._compact_greeks(item) for item in values.get("greeks", [])
            ]
            if point_count > self._max_points:
                logger.info(
                    f"Trimmed intelligence memory for {instrument}: "
                    f"{point_count} -> {self._max_points} points"
                )

    def save(self):
        """Save history to disk safely using Write-Rename strategy"""
        with self._lock:
            temp_path = None
            try:
                os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
                temp_path = f"{self.storage_path}.{os.getpid()}.{threading.get_ident()}.tmp"
                
                # 1. Write to temporary file
                with open(temp_path, 'w') as f:
                    json.dump(self.memory, f)
                    
                # 2. Atomic rename/replace to target file
                for attempt in range(5):
                    try:
                        os.replace(temp_path, self.storage_path)
                        return
                    except PermissionError:
                        if attempt == 4:
                            raise
                        time.sleep(0.05 * (attempt + 1))
            except Exception as e:
                logger.error(f"Failed to save intelligence memory: {e}")
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass

    def record(self, instrument: str, timestamp: float, intel_result: Dict):
        """Record a snapshot of intelligence results"""
        with self._lock:
            if instrument not in self.memory:
                self.memory[instrument] = {
                    "timestamps": [],
                    "vol_ratio": [],
                    "oi_signal": [],
                    "ca_oi": [], # System CA OI
                    "pcr": [],
                    "regime": [],
                    "order_ratio": [],
                    "greeks": [],
                    "score": []
                }

            m = self.memory[instrument]
            
            # Schema Migration: Ensure all new keys exist in recovered memory
            required_keys = ["ca_oi", "order_ratio", "greeks", "score"]
            for k in required_keys:
                if k not in m:
                    m[k] = [0.0] * len(m.get("timestamps", []))
            
            
            # Persist one mutable point per minute instead of one near-identical
            # full snapshot per scanner cycle.
            timestamp = int(float(timestamp) // 60) * 60
            replace_last = bool(m["timestamps"] and m["timestamps"][-1] == timestamp)
            if not replace_last:
                m["timestamps"].append(timestamp)
            
            # Helper to safely get nested values
            def safe_get(d, keys, default):
                curr = d
                for k in keys:
                    if isinstance(curr, dict):
                        curr = curr.get(k, default)
                    else:
                        return default
                return curr

            def store(key, value):
                if replace_last and m[key]:
                    m[key][-1] = value
                else:
                    m[key].append(value)

            store("vol_ratio", safe_get(intel_result, ["volume", "buy_sell_ratio"], 1.0))
            store("oi_signal", safe_get(intel_result, ["oi", "signal"], "NEUTRAL"))
            store("ca_oi", self._compact_ca_oi(
                safe_get(intel_result, ["oi", "cumulative_analysis"], {})
            ))
            
            # PCR handling (supports both 'pcr' and 'pcr_oi' keys)
            pcr_data = intel_result.get("pcr", {})
            if isinstance(pcr_data, dict):
                pcr_val = pcr_data.get("pcr", pcr_data.get("pcr_oi", 0.0))
            else:
                pcr_val = float(pcr_data)
            store("pcr", pcr_val)

            store("regime", safe_get(intel_result, ["regime", "regime"], "UNKNOWN"))
            store("order_ratio", safe_get(intel_result, ["order_flow", "ratio"], 1.0))
            store("greeks", self._compact_greeks(intel_result.get("greeks", {})))
            store("score", safe_get(intel_result, ["aggregate", "score"], 0.0))

            # Trim to max points
            if len(m["timestamps"]) > self._max_points:
                for key in m:
                    m[key] = m[key][-self._max_points:]

    def get_history(self, instrument: str) -> Dict[str, List[Any]]:
        """Get the full recorded history for an instrument"""
        with self._lock:
            return self.memory.get(instrument, {
                "timestamps": [], "vol_ratio": [], "oi_signal": [], 
                "pcr": [], "regime": [], "score": []
            })

    def clear(self, instrument: Optional[str] = None):
        """Clear memory (useful on mode switch)"""
        with self._lock:
            if instrument:
                if instrument in self.memory:
                    del self.memory[instrument]
            else:
                self.memory = {}
            self.save()
