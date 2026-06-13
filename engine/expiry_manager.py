import requests
import pandas as pd
import json
from datetime import datetime, date
from typing import Dict, List, Optional
from loguru import logger
from pathlib import Path

class ExpiryManager:
    """
    Intelligent Expiry Resolver for AngelOne
    Resolves Weekly/Monthly expiries, handling holiday shifts and instrument specific rules.
    """
    
    TOKEN_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
    CACHE_DIR = Path("data_store")
    CACHE_FILE = CACHE_DIR / "instruments.json"
    OPTION_ROLLOVER_DAYS = 2

    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.expiries: Dict[str, Dict] = {}
        self.CACHE_DIR.mkdir(exist_ok=True)
        self._loaded_date = date.today()
        self._token_cache: Dict[tuple, Dict] = {}

    def download_master(self, force=False):
        """Download and cache the instrument master from AngelOne"""
        try:
            # Clear token cache on reload
            self._token_cache = {}
            # Check if cache is fresh (today's date)
            if not force and self.CACHE_FILE.exists():
                file_time = datetime.fromtimestamp(self.CACHE_FILE.stat().st_mtime).date()
                if file_time == date.today():
                    logger.info("Using cached instrument master.")
                    with open(self.CACHE_FILE, 'r') as f:
                        data = json.load(f)
                    self.df = pd.DataFrame(data)
                    self._loaded_date = date.today()
                    logger.info(f"Loaded master with columns: {self.df.columns.tolist()[:5]}")
                    return True

            logger.info(f"Downloading instrument master from AngelOne...")
            response = requests.get(self.TOKEN_URL, timeout=30)
            if response.status_code == 200:
                data = response.json()
                with open(self.CACHE_FILE, 'w') as f:
                    json.dump(data, f)
                self.df = pd.DataFrame(data)
                self._loaded_date = date.today()
                logger.success("Instrument master downloaded and cached.")
                return True
            else:
                logger.error(f"Failed to download master: Status {response.status_code}")
                # Fallback to CSV if available
                csv_path = Path("data_store/instruments.csv")
                if csv_path.exists():
                    logger.info("Falling back to data_store/instruments.csv")
                    self.df = pd.read_csv(csv_path)
                    self._loaded_date = date.today()
                    return True
        except Exception as e:
            logger.error(f"Failed to download instruments: {e}")
            # Fallback to CSV if available
            csv_path = Path("data_store/instruments.csv")
            if csv_path.exists():
                logger.info("Falling back to data_store/instruments.csv")
                self.df = pd.read_csv(csv_path)
                self._loaded_date = date.today()
                return True
        return False

    def analyze_expiries(self):
        """Analyze the master list to identify Weekly/Monthly expiries for core indices"""
        # Clear token cache on analysis
        self._token_cache = {}
        # Automatic calendar rollover reload check
        if hasattr(self, "_loaded_date") and date.today() != self._loaded_date:
            logger.info("Calendar day rollover detected! Reloading instruments master...")
            self.download_master(force=True)
            self._loaded_date = date.today()

        if self.df is None or self.df.empty:
            logger.warning("No data to analyze expiries.")
            return

        indices = ["NIFTY", "BANKNIFTY", "SENSEX", "MIDCPNIFTY"]
        today_str = date.today().strftime("%d%b%Y").upper()
        
        # Convert expiry column to datetime for easier sorting/comparison
        # AngelOne format is usually '25APR2024'
        self.df['expiry_dt'] = pd.to_datetime(self.df['expiry'], format='%d%b%Y', errors='coerce')
        
        for name in indices:
            # 1. Filter Futures
            fut_df = self.df[(self.df['name'] == name) & (self.df['instrumenttype'] == 'FUTIDX')].copy()
            fut_df = fut_df.sort_values('expiry_dt')
            
            # 2. Filter Options
            opt_df = self.df[(self.df['name'] == name) & (self.df['instrumenttype'].isin(['OPTIDX']))].copy()
            opt_df = opt_df.sort_values('expiry_dt')
            
            all_expiries = sorted(opt_df['expiry_dt'].dropna().unique())
            now = pd.Timestamp(date.today())
            
            # Current/Nearest Expiry (Weekly)
            future_expiries = [d for d in all_expiries if d >= now]
            nearest_expiry = future_expiries[0] if len(future_expiries) > 0 else None
            next_weekly = future_expiries[1] if len(future_expiries) > 1 else nearest_expiry
            
            # Determine Monthly Expiries (Last expiry of each month)
            monthly_list = []
            for exp in future_expiries:
                m_list = [d for d in all_expiries if d.month == exp.month and d.year == exp.year]
                if m_list:
                    monthly_list.append(m_list[-1])
            
            unique_monthlies = sorted(list(set(monthly_list)))
            monthly_expiry = unique_monthlies[0] if len(unique_monthlies) > 0 else None
            next_monthly = unique_monthlies[1] if len(unique_monthlies) > 1 else monthly_expiry

            # Calculate DTE for Rollover logic (2 day prior rule)
            # We use nearest_expiry for weekly and monthly_expiry for monthly
            dte_weekly = (nearest_expiry - now).days if nearest_expiry else 10
            dte_monthly = (monthly_expiry - now).days if monthly_expiry else 10
            
            # User Rule: rollover two calendar days before expiry (T-2, T-1, T-0).
            is_weekly_rollover = dte_weekly <= self.OPTION_ROLLOVER_DAYS
            is_monthly_rollover = dte_monthly <= self.OPTION_ROLLOVER_DAYS

            # 3. Determine Final Active Expiries based on rollover rules
            # NIFTY/SENSEX use weekly contracts and roll to next_weekly at T-2.
            final_weekly = next_weekly if is_weekly_rollover else nearest_expiry
            
            # BANKNIFTY/MIDCPNIFTY use monthly contracts and roll to next_monthly at T-2.
            final_monthly = next_monthly if is_monthly_rollover else monthly_expiry

            # 4. Futures Rollover (1 day before expiry rule)
            current_fut_expiry = fut_df.iloc[0]['expiry_dt'] if not fut_df.empty else None
            fut_dte = (current_fut_expiry - now).days if current_fut_expiry else 10
            is_fut_rollover = fut_dte <= 1
            
            fut_idx = 1 if (is_fut_rollover and len(fut_df) > 1) else 0
            if is_fut_rollover and len(fut_df) > 1:
                logger.info(f"🔄 Futures Rollover triggered for {name} (DTE: {fut_dte}). Using next month's contract.")

            # Results Mapping
            self.expiries[name] = {
                "weekly": nearest_expiry.strftime('%d%b%Y').upper() if nearest_expiry else None,
                "next_weekly": next_weekly.strftime('%d%b%Y').upper() if next_weekly else None,
                "monthly": monthly_expiry.strftime('%d%b%Y').upper() if monthly_expiry else None,
                "next_monthly": next_monthly.strftime('%d%b%Y').upper() if next_monthly else None,
                "active_weekly": final_weekly.strftime('%d%b%Y').upper() if final_weekly else None,
                "active_monthly": final_monthly.strftime('%d%b%Y').upper() if final_monthly else None,
                "current_fut": fut_df.iloc[fut_idx]['symbol'] if not fut_df.empty else None,
                "current_fut_token": fut_df.iloc[fut_idx]['token'] if not fut_df.empty else None,
                "is_monthly_week": nearest_expiry == monthly_expiry if nearest_expiry and monthly_expiry else False,
                "is_weekly_rollover": is_weekly_rollover,
                "is_monthly_rollover": is_monthly_rollover
            }
            
            logger.info(f"Resolved {name}: Weekly={self.expiries[name]['weekly']}, Monthly={self.expiries[name]['monthly']}")
            if is_weekly_rollover: logger.info(f"⚠️ {name} Weekly Rollover Active: Using {self.expiries[name]['active_weekly']}")
            if is_monthly_rollover: logger.info(f"⚠️ {name} Monthly Rollover Active: Using {self.expiries[name]['active_monthly']}")

    def _ensure_expiry_dates(self):
        if self.df is not None and "expiry_dt" not in self.df.columns:
            self.df["expiry_dt"] = pd.to_datetime(self.df["expiry"], format="%d%b%Y", errors="coerce")

    def _ensure_ready(self) -> bool:
        """Load and analyze the instrument master before token lookups."""
        needs_reload = (
            self.df is None
            or self.df.empty
            or not self.expiries
            or (hasattr(self, "_loaded_date") and date.today() != self._loaded_date)
        )
        if needs_reload:
            if not self.download_master():
                return False
            self.analyze_expiries()
        return self.df is not None and not self.df.empty and bool(self.expiries)

    def _select_option_expiry_for_date(self, name: str, target_date: date, expiry_type: str = "weekly") -> Optional[pd.Timestamp]:
        self._ensure_expiry_dates()
        opt_df = self.df[
            (self.df["name"] == name)
            & (self.df["instrumenttype"].isin(["OPTIDX"]))
            & (self.df["expiry_dt"].dt.date >= target_date)
        ].copy()
        if opt_df.empty:
            return None

        all_expiries = sorted(opt_df["expiry_dt"].dropna().unique())
        if expiry_type in ("monthly", "active_monthly"):
            monthlies = []
            for exp in all_expiries:
                same_month = [d for d in all_expiries if d.month == exp.month and d.year == exp.year]
                if same_month:
                    monthlies.append(same_month[-1])
            candidates = sorted(set(monthlies))
        else:
            candidates = all_expiries

        if not candidates:
            return None

        current = candidates[0]
        next_expiry = candidates[1] if len(candidates) > 1 else current
        dte = (current.date() - target_date).days
        if dte <= self.OPTION_ROLLOVER_DAYS and next_expiry != current:
            return pd.Timestamp(next_expiry)
        return pd.Timestamp(current)

    def get_token_for_date(self, name: str, strike: float, option_type: str, target_date: date, expiry_type: str = "weekly") -> Optional[Dict]:
        """Find an option token for the signal date, respecting weekly/monthly T-2 rollover."""
        if not self._ensure_ready():
            return None

        self._ensure_expiry_dates()
        cache_key = (name, float(strike), option_type, target_date.isoformat(), expiry_type)
        if cache_key in self._token_cache:
            return self._token_cache[cache_key]

        expiry_dt = self._select_option_expiry_for_date(name, target_date, expiry_type)
        if expiry_dt is None:
            return None

        opt_df = self.df[
            (self.df["name"] == name)
            & (self.df["instrumenttype"].isin(["OPTIDX"]))
            & (self.df["symbol"].str.endswith(option_type))
            & (self.df["expiry_dt"] == expiry_dt)
        ].copy()
        if opt_df.empty:
            return None

        expiry_df = opt_df.copy()
        expiry_str = expiry_dt.strftime("%d%b%Y").upper()

        exact = expiry_df[expiry_df["strike"].astype(float) == float(strike) * 100]
        if not exact.empty:
            res = exact.iloc[0].to_dict()
            res["date_aware_expiry"] = expiry_str
            self._token_cache[cache_key] = res
            return res

        expiry_df["strike_val"] = expiry_df["strike"].astype(float) / 100.0
        expiry_df["diff"] = (expiry_df["strike_val"] - float(strike)).abs()
        best_match = expiry_df.loc[expiry_df["diff"].idxmin()]
        res = best_match.to_dict()
        res["date_aware_expiry"] = expiry_str
        self._token_cache[cache_key] = res
        logger.info(f"Using closest dated strike: {name} {expiry_str} {best_match['strike_val']} {option_type}")
        return res

    def get_token(self, name: str, strike: float, option_type: str, expiry_type: str = "weekly") -> Optional[Dict]:
        """Find the specific token for an option strike"""
        if not self._ensure_ready(): return None
        
        # Check token cache
        cache_key = (name, float(strike), option_type, expiry_type)
        if cache_key in self._token_cache:
            return self._token_cache[cache_key]
            
        expiry_val = self.expiries.get(name, {}).get(expiry_type)
        if not expiry_val: return None
        
        logger.debug(f"Searching for {name} {expiry_type} ({expiry_val}) strike {strike} ({float(strike)*100})")
        
        nifty_opt = self.df[(self.df['name'] == name) & (self.df['expiry'] == expiry_val)]
        
        # Exact match logic
        match = self.df[
            (self.df['name'] == name) & 
            (self.df['expiry'] == expiry_val) & 
            (self.df['strike'].astype(float) == float(strike) * 100) & # AngelOne uses strike * 100
            (self.df['symbol'].str.endswith(option_type))
        ]
        
        if not match.empty:
            res = match.iloc[0].to_dict()
            self._token_cache[cache_key] = res
            return res
            
        # Fallback: Find closest available strike
        if not nifty_opt.empty:
            logger.warning(f"Exact strike {strike} not found for {name} on {expiry_val}. Finding closest available...")
            # Filter by option type too to avoid mismatch
            opt_type_filtered = nifty_opt[nifty_opt['symbol'].str.endswith(option_type)].copy()
            if not opt_type_filtered.empty:
                opt_type_filtered['strike_val'] = opt_type_filtered['strike'].astype(float) / 100.0
                opt_type_filtered['diff'] = (opt_type_filtered['strike_val'] - float(strike)).abs()
                best_match = opt_type_filtered.loc[opt_type_filtered['diff'].idxmin()]
                res = best_match.to_dict()
                self._token_cache[cache_key] = res
                logger.info(f"Using closest available strike: {best_match['strike_val']}")
                return res
                
        return None

    def is_expiry_day(self, name: str, target_date: Optional[date] = None) -> bool:
        """Returns True if target_date is the weekly/monthly expiry day for the instrument"""
        dt = target_date or date.today()
        
        # Check against dynamically fetched expiries
        if self.expiries and name in self.expiries:
            exp_data = self.expiries[name]
            dt_str = dt.strftime('%d%b%Y').upper()
            active_key = "active_monthly" if name in {"BANKNIFTY", "MIDCPNIFTY"} else "active_weekly"
            return dt_str == exp_data.get(active_key)
                
        # Fallback to hardcoded defaults only if fetch failed
        if name == "NIFTY":
            return dt.weekday() == 3 # Thursday
        elif name == "SENSEX":
            return dt.weekday() == 4 # Friday
        elif name == "BANKNIFTY":
            return dt.weekday() == 2 # Wednesday
        elif name == "MIDCPNIFTY":
            return dt.weekday() == 1 # Tuesday
        return False

    def get_dte(self, name: str, target_date: Optional[date] = None) -> int:
        """Get Days To Expiry (DTE) for the given instrument on a specific date"""
        dt = target_date or date.today()
        expiry_type = "active_monthly" if name in {"BANKNIFTY", "MIDCPNIFTY"} else "active_weekly"
        exp_dt = None
        if target_date is not None and self.df is not None and not self.df.empty:
            selected = self._select_option_expiry_for_date(name, target_date, expiry_type)
            if selected is not None:
                exp_dt = selected.date()
        if exp_dt is None:
            exp_str = (
                self.expiries.get(name, {}).get(expiry_type)
                or self.expiries.get(name, {}).get("weekly")
            )
            if not exp_str:
                return 3
            try:
                exp_dt = datetime.strptime(exp_str, "%d%b%Y").date()
            except Exception:
                return 3
        diff = (exp_dt - dt).days
        return max(0, diff)

    def pre_market_check(self):
        """Main entry point for daily pre-market analysis"""
        logger.info("═══ PRE-MARKET EXPIRY ANALYSIS ═══")
        if self.download_master():
            self.analyze_expiries()
            return self.expiries
        return {}

# Singleton for system-wide use
expiry_manager = ExpiryManager()
