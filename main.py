"""
UT1 Index Trading System — Main Entry Point
═══════════════════════════════════════════════════════════════
Run: python main.py
Dashboard: http://localhost:7000
"""

import sys
import asyncio
import os
import subprocess
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

import uvicorn
from loguru import logger

from config.settings import get_settings
from data.market_data import MarketDataProvider
from data.candle_builder import CandleBuilder
from engine.multi_timeframe import MultiTimeframeEngine
from engine.signal_manager import SignalManager
from intelligence.intelligence_aggregator import IntelligenceAggregator
from trading.trade_manager import TradeManager
from trading.performance_tracker import PerformanceTracker
from trading.broker import SignalSimBroker, SmartApiBroker
from scanner import Scanner
from notifications import send_desktop_notification
from dashboard.server import app, broadcast
from engine.safe_mode import validate_real_mode_readiness
import dashboard.server as dashboard_module

# Configure logging
app_log_level = os.getenv("LOG_LEVEL", "INFO").upper()
file_log_level = os.getenv("LOG_FILE_LEVEL", app_log_level).upper()
logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | <level>{message}</level>",
    level=app_log_level,
)
logger.add(
    "logs/ut1.log",
    rotation="00:00",
    retention="3 months",
    compression="zip",
    level=file_log_level,
)

def start_angelone_log_cleaner():
    def _cleaner_loop():
        import glob
        import zipfile
        import shutil
        import datetime
        while True:
            try:
                now = datetime.datetime.now()
                # Do not run heavy zip operations during active market hours (09:00 to 15:45)
                market_start = datetime.time(9, 0)
                market_end = datetime.time(15, 45)
                
                if not (market_start <= now.time() <= market_end):
                    today_str = now.strftime("%Y-%m-%d")
                    logs_dir = os.path.join(os.getcwd(), "logs")
                    for folder_path in glob.glob(os.path.join(logs_dir, "202*")):
                        if os.path.isdir(folder_path):
                            folder_name = os.path.basename(folder_path)
                            # Ensure it's a valid date folder and older than today
                            if len(folder_name) == 10 and folder_name < today_str:
                                app_log_path = os.path.join(folder_path, "app.log")
                                if os.path.exists(app_log_path):
                                    zip_name = f"smartconnect_{folder_name}.log.zip"
                                    zip_path = os.path.join(logs_dir, zip_name)
                                    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                                        zipf.write(app_log_path, arcname=f"app_{folder_name}.log")
                                shutil.rmtree(folder_path)
                                logger.info(f"Compressed legacy AngelOne log folder: {folder_name}")

                    # Database Expiry Pruning
                    import calendar
                    from data.sqlite_db import DatabaseManager
                    
                    def get_last_thursday(year, month):
                        month_cal = calendar.monthcalendar(year, month)
                        if month_cal[-1][calendar.THURSDAY] != 0:
                            return datetime.date(year, month, month_cal[-1][calendar.THURSDAY])
                        else:
                            return datetime.date(year, month, month_cal[-2][calendar.THURSDAY])
                            
                    last_thursday = get_last_thursday(now.year, now.month)
                    
                    if now.date() >= last_thursday:
                        cleanup_marker = os.path.join(logs_dir, ".last_db_cleanup")
                        current_month_str = now.strftime("%Y-%m")
                        
                        last_cleaned_month = ""
                        if os.path.exists(cleanup_marker):
                            with open(cleanup_marker, "r") as f:
                                last_cleaned_month = f.read().strip()
                                
                        if last_cleaned_month != current_month_str:
                            logger.info(f"Triggering automated Expiry-Based DB Pruning for {current_month_str}...")
                            try:
                                db = DatabaseManager()
                                db.cleanup_old_records(days=31)
                                with open(cleanup_marker, "w") as f:
                                    f.write(current_month_str)
                            except Exception as db_err:
                                logger.error(f"Failed DB Pruning: {db_err}")

            except Exception as e:
                logger.error(f"Error in AngelOne log cleaner: {e}")
            time.sleep(3600) # Check every 1 hour

    threading.Thread(target=_cleaner_loop, daemon=True, name="LogCleaner").start()



def create_system():
    """Initialize all system components"""
    settings = get_settings()
    instruments = settings.get_instruments()

    logger.info("═" * 60)
    logger.info("🚀 UT1 Index Trading System v2.0")
    logger.info("═" * 60)

    # 1. Data Provider (Yahoo Finance + AngelOne)
    data_provider = MarketDataProvider(
        api_key=settings.angelone_api_key,
        client_id=settings.angelone_client_id,
        password=settings.angelone_password,
        totp_secret=settings.angelone_totp_secret,
    )
    data_provider.connect()
    if settings.trading_mode == "REAL":
        readiness = validate_real_mode_readiness(settings, data_provider)
        if readiness.get("blockers") == ["all broker sources unavailable"]:
            for _ in range(12):
                time.sleep(0.25)
                readiness = validate_real_mode_readiness(settings, data_provider)
                if readiness.get("blockers") != ["all broker sources unavailable"]:
                    break
        if getattr(settings, "production_safe_mode_required", True) and not readiness.get("ok"):
            logger.error(
                "REAL startup refused by safe mode: "
                + "; ".join(readiness.get("blockers") or ["readiness failed"])
            )
            settings.trading_mode = "HISTORICAL"
            try:
                settings.save_to_env()
            except Exception as exc:
                logger.error(f"Failed to persist HISTORICAL safe-mode fallback: {exc}")
    
    # 1b. Broker Setup (Dual Mode)
    sim_broker = SignalSimBroker()
    real_broker = None
    if data_provider.is_connected:
        real_broker = SmartApiBroker(data_provider.smart_api)
        data_provider.fetch_instruments()
        logger.info("✅ Broker (AngelOne LIVE Ready)")
    else:
        logger.warning("⚠️ Real Broker not available (AngelOne not connected)")

    # Initial broker selection based on settings
    active_broker = real_broker if (settings.trading_mode == "REAL" and real_broker) else sim_broker
    logger.info(f"✅ Active Broker: {'REAL' if active_broker == real_broker else 'SIGNAL SIMULATION (Real Data)'}")

    # 2. Candle Builder
    candle_builder = CandleBuilder(max_candles=2000)
    logger.info("✅ Candle Builder")

    # Connect tick streams
    data_provider.on_tick = candle_builder.update_from_tick

    # 3. UT Bot Multi-TF Engine
    mtf_engine = MultiTimeframeEngine(settings.get_ut_engine_params())
    logger.info("✅ UT1 Multi-TF Engine (3 × 3 = 9 scanners)")

    # 4. Signal Manager
    signal_manager = SignalManager()
    logger.info("✅ Signal Manager (dedup + grading)")

    # 5. Intelligence Aggregator
    intelligence = IntelligenceAggregator()
    logger.info("✅ Intelligence (Vol/OI/PCR/Greeks/Regime/OrderFlow)")

    # 6. Trade Manager
    trade_manager = TradeManager(
        broker=active_broker,
        max_positions=settings.max_concurrent_positions,
        session_end=settings.ut_session_end,
        product_type=settings.trade_product_type,
    )
    # Give trade_manager references to both for hot-swapping
    trade_manager.mock_broker = sim_broker
    trade_manager.real_broker = real_broker

    # 7. Performance Tracker
    performance = PerformanceTracker()
    logger.info("✅ Performance Tracker")

    # 8. Scanner
    scanner = Scanner(
        data_provider=data_provider,
        candle_builder=candle_builder,
        mtf_engine=mtf_engine,
        signal_manager=signal_manager,
        intelligence=intelligence,
        trade_manager=trade_manager,
        performance=performance,
        instruments_config=instruments,
        trading_mode=settings.trading_mode,
        on_update=broadcast,
        on_notification=send_desktop_notification,
    )
    scanner.configure(
        capital_fut=settings.capital_fut,
        risk_fut_pct=settings.risk_fut_pct,
        capital_opt=settings.capital_opt,
        risk_opt_pct=settings.risk_opt_pct,
    )
    scanner.max_daily_loss_pct = settings.max_daily_loss_pct
    logger.info("✅ Scanner (exit-before-entry, 30s data cache)")

    # Set scanner reference for dashboard API
    dashboard_module.scanner_ref = scanner

    logger.info("═" * 60)
    logger.info(f"📊 Dashboard: http://localhost:{settings.dashboard_port}")
    logger.info(f"📊 Mode: {settings.trading_mode.upper()}")
    logger.info(f"📊 Max Daily Loss: {settings.max_daily_loss_pct}%")
    logger.info("═" * 60)

    return scanner, settings


async def run_scanner(scanner: Scanner):
    """Run scanner in background"""
    await scanner.run()


def main():
    settings = get_settings()

    async def _bootstrap_system_after_dashboard_start():
        await asyncio.sleep(0.5)
        try:
            dashboard_loop = asyncio.get_running_loop()
            scanner, _ = await asyncio.to_thread(create_system)

            async def _threadsafe_broadcast(payload):
                future = asyncio.run_coroutine_threadsafe(broadcast(payload), dashboard_loop)
                await asyncio.wrap_future(future)

            scanner.on_update = _threadsafe_broadcast

            def _run_scanner_loop():
                asyncio.run(scanner.run())

            threading.Thread(
                target=_run_scanner_loop,
                name="ScannerEventLoop",
                daemon=True,
            ).start()
        except Exception as exc:
            logger.exception(f"System bootstrap failed after dashboard start: {exc}")

    dashboard_module.startup_hook = lambda: asyncio.create_task(_bootstrap_system_after_dashboard_start())

    # Start the log cleaner thread
    start_angelone_log_cleaner()

    logger.info(f"Starting dashboard server on {settings.dashboard_host}:{settings.dashboard_port}")
    uvicorn.run(
        app,
        host=settings.dashboard_host,
        port=settings.dashboard_port,
        log_level="warning",
        ws_max_size=16777216,
    )
    return


if __name__ == "__main__":
    main()
