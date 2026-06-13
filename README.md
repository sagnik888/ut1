# UT1 Index Trading System - Final Optimized Build

A state-of-the-art algorithmic trading system for Indian Indices (NIFTY, BANKNIFTY, SENSEX, MIDCPNIFTY), featuring real-time market intelligence, automated order execution, multi-timeframe regime adaptation, and advanced concurrency controls.

## Key Features
- **Multi-Timeframe Regime Adaptation:** Dynamically adjusts trading parameters based on 5-minute and 15-minute market volatility, trend strength (ADX), and momentum (RSI/MACD).
- **Market Intelligence Modules:**
  - **Option Greeks Engine:** Real-time calculation of Delta, Gamma, Theta, and Vega.
  - **Put-Call Ratio (PCR) Analysis:** Near-money and broad sentiment tracking.
  - **Order Flow & Volume Profiling:** VWAP deviations and smart volume surge detection.
- **Robust Concurrency Guard:** Prevents over-exposure by managing simultaneous trades across correlated indices, automatically grading and selecting the highest conviction setups.
- **Dynamic Risk Management:** Daily circuit breakers, trailing stop-losses, and options-specific stop-loss logic (Modified ATR vs. Natural SL).
- **Interactive UI Dashboard:** A sleek, fully responsive dashboard for monitoring system health, tracking open/closed trades, reading live market intelligence signals, and hot-swapping configurations on the fly via a persistent `.env` backend.

## Architecture
- **Scanner Engine (`scanner.py`):** The beating heart of the system orchestrating data fetching, signal generation, and trade management.
- **Signal Processor (`engine/signal_processor.py`):** Centralized brain logic for grading setups, calculating relaxed thresholds for high-conviction trades, and evaluating choppy-market confidence gates.
- **Data Management (`data_store`):** Heavily optimized SQLite database backend for recording trades and system signals with automatic garbage collection and VACUUM maintenance.
- **UI Server (`dashboard/server.py`):** A fast, thread-safe asynchronous FastAPI + WebSockets backend driving the frontend dashboard.

## Recent Optimizations
- **Architectural Decoupling:** Signal processing logic decoupled from the scanner to ensure exact parity between historical backtesting and live real-time execution.
- **Database Cleanup:** Auto-pruning of stale signals and ledger entries older than 30 days to drastically reduce disk space usage and query times (reduced DB size by 65%).
- **UI Settings Persistence:** All dynamic toggles and settings (Capital, Risk, Regime Adaptation, SL Mode) instantly persist to the local `.env` configuration file, surviving reboots seamlessly.

## Setup Instructions
1. Install requirements from `requirements.txt`.
2. Populate `.env` with AngelOne and Fyers API credentials.
3. Run `python main.py` to start the backend scanner and UI server.
4. Access the dashboard at `http://localhost:7000`.

## Disclaimer
*This software is for educational and experimental purposes. Algorithmic trading carries significant financial risk.*
