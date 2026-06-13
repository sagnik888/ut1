# UT Bot Final End-to-End Performance Audit and Parallelization Report

Date: 2026-06-12
Mode audited: REAL live session plus local test suite
Workspace: C:\Users\sagnik\Desktop\ut index 2
Scope: report-only audit. No runtime architecture changes were integrated in this pass.

## Executive Summary

The data/feed layer is healthy, but the live scanner is still under runtime pressure.

The user's audit is mostly correct on the important bottleneck: the system mixes broker IO, option-chain fetches, historical fetches, and heavy signal computation through the default asyncio executor. That can create queue contention and delayed signal processing.

However, the proposed fix should not push the entire `Scanner._process_instrument()` into a `ProcessPoolExecutor` immediately. On Windows, process pools use spawn semantics and require picklable payloads. The current scanner path carries live broker clients, caches, locks, trade manager state, dashboard state, settings, diagnostics, and mutable candle stores. Moving that whole object graph into a process pool is high-risk and could create stale-state or serialization bugs.

Recommended path:

1. Stabilize current functional regressions first.
2. Split IO and compute executors.
3. Extract a pure snapshot-based analysis function.
4. Measure ThreadPool + Numba first.
5. Use ProcessPool only for isolated heavy analysis batches after parity tests pass.
6. Use GPU only for large vector workloads, backtests, and reporting until deterministic trading parity is proven.

## Verification Results

### Passed

- Python compile check passed for core files:
  - `scanner.py`
  - `main.py`
  - `dashboard/server.py`
  - `data/market_data.py`
  - `data/warm_memory.py`
  - `engine/multi_timeframe.py`
  - `engine/ut_bot_core.py`
  - `engine/signal_processor.py`
  - `trading/trade_manager.py`
- JavaScript syntax check passed for `dashboard/static/js/app.js`.
- Built-in `scripts/full_system_audit.py` completed:
  - AngelOne connected.
  - Fyers connected.
  - 5-minute candles fetched/aligned for NIFTY, BANKNIFTY, SENSEX, MIDCPNIFTY.
  - Latest historical candle aligned to 2026-06-12 15:20:00 during audit.
  - Fyers volume present.
  - Live prices returned for all indices.
  - Option-chain fetch succeeded for all indices with 61 strikes each.
- Recovery checkpoint is enabled:
  - Path: `data_store\warm_memory\live_warm_memory.json`
  - Interval: 15 seconds
  - TTL: 15 minutes

### Failed / Blockers

Local `pytest -q` result:

- 5 failed
- 261 passed
- 4 deselected

Failing tests:

1. `test_dashboard_server.py::test_startup_hydration_does_not_run_foreground_scan_in_parallel`
   - Startup status literal expected by test is missing from `scanner.py`.

2. `test_futures_spot_basis.py::test_active_futures_profit_lock_uses_intrabar_peak_when_ltp_reverses`
   - Expected exit: `SMART_PROFIT_LOCK`
   - Actual exit: `ULTIMATE_STOP_HIT`
   - Meaning: the recently added failsafe is firing before the intended intrabar profit-lock logic in at least one futures case.

3. `test_hist_real_alignment.py::test_historical_backfill_has_5min_gate`
   - Test expects a 5-minute-specific historical quality gate.
   - Current local code generalized the gate to `5min` and `15min`, causing test drift.

4. `test_hist_real_alignment.py::test_historical_backfill_confidence_thresholds_match`
   - Live scanner no longer contains the expected explicit `confidence >= 0.72` threshold occurrences.
   - Meaning: live/historical parity tests are no longer aligned with current gate code.

5. `test_toggle_behaviour.py::test_regime_adaptation_toggle_changes_choppy_quality_gate`
   - When regime adaptation is disabled, a B+ 15-minute option setup in choppy regime is still blocked.
   - Meaning: the toggle behavior is no longer matching the intended setting contract.

These must be fixed before adding multiprocessing. Parallelizing while signal-gate behavior is drifting would make debugging much harder.

## Live Runtime Health Snapshot

From `/api/diagnostics`:

- `system_performance.status`: `CRITICAL`
- Scanner stale: true
- Scanner age: 6.83 seconds
- Stale threshold: 5.0 seconds
- Calculation lock: true
- Reported scan latency: 5350 ms
- Process RAM: 465.8 MB
- Process CPU: 71.9%
- Threads: 23
- System RAM used: 85.0%
- Disk used: 83.8%
- Broker degraded: false
- Angel tick age: 7.09 seconds
- Fyers tick age: 0.46 seconds

Interpretation:

- Feeds are not the main issue.
- Scanner compute/coordination is the main pressure point.
- RAM pressure is not fatal, but 85% system RAM means large backtest/chart payloads and process pools must be bounded carefully.

## Comparison With User Audit

### Claim: False parallelism due to default ThreadPoolExecutor and GIL

Verdict: mostly correct, with nuance.

Evidence:

- `scanner.py` uses `loop.run_in_executor(None, ...)` for:
  - live LTP fallbacks
  - option-chain/intelligence fetches
  - history fetches
  - `_process_instrument()`
- `None` means the default event-loop executor, so IO and compute can contend in the same pool.
- `scanner.py` uses `asyncio.gather()`, but the gathered tasks still submit blocking work into the same shared pool.

Nuance:

- Pandas and NumPy are not always fully GIL-bound; many vectorized operations release the GIL internally.
- But Python orchestration, object mutation, signal filtering, intelligence aggregation, DB/dashboard work, and cache updates still cause contention.
- The observed scanner stale/latency state confirms real runtime pressure.

### Claim: Shared IO / CPU thread pool is a bottleneck

Verdict: correct.

This is the highest-confidence finding.

The system should not let slow broker/API calls compete with candle generation, UT Bot math, signal scoring, dashboard snapshots, and trade-state updates in the same default executor.

### Claim: Multi-timeframe processing is sequential

Verdict: correct.

Evidence:

- `engine/multi_timeframe.py` loops through `["1min", "5min", "15min"]` sequentially inside `process_instrument()`.
- `process_all()` loops instruments sequentially too, though the live scanner itself wraps instruments in async tasks.

Nuance:

- Parallelizing every timeframe for every instrument may oversubscribe CPU: 4 indices x 3 timeframes plus intelligence workers plus dashboard work.
- The safer first target is instrument-level parallelism with clean worker boundaries, then benchmark timeframe-level parallelism only if needed.

### Claim: Add Numba to heavy UT math

Verdict: partially already implemented.

Evidence:

- `engine/ut_bot_core.py` already imports `numba.njit`.
- Existing Numba functions:
  - `_nb_ffill`
  - `_nb_heikin_ashi`
  - `_nb_trailing_stop`

Gap:

- Decorators are currently `@njit`, not explicitly `@njit(nogil=True, cache=True)`.
- ATR and ADX still rely heavily on Pandas `ewm`, `shift`, `where`, and Series construction.

Recommendation:

- Do not install Numba blindly; it is already available and used.
- Add explicit `nogil=True` where valid.
- Consider pure NumPy/Numba ATR and ADX implementations only after parity tests prove exact TradingView/Pandas equivalence.

### Claim: Use ProcessPoolExecutor for compute

Verdict: directionally valid, but not as a first step.

Why risky now:

- `_process_instrument()` is not a pure function.
- It touches scanner state, market data, candle caches, trade candidates, diagnostics, settings, intelligence cache, option-chain data, session rows, and dashboard payload construction.
- Windows process pools require pickling and spawn new interpreter processes.
- Passing Pandas DataFrames into processes can cost more than the computation if the payload is not minimized.

Safer design:

- First extract:
  - `analyze_instrument_snapshot(snapshot: dict) -> dict`
- Snapshot should contain only:
  - instrument
  - lot size and risk params
  - 1min/5min/15min OHLCV arrays or compact DataFrames
  - cached option-chain/intelligence inputs
  - immutable settings relevant to scoring
- Worker should return only:
  - MTF states
  - candidate signals
  - scores
  - rejection reasons
  - timing metrics

Trade execution, DB writes, broker orders, dashboard cache mutation, and recovery checkpoints should remain in the main process.

### Claim: GPU/CuPy should be considered

Verdict: available but not first priority.

Evidence:

- GPU detected: NVIDIA GeForce RTX 4060 Laptop GPU.
- CuPy backend available: `cupy_cuda`.
- Benchmark sample size 1,000,000:
  - GPU: 9.58 ms
  - CPU: 38.33 ms
  - Speedup: 4.0x
  - Parity: true
- `engine/gpu_accelerator.py` explicitly marks `decision_acceleration` as disabled.

Recommendation:

- Keep trading decisions CPU/Numba first.
- Use GPU for:
  - large backtest vector stats
  - PnL/drawdown summaries
  - report analytics
  - massive historical sweeps
- Do not move live per-signal decision logic to GPU until deterministic parity and transfer overhead are proven.

## Additional Findings Beyond User Audit

### 1. Current local workspace is not clean

Tracked modified files:

- `RESTORE_POINT_26_INFO.txt`
- `config/settings.py`
- `engine/signal_processor.py`
- `scanner.py`
- `trading/trade_manager.py`

There are many untracked scratch/repair files. This matters because a performance audit should be based on a stable baseline. The previously published clean system passed more tests; the current local tree does not.

### 2. Signal gate logic has drifted

Recent local changes introduced:

- `is_explosive_bypass`
- generalized 5min/15min gates
- explosive move bypass logic
- ultimate failsafe earlier in historical/active logic

These may be valuable, but the failing tests show behavior contracts are now inconsistent.

### 3. Failsafe priority may be too aggressive

The futures profit-lock test shows `ULTIMATE_STOP_HIT` beating `SMART_PROFIT_LOCK`.

Risk:

- A profitable intrabar move can be classified as a stop/failsafe exit instead of a protected win.
- Dashboard reports and strategy metrics can become misleading.

### 4. Diagnostics are good but dashboard state is lightened

`/api/state` intentionally strips/limits heavy payloads. Full runtime health is better read from `/api/diagnostics`.

This is correct for speed, but the dashboard should make it obvious when it is showing a light state versus full paged trade history.

### 5. Startup pressure is not only scan math

Startup work includes:

- data hydration
- websocket connections
- historical fetch/cache merge
- dashboard cache initialization
- recovery checkpoint load
- possible backtest/session hydration
- chart snapshots

Parallel compute helps, but startup needs cancellation/superseding and bounded hydration too.

### 6. Queue superseding is present but needs broader coverage

Diagnostics show recalculation queue state fields exist. But expensive paths still use direct `run_in_executor(None, ...)`. Any setting-change pipeline should use "latest request wins" semantics, especially for backtest days, mode changes, and chart reloads.

### 7. Trade execution must stay priority path

Parallelizing all scanner work equally can harm trading if exit checks wait behind analytics. Priority should be:

1. Active trade exits and broker order status
2. LTP and candle updates
3. matured signal confirmation
4. new signal scan
5. dashboard/chart/report generation
6. historical/backtest hydration

### 8. ProcessPool can worsen latency if payloads are too large

With only 4 indices and 3 timeframes, copying DataFrames to child processes every scan can cost more than the calculation. The process-pool design must use compact snapshots, shared memory, or persistent workers with small payloads.

### 9. `asyncio.gather()` is not enough

Gather only coordinates awaitables. It does not guarantee CPU parallelism. True speed comes from:

- non-blocking IO
- dedicated IO pool
- bounded compute pool
- GIL-free compiled loops
- process isolation for truly CPU-bound work
- avoiding unnecessary work entirely through caching and invalidation

### 10. Logs/reports should include per-stage timing

Current diagnostics show scan-level latency. To find the real bottleneck, add timing buckets:

- LTP fetch
- candle update/build
- 1min UT
- 5min UT
- 15min UT
- intelligence analysis
- signal gate
- trade manager update
- dashboard payload build
- DB/session save
- websocket broadcast

Without these histograms, performance tuning becomes guesswork.

## Recommended Parallelization Plan

### Phase 0: Stabilize Before Speed

Do this before any parallel architecture change:

- Fix the 5 failing tests.
- Decide whether recent explosive-bypass/failsafe logic is intended.
- Re-align historical and live gates.
- Re-run `pytest -q` until green.
- Capture baseline metrics:
  - average scan latency
  - p95 scan latency
  - p99 scan latency
  - stale scanner count per hour
  - missed/rejected signal count
  - feed tick age
  - dashboard render payload size

Exit gate:

- Test suite green.
- Compile and JS checks green.
- Live diagnostics not CRITICAL during normal feed conditions.

### Phase 1: Split IO and Compute Pools

Add explicit executors:

- `io_executor = ThreadPoolExecutor(max_workers=8)`
- `compute_executor = ThreadPoolExecutor(max_workers=min(4, os.cpu_count()))`
- Optional `db_executor = ThreadPoolExecutor(max_workers=1 or 2)`

Route:

- Broker/API calls -> IO pool
- candle/signal analysis -> compute pool
- DB/report/checkpoint writes -> DB pool or async queue

Do not use the default executor for everything.

Expected benefit:

- Lower random latency spikes.
- Faster responsiveness when broker APIs slow down.
- Lower dashboard throttling during scans.

Risk:

- Low, if wiring is careful and tests are green.

### Phase 2: Pure Snapshot Analysis Function

Extract scanner-heavy math into a pure function:

`analyze_instrument_snapshot(snapshot) -> result`

Rules:

- No broker calls inside worker.
- No DB writes.
- No dashboard mutation.
- No trade execution.
- No global cache mutation.
- No reliance on live `self`.

Expected benefit:

- Makes ThreadPool/ProcessPool selectable.
- Makes reproducible benchmarking possible.
- Makes tests easier and signal parity safer.

Risk:

- Medium, because scanner behavior is currently stateful.

### Phase 3: Numba Upgrade With Parity Tests

Upgrade current `@njit` functions to explicit:

- `@njit(nogil=True, cache=True)`

Then consider Numba versions of:

- ATR RMA
- ADX / DI
- crossover detection
- signal extraction

Required tests:

- Pandas vs Numba parity on all index/timeframe samples.
- TradingView reference parity where available.
- No signal timestamp drift.
- No stop/trailing drift beyond tiny float tolerance.

Expected benefit:

- Real CPU parallelism even in threads.
- Less need for ProcessPool.

Risk:

- Medium to high if ATR/ADX exactness changes.

### Phase 4: ProcessPool Only For Isolated Compute

Use `ProcessPoolExecutor` only after Phase 2 is complete.

Good candidates:

- historical backtest sweeps
- full 30-day recalculation
- multi-index replay
- expensive intelligence summaries
- large report generation

Bad initial candidates:

- live active-trade exit checks
- broker order path
- per-tick LTP updates
- dashboard cache mutation

Expected benefit:

- True multicore speed on heavy CPU batches.

Risk:

- High if state isolation is incomplete.
- Windows pickling/spawn cost must be measured.

### Phase 5: Dashboard and Chart Decoupling

Keep `/api/state` light.

Improve:

- chart snapshots should be served only from `/api/chart`
- chart rebuild should be throttled and cached by `(instrument, timeframe, last_candle_timestamp)`
- trade table should page via `/api/trades`
- websocket updates should send deltas where possible

Expected benefit:

- Less UI lag.
- Lower CPU during refresh.
- Lower JSON serialization pressure.

### Phase 6: GPU For Batch Analytics

Use GPU where payload is large enough:

- backtest portfolio stats
- drawdown curves
- Monte Carlo or parameter sweeps
- large historical signal score matrix

Do not use GPU for live decision path until:

- deterministic parity passes
- CPU transfer overhead is measured
- fallback behavior is proven

## Priority Work Queue

1. Fix current test regressions.
2. Add per-stage scan timing metrics.
3. Split IO/compute/default executors.
4. Route broker/history/option-chain calls to IO pool.
5. Extract pure instrument analysis snapshot.
6. Benchmark ThreadPool + Numba before ProcessPool.
7. Add ProcessPool for historical/backtest batches only.
8. Add dashboard chart/trade payload cache metrics.
9. Add "latest setting wins" cancellation to all heavy setting-triggered jobs.
10. Re-run full tests and live diagnostics after each phase.

## Final Recommendation

Do not approve a direct jump to ProcessPool-based `_process_instrument()` yet.

Approve Phase 0 and Phase 1 first:

- stabilize failing tests
- separate IO and compute pools
- add detailed timing instrumentation

After that, approve Phase 2 snapshot extraction. Once a pure analysis function exists, ProcessPool and Numba/GPU acceleration can be introduced safely and measured honestly.

The system can be made faster, but the next move should be precision first, speed second.
