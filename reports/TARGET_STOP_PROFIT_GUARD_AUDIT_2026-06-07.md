# Target, Stop, Quality Gate, Repaint Guard Audit - 2026-06-07

## Scope

Audited the live and historical paths for:

- Entry target and stoploss calculation.
- Dynamic stop/target adjustment when trade moves in favor, against, choppy, volatile, or trending.
- Quality gates, filters, and choppy-market rejection behavior.
- Anti-repaint stabilization and repaint-abort guards.
- Profit protection: breakeven lock, runner mode, smart profit lock, major-win guard, low-gain guard, stagnation exit.
- Market-intelligence use in signal generation, active trade tracking, and live 1min/native timeframe handling.

## Fixes Made

### 1. Fixed pytest collection contamination

`test_repaint_guard.py` globally stubbed real packages during collection:

- `websockets`
- `intelligence`
- `trading.trade_manager`

That made later tests import `MagicMock` instead of real modules depending on collection order. Removed those broad stubs so the safety suite now tests the real project modules.

### 2. Fixed live/historical RR target flattening

The live signal path calculated grade/intel-aware RR, then immediately overwrote it with confidence-only RR. That meant A+, A, B+, and supportive/contradicting intel did not actually affect live target runway.

Added `SignalProcessor._dynamic_rr()`:

- Confidence remains the baseline.
- A+/A/B+ grades add bounded target runway.
- Contradicting/supportive intelligence adjusts RR within a cap.
- RR is capped between 1.10 and 3.00.

Applied to:

- Live `SignalProcessor.process_best_signal()`.
- Historical option/futures simulation in `engine/signal_processor.py`.
- Scanner-side legacy/fallback candidate generation in `scanner.py`.

Added tests proving:

- B+ gets more runway than B.
- A+ with supportive intel gets more runway than B+.
- Weak/contradicted signals cannot go below 1.10.
- Extreme inputs cannot exceed 3.00.

## How The System Currently Determines Stoploss And Targets

### Entry stop

- UTBot provides natural trailing-stop / ATR-derived stop distance.
- Futures stop is capped by `futures_sl_pct`.
- Options are premium-long instruments:
  - CE and PE are both treated as buy-premium trades.
  - Natural index stop is converted through delta/multiplier.
  - Premium stop is capped by `options_sl_pct`.
  - In `NATURAL` SL mode, the natural UTBot stop is respected.

### Entry target

- Target is based on stop distance multiplied by dynamic RR.
- Dynamic RR now uses confidence + grade + intel.
- Options live entries convert to actual premium LTP first, then set premium stop/target.
- Futures live entries use futures contract LTP when available, otherwise preserve spot/futures basis.

### Multiple target behavior

The system does not yet have true partial scale-out targets. It has a runner-style target model:

- T1 trigger occurs around 95 percent of the hard target.
- Once T1 is reached, `runner_mode` is enabled.
- Hard target is removed.
- Stop trails to lock roughly 90 percent of current gains.

So this is better described as "T1 into runner mode", not true T1/T2/T3 partial booking.

## Dynamic Trade Management

### Trending / strong trend

- Uses the native trade timeframe UTBot trailing stop when `UT_SMART_TRAILING=True`.
- Stop only moves in the favorable direction.
- For futures, spot-level stop is converted to futures contract basis.

### Choppy / volatile / mean reverting

- Uses previous 1min candle low/high as tighter stop:
  - Long: prior 1min low.
  - Short: prior 1min high.
- This makes the system cut faster when market becomes messy.

### Profit protection

- Profit protector locks breakeven-plus buffer after profit exceeds initial risk.
- Smart profit lock protects 65 percent of peak after peak >= Rs.1000.
- Scalp-lock mode uses lower threshold Rs.500 and 70 percent retention.
- Major-win guard protects 75 percent after peak >= Rs.3000.
- Low-gain guard exits if a 10 percent gain collapses near breakeven.
- Stagnation exit closes profitable trades held 15+ minutes if they fade below 85 percent of peak.
- Intrabar 1min candle extremes are used to reconstruct missed peaks, so a fast spike is still protected even if sampled LTP has already reversed.

### Market-intelligence exits

When `UT_INTEL_EARLY_EXIT=True`:

- Explicit OI/PCR reversal exits fire before generic aggregate score exits.
- LONG exits on strong bearish intel flip.
- SHORT exits on strong bullish intel flip.

## Signal Generation And Tracking

### Signal generation

- MTF engines track 1min, 5min, and 15min.
- Current entry policy is `INCLUDE_5MIN`.
- 5min entries are allowed only when quality is strong enough, especially in choppy regimes.
- 1min is primarily used for fast tracking/trailing/market context, not as the main clean-entry source.

### Live anti-repaint protection

- Live entries go into a stabilization buffer:
  - 25s for 1min/5min.
  - 30s for 15min.
- Candidate must still have the same signal timestamp and signal type after the buffer.
- Future timestamp and stale pending signals are discarded.
- After entry/session ledger creation:
  - 5min/15min get periodic 2-minute intrabar repaint checks.
  - Final candle-close check confirms the source signal still exists.
  - If signal disappears, system creates `REPAINT_ABORT` exit.

## Quality Gates And Filters

Verified filters include:

- Grade and confidence gates.
- Choppy/ranging/volatile stricter option gates.
- 5min exceptional-entry gate.
- Option premium quality gate.
- Market-location gate using 1min EMA9/EMA21, day position, impulse, and reversal structure.
- No-fresh-entry time gate.
- Correlated index exposure guard.
- Historical/live boundary guard.
- Repaint stabilization guard.

## Verification

Targeted suite after patch:

- `73 passed, 1 deselected`

Full suite after patch:

- `141 passed, 4 deselected, 1 warning`

Runtime API sanity:

- `GET /api/status`: running.
- Mode: `REAL`.
- Scanner running: true.
- Connected clients: 2.
- Last scan latency: about 224 ms.
- Yahoo fallback: `idle_standby`, request count 0.
- Open trades: 0.
- Closed trades in current real session: 0.

## Current Verdict

The system is reasonably smart and layered for live trade management:

- It does read native timeframe plus 1min context.
- It does use market intelligence for signal filtering and early exits.
- It has strong anti-repaint guardrails.
- It has meaningful profit protection, including intrabar peak reconstruction.

But two limitations remain:

- Multiple targets are not true partial exits. It is runner-mode after T1, not T1/T2/T3 scale-out.
- Historical backtests still cannot perfectly reproduce every live trailing/profit-lock behavior unless every active-trade tick/1min path is replayed exactly.

## Recommended Next Upgrade

If we want true institutional target management, implement explicit scale-out:

- T1: partial book and stop to breakeven.
- T2: second partial book or trail.
- T3: runner with 70-90 percent gain lock.
- Persist per-leg exit rows so dashboard PnL and backtest PnL stay exact.

