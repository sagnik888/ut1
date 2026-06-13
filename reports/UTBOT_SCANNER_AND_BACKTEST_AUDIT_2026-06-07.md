# UTBot Scanner, Backtest, Diagnostics, and Broker Failover Audit

Date: 2026-06-07
Runtime: http://localhost:7000
Final mode: HISTORICAL
Final window: 22 sessions

## Executive Result

The unusually low option count was a real deterministic bug, not normal market
selection. Historical option trades were paired with comparison-only futures
shadow trades. The concurrency sorter considered the FUT shadow first, allowing
it to occupy the same-index/timeframe slot and suppress the primary OPT trade.

After prioritizing the primary trade over `FUT_SHADOW`, the same 22-session run
changed from:

- Before: 140 FUT, 17 OPT, total PnL about Rs 207,026
- After: 37 FUT, 120 OPT, displayed-row PnL Rs 202,317

The total trade count remained 157. This proves that the old ratio was caused by
instrument replacement in the display/concurrency layer, not by a shortage of
option-qualified signals.

## Verified Backtest Windows

| Requested window | Trades | OPT | FUT | Win rate | Displayed PnL | Top rejects |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1 | 1 | 0 | 0.00% | Rs -856 | Choppy 20, Quality 16, C grade 5 |
| 14 | 81 | 67 | 14 | 64.20% | Rs 72,292 | Choppy 183, Quality 171, C grade 37, Confidence 28 |
| 22 | 157 | 120 | 37 | 58.60% | Rs 202,317 | Choppy 336, Quality 311, C grade 79, Confidence 53, Late 2 |
| 30 | 167 | 125 | 42 | 56.29% | Rs 202,434 | Choppy 342, Quality 322, C grade 84, Confidence 55, Late 2 |

The 30-session request cannot currently represent 30 complete sessions. Local
5-minute candle stores contain only 23-24 dates from 2026-05-05 through
2026-06-05. The small difference between the 22- and 30-session results is
therefore expected from current data coverage, not a diagnostics failure.

Machine-readable evidence:
`reports/backtest_window_diagnostics_20260607.json`

## Diagnostics Corrections

1. Reject counters now reset at the start of each full simulation.
2. Warm-up rows before the selected trade cutoff no longer increment rejects.
3. Rejects are counted once per instrument, timeframe, timestamp, direction,
   instrument type, and reason.
4. The panel displays the active window with Top Reject, for example `336, 22d`.
5. Historical mode shows pre-gate instrument selection instead of unrelated live
   session-ledger counts.
6. Historical data age is labeled `Cached history`; it is no longer shown as a
   live-feed stale warning.
7. Headline PnL and analytics are rebuilt from the final reconciled rows. The
   displayed row sum and headline now use the same accounting source.

Interpretation:

- Pre-gate selection is not the number of accepted trades. In the 22-session
  run it was 413 OPT and 531 FUT candidate selections before grade, regime,
  quality, reversal-pair, option-history, and concurrency processing.
- Reject categories are sequential. A signal appears only in the first gate
  that rejects it; categories should not be added as if each is an independent
  population.
- Option-history `attempts`, `hits`, and `misses` are operational cache/fetch
  counters, not trade counts. A warm cache can produce zero network attempts
  while still producing many hits.

## UTBot Scanner Audit

Verified strengths:

- ATR uses Wilder/RMA smoothing with alpha `1 / period`.
- The trailing-stop state machine follows the Pine-style branch logic.
- BUY/SELL flips use the same previous-stop crossover comparison as position
  state.
- 5-minute and 15-minute signals are separated from 1-minute tracking.
- Active signal selection excludes the forming candle.
- Historical concurrency is deterministic.
- Session gates apply dedicated 15-minute and 5-minute end cutoffs.
- ADX is informational unless strict ADX is enabled, then later quality grading
  applies regime-aware filtering.

Corrections made:

- `confirmed` mode now ignores a flip on the forming last bar and derives state
  from the last closed bar.
- Reprocessing unchanged candle history no longer duplicates signals in the
  engine's `signals_history`.
- Regression tests cover confirmed mode and signal-memory deduplication.

Residual scanner risks:

- The 1-minute engine remains a tracking/confluence input even though direct
  1-minute trade entries are disabled. This is intentional, but it should remain
  covered by repaint tests.
- Session settings allow values beyond the hard-coded per-timeframe caps. The
  effective close is 15:00 for 15-minute signals and 15:15 for 5-minute signals,
  regardless of a later configured general session end.
- Historical PCR/OI/Greeks quality still depends on option-chain availability.
  Missing historical chain data degrades gracefully but cannot reproduce every
  live intelligence observation.

## Broker and Yahoo Failover

Corrected behavior:

- AngelOne and Fyers now have independent WebSocket freshness clocks. A fresh
  tick from one broker can no longer make the other broker appear healthy.
- Health reports provider-specific tick age, broker degradation, and total
  broker unavailability.
- Yahoo starts in `idle_standby` and performs no polling or background API work.
  The verified startup and all backtest windows showed zero Yahoo requests.
- Yahoo is called only after live broker/REST and local historical paths fail or
  are stale.
- MIDCPNIFTY now has a Yahoo emergency mapping.
- Yahoo data is labeled `DELAYED` and `entry_eligible=false`.
- In REAL mode, delayed/context-only data cannot authorize a new ENTRY. EXIT
  candidates remain allowed so risk-reducing actions are not suppressed.
- Dashboard source status now reports `Y:STBY` or `Y:EMERG`, not a misleading
  permanent `Y:ON`.

Failover behavior:

- If AngelOne fails but Fyers remains fresh, Fyers index quotes, history, and
  option-chain support can continue the scanner.
- If Fyers fails but AngelOne remains fresh, AngelOne quotes, history, market
  data, and option instruments continue the scanner.
- If both brokers lose fresh entry-eligible data, Yahoo can preserve delayed
  context but the system blocks new live entries.

Important limitation:

Yahoo cannot replace broker order execution, live option depth, or reliable
real-time OI/Greeks. It is an emergency continuity/context source, not a third
tradable broker. Blocking new entries when only Yahoo remains is the correct
risk behavior.

## Signal Panel

The backend dashboard feed limit is now 1000 rows instead of 100. A regression
test creates 1005 closed rows and verifies exactly 1000 are returned.

## Verification

- Full pytest suite: 139 passed, 4 deselected
- Focused strategy/provider suite: 50 passed
- Python compile checks: passed
- Dashboard JavaScript syntax check: passed
- Browser verification:
  - 157 rendered rows in the final 22-session view
  - PnL Rs 202,317
  - `A:ON F:ON Y:STBY`
  - `413 OPT / 531 FUT` pre-gate selection
  - `Choppy market blocked (336, 22d)`
- Fresh runtime log: no errors after the final restart

## Recommended Next Work

1. Extend and validate local candle coverage before treating a 30-session test
   as a true 30-session comparison.
2. Add explicit intelligence-quality badges when option chain/OI/Greeks degrade
   during single-broker operation.
3. Add a controlled outage integration test that simulates Angel-only,
   Fyers-only, and both-down conditions through the full scanner loop.
