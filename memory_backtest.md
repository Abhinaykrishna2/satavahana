# Backtest Research Memory — things already tried, DO NOT REPEAT

Last updated 2026-08-24. Every item below was measured, not guessed.
Rule for this file: record the RESULT and the MECHANISM, never a chosen parameter value.

---

## 0. Sample sizes (why almost nothing is conclusive)

| population | trades | record | net |
|---|---|---|---|
| BACKTEST, 40 clean days | 11 | 9W/2L (82%) | +₹4,505 |
| **LIVE, broker-verified** | **16** | **8W/8L (50%)** | **−₹669.91** |

Clean days exclude 2026-07-17, 07-20, 08-11, 08-21 (see data/meta_data.txt).
Concentration: 2 trades = 54% of gross wins; 2 trades = 55% of gross losses.
**No parameter can be validated on this sample.** Anything that improves 11-16
trades is unfalsifiable.

---

## 1. THE BACKTEST IS NOT A FAITHFUL REPLAY OF LIVE  ← most important finding

08-24, same day, same config:
- LIVE:     entry 65.65 -> exit 70.40, net **+₹251.30**
- BACKTEST: entry 66.90 -> exit 68.65, net **+₹56.48**

Decomposition (fill offset explains only ₹29 of the ₹195 gap):
- offset ±0.50 -> +56.48 | ±0.25 -> +82.47 | ±0.00 -> +85.75
- Live trail: entry 65.65, peak **75.60** -> stop 70.62. Backtest stop 69.15
  implies the peak IT saw was only 71.40.
- The 75.60 print IS in the data (789 ticks in the 9-min window). The backtest
  missed it because it ENTERED AT A DIFFERENT TIME (spot 24242 vs live 24228).

ROOT CAUSE: the replay's scan clock starts at the first CSV row and steps by
scan_interval; live's scan clock is wall-clock aligned. **Scan phase differs**,
so signals fire on different scans at different prices, and that cascades into a
different trail peak and a different exit.

CONSEQUENCE: backtest and live take *largely different trades on the same days*
(11 vs 16 trades, barely overlapping). Day-level differences below ~₹200 are
inside the replay's own error bar. The backtest also SYSTEMATICALLY UNDERSTATES
trail-managed exits, and 7 of 10 backtest winners exit on trailed stops.

**FIX BEFORE TRUSTING ANY FURTHER BACKTEST NUMBER:** align replay scan phase to
wall-clock, and drive position checks from every tick rather than scan boundaries.

---

## 2. Entry lateness — CONFIRMED, and the cause is identified

Capture ratio = (exit-entry)/(peak - pre_entry_low):
06-23 40.6% | 06-24 58.3% | 07-09 1.8% | 07-13 13.5% | 07-14 27.9%
07-22 -60.9% | 07-23 3.7% | 07-24 17.7% | 08-05 5.8% | 08-24 11.2%
**MEDIAN ~13.5%.** In every trade the option had roughly DOUBLED before entry.

RULED OUT — execution latency: live signal 05:37:07.16 -> order 05:37:12.51 =
**5.3s** (composite >=90 takes the 5s path). Scan cadence adds <=30s. ~2% of the problem.

RULED OUT — the confidence threshold: sweeping 45..70 across 39 days admits
exactly ONE new trade (08-12). Not the binding gate.

CAUSE: the composite score is a LAGGING CONFIRMATION. It only clears once several
strategies agree, which happens after the move is visible. Live 08-24: 11:02
composite 55 (2 strategies), 11:07 composite 100 (4 strategies) -> entered ₹65.65
when the option had run from ₹33.20.

---

## 3. What actually gates trades (empirical, ~15,300 blocked decisions / 40 days)

| blocking reason | count | share |
|---|---|---|
| **no strategy fired at all** | 10,621 | **69.5%** |
| **technical veto (spot bias disagrees)** | 2,470 | **16.2%** |
| confluence / flow-confirmation / RSI-chase | 983 | 6.4% |
| **R:R floor** | 616 | **4.0%** |
| PCR conviction floor | 345 | 2.3% |
| structural gates A-E | 256 | 1.7% |

R:R is only 4% by volume BUT it is the LAST gate, so those 616 are otherwise
fully-qualified candidates. The technical veto is 4x larger — and it is fed by
indicators built from ~2 samples per "1-minute" bar (see §6).

---

## 4. R:R FLOOR SWEEP — TRIED, REJECTED, DO NOT REPEAT

| floor | trades | W/L | total | new vs base |
|---|---|---|---|---|
| base | 11 | 9/2 | +4505.25 | 0 |
| 0.95 | 13 | 10/3 | +5040.81 | 2 (1W/1L) |
| 0.90 | 14 | 10/4 | +4216.68 | 3 (1W/2L) |
| 0.85 | 14 | 10/4 | +3980.11 | 3 (1W/2L) |
| 0.80 | 33 | 17/16 | **-2248.58** | 22 (9W/13L) |
| 0.75 | 34 | 16/18 | -3204.00 | 23 (8W/15L) |

LOOSENING ALSO DESTROYS EXISTING WINNERS (floor 0.80 vs base):
06-19 +482 -> -586 | **06-23 +1756 -> +52** | 07-23 +59 -> -711
Cause: the one-trade-per-day cap. A marginal earlier entry CONSUMES the slot so
the better later trade never happens. "Enter earlier" and "keep the winners" are
in direct structural conflict through that cap.

WALK-FORWARD (train days 1..k, score day k+1, 26 OOS days):
- adaptive (re-pick floor daily from history): ₹-34.12
- baseline (never change): ₹-34.12
- **EDGE FROM ADAPTING: ₹0.00**

Fixed floor chosen on first 15 days, held for remaining 26:
base +4539/-34 | 0.95 +4539/+501 | 0.90 +4539/-323 | 0.85 +4539/-559 | 0.80 +127/-2375

**DECISIVE:** base/0.95/0.90/0.85 have IDENTICAL training P&L (+4539.37). The
training window carries ZERO information to choose among them. 0.95 scores +501
OOS but nothing observable in advance justified picking it. Selecting it because
the OOS number is visible IS selection bias.
STABLE finding: 0.80 and below are bad in BOTH train and OOS. **The R:R gate is
protective, not obstructive. LEAVE IT AT BASE.**

---

## 5. min_confidence sweep — TRIED, REJECTED

45:+5716 50:+4602 55:+4566 60:+4505 65:+6114 70:+4615 — ragged, non-monotone.
The "best" (65) traces ENTIRELY to ONE day: 07-22 flips +₹1651, every other day
moves <±42. And 70 flips 08-05 from +90 to **-1395**. Single-day flips in both
directions = noise.

DEFECT FOUND (documented, fix REVERTED at user request 2026-08-24 as not useful):
`conf_norm = ((composite - confidence_floor)/30)` where confidence_floor IS
min_confidence — so the ENTRY threshold silently rescaled the STOP. Proven on
07-22: floor 60 vs 65 = same side, same strike, same entry ₹84.80, only the stop
differs, flipping -₹1289 to +₹362. **Any min_confidence sweep is CONFOUNDED and
cannot be read as a test of entry selectivity.**

---

## 6. Latent-separation attempts on the 23 marginal trades — MOSTLY REFUTED

Split-half stability test (1st half vs 2nd half, must hold in BOTH):

| candidate rule | 1st half | 2nd half | verdict |
|---|---|---|---|
| entry hour == 10 | 2W/4L, -1339 | 5W/1L, +2119 | **SIGN FLIPS -> NOISE** |
| DTE 3.5-4.5 loses | 0W/2L, -1478 | 0W/2L, -1319 | holds, but n=2/half |
| **confidence >= 87.5 loses** | 2W/4L, -1462 | 1W/3L, -1382 | **HOLDS BOTH HALVES** |

**HOURLY / TIME-OF-DAY CONDITIONING IS REFUTED on this data.** Do not retry.

SURVIVING LEAD: within marginal setups, HIGHER composite confidence performs
WORSE (3W/7L, -₹2845 for >=87.5), consistently across both halves.
Mechanism: a high composite means many strategies already agree = the move is
already visible = late. Corroborates §2 independently. Points AWAY from "wait for
stronger confirmation".
BUT it cannot isolate the wanted trades: 07-09 (conf 88, +₹548) sits inside the
"avoid" bucket. Best any rule achieves on the marginal set is -₹431 (still negative).

DTE/SESSION DYNAMIC PARAMETERS — NOT ADDED. The engine ALREADY has 31
DTE-conditioned branches and 43 session-conditioned branches; sideways_window_mins
already varies by both. More conditioning multiplies free parameters against an
11-trade sample.

---

## 7. Multi-leg (for completeness — user has deprioritised it)

- Six exit interventions all tested, ALL WORSE, all reverted (move stop -344,
  trail -3482, ML stop tighten -1271, hard cap 0, BE-lock gross -2135, BE-lock net -2944).
- Direction signal measured as NOISE: corr(edge_pos, forward move) = +0.035;
  first half +0.274, second half -0.114 (SIGN FLIPS).
- Losers have no ex-ante signature: they were CALMER (open range 44.4 vs 58.8pt)
  and moved LESS (99 vs 110pt) than winners. Ranges overlap completely.
- Short-delta sweep 0.30..0.10: the SAME five days lose at every distance.
- Neutral-only ladder (drop CRED): 5W/5L, **-₹2590**. Condor's 3W/0L was survivorship.
- **STRUCTURALLY BLOCKED ANYWAY:** live margin ₹36,209-₹69,339 for 1 lot vs
  ₹11,531 capital. The multileg BACKTEST HAS NO MARGIN MODEL AT ALL, so its
  +30.94% is unattainable on this account.

---

## 8. Open leads worth pursuing (NOT yet tested)

1. **Tick-resolution artifact (highest value).** `spot_series.ingest()` is called
   once per SCAN, so each "1-minute" OHLC bar is built from ~2 price samples at a
   30s cadence. ATR/ADX/Bollinger/opening-range/reversal-detector are all computed
   on corrupted bars — and the technical veto (16.2% of all blocks, the largest
   active gate) is fed by them. A true 60-pt minute can register as 3 pts.
   FIX: feed every spot tick to the bar builder; keep evaluation at 30s. This is a
   DATA-PATH fix, not a parameter, so it cannot be overfit the way §4/§5 were.
2. Replay scan-phase alignment (see §1).
3. Strike selection / payoff geometry — see §9.

---

## 9. Candidate-stream meta-model (meta-labeling) — TRIED, NO DEPLOYMENT (2026-08-25)

Full protocol in `satakarni/REPORT.md` + `satakarni/RESEARCH_LOG.md` (pre-registered
gates, frozen splits, exam = 8 live-loss days + 08-24).

- Built faithful candidate funnel: Rust dumps every scan/candidate with gate-outcome tags;
  2,497 labeled candidates over 41 days; labels match known single-trade days within ₹50–150.
- Purged-K-fold AUC 0.619 — ranking skill EXISTS across the pool, but it separates junk
  from real setups, which cap=1/day can never monetize.
- Day-paired test (one trade/day): engine's own first-confirmed signal BEATS both a veto
  overlay and a model-pick at EVERY tau threshold. Model-pick significantly worse at tau>=0.6.
- MECHANISM (triple-confirms §2/§13): within the tradable queue the model prefers LATER,
  higher-confirmation entries — systematically worse prices. Earliest confirmed signal wins.
- Replay fidelity root cause fixed as far as data allows: live index-token spot was never
  recorded; parity-synthetic spot differs by 2–16 pts → replay skips 6 of 8 live-loss days.
  FIX GOING FORWARD: subscribe+record the NIFTY index token in options_ticks.
- Infrastructure kept behind flags (defaults byte-identical, 185 tests pass):
  `--scan-phase-secs` wall-clock scan anchoring, `--dump-dir` research dumps.
- DO NOT REPEAT: another selector on top of the current gate stack. Sample remains binding
  (~130 queued candidates / 32 days). Revisit only at ~250+ days or after recording index spot.
