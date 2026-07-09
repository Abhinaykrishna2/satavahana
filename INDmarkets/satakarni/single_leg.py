#!/usr/bin/env python3
"""satakarni/single_leg.py — single-leg BUYER exit experiment (sandbox, ₹15k).

QUESTION (from the live 25-Jun trade): the CE peaked +19% then round-tripped to the
+2% one-shot lock, netting ~breakeven. Instead of locking and praying for the +25%
target, what if we KEEP SCANNING and — while long a CE — close the moment a fresh PE
(bearish) trigger fires? And symmetrically for a PE. The opposite signal is an
early-warning exit that should fire as the move rolls over, near the peak, instead of
giving everything back to a static +2% stop.

DESIGN — an EXIT A/B on the SAME entries (so the delta is purely the exit):
  arm A  baseline   = the live exits: +12%→entry+2% one-shot lock / +25% target /
                      -35% stop / 15:15 time-exit.
  arm B  opp-exit   = arm A PLUS: a fresh OPPOSITE-side trigger closes the position.
Identical entry, identical management except B's extra exit => clean comparison.

SCOPE / HONESTY (this is NOT a verdict on the live engine):
  • The live single-leg is a 6-strategy composite (today fired as "Composite
    Multi-Signal"). This sandbox uses ONE fixed reversal trigger — spot reclaim off a
    30-bar low (CE) / breakdown off a 30-bar high (PE) — the same SHAPE as the live
    "Spot reclaimed from recent low over 30 closed bars" entry, but not the IV/GEX/OI
    legs. So the result is "opp-exit vs lock ON REVERSAL-TYPE ENTRIES", not on the
    whole engine. If it looks good, the definitive test is adding the opp-exit to the
    Rust engine (which reproduces real entries) and backtesting there.
  • N = 4 days (22–25 Jun; 19-Jun has no selling file = no spot). DIRECTIONAL, not
    validated. Every threshold below is a-priori from the live signal text, not tuned.
  • Fills use the bid/ask MID + the src cost model; the bid/ask spread itself is not
    charged (mid-to-mid), so both arms are equally, mildly optimistic. Swap to
    ask-in/bid-out if spread realism ever matters.
"""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

DATA = Path(__file__).resolve().parent.parent / "data"
LOT = 65

# ── entry trigger (a-priori from the live "30 closed bars / ~0.2% reclaim" signal) ──
W = 30              # rolling window, minutes (= "30 closed bars")
MOVE = 0.0015       # reclaim/breakdown ≥ 0.15% off the window extreme (live showed 0.18–0.21%)
NEAR = 0.0007       # the extreme must have been TOUCHED within RECLAIM_LB bars (a reversal, not stale)
RECLAIM_LB = 5      # bars back to look for the touched extreme
COOLDOWN = 10       # minutes between same-side triggers (de-bounce)
ENTRY_AFTER = "09:45"   # need W bars of warmup before the first trigger can arm
TIME_EXIT = "15:15"     # square off — single-leg never carries

# ── management (matches src/options_engine.rs) ──
LOCK_TRIGGER = 0.12     # +12% gain → arm the one-shot lock
LOCK_STOP    = 1.02     # ...stop = entry +2%
MIN_HOLD_MIN = 3        # lock only after ≥3 min hold
TARGET       = 1.25     # +25% take-profit
HARD_STOP    = 0.65     # -35% stop
# Above the +15% floor, give back at most 50% of the gain-above-15% before closing (an
# option-level trail that sits ON TOP of the +2% lock: the +2% cap still governs +12–15%,
# this governs +15%+). stop_gain = FLOOR + GIVEBACK*(peak_gain - FLOOR).
TRAIL_FLOOR    = 0.15
TRAIL_GIVEBACK = 0.50

# cost model ported from src (per leg: ₹20+GST flat, exch/sebi/gst, STT sell, stamp buy)
def leg_cost(price, qty, side):
    prem = price * qty
    brokerage = 20.0 * 1.18
    exch = 0.000311 * prem
    sebi = 0.000001 * prem
    gst = 0.18 * (exch + sebi)
    stt = 0.001 * prem if side == "SELL" else 0.0
    stamp = 0.00003 * prem if side == "BUY" else 0.0
    return brokerage + exch + sebi + gst + stt + stamp

def roundtrip_cost(entry_px, exit_px):
    return leg_cost(entry_px, LOT, "BUY") + leg_cost(exit_px, LOT, "SELL")

# ── data ──
def load(path):
    cols = pd.read_csv(path, nrows=0).columns.tolist()
    rich = "spot" in cols and "mid" in cols              # selling file: real spot + bid/ask mid
    use = ["recv_ts", "strike", "option_type"] + (["spot", "mid"] if rich else ["ltp"])
    df = pd.read_csv(path, usecols=use)                  # read_csv auto-decompresses *.gz
    df["recv_ts"] = pd.to_datetime(df["recv_ts"])
    df["px"] = df["mid"] if rich else df["ltp"]          # buyer fill proxy
    df.attrs["rich"] = rich
    return df

def parity_spot(df):
    """Basic options file has no spot — reconstruct via put-call parity at the most-liquid
    (≈ATM) strike: spot ≈ K + CE - PE, ignoring the small rT discount at 5 DTE."""
    both = df.groupby("strike")["option_type"].nunique()
    cand = list(both[both >= 2].index)
    K = df[df.strike.isin(cand)].groupby("strike").size().idxmax()
    leg = lambda ot: (df[(df.strike == K) & (df.option_type == ot)]
                      .set_index("recv_ts")["px"].sort_index().resample("1min").last().ffill())
    return (K + leg("CE") - leg("PE")).dropna()

def spot_bars(df):
    if df.attrs.get("rich"):
        s = (df.dropna(subset=["spot"]).drop_duplicates("recv_ts")
               .set_index("recv_ts")["spot"].sort_index().resample("1min").last())
        return s.dropna()
    return parity_spot(df)

def triggers(bars):
    """Ordered list of (timestamp, 'CE'|'PE') reversal signals off the W-bar extremes."""
    lo, hi = bars.rolling(W).min(), bars.rolling(W).max()
    out, last = [], {"CE": None, "PE": None}
    vals = bars.values; idx = bars.index
    for i in range(len(bars)):
        t, px = idx[i], vals[i]
        if i < W or np.isnan(lo.iloc[i]):
            continue
        recent = vals[max(0, i - RECLAIM_LB):i + 1]
        side = None
        # reclaim off the low (was just at the low, now ≥MOVE above it) → bullish → CE
        if px >= lo.iloc[i] * (1 + MOVE) and recent.min() <= lo.iloc[i] * (1 + NEAR):
            side = "CE"
        # breakdown off the high (was just at the high, now ≥MOVE below it) → bearish → PE
        elif px <= hi.iloc[i] * (1 - MOVE) and recent.max() >= hi.iloc[i] * (1 - NEAR):
            side = "PE"
        if side and (last[side] is None or (t - last[side]).total_seconds() >= COOLDOWN * 60):
            out.append((t, side)); last[side] = t
    return out

def opt_series(df, strike, otype):
    o = df[(df.strike == strike) & (df.option_type == otype)]
    return o.set_index("recv_ts")["px"].dropna().sort_index()

def default_files():
    """One file per day: prefer the rich selling file, else the basic options file
    (19-Jun predates the selling recorder → parity-reconstructed spot + ltp)."""
    by_day = {}
    for p in sorted(DATA.glob("*_ticks.csv*")):
        day = p.name.split("_")[0]
        if "_option_selling_ticks." in p.name:
            by_day[day] = p
        elif "_options_ticks." in p.name:
            by_day.setdefault(day, p)
    return [by_day[d] for d in sorted(by_day)]

# ── one managed trade under a chosen exit policy ──
def manage(opt, entry_t, entry_px, opp_times, day, use_opp=False, trail15=False):
    locked = trail_armed = False
    peak = entry_px
    next_opp = next((tt for tt in opp_times if tt > entry_t), None)
    hard_exit = pd.Timestamp(f"{day} {TIME_EXIT}")
    seg = opt[(opt.index >= entry_t) & (opt.index <= hard_exit)]
    for t, px in seg.items():
        peak = max(peak, px)
        gain, peak_gain = px / entry_px - 1.0, peak / entry_px - 1.0
        held = (t - entry_t).total_seconds() / 60.0
        if not locked and gain >= LOCK_TRIGGER and held >= MIN_HOLD_MIN:
            locked = True
        if gain >= TRAIL_FLOOR:
            trail_armed = True
        if px >= entry_px * TARGET:
            return t, px, "target"
        if px <= entry_px * HARD_STOP:
            return t, px, "stop"
        if trail15 and trail_armed and gain < peak_gain:  # +15%+ : half-give-back trail
            stop_gain = TRAIL_FLOOR + TRAIL_GIVEBACK * (peak_gain - TRAIL_FLOOR)
            if gain <= stop_gain:                          # ...only on a pullback from the peak,
                return t, px, "trail"                      #    not on the tick that first hits +15%
        elif locked and px <= entry_px * LOCK_STOP:        # +12–15% : the +2% cap still governs
            return t, px, "lock"
        if use_opp and next_opp is not None and t >= next_opp:
            return t, px, "opp"          # opposite signal fired → close
    if len(seg):
        return seg.index[-1], seg.iloc[-1], "time"
    return entry_t, entry_px, "no-ticks"

def run_day(path):
    day = path.name.split("_")[0]
    df = load(path)
    bars = spot_bars(df)
    assert 18000 < bars.median() < 32000, f"{day}: spot out of range ({bars.median()})"
    trigs = triggers(bars)
    after = pd.Timestamp(f"{day} {ENTRY_AFTER}")
    entry = next((tt for tt in trigs if tt[0] >= after), None)
    if entry is None:
        return {"day": day, "traded": False, "trigs": len(trigs)}
    et, side = entry
    K = round(float(bars.asof(et)) / 50) * 50
    opt = opt_series(df, K, side)
    if opt.empty:                                  # ATM strike not in the chain → nearest recorded
        avail = sorted(df[df.option_type == side].strike.unique(), key=lambda k: abs(k - K))
        K = avail[0]; opt = opt_series(df, K, side)
    epx = float(opt.asof(et))
    opp_times = [tt for tt, s in trigs if s != side]
    res = {"day": day, "traded": True, "trigs": len(trigs), "side": side, "strike": K,
           "entry_t": et, "entry_px": epx, "opp": opp_times}
    for arm, kw in (("A", {}), ("C", {"trail15": True})):
        xt, xpx, why = manage(opt, et, epx, opp_times, day, **kw)
        net = (xpx - epx) * LOT - roundtrip_cost(epx, xpx)
        res[arm] = {"xt": xt, "xpx": xpx, "why": why, "gain": xpx / epx - 1.0, "net": net}
    return res

# The MOTIVATING trade: the 25-Jun live single-leg (CE 24300 entered 11:59:23, peaked
# ~+17%, round-tripped to the +2% lock at 12:22). The whole question is "would opp-exit
# have done better HERE?" — so we test it directly on the real entry, not just signals.
LIVE = {"file": "2026-06-25_option_selling_ticks", "day": "2026-06-25",
        "t": "2026-06-25 11:59:23", "strike": 24300, "side": "CE"}

def anchored_live(files):
    p = next((f for f in files if LIVE["file"] in f.name), None)
    if p is None:
        return
    df = load(p); trg = triggers(spot_bars(df))
    et = pd.Timestamp(LIVE["t"]); opt = opt_series(df, LIVE["strike"], LIVE["side"])
    if opt.empty:
        return
    epx = float(opt.asof(et))
    opp = [t for t, s in trg if s != LIVE["side"]]
    seg = opt[(opt.index >= et) & (opt.index <= pd.Timestamp(f"{LIVE['day']} {TIME_EXIT}"))]
    print("\n MOTIVATING TRADE — anchored to the live 25-Jun entry "
          f"({LIVE['side']} {LIVE['strike']} @ {et.strftime('%H:%M')}):")
    print(f"   option peaked {(seg.max()/epx-1)*100:+.0f}% at {seg.idxmax().strftime('%H:%M')}")
    for arm, kw in (("A +2% lock", {}), ("C +15% trail", {"trail15": True})):
        xt, xpx, why = manage(opt, et, epx, opp, LIVE["day"], **kw)
        net = (xpx - epx) * LOT - roundtrip_cost(epx, xpx)
        print(f"   {arm:13} -> exit {xpx:.1f} @ {xt.strftime('%H:%M')} [{why}] "
              f"{(xpx/epx-1)*100:+.0f}%  net ₹{net:+.0f}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="*", help="selling csv(.gz); default = all in data/")
    a = ap.parse_args()
    files = [Path(f) for f in a.files] or default_files()
    if not files:
        print("no *_option_selling_ticks.csv* in", DATA); return
    rows = [run_day(p) for p in files]

    print("\n SINGLE-LEG GUARDRAIL — PAST (A: +2% lock)  vs  CURRENT (C: +2% lock + half-give-back trail >+15%)")
    print(" entry/exit shown as TIME@price;  19-Jun priced off ltp+parity-spot (no selling file), 22–25 off bid/ask mid")
    print(" " + "-" * 104)
    print(f" {'day':10} {'side/stk':9} {'entry':12} | {'A: exit':17} {'A ₹':>7} | {'C: exit':17} {'C ₹':>7}")
    print(" " + "-" * 104)
    ta = tc = 0.0
    for r in rows:
        if not r.get("traded"):
            print(f" {r['day']:10} (no trigger after {ENTRY_AFTER}; {r['trigs']} triggers)")
            continue
        A, C = r["A"], r["C"]; ta += A["net"]; tc += C["net"]
        es  = f"{r['side']} {r['strike']:.0f}"
        ein = f"{r['entry_t'].strftime('%H:%M')}@{r['entry_px']:.1f}"
        ax  = f"{A['xt'].strftime('%H:%M')}@{A['xpx']:.1f} {A['why']}"
        cx  = f"{C['xt'].strftime('%H:%M')}@{C['xpx']:.1f} {C['why']}"
        print(f" {r['day']:10} {es:9} {ein:12} | {ax:17} {A['net']:>+7.0f} | {cx:17} {C['net']:>+7.0f}")
    print(" " + "-" * 104)
    print(f" {'TOTAL':10} {'':9} {'':12} | {'':17} {ta:>+7.0f} | {'':17} {tc:>+7.0f}   Δ(C-A) ₹{tc-ta:+.0f}")

    print("\n where the +15% trail changed the exit (vs the +2% lock alone):")
    for r in rows:
        if not r.get("traded"):
            continue
        A, C = r["A"], r["C"]
        if abs(C["net"] - A["net"]) < 1e-6:
            print(f"  {r['day']}: identical — never armed/triggered the trail (exited '{C['why']}')")
        else:
            print(f"  {r['day']}: A '{A['why']}' {A['gain']*100:+.0f}% (₹{A['net']:+.0f})  →  "
                  f"C '{C['why']}' {C['gain']*100:+.0f}% (₹{C['net']:+.0f})   Δ ₹{C['net']-A['net']:+.0f}")

    anchored_live(files)

if __name__ == "__main__":
    main()
