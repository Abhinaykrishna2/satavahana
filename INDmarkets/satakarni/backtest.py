#!/usr/bin/env python3
"""satakarni — INTRADAY option-SELLING sandbox (₹150k), theta harvest with compounding.

Intraday only: every position opens AND closes the same session (overnight holds carry
news/gap risk that breaks the Markov assumption — we never carry). Selling-only here;
option *buying* is handled in INDmarkets/src with the existing single-leg engine.

Two defined-risk premium structures, both sideways-gated and intraday-compounded:

  CONDOR  short ~0.25Δ CE+PE, long 100pt wings. Wide profit zone, HIGH win-prob,
          but small max-profit vs max-loss (poor payoff — risk a lot to make a little).
  FLY     short ATM straddle, long 100pt wings. Collects ~ATM premium → max-profit ≫
          max-loss (great payoff) but a NARROW profit zone (low win-prob, high gamma).

COMPOUNDING: within the day we re-sell after each take-profit, sizing the next cycle off
the *grown* capital. Halt the day after a stop-loss (no revenge selling).

METHODOLOGY (non-negotiable — 1 selling day of data): every parameter is fixed A-PRIORI
from convention; the backtest runs ONCE and is reported straight, losses included. One
day is a unit test of the machinery, NOT evidence of edge or of real compounding.

HONESTY: defined-risk "max loss" is an EXPIRY bound. Intraday, a sharp move marks worse
than that bound while time value remains — so a multi-lot fly's true intraday risk is
LARGER than the stated margin. A calm day (this one) flatters premium selling; a gap day
is what empties the account. That asymmetry is the whole game.
"""
import argparse
from pathlib import Path
import numpy as np
import pandas as pd

DATA = Path(__file__).resolve().parent.parent / "data"   # INDmarkets/data
CAPITAL = 150_000.0
LOT = 65
WING = 100             # wing width (pts) — the only width that fits the recorded ±5-strike chain
SIDEWAYS_PCT = 0.0040  # morning 09:16-09:45 range <= 0.40% of spot => sideways => sell premium
RISK_FRAC = 0.10       # size each cycle to risk <= 10% of *current* capital as expiry-max-loss
MAX_LOTS = 5           # hard cap — bounds intraday gamma (expiry-max-loss understates intraday risk)
ENTRY = "09:45"        # first entry, after the open warmup
CUTOFF = "14:30"       # no NEW cycles after this; existing managed to EXIT
EXIT = "15:15"         # square off — never carry overnight
REENTRY_GAP_MIN = 2
MAX_CYCLES_PER_DAY = 1  # one trade/day — on both days re-entry only ever gave back gains (over-trading)
# Management chosen A-PRIORI from option-selling convention, NOT optimized to the 2 days we have:
TARGET_FRAC = 0.50     # book at 50% of credit — the industry-standard "manage at 50%" rule
STOP_FRAC_ML = 0.50    # cut at 50% of DEFINED MAX LOSS — a principled tail cap that actually binds
                       # (a 2× credit stop never fires when 2×credit > max loss, so trades rode to near-max).
MOVE_PCT = 0.0025      # SUDDEN-MOVE trim: short premium is short gamma, so a fast realized move in the
MOVE_WINDOW = 5        # underlying IS the risk — exit when |spot move| over MOVE_WINDOW min exceeds this.
ENTRY_DRIFT_ZONE_CAP = 0.50  # no entry if the opening range has already consumed >50% of the
                             # structure's credit-adjusted profit zone.
MOVE_ZONE_CAP = 0.35         # in-trade trim when a 5m move consumes >35% of that same zone.
# (A dynamic no-arm trailing was tried and REMOVED: with no minimum it fired on micro-noise and
#  churned the bid/ask spread 20-30×/day — a net loss. Demonstrates why exit-tuning on N=2 backfires.)
SLIP = 0.003           # adverse slippage if ever priced off a basic (no-book) file

# Sideways premium sellers — all DEFINED-RISK: name -> (short-strike target Δ or "ATM", wing width pts).
# They differ in credit, profit-zone width, and risk geometry. (Naked selling intentionally excluded.)
STRUCTURES = {
    "condor":   (0.25, WING),       # OTM strangle + wings — WIDE zone, low credit, high win-prob
    "tight":    (0.33, WING),       # shorts near ATM — narrower zone, more credit (aggressive condor)
    "fly":      ("ATM", WING),      # ATM straddle + wings — narrow zone, big credit, high gamma
    "widefly":  ("ATM", 2 * WING),  # ATM straddle + 200pt wings — keeps more credit, larger max-loss
}

# ---------------------------------------------------------------- cost model (ported from src)
def leg_cost(price, qty, side):
    prem = price * qty
    brokerage = 20.0 * 1.18                              # flat ₹20/order + GST (per leg, not per lot)
    exch = 0.000311 * prem
    sebi = 0.000001 * prem
    gst = 0.18 * (exch + sebi)
    stt = 0.001 * prem if side == "SELL" else 0.0        # 0.1% options-sell STT on premium
    stamp = 0.00003 * prem if side == "BUY" else 0.0
    return brokerage + exch + sebi + gst + stt + stamp

# ---------------------------------------------------------------- data
def load(path):
    # Selling needs the RICH feed (bid/ask + greeks); there is no basic-file fallback — a
    # short-premium model can't be priced from LTP alone. Fail clearly, not cryptically later.
    cols = pd.read_csv(path, nrows=0).columns.tolist()
    if "bid" not in cols or "delta_mid" not in cols:
        raise SystemExit(f"{path.name}: this selling sandbox needs '_option_selling_ticks.csv' "
                         f"(bid/ask + greeks); a basic options_ticks.csv has no fallback here.")
    use = ["recv_ts", "expiry", "strike", "option_type", "oi", "spot", "bid", "ask", "mid", "delta_mid"]
    df = pd.read_csv(path, usecols=use)
    df["recv_ts"] = pd.to_datetime(df["recv_ts"])
    df["px"] = df["mid"]
    df.attrs["rich"] = True
    return df

def ts(day, hhmm):
    return pd.Timestamp(f"{day} {hhmm}:00")

def snapshot(df, t):
    """As-of snapshot: last tick per (strike,option_type) at-or-before t. No look-ahead."""
    s = df[df.recv_ts <= t].sort_values("recv_ts").drop_duplicates(["strike", "option_type"], keep="last")
    return s.set_index(["strike", "option_type"])

def spot_of(df, snap):
    if df.attrs["rich"]:
        sp = snap["spot"].replace(0, np.nan).dropna()
        if len(sp):
            return float(sp.median())
    best, gap = None, 1e9                                # put-call parity at ATM
    for K in snap.index.get_level_values(0).unique():
        if (K, "CE") in snap.index and (K, "PE") in snap.index:
            ce, pe = float(snap.loc[(K, "CE"), "px"]), float(snap.loc[(K, "PE"), "px"])
            if ce > 0 and pe > 0 and abs(ce - pe) < gap:
                gap, best = abs(ce - pe), K + ce - pe
    return best

def morning_range_pct(df, day):
    """Realized spot range over [09:16, ENTRY] / spot. Small => sideways."""
    t_open, t0 = ts(day, "09:16"), ts(day, ENTRY)
    if df.attrs["rich"]:
        sp = df[(df.recv_ts >= t_open) & (df.recv_ts <= t0)]["spot"].replace(0, np.nan).dropna()
        return (sp.max() - sp.min()) / sp.median() if len(sp) >= 2 else None
    spots = [spot_of(df, snapshot(df, ts(day, f"09:{m:02d}"))) for m in range(20, 46, 5)]
    spots = np.array([s for s in spots if s])
    return (spots.max() - spots.min()) / np.median(spots) if len(spots) >= 2 else None

def fill(rich, row, side):
    if rich:
        return float(row["ask"]) if side == "BUY" else float(row["bid"])
    px = float(row["px"])
    return px * (1 + SLIP) if side == "BUY" else px * (1 - SLIP)

def pnl(df, snap_e, snap_x, legs, lots=1):
    """Realized net P&L for legs=[(strike,otype,pos±1)]. qty=LOT*lots scales premium costs;
    brokerage stays flat per leg-order."""
    rich = df.attrs["rich"]
    qty = LOT * lots
    gross_unit, costs = 0.0, 0.0
    for K, ot, pos in legs:
        if (K, ot) not in snap_e.index or (K, ot) not in snap_x.index:
            return None
        ef = fill(rich, snap_e.loc[(K, ot)], "BUY" if pos > 0 else "SELL")
        xf = fill(rich, snap_x.loc[(K, ot)], "SELL" if pos > 0 else "BUY")
        gross_unit += pos * (xf - ef)
        costs += leg_cost(ef, qty, "BUY" if pos > 0 else "SELL")
        costs += leg_cost(xf, qty, "SELL" if pos > 0 else "BUY")
    gross = gross_unit * qty
    return gross - costs, gross, costs

# ---------------------------------------------------------------- structures
def build_legs(snap, strikes, spot, kind):
    """Return (legs, wing) for STRUCTURES[kind]. legs = [(strike, otype, pos±1)].
    Returns (None, None) if the chain can't seat the wings."""
    tgt, wing = STRUCTURES[kind]
    kmin, kmax = min(strikes), max(strikes)
    w = wing

    def pick(ot):
        if tgt == "ATM":
            # ATM fly uses one strike for both shorts → needs BOTH wings in range. Filter for
            # room first, THEN take nearest to spot, so a near-edge fly isn't dropped. (Matches
            # Rust multileg::select_legs.)
            room = [K for K in strikes if K + w <= kmax and K - w >= kmin]
            return min(room, key=lambda K: abs(K - spot)) if room else None
        cand = [(K, abs(float(snap.loc[(K, ot), "delta_mid"]))) for K in strikes if (K, ot) in snap.index]
        room = (lambda K: K + w <= kmax) if ot == "CE" else (lambda K: K - w >= kmin)
        cand = [(K, d) for K, d in cand if abs(d - tgt) <= 0.12 and room(K)]
        return min(cand, key=lambda kd: abs(kd[1] - tgt))[0] if cand else None

    ce_s, pe_s = pick("CE"), pick("PE")
    if ce_s is None or pe_s is None:
        return None, None
    if ce_s + wing > kmax or pe_s - wing < kmin:
        return None, None
    return [(ce_s, "CE", -1), (ce_s + wing, "CE", +1), (pe_s, "PE", -1), (pe_s - wing, "PE", +1)], wing

def profit_zone(legs, credit):
    """Credit-adjusted expiry profit zone for the short strikes.

    For a condor/tight condor this is [short_put - credit, short_call + credit].
    For a fly/widefly both shorts are ATM, so it becomes ATM ± credit.
    """
    shorts = [K for K, _ot, pos in legs if pos < 0]
    if not shorts or credit <= 0:
        return None
    lower = min(shorts) - credit
    upper = max(shorts) + credit
    width = upper - lower
    return (lower, upper, width) if width > 0 else None

def opening_drift_latent(df, day, t_entry, legs, credit):
    """Opening-range pressure as a fraction of the structure's own profit zone."""
    zone = profit_zone(legs, credit)
    if zone is None:
        return None
    _lower, _upper, width = zone
    sp = df[(df.recv_ts >= ts(day, "09:16")) & (df.recv_ts <= t_entry)]["spot"].replace(0, np.nan).dropna()
    if len(sp) < 2:
        return None
    range_pts = float(sp.max() - sp.min())
    return {
        "range_pts": range_pts,
        "zone_width": width,
        "zone_frac": range_pts / width,
    }

def manage(df, t0, tN, legs, credit, maxloss_u, zone_width):
    """Mark-to-mid early-exit management (a-priori, not data-fit). First to fire:
      TARGET : book at TARGET_FRAC of credit.
      STOP   : cut at STOP_FRAC_ML of the DEFINED MAX LOSS (binds, unlike a 2× credit stop).
      MOVE   : trim on a fast underlying move; measured both as spot % and as profit-zone usage.
    15:15 is only a backstop. Thresholds are per-unit ratios (scale-invariant in lots).
    Returns (t_exit, why)."""
    idx = pd.date_range(t0, tN, freq="1min")
    neg_prem = pd.Series(0.0, index=idx)
    for K, ot, pos in legs:
        s = df[(df.strike == K) & (df.option_type == ot)].set_index("recv_ts")["mid"]
        neg_prem = neg_prem.add(pos * s.resample("1min").last().ffill().reindex(idx, method="ffill"),
                                fill_value=0.0)
    gain = neg_prem - (-credit)                          # per-unit running gain (+ as premium decays)
    # Sudden-move detector on the UNDERLYING: |spot move| over MOVE_WINDOW min. A short-gamma
    # position can't tolerate a fast move, so trim/exit on it — at whatever the mark then is.
    spot_s = (df.set_index("recv_ts")["spot"].replace(0, np.nan)
              .resample("1min").last().ffill().reindex(idx, method="ffill"))
    move_pts = (spot_s - spot_s.shift(MOVE_WINDOW)).abs()
    move_spot = move_pts / spot_s
    move_zone = move_pts / zone_width if zone_width > 0 else move_pts * 0.0
    for t, g in gain.items():
        if t <= t0:
            continue
        if move_spot.get(t, 0.0) > MOVE_PCT or move_zone.get(t, 0.0) > MOVE_ZONE_CAP:
            return t, (
                f"MOVE {move_spot.get(t, 0.0)*100:.2f}%/{MOVE_WINDOW}m "
                f"zone {move_zone.get(t, 0.0)*100:.0f}%"
            )
        if g >= TARGET_FRAC * credit:
            return t, f"TARGET {int(TARGET_FRAC*100)}%"
        if g <= -STOP_FRAC_ML * maxloss_u:
            return t, f"STOP {int(STOP_FRAC_ML*100)}%ML"
    return tN, "15:15"

def run_cycle(df, day, t_entry, kind, cap):
    """One sell cycle from t_entry. Sizes off `cap` (compounding). Returns dict or None."""
    snap = snapshot(df, t_entry)
    spot = spot_of(df, snap)
    if spot is None:
        return None, "missing spot"
    strikes = sorted(snap.index.get_level_values(0).unique())
    legs, wing = build_legs(snap, strikes, spot, kind)
    if legs is None:
        return None, "no valid legs"
    credit = -sum(pos * fill(True, snap.loc[(K, ot)], "BUY" if pos > 0 else "SELL") for K, ot, pos in legs)
    maxloss_u = wing - credit                            # defined risk: margin ≈ max loss
    if credit <= 0 or maxloss_u <= 0:
        return None, "invalid credit/max-loss"
    latent = opening_drift_latent(df, day, t_entry, legs, credit)
    if latent is None:
        return None, "insufficient drift-latent data"
    if latent["zone_frac"] > ENTRY_DRIFT_ZONE_CAP:
        return None, (
            f"DRIFT-ZONE {latent['zone_frac']*100:.0f}% > {ENTRY_DRIFT_ZONE_CAP*100:.0f}% "
            f"(range {latent['range_pts']:.0f} / zone {latent['zone_width']:.0f})"
        )
    lots = min(MAX_LOTS, int((cap * RISK_FRAC) // (maxloss_u * LOT)))
    if lots == 0:
        return None, "risk budget can't fund one lot"  # never force a lot; matches Rust size_lots
    margin = maxloss_u * LOT * lots
    t_exit, why = manage(df, t_entry, ts(day, EXIT), legs, credit, maxloss_u, latent["zone_width"])
    res = pnl(df, snapshot(df, t_entry), snapshot(df, t_exit), legs, lots)
    if res is None:
        return None, "missing entry/exit marks"
    net, gross, costs = res
    return dict(t0=t_entry, t1=t_exit, why=why, lots=lots, credit=credit, margin=margin,
                net=net, gross=gross, costs=costs, spot=spot, drift_zone=latent["zone_frac"]), None

def sell_intraday(df, day, kind, start_cap):
    """One day of intraday-compounded selling, sized off `start_cap`. Returns a result dict."""
    rng = morning_range_pct(df, day)
    base = dict(day=day, kind=kind, start_cap=start_cap, end_cap=start_cap, day_pnl=0.0, traded=False, rng=rng)
    if rng is None:
        return {**base, "reason": "insufficient morning data"}
    if rng > SIDEWAYS_PCT:
        return {**base, "reason": f"TRENDING {rng*100:.2f}% > {SIDEWAYS_PCT*100:.2f}% — stand aside"}
    cap, t, cyc, last_reason = start_cap, ts(day, ENTRY), [], None
    while t < ts(day, CUTOFF):
        c, reason = run_cycle(df, day, t, kind, cap)
        if c is None:
            last_reason = reason
            break
        cap += c["net"]
        c["cap"] = cap
        cyc.append(c)
        if len(cyc) >= MAX_CYCLES_PER_DAY:
            break                                        # bank the day's setup; don't over-trade
        if "STOP" in c["why"]:
            break                                        # halt after a stop-loss (no revenge selling)
        t = c["t1"] + pd.Timedelta(minutes=REENTRY_GAP_MIN)
    if not cyc:
        return {**base, "reason": last_reason or "no valid cycle"}
    return {**base, "traded": True, "cycles": cyc, "end_cap": cap, "day_pnl": cap - start_cap,
            "n_cycles": len(cyc), "peak_risk": max(c["margin"] for c in cyc), "spot": cyc[0]["spot"]}

def format_day(r):
    if not r["traded"]:
        return f"  {r['day']}: {r.get('reason', '-')}"
    head = (f"  {r['day']}  SIDEWAYS {r['rng']*100:.2f}%  spot≈{r['spot']:.0f}  {r['n_cycles']} cyc  "
            f"day ₹{r['day_pnl']:+,.0f}  cap ₹{r['end_cap']:,.0f}")
    rows = [f"     {i}. {c['t0'].strftime('%H:%M')}->{c['t1'].strftime('%H:%M')} x{c['lots']}lot "
            f"cr{c['credit']:.0f}/u maxloss ₹{c['margin']:,.0f} "
            f"driftZ {c['drift_zone']*100:.0f}% [{c['why']}] net ₹{c['net']:+,.0f}"
            for i, c in enumerate(r["cycles"], 1)]
    return "\n".join([head, *rows])

def metrics(results, start_cap):
    """Aggregate the numbers that actually decide 'reliable': win-rate, expectancy, max drawdown."""
    traded = [r for r in results if r["traded"]]
    if not traded:
        return "    no trading days (all gated out / no data)"
    pnls = [r["day_pnl"] for r in traded]
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    end_cap = results[-1]["end_cap"]
    peak, maxdd = start_cap, 0.0
    for r in results:                                    # equity curve carries cap through no-trade days
        peak = max(peak, r["end_cap"])
        maxdd = max(maxdd, (peak - r["end_cap"]) / peak)
    total = end_cap - start_cap
    return (f"    days {len(traded)}/{len(results)} traded | win-rate {wins/len(traded)*100:.0f}% "
            f"({wins}W/{losses}L) | expectancy ₹{total/len(traded):+,.0f}/day | "
            f"total ₹{total:+,.0f} ({total/start_cap*100:+.2f}%) | max DD {maxdd*100:.1f}% | "
            f"final ₹{end_cap:,.0f}")

# ---------------------------------------------------------------- self-test
def _selftest():
    rows, S = [], 24100
    for t in ("2026-06-22 09:45:00", "2026-06-22 15:15:00"):
        for K in (24000, 24100, 24200):                 # NIFTY-scale, 100pt spacing (matches WING)
            extr = max(3.0 - 0.02 * abs(K - S), 0.5)
            for ot, mid in (("CE", max(S - K, 0) + extr), ("PE", max(K - S, 0) + extr)):
                rows.append(dict(recv_ts=t, expiry="2026-06-23", strike=K, option_type=ot, oi=1000,
                                 spot=S, bid=mid - 0.5, ask=mid + 0.5, mid=mid,
                                 delta_mid=0.25 if ot == "CE" else -0.25))
    df = pd.DataFrame(rows); df["recv_ts"] = pd.to_datetime(df["recv_ts"]); df["px"] = df["mid"]; df.attrs["rich"] = True
    snap = snapshot(df, ts("2026-06-22", "09:45"))
    assert abs(spot_of(df, snap) - S) < 1e-6, "parity spot"
    assert fill(True, snap.loc[(S, "CE")], "BUY") > fill(True, snap.loc[(S, "CE")], "SELL"), "adverse fills"
    fly, w = build_legs(snap, [24000, 24100, 24200], S, "fly")
    assert w == 100 and fly == [(24100, "CE", -1), (24200, "CE", 1), (24100, "PE", -1), (24000, "PE", 1)], "fly legs"
    credit = -sum(pos * fill(True, snap.loc[(K, ot)], "BUY" if pos > 0 else "SELL") for K, ot, pos in fly)
    assert credit > 0, f"fly credit positive, got {credit}"
    legs = [(24100, "CE", -1), (24200, "CE", 1)]
    r1 = pnl(df, snap, snapshot(df, ts("2026-06-22", "15:15")), legs, 1)
    r3 = pnl(df, snap, snapshot(df, ts("2026-06-22", "15:15")), legs, 3)
    assert r1[1] < 0, "flat market loses the crossed spread"
    assert abs(r3[1] - 3 * r1[1]) < 1e-6, "gross scales with lots"
    assert leg_cost(50, LOT, "SELL") > leg_cost(50, LOT, "BUY"), "sell costs more (STT)"
    print("selftest OK")

# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    if ap.parse_args().selftest:
        _selftest(); return

    days = sorted((f.name.split("_")[0], f) for f in DATA.glob("*_option_selling_ticks.csv"))
    if not days:
        print("No *_option_selling_ticks.csv in", DATA); return
    print(f"satakarni INTRADAY selling | start ₹{CAPITAL:,.0f} | {len(days)} day(s) | a-priori params, run once")
    print("Account compounds across days; each day compounds intraday; squared by 15:15 (never carry overnight).\n")
    for kind in STRUCTURES:
        print(f"=== {kind.upper()} ===")
        cap, results = CAPITAL, []
        for day, f in days:
            r = sell_intraday(load(f), day, kind, cap)
            cap = r["end_cap"]
            results.append(r)
            print(format_day(r))
        print(metrics(results, CAPITAL), "\n")
    n = len(days)
    print(f"REALITY CHECK: {n} day(s) of data. Win-rate/expectancy/maxDD above are placeholders until you have\n"
          f"~30+ days INCLUDING expiry days (0-DTE, where intraday theta is real) and gap days (where the tail\n"
          f"shows up). Compounding a positive expectancy is how you win; one calm Tuesday proves none of it.")

if __name__ == "__main__":
    main()
