#!/usr/bin/env python3
"""
select_strategy.py — Adaptive strategy selector + parameter fine-tuner.

Runs BEFORE bot.py.  Reads cached OHLCV data (zero network calls), computes
15 continuous market signals, detects the active scenario, selects the best
base profile, then fine-tunes every parameter for today's exact conditions.

Research basis: Daniel & Moskowitz (2014 NBER), Moskowitz/Ooi/Pedersen (2012 JFE),
George & Hwang (2004 Bauer UH), NSE Multi-Factor Index whitepaper,
QuantifiedStrategies backtests, Wyckoff analytics, AQR momentum research.

────────────────────────────────────────────────────────────────────────────
SIGNALS COMPUTED (15 total)
────────────────────────────────────────────────────────────────────────────
  bull_ratio            % of large-cap proxies above 50-SMA
  avg_rsi               Average RSI(14)
  volatility_pct        Average 14-day ATR as % of price
  momentum_5d           Average 5-day % return
  vol_ratio             Average (5d vol) / (20d vol)
  trend_consistency     Fraction of recent days confirming trend direction
  stretch_from_sma20    Avg distance from 20-SMA in ATR units
  down_vol_surge        Fraction of proxies with climactic selling volume
  price_vol_divergence  Correlation of price direction vs volume direction
  sector_breadth_spread Std of per-sector bull_ratios (rotation signal)
  regime_velocity       Change in bull_ratio over last 5 days
  ── NEW ──
  hi52_proximity        Fraction of proxies within 5% of 52-week high
  choppiness_index      Avg Choppiness Index (<38=trending, >62=choppy)
  obv_divergence        OBV trend vs price trend alignment (-1 to +1)
  adx_avg               Average ADX (<20=no trend, >25=trending)
  ── DERIVED FLAG ──
  momentum_crash_risk   True when 1-month bounce >8% follows >20% drawdown

────────────────────────────────────────────────────────────────────────────
SCENARIOS DETECTED
────────────────────────────────────────────────────────────────────────────
  CAPITULATION       Panic selling bottom — most aggressive bypass entry
  OVERBOUGHT_BULL    Bull exhausted — demand pullback, tighten exits
  BEAR_RALLY         Low-volume bounce or momentum_crash_risk — stay out
  DISTRIBUTION       OBV declining while price holds — smart money exiting
  CHOPPY             CI>62 + ADX<22 — range market, mean-reversion only
  MOMENTUM_ACCEL     52wk highs + consistency + volume — max aggression
  (none)             Standard regime-interpolated parameters

────────────────────────────────────────────────────────────────────────────
FINE-TUNING: 3-STEP ENGINE
────────────────────────────────────────────────────────────────────────────
  Step 1  Base interpolation across regime_pos (0=crash → 3=bull)
  Step 2  10 signal overlays: volatility, momentum, volume, consistency,
          stretch, sector rotation, 52wk proximity, OBV divergence,
          momentum crash risk, extreme chop (CI+ADX)
  Step 3  Scenario overrides replace Step 1+2 output
          Multiple scenarios → most conservative value per-parameter

────────────────────────────────────────────────────────────────────────────
USAGE
────────────────────────────────────────────────────────────────────────────
  python select_strategy.py              # auto-detect + fine-tune + write .env
  python select_strategy.py --dry-run    # print params, don't write
  python select_strategy.py --no-tune    # profile only, skip fine-tuning
  python select_strategy.py --profile bear-fighter   # force base profile

.env controls:
  STRATEGY_AUTO_SELECT=true
  STRATEGY_PROFILE_OVERRIDE=bear-fighter   # skip detection, use this base
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
BOT_DIR = Path(__file__).parent

PROTECTED_KEYS = {
    "GROWW_API_KEY", "GROWW_SECRET", "SNS_TOPIC_ARN",
    "BOT_DRY_RUN", "RISK_DRY_RUN_BALANCE",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION",
    "PAPER_LEDGER_PATH", "FETCHER_CACHE_ONLY",
    "STRATEGY_AUTO_SELECT", "STRATEGY_PROFILE_OVERRIDE",
    "LOG_LEVEL", "TZ",
}

# Large-cap proxy symbols with sector labels for breadth-spread computation
PROXIES: list[tuple[str, str]] = [
    ("RELIANCE",   "energy"),
    ("TCS",        "it"),
    ("HDFCBANK",   "banking"),
    ("ICICIBANK",  "banking"),
    ("INFY",       "it"),
    ("HINDUNILVR", "fmcg"),
    ("BHARTIARTL", "telecom"),
    ("LT",         "infra"),
    ("AXISBANK",   "banking"),
    ("KOTAKBANK",  "banking"),
    ("SBIN",       "banking"),
    ("WIPRO",      "it"),
    ("BAJFINANCE", "finance"),
    ("MARUTI",     "auto"),
    ("TATAMOTORS", "auto"),
]

VALID_PROFILES = ["max-profit", "bear-fighter", "aggressive", "contrarian", "balanced"]


# ─────────────────────────────────────────────────────────────────────────────
#  Market signals dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class MarketSignals:
    # ── Core regime signals ──────────────────────────────────────────────────
    bull_ratio:            float = 0.5    # 0–1   fraction of proxies above 50-SMA
    avg_rsi:               float = 50.0   # 0–100 average RSI(14)
    volatility_pct:        float = 2.0    # 0–10  avg 14d ATR as % of price
    momentum_5d:           float = 0.0    # −10/+10 avg 5d % return
    vol_ratio:             float = 1.0    # 0–3   avg (5d vol)/(20d vol)

    # ── Quality / character signals ──────────────────────────────────────────
    trend_consistency:     float = 0.5    # 0–1   fraction of days confirming trend
    stretch_from_sma20:    float = 0.0    # −3/+3 ATR units above/below 20-SMA
    down_vol_surge:        float = 0.0    # 0–1   fraction of proxies with climactic sell vol
    price_vol_divergence:  float = 0.0    # −1/+1 price/volume direction correlation (legacy)
    sector_breadth_spread: float = 0.1    # 0–0.5 std of per-sector bull_ratios
    regime_velocity:       float = 0.0    # change in bull_ratio over 5 days

    # ── NEW: research-backed signals ─────────────────────────────────────────
    hi52_proximity:        float = 0.0    # 0–1   fraction within 5% of 52-week high
    choppiness_index:      float = 50.0   # 28–100 (<38=trending, >62=choppy)
    obv_divergence:        float = 0.0    # −1/+1 (+= accumulation, −= distribution)
    adx_avg:               float = 25.0   # 0–60  avg ADX (<20=no trend, >25=trending)
    momentum_crash_risk:   bool  = False  # 1m bounce >8% after >20% trough

    # ── Supporting aggregates ─────────────────────────────────────────────────
    momentum_1m:           float = 0.0    # avg 1-month % return across proxies
    max_dd_6m:             float = 0.0    # avg 6-month max drawdown % (negative)
    proxy_count:           int   = 0

    @property
    def regime_pos(self) -> float:
        """
        Continuous position: 0.0 = crash, 1.0 = bear, 2.0 = neutral, 3.0 = bull.

        Primary driver:  bull_ratio (0→3)
        Adjustments:
          RSI            overbought→bull, oversold→crash      (±0.40)
          Momentum 5d    confirms trend direction              (±0.30)
          Consistency    consistent trend = more certainty     (±0.15)
          Velocity       improving = slight boost              (±0.20)
          52wk proximity stocks near highs = bull confirmation (−0.13/+0.25)
        """
        pos      = self.bull_ratio * 3.0
        rsi_adj  = (self.avg_rsi - 50.0) / 50.0 * 0.4
        mom_adj  = float(np.clip(self.momentum_5d / 5.0, -0.3, 0.3))
        con_adj  = (self.trend_consistency - 0.5) * 0.3        # ±0.15
        vel_adj  = float(np.clip(self.regime_velocity * 5.0, -0.2, 0.2))
        # 52wk high: 0→−0.125, 0.5→0, 1→+0.25  (bull bonus > bear penalty)
        hi52_adj = float(np.clip((self.hi52_proximity - 0.5) * 0.5, -0.125, 0.25))
        return float(np.clip(pos + rsi_adj + mom_adj + con_adj + vel_adj + hi52_adj, 0.0, 3.0))

    @property
    def scenario(self) -> str:
        """Detect the dominant market scenario from signal combinations."""
        r, m, v, vr, rsi = (self.bull_ratio, self.momentum_5d,
                            self.volatility_pct, self.vol_ratio, self.avg_rsi)

        # CAPITULATION: panic bottom — requires climactic sell vol + oversold + volatility spike
        if r < 0.25 and rsi < 38 and self.down_vol_surge > 0.30 and vr > 1.5 and v > 3.0:
            return "CAPITULATION"

        # OVERBOUGHT_BULL: exhausted bull — stretch + RSI extreme + vol drying up
        if r > 0.65 and rsi > 68 and self.stretch_from_sma20 > 1.8 and vr < 1.0:
            return "OVERBOUGHT_BULL"

        # MOMENTUM_ACCEL: research requires 52wk high confirmation (George & Hwang 2004:
        # stocks near 52wk high = 70% of momentum portfolio returns)
        if (r > 0.65 and m > 3.5 and self.trend_consistency > 0.65 and vr > 1.3
                and self.hi52_proximity > 0.45 and not self.momentum_crash_risk):
            return "MOMENTUM_ACCEL"

        # BEAR_RALLY: low-vol bounce trap OR momentum crash risk (Daniel & Moskowitz 2014:
        # 1-month bounce >8% after prolonged bear = momentum crash danger)
        if (r < 0.40 and m > 1.5 and vr < 1.0) or self.momentum_crash_risk:
            return "BEAR_RALLY"

        # DISTRIBUTION: OBV declining while price holds (Wyckoff: "effort without result")
        if r > 0.50 and self.obv_divergence < -0.20 and vr > 1.1:
            return "DISTRIBUTION"

        # CHOPPY: research-validated CI+ADX thresholds (not momentum heuristics)
        if self.choppiness_index > 62 and self.adx_avg < 22 and abs(m) < 1.5:
            return "CHOPPY"

        return "none"

    @property
    def regime_label(self) -> str:
        sc = self.scenario
        if sc != "none":
            return sc
        p = self.regime_pos
        if p >= 2.5:  return "BULL"
        if p >= 1.5:  return "NEUTRAL"
        if p >= 0.75: return "BEAR"
        return "CRASH"


# ─────────────────────────────────────────────────────────────────────────────
#  Signal computation helpers (original)
# ─────────────────────────────────────────────────────────────────────────────

def _rsi(series: pd.Series, period: int = 14) -> float:
    d = series.diff().dropna()
    if len(d) < period:
        return 50.0
    g = d.clip(lower=0).rolling(period).mean()
    l = (-d.clip(upper=0)).rolling(period).mean()
    ll = float(l.iloc[-1])
    return round(100.0 - 100.0 / (1.0 + float(g.iloc[-1]) / ll), 1) if ll > 1e-9 else 100.0


def _atr_pct(df: pd.DataFrame, period: int = 14) -> float:
    h, l, c = df["High"], df["Low"], df["Close"]
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr = tr.rolling(period).mean().iloc[-1]
    price = float(c.iloc[-1])
    return float(atr / price * 100) if price > 0 else 2.0


def _vol_ratio(df: pd.DataFrame) -> float:
    v = df["Volume"].replace(0, np.nan).dropna()
    if len(v) < 20:
        return 1.0
    return round(float(v.rolling(5).mean().iloc[-1]) / float(v.rolling(20).mean().iloc[-1]), 3)


def _trend_consistency(close: pd.Series, window: int = 20) -> float:
    """Fraction of last `window` days whose direction matches the net 5-day move."""
    if len(close) < window + 1:
        return 0.5
    net_dir   = np.sign(float(close.iloc[-1]) - float(close.iloc[-6]))
    daily_dir = np.sign(close.diff().tail(window).values)
    if net_dir == 0:
        return 0.5
    return float((daily_dir == net_dir).mean())


def _stretch_sma20(close: pd.Series, atr_pct: float) -> float:
    """Distance from 20-SMA expressed in ATR units."""
    if len(close) < 20 or atr_pct <= 0:
        return 0.0
    sma20 = float(close.rolling(20).mean().iloc[-1])
    price = float(close.iloc[-1])
    atr   = atr_pct / 100.0 * price
    return round((price - sma20) / atr, 2) if atr > 0 else 0.0


def _down_vol_surge(df: pd.DataFrame) -> bool:
    """True if the highest-volume day in last 10 days was a down day AND 2× avg volume."""
    if len(df) < 20:
        return False
    last10   = df.tail(10)
    avg_vol  = float(df["Volume"].rolling(20).mean().iloc[-1])
    idx      = last10["Volume"].idxmax()
    peak_vol = float(last10.loc[idx, "Volume"])
    is_down  = float(last10.loc[idx, "Close"]) < float(last10.loc[idx, "Open"])
    return is_down and (peak_vol > 2.0 * avg_vol)


def _price_vol_divergence(df: pd.DataFrame, window: int = 10) -> float:
    """Pearson correlation of daily price-change direction vs volume-change direction."""
    if len(df) < window + 1:
        return 0.0
    tail = df.tail(window + 1)
    pc   = tail["Close"].pct_change().dropna()
    vc   = tail["Volume"].pct_change().dropna()
    n    = min(len(pc), len(vc))
    if n < 4:
        return 0.0
    corr = float(pc.tail(n).corr(vc.tail(n)))
    return 0.0 if np.isnan(corr) else round(corr, 3)


def _above_50sma_5d_ago(close: pd.Series) -> bool:
    """Was the closing price above its 50-SMA as of 5 trading days ago?"""
    if len(close) < 56:
        return False
    price_5d_ago = float(close.iloc[-6])
    sma50_5d_ago = float(close.iloc[-56:-6].mean())
    return price_5d_ago > sma50_5d_ago


# ─────────────────────────────────────────────────────────────────────────────
#  NEW helper functions (research-backed)
# ─────────────────────────────────────────────────────────────────────────────

def _hi52_proximity(close: pd.Series, threshold: float = 0.05) -> bool:
    """
    True if current price is within `threshold` (5%) of its 52-week high.

    Research: George & Hwang (2004, Bauer UH) — stocks near 52-week high
    represent 45% of a momentum portfolio but 70% of its returns.
    The 52-week high proximity is the single strongest momentum sub-signal.
    """
    lookback = min(len(close), 252)
    if lookback < 20:
        return False
    hi52  = float(close.tail(lookback).max())
    price = float(close.iloc[-1])
    return (hi52 - price) / hi52 <= threshold if hi52 > 0 else False


def _choppiness_index(df: pd.DataFrame, period: int = 14) -> float:
    """
    Choppiness Index = 100 × log10(Σ|ATR(1,i)| / (HH−LL)) / log10(n).

    Research-validated thresholds (QuantifiedStrategies):
      > 62 = choppy / range-bound
      < 38 = strongly trending
      Range: ~28 (perfect trend) to ~100 (maximum chop).
    """
    if len(df) < period + 1:
        return 50.0
    tail = df.tail(period + 1)
    h, l, c = tail["High"], tail["Low"], tail["Close"]
    tr = pd.concat([h - l, (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    tr = tr.dropna()
    if len(tr) < period:
        return 50.0
    atr_sum = float(tr.tail(period).sum())
    hh      = float(h.tail(period).max())
    ll      = float(l.tail(period).min())
    if (hh - ll) <= 0 or atr_sum <= 0:
        return 50.0
    ci = 100.0 * np.log10(atr_sum / (hh - ll)) / np.log10(period)
    return round(float(np.clip(ci, 28.0, 100.0)), 1)


def _adx(df: pd.DataFrame, period: int = 14) -> float:
    """
    Average Directional Index (Wilder's method).

    < 20 = no directional trend
    20–25 = weak trend
    > 25 = trending
    > 40 = strong trend

    Used alongside Choppiness Index to double-confirm choppy vs ranging markets.
    """
    if len(df) < period * 2 + 5:
        return 25.0

    high  = df["High"].values.astype(float)
    low   = df["Low"].values.astype(float)
    close = df["Close"].values.astype(float)
    n     = len(high)

    tr_arr    = np.zeros(n)
    plus_arr  = np.zeros(n)
    minus_arr = np.zeros(n)

    for i in range(1, n):
        tr_arr[i] = max(high[i] - low[i],
                        abs(high[i] - close[i - 1]),
                        abs(low[i]  - close[i - 1]))
        up   = high[i] - high[i - 1]
        down = low[i - 1] - low[i]
        plus_arr[i]  = up   if (up > down and up > 0)   else 0.0
        minus_arr[i] = down if (down > up and down > 0) else 0.0

    def _wilder(arr: np.ndarray) -> list[float]:
        smoothed = [float(arr[1: period + 1].sum())]
        for v in arr[period + 1:]:
            smoothed.append(smoothed[-1] - smoothed[-1] / period + float(v))
        return smoothed

    tr_s  = _wilder(tr_arr)
    pd_s  = _wilder(plus_arr)
    md_s  = _wilder(minus_arr)

    dx_vals: list[float] = []
    for ts, ps, ms in zip(tr_s, pd_s, md_s):
        if ts == 0:
            continue
        pdi  = 100.0 * ps / ts
        mdi  = 100.0 * ms / ts
        dsum = pdi + mdi
        if dsum == 0:
            continue
        dx_vals.append(100.0 * abs(pdi - mdi) / dsum)

    if len(dx_vals) < period:
        return 25.0

    adx_val = float(np.mean(dx_vals[:period]))
    for dx in dx_vals[period:]:
        adx_val = (adx_val * (period - 1) + dx) / period

    return round(adx_val, 1)


def _obv_divergence(df: pd.DataFrame, lookback: int = 5) -> float:
    """
    OBV trend vs price trend alignment over `lookback` days.

    +1 = OBV confirms price direction (healthy accumulation / selling)
    -1 = OBV contradicts rising price (distribution — smart money exiting)
     0 = flat price or insufficient data

    Research (Wyckoff): OBV declining while price holds = distribution.
    Cumulative nature makes it harder to fake than single-period volume.
    """
    if len(df) < lookback + 5:
        return 0.0
    close = df["Close"].dropna()
    vol   = df["Volume"].fillna(0)

    direction = np.sign(close.diff()).fillna(0)
    obv       = (direction * vol).cumsum()

    price_chg = float(close.iloc[-1] - close.iloc[-(lookback + 1)]) / (
        float(close.iloc[-(lookback + 1)]) + 1e-9
    )
    avg_vol  = float(vol.mean()) or 1.0
    obv_chg  = float(obv.iloc[-1] - obv.iloc[-(lookback + 1)]) / (avg_vol * lookback)

    if abs(price_chg) < 0.002:
        return 0.0

    obv_scaled = float(np.clip(obv_chg, -1.0, 1.0))

    if price_chg > 0:
        return round(obv_scaled, 3)
    else:
        # Price falling with OBV rising = accumulation on dip (half-weight)
        return round(-obv_scaled * 0.5, 3)


def _max_drawdown_6m(close: pd.Series) -> float:
    """Maximum drawdown (%) over the last 126 trading days. Returns negative number."""
    if len(close) < 22:
        return 0.0
    tail     = close.tail(min(126, len(close)))
    roll_max = tail.cummax()
    dd       = (tail - roll_max) / roll_max * 100
    return float(dd.min())


# ─────────────────────────────────────────────────────────────────────────────
#  Main signal computation
# ─────────────────────────────────────────────────────────────────────────────

def compute_signals(verbose: bool = True) -> MarketSignals:
    ohlcv_dir = BOT_DIR / "cache" / "ohlcv"
    if not ohlcv_dir.exists():
        print("  ⚠  cache/ohlcv not found — using neutral defaults")
        return MarketSignals()

    rows: list[dict] = []
    for sym, sector in PROXIES:
        f = ohlcv_dir / f"{sym}.parquet"
        if not f.exists():
            continue
        try:
            df    = pd.read_parquet(f)
            if len(df) < 50:
                continue
            close = df["Close"].dropna()
            price = float(close.iloc[-1])
            sma50 = float(close.rolling(50).mean().iloc[-1])
            rsi   = _rsi(close)
            atrp  = _atr_pct(df)
            vr    = _vol_ratio(df)
            ret5  = float((close.iloc[-1] / close.iloc[-6]  - 1) * 100) if len(close) >= 6  else 0.0
            ret22 = float((close.iloc[-1] / close.iloc[-23] - 1) * 100) if len(close) >= 23 else 0.0
            tc    = _trend_consistency(close)
            st    = _stretch_sma20(close, atrp)
            pvd   = _price_vol_divergence(df)
            dvs   = _down_vol_surge(df)
            a5    = _above_50sma_5d_ago(close)
            h52   = _hi52_proximity(close)
            ci    = _choppiness_index(df)
            adxv  = _adx(df)
            obv_d = _obv_divergence(df)
            mdd   = _max_drawdown_6m(close)

            rows.append({
                "sym": sym, "sector": sector,
                "price": price, "sma50": sma50,
                "above_50": price > sma50,
                "above_50_5d": a5,
                "rsi": rsi, "atr_pct": atrp, "vol_ratio": vr,
                "ret5d": ret5, "ret22d": ret22,
                "trend_consistency": tc, "stretch": st,
                "down_vol_surge": dvs, "price_vol_div": pvd,
                "hi52": h52, "choppiness": ci, "adx": adxv,
                "obv_div": obv_d, "max_dd_6m": mdd,
            })

            if verbose:
                trend     = "▲" if price > sma50 else "▼"
                hi_marker = "★" if h52 else " "
                print(f"    {sym:<15}  ₹{price:>9.2f}  sma50={sma50:>9.2f}"
                      f"  RSI={rsi:>4.0f}  ATR={atrp:.1f}%"
                      f"  5d={ret5:+.1f}%  vol×{vr:.2f}"
                      f"  CI={ci:.0f}  ADX={adxv:.0f}  {trend}{hi_marker}")
        except Exception as exc:
            if verbose:
                print(f"    {sym:<15}  ✗ {exc}")

    if not rows:
        return MarketSignals()

    n = len(rows)

    bull_ratio   = sum(1 for r in rows if r["above_50"])    / n
    bull_5d_ago  = sum(1 for r in rows if r["above_50_5d"]) / n
    avg_rsi      = float(np.mean([r["rsi"]              for r in rows]))
    vol_pct      = float(np.mean([r["atr_pct"]          for r in rows]))
    mom_5d       = float(np.mean([r["ret5d"]             for r in rows]))
    mom_1m       = float(np.mean([r["ret22d"]            for r in rows]))
    vol_rat      = float(np.mean([r["vol_ratio"]         for r in rows]))
    trend_con    = float(np.mean([r["trend_consistency"] for r in rows]))
    stretch      = float(np.mean([r["stretch"]           for r in rows]))
    dvs_frac     = sum(1 for r in rows if r["down_vol_surge"]) / n
    pvd_avg      = float(np.mean([r["price_vol_div"]     for r in rows]))
    regime_vel   = bull_ratio - bull_5d_ago
    hi52_frac    = sum(1 for r in rows if r["hi52"])     / n
    ci_avg       = float(np.mean([r["choppiness"]        for r in rows]))
    adx_avg      = float(np.mean([r["adx"]               for r in rows]))
    obv_div_avg  = float(np.mean([r["obv_div"]           for r in rows]))
    max_dd_avg   = float(np.mean([r["max_dd_6m"]         for r in rows]))

    # Momentum crash risk: 1-month bounce >8% after 6-month drawdown >20%
    # Research: Daniel & Moskowitz (NBER 2014) — primary momentum crash indicator
    crash_risk = (mom_1m > 8.0) and (max_dd_avg < -20.0)

    # Per-sector bull_ratios for breadth spread
    sectors: dict[str, list[bool]] = {}
    for r in rows:
        sectors.setdefault(r["sector"], []).append(r["above_50"])
    sector_ratios  = [sum(v) / len(v) for v in sectors.values() if v]
    breadth_spread = float(np.std(sector_ratios)) if len(sector_ratios) > 1 else 0.0

    sig = MarketSignals(
        bull_ratio            = bull_ratio,
        avg_rsi               = avg_rsi,
        volatility_pct        = vol_pct,
        momentum_5d           = mom_5d,
        vol_ratio             = vol_rat,
        trend_consistency     = trend_con,
        stretch_from_sma20    = stretch,
        down_vol_surge        = dvs_frac,
        price_vol_divergence  = pvd_avg,
        sector_breadth_spread = breadth_spread,
        regime_velocity       = regime_vel,
        hi52_proximity        = hi52_frac,
        choppiness_index      = ci_avg,
        obv_divergence        = obv_div_avg,
        adx_avg               = adx_avg,
        momentum_crash_risk   = crash_risk,
        momentum_1m           = mom_1m,
        max_dd_6m             = max_dd_avg,
        proxy_count           = n,
    )

    if verbose:
        print(f"\n  {'─'*70}")
        print(f"  bull={sig.bull_ratio:.0%}  rsi={sig.avg_rsi:.0f}  "
              f"vol={sig.volatility_pct:.1f}%  mom5d={sig.momentum_5d:+.1f}%  "
              f"mom1m={sig.momentum_1m:+.1f}%  vol_ratio={sig.vol_ratio:.2f}x")
        print(f"  consistency={sig.trend_consistency:.2f}  "
              f"stretch={sig.stretch_from_sma20:+.1f}ATR  "
              f"dvs={sig.down_vol_surge:.2f}  "
              f"obv_div={sig.obv_divergence:+.2f}")
        print(f"  hi52={sig.hi52_proximity:.0%}  "
              f"CI={sig.choppiness_index:.0f}  ADX={sig.adx_avg:.0f}  "
              f"sector_spread={sig.sector_breadth_spread:.2f}  "
              f"vel={sig.regime_velocity:+.2f}/5d")
        crash_str = "  ⚠ MOMENTUM_CRASH_RISK" if sig.momentum_crash_risk else ""
        print(f"  regime_pos={sig.regime_pos:.2f}/3.0  "
              f"scenario={sig.scenario}  →  {sig.regime_label}{crash_str}")

    return sig


# ─────────────────────────────────────────────────────────────────────────────
#  Fine-tuning engine
# ─────────────────────────────────────────────────────────────────────────────

def _lerp4(vals: tuple, pos: float) -> float:
    """4-anchor linear interpolation. pos: 0=crash, 1=bear, 2=neutral, 3=bull."""
    i    = min(int(pos), 2)
    frac = pos - i
    return round(float(vals[i]) + (float(vals[i + 1]) - float(vals[i])) * frac, 4)


# ── Scenario override tables ──────────────────────────────────────────────────
# Exact parameter values per scenario — override the interpolated base.
# Multiple simultaneous scenarios → most conservative value per-parameter.

_SCENARIO_OVERRIDES: dict[str, dict[str, float]] = {
    "CAPITULATION": {
        # Panic bottom: climactic volume + oversold + high volatility.
        # Research: average capitulation bounce = +30–50% over 3 months on NSE.
        # Wide stops because post-cap whipsaw is extreme; low bypass bar because
        # quality stocks are indiscriminately beaten down.
        "SCORE_BUY_THRESHOLD":               70.0,
        "ENTRY_BULL_RATIO_MIN":              0.03,
        "ENTRY_MIN_QUALITY":                 50.0,
        "ENTRY_PULLBACK_MULT":               2.2,
        "ENTRY_RSI_IDEAL_MAX":               45.0,
        "ENTRY_BOLLINGER_B_MAX":             0.40,
        "ENTRY_VOL_MIN_RATIO":               0.80,
        "ENTRY_REGIME_BYPASS_MIN_SCORE":     68.0,
        "ENTRY_REGIME_BYPASS_MAX_RSI":       44.0,
        "ENTRY_REGIME_BYPASS_MIN_VELOCITY":  0.5,
        "EXIT_ATR_STOP_MULT":                3.5,
        "EXIT_ATR_CHANDELIER_MULT":          5.0,
        "EXIT_RISK_REWARD_RATIO":            4.5,   # violent reversals = big upside
        "EXIT_TRAILING_ACTIVATION_PCT":      8.0,   # don't arm trail too early — let it run
        "RISK_MAX_HOLDINGS":                 4.0,
        "SCORE_TOP_N":                       30.0,
        "INTRADAY_PULSE_WEIGHT":             0.35,
        "SCORE_SELL_THRESHOLD":              30.0,  # hold through the recovery
        "SCORE_EMERGENCY_SELL_THRESHOLD":    14.0,
    },
    "OVERBOUGHT_BULL": {
        # Bull exhausted — RSI >68, stretch >1.8 ATR, volume drying up.
        # Research: arm trailing very early, modest R:R — big moves are behind.
        # No chasing: demand pullback to lower band before entering.
        "SCORE_BUY_THRESHOLD":               75.0,
        "ENTRY_BULL_RATIO_MIN":              0.40,
        "ENTRY_MIN_QUALITY":                 58.0,
        "ENTRY_PULLBACK_MULT":               1.0,
        "ENTRY_RSI_IDEAL_MAX":               52.0,
        "ENTRY_BOLLINGER_B_MAX":             0.42,
        "ENTRY_VOL_MIN_RATIO":               0.90,
        "ENTRY_REGIME_BYPASS_MIN_SCORE":     82.0,
        "ENTRY_REGIME_BYPASS_MAX_RSI":       35.0,
        "ENTRY_REGIME_BYPASS_MIN_VELOCITY":  1.5,
        "EXIT_ATR_STOP_MULT":                2.3,
        "EXIT_ATR_CHANDELIER_MULT":          3.5,
        "EXIT_RISK_REWARD_RATIO":            1.8,
        "EXIT_TRAILING_ACTIVATION_PCT":      1.5,   # arm early to protect gains
        "RISK_MAX_HOLDINGS":                 7.0,
        "SCORE_TOP_N":                       35.0,
        "INTRADAY_PULSE_WEIGHT":             0.15,
        "SCORE_SELL_THRESHOLD":              47.0,  # exit on first weakness
        "SCORE_EMERGENCY_SELL_THRESHOLD":    26.0,
    },
    "MOMENTUM_ACCEL": {
        # Consistent trend + expanding volume + 52-week high breakouts.
        # Research (George & Hwang): 52wk high proximity = 70% of momentum returns.
        # NSE Nifty200 Momentum30 = best-performing factor in bull regime.
        # Indian bull runs average 24–75 months — hold winners very long.
        "SCORE_BUY_THRESHOLD":               61.0,
        "ENTRY_BULL_RATIO_MIN":              0.35,
        "ENTRY_MIN_QUALITY":                 44.0,
        "ENTRY_PULLBACK_MULT":               0.0,   # buy breakouts immediately
        "ENTRY_RSI_IDEAL_MAX":               74.0,  # momentum stocks run hot
        "ENTRY_BOLLINGER_B_MAX":             0.80,  # band expansion = continuation
        "ENTRY_VOL_MIN_RATIO":               0.60,
        "ENTRY_REGIME_BYPASS_MIN_SCORE":     80.0,
        "ENTRY_REGIME_BYPASS_MAX_RSI":       38.0,
        "ENTRY_REGIME_BYPASS_MIN_VELOCITY":  1.8,
        "EXIT_ATR_STOP_MULT":                1.4,   # tight stops in strong trend
        "EXIT_ATR_CHANDELIER_MULT":          2.2,
        "EXIT_RISK_REWARD_RATIO":            4.5,   # let the momentum run
        "EXIT_TRAILING_ACTIVATION_PCT":      0.8,   # arm trail very early
        "RISK_MAX_HOLDINGS":                 15.0,
        "SCORE_TOP_N":                       68.0,
        "INTRADAY_PULSE_WEIGHT":             0.33,
        "SCORE_SELL_THRESHOLD":              34.0,  # hold longer (Indian bull runs)
        "SCORE_EMERGENCY_SELL_THRESHOLD":    14.0,
    },
    "BEAR_RALLY": {
        # Low-volume countertrend bounce OR momentum crash risk.
        # Research (Daniel & Moskowitz NBER 2014): momentum crashes happen when
        # 1-month bounce >8% follows prolonged bear. Short side of momentum kills.
        # Average bear market rally = 44 days, +10–15% — don't overstay.
        "SCORE_BUY_THRESHOLD":               78.0,
        "ENTRY_BULL_RATIO_MIN":              0.40,
        "ENTRY_MIN_QUALITY":                 60.0,
        "ENTRY_PULLBACK_MULT":               1.8,
        "ENTRY_RSI_IDEAL_MAX":               40.0,  # only deeply beaten stocks
        "ENTRY_BOLLINGER_B_MAX":             0.40,
        "ENTRY_VOL_MIN_RATIO":               0.88,
        "ENTRY_REGIME_BYPASS_MIN_SCORE":     82.0,
        "ENTRY_REGIME_BYPASS_MAX_RSI":       38.0,
        "ENTRY_REGIME_BYPASS_MIN_VELOCITY":  1.5,
        "EXIT_ATR_STOP_MULT":                3.2,   # bear rallies whipsaw violently
        "EXIT_ATR_CHANDELIER_MULT":          4.2,
        "EXIT_RISK_REWARD_RATIO":            2.0,   # take profits quickly
        "EXIT_TRAILING_ACTIVATION_PCT":      4.0,
        "RISK_MAX_HOLDINGS":                 4.0,
        "SCORE_TOP_N":                       28.0,
        "INTRADAY_PULSE_WEIGHT":             0.15,
        "SCORE_SELL_THRESHOLD":              46.0,  # exit fast on any weakness
        "SCORE_EMERGENCY_SELL_THRESHOLD":    26.0,
    },
    "DISTRIBUTION": {
        # Price holding but OBV declining — Wyckoff "effort without result".
        # Smart money exiting into retail strength. Ends in sharp breakdown.
        # Exit threshold raised: don't wait for fundamental deterioration.
        "SCORE_BUY_THRESHOLD":               75.0,
        "ENTRY_BULL_RATIO_MIN":              0.38,
        "ENTRY_MIN_QUALITY":                 58.0,
        "ENTRY_PULLBACK_MULT":               1.0,
        "ENTRY_RSI_IDEAL_MAX":               50.0,  # no buying extended stocks
        "ENTRY_BOLLINGER_B_MAX":             0.44,
        "ENTRY_VOL_MIN_RATIO":               0.90,
        "ENTRY_REGIME_BYPASS_MIN_SCORE":     80.0,
        "ENTRY_REGIME_BYPASS_MAX_RSI":       40.0,
        "ENTRY_REGIME_BYPASS_MIN_VELOCITY":  1.2,
        "EXIT_ATR_STOP_MULT":                2.8,
        "EXIT_ATR_CHANDELIER_MULT":          3.2,   # tighter chandelier to lock in gains
        "EXIT_RISK_REWARD_RATIO":            1.9,
        "EXIT_TRAILING_ACTIVATION_PCT":      1.8,   # arm early — protect from breakdown
        "RISK_MAX_HOLDINGS":                 6.0,
        "SCORE_TOP_N":                       32.0,
        "INTRADAY_PULSE_WEIGHT":             0.18,
        "SCORE_SELL_THRESHOLD":              47.0,
        "SCORE_EMERGENCY_SELL_THRESHOLD":    25.0,
    },
    "CHOPPY": {
        # Range-bound: CI>62, ADX<22. Breakouts fail, mean-reversion dominates.
        # Research (QuantifiedStrategies): CI>62 + ADX<20 = range market confirmed.
        # "No stop at all produces best MAR ratio in mean-reversion" — use signal exits.
        # Highest quality bar of all scenarios; tiny targets = range boundaries only.
        "SCORE_BUY_THRESHOLD":               76.0,
        "ENTRY_BULL_RATIO_MIN":              0.30,
        "ENTRY_MIN_QUALITY":                 64.0,
        "ENTRY_PULLBACK_MULT":               1.3,   # always wait for pullback in a range
        "ENTRY_RSI_IDEAL_MAX":               48.0,
        "ENTRY_BOLLINGER_B_MAX":             0.38,  # near lower band only
        "ENTRY_VOL_MIN_RATIO":               0.95,  # volume confirmation critical
        "ENTRY_REGIME_BYPASS_MIN_SCORE":     78.0,
        "ENTRY_REGIME_BYPASS_MAX_RSI":       42.0,
        "ENTRY_REGIME_BYPASS_MIN_VELOCITY":  1.2,
        "EXIT_ATR_STOP_MULT":                2.0,
        "EXIT_ATR_CHANDELIER_MULT":          2.8,
        "EXIT_RISK_REWARD_RATIO":            1.5,   # range trade targets only
        "EXIT_TRAILING_ACTIVATION_PCT":      1.5,
        "RISK_MAX_HOLDINGS":                 6.0,
        "SCORE_TOP_N":                       28.0,
        "INTRADAY_PULSE_WEIGHT":             0.12,  # intraday is noise in chop
        "SCORE_SELL_THRESHOLD":              48.0,
        "SCORE_EMERGENCY_SELL_THRESHOLD":    24.0,
    },
}


def _most_conservative_merge(a: dict, b: dict) -> dict:
    """
    When two scenarios fire simultaneously, take the more conservative value per param.
    Conservative = harder to enter + safer to hold.
    """
    higher_wins = {
        "SCORE_BUY_THRESHOLD", "ENTRY_BULL_RATIO_MIN", "ENTRY_MIN_QUALITY",
        "ENTRY_PULLBACK_MULT", "ENTRY_VOL_MIN_RATIO",
        "ENTRY_REGIME_BYPASS_MIN_SCORE", "ENTRY_REGIME_BYPASS_MIN_VELOCITY",
        "EXIT_ATR_STOP_MULT", "EXIT_ATR_CHANDELIER_MULT",
        "SCORE_SELL_THRESHOLD", "SCORE_EMERGENCY_SELL_THRESHOLD",
    }
    lower_wins = {
        "ENTRY_RSI_IDEAL_MAX", "ENTRY_BOLLINGER_B_MAX",
        "EXIT_RISK_REWARD_RATIO", "EXIT_TRAILING_ACTIVATION_PCT",
        "RISK_MAX_HOLDINGS", "SCORE_TOP_N",
        "ENTRY_REGIME_BYPASS_MAX_RSI", "INTRADAY_PULSE_WEIGHT",
    }
    result: dict = {}
    for k in set(a) | set(b):
        av, bv = a.get(k), b.get(k)
        if av is None:         result[k] = bv; continue
        if bv is None:         result[k] = av; continue
        if k in higher_wins:   result[k] = max(av, bv)
        elif k in lower_wins:  result[k] = min(av, bv)
        else:                  result[k] = (av + bv) / 2
    return result


def fine_tune(sig: MarketSignals) -> dict[str, str]:
    """
    Compute optimal .env parameters for today's market conditions.

    Step 1: 4-anchor base interpolation across regime_pos (0=crash → 3=bull).
            Anchors calibrated from academic research:
            - EXIT_ATR_STOP_MULT: research standard = 3× ATR (14-period)
            - EXIT_RISK_REWARD_RATIO: 1:3.5 in bull (requires only 22% win rate)
            - EXIT_TRAILING_ACTIVATION_PCT: activate after ~1× ATR gain
            - RISK_MAX_HOLDINGS: 12–18 large-caps = 90% diversification benefit
    Step 2: 10 signal overlays applied sequentially.
    Step 3: Scenario overrides replace Steps 1+2. Multi-scenario → conservative merge.
    """
    p  = sig.regime_pos
    v  = sig.volatility_pct
    m  = sig.momentum_5d
    vr = sig.vol_ratio
    tc = sig.trend_consistency

    # ── Step 1: Base interpolation ────────────────────────────────────────────
    raw: dict[str, float] = {}

    #                                          crash  bear  neutral  bull
    raw["SCORE_BUY_THRESHOLD"]              = _lerp4((78,   74,  69,  63), p)
    raw["ENTRY_BULL_RATIO_MIN"]             = _lerp4((0.03, 0.05, 0.18, 0.35), p)
    raw["ENTRY_MIN_QUALITY"]                = _lerp4((50,   48,  50,  44), p)
    raw["ENTRY_PULLBACK_MULT"]              = _lerp4((1.8,  1.2, 0.5, 0.1), p)
    raw["ENTRY_RSI_IDEAL_MAX"]              = _lerp4((48,   55,  58,  70), p)
    raw["ENTRY_BOLLINGER_B_MAX"]            = _lerp4((0.35, 0.50, 0.55, 0.72), p)
    raw["ENTRY_VOL_MIN_RATIO"]              = _lerp4((0.85, 0.82, 0.78, 0.60), p)
    raw["ENTRY_REGIME_BYPASS_MIN_SCORE"]    = _lerp4((72,   74,  76,  80), p)
    raw["ENTRY_REGIME_BYPASS_MAX_RSI"]      = _lerp4((62,   57,  50,  45), p)
    raw["ENTRY_REGIME_BYPASS_MIN_VELOCITY"] = _lerp4((0.3,  0.5, 0.8, 1.2), p)
    raw["EXIT_ATR_STOP_MULT"]               = _lerp4((3.2,  2.5, 2.0, 1.4), p)
    raw["EXIT_ATR_CHANDELIER_MULT"]         = _lerp4((4.5,  3.8, 3.0, 2.5), p)
    raw["EXIT_RISK_REWARD_RATIO"]           = _lerp4((1.6,  2.0, 2.5, 3.5), p)
    raw["EXIT_TRAILING_ACTIVATION_PCT"]     = _lerp4((5.0,  3.5, 2.0, 1.0), p)
    raw["RISK_MAX_HOLDINGS"]                = _lerp4((4,    6,   10,  15), p)
    raw["SCORE_TOP_N"]                      = _lerp4((35,   45,  50,  60), p)
    raw["INTRADAY_PULSE_WEIGHT"]            = _lerp4((0.15, 0.20, 0.22, 0.30), p)
    raw["SCORE_SELL_THRESHOLD"]             = _lerp4((45,   42,  40,  36), p)
    raw["SCORE_EMERGENCY_SELL_THRESHOLD"]   = _lerp4((22,   20,  18,  16), p)

    # ── Step 2: Signal overlays ───────────────────────────────────────────────

    # a) Volatility — high ATR widens stops, shrinks holdings
    if v > 4.0:
        raw["EXIT_ATR_STOP_MULT"]       = max(raw["EXIT_ATR_STOP_MULT"],       3.5)
        raw["EXIT_ATR_CHANDELIER_MULT"] = max(raw["EXIT_ATR_CHANDELIER_MULT"], 4.5)
        raw["RISK_MAX_HOLDINGS"]        = max(4, raw["RISK_MAX_HOLDINGS"] - 3)
    elif v > 3.0:
        raw["EXIT_ATR_STOP_MULT"]       = max(raw["EXIT_ATR_STOP_MULT"],       3.0)
        raw["EXIT_ATR_CHANDELIER_MULT"] = max(raw["EXIT_ATR_CHANDELIER_MULT"], 4.0)
        raw["RISK_MAX_HOLDINGS"]        = max(5, raw["RISK_MAX_HOLDINGS"] - 2)
    elif v < 1.0:
        raw["EXIT_ATR_STOP_MULT"]       = min(raw["EXIT_ATR_STOP_MULT"],       1.8)

    # b) Momentum — strong trend lowers buy threshold; downtrend raises it
    if m > 3.0:
        raw["SCORE_BUY_THRESHOLD"]      = max(62,  raw["SCORE_BUY_THRESHOLD"]    - 2.5)
        raw["ENTRY_PULLBACK_MULT"]      = max(0.0, raw["ENTRY_PULLBACK_MULT"]    - 0.3)
        raw["EXIT_RISK_REWARD_RATIO"]   = min(4.5, raw["EXIT_RISK_REWARD_RATIO"] + 0.3)
    elif m < -3.0:
        raw["SCORE_BUY_THRESHOLD"]      = min(80,  raw["SCORE_BUY_THRESHOLD"]    + 2.5)
        raw["ENTRY_PULLBACK_MULT"]      = min(2.5, raw["ENTRY_PULLBACK_MULT"]    + 0.5)

    # c) Volume — expanding volume = confirmed moves = relax filter
    raw["ENTRY_VOL_MIN_RATIO"] = float(np.clip(
        raw["ENTRY_VOL_MIN_RATIO"] * max(0.75, 1.0 - 0.15 * (vr - 1.0)), 0.50, 0.95
    ))
    if vr > 1.5:
        raw["INTRADAY_PULSE_WEIGHT"] = min(0.35, raw["INTRADAY_PULSE_WEIGHT"] + 0.05)

    # d) Trend consistency
    if tc > 0.68:
        raw["ENTRY_PULLBACK_MULT"] = max(0.0, raw["ENTRY_PULLBACK_MULT"] * 0.8)
        raw["ENTRY_MIN_QUALITY"]   = max(42,  raw["ENTRY_MIN_QUALITY"]   - 2)
    elif tc < 0.40:
        raw["ENTRY_PULLBACK_MULT"] = min(2.5, raw["ENTRY_PULLBACK_MULT"] * 1.3)
        raw["ENTRY_MIN_QUALITY"]   = min(66,  raw["ENTRY_MIN_QUALITY"]   + 4)

    # e) Stretch — overextended = tighter entry; deeply oversold = lower bypass
    if sig.stretch_from_sma20 > 2.0:
        raw["ENTRY_RSI_IDEAL_MAX"]           = min(raw["ENTRY_RSI_IDEAL_MAX"],           54)
        raw["ENTRY_BOLLINGER_B_MAX"]         = min(raw["ENTRY_BOLLINGER_B_MAX"],         0.45)
        raw["ENTRY_PULLBACK_MULT"]           = min(2.5, raw["ENTRY_PULLBACK_MULT"] * 1.4)
    elif sig.stretch_from_sma20 < -1.5:
        raw["ENTRY_REGIME_BYPASS_MIN_SCORE"] = max(68, raw["ENTRY_REGIME_BYPASS_MIN_SCORE"] - 2)

    # f) Sector breadth spread — high rotation = raise quality bar, widen pool
    if sig.sector_breadth_spread > 0.25:
        raw["ENTRY_MIN_QUALITY"] = min(66, raw["ENTRY_MIN_QUALITY"] + 4)
        raw["SCORE_TOP_N"]       = min(72, raw["SCORE_TOP_N"]       + 10)

    # g) NEW: 52-week high proximity
    # Research (George & Hwang 2004): stocks near 52wk high = 70% of momentum returns.
    # High proximity = buy breakouts aggressively; deep bear = only quality bypass relief.
    if sig.hi52_proximity > 0.55:
        raw["ENTRY_PULLBACK_MULT"]  = max(0.0, raw["ENTRY_PULLBACK_MULT"] * 0.70)
        raw["SCORE_BUY_THRESHOLD"]  = max(61,  raw["SCORE_BUY_THRESHOLD"] - 2.5)
    elif sig.hi52_proximity < 0.10:
        raw["ENTRY_REGIME_BYPASS_MIN_SCORE"] = max(68, raw["ENTRY_REGIME_BYPASS_MIN_SCORE"] - 2)

    # h) NEW: OBV divergence
    # Wyckoff: OBV declining while price holds = distribution (smart money out).
    # OBV rising ahead of price = stealth accumulation.
    if sig.obv_divergence < -0.25:
        raw["SCORE_BUY_THRESHOLD"]  = min(82, raw["SCORE_BUY_THRESHOLD"]  + 3.0)
        raw["SCORE_SELL_THRESHOLD"] = min(54, raw["SCORE_SELL_THRESHOLD"] + 4.0)
    elif sig.obv_divergence > 0.25:
        raw["ENTRY_BULL_RATIO_MIN"] = max(0.02, raw["ENTRY_BULL_RATIO_MIN"] * 0.80)

    # i) NEW: Momentum crash risk
    # Research (Daniel & Moskowitz NBER 2014): 1-month bounce >8% after >20% drawdown
    # = primary momentum crash trigger. Severely restrict new longs.
    if sig.momentum_crash_risk:
        raw["SCORE_BUY_THRESHOLD"]  = min(82,  raw["SCORE_BUY_THRESHOLD"]  + 5.0)
        raw["RISK_MAX_HOLDINGS"]    = min(float(raw["RISK_MAX_HOLDINGS"]),   5.0)
        raw["ENTRY_PULLBACK_MULT"]  = min(2.5,  raw["ENTRY_PULLBACK_MULT"]  + 0.8)
        raw["SCORE_SELL_THRESHOLD"] = min(52,   raw["SCORE_SELL_THRESHOLD"] + 4.0)

    # j) NEW: Extreme chop (both CI and ADX double-confirm)
    # Research: CI>65 + ADX<18 = maximum chop. Mean-reversion only.
    # "No stop is better than fixed stop for mean-reversion" (QuantifiedStrategies).
    if sig.choppiness_index > 65 and sig.adx_avg < 18:
        raw["ENTRY_MIN_QUALITY"]     = min(68,  raw["ENTRY_MIN_QUALITY"]     + 6)
        raw["EXIT_RISK_REWARD_RATIO"]= min(raw["EXIT_RISK_REWARD_RATIO"],     1.5)
        raw["SCORE_SELL_THRESHOLD"]  = min(54,  raw["SCORE_SELL_THRESHOLD"]  + 3.0)

    # ── Step 3: Scenario overrides ────────────────────────────────────────────
    sc = sig.scenario
    if sc != "none" and sc in _SCENARIO_OVERRIDES:
        active: list[dict] = [_SCENARIO_OVERRIDES[sc]]

        # Overlapping near-scenarios → conservative merge
        if sc == "OVERBOUGHT_BULL" and sig.obv_divergence < -0.2 and sig.vol_ratio > 1.0:
            active.append(_SCENARIO_OVERRIDES["DISTRIBUTION"])
        if sc == "DISTRIBUTION" and sig.avg_rsi > 65 and sig.stretch_from_sma20 > 1.5:
            active.append(_SCENARIO_OVERRIDES["OVERBOUGHT_BULL"])
        if sc == "BEAR_RALLY" and sig.momentum_crash_risk:
            # Double danger: momentum crash + bear rally = maximum restriction
            active.append(_SCENARIO_OVERRIDES["DISTRIBUTION"])

        combined = active[0]
        for extra in active[1:]:
            combined = _most_conservative_merge(combined, extra)

        raw.update(combined)

    # ── Finalise: round and apply hard safety clips ───────────────────────────
    t: dict[str, str] = {}
    for k, val in raw.items():
        if k in ("RISK_MAX_HOLDINGS", "SCORE_TOP_N"):
            t[k] = str(int(round(val)))
        else:
            t[k] = str(round(float(val), 3)).rstrip("0").rstrip(".")

    # Hard limits — never go outside these regardless of scenario
    t["ENTRY_BULL_RATIO_MIN"]  = str(round(max(0.02, min(0.50, float(t["ENTRY_BULL_RATIO_MIN"]))), 3))
    t["ENTRY_MIN_QUALITY"]     = str(round(max(38,   min(70,   float(t["ENTRY_MIN_QUALITY"]))),   1))
    t["EXIT_ATR_STOP_MULT"]    = str(round(max(1.0,  min(4.0,  float(t["EXIT_ATR_STOP_MULT"]))),  1))
    t["RISK_MAX_HOLDINGS"]     = str(max(3,  min(18, int(t["RISK_MAX_HOLDINGS"]))))
    t["SCORE_TOP_N"]           = str(max(25, min(75, int(t["SCORE_TOP_N"]))))
    t["INTRADAY_PULSE_WEIGHT"] = str(round(max(0.08, min(0.40, float(t["INTRADAY_PULSE_WEIGHT"]))), 2))
    t["SCORE_BUY_THRESHOLD"]   = str(round(max(60,   min(85,   float(t["SCORE_BUY_THRESHOLD"]))),  1))

    return t


# ─────────────────────────────────────────────────────────────────────────────
#  Profile detection
# ─────────────────────────────────────────────────────────────────────────────

def detect_profile(sig: MarketSignals) -> str:
    sc = sig.scenario
    if sc == "CAPITULATION":                              return "contrarian"
    if sc == "MOMENTUM_ACCEL":                            return "aggressive"
    if sc == "CHOPPY":                                    return "balanced"
    if sc in ("BEAR_RALLY", "DISTRIBUTION", "OVERBOUGHT_BULL"): return "bear-fighter"
    p = sig.regime_pos
    if p >= 2.5:  return "aggressive"
    if p >= 1.75: return "balanced"
    if p >= 1.0:  return "bear-fighter"
    return "contrarian"


# ─────────────────────────────────────────────────────────────────────────────
#  .env I/O
# ─────────────────────────────────────────────────────────────────────────────

def _load_env(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    if not path.exists():
        return result
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        val = val.split(" #")[0].split("\t#")[0].strip()
        result[key.strip()] = val
    return result


def _write_env(path: Path, merged: dict[str, str], profile: str, sig: MarketSignals) -> None:
    lines = path.read_text().splitlines() if path.exists() else []
    written: set[str] = set()
    out: list[str] = []

    for line in lines:
        s = line.strip()
        if s.startswith("#") or not s or "=" not in s:
            out.append(line)
            continue
        key = s.partition("=")[0].strip()
        if key in merged:
            out.append(f"{key}={merged[key]}")
            written.add(key)
        else:
            out.append(line)

    new_keys = [k for k in merged if k not in written]
    if new_keys:
        out.append("")
        out.append(
            f"# ── Auto-tuned: profile={profile}  scenario={sig.scenario}  "
            f"regime={sig.regime_label}  pos={sig.regime_pos:.2f}  "
            f"bull={sig.bull_ratio:.0%}  rsi={sig.avg_rsi:.0f}  "
            f"hi52={sig.hi52_proximity:.0%}  CI={sig.choppiness_index:.0f}  "
            f"ADX={sig.adx_avg:.0f}  atr={sig.volatility_pct:.1f}%  "
            f"mom5d={sig.momentum_5d:+.1f}%  obv_div={sig.obv_divergence:+.2f}"
            + ("  ⚠ CRASH_RISK" if sig.momentum_crash_risk else "")
        )
        for k in new_keys:
            out.append(f"{k}={merged[k]}")

    path.write_text("\n".join(out) + "\n")


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true",
                    help="Print tuned params without writing .env")
    ap.add_argument("--no-tune", action="store_true",
                    help="Apply base profile only, skip fine-tuning")
    ap.add_argument("--profile", choices=VALID_PROFILES, default=None,
                    help="Force base profile (still fine-tuned unless --no-tune)")
    ap.add_argument("--quiet",   action="store_true",
                    help="Suppress per-symbol table")
    args = ap.parse_args()

    env_path = BOT_DIR / ".env"
    if not env_path.exists():
        print(f"ERROR: {env_path} not found."); sys.exit(1)

    current_env = _load_env(env_path)

    if current_env.get("STRATEGY_AUTO_SELECT", "true").lower() == "false" and not args.profile:
        print("STRATEGY_AUTO_SELECT=false — skipping.  Set to 'true' to enable.")
        return

    verbose = not args.quiet
    print("=" * 70)
    print("  select_strategy.py — adaptive strategy selector  (15 signals)")
    print("=" * 70)
    print("\nComputing market signals from cached OHLCV…\n")

    sig = compute_signals(verbose=verbose)

    override = current_env.get("STRATEGY_PROFILE_OVERRIDE", "").strip()
    if args.profile:
        base_profile, source = args.profile, "--profile flag"
    elif override and override in VALID_PROFILES:
        base_profile, source = override, "STRATEGY_PROFILE_OVERRIDE"
    else:
        base_profile, source = detect_profile(sig), "auto-detected"

    print(f"\n  Base profile : {base_profile}  ({source})")

    profile_path = BOT_DIR / "configs" / f".env.{base_profile}"
    if not profile_path.exists():
        print(f"ERROR: {profile_path} not found."); sys.exit(1)

    profile_env = _load_env(profile_path)
    merged = dict(current_env)
    for k, v in profile_env.items():
        if k not in PROTECTED_KEYS:
            merged[k] = v

    if not args.no_tune:
        tuned = fine_tune(sig)
        print(f"\n  {'Parameter':<44} {'Base':>10}  {'Tuned':>10}")
        print(f"  {'─'*44} {'─'*10}  {'─'*10}")
        for k, tv in tuned.items():
            bv   = profile_env.get(k, "—")
            flag = "  ◀" if str(tv) != str(bv) else ""
            print(f"  {k:<44} {str(bv):>10}  {str(tv):>10}{flag}")
        for k, v in tuned.items():
            if k not in PROTECTED_KEYS:
                merged[k] = v
    else:
        print("  Fine-tuning disabled (--no-tune).")

    print(f"\n  ── Key params ──────────────────────────────────────────────────────")
    for k in ["SCORE_BUY_THRESHOLD", "ENTRY_BULL_RATIO_MIN", "ENTRY_MIN_QUALITY",
              "ENTRY_PULLBACK_MULT", "EXIT_ATR_STOP_MULT", "EXIT_RISK_REWARD_RATIO",
              "RISK_MAX_HOLDINGS", "INTRADAY_PULSE_WEIGHT", "SCORE_SELL_THRESHOLD"]:
        print(f"    {k:<44} = {merged.get(k, '—')}")

    if args.dry_run:
        print(f"\n  [dry-run] .env NOT modified.")
    else:
        _write_env(env_path, merged, base_profile, sig)
        print(f"\n✓ .env updated — profile={base_profile}  "
              f"scenario={sig.scenario}  regime={sig.regime_label}  "
              f"pos={sig.regime_pos:.2f}/3.0"
              + ("  ⚠ MOMENTUM_CRASH_RISK" if sig.momentum_crash_risk else ""))


if __name__ == "__main__":
    main()
