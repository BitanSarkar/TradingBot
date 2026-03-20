"""
scoring/formulas/fundamental.py — Industry-standard fundamental ratio scores.

Each function takes (fundamentals: dict) and returns float [0, 100].

Metrics implemented
-------------------
P/E Ratio         — price vs earnings (lower is better, sector-relative)
P/B Ratio         — price vs book value
ROE               — return on equity (higher is better)
Debt-to-Equity    — leverage risk (lower is better)
Current Ratio     — liquidity
Revenue Growth    — top-line momentum
Earnings Growth   — bottom-line momentum
Profit Margin     — operational efficiency
Dividend Yield    — income component
Beta              — systematic risk (close to 1.0 is neutral)
52-Week Position  — where price sits in its annual range

FundamentalScorer assembles these into one score using configurable weights.
"""

from __future__ import annotations

import logging
import math
from typing import Optional

log = logging.getLogger("FundamentalScorer")


def _safe(val, default: float = 50.0) -> float:
    if val is None:
        return default
    try:
        v = float(val)
        return default if math.isnan(v) or math.isinf(v) else v
    except (TypeError, ValueError):
        return default


def _clamp(v: float) -> float:
    return max(0.0, min(100.0, v))


# ============================================================
# Individual ratio scorers — all return float [0, 100]
# ============================================================

def pe_score(fund: dict, sector: str = "DEFAULT") -> float:
    """
    P/E ratio score.  Ideal P/E varies by sector.
    Lower P/E → higher score; very high P/E → low score.
    """
    # Sector-specific ideal P/E ranges (min, ideal, max)
    sector_pe = {
        "IT":       (15, 25, 60),
        "BANKING":  (8,  14, 30),
        "PHARMA":   (15, 25, 50),
        "FMCG":     (25, 40, 80),
        "AUTO":     (10, 18, 40),
        "METAL":    (5,  12, 25),
        "REALTY":   (8,  20, 50),
        "DEFAULT":  (10, 20, 50),
    }
    lo, ideal, hi = sector_pe.get(sector.upper(), sector_pe["DEFAULT"])
    pe = _safe(fund.get("trailingPE"), default=None)
    if pe is None or pe <= 0:
        return 50.0   # unknown — neutral

    if pe <= ideal:
        # Below ideal: linearly from 100 at lo → 80 at ideal
        return _clamp(100 - (pe - lo) / max(ideal - lo, 1) * 20)
    else:
        # Above ideal: drops off as P/E rises past ideal
        return _clamp(80 - (pe - ideal) / max(hi - ideal, 1) * 80)


def pb_score(fund: dict) -> float:
    """
    P/B ratio score.
      • P/B < 1  → potential deep value → high score
      • P/B 1–3  → fair value range
      • P/B > 5  → expensive → low score
    """
    pb = _safe(fund.get("priceToBook"), default=None)
    if pb is None or pb <= 0:
        return 50.0
    if pb < 1.0:
        return 90.0
    if pb <= 3.0:
        return _clamp(90 - (pb - 1) / 2 * 40)   # 90 → 50
    return _clamp(50 - (pb - 3) / 5 * 50)        # 50 → 0 at P/B=8


def roe_score(fund: dict) -> float:
    """
    Return on Equity.
      • ROE > 20% → excellent → high score
      • ROE 10–20% → good
      • ROE < 5%   → poor → low score
      • Negative ROE → very low
    """
    roe = _safe(fund.get("returnOnEquity"), default=None)
    if roe is None:
        return 50.0
    roe_pct = roe * 100
    if roe_pct < 0:
        return max(0, 30 + roe_pct * 2)  # negative ROE penalty
    return _clamp(roe_pct * 4)            # 25% ROE → 100 score


def debt_equity_score(fund: dict) -> float:
    """
    Debt-to-Equity score (lower is safer → higher score).
    Banks/NBFCs naturally carry high D/E so this may be overridden in sector scorers.
    """
    de = _safe(fund.get("debtToEquity"), default=None)
    if de is None:
        return 50.0
    if de < 0:
        return 50.0   # unusual — neutral
    # D/E 0 → 100; D/E 1 → 70; D/E 3 → 30; D/E 5+ → ~5
    return _clamp(100 - de * 19)


def current_ratio_score(fund: dict) -> float:
    """
    Current Ratio (liquidity). Ideal: 1.5–3.0.
      • < 1.0 → risky   → low score
      • 1.5–3 → healthy → high score
      • > 5   → may indicate hoarding (neutral)
    """
    cr = _safe(fund.get("currentRatio"), default=None)
    if cr is None:
        return 50.0
    if cr < 1.0:
        return _clamp(cr * 40)          # 0 → 0, 1 → 40
    if cr <= 3.0:
        return _clamp(40 + (cr - 1) / 2 * 55)   # 40 → 95
    return _clamp(95 - (cr - 3) * 5)   # slight drop for very high ratio


def revenue_growth_score(fund: dict) -> float:
    """
    Revenue growth YoY.
      • > 30% → 100
      • ~10%  → 70
      • 0%    → 40
      • Negative → penalty
    """
    rg = _safe(fund.get("revenueGrowth"), default=None)
    if rg is None:
        return 50.0
    pct = rg * 100
    return _clamp(40 + pct * 2)


def earnings_growth_score(fund: dict) -> float:
    """EPS growth YoY.  Similar to revenue growth but earnings-focused."""
    eg = _safe(fund.get("earningsGrowth"), default=None)
    if eg is None:
        return 50.0
    pct = eg * 100
    return _clamp(40 + pct * 2)


def profit_margin_score(fund: dict) -> float:
    """
    Net profit margin.
      • > 20%   → 90–100
      • 10–20%  → 70–90
      • 5–10%   → 50–70
      • < 5%    → 0–50
    """
    pm = _safe(fund.get("profitMargins"), default=None)
    if pm is None:
        return 50.0
    pct = pm * 100
    return _clamp(pct * 4)


def dividend_yield_score(fund: dict) -> float:
    """
    Dividend yield.  Moderate dividend = good; zero or extreme = neutral.
      • 2–5% yield → 70–90 (sweet spot)
      • 0%         → 40  (no income)
      • >8%        → suspect (yield trap) → drops back
    """
    dy = _safe(fund.get("dividendYield"), default=None)
    if dy is None or dy == 0:
        return 40.0
    pct = dy * 100
    if pct <= 5.0:
        return _clamp(40 + pct * 10)   # 0% → 40; 5% → 90
    return _clamp(90 - (pct - 5) * 10) # drops back above 5%


def beta_score(fund: dict) -> float:
    """
    Beta score (systematic risk).  Beta close to 1 = market-like = neutral.
    Conservative strategies prefer low beta; aggressive ones may prefer > 1.
    Default: penalise extremes in both directions.
    """
    beta = _safe(fund.get("beta"), default=None)
    if beta is None:
        return 50.0
    # Ideal beta range: 0.8–1.3 → score ~70
    return _clamp(100 - abs(beta - 1.0) * 35)


def fifty_two_week_position_score(fund: dict) -> float:
    """
    Where is the stock relative to its 52-week high/low?
    Lower in range → potential value → higher score (mean-reversion lens).
    """
    hi  = _safe(fund.get("fiftyTwoWeekHigh"), default=None)
    lo  = _safe(fund.get("fiftyTwoWeekLow"),  default=None)
    avg = _safe(fund.get("fiftyDayAverage"),  default=None)

    if hi is None or lo is None or avg is None or hi == lo:
        return 50.0

    position = (avg - lo) / (hi - lo)  # 0 = at 52w low, 1 = at 52w high
    return _clamp((1 - position) * 100)


# ============================================================
# FundamentalScorer — assembles ratios into one score
# ============================================================

class FundamentalScorer:
    """
    Combines all fundamental metrics into a weighted score [0, 100].

    Default weights:
        pe          15%
        pb          10%
        roe         15%
        debt_equity 12%
        curr_ratio   8%
        rev_growth  12%
        earn_growth 12%
        margin      10%
        dividend     6%
    """

    DEFAULT_WEIGHTS = {
        "pe":          0.15,
        "pb":          0.10,
        "roe":         0.15,
        "debt_equity": 0.12,
        "curr_ratio":  0.08,
        "rev_growth":  0.12,
        "earn_growth": 0.12,
        "margin":      0.10,
        "dividend":    0.06,
    }

    def __init__(self) -> None:
        self._weights = dict(self.DEFAULT_WEIGHTS)

    def set_weights(self, **kwargs: float) -> "FundamentalScorer":
        for k, v in kwargs.items():
            if k in self._weights:
                self._weights[k] = float(v)
        total = sum(self._weights.values())
        if total > 0:
            for k in self._weights:
                self._weights[k] /= total
        return self

    def compute(self, fund: dict, sector: str = "DEFAULT") -> tuple[float, dict]:
        """Returns (composite_score, components_dict)."""
        if not fund:
            return 50.0, {}

        metrics = {
            "pe":          pe_score(fund, sector),
            "pb":          pb_score(fund),
            "roe":         roe_score(fund),
            "debt_equity": debt_equity_score(fund),
            "curr_ratio":  current_ratio_score(fund),
            "rev_growth":  revenue_growth_score(fund),
            "earn_growth": earnings_growth_score(fund),
            "margin":      profit_margin_score(fund),
            "dividend":    dividend_yield_score(fund),
        }

        composite = sum(metrics[k] * self._weights.get(k, 0) for k in metrics)
        return round(composite, 2), metrics
