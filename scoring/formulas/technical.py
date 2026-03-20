"""
scoring/formulas/technical.py — Industry-standard technical indicator scores.

Each function takes (df: pd.DataFrame) and returns a float in [0, 100].

Indicators implemented
----------------------
RSI         — momentum oscillator (standard 14-period)
MACD        — trend / momentum convergence-divergence
BB Position — where price sits within Bollinger Bands
SMA Cross   — 50-day vs 200-day golden/death cross
Volume Trend— OBV trend and relative volume
ATR Ratio   — volatility normalised to price (lower = calmer)
Price vs VWAP — intraday positioning relative to volume-weighted avg price

TechnicalScorer assembles these into one score using configurable weights.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger("TechnicalScorer")


# ============================================================
# Individual indicator functions — all return float [0, 100]
# ============================================================

def rsi_score(df: pd.DataFrame, period: int = 14) -> float:
    """
    RSI score:
      • RSI > 70 → overbought → low score (momentum exhaustion)
      • RSI < 30 → oversold   → high score (potential reversal)
      • RSI 40–60 → neutral   → 50
    For trend-following strategies, you may invert this.
    """
    if len(df) < period + 1:
        return 50.0
    close = df["Close"].astype(float)
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    latest = rsi.iloc[-1]
    if pd.isna(latest):
        return 50.0
    # Oversold → score near 80; overbought → score near 20; neutral → 50
    if latest <= 30:
        return 70 + (30 - latest)           # oversold bonus
    if latest >= 70:
        return max(0.0, 70 - (latest - 70)) # overbought penalty
    return 50.0 + (50 - latest) * 0.2       # slight mean-reversion tilt


def macd_score(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> float:
    """
    MACD histogram score:
      • Positive and growing  → strong bullish  → ~80–100
      • Positive but shrinking→ weakening bull  → ~60–80
      • Negative and shrinking→ bear easing off → ~40–60
      • Negative and growing  → strong bearish  → ~0–40
    """
    if len(df) < slow + signal:
        return 50.0
    close = df["Close"].astype(float)
    ema_fast   = close.ewm(span=fast,   adjust=False).mean()
    ema_slow   = close.ewm(span=slow,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram  = macd_line - signal_line

    curr = histogram.iloc[-1]
    prev = histogram.iloc[-2] if len(histogram) > 1 else curr
    if pd.isna(curr) or pd.isna(prev):
        return 50.0

    # Normalise histogram to recent range
    std = histogram.rolling(50).std().iloc[-1]
    if pd.isna(std) or std == 0:
        std = abs(curr) if curr != 0 else 1.0
    normalised = np.tanh(curr / std)  # maps to (-1, +1)
    trend_up   = curr > prev

    base = 50 + normalised * 40       # -40 to +40 from zero-cross
    bonus = 5 if trend_up else -5     # momentum direction bonus
    return float(np.clip(base + bonus, 0, 100))


def bollinger_score(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> float:
    """
    Bollinger Band position score:
      • Price near lower band → oversold → high score (buy signal)
      • Price near upper band → overbought → low score
      • Price near middle     → neutral (50)
    """
    if len(df) < period:
        return 50.0
    close = df["Close"].astype(float)
    sma   = close.rolling(period).mean()
    std   = close.rolling(period).std()
    upper = sma + std_dev * std
    lower = sma - std_dev * std

    latest_close = close.iloc[-1]
    upper_val    = upper.iloc[-1]
    lower_val    = lower.iloc[-1]
    band_width   = upper_val - lower_val

    if pd.isna(band_width) or band_width == 0:
        return 50.0

    # Position 0 = at lower band, 1 = at upper band
    position = (latest_close - lower_val) / band_width
    position = float(np.clip(position, 0, 1))
    # Lower position → higher score (mean-reversion)
    return round((1 - position) * 100, 2)


def sma_crossover_score(df: pd.DataFrame, short: int = 50, long: int = 200) -> float:
    """
    Golden/Death cross score:
      • Golden cross (50 > 200) and widening → 70–90
      • Golden cross but narrowing           → 55–70
      • Death cross (50 < 200) and widening  → 10–30
      • Death cross but recovering           → 30–45
    """
    if len(df) < long + 5:
        return 50.0
    close = df["Close"].astype(float)
    sma_short = close.rolling(short).mean()
    sma_long  = close.rolling(long).mean()

    curr_diff = sma_short.iloc[-1] - sma_long.iloc[-1]
    prev_diff = sma_short.iloc[-5] - sma_long.iloc[-5]  # 5-day trend

    if pd.isna(curr_diff) or pd.isna(prev_diff):
        return 50.0

    # Normalise difference as % of long SMA
    pct_diff = curr_diff / sma_long.iloc[-1] * 100 if sma_long.iloc[-1] != 0 else 0.0
    trending_up = curr_diff > prev_diff

    base = 50 + np.tanh(pct_diff / 3) * 40  # sigmoid-like mapping
    bonus = 5 if trending_up else -5
    return float(np.clip(base + bonus, 0, 100))


def volume_trend_score(df: pd.DataFrame, period: int = 20) -> float:
    """
    Volume quality score using OBV trend and relative volume.
      • Rising price + rising volume  → strong  → ~70–90
      • Rising price + falling volume → weak     → ~40–60
      • Falling price + rising volume → bearish  → ~20–40
    """
    if len(df) < period + 1:
        return 50.0
    close  = df["Close"].astype(float)
    volume = df["Volume"].astype(float)

    # OBV
    direction = np.sign(close.diff().fillna(0))
    obv = (direction * volume).cumsum()
    obv_sma = obv.rolling(period).mean()
    obv_trend = obv.iloc[-1] - obv_sma.iloc[-1]

    # Relative volume
    rel_vol = volume.iloc[-1] / volume.rolling(period).mean().iloc[-1]
    if pd.isna(rel_vol):
        rel_vol = 1.0

    price_up = close.iloc[-1] > close.rolling(period).mean().iloc[-1]
    obv_up   = obv_trend > 0

    if price_up and obv_up:
        base = 70 + min(rel_vol - 1, 1) * 20   # up to 90 for heavy volume
    elif price_up and not obv_up:
        base = 45 + min(rel_vol - 1, 0.5) * 10
    elif not price_up and obv_up:
        base = 35
    else:
        base = 20 + min(rel_vol - 1, 0.5) * 5  # deep bearish

    return float(np.clip(base, 0, 100))


def atr_score(df: pd.DataFrame, period: int = 14) -> float:
    """
    ATR as % of price.  Lower volatility → higher score (calmer stock).
    This is a risk metric — high volatility stocks score lower here.
    """
    if len(df) < period + 1:
        return 50.0
    high  = df["High"].astype(float)
    low   = df["Low"].astype(float)
    close = df["Close"].astype(float)

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)

    atr = tr.rolling(period).mean().iloc[-1]
    atr_pct = atr / close.iloc[-1] * 100 if close.iloc[-1] != 0 else 5.0

    if pd.isna(atr_pct):
        return 50.0

    # atr_pct: 0–2% → calm (score 70–90); >5% → volatile (score 10–30)
    return float(np.clip(100 - atr_pct * 15, 0, 100))


def price_momentum_score(df: pd.DataFrame) -> float:
    """
    Multi-timeframe price momentum:
      1-month (21d), 3-month (63d), 6-month (126d) returns, equally weighted.
    """
    close = df["Close"].astype(float)
    scores = []
    for window in [21, 63, 126]:
        if len(close) > window:
            ret = (close.iloc[-1] / close.iloc[-window] - 1) * 100
            # Map -20% → 0, +20% → 100, linear
            s = np.clip(50 + ret * 2.5, 0, 100)
            scores.append(float(s))
    return float(np.mean(scores)) if scores else 50.0


# ============================================================
# TechnicalScorer — assembles the above into one score
# ============================================================

class TechnicalScorer:
    """
    Combines all technical metrics into a weighted score [0, 100].

    Default weights (adjustable):
        rsi          15%
        macd         20%
        bollinger    15%
        sma_cross    20%
        volume       15%
        momentum     15%

    Example usage (standalone):
        ts = TechnicalScorer()
        ts.set_weights(macd=0.30, rsi=0.20, sma_cross=0.25,
                       bollinger=0.10, volume=0.10, momentum=0.05)
        score = ts.compute(df)  # returns float 0-100
    """

    DEFAULT_WEIGHTS = {
        "rsi":       0.15,
        "macd":      0.20,
        "bollinger": 0.15,
        "sma_cross": 0.20,
        "volume":    0.15,
        "momentum":  0.15,
    }

    def __init__(self) -> None:
        self._weights = dict(self.DEFAULT_WEIGHTS)

    def set_weights(self, **kwargs: float) -> "TechnicalScorer":
        for k, v in kwargs.items():
            if k in self._weights:
                self._weights[k] = float(v)
        # Normalise
        total = sum(self._weights.values())
        if total > 0:
            for k in self._weights:
                self._weights[k] /= total
        return self

    def compute(self, df: pd.DataFrame) -> tuple[float, dict]:
        """
        Returns (composite_score, components_dict).
        components_dict maps metric name → individual score.
        """
        if df is None or len(df) < 30:
            return 50.0, {}

        metrics = {
            "rsi":       rsi_score(df),
            "macd":      macd_score(df),
            "bollinger": bollinger_score(df),
            "sma_cross": sma_crossover_score(df),
            "volume":    volume_trend_score(df),
            "momentum":  price_momentum_score(df),
        }

        composite = sum(metrics[k] * self._weights.get(k, 0) for k in metrics)
        return round(composite, 2), metrics
