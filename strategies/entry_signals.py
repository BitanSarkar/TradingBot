"""
strategies/entry_signals.py — Statistically-derived optimal entry timing.

The problem with "score >= 70 → buy immediately at market":
  - A stock scoring 71 that was at 85 two ticks ago is FALLING — you're buying
    into declining momentum, likely chasing a peak.
  - A stock scoring 71 that was at 60 two ticks ago is RISING — you're entering
    early in a developing move, which maximises the entry-to-exit profit window.
  - Buying at the current market price when the stock is already extended
    (overbought RSI, near upper Bollinger band, above average volume) leaves
    almost no room between your entry and the exit triggers.

This module computes a composite entry quality index (0–100) before every BUY
signal and either:
  - Approves immediately at market (high quality, strong confirmation)
  - Suggests a limit order below current price (medium quality — wait for a
    small pullback to improve the entry price)
  - Rejects (low quality — this tick's setup is poor; wait for next tick)

Five research-backed components, each scored 0–100:

1. SCORE VELOCITY & ACCELERATION  (Jegadeesh & Titman, 1993)
   ─────────────────────────────────────────────────────────
   Compute the slope (velocity) and curvature (acceleration) of the composite
   score over the last N ticks using ordinary least squares.
   - Positive slope → score is trending up → early in the move  ✓
   - Negative slope → score is falling through the threshold → avoid  ✗
   - Positive acceleration → improvement is speeding up → highest quality  ✓

2. PRICE ENTRY QUALITY  (Bollinger 1992, Wilder 1978)
   ────────────────────────────────────────────────────
   Measures whether the current price is at a favourable entry point:
   - RSI at entry: RSI < 50 = oversold with improving score = ideal entry
     RSI > 65 = overbought = chasing; too late for maximum profit
   - Bollinger %B: %B < 0.4 = buying near lower band = maximum upside
     %B > 0.7 = extended = little room left before mean reversion
   - Price vs ATR support: how close is the price to a natural support level?
     Buying near support gives a tight stop, which improves the R:R ratio.

3. VOLUME CONFIRMATION  (Granville, 1963; Wyckoff, 1930s)
   ──────────────────────────────────────────────────────
   High volume confirms institutional participation behind the move.
   The Wyckoff method emphasises that genuine breakouts occur on expanding
   volume; low-volume moves are likely to reverse.
   - vol_ratio > 1.2 and trending up: institutions accumulating  ✓
   - vol_ratio < 0.8 and flat: retail noise, no conviction  ✗

4. MARKET REGIME  (Lo & MacKinlay, 1988; Faber, 2007)
   ────────────────────────────────────────────────────
   Buying individual stocks in a bear market dramatically reduces win rate.
   Mebane Faber's tactical asset allocation research showed that a simple
   rule — only buy when price > SMA200 — filters out most bear market losses.
   We use the UNIVERSE's current score distribution as a real-time regime proxy:
   - bull_ratio = fraction of all NSE stocks with composite score > 50
   - High bull_ratio → broad market participation → favourable  ✓
   - Low bull_ratio → most stocks deteriorating → unfavourable  ✗

5. OPTIMAL ENTRY PRICE  (Van Tharp, 1998 — "Trade Your Way to Financial Freedom")
   ───────────────────────────────────────────────────────────────────────────────
   Van Tharp's core principle: the entry price determines R (risk per trade).
   Smaller R → larger R:R multiple from same exit → more profit per unit of risk.
   Strategy: after a stock qualifies, don't buy at the ask immediately.
   Place a limit order at `current_price − entry_pullback_mult × ATR`.
   Small intraday pullbacks happen frequently (60–70% of qualifying setups).
   When the limit fills, your stop_loss is the same distance in ₹ but the
   take-profit is further away → larger realised profit.
   If the limit does NOT fill within entry_limit_timeout_ticks, cancel and
   accept market price (don't miss a genuine breakout just for a few rupees).

References:
  Wilder, J.W. (1978). New Concepts in Technical Trading Systems.
  Granville, J. (1963). Granville's New Key to Stock Market Profits.
  Jegadeesh, N. & Titman, S. (1993). Returns to Buying Winners. JoF.
  Wyckoff, R. (1931). Stock Market Technique. Wyckoff Associates.
  Van Tharp, R. (1998). Trade Your Way to Financial Freedom. McGraw-Hill.
  Faber, M. (2007). A Quantitative Approach to Tactical Asset Allocation. JOIM.
  Lo, A. & MacKinlay, C. (1988). Stock Market Prices Do Not Follow Random Walks. RFS.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Sequence

import numpy as np
import pandas as pd

log = logging.getLogger("EntrySignals")


# ── Output dataclass ──────────────────────────────────────────────────────────

@dataclass
class EntryQuality:
    """
    Result of entry quality analysis for one BUY candidate.

    qualified       : bool   — True = generate signal this tick
    entry_price     : float  — suggested limit price (0 = use market order)
    use_limit       : bool   — True = place LIMIT at entry_price; False = MARKET
    quality_score   : float  — composite 0–100
    score_velocity  : float  — score pts/tick (positive = improving)
    score_accel     : float  — velocity trend (positive = accelerating)
    velocity_score  : float  — 0–100 sub-score
    price_score     : float  — 0–100 sub-score (entry price quality)
    volume_score    : float  — 0–100 sub-score
    regime_score    : float  — 0–100 sub-score
    atr             : float  — ATR used for limit price calculation
    reason          : str    — human-readable explanation
    """
    qualified:      bool
    entry_price:    float
    use_limit:      bool
    quality_score:  float
    score_velocity: float
    score_accel:    float
    velocity_score: float  = 0.0
    price_score:    float  = 0.0
    volume_score:   float  = 0.0
    regime_score:   float  = 0.0
    atr:            float  = 0.0
    reason:         str    = ""


# ── Score history tracker (maintained by the strategy across ticks) ───────────

class ScoreHistory:
    """
    Maintains a rolling window of composite scores per symbol.
    Call update() every tick after scoring.
    """
    def __init__(self, window: int = 10):
        self._window = window
        self._history: dict[str, deque[float]] = {}

    def update(self, symbol: str, score: float) -> None:
        if symbol not in self._history:
            self._history[symbol] = deque(maxlen=self._window)
        self._history[symbol].append(score)

    def get(self, symbol: str) -> list[float]:
        return list(self._history.get(symbol, []))

    def update_batch(self, scores: list) -> None:
        """Pass the full scores list from the scoring engine each tick."""
        for s in scores:
            self.update(s.symbol, s.composite)


# ── Core computation ──────────────────────────────────────────────────────────

def compute_entry_quality(
    df:                     pd.DataFrame,
    current_score:          float,
    score_history:          list[float],         # last N composites for this symbol
    universe_scores:        list[float],         # all composites this tick (regime)
    current_ltp:            float,
    *,
    # Velocity / acceleration
    min_score_velocity:     float = 0.0,         # reject if score falling
    velocity_window:        int   = 5,           # ticks for velocity regression
    # Price entry quality
    rsi_ideal_max:          float = 55.0,        # RSI below this = good entry
    bollinger_b_ideal_max:  float = 0.55,        # %B below this = good entry
    # Volume
    vol_min_ratio:          float = 0.8,         # reject if vol < 80% of avg
    # Market regime
    bull_ratio_min:         float = 0.40,        # reject if < 40% of stocks bullish
    # Entry price
    atr_period:             int   = 14,
    entry_pullback_mult:    float = 0.5,         # limit = ltp − mult × ATR
    # Quality gate
    min_quality_score:      float = 55.0,        # reject below this
    # Sub-weights (must sum to 1.0)
    w_velocity:             float = 0.30,
    w_price:                float = 0.35,
    w_volume:               float = 0.15,
    w_regime:               float = 0.20,
) -> EntryQuality:
    """
    Compute entry quality for a single BUY candidate.

    Parameters
    ----------
    df              : OHLCV DataFrame (Open, High, Low, Close, Volume).
    current_score   : composite score this tick.
    score_history   : list of composite scores from previous ticks (oldest first).
    universe_scores : all symbols' composite scores this tick (for regime).
    current_ltp     : live last traded price.
    """

    # ── Guard: not enough data ────────────────────────────────────────────────
    if df is None or len(df) < max(atr_period + 2, 21):
        return EntryQuality(
            qualified=False, entry_price=current_ltp, use_limit=False,
            quality_score=0.0, score_velocity=0.0, score_accel=0.0,
            reason="insufficient OHLCV history",
        )

    close  = df["Close"].astype(float)
    high   = df["High"].astype(float)
    low    = df["Low"].astype(float)
    volume = df["Volume"].astype(float)

    # ─────────────────────────────────────────────────────────────────────────
    # 1. SCORE VELOCITY & ACCELERATION
    # ─────────────────────────────────────────────────────────────────────────
    all_scores = list(score_history) + [current_score]
    velocity   = 0.0
    accel      = 0.0

    if len(all_scores) >= 3:
        n    = min(velocity_window, len(all_scores))
        vals = np.array(all_scores[-n:], dtype=float)
        x    = np.arange(n, dtype=float)

        # Linear regression slope = velocity
        coeffs_lin  = np.polyfit(x, vals, 1)
        velocity    = float(coeffs_lin[0])   # score pts per tick

        # Quadratic fit — second-order coefficient × 2 = curvature = acceleration
        if n >= 4:
            coeffs_quad = np.polyfit(x, vals, 2)
            accel       = float(coeffs_quad[0]) * 2.0   # d²score/dt²

    # Score: positive velocity up to ~+5 pts/tick = full score
    #        negative velocity = poor or zero
    if velocity < min_score_velocity:
        # Score is falling through the threshold → reject outright
        return EntryQuality(
            qualified=False, entry_price=current_ltp, use_limit=False,
            quality_score=0.0, score_velocity=velocity, score_accel=accel,
            reason=f"score declining (velocity={velocity:+.2f} pts/tick)",
        )

    # Map velocity to 0–100: 0 pts/tick → 50, +5 pts/tick → 100, < 0 → 0
    velocity_raw   = max(0.0, min(velocity, 5.0))
    velocity_score = 50.0 + velocity_raw * 10.0   # 0 → 50, 5 → 100

    # Acceleration bonus: rising velocity gets a nudge; decelerating gets penalised
    if accel > 0:
        velocity_score = min(100.0, velocity_score + accel * 5.0)
    else:
        velocity_score = max(0.0, velocity_score + accel * 3.0)

    # ─────────────────────────────────────────────────────────────────────────
    # 2. PRICE ENTRY QUALITY
    # ─────────────────────────────────────────────────────────────────────────

    # 2a. RSI (14-day)
    delta     = close.diff()
    gain      = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss      = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    rs        = gain / loss.replace(0, np.nan)
    rsi       = float((100 - 100 / (1 + rs)).iloc[-1])
    if np.isnan(rsi):
        rsi = 50.0

    # RSI < 40 = ideal oversold entry → score 100
    # RSI 40–55 = acceptable → score 70–100
    # RSI 55–70 = extended → score 20–70
    # RSI > 70 = overbought, chasing → score 0
    if rsi <= 40:
        rsi_score = 100.0
    elif rsi <= rsi_ideal_max:
        rsi_score = 70.0 + (rsi_ideal_max - rsi) / (rsi_ideal_max - 40) * 30.0
    elif rsi <= 70:
        rsi_score = 70.0 * (70.0 - rsi) / (70.0 - rsi_ideal_max)
    else:
        rsi_score = 0.0

    # 2b. Bollinger %B
    sma20    = close.rolling(20).mean()
    std20    = close.rolling(20).std(ddof=1)
    upper    = sma20 + 2 * std20
    lower    = sma20 - 2 * std20
    pct_b    = float(((close - lower) / (upper - lower)).iloc[-1])
    if np.isnan(pct_b):
        pct_b = 0.5

    # %B < 0.2 = near lower band → ideal → score 100
    # %B 0.2–0.55 = acceptable → score 60–100
    # %B > 0.55 = extended → score 0–60
    if pct_b <= 0.2:
        bb_score = 100.0
    elif pct_b <= bollinger_b_ideal_max:
        bb_score = 60.0 + (bollinger_b_ideal_max - pct_b) / (bollinger_b_ideal_max - 0.2) * 40.0
    elif pct_b <= 1.0:
        bb_score = 60.0 * (1.0 - pct_b) / (1.0 - bollinger_b_ideal_max)
    else:
        bb_score = 0.0

    # 2c. ATR and distance from ATR-based support
    prev_close = close.shift(1)
    tr         = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr        = float(tr.ewm(alpha=1/atr_period, adjust=False).mean().iloc[-1])

    # Distance from SMA20 (dynamic support) — closer = better entry
    sma20_val    = float(sma20.iloc[-1]) if not np.isnan(sma20.iloc[-1]) else current_ltp
    dist_support = current_ltp - sma20_val   # positive = above SMA, negative = below

    # Buying near or below SMA20 = great entry.  Far above = chasing.
    # Normalise by ATR: dist_support / ATR → 0 = at SMA, 2 = 2 ATRs above SMA
    if atr > 0:
        dist_ratio = dist_support / atr
    else:
        dist_ratio = 0.0

    # dist_ratio ≤ 0 (at/below SMA) = score 100
    # dist_ratio = 1 ATR above SMA = score 60
    # dist_ratio = 2 ATRs above SMA = score 20
    # dist_ratio > 3 ATRs = score 0
    support_score = max(0.0, 100.0 - max(0.0, dist_ratio) * 40.0)

    # Blend price sub-scores
    price_score = 0.40 * rsi_score + 0.35 * bb_score + 0.25 * support_score

    # ─────────────────────────────────────────────────────────────────────────
    # 3. VOLUME CONFIRMATION
    # ─────────────────────────────────────────────────────────────────────────
    vol_avg20  = float(volume.rolling(20).mean().iloc[-1])
    vol_today  = float(volume.iloc[-1])
    vol_ratio  = vol_today / vol_avg20 if vol_avg20 > 0 else 1.0

    if vol_ratio < vol_min_ratio:
        # Volume too thin — signal lacks institutional confirmation
        return EntryQuality(
            qualified=False, entry_price=current_ltp, use_limit=False,
            quality_score=0.0, score_velocity=velocity, score_accel=accel,
            velocity_score=velocity_score, price_score=price_score,
            atr=atr,
            reason=f"volume too thin (ratio={vol_ratio:.2f} < {vol_min_ratio:.2f})",
        )

    # Volume trend: slope over last 5 days
    recent_vol = volume.tail(5).values.astype(float)
    if len(recent_vol) >= 3:
        vol_slope = float(np.polyfit(np.arange(len(recent_vol)), recent_vol, 1)[0])
        vol_trend_bonus = 10.0 if vol_slope > 0 else 0.0
    else:
        vol_trend_bonus = 0.0

    # vol_ratio 1.0 → score 60.  2.0 → score 90.  < 0.8 → already rejected.
    volume_score = min(100.0, 50.0 + (vol_ratio - 1.0) * 40.0 + vol_trend_bonus)
    volume_score = max(0.0, volume_score)

    # ─────────────────────────────────────────────────────────────────────────
    # 4. MARKET REGIME
    # ─────────────────────────────────────────────────────────────────────────
    regime_score = 50.0
    if universe_scores:
        bull_ratio  = sum(1 for s in universe_scores if s > 50) / len(universe_scores)
        avg_score   = float(np.mean(universe_scores))

        if bull_ratio < bull_ratio_min:
            return EntryQuality(
                qualified=False, entry_price=current_ltp, use_limit=False,
                quality_score=0.0, score_velocity=velocity, score_accel=accel,
                velocity_score=velocity_score, price_score=price_score,
                volume_score=volume_score, atr=atr,
                reason=f"unfavourable market regime (bull_ratio={bull_ratio:.0%})",
            )

        # bull_ratio 0.4 → score 0; 0.5 → 50; 0.65 → 100
        regime_score = min(100.0, max(0.0, (bull_ratio - 0.40) / 0.25 * 100.0))

        # Bonus if the overall universe average score is high
        if avg_score > 55:
            regime_score = min(100.0, regime_score + (avg_score - 55) * 1.5)

    # ─────────────────────────────────────────────────────────────────────────
    # 5. COMPOSITE ENTRY QUALITY
    # ─────────────────────────────────────────────────────────────────────────
    quality = (
        w_velocity * velocity_score
      + w_price    * price_score
      + w_volume   * volume_score
      + w_regime   * regime_score
    )

    if quality < min_quality_score:
        return EntryQuality(
            qualified=False, entry_price=current_ltp, use_limit=False,
            quality_score=quality, score_velocity=velocity, score_accel=accel,
            velocity_score=velocity_score, price_score=price_score,
            volume_score=volume_score, regime_score=regime_score, atr=atr,
            reason=(
                f"quality={quality:.1f} < threshold={min_quality_score:.0f} "
                f"(vel={velocity_score:.0f} price={price_score:.0f} "
                f"vol={volume_score:.0f} regime={regime_score:.0f})"
            ),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # 6. OPTIMAL ENTRY PRICE
    # ─────────────────────────────────────────────────────────────────────────
    # High quality (≥ 80): enter at market — don't risk missing the move
    # Medium quality (55–80): set a limit order slightly below for better fill
    if quality >= 80.0 or entry_pullback_mult == 0:
        use_limit   = False
        entry_price = current_ltp
    else:
        # Limit price = current LTP − pullback_mult × ATR
        # The pullback_mult scales with quality — better quality = smaller pullback target
        # (you're more confident it's already the right entry)
        quality_factor = 1.0 - (quality - 55.0) / 25.0   # 1.0 at quality=55, 0 at quality=80
        adjusted_mult  = entry_pullback_mult * quality_factor
        entry_price    = current_ltp - adjusted_mult * atr
        use_limit      = True

    reason = (
        f"quality={quality:.1f}/100 ✓ "
        f"| vel={velocity:+.2f}pt/tick (score={velocity_score:.0f}) "
        f"| price_score={price_score:.0f} (RSI={rsi:.0f} %B={pct_b:.2f}) "
        f"| vol={vol_ratio:.2f}x avg (score={volume_score:.0f}) "
        f"| regime={regime_score:.0f} "
        f"| {'LIMIT @₹' + str(round(entry_price,2)) if use_limit else 'MARKET'}"
    )

    return EntryQuality(
        qualified=True,
        entry_price=entry_price,
        use_limit=use_limit,
        quality_score=quality,
        score_velocity=velocity,
        score_accel=accel,
        velocity_score=velocity_score,
        price_score=price_score,
        volume_score=volume_score,
        regime_score=regime_score,
        atr=atr,
        reason=reason,
    )
