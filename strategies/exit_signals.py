"""
strategies/exit_signals.py — Statistically-derived, per-stock exit levels.

Instead of hardcoded percentages (e.g. "stop at -3%"), every exit level is
computed from the stock's own historical price distribution.  A volatile
small-cap gets a wider stop (normal noise won't trigger it); a stable
large-cap gets a tighter stop (less room to lose).

Three research-backed methods are blended:

1. ATR-based (Wilder, 1978)
   ─────────────────────────
   ATR(n) = EMA of True Range = max(High-Low, |High-PrevClose|, |Low-PrevClose|)
   Stop   = entry − atr_stop_mult × ATR
   Trail  = peak  − atr_chandelier_mult × ATR   ← "Chandelier Exit" (LeBeau 1999)
   TP     = entry + atr_tp_mult × ATR            (ATR is the natural "unit of risk")

2. Historical VaR (parametric — Normal distribution fit)
   ──────────────────────────────────────────────────────
   Fit daily log-returns over the last `var_period` days to a Normal(μ, σ).
   Stop = entry × exp(z_score × σ + μ)
   where z_score = norm.ppf(var_confidence, e.g. 0.05 for 95% VaR)
   Interpretation: only 5% of historical days had a loss larger than this.

3. Historical VaR (non-parametric — empirical percentile)
   ─────────────────────────────────────────────────────────
   Stop = entry × (1 + percentile(returns, var_confidence × 100))
   No distribution assumption — uses the raw sorted return histogram.

Final stop-loss = tightest of ATR-stop and VaR-stop
  (be conservative: if either method says "this move is abnormal", respect it)
Final take-profit = entry + risk_reward_ratio × stop_distance
  (maintain a fixed R:R so the strategy has positive expected value)
Final trail = ATR Chandelier (always ATR-based — most robust trailing method)

References:
  Wilder, J.W. (1978). New Concepts in Technical Trading Systems.
  LeBeau, C. (1999). Chandelier Exits. Technical Analysis of Stocks & Commodities.
  Vince, R. (1990). Portfolio Management Formulas. Wiley.
  Jorion, P. (2006). Value at Risk, 3rd ed. McGraw-Hill.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

log = logging.getLogger("ExitSignals")

# ── Score high-water mark tracking (in-memory, resets on restart) ─────────────
_score_high_water: dict[str, float] = {}  # symbol → highest score seen while holding


# ── Output dataclass ──────────────────────────────────────────────────────────

@dataclass
class ExitLevels:
    """
    All prices are absolute (INR), not percentages.

    stop_loss    — sell immediately if LTP ≤ this value
    take_profit  — sell immediately if LTP ≥ this value (0 = disabled)
    trail_level  — sell if LTP ≤ this value AND trailing stop is armed
    trail_armed  — True once price exceeded trailing_activation_price
    atr          — raw ATR value (for logging / debugging)
    stop_pct     — effective stop distance as % of entry (for logging)
    """
    stop_loss:              float
    take_profit:            float
    trail_level:            float
    trail_armed:            bool
    atr:                    float
    stop_pct:               float
    method:                 str    # which method set the stop_loss


def compute_exit_levels(
    df:                     pd.DataFrame,
    avg_buy_price:          float,
    peak_price:             float,
    *,
    atr_period:             int   = 14,
    atr_stop_mult:          float = 2.0,
    atr_chandelier_mult:    float = 3.0,
    atr_tp_mult:            float = 0.0,    # 0 = use risk_reward_ratio instead
    risk_reward_ratio:      float = 2.0,    # take-profit = entry + R:R × stop_distance
    var_period:             int   = 252,    # 1 trading year for VaR estimate
    var_confidence:         float = 0.05,   # 5th percentile = 95% confidence
    trailing_activation_pct: float = 2.0,  # arm trailing stop after this % gain
) -> ExitLevels:
    """
    Compute statistically-derived exit levels for a live position.

    Parameters
    ----------
    df              : OHLCV DataFrame (columns: Open, High, Low, Close, Volume)
                      Must have at least atr_period + 1 rows.
    avg_buy_price   : average entry price of the current position
    peak_price      : highest LTP seen since entry (for chandelier trailing stop)
    """
    if df is None or len(df) < atr_period + 2:
        # Not enough data — fall back to a wide 5% stop
        stop = avg_buy_price * 0.95
        tp   = avg_buy_price * 1.10
        return ExitLevels(
            stop_loss=stop, take_profit=tp,
            trail_level=peak_price * 0.95, trail_armed=False,
            atr=0.0, stop_pct=5.0, method="fallback",
        )

    close  = df["Close"].astype(float)
    high   = df["High"].astype(float)
    low    = df["Low"].astype(float)

    # ── 1. ATR (Wilder's smoothed average true range) ─────────────────────────
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Wilder smoothing: EMA with alpha = 1/period
    atr = tr.ewm(alpha=1 / atr_period, adjust=False).mean().iloc[-1]

    atr_stop_dist = atr_stop_mult * atr
    atr_stop      = avg_buy_price - atr_stop_dist

    # ── 2. VaR-based stop (parametric — Normal fit) ───────────────────────────
    returns = close.pct_change().dropna()
    recent  = returns.tail(var_period)

    var_stop_param = avg_buy_price       # default if not enough data
    var_stop_emp   = avg_buy_price
    var_method     = "atr"

    if len(recent) >= 30:
        mu    = recent.mean()
        sigma = recent.std(ddof=1)

        if sigma > 0:
            # Parametric (Normal distribution): z-score for var_confidence
            from scipy.stats import norm as _norm
            z             = _norm.ppf(var_confidence)      # e.g. -1.645 at 5%
            var_1day_pct  = z * sigma + mu                 # negative number
            var_stop_param = avg_buy_price * (1 + var_1day_pct)

            # Non-parametric (empirical): raw sorted percentile
            emp_pct       = float(np.percentile(recent, var_confidence * 100))
            var_stop_emp  = avg_buy_price * (1 + emp_pct)

            var_method = "var"

    # ── 3. Blend: use the tightest (most conservative) stop ───────────────────
    #    ATR-stop ← adapts to daily range volatility
    #    VaR-stop ← adapts to return distribution tail
    #    We take the higher (less negative) value = tightest stop = less loss
    stop_candidates = [atr_stop]
    if var_method == "var":
        stop_candidates.extend([var_stop_param, var_stop_emp])

    stop_loss = max(stop_candidates)   # highest floor = tightest stop

    # Safety: stop can never be above entry (that would trigger immediately)
    stop_loss = min(stop_loss, avg_buy_price * 0.995)

    stop_pct  = (avg_buy_price - stop_loss) / avg_buy_price * 100
    stop_dist = avg_buy_price - stop_loss

    # ── 4. Take-profit ────────────────────────────────────────────────────────
    if atr_tp_mult > 0:
        take_profit = avg_buy_price + atr_tp_mult * atr
    elif risk_reward_ratio > 0 and stop_dist > 0:
        # Fixed R:R: TP is risk_reward_ratio × the stop distance
        # e.g. stop = -₹20, R:R = 2 → TP = +₹40
        take_profit = avg_buy_price + risk_reward_ratio * stop_dist
    else:
        take_profit = 0.0   # disabled

    # ── 5. Chandelier trailing stop (LeBeau, 1999) ────────────────────────────
    #    trail = peak_price - atr_chandelier_mult × ATR
    #    As price rises, peak rises → trail rises → locks in gains
    effective_peak = max(peak_price, avg_buy_price)
    trail_level    = effective_peak - atr_chandelier_mult * atr

    # Trailing stop only activates once position is up trailing_activation_pct
    activation_price = avg_buy_price * (1 + trailing_activation_pct / 100)
    trail_armed      = effective_peak >= activation_price

    # Determine which method actually set the stop_loss for logging
    if abs(stop_loss - atr_stop) < 0.01:
        method = f"ATR({atr_period})×{atr_stop_mult}"
    elif abs(stop_loss - var_stop_param) < 0.01:
        method = f"VaR-parametric(σ={sigma*100:.2f}%,{int((1-var_confidence)*100)}%CL)"
    else:
        method = f"VaR-empirical({int((1-var_confidence)*100)}%CL)"

    return ExitLevels(
        stop_loss=stop_loss,
        take_profit=take_profit,
        trail_level=trail_level,
        trail_armed=trail_armed,
        atr=atr,
        stop_pct=stop_pct,
        method=method,
    )


# ── Intraday score-based smart exits ─────────────────────────────────────────

def check_intraday_exit(
    symbol:          str,
    composite_score: float,
    score_velocity:  float,
    v_recent:        float,
    min_score:       float,
    peak_exit_pct:   float = 0.07,
    collapse_score_ratio:     float = 0.80,
    collapse_velocity_floor:  float = -4.0,
    collapse_price_vel_floor: float = -0.3,
) -> tuple[bool, str]:
    """
    Returns (should_exit, reason).

    Signal A — Sell at peak:
      Update score high-water mark.
      If score dropped >= peak_exit_pct from high-water → exit.
      Reason: "score_peak_exit: {high:.1f}→{now:.1f} ({drop:.1%} drop)"

    Signal B — Collapse:
      All three: score < min×ratio AND vel < floor AND price_vel < floor
      Reason: "collapse_exit: score={score:.1f} vel={vel:.1f} pvel={pvel:.2f}"

    Cleans up high-water entry when exit fires.
    """
    # Update high-water mark
    prev_hw = _score_high_water.get(symbol, composite_score)
    hw = max(prev_hw, composite_score)
    _score_high_water[symbol] = hw

    # Signal A — sell at peak
    if hw > 0:
        drop = (hw - composite_score) / hw
        if drop >= peak_exit_pct:
            del _score_high_water[symbol]
            return True, (
                f"score_peak_exit: {hw:.1f}→{composite_score:.1f} ({drop:.1%} drop)"
            )

    # Signal B — collapse
    collapse = (
        composite_score < min_score * collapse_score_ratio
        and score_velocity  < collapse_velocity_floor
        and v_recent        < collapse_price_vel_floor
    )
    if collapse:
        _score_high_water.pop(symbol, None)
        return True, (
            f"collapse_exit: score={composite_score:.1f} "
            f"vel={score_velocity:.1f} pvel={v_recent:.2f}"
        )

    return False, ""


def clear_high_water(symbol: str) -> None:
    """Call this when a position is closed for any reason."""
    _score_high_water.pop(symbol, None)
