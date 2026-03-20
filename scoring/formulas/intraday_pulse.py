"""
scoring/formulas/intraday_pulse.py — IntraDayPulse scorer.

Active ONLY during market hours (09:15–15:30 IST).
Uses the live OHLCV candle injected by DataFetcher._inject_live_candle()
into the last row of every stock's OHLCV dataframe.

Four components (each 0–100, higher = more bullish intraday signal)
────────────────────────────────────────────────────────────────────
  day_return      : (LTP − prev_close) / prev_close
                    ±3% maps to 0–100  (flat = 50)

  range_position  : (LTP − day_low) / (day_high − day_low)
                    at day low = 0, at day high = 100, mid-range = 50

  volume_pace     : today_vol / (avg_20d_vol × elapsed_session_fraction)
                    on-pace = 50, 2× expected = 100, no volume = 0

  open_distance   : (LTP − open) / open
                    ±2% maps to 0–100  (at open price = 50)

Default sub-weights (configurable via .env):
  day_return      35 %
  range_position  30 %
  volume_pace     25 %
  open_distance   10 %

Blending formula (delta-based — identical to sentiment blender):
    delta   = (pulse_score − 50) / 50        # −1.0 to +1.0
    boost   = delta × intraday_weight × base_composite
    new_composite = base_composite + boost

Key property: pulse = 50 (flat, mid-range, on-pace) → delta = 0 → no change.
Only stocks that are ACTUALLY moving intraday get a score nudge.

Examples (base=68, intraday_weight=0.20):
    pulse=75  (+1.5% day, near high, volume surge) → boost=+3.4 → new=71.4 ✓ BUY!
    pulse=30  (−1.5% day, near low,  light volume) → boost=−8.2 → new=59.8 ✗ skip
    pulse=50  (flat day, mid-range, on-pace)        → boost= 0.0 → new=68.0   unchanged
"""

from __future__ import annotations

import logging
import math
from typing import Optional

import pandas as pd

from market_hours import elapsed_market_fraction

log = logging.getLogger("IntraDayPulse")

# Caps for linear mapping to 0–100
_DAY_RETURN_CAP  = 0.03   # ±3%  (−3% → score 0,  0% → 50,  +3% → 100)
_OPEN_DIST_CAP   = 0.02   # ±2%  (−2% → score 0,  0% → 50,  +2% → 100)
_VOL_PACE_CAP    = 2.0    # 200% of expected pace → score 100 (capped)

# Minimum session elapsed before volume_pace is meaningful
_MIN_ELAPSED     = 0.02   # 2% of 375 min ≈ 7.5 minutes after open


def _safe(v, default: float = 50.0) -> float:
    """Return default when v is None / NaN / inf."""
    if v is None:
        return default
    try:
        f = float(v)
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


class IntraDayPulse:
    """
    Stateless scorer — compute() is safe to call from multiple threads.

    Usage::
        pulse_score, components = IntraDayPulse().compute(df)
    """

    def compute(
        self,
        df: pd.DataFrame,
        w_day_return:     float = 0.35,
        w_range_position: float = 0.30,
        w_volume_pace:    float = 0.25,
        w_open_distance:  float = 0.10,
    ) -> tuple[float, dict]:
        """
        Compute an intraday momentum score (0–100) from the OHLCV dataframe.

        Parameters
        ----------
        df               : OHLCV DataFrame; last row must be today's live candle
                           (injected by DataFetcher._inject_live_candle).
        w_*              : sub-component weights (passed from Config / engine).

        Returns
        -------
        (pulse_score, components_dict)
            pulse_score  : 0–100  (50 = perfectly neutral intraday)
            components   : dict of individual sub-scores for audit/logging
        """
        if df is None or len(df) < 2:
            return 50.0, {"intraday_error": "insufficient_data"}

        today = df.iloc[-1]
        prev  = df.iloc[-2]

        ltp        = _safe(today.get("Close"))
        day_open   = _safe(today.get("Open"))
        day_high   = _safe(today.get("High"))
        day_low    = _safe(today.get("Low"))
        today_vol  = _safe(today.get("Volume"), 0.0)
        prev_close = _safe(prev.get("Close"))

        # ── 1. day_return ────────────────────────────────────────────────────
        # Maps [-_DAY_RETURN_CAP, +_DAY_RETURN_CAP] → [0, 100]
        if prev_close > 0:
            ret = (ltp - prev_close) / prev_close
            score_day_return = _clamp(
                (ret + _DAY_RETURN_CAP) / (2.0 * _DAY_RETURN_CAP) * 100.0
            )
        else:
            score_day_return = 50.0

        # ── 2. range_position ────────────────────────────────────────────────
        # (LTP − low) / (high − low) → 0–100
        spread = day_high - day_low
        if spread > 0:
            score_range = _clamp((ltp - day_low) / spread * 100.0)
        else:
            score_range = 50.0

        # ── 3. volume_pace ───────────────────────────────────────────────────
        # Compare today's accumulated volume to "expected by now" based on
        # the 20-session average daily volume × fraction of session elapsed.
        # Exclude today from the average (iloc[-21:-1]).
        hist_vols = df["Volume"].iloc[-21:-1] if "Volume" in df.columns else pd.Series(dtype=float)
        avg_vol   = float(hist_vols.mean()) if len(hist_vols) >= 5 else 0.0
        elapsed   = elapsed_market_fraction()

        if avg_vol > 0 and elapsed >= _MIN_ELAPSED:
            expected     = avg_vol * elapsed
            pace         = today_vol / expected          # 1.0 = on-pace
            score_volume = _clamp(pace / _VOL_PACE_CAP * 100.0)
        else:
            score_volume = 50.0   # not enough data yet → neutral

        # ── 4. open_distance ─────────────────────────────────────────────────
        # Maps [-_OPEN_DIST_CAP, +_OPEN_DIST_CAP] → [0, 100]
        if day_open > 0:
            dist       = (ltp - day_open) / day_open
            score_open = _clamp(
                (dist + _OPEN_DIST_CAP) / (2.0 * _OPEN_DIST_CAP) * 100.0
            )
        else:
            score_open = 50.0

        # ── Weighted composite ───────────────────────────────────────────────
        total_w = w_day_return + w_range_position + w_volume_pace + w_open_distance
        if total_w <= 0:
            total_w = 1.0

        pulse = (
            score_day_return * w_day_return   +
            score_range      * w_range_position +
            score_volume     * w_volume_pace  +
            score_open       * w_open_distance
        ) / total_w

        components = {
            "intraday_day_return":     round(score_day_return, 2),
            "intraday_range_position": round(score_range,      2),
            "intraday_volume_pace":    round(score_volume,     2),
            "intraday_open_distance":  round(score_open,       2),
            "intraday_elapsed_pct":    round(elapsed * 100.0,  1),
        }
        return round(_clamp(pulse), 2), components
