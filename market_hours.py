"""
market_hours.py — NSE market-hours utilities.

NSE schedule (IST = UTC+5:30)
──────────────────────────────
  Pre-open session   : 09:00 - 09:15
  Regular session    : 09:15 - 15:30
  EOD data window    : 15:30 - 15:55  ← NSE publishes final EOD prices here
  After-hours        : 15:55 - 09:00 next day

This module exposes:
  • market_state()             → "open" | "eod_window" | "closed"
  • is_market_open()           → bool
  • is_eod_window()            → bool
  • seconds_until_open()       → float (seconds until next market open)
  • elapsed_market_fraction()  → float 0.0–1.0 (fraction of session elapsed)

All comparisons are in IST.  No external dependencies.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo   # stdlib Python 3.9+

IST = ZoneInfo("Asia/Kolkata")

# NSE session boundaries (IST)
_OPEN_TIME  = time(9, 15)    # regular session opens
_CLOSE_TIME = time(15, 30)   # regular session closes
_EOD_END    = time(15, 55)   # EOD data finishes publishing
_PRE_OPEN   = time(9, 0)     # pre-open warm-up starts


def _now_ist() -> datetime:
    return datetime.now(IST)


def market_state() -> str:
    """
    Returns one of:
      "open"       — regular session 09:15–15:30, Mon–Fri
      "eod_window" — NSE publishing EOD data 15:30–15:55, Mon–Fri
      "closed"     — everything else (nights, weekends)
    """
    now = _now_ist()
    if now.weekday() >= 5:           # Saturday=5, Sunday=6
        return "closed"
    t = now.time()
    if _OPEN_TIME <= t < _CLOSE_TIME:
        return "open"
    if _CLOSE_TIME <= t < _EOD_END:
        return "eod_window"
    return "closed"


def is_market_open() -> bool:
    return market_state() == "open"


def is_eod_window() -> bool:
    return market_state() == "eod_window"


def seconds_until_open() -> float:
    """
    Returns seconds until the next 09:00 IST pre-open warm-up.
    Used to sleep the bot overnight / over weekends.

    Examples (all IST):
      20:00 Fri  →  ~13 hours (next Mon 09:00)
      20:00 Sun  →  ~13 hours (Mon 09:00)
      07:00 Mon  →  2 hours   (same day 09:00)
    """
    now  = _now_ist()
    candidate = now.replace(hour=_PRE_OPEN.hour, minute=_PRE_OPEN.minute,
                            second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)

    # Skip weekends
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)

    return max(0.0, (candidate - now).total_seconds())


_MARKET_TOTAL_MINUTES: float = (
    _CLOSE_TIME.hour * 60 + _CLOSE_TIME.minute
) - (
    _OPEN_TIME.hour * 60 + _OPEN_TIME.minute
)  # 375 minutes  (09:15 → 15:30)


def elapsed_market_fraction() -> float:
    """
    Returns the fraction of the regular NSE session elapsed today (0.0–1.0).

      0.0 → at or before 09:15 IST  (session just opened)
      1.0 → at or after  15:30 IST  (session closed / EOD window)

    Used by IntraDayPulse to normalise intraday volume pace:
        volume_pace = today_volume / (avg_daily_volume × elapsed_fraction)

    Always returns 0.0 before the market opens on a given day so callers can
    safely guard with ``if elapsed > 0.02`` before dividing.
    """
    now = _now_ist()
    if now.weekday() >= 5:           # weekend → treat as closed
        return 1.0
    t = now.time()
    if t < _OPEN_TIME:
        return 0.0
    if t >= _CLOSE_TIME:
        return 1.0
    now_minutes  = now.hour * 60 + now.minute + now.second / 60.0
    open_minutes = _OPEN_TIME.hour * 60 + _OPEN_TIME.minute
    elapsed      = now_minutes - open_minutes
    return min(1.0, max(0.0, elapsed / _MARKET_TOTAL_MINUTES))


def market_status_line() -> str:
    """Human-readable one-liner for logging."""
    state = market_state()
    now   = _now_ist()
    if state == "open":
        remaining = (_now_ist().replace(
            hour=_CLOSE_TIME.hour, minute=_CLOSE_TIME.minute,
            second=0, microsecond=0) - now).total_seconds()
        return f"OPEN  (closes in {int(remaining/60)}m)"
    if state == "eod_window":
        return "EOD WINDOW  (NSE publishing final prices)"
    secs = seconds_until_open()
    h, m = divmod(int(secs) // 60, 60)
    return f"CLOSED  (opens in {h}h {m}m)"
