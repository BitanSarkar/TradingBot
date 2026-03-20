import os
from dataclasses import dataclass, field


@dataclass
class Config:
    # ── Groww API Credentials ──────────────────────────────────────────
    # Recommended: set via environment variables, not hard-coded here
    api_key: str = field(default_factory=lambda: os.getenv("GROWW_API_KEY", "your_api_key"))
    secret:  str = field(default_factory=lambda: os.getenv("GROWW_SECRET",  "your_secret"))

    # ── Bot Behavior ───────────────────────────────────────────────────
    dry_run:       bool = True   # True = simulate orders; set False only when ready to go live
    poll_interval: int  = 300    # seconds between ticks (5 min default for EOD scoring)

    # ── Scoring Thresholds ─────────────────────────────────────────────
    score_buy_threshold:  float = 70.0  # composite score to trigger BUY
    score_sell_threshold: float = 40.0  # composite score to trigger SELL
    score_top_n:          int   = 50    # only consider top-N stocks as BUY candidates

    # ── Position Sizing ────────────────────────────────────────────────
    quantity_per_trade: int = 1         # fixed qty per order (increase carefully)
    max_holdings:       int = 10        # max simultaneous open positions

    # ── Risk Controls ──────────────────────────────────────────────────
    max_quantity_per_order: int   = 10       # hard cap regardless of signal
    max_daily_loss:         float = 1000.0   # pause if realized loss exceeds (INR)

    # ── Exchange / Segment Defaults ────────────────────────────────────
    default_exchange: str = "NSE"
    default_segment:  str = "CASH"
    default_product:  str = "MIS"   # MIS = intraday  |  CNC = delivery
    default_validity: str = "DAY"
