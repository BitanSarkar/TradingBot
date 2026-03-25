"""
config.py — Single source of truth for all bot settings.

Every value is read from the .env file (via python-dotenv).
Hardcoded defaults are safe fallbacks — they are never used in production
because the .env file is always loaded first.

To change anything: edit .env, restart the bot. No code changes needed.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root (same directory as this file).
# override=True ensures select_strategy.py changes take effect even if
# systemd EnvironmentFile already injected stale values into the process env.
load_dotenv(Path(__file__).parent / ".env", override=True)


# ── helpers ──────────────────────────────────────────────────────────────────

def _str(key: str, default: str) -> str:
    return os.getenv(key, default)

def _bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes")

def _int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except (ValueError, TypeError):
        return default

def _float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except (ValueError, TypeError):
        return default


# ── Config dataclass ─────────────────────────────────────────────────────────

@dataclass
class Config:

    # ── Groww API Credentials ─────────────────────────────────────────────────
    api_key:     str = field(default_factory=lambda: _str("GROWW_API_KEY",     "your_api_key"))
    totp_secret: str = field(default_factory=lambda: _str("GROWW_TOTP_SECRET", ""))
    secret:      str = field(default_factory=lambda: _str("GROWW_SECRET",      ""))

    # ── Bot Behaviour ─────────────────────────────────────────────────────────
    dry_run:              bool = field(default_factory=lambda: _bool("BOT_DRY_RUN",              True))
    poll_interval:        int  = field(default_factory=lambda: _int ("BOT_POLL_INTERVAL",        300))
    poll_interval_open:   int  = field(default_factory=lambda: _int ("BOT_POLL_INTERVAL_OPEN",   60))
    poll_interval_closed: int  = field(default_factory=lambda: _int ("BOT_POLL_INTERVAL_CLOSED", 3600))

    # ── Notifications ─────────────────────────────────────────────────────────
    sns_topic_arn:  str = field(default_factory=lambda: os.getenv("SNS_TOPIC_ARN", ""))

    # ── Risk Controls ─────────────────────────────────────────────────────────
    max_daily_loss:         float = field(default_factory=lambda: _float("RISK_MAX_DAILY_LOSS",    1000.0))
    max_holdings:           int   = field(default_factory=lambda: _int  ("RISK_MAX_HOLDINGS",      10))
    max_quantity_per_order: int   = field(default_factory=lambda: _int  ("RISK_MAX_QTY_PER_ORDER", 10))
    quantity_per_trade:     int   = field(default_factory=lambda: _int  ("RISK_QUANTITY_PER_TRADE",1))

    # ── Exit Controls — statistically-derived per-stock exit levels ───────────
    #
    # Exit levels are computed from each stock's own historical price distribution
    # (ATR + VaR) rather than hardcoded percentages.  A volatile stock gets a
    # wider stop automatically; a stable stock gets a tighter one.

    # ATR period for True Range calculation (Wilder default = 14)
    exit_atr_period:            int   = field(default_factory=lambda: _int  ("EXIT_ATR_PERIOD",            14))

    # Stop-loss = entry − (exit_atr_stop_mult × ATR)
    # Larger multiplier = wider stop = fewer false stops on volatile stocks
    exit_atr_stop_mult:         float = field(default_factory=lambda: _float("EXIT_ATR_STOP_MULT",         2.0))

    # Chandelier trailing stop = peak − (exit_atr_chandelier_mult × ATR)
    # LeBeau's recommended value is 3.0
    exit_atr_chandelier_mult:   float = field(default_factory=lambda: _float("EXIT_ATR_CHANDELIER_MULT",   3.0))

    # Risk:Reward ratio — take-profit placed at this multiple of the stop distance
    # e.g. 2.0 → if stop is ₹20 below entry, TP is ₹40 above entry
    exit_risk_reward_ratio:     float = field(default_factory=lambda: _float("EXIT_RISK_REWARD_RATIO",     2.0))

    # VaR lookback window in trading days (252 = 1 year)
    exit_var_period:            int   = field(default_factory=lambda: _int  ("EXIT_VAR_PERIOD",            252))

    # VaR confidence level — 0.05 = 95% confidence (5th percentile of returns)
    # Lower = tighter stop (less tolerance for loss)
    exit_var_confidence:        float = field(default_factory=lambda: _float("EXIT_VAR_CONFIDENCE",        0.05))

    # Arm the Chandelier trailing stop only after the position is up this much %
    exit_trailing_activation_pct: float = field(default_factory=lambda: _float("EXIT_TRAILING_ACTIVATION_PCT", 2.0))

    # Score-based sell: only fires when already in profit (score < sell_threshold)
    # For a total score collapse below this emergency level, sell even at a loss
    score_emergency_sell_threshold: float = field(default_factory=lambda: _float("SCORE_EMERGENCY_SELL_THRESHOLD", 20.0))

    # ── Entry Quality Controls ─────────────────────────────────────────────────
    # Statistically-derived entry timing — ensures you enter early in a move,
    # at a favourable price, with volume confirmation, in a healthy market.
    # See strategies/entry_signals.py for full documentation.

    # Minimum composite entry quality score (0–100) to generate a BUY signal.
    # Below this threshold the tick is skipped — wait for a better setup.
    entry_min_quality:          float = field(default_factory=lambda: _float("ENTRY_MIN_QUALITY",           55.0))

    # Reject if the composite score's velocity (rate of change) is below this.
    # -0.5 = allow slight drift; only reject clearly declining scores (< -0.5 pts/tick).
    #  0.0 = reject any negative velocity (too strict — floating-point noise causes false rejects).
    #  0.5 = require the score to be actively rising.
    entry_min_score_velocity:   float = field(default_factory=lambda: _float("ENTRY_MIN_SCORE_VELOCITY",   -0.5))

    # Score history window (ticks) used for velocity/acceleration regression.
    entry_velocity_window:      int   = field(default_factory=lambda: _int  ("ENTRY_VELOCITY_WINDOW",       5))

    # RSI at or below this is considered a "good entry" (not overbought).
    entry_rsi_ideal_max:        float = field(default_factory=lambda: _float("ENTRY_RSI_IDEAL_MAX",         55.0))

    # Bollinger %B at or below this is a "good entry" (price near lower band).
    entry_bollinger_b_max:      float = field(default_factory=lambda: _float("ENTRY_BOLLINGER_B_MAX",       0.55))

    # Reject if today's volume < this fraction of 20-day average.
    entry_vol_min_ratio:        float = field(default_factory=lambda: _float("ENTRY_VOL_MIN_RATIO",         0.8))

    # Reject if fewer than this fraction of universe stocks have score > 50.
    entry_bull_ratio_min:       float = field(default_factory=lambda: _float("ENTRY_BULL_RATIO_MIN",        0.40))

    # ── Bullish Divergence Bypass ──────────────────────────────────────────────
    # When the market is in a bear regime (bull_ratio < entry_bull_ratio_min),
    # the bot normally blocks ALL buys.  The bypass allows an exceptional stock
    # to be purchased if it shows relative strength divergence — its score is
    # high AND rising even while the broad market deteriorates, AND the price
    # is already beaten down (oversold RSI).  ALL three conditions must be met.
    #
    # These buys always use a LIMIT order with 1.5× the normal pullback to
    # protect against further downside in the weak market environment.

    # Minimum composite score to be eligible for bypass (default 78 — very high)
    entry_regime_bypass_min_score:    float = field(default_factory=lambda: _float("ENTRY_REGIME_BYPASS_MIN_SCORE",    78.0))
    # Minimum score velocity (pts/tick) — score must be actively rising
    entry_regime_bypass_min_velocity: float = field(default_factory=lambda: _float("ENTRY_REGIME_BYPASS_MIN_VELOCITY", 1.5))
    # Maximum RSI — price must be oversold to qualify (default 45)
    entry_regime_bypass_max_rsi:      float = field(default_factory=lambda: _float("ENTRY_REGIME_BYPASS_MAX_RSI",      45.0))

    # Limit order pullback: place limit at `ltp − mult × ATR` for medium-quality entries.
    # 0.0 = always use market orders (faster, but worse average fill price).
    entry_pullback_mult:        float = field(default_factory=lambda: _float("ENTRY_PULLBACK_MULT",         0.5))

    # Cancel limit order and switch to market after this many ticks with no fill.
    entry_limit_timeout_ticks:  int   = field(default_factory=lambda: _int  ("ENTRY_LIMIT_TIMEOUT_TICKS",   3))

    # Sub-weights for entry quality composite (must sum to 1.0)
    entry_w_velocity:           float = field(default_factory=lambda: _float("ENTRY_W_VELOCITY",            0.25))
    entry_w_price:              float = field(default_factory=lambda: _float("ENTRY_W_PRICE",               0.25))
    entry_w_volume:             float = field(default_factory=lambda: _float("ENTRY_W_VOLUME",              0.15))
    entry_w_regime:             float = field(default_factory=lambda: _float("ENTRY_W_REGIME",              0.20))
    entry_w_price_velocity:     float = field(default_factory=lambda: _float("ENTRY_W_PRICE_VELOCITY",      0.15))

    # ── Intraday velocity ──────────────────────────────────────────────────────
    intraday_momentum_chase_premium: float = field(
        default_factory=lambda: _float("INTRADAY_MOMENTUM_CHASE_PREMIUM", 0.01))
    intraday_velocity_min_candles: int = field(
        default_factory=lambda: _int("INTRADAY_VELOCITY_MIN_CANDLES", 3))

    # ── Score peak exit (Signal A) ────────────────────────────────────────────
    score_peak_exit_pct: float = field(
        default_factory=lambda: _float("SCORE_PEAK_EXIT_PCT", 0.07))  # 7% drop from peak

    # ── Collapse exit (Signal B) ──────────────────────────────────────────────
    collapse_score_ratio:    float = field(
        default_factory=lambda: _float("COLLAPSE_SCORE_RATIO",    0.80))  # 80% of min_score
    collapse_velocity_threshold: float = field(
        default_factory=lambda: _float("COLLAPSE_VELOCITY_THRESHOLD", -4.0))
    collapse_price_vel_threshold: float = field(
        default_factory=lambda: _float("COLLAPSE_PRICE_VEL_THRESHOLD", -0.3))

    # ── Position Sizing ───────────────────────────────────────────────────────
    # deploy_fraction: fraction of available CNC balance to spread across holdings
    #   e.g. 0.90 → use 90% of wallet, keep 10% as buffer
    # If dynamic sizing is disabled (RISK_DYNAMIC_SIZING=false), falls back to
    # quantity_per_trade (static qty).
    dynamic_sizing:    bool  = field(default_factory=lambda: _bool ("RISK_DYNAMIC_SIZING",    True))
    deploy_fraction:   float = field(default_factory=lambda: _float("RISK_DEPLOY_FRACTION",   0.90))
    dry_run_balance:   float = field(default_factory=lambda: _float("RISK_DRY_RUN_BALANCE", 100000.0))
    paper_ledger_path: str   = field(default_factory=lambda: _str  ("PAPER_LEDGER_PATH",   "ledgers/paper_ledger.json"))
    # Set to true on EC2 — all OHLCV + fundamentals arrive via rsync from Mac.
    # Skips every network fetch attempt (prevents 401 spam from yfinance/NSE API).
    fetcher_cache_only: bool = field(default_factory=lambda: _bool ("FETCHER_CACHE_ONLY",   False))

    # ── Signal Thresholds ─────────────────────────────────────────────────────
    score_buy_threshold:  float = field(default_factory=lambda: _float("SCORE_BUY_THRESHOLD",  70.0))
    score_sell_threshold: float = field(default_factory=lambda: _float("SCORE_SELL_THRESHOLD", 40.0))
    score_top_n:          int   = field(default_factory=lambda: _int  ("SCORE_TOP_N",          50))
    score_sector_top_n:   int   = field(default_factory=lambda: _int  ("SCORE_SECTOR_TOP_N",    5))

    # ── Exchange / Segment Defaults ───────────────────────────────────────────
    default_exchange: str = "NSE"
    default_segment:  str = "CASH"
    default_product:  str = "MIS"   # MIS = intraday  |  CNC = delivery
    default_validity: str = "DAY"

    # ── Composite Pillar Weights — DEFAULT sector ─────────────────────────────
    w_default_technical:   float = field(default_factory=lambda: _float("WEIGHT_DEFAULT_TECHNICAL",   0.40))
    w_default_fundamental: float = field(default_factory=lambda: _float("WEIGHT_DEFAULT_FUNDAMENTAL", 0.35))
    w_default_momentum:    float = field(default_factory=lambda: _float("WEIGHT_DEFAULT_MOMENTUM",    0.25))

    # ── Composite Pillar Weights — IT ─────────────────────────────────────────
    w_it_technical:   float = field(default_factory=lambda: _float("WEIGHT_IT_TECHNICAL",   0.45))
    w_it_fundamental: float = field(default_factory=lambda: _float("WEIGHT_IT_FUNDAMENTAL", 0.30))
    w_it_momentum:    float = field(default_factory=lambda: _float("WEIGHT_IT_MOMENTUM",    0.25))

    # ── Composite Pillar Weights — BANKING ────────────────────────────────────
    w_banking_technical:   float = field(default_factory=lambda: _float("WEIGHT_BANKING_TECHNICAL",   0.35))
    w_banking_fundamental: float = field(default_factory=lambda: _float("WEIGHT_BANKING_FUNDAMENTAL", 0.45))
    w_banking_momentum:    float = field(default_factory=lambda: _float("WEIGHT_BANKING_MOMENTUM",    0.20))

    # ── Composite Pillar Weights — PSU BANK ──────────────────────────────────
    w_psu_bank_technical:   float = field(default_factory=lambda: _float("WEIGHT_PSU_BANK_TECHNICAL",   0.38))
    w_psu_bank_fundamental: float = field(default_factory=lambda: _float("WEIGHT_PSU_BANK_FUNDAMENTAL", 0.42))
    w_psu_bank_momentum:    float = field(default_factory=lambda: _float("WEIGHT_PSU_BANK_MOMENTUM",    0.20))

    # ── Composite Pillar Weights — PHARMA ─────────────────────────────────────
    w_pharma_technical:   float = field(default_factory=lambda: _float("WEIGHT_PHARMA_TECHNICAL",   0.40))
    w_pharma_fundamental: float = field(default_factory=lambda: _float("WEIGHT_PHARMA_FUNDAMENTAL", 0.35))
    w_pharma_momentum:    float = field(default_factory=lambda: _float("WEIGHT_PHARMA_MOMENTUM",    0.25))

    # ── Composite Pillar Weights — AUTO ───────────────────────────────────────
    w_auto_technical:   float = field(default_factory=lambda: _float("WEIGHT_AUTO_TECHNICAL",   0.42))
    w_auto_fundamental: float = field(default_factory=lambda: _float("WEIGHT_AUTO_FUNDAMENTAL", 0.33))
    w_auto_momentum:    float = field(default_factory=lambda: _float("WEIGHT_AUTO_MOMENTUM",    0.25))

    # ── Composite Pillar Weights — FMCG ───────────────────────────────────────
    w_fmcg_technical:   float = field(default_factory=lambda: _float("WEIGHT_FMCG_TECHNICAL",   0.30))
    w_fmcg_fundamental: float = field(default_factory=lambda: _float("WEIGHT_FMCG_FUNDAMENTAL", 0.50))
    w_fmcg_momentum:    float = field(default_factory=lambda: _float("WEIGHT_FMCG_MOMENTUM",    0.20))

    # ── Composite Pillar Weights — METAL ──────────────────────────────────────
    w_metal_technical:   float = field(default_factory=lambda: _float("WEIGHT_METAL_TECHNICAL",   0.50))
    w_metal_fundamental: float = field(default_factory=lambda: _float("WEIGHT_METAL_FUNDAMENTAL", 0.20))
    w_metal_momentum:    float = field(default_factory=lambda: _float("WEIGHT_METAL_MOMENTUM",    0.30))

    # ── Composite Pillar Weights — ENERGY ─────────────────────────────────────
    w_energy_technical:   float = field(default_factory=lambda: _float("WEIGHT_ENERGY_TECHNICAL",   0.42))
    w_energy_fundamental: float = field(default_factory=lambda: _float("WEIGHT_ENERGY_FUNDAMENTAL", 0.35))
    w_energy_momentum:    float = field(default_factory=lambda: _float("WEIGHT_ENERGY_MOMENTUM",    0.23))

    # ── Composite Pillar Weights — REALTY ─────────────────────────────────────
    w_realty_technical:   float = field(default_factory=lambda: _float("WEIGHT_REALTY_TECHNICAL",   0.45))
    w_realty_fundamental: float = field(default_factory=lambda: _float("WEIGHT_REALTY_FUNDAMENTAL", 0.25))
    w_realty_momentum:    float = field(default_factory=lambda: _float("WEIGHT_REALTY_MOMENTUM",    0.30))

    # ── Composite Pillar Weights — INFRA ──────────────────────────────────────
    w_infra_technical:   float = field(default_factory=lambda: _float("WEIGHT_INFRA_TECHNICAL",   0.43))
    w_infra_fundamental: float = field(default_factory=lambda: _float("WEIGHT_INFRA_FUNDAMENTAL", 0.32))
    w_infra_momentum:    float = field(default_factory=lambda: _float("WEIGHT_INFRA_MOMENTUM",    0.25))

    # ── Composite Pillar Weights — FINANCIAL ──────────────────────────────────
    w_financial_technical:   float = field(default_factory=lambda: _float("WEIGHT_FINANCIAL_TECHNICAL",   0.35))
    w_financial_fundamental: float = field(default_factory=lambda: _float("WEIGHT_FINANCIAL_FUNDAMENTAL", 0.45))
    w_financial_momentum:    float = field(default_factory=lambda: _float("WEIGHT_FINANCIAL_MOMENTUM",    0.20))

    # ── Composite Pillar Weights — MEDIA ──────────────────────────────────────
    w_media_technical:   float = field(default_factory=lambda: _float("WEIGHT_MEDIA_TECHNICAL",   0.45))
    w_media_fundamental: float = field(default_factory=lambda: _float("WEIGHT_MEDIA_FUNDAMENTAL", 0.30))
    w_media_momentum:    float = field(default_factory=lambda: _float("WEIGHT_MEDIA_MOMENTUM",    0.25))

    # ── Composite Pillar Weights — CONSUMER ───────────────────────────────────
    w_consumer_technical:   float = field(default_factory=lambda: _float("WEIGHT_CONSUMER_TECHNICAL",   0.40))
    w_consumer_fundamental: float = field(default_factory=lambda: _float("WEIGHT_CONSUMER_FUNDAMENTAL", 0.38))
    w_consumer_momentum:    float = field(default_factory=lambda: _float("WEIGHT_CONSUMER_MOMENTUM",    0.22))

    # ── Technical Indicator Sub-Weights ───────────────────────────────────────
    w_tech_rsi:       float = field(default_factory=lambda: _float("WEIGHT_TECH_RSI",       0.15))
    w_tech_macd:      float = field(default_factory=lambda: _float("WEIGHT_TECH_MACD",      0.20))
    w_tech_bollinger: float = field(default_factory=lambda: _float("WEIGHT_TECH_BOLLINGER", 0.15))
    w_tech_sma_cross: float = field(default_factory=lambda: _float("WEIGHT_TECH_SMA_CROSS", 0.20))
    w_tech_volume:    float = field(default_factory=lambda: _float("WEIGHT_TECH_VOLUME",    0.15))
    w_tech_momentum:  float = field(default_factory=lambda: _float("WEIGHT_TECH_MOMENTUM",  0.15))

    # ── Fundamental Sub-Weights — DEFAULT ─────────────────────────────────────
    w_fund_pe:         float = field(default_factory=lambda: _float("WEIGHT_FUND_PE",         0.15))
    w_fund_pb:         float = field(default_factory=lambda: _float("WEIGHT_FUND_PB",         0.10))
    w_fund_roe:        float = field(default_factory=lambda: _float("WEIGHT_FUND_ROE",        0.15))
    w_fund_de:         float = field(default_factory=lambda: _float("WEIGHT_FUND_DE",         0.12))
    w_fund_curr_ratio: float = field(default_factory=lambda: _float("WEIGHT_FUND_CURR_RATIO", 0.08))
    w_fund_rev_growth: float = field(default_factory=lambda: _float("WEIGHT_FUND_REV_GROWTH", 0.12))
    w_fund_eps_growth: float = field(default_factory=lambda: _float("WEIGHT_FUND_EPS_GROWTH", 0.12))
    w_fund_margin:     float = field(default_factory=lambda: _float("WEIGHT_FUND_MARGIN",     0.10))
    w_fund_dividend:   float = field(default_factory=lambda: _float("WEIGHT_FUND_DIVIDEND",   0.06))

    # ── Fundamental Sub-Weights — BANKING ─────────────────────────────────────
    w_fund_banking_pe:         float = field(default_factory=lambda: _float("WEIGHT_FUND_BANKING_PE",         0.08))
    w_fund_banking_pb:         float = field(default_factory=lambda: _float("WEIGHT_FUND_BANKING_PB",         0.20))
    w_fund_banking_roe:        float = field(default_factory=lambda: _float("WEIGHT_FUND_BANKING_ROE",        0.22))
    w_fund_banking_de:         float = field(default_factory=lambda: _float("WEIGHT_FUND_BANKING_DE",         0.00))
    w_fund_banking_curr_ratio: float = field(default_factory=lambda: _float("WEIGHT_FUND_BANKING_CURR_RATIO", 0.00))
    w_fund_banking_rev_growth: float = field(default_factory=lambda: _float("WEIGHT_FUND_BANKING_REV_GROWTH", 0.15))
    w_fund_banking_eps_growth: float = field(default_factory=lambda: _float("WEIGHT_FUND_BANKING_EPS_GROWTH", 0.15))
    w_fund_banking_margin:     float = field(default_factory=lambda: _float("WEIGHT_FUND_BANKING_MARGIN",     0.12))
    w_fund_banking_dividend:   float = field(default_factory=lambda: _float("WEIGHT_FUND_BANKING_DIVIDEND",   0.08))

    # ── Fundamental Sub-Weights — IT ──────────────────────────────────────────
    w_fund_it_pe:         float = field(default_factory=lambda: _float("WEIGHT_FUND_IT_PE",         0.10))
    w_fund_it_pb:         float = field(default_factory=lambda: _float("WEIGHT_FUND_IT_PB",         0.06))
    w_fund_it_roe:        float = field(default_factory=lambda: _float("WEIGHT_FUND_IT_ROE",        0.15))
    w_fund_it_de:         float = field(default_factory=lambda: _float("WEIGHT_FUND_IT_DE",         0.08))
    w_fund_it_curr_ratio: float = field(default_factory=lambda: _float("WEIGHT_FUND_IT_CURR_RATIO", 0.05))
    w_fund_it_rev_growth: float = field(default_factory=lambda: _float("WEIGHT_FUND_IT_REV_GROWTH", 0.20))
    w_fund_it_eps_growth: float = field(default_factory=lambda: _float("WEIGHT_FUND_IT_EPS_GROWTH", 0.18))
    w_fund_it_margin:     float = field(default_factory=lambda: _float("WEIGHT_FUND_IT_MARGIN",     0.15))
    w_fund_it_dividend:   float = field(default_factory=lambda: _float("WEIGHT_FUND_IT_DIVIDEND",   0.03))

    # ── Fundamental Sub-Weights — PHARMA ──────────────────────────────────────
    w_fund_pharma_pe:         float = field(default_factory=lambda: _float("WEIGHT_FUND_PHARMA_PE",         0.12))
    w_fund_pharma_pb:         float = field(default_factory=lambda: _float("WEIGHT_FUND_PHARMA_PB",         0.06))
    w_fund_pharma_roe:        float = field(default_factory=lambda: _float("WEIGHT_FUND_PHARMA_ROE",        0.18))
    w_fund_pharma_de:         float = field(default_factory=lambda: _float("WEIGHT_FUND_PHARMA_DE",         0.10))
    w_fund_pharma_curr_ratio: float = field(default_factory=lambda: _float("WEIGHT_FUND_PHARMA_CURR_RATIO", 0.03))
    w_fund_pharma_rev_growth: float = field(default_factory=lambda: _float("WEIGHT_FUND_PHARMA_REV_GROWTH", 0.14))
    w_fund_pharma_eps_growth: float = field(default_factory=lambda: _float("WEIGHT_FUND_PHARMA_EPS_GROWTH", 0.20))
    w_fund_pharma_margin:     float = field(default_factory=lambda: _float("WEIGHT_FUND_PHARMA_MARGIN",     0.16))
    w_fund_pharma_dividend:   float = field(default_factory=lambda: _float("WEIGHT_FUND_PHARMA_DIVIDEND",   0.01))

    # ── News Sentiment Weights ────────────────────────────────────────────────
    sentiment_composite_weight: float = field(default_factory=lambda: _float("SENTIMENT_COMPOSITE_WEIGHT", 0.15))
    sentiment_keyword_weight:   float = field(default_factory=lambda: _float("SENTIMENT_KEYWORD_WEIGHT",   0.40))
    sentiment_max_age_hours:    int   = field(default_factory=lambda: _int  ("SENTIMENT_MAX_AGE_HOURS",    48))
    sentiment_recency_decay:    float = field(default_factory=lambda: _float("SENTIMENT_RECENCY_DECAY",    0.05))
    sentiment_min_articles:     int   = field(default_factory=lambda: _int  ("SENTIMENT_MIN_ARTICLES",     2))
    sentiment_cache_minutes:    int   = field(default_factory=lambda: _int  ("SENTIMENT_CACHE_MINUTES",    30))

    # ── IntraDayPulse — live price sensitivity during market hours ────────────
    # Blended into every stock's composite ONLY during 09:15–15:30 IST.
    # Uses the live candle injected by DataFetcher each tick.
    #
    # Formula (delta-based, identical to sentiment blender):
    #   delta = (pulse - 50) / 50            # −1 to +1
    #   boost = delta × weight × base        # proportional nudge
    #   new   = base + boost                 # pulse=50 → no change
    intraday_pulse_weight:     float = field(default_factory=lambda: _float("INTRADAY_PULSE_WEIGHT",     0.20))
    intraday_w_day_return:     float = field(default_factory=lambda: _float("INTRADAY_W_DAY_RETURN",     0.35))
    intraday_w_range_position: float = field(default_factory=lambda: _float("INTRADAY_W_RANGE_POSITION", 0.30))
    intraday_w_volume_pace:    float = field(default_factory=lambda: _float("INTRADAY_W_VOLUME_PACE",    0.25))
    intraday_w_open_distance:  float = field(default_factory=lambda: _float("INTRADAY_W_OPEN_DISTANCE",  0.10))
