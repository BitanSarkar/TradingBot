"""
news/sources.py — Registry of all Indian financial news RSS feeds.

Each source is tagged with a reliability tier:
  TIER_1 — Authoritative, low noise  (ET, Mint, BSE, NSE)
  TIER_2 — Good coverage             (Moneycontrol, Business Standard)
  TIER_3 — High volume, more noise   (Google News, social aggregators)

Weight in sentiment blending: TIER_1 > TIER_2 > TIER_3
"""

from __future__ import annotations
from dataclasses import dataclass

TIER_1 = 1
TIER_2 = 2
TIER_3 = 3


@dataclass(frozen=True)
class NewsSource:
    name: str
    url_template: str   # use {symbol} and {company} as placeholders
    tier: int
    is_symbol_specific: bool  # True = one feed per stock, False = general market feed


# ── TIER 1 — Authoritative ─────────────────────────────────────────────────

BSE_ANNOUNCEMENTS = NewsSource(
    name="BSE Corporate Announcements",
    # BSE public XML feed for corporate filings — earnings, board meetings, buybacks
    url_template="https://www.bseindia.com/xml-data/corpfiling/AttachHis/{symbol}.xml",
    tier=TIER_1,
    is_symbol_specific=True,
)

ECONOMIC_TIMES_MARKET = NewsSource(
    name="Economic Times — Markets",
    url_template="https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    tier=TIER_1,
    is_symbol_specific=False,
)

ECONOMIC_TIMES_COMPANY = NewsSource(
    name="Economic Times — Company Search",
    # Google News scoped to ET, searching for the company name
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:economictimes.indiatimes.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=True,
)

LIVEMINT = NewsSource(
    name="LiveMint — Markets",
    url_template="https://www.livemint.com/rss/markets",
    tier=TIER_1,
    is_symbol_specific=False,
)

# ── TIER 2 — Good coverage ──────────────────────────────────────────────────

NDTV_PROFIT = NewsSource(
    name="NDTV Profit — Markets",
    url_template="https://feeds.feedburner.com/ndtvprofit-latest",
    tier=TIER_2,
    is_symbol_specific=False,
)

FINANCIAL_EXPRESS = NewsSource(
    name="Financial Express — Markets",
    url_template="https://www.financialexpress.com/market/feed/",
    tier=TIER_2,
    is_symbol_specific=False,
)

MONEYCONTROL_COMPANY = NewsSource(
    name="Moneycontrol — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:moneycontrol.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=True,
)

# ── TIER 3 — High volume, filtered ──────────────────────────────────────────

GOOGLE_NEWS_COMPANY = NewsSource(
    name="Google News — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+NSE+stock+India"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_3,
    is_symbol_specific=True,
)

GOOGLE_NEWS_SYMBOL = NewsSource(
    name="Google News — Ticker",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={symbol}+NSE+share+price"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_3,
    is_symbol_specific=True,
)


# ── What gets fetched per symbol ──────────────────────────────────────────────

#  symbol-specific sources (fetched with company name / ticker)
SYMBOL_SOURCES: list[NewsSource] = [
    ECONOMIC_TIMES_COMPANY,
    MONEYCONTROL_COMPANY,
    GOOGLE_NEWS_COMPANY,
    GOOGLE_NEWS_SYMBOL,
]

# General market feeds (fetched once, articles filtered by keyword)
MARKET_SOURCES: list[NewsSource] = [
    ECONOMIC_TIMES_MARKET,
    LIVEMINT,
    NDTV_PROFIT,
    FINANCIAL_EXPRESS,
]

# Tier weights used during sentiment blending
TIER_WEIGHT: dict[int, float] = {
    TIER_1: 1.0,
    TIER_2: 0.7,
    TIER_3: 0.4,
}
