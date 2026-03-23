"""
news/sources.py — Registry of all Indian financial news RSS feeds.

Each source is tagged with a reliability tier:
  TIER_1 — Authoritative, low noise  (ET, Reuters, CNBCTV18, NDTV Profit, BQ Prime)
  TIER_2 — Good coverage             (Moneycontrol, Business Standard, Mint, Zee Business, etc.)
  TIER_3 — High volume, more noise   (Google News broad aggregations)

Weight in sentiment blending: TIER_1 > TIER_2 > TIER_3

AWS/EC2 compatibility
──────────────────────
Direct RSS from Indian portals (Moneycontrol, Business Standard, LiveMint,
NDTV Profit, Reuters feeds.reuters.com) all fail from AWS IPs — either
HTTP 403 or DNS resolution failure.

Strategy: route everything through Google News RSS which is never blocked
on AWS.  Google News indexes all major Indian financial publications and
serves their articles as RSS from news.google.com.

The only direct-URL sources kept are:
  • Economic Times CMS feed  — served from Indiatimes CDN, AWS-safe
  • Hindu Business Line RSS  — tested working from cloud IPs

All others: news.google.com/rss/search?q=...+site:<publisher.com>
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


# ── TIER 1 — Authoritative ────────────────────────────────────────────────────

ECONOMIC_TIMES_MARKET = NewsSource(
    name="Economic Times — Markets",
    url_template="https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    tier=TIER_1,
    is_symbol_specific=False,
)

ECONOMIC_TIMES_COMPANY = NewsSource(
    name="Economic Times — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:economictimes.indiatimes.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=True,
)

REUTERS_INDIA = NewsSource(
    name="Reuters — India Business",
    # feeds.reuters.com DNS fails from AWS — route through Google News
    url_template=(
        "https://news.google.com/rss/search"
        "?q=India+business+economy+markets+site:reuters.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=False,
)

REUTERS_COMPANY = NewsSource(
    name="Reuters — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:reuters.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=True,
)

CNBCTV18_MARKET = NewsSource(
    name="CNBC TV18 — Markets",
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+stocks+markets+site:cnbctv18.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=False,
)

CNBCTV18_COMPANY = NewsSource(
    name="CNBC TV18 — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:cnbctv18.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=True,
)

NDTV_PROFIT_MARKET = NewsSource(
    name="NDTV Profit — Markets",
    # ndtvprofit.com blocks AWS directly; route through Google News
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+Nifty+stocks+site:ndtvprofit.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=False,
)

NDTV_PROFIT_COMPANY = NewsSource(
    name="NDTV Profit — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:ndtvprofit.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=True,
)

BQ_PRIME_MARKET = NewsSource(
    name="BQ Prime — Markets",
    # Bloomberg Quint India — high quality financial journalism
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+stocks+markets+site:bqprime.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=False,
)

BQ_PRIME_COMPANY = NewsSource(
    name="BQ Prime — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:bqprime.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=True,
)


# ── TIER 2 — Good coverage ────────────────────────────────────────────────────

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

BUSINESS_STANDARD_COMPANY = NewsSource(
    name="Business Standard — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:business-standard.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=True,
)

BUSINESS_STANDARD_MARKET = NewsSource(
    name="Business Standard — Markets",
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+Sensex+Nifty+site:business-standard.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=False,
)

BUSINESS_LINE_COMPANY = NewsSource(
    name="The Hindu Business Line — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:thehindubusinessline.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=True,
)

BUSINESS_LINE_MARKET = NewsSource(
    name="The Hindu Business Line — Markets",
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+stocks+markets+site:thehindubusinessline.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=False,
)

MINT_COMPANY = NewsSource(
    name="LiveMint — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:livemint.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=True,
)

MINT_MARKET = NewsSource(
    name="LiveMint — Markets",
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+Sensex+Nifty+stocks+site:livemint.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=False,
)

FINANCIAL_EXPRESS_COMPANY = NewsSource(
    name="Financial Express — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:financialexpress.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=True,
)

FINANCIAL_EXPRESS_MARKET = NewsSource(
    name="Financial Express — Markets",
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+stocks+site:financialexpress.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=False,
)

ZEE_BUSINESS_COMPANY = NewsSource(
    name="Zee Business — Company",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:zeebiz.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=True,
)

ZEE_BUSINESS_MARKET = NewsSource(
    name="Zee Business — Markets",
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+Nifty+stocks+site:zeebiz.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_2,
    is_symbol_specific=False,
)


# ── TIER 3 — High volume, filtered ───────────────────────────────────────────

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

GOOGLE_NEWS_INDIA_MARKETS = NewsSource(
    name="Google News — India Markets",
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+Sensex+Nifty+stocks"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_3,
    is_symbol_specific=False,
)


# ── What gets fetched per symbol ──────────────────────────────────────────────
#
# 12 symbol-specific sources — all via Google News, guaranteed EC2-safe.
# Covers ET, Reuters, CNBCTV18, NDTV Profit, BQ Prime (TIER_1) plus
# Moneycontrol, Business Standard, Business Line, Mint, Financial Express,
# Zee Business (TIER_2) plus broad Google News fallback (TIER_3).

SYMBOL_SOURCES: list[NewsSource] = [
    # TIER_1 — authoritative
    ECONOMIC_TIMES_COMPANY,
    REUTERS_COMPANY,
    CNBCTV18_COMPANY,
    NDTV_PROFIT_COMPANY,
    BQ_PRIME_COMPANY,
    # TIER_2 — good coverage
    MONEYCONTROL_COMPANY,
    BUSINESS_STANDARD_COMPANY,
    BUSINESS_LINE_COMPANY,
    MINT_COMPANY,
    FINANCIAL_EXPRESS_COMPANY,
    ZEE_BUSINESS_COMPANY,
    # TIER_3 — broad fallback
    GOOGLE_NEWS_COMPANY,
    GOOGLE_NEWS_SYMBOL,
]

# ── General market feeds ──────────────────────────────────────────────────────
#
# 10 market-wide feeds fetched once per tick, articles filtered by keyword.
# All via Google News except ET (CDN-served, AWS-safe).

MARKET_SOURCES: list[NewsSource] = [
    # TIER_1
    ECONOMIC_TIMES_MARKET,
    REUTERS_INDIA,
    CNBCTV18_MARKET,
    NDTV_PROFIT_MARKET,
    BQ_PRIME_MARKET,
    # TIER_2
    BUSINESS_STANDARD_MARKET,
    BUSINESS_LINE_MARKET,
    MINT_MARKET,
    FINANCIAL_EXPRESS_MARKET,
    ZEE_BUSINESS_MARKET,
    # TIER_3
    GOOGLE_NEWS_INDIA_MARKETS,
]

# Tier weights used during sentiment blending
TIER_WEIGHT: dict[int, float] = {
    TIER_1: 1.0,
    TIER_2: 0.7,
    TIER_3: 0.4,
}
