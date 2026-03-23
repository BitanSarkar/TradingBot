"""
news/sources.py — Registry of all Indian financial news RSS feeds.

Each source is tagged with a reliability tier:
  TIER_1 — Authoritative, low noise  (ET, Mint, BSE, NSE, Reuters)
  TIER_2 — Good coverage             (Moneycontrol, Business Standard via GNews)
  TIER_3 — High volume, more noise   (Google News, social aggregators)

Weight in sentiment blending: TIER_1 > TIER_2 > TIER_3

AWS/EC2 compatibility notes
────────────────────────────
Many Indian news portals (Moneycontrol, Business Standard, NDTV Profit via
Feedburner, LiveMint) block requests from AWS IP ranges with HTTP 403.
All sources below are routed through Google News RSS or use feeds that are
known to respond correctly from cloud servers.  Direct-source URLs are only
used where the publisher explicitly allows programmatic access (BSE XML,
Economic Times CMS feeds).  Reuters direct RSS (feeds.reuters.com) fails
DNS resolution from AWS IPs and is routed through Google News.
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
    # CMS feed — served from Indiatimes CDN, works from AWS
    url_template="https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms",
    tier=TIER_1,
    is_symbol_specific=False,
)

ECONOMIC_TIMES_COMPANY = NewsSource(
    name="Economic Times — Company Search",
    # Google News scoped to ET — avoids direct ET bot-blocking
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
    # feeds.reuters.com DNS fails from AWS IPs — route through Google News instead
    url_template=(
        "https://news.google.com/rss/search"
        "?q=India+business+economy+markets+site:reuters.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=False,
)

REUTERS_COMPANY = NewsSource(
    name="Reuters — Company Search",
    url_template=(
        "https://news.google.com/rss/search"
        "?q={company}+site:reuters.com"
        "&hl=en-IN&gl=IN&ceid=IN:en"
    ),
    tier=TIER_1,
    is_symbol_specific=True,
)

# ── TIER 2 — Good coverage ──────────────────────────────────────────────────

# Routed via Google News to avoid direct 403 blocks on AWS IPs
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
    # Direct RSS — THNBL serves open RSS feeds correctly from cloud IPs
    url_template="https://www.thehindubusinessline.com/markets/?service=rss",
    tier=TIER_2,
    is_symbol_specific=False,
)

FINANCIAL_EXPRESS_MARKET = NewsSource(
    name="Financial Express — Markets (via GNews)",
    # FE direct feed sometimes 403s from AWS; route through Google News instead
    url_template=(
        "https://news.google.com/rss/search"
        "?q=NSE+BSE+stocks+site:financialexpress.com"
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

# Symbol-specific sources (fetched with company name / ticker substituted)
# All routed through Google News → no direct-site 403 risk from EC2
SYMBOL_SOURCES: list[NewsSource] = [
    ECONOMIC_TIMES_COMPANY,    # TIER_1
    REUTERS_COMPANY,           # TIER_1
    MONEYCONTROL_COMPANY,      # TIER_2
    BUSINESS_STANDARD_COMPANY, # TIER_2
    BUSINESS_LINE_COMPANY,     # TIER_2
    MINT_COMPANY,              # TIER_2
    GOOGLE_NEWS_COMPANY,       # TIER_3
    GOOGLE_NEWS_SYMBOL,        # TIER_3
]

# General market feeds (fetched once, articles filtered by keyword)
# Mix: one direct CDN feed (ET), one open RSS (Reuters, THNBL) + Google News fallbacks
MARKET_SOURCES: list[NewsSource] = [
    ECONOMIC_TIMES_MARKET,     # TIER_1 — ET CDN, works from AWS
    REUTERS_INDIA,             # TIER_1 — Reuters open RSS
    BUSINESS_LINE_MARKET,      # TIER_2 — THNBL open RSS
    FINANCIAL_EXPRESS_MARKET,  # TIER_2 — via Google News
    GOOGLE_NEWS_INDIA_MARKETS, # TIER_3 — broadest fallback
]

# Tier weights used during sentiment blending
TIER_WEIGHT: dict[int, float] = {
    TIER_1: 1.0,
    TIER_2: 0.7,
    TIER_3: 0.4,
}
