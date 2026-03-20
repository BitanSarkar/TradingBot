"""
news/fetcher.py — Fetches and caches news articles for each stock symbol.

Flow
────
  1. For each symbol, build URLs from SYMBOL_SOURCES (company-specific feeds).
  2. Parse RSS feeds via feedparser.
  3. Cache results for `cache_minutes` minutes (default 30) to avoid hammering feeds.
  4. Return a flat list of Article dicts, sorted newest-first.

Article dict schema
───────────────────
  {
    "title":     str,
    "summary":   str,
    "url":       str,
    "published": datetime,   # UTC
    "source":    str,        # source name
    "tier":      int,        # 1 / 2 / 3
  }
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

import feedparser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from news.sources import (
    SYMBOL_SOURCES, MARKET_SOURCES, TIER_WEIGHT, NewsSource,
)

log = logging.getLogger("NewsFetcher")

# ── Shared HTTP session (sized pool, retry on transient errors) ───────────────
#   Pool size = 8 news workers × 4 feeds = up to 32 concurrent connections.
#   Using one session avoids "Connection pool is full" warnings and enables
#   TCP keep-alive reuse across multiple feeds.
_NEWS_POOL_SIZE = 32

def _make_session() -> requests.Session:
    retry = Retry(
        total=2,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(
        pool_connections = _NEWS_POOL_SIZE,
        pool_maxsize     = _NEWS_POOL_SIZE,
        max_retries      = retry,
    )
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0 (compatible; TradingBot/1.0)"
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session

_http: requests.Session = _make_session()

# ── Company name lookup ───────────────────────────────────────────────────────
# Maps NSE symbols → common company name used in news searches
# The universe module fills this at startup; we keep a lightweight default here.
_SYMBOL_TO_NAME: dict[str, str] = {}


def register_company_names(mapping: dict[str, str]) -> None:
    """Called by StockUniverse at startup to populate the name lookup."""
    _SYMBOL_TO_NAME.update(mapping)


def _company_name(symbol: str) -> str:
    return _SYMBOL_TO_NAME.get(symbol, symbol)


# ── Article ───────────────────────────────────────────────────────────────────

@dataclass
class Article:
    title: str
    summary: str
    url: str
    published: datetime
    source: str
    tier: int

    @property
    def text(self) -> str:
        """Full text used for sentiment scoring."""
        return f"{self.title}. {self.summary}"

    @property
    def age_hours(self) -> float:
        now = datetime.now(timezone.utc)
        pub = self.published
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        return (now - pub).total_seconds() / 3600


# ── Cache ─────────────────────────────────────────────────────────────────────

@dataclass
class _CacheEntry:
    articles: list[Article]
    fetched_at: float = field(default_factory=time.time)


_cache: dict[str, _CacheEntry] = {}   # symbol → CacheEntry
_market_cache: Optional[_CacheEntry] = None


# ── Core fetcher ──────────────────────────────────────────────────────────────

class NewsFetcher:
    """
    Fetch and cache news articles for NSE-listed stocks.

    Parameters
    ----------
    cache_minutes : int
        How long to reuse cached articles before re-fetching.
    max_age_hours : int
        Discard articles older than this many hours.
    max_articles  : int
        Max articles to return per symbol (newest first).
    """

    def __init__(
        self,
        cache_minutes: int = 30,
        max_age_hours: int = 48,
        max_articles: int = 20,
    ) -> None:
        self._cache_ttl   = cache_minutes * 60
        self._max_age_hrs = max_age_hours
        self._max_articles = max_articles

    # ── Public API ────────────────────────────────────────────────────────────

    def get_articles(self, symbol: str) -> list[Article]:
        """
        Return recent news articles for a symbol, newest first.
        Results are cached for `cache_minutes` minutes.
        """
        global _cache

        entry = _cache.get(symbol)
        if entry and (time.time() - entry.fetched_at) < self._cache_ttl:
            return entry.articles

        articles = self._fetch_symbol(symbol)
        _cache[symbol] = _CacheEntry(articles=articles)
        return articles

    def get_market_articles(self) -> list[Article]:
        """Return general market news (not symbol-specific)."""
        global _market_cache

        if _market_cache and (time.time() - _market_cache.fetched_at) < self._cache_ttl:
            return _market_cache.articles

        articles: list[Article] = []
        for source in MARKET_SOURCES:
            articles += self._fetch_feed(source.url_template, source)

        articles = self._dedupe_sort(articles)
        _market_cache = _CacheEntry(articles=articles)
        return articles

    # ── Internal ──────────────────────────────────────────────────────────────

    def _fetch_symbol(self, symbol: str) -> list[Article]:
        company = _company_name(symbol)
        articles: list[Article] = []

        for source in SYMBOL_SOURCES:
            url = source.url_template.format(
                symbol=symbol,
                company=requests.utils.quote(company),
            )
            fetched = self._fetch_feed(url, source)
            articles += fetched

        # Also grab relevant articles from general market feeds
        market = self.get_market_articles()
        kw = {symbol.lower(), company.lower().split()[0]}  # quick keyword match
        for art in market:
            if any(k in art.title.lower() or k in art.summary.lower() for k in kw):
                articles.append(art)

        return self._dedupe_sort(articles)[: self._max_articles]

    def _fetch_feed(self, url: str, source: NewsSource) -> list[Article]:
        try:
            resp = _http.get(url, timeout=5)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
        except Exception as exc:
            log.debug("Feed fetch failed [%s]: %s", source.name, exc)
            return []

        articles: list[Article] = []
        for entry in feed.entries:
            published = self._parse_date(entry)
            if published is None:
                continue

            art = Article(
                title    = entry.get("title", "").strip(),
                summary  = entry.get("summary", entry.get("description", "")).strip(),
                url      = entry.get("link", ""),
                published= published,
                source   = source.name,
                tier     = source.tier,
            )

            if art.age_hours > self._max_age_hrs:
                continue  # too old

            articles.append(art)

        return articles

    @staticmethod
    def _parse_date(entry) -> Optional[datetime]:
        for attr in ("published_parsed", "updated_parsed", "created_parsed"):
            t = getattr(entry, attr, None)
            if t:
                try:
                    return datetime(*t[:6], tzinfo=timezone.utc)
                except Exception:
                    pass
        return None

    @staticmethod
    def _dedupe_sort(articles: list[Article]) -> list[Article]:
        seen: set[str] = set()
        unique: list[Article] = []
        for art in articles:
            key = art.title[:60].lower().strip()
            if key and key not in seen:
                seen.add(key)
                unique.append(art)
        return sorted(unique, key=lambda a: a.published, reverse=True)
