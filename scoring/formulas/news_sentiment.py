"""
scoring/formulas/news_sentiment.py — News Sentiment Scorer.

Plugs into the scoring engine as the 4th pillar alongside
Technical, Fundamental, and Momentum scores.

Score interpretation
────────────────────
  75 – 100  →  Strong positive news flow  (product launches, earnings beats,
                                           upgrades, order wins)
  55 – 75   →  Mild positive / neutral     (routine coverage, no major events)
  45 – 55   →  Truly neutral               (no news or perfectly mixed)
  25 – 45   →  Mild negative               (downgrades, weak results)
   0 – 25   →  Strong negative news flow   (fraud, SEBI notice, guidance cut)

Role in composite score
────────────────────────
  By default this pillar carries 15% weight in the composite.
  Raise it (e.g. 25%) if you want the bot to be more news-reactive.
  Lower it (e.g. 5%) if you want the bot to stay mostly technical.

  You can also use it as a MULTIPLIER (not implemented here, but easy to add):
    if sentiment < 30 → suppress BUY signals regardless of technical score
    if sentiment > 75 → amplify composite by 10%
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from news.fetcher import NewsFetcher
    from news.sentiment import SentimentAnalyzer

log = logging.getLogger("NewsSentimentScorer")


class NewsSentimentScorer:
    """
    Scores a stock based on its recent news sentiment.

    Parameters
    ----------
    fetcher   : NewsFetcher   — fetches and caches news articles
    analyzer  : SentimentAnalyzer — scores articles to 0-100
    min_articles : int
        If fewer than this many articles are found, returns `no_news_score`
        (default 50 = neutral) instead of a potentially misleading score.
    no_news_score : float
        Score returned when there is insufficient news coverage.
        Default 50 (neutral) — absence of news is not a negative signal.
    """

    def __init__(
        self,
        fetcher: "NewsFetcher",
        analyzer: "SentimentAnalyzer",
        min_articles: int  = 2,
        no_news_score: float = 50.0,
    ) -> None:
        self._fetcher       = fetcher
        self._analyzer      = analyzer
        self._min_articles  = min_articles
        self._no_news_score = no_news_score

    def score(self, symbol: str, df: "pd.DataFrame" = None, fundamentals: dict = None) -> float:
        """
        Returns sentiment score for `symbol` in [0, 100].

        `df` and `fundamentals` are accepted for API compatibility with other
        scorers but are not used here.
        """
        try:
            articles = self._fetcher.get_articles(symbol)

            if len(articles) < self._min_articles:
                log.debug("%s: only %d article(s) found — returning neutral score.", symbol, len(articles))
                return self._no_news_score

            score = self._analyzer.score_articles(articles)
            log.debug("%s: %d articles → sentiment score = %.1f", symbol, len(articles), score)
            return score

        except Exception as exc:
            log.warning("Sentiment scoring failed for %s: %s", symbol, exc)
            return self._no_news_score

    def top_headlines(self, symbol: str, n: int = 5) -> list[str]:
        """Return the n most recent headlines for a symbol (useful for debugging)."""
        articles = self._fetcher.get_articles(symbol)
        return [f"[{a.source}] {a.title}" for a in articles[:n]]
