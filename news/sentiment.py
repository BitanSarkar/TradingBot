"""
news/sentiment.py — Sentiment scoring for financial news articles.

Two backends, selected at startup:

  VADER (default — lightweight, no download)
  ──────────────────────────────────────────
  • Rule-based lexicon developed at Georgia Tech.
  • Works offline, no API key, instant.
  • Good enough for headlines; misses subtle financial language.
  • `pip install vaderSentiment`

  FinBERT (optional — requires ~400 MB model download)
  ─────────────────────────────────────────────────────
  • BERT model fine-tuned specifically on financial text
    (10-K filings, analyst reports, earnings transcripts).
  • Far better at: "revenue miss", "guidance cut", "margin compression".
  • Enable with: SentimentAnalyzer(backend="finbert")
  • `pip install transformers torch`

Signal Boosters
───────────────
On top of the raw VADER/FinBERT score, we run a keyword pass that detects
high-conviction financial events and directly adjusts the sentiment score:

  Strongly Positive (+0.3 to +0.5 boost)
    "product launch", "record revenue", "buyback", "order win",
    "capacity expansion", "debt free", "dividend hike", "upgrade"

  Strongly Negative (-0.3 to -0.5 hit)
    "recall", "fraud", "penalty", "sebi notice", "promoter pledge",
    "earnings miss", "guidance cut", "plant shutdown", "insolvency"

This is what makes the bot catch your "product launch + good survey"
example — even if VADER misses the nuance, the keyword booster fires.
"""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from news.fetcher import Article

log = logging.getLogger("Sentiment")


# ── Keyword signal boosters ───────────────────────────────────────────────────
# (keyword_fragment, sentiment_delta)
# Delta is added directly to the raw -1..+1 compound score, then clamped.

POSITIVE_EVENTS: list[tuple[str, float]] = [
    # Product / business wins
    ("product launch",        +0.40),
    ("new product",           +0.30),
    ("order win",             +0.40),
    ("order received",        +0.35),
    ("capacity expansion",    +0.30),
    ("new plant",             +0.25),
    ("new facility",          +0.25),
    ("partnership",           +0.20),
    ("joint venture",         +0.20),
    ("export order",          +0.30),
    # Financial milestones
    ("record revenue",        +0.45),
    ("record profit",         +0.45),
    ("highest ever",          +0.40),
    ("beat estimates",        +0.40),
    ("earnings beat",         +0.40),
    ("guidance raised",       +0.40),
    ("buyback",               +0.30),
    ("dividend hike",         +0.35),
    ("special dividend",      +0.30),
    ("debt free",             +0.35),
    ("debt reduction",        +0.25),
    # Analyst / rating
    ("upgrade",               +0.30),
    ("target raised",         +0.35),
    ("buy rating",            +0.30),
    ("strong buy",            +0.35),
    # Market events
    ("52-week high",          +0.25),
    ("all-time high",         +0.35),
    ("breakout",              +0.20),
    ("fii buying",            +0.25),
    ("bulk deal buy",         +0.25),
    # Regulatory / structural
    ("approved",              +0.20),
    ("cleared",               +0.15),
    ("nod received",          +0.20),
    ("listing gain",          +0.25),
    # Sentiment / survey
    ("positive survey",       +0.35),
    ("high demand",           +0.30),
    ("strong demand",         +0.30),
    ("sold out",              +0.25),
    ("oversubscribed",        +0.30),
]

NEGATIVE_EVENTS: list[tuple[str, float]] = [
    # Fraud / legal
    ("fraud",                 -0.50),
    ("scam",                  -0.50),
    ("sebi notice",           -0.45),
    ("sebi order",            -0.45),
    ("penalty",               -0.35),
    ("fine imposed",          -0.35),
    ("cbi raid",              -0.50),
    ("ed raid",               -0.50),
    ("tax evasion",           -0.45),
    ("forensic audit",        -0.40),
    ("accounting fraud",      -0.50),
    # Product / operations
    ("recall",                -0.40),
    ("plant shutdown",        -0.35),
    ("factory fire",          -0.30),
    ("production halt",       -0.35),
    ("strike",                -0.25),
    # Financial distress
    ("earnings miss",         -0.40),
    ("revenue miss",          -0.40),
    ("guidance cut",          -0.45),
    ("profit warning",        -0.45),
    ("loss widened",          -0.40),
    ("margin compression",    -0.35),
    ("debt default",          -0.50),
    ("insolvency",            -0.50),
    ("nclt",                  -0.45),
    ("promoter pledge",       -0.30),
    ("promoter selling",      -0.25),
    # Analyst / rating
    ("downgrade",             -0.35),
    ("target cut",            -0.35),
    ("sell rating",           -0.30),
    ("underperform",          -0.25),
    # Market events
    ("52-week low",           -0.25),
    ("fii selling",           -0.25),
    ("bulk deal sell",        -0.25),
    # Regulatory
    ("ban",                   -0.40),
    ("rejected",              -0.25),
    ("licence cancelled",     -0.45),
]


def _keyword_delta(text: str) -> float:
    """Scan text for high-conviction financial events. Returns raw delta (-1..+1)."""
    text_lower = text.lower()
    delta = 0.0
    for kw, boost in POSITIVE_EVENTS:
        if kw in text_lower:
            delta += boost
    for kw, hit in NEGATIVE_EVENTS:
        if kw in text_lower:
            delta += hit   # hit is already negative
    return max(-1.0, min(1.0, delta))


# ── VADER backend ─────────────────────────────────────────────────────────────

class _VaderBackend:
    def __init__(self) -> None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        self._sia = SentimentIntensityAnalyzer()
        log.info("Sentiment backend: VADER (lightweight, offline)")

    def score(self, text: str) -> float:
        return float(self._sia.polarity_scores(text)["compound"])


# ── FinBERT backend ───────────────────────────────────────────────────────────

class _FinBERTBackend:
    def __init__(self) -> None:
        log.info("Loading FinBERT model (first run downloads ~400 MB)...")
        from transformers import pipeline
        self._pipe = pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            top_k=None,
            truncation=True,
            max_length=512,
        )
        log.info("Sentiment backend: FinBERT (financial-domain BERT)")

    def score(self, text: str) -> float:
        results = self._pipe(text[:512])[0]  # list of {label, score}
        label_score = {r["label"]: r["score"] for r in results}
        # FinBERT labels: positive / negative / neutral
        return label_score.get("positive", 0) - label_score.get("negative", 0)


# ── Public class ──────────────────────────────────────────────────────────────

class SentimentAnalyzer:
    """
    Score a list of news articles for a stock and return a 0–100 sentiment score.

    Parameters
    ----------
    backend : str
        "vader"   — fast, offline, no download  (default)
        "finbert" — accurate, financial-domain, ~400 MB model download
    keyword_weight : float
        Weight given to the keyword booster vs the NLP model score.
        0.0 = pure NLP,  1.0 = pure keywords,  0.4 = balanced (default)
    recency_decay : float
        Exponential decay per hour of article age.
        0.05 means an article 14h old has half the weight of a fresh one.
    """

    def __init__(
        self,
        backend: str = "vader",
        keyword_weight: float = 0.4,
        recency_decay: float = 0.05,
    ) -> None:
        if backend == "finbert":
            self._backend = _FinBERTBackend()
        else:
            self._backend = _VaderBackend()

        self._kw_weight = keyword_weight
        self._decay     = recency_decay

    def score_articles(self, articles: list["Article"]) -> float:
        """
        Blend NLP + keyword signals across all articles, weighted by recency and tier.
        Returns a score in [0, 100].  50 = neutral.
        """
        if not articles:
            return 50.0   # neutral when no news

        from news.sources import TIER_WEIGHT

        total_weight = 0.0
        weighted_sum = 0.0

        for art in articles:
            # Recency weight: e^(-decay × age_hours)
            recency_w = math.exp(-self._decay * art.age_hours)
            # Tier weight: TIER_1 = 1.0, TIER_2 = 0.7, TIER_3 = 0.4
            tier_w = TIER_WEIGHT.get(art.tier, 0.4)
            w = recency_w * tier_w

            nlp_score = self._backend.score(art.text)          # -1 .. +1
            kw_delta  = _keyword_delta(art.text)               # -1 .. +1

            # Blend NLP and keyword
            blended = (
                nlp_score * (1 - self._kw_weight)
                + kw_delta  * self._kw_weight
            )
            blended = max(-1.0, min(1.0, blended))

            weighted_sum  += blended * w
            total_weight  += w

        if total_weight == 0:
            return 50.0

        compound = weighted_sum / total_weight   # -1 .. +1
        return round((compound + 1) / 2 * 100, 2)   # map to 0 – 100

    def score_symbol(self, symbol: str, fetcher) -> float:
        """Convenience: fetch articles and score in one call."""
        articles = fetcher.get_articles(symbol)
        return self.score_articles(articles)
