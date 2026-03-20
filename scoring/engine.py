"""
scoring/engine.py — ScoringEngine

Two-pass scoring pipeline
─────────────────────────
  Pass 1  (all symbols, no network):
    Technical + Fundamental + Momentum → composite score for every stock.
    Parallelised, pure CPU/cache — typically 10–30 seconds for 2,117 stocks.

  Pass 2  (top-N candidates only):
    Fetch news → run FinBERT → blend sentiment as 4th pillar.
    Only runs on `sentiment_top_n` stocks (default 100).
    Reduces news HTTP requests from ~8,400 → ~400 per tick.

Composite Score Formula
───────────────────────
  Without sentiment or intraday pulse:
    composite = sector_scorer.score()          → 0–100

  With sentiment blended in (Pass 2):
    delta   = (sentiment − 50) / 50            # −1.0 to +1.0
    boost   = delta × sentiment_weight × base
    blended = base + boost
    Default sentiment_weight = 0.15  (15%)

  With IntraDayPulse blended in (market hours only, inline in Pass 1):
    delta   = (pulse − 50) / 50                # −1.0 to +1.0
    boost   = delta × intraday_pulse_weight × base
    blended = base + boost
    Default intraday_pulse_weight = 0.20  (20%)

  Both use the same delta formula: score = 50 → delta = 0 → no change.
  Only stocks that are ACTUALLY moving intraday get a nudge.

Usage
-----
    engine = ScoringEngine(universe, fetcher, registry)
    scores = engine.run(symbols)           # list[StockScore], sorted by composite desc
    top20  = engine.top_n(symbols, n=20)
    df     = engine.to_dataframe(scores)   # pandas DataFrame for analysis
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd

from market_hours import is_market_open
from scoring.formulas.base import StockScore
from scoring.formulas.intraday_pulse import IntraDayPulse
from scoring.registry import ScoreRegistry

log = logging.getLogger("ScoringEngine")

_DEFAULT_WORKERS       = 8    # parallelism for scoring (CPU-bound is mild here)
_DEFAULT_SENTIMENT_N   = 50   # run news/FinBERT on top-N stocks only (pass 2)
_NEWS_WORKERS          = 8    # parallel workers for news fetching in pass 2


class ScoringEngine:
    def __init__(
        self,
        universe,                               # StockUniverse
        fetcher,                                # DataFetcher
        registry: ScoreRegistry,
        workers: int = _DEFAULT_WORKERS,
        sentiment_scorer=None,                  # NewsSentimentScorer | None
        sentiment_weight: float = 0.15,         # 0.0 = disable, 0.15 = default
        sentiment_top_n: int = _DEFAULT_SENTIMENT_N,  # only run news on top-N
        # IntraDayPulse — live market-hours sensitivity (set 0.0 to disable)
        intraday_pulse_weight:     float = 0.20,
        intraday_w_day_return:     float = 0.35,
        intraday_w_range_position: float = 0.30,
        intraday_w_volume_pace:    float = 0.25,
        intraday_w_open_distance:  float = 0.10,
    ) -> None:
        self._universe          = universe
        self._fetcher           = fetcher
        self._registry          = registry
        self._workers           = workers
        self._sentiment         = sentiment_scorer
        self._sentiment_weight  = sentiment_weight if sentiment_scorer else 0.0
        self._sentiment_top_n   = sentiment_top_n

        # IntraDayPulse scorer (stateless — one instance, reused every tick)
        self._intraday               = IntraDayPulse()
        self._intraday_weight        = intraday_pulse_weight
        self._intraday_w_day_return  = intraday_w_day_return
        self._intraday_w_range       = intraday_w_range_position
        self._intraday_w_volume      = intraday_w_volume_pace
        self._intraday_w_open        = intraday_w_open_distance

        if sentiment_scorer:
            log.info(
                "News sentiment enabled — weight=%.0f%%  (top-%d candidates only)",
                self._sentiment_weight * 100, self._sentiment_top_n,
            )
        if intraday_pulse_weight > 0:
            log.info(
                "IntraDayPulse enabled — weight=%.0f%%  "
                "(day_ret=%.0f%% range=%.0f%% vol=%.0f%% open=%.0f%%)",
                intraday_pulse_weight * 100,
                intraday_w_day_return * 100, intraday_w_range_position * 100,
                intraday_w_volume_pace * 100, intraday_w_open_distance * 100,
            )

    # ------------------------------------------------------------------
    # Main entry points
    # ------------------------------------------------------------------

    def run(self, symbols: list[str]) -> list[StockScore]:
        """
        Two-pass scoring for all supplied symbols.

        Pass 1 — Technical + Fundamental + Momentum for every symbol.
                  No network I/O; runs in parallel; typically 10–30 seconds.
        Pass 2 — News sentiment blended only into the top-N candidates.
                  Limits news HTTP requests to sentiment_top_n × 4 instead of
                  len(symbols) × 4.

        Returns list sorted by composite score (desc).
        """
        # ── Pass 1: score everything without sentiment ─────────────────
        scores = self._run_pass1(symbols)

        # ── Pass 2: blend news into the top-N only ─────────────────────
        if self._sentiment and self._sentiment_weight > 0:
            self._run_pass2(scores)

        scores.sort(key=lambda s: s.composite, reverse=True)
        if scores:
            log.info(
                "Scoring complete.  Top: %s (%.1f)   Bottom: %s (%.1f)",
                scores[0].symbol,  scores[0].composite,
                scores[-1].symbol, scores[-1].composite,
            )
        return scores

    def top_n(self, symbols: list[str], n: int = 20) -> list[StockScore]:
        return self.run(symbols)[:n]

    def bottom_n(self, symbols: list[str], n: int = 20) -> list[StockScore]:
        return self.run(symbols)[-n:]

    def score_one(self, symbol: str) -> Optional[StockScore]:
        """Score a single symbol (useful for on-demand re-scoring)."""
        s = self._score_one_no_sentiment(symbol)
        if s and self._sentiment and self._sentiment_weight > 0:
            self._blend_sentiment(s)
        return s

    def to_dataframe(self, scores: list[StockScore]) -> pd.DataFrame:
        """Convert a list of StockScore objects to a ranked DataFrame."""
        if not scores:
            return pd.DataFrame()
        rows = [s.to_dict() for s in scores]
        df = pd.DataFrame(rows)
        df.index = range(1, len(df) + 1)
        df.index.name = "rank"
        return df

    # ------------------------------------------------------------------
    # Sector breakdown helpers
    # ------------------------------------------------------------------

    def scores_by_sector(self, scores: list[StockScore]) -> dict[str, list[StockScore]]:
        """Group scored stocks by sector."""
        result: dict[str, list[StockScore]] = {}
        for s in scores:
            result.setdefault(s.sector, []).append(s)
        return result

    def top_n_per_sector(self, scores: list[StockScore], n: int = 5) -> dict[str, list[StockScore]]:
        """Return top N stocks per sector."""
        by_sector = self.scores_by_sector(scores)
        return {sector: stocks[:n] for sector, stocks in by_sector.items()}

    # ------------------------------------------------------------------
    # Pass 1 — Technical + Fundamental + Momentum (no news, no network)
    # ------------------------------------------------------------------

    def _run_pass1(self, symbols: list[str]) -> list[StockScore]:
        total = len(symbols)
        log.info("Pass 1 — scoring %d symbols (Technical + Fundamental + Momentum)…", total)
        scores: list[StockScore] = []
        done = 0
        _PROGRESS_EVERY = max(1, min(200, total // 10))

        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            futures = {pool.submit(self._score_one_no_sentiment, sym): sym for sym in symbols}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    scores.append(result)
                done += 1
                if done % _PROGRESS_EVERY == 0 or done == total:
                    if scores:
                        best = max(scores, key=lambda s: s.composite)
                        log.info(
                            "  Pass 1  [%d/%d]  %.0f%%  best so far: %s (%.1f)",
                            done, total, done / total * 100, best.symbol, best.composite,
                        )
                    else:
                        log.info("  Pass 1  [%d/%d]  %.0f%%", done, total, done / total * 100)

        scores.sort(key=lambda s: s.composite, reverse=True)
        log.info(
            "Pass 1 complete.  Top: %s (%.1f) — running Pass 2 on top-%d…",
            scores[0].symbol, scores[0].composite, self._sentiment_top_n,
        ) if scores else log.info("Pass 1 complete — no scores.")
        return scores

    # ------------------------------------------------------------------
    # Pass 2 — News sentiment blended into top-N only
    # ------------------------------------------------------------------

    def _run_pass2(self, scores: list[StockScore]) -> None:
        """
        Mutates the top-N StockScore objects in-place by blending
        news sentiment into their composite scores.
        """
        candidates = scores[: self._sentiment_top_n]
        total = len(candidates)
        log.info(
            "Pass 2 — fetching news + FinBERT for top-%d candidates (%d RSS feeds each)…",
            total, 4,
        )
        done = blended = 0
        _PROGRESS_EVERY = max(1, min(20, total // 5))

        # Use more parallel workers for news fetching (I/O bound, not CPU bound)
        news_workers = _NEWS_WORKERS
        with ThreadPoolExecutor(max_workers=news_workers) as pool:
            futures = {pool.submit(self._blend_sentiment, s): s.symbol for s in candidates}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    future.result()
                    blended += 1
                except Exception as exc:
                    log.debug("Sentiment blend failed for %s: %s", sym, exc)
                done += 1
                if done % _PROGRESS_EVERY == 0 or done == total:
                    log.info(
                        "  Pass 2  [%d/%d]  %.0f%%  ✓ %d blended",
                        done, total, done / total * 100, blended,
                    )

        log.info("Pass 2 complete — sentiment blended into %d/%d candidates.", blended, total)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _score_one_no_sentiment(self, symbol: str) -> Optional[StockScore]:
        """
        Technical + Fundamental + Momentum only. No network I/O.
        IntraDayPulse is also blended here when the market is open — it reads
        only cached OHLCV data (already in memory from get_ohlcv()), so there
        is no additional I/O cost.
        """
        symbol = symbol.upper()
        sector = self._universe.sector_of(symbol)
        df     = self._fetcher.get_ohlcv(symbol)
        fund   = self._fetcher.get_fundamentals(symbol)

        if df is None or df.empty:
            return StockScore(
                symbol=symbol, sector=sector, composite=0.0,
                components={"error": "no_data"},
            )

        scorer = self._registry.get(sector)
        try:
            stock_score = scorer.score(symbol=symbol, sector=sector, df=df, fundamentals=fund)
        except Exception as exc:
            log.warning("Scoring failed for %s: %s", symbol, exc)
            return StockScore(
                symbol=symbol, sector=sector, composite=0.0,
                components={"error": str(exc)},
            )

        # ── IntraDayPulse blend (market hours only) ───────────────────────────
        if self._intraday_weight > 0 and is_market_open():
            self._blend_intraday(stock_score, df)

        return stock_score

    def _blend_intraday(self, stock_score: StockScore, df) -> None:
        """
        Mutates stock_score in-place — blends IntraDayPulse into composite.

        Formula (delta-based, identical to sentiment blender):
            pulse         = IntraDayPulse.compute(df)         → 0–100
            delta         = (pulse − 50) / 50                 → −1.0 to +1.0
            boost         = delta × intraday_weight × base
            new_composite = base + boost

        Key: pulse=50 (flat day, mid-range, on-pace) → delta=0 → no change.

        Examples (base=68, weight=0.20):
            pulse=75  (+1.5% day, near high, vol surge) → boost=+3.4 → 71.4 ✓
            pulse=30  (−1.5% day, near low,  light vol) → boost=−8.2 → 59.8 ✗
            pulse=50  (perfectly neutral)               → boost= 0.0 → 68.0 ~
        """
        try:
            pulse_score, pulse_components = self._intraday.compute(
                df,
                w_day_return     = self._intraday_w_day_return,
                w_range_position = self._intraday_w_range,
                w_volume_pace    = self._intraday_w_volume,
                w_open_distance  = self._intraday_w_open,
            )
            base  = stock_score.composite
            delta = (pulse_score - 50.0) / 50.0          # −1.0 to +1.0
            boost = delta * self._intraday_weight * base
            stock_score.composite = round(
                max(0.0, min(100.0, base + boost)), 2
            )
            stock_score.components["intraday_pulse"] = round(pulse_score, 2)
            stock_score.components.update(pulse_components)
        except Exception as exc:
            log.debug("IntraDayPulse blend failed for %s: %s", stock_score.symbol, exc)

    def _blend_sentiment(self, stock_score: StockScore) -> None:
        """
        Mutates stock_score in-place — blends news sentiment into composite.

        Formula (delta-based, neutral-safe):
            delta   = (sentiment - 50) / 50      # maps 0-100 → -1.0 to +1.0
            boost   = delta × weight × base       # proportional nudge
            blended = base + boost

        Key property: sentiment = 50 (neutral / no news) → delta = 0 → no change.
        Only strong positive (>50) or negative (<50) news moves the score.

        Examples with base=68, weight=0.15:
            sentiment=50  (neutral)  → boost=0.00  → blended=68.0  (unchanged)
            sentiment=80  (positive) → boost=+3.06 → blended=71.1  (buy signal!)
            sentiment=20  (negative) → boost=-3.06 → blended=64.9  (no buy)
        """
        try:
            df   = self._fetcher.get_ohlcv(stock_score.symbol)
            fund = self._fetcher.get_fundamentals(stock_score.symbol)
            sentiment_score = self._sentiment.score(stock_score.symbol, df, fund)
            base  = stock_score.composite
            delta = (sentiment_score - 50.0) / 50.0   # -1.0 to +1.0
            boost = delta * self._sentiment_weight * base
            blended = max(0.0, min(100.0, base + boost))
            stock_score.components["sentiment"] = round(sentiment_score, 2)
            stock_score.composite = round(blended, 2)
        except Exception as exc:
            log.debug("Sentiment blend failed for %s: %s", stock_score.symbol, exc)
