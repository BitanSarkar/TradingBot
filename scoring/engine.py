"""
scoring/engine.py — ScoringEngine

Orchestrates the full scoring pipeline:
  1. Accepts a list of symbols
  2. Looks up each symbol's sector from the universe
  3. Pulls OHLCV + fundamentals from the data fetcher
  4. Delegates to the correct sector scorer (via registry)
  5. Returns a ranked list of StockScore objects

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

from scoring.formulas.base import StockScore
from scoring.registry import ScoreRegistry

log = logging.getLogger("ScoringEngine")

_DEFAULT_WORKERS = 8   # parallelism for scoring (CPU-bound is mild here)


class ScoringEngine:
    def __init__(
        self,
        universe,           # StockUniverse
        fetcher,            # DataFetcher
        registry: ScoreRegistry,
        workers: int = _DEFAULT_WORKERS,
    ) -> None:
        self._universe = universe
        self._fetcher  = fetcher
        self._registry = registry
        self._workers  = workers

    # ------------------------------------------------------------------
    # Main entry points
    # ------------------------------------------------------------------

    def run(self, symbols: list[str]) -> list[StockScore]:
        """
        Score all supplied symbols.  Returns list sorted by composite score (desc).
        Symbols with no data get score=0 and are placed at the bottom.
        """
        log.info("Scoring %d symbols...", len(symbols))
        scores = []

        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            futures = {pool.submit(self._score_one, sym): sym for sym in symbols}
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    scores.append(result)

        scores.sort(key=lambda s: s.composite, reverse=True)
        log.info("Scoring complete. Top: %s (%.1f)  Bottom: %s (%.1f)",
                 scores[0].symbol,  scores[0].composite,
                 scores[-1].symbol, scores[-1].composite) if scores else None
        return scores

    def top_n(self, symbols: list[str], n: int = 20) -> list[StockScore]:
        return self.run(symbols)[:n]

    def bottom_n(self, symbols: list[str], n: int = 20) -> list[StockScore]:
        return self.run(symbols)[-n:]

    def score_one(self, symbol: str) -> Optional[StockScore]:
        """Score a single symbol (useful for on-demand re-scoring)."""
        return self._score_one(symbol)

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
    # Internal
    # ------------------------------------------------------------------

    def _score_one(self, symbol: str) -> Optional[StockScore]:
        symbol  = symbol.upper()
        sector  = self._universe.sector_of(symbol)
        df      = self._fetcher.get_ohlcv(symbol)
        fund    = self._fetcher.get_fundamentals(symbol)

        if df is None or df.empty:
            return StockScore(symbol=symbol, sector=sector, composite=0.0,
                              components={"error": "no_data"})

        scorer = self._registry.get(sector)
        try:
            return scorer.score(symbol=symbol, sector=sector, df=df, fundamentals=fund)
        except Exception as exc:
            log.warning("Scoring failed for %s: %s", symbol, exc, exc_info=False)
            return StockScore(symbol=symbol, sector=sector, composite=0.0,
                              components={"error": str(exc)})
