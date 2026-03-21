"""
strategies/score_based.py — Score-based trading strategy.

How it works
------------
On every tick:
  1. Refresh stale market data (OHLCV + fundamentals) from yfinance.
  2. Run the ScoringEngine on the entire stock universe.
  3. Any stock with composite score >= BUY_THRESHOLD that you don't hold → BUY.
  4. Any stock you hold whose score drops below SELL_THRESHOLD → SELL.
  5. Never hold more than MAX_HOLDINGS stocks simultaneously.

Thresholds are fully configurable in Config (or overridden at runtime).

This file intentionally has NO hard-coded alpha — the scoring registry
is where the "intelligence" lives, and you control it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from strategies.base import BaseStrategy, Signal, TradeSignal

if TYPE_CHECKING:
    from scoring.engine import ScoringEngine
    from universe import StockUniverse
    from data.fetcher import DataFetcher
    from config import Config
    from orders import OrderManager
    from positions import PositionTracker

log = logging.getLogger("ScoreBasedStrategy")


class ScoreBasedStrategy(BaseStrategy):
    """
    Parameters (all in Config)
    --------------------------
    score_buy_threshold  : float  — minimum composite score to trigger BUY  (default 70)
    score_sell_threshold : float  — composite score below which we SELL      (default 40)
    max_holdings         : int    — max simultaneous stock positions          (default 10)
    quantity_per_trade   : int    — fixed qty per order (1 = safe start)      (default 1)
    score_top_n          : int    — only consider top-N scored stocks for buy (default 50)
    """

    def __init__(
        self,
        config: "Config",
        orders: "OrderManager",
        positions: "PositionTracker",
        universe: "StockUniverse",
        fetcher: "DataFetcher",
        engine: "ScoringEngine",
    ) -> None:
        super().__init__(config, orders, positions)
        self._universe  = universe
        self._fetcher   = fetcher
        self._engine    = engine
        # Stores the last scored list so print_score_table() / REPL can inspect it
        self.last_scores: list = []

    @property
    def fetcher(self) -> "DataFetcher":
        """Expose fetcher so TradingBot can pass it to print_portfolio()."""
        return self._fetcher

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        import time as _time
        self.log.info("ScoreBasedStrategy starting.")
        self.log.info("Universe size : %d symbols", self._universe.size())
        self.log.info("Buy threshold : %.0f", self.config.score_buy_threshold)
        self.log.info("Sell threshold: %.0f", self.config.score_sell_threshold)
        self.log.info("Max holdings  : %d",   self.config.max_holdings)

        # Initial data refresh (may take a few minutes for large universe)
        self.log.info("━" * 55)
        self.log.info("  Phase 1 of 1: Initial data refresh")
        self.log.info("  Downloading OHLCV for %d stocks — please wait…", self._universe.size())
        self.log.info("  (Progress will appear below every ~10%% of symbols)")
        self.log.info("━" * 55)
        t0 = _time.monotonic()
        self._fetcher.refresh(self._universe.all_symbols())
        elapsed = _time.monotonic() - t0
        self.log.info("━" * 55)
        self.log.info("  Data refresh complete in %.1f seconds.", elapsed)
        self.log.info("  Bot is live — first tick starting NOW.")
        self.log.info("━" * 55)

    def on_stop(self) -> None:
        self.log.info("Shutdown — squaring off all open positions.")
        for pos in self.positions.all_open():
            self.orders.sell(pos.symbol, pos.quantity, reason="shutdown square-off")

    # ------------------------------------------------------------------
    # Core tick
    # ------------------------------------------------------------------

    def generate_signals(self, force_refresh: bool = False) -> list[TradeSignal]:
        symbols = self._universe.all_symbols()

        # force_refresh=True is set during the EOD window to pick up today's close
        self._fetcher.refresh(symbols, force=force_refresh)

        # Score everything
        scores = self._engine.run(symbols)
        self.last_scores = scores

        # Log top 5 / bottom 5
        self._log_scores(scores)

        signals: list[TradeSignal] = []

        # ---- BUY candidates (top-N scoring stocks) ----
        buy_candidates = [
            s for s in scores[: self.config.score_top_n]
            if s.composite >= self.config.score_buy_threshold
        ]
        # effective_holdings = confirmed positions + pending BUY orders
        # This prevents generating a second BUY signal for a stock that was
        # just ordered but not yet filled (T+1 settlement / pending order)
        effective_holdings = self.positions.effective_holdings()

        for candidate in buy_candidates:
            if candidate.symbol in effective_holdings:
                continue  # already holding or pending
            if len(effective_holdings) + len(signals) >= self.config.max_holdings:
                break     # at capacity

            # open_slots = remaining capacity across holdings + already queued signals
            open_slots = self.config.max_holdings - len(effective_holdings) - len(signals)
            qty = self.orders.compute_quantity(candidate.symbol, open_slots)
            if qty < 1:
                continue  # can't afford even 1 share — skip

            signals.append(TradeSignal(
                symbol=candidate.symbol,
                signal=Signal.BUY,
                quantity=qty,
                reason=f"score={candidate.composite:.1f} (tech={candidate.technical:.0f} "
                       f"fund={candidate.fundamental:.0f} mom={candidate.momentum:.0f})",
            ))

        # ---- SELL candidates (held stocks whose score dropped) ----
        score_map = {s.symbol: s for s in scores}
        for pos in self.positions.all_open():
            stock_score = score_map.get(pos.symbol)
            if stock_score is None:
                continue
            if stock_score.composite < self.config.score_sell_threshold:
                signals.append(TradeSignal(
                    symbol=pos.symbol,
                    signal=Signal.SELL,
                    quantity=pos.quantity,
                    reason=f"score dropped to {stock_score.composite:.1f} "
                           f"(threshold={self.config.score_sell_threshold})",
                ))

        self.log.info(
            "Signals this tick: %d BUY, %d SELL",
            sum(1 for s in signals if s.signal == Signal.BUY),
            sum(1 for s in signals if s.signal == Signal.SELL),
        )
        return signals

    # ------------------------------------------------------------------
    # Reporting helpers
    # ------------------------------------------------------------------

    def _log_scores(self, scores: list) -> None:
        if not scores:
            return
        k      = self.config.score_sector_top_n
        top    = scores[:k]
        bottom = scores[-k:]
        self.log.info("── Top %d ──", k)
        for s in top:
            self.log.info("  %-12s  %s  composite=%.1f", s.symbol, s.sector, s.composite)
        self.log.info("── Bottom %d ──", k)
        for s in bottom:
            self.log.info("  %-12s  %s  composite=%.1f", s.symbol, s.sector, s.composite)

    def print_score_table(self, n: int | None = None) -> None:
        """Pretty-print the top-N scores (call from REPL for debugging)."""
        n = n or self.config.score_sector_top_n
        df = self._engine.to_dataframe(self.last_scores[:n])
        if not df.empty:
            cols = ["symbol", "sector", "composite", "technical", "fundamental", "momentum"]
            # Show intraday_pulse column if the engine computed it this tick
            if "comp_intraday_pulse" in df.columns:
                cols.append("comp_intraday_pulse")
            print(df[cols].to_string())
