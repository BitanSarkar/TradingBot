"""
bot.py — Main trading bot runner.

Usage
-----
    python bot.py

Startup sequence
----------------
  1. Load Config.
  2. Authenticate with Groww (skipped in dry_run).
  3. Build StockUniverse  → refresh sector map from NSE.
  4. Build DataFetcher    → lazy OHLCV / fundamentals from yfinance.
  5. Build ScoreRegistry  → wire sector → scorer (with your tweaks below).
  6. Build ScoringEngine  → parallelised scorer orchestrator.
  7. Build ScoreBasedStrategy.
  8. Run the bot loop.

Customise scoring
-----------------
    See the "CUSTOMISE SCORING" section below.  You can:
      • Override sector weights
      • Inject custom metrics per sector
      • Swap out entire sector scorers
    All changes take effect immediately without restarting.
"""

from __future__ import annotations

import time

from config import Config
from logger import get_logger
from orders import OrderManager
from positions import PositionTracker
from universe import StockUniverse
from data import DataFetcher
from data.cache import DataCache
from scoring import ScoringEngine, ScoreRegistry
from strategies.base import Signal
from strategies.score_based import ScoreBasedStrategy

log = get_logger("Bot")


# ======================================================================
#  CUSTOMISE SCORING — edit this function freely
# ======================================================================

def configure_registry(registry: ScoreRegistry) -> None:
    """
    This is YOUR place to tweak the scoring formulas.
    Called once at startup.  All changes are live immediately.

    Examples (uncomment and adapt):
    """

    # ── 1. Shift top-level weights for a sector ──────────────────────
    # registry.set_weights("IT", technical=0.55, fundamental=0.25, momentum=0.20)

    # ── 2. Tune technical sub-weights ────────────────────────────────
    # registry.set_technical_weights("PHARMA", macd=0.30, momentum=0.25, rsi=0.15)

    # ── 3. Tune fundamental sub-weights ──────────────────────────────
    # registry.set_fundamental_weights("BANKING", roe=0.28, pb=0.22, rev_growth=0.18)

    # ── 4. Inject a fully custom metric ──────────────────────────────
    #
    #   def npa_quality(df, fund):
    #       """Lower NPA ratio → higher score."""
    #       npa = fund.get("npa_ratio", 0.03)   # populate via DataFetcher
    #       return max(0.0, 100.0 - npa * 2000)
    #
    #   registry.add_metric("BANKING", "npa_quality", npa_quality, weight=0.12)

    # ── 5. Replace an entire sector scorer ───────────────────────────
    #
    #   from strategies.my_scorer import MyFintechScorer
    #   registry.register("FINANCIAL", MyFintechScorer())

    pass  # remove this when you add real config


# ======================================================================
#  Bot wiring — normally no need to edit below this line
# ======================================================================

class TradingBot:
    def __init__(self, config: Config, strategy: ScoreBasedStrategy) -> None:
        self.config   = config
        self.strategy = strategy
        self._running = False

    def start(self) -> None:
        mode = "DRY RUN" if self.config.dry_run else "⚠️  LIVE TRADING"
        log.info("=" * 55)
        log.info("  TradingBot  [%s]", mode)
        log.info("  Strategy  : %s", self.strategy.__class__.__name__)
        log.info("  Interval  : %ds", self.config.poll_interval)
        log.info("  Buy ≥     : %.0f  |  Sell < %.0f",
                 self.config.score_buy_threshold,
                 self.config.score_sell_threshold)
        log.info("=" * 55)

        self._running = True
        self.strategy.on_start()

        try:
            while self._running:
                self._tick()
                time.sleep(self.config.poll_interval)
        except KeyboardInterrupt:
            log.info("Shutdown requested (Ctrl-C).")
        except Exception as exc:
            log.exception("Unhandled exception: %s", exc)
        finally:
            self._shutdown()

    def stop(self) -> None:
        self._running = False

    def _tick(self) -> None:
        log.info("─" * 40 + "  tick  " + "─" * 40)
        try:
            signals = self.strategy.generate_signals()
        except Exception as exc:
            log.error("Strategy error: %s", exc, exc_info=True)
            return

        for sig in signals:
            if sig.signal is Signal.HOLD:
                continue
            self._execute(sig)

    def _execute(self, sig) -> None:
        if not self._passes_risk(sig):
            return

        pos    = self.strategy.positions
        orders = self.strategy.orders

        if sig.signal is Signal.BUY:
            log.info("BUY  %-12s  qty=%-3d  reason: %s", sig.symbol, sig.quantity, sig.reason)
            order_id = orders.buy(sig.symbol, sig.quantity, sig.order_type, sig.price)
            if order_id and not self.config.dry_run:
                pos.record_buy(sig.symbol, sig.quantity, sig.price)

        elif sig.signal is Signal.SELL:
            log.info("SELL %-12s  qty=%-3d  reason: %s", sig.symbol, sig.quantity, sig.reason)
            order_id = orders.sell(sig.symbol, sig.quantity, sig.order_type, sig.price)
            if order_id and not self.config.dry_run:
                pos.record_sell(sig.symbol, sig.quantity, sig.price)

    def _passes_risk(self, sig) -> bool:
        pos = self.strategy.positions

        if pos.total_realized_pnl() <= -abs(self.config.max_daily_loss):
            log.warning("Daily loss limit hit. Signal for %s rejected.", sig.symbol)
            return False

        if sig.signal is Signal.BUY and pos.count_open() >= self.config.max_holdings:
            log.warning("Max holdings (%d) reached. BUY for %s rejected.",
                        self.config.max_holdings, sig.symbol)
            return False

        if sig.signal is Signal.SELL:
            held = pos.get(sig.symbol).quantity
            if held == 0:
                return False
            sig.quantity = min(sig.quantity, held)

        return True

    def _shutdown(self) -> None:
        log.info("Running shutdown hook...")
        try:
            self.strategy.on_stop()
        except Exception as exc:
            log.error("on_stop() raised: %s", exc)

        log.info("Orders placed  : %d", len(self.strategy.orders.get_order_history()))
        log.info("Realized P&L   : %.2f INR", self.strategy.positions.total_realized_pnl())
        log.info("Bot stopped cleanly.")


# ======================================================================
#  Entry point
# ======================================================================

def build_bot() -> TradingBot:
    config = Config()

    groww_client = None
    if not config.dry_run:
        from growwapi import GrowwAPI
        token = GrowwAPI.get_access_token(api_key=config.api_key, secret=config.secret)
        groww_client = GrowwAPI(token)
        log.info("Authenticated with Groww.")

    orders    = OrderManager(groww_client, config)
    positions = PositionTracker(groww_client, config)
    cache     = DataCache()
    fetcher   = DataFetcher(cache)
    universe  = StockUniverse()
    universe.refresh()

    registry  = ScoreRegistry()
    configure_registry(registry)   # ← your customisations applied here
    registry.summary()             # print table to console on startup

    engine   = ScoringEngine(universe, fetcher, registry)
    strategy = ScoreBasedStrategy(
        config=config, orders=orders, positions=positions,
        universe=universe, fetcher=fetcher, engine=engine,
    )

    return TradingBot(config=config, strategy=strategy)


if __name__ == "__main__":
    bot = build_bot()
    bot.start()
