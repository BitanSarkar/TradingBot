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
from market_hours import market_state, seconds_until_open, market_status_line
from orders import OrderManager
from positions import PositionTracker
from universe import StockUniverse
from data import DataFetcher
from data.cache import DataCache
from scoring import ScoringEngine, ScoreRegistry
from news import NewsFetcher, SentimentAnalyzer
from scoring.formulas.news_sentiment import NewsSentimentScorer
from strategies.base import Signal
from strategies.score_based import ScoreBasedStrategy

log = get_logger("Bot")


# ======================================================================
#  CUSTOMISE SCORING — edit this function freely
# ======================================================================

def configure_registry(registry: ScoreRegistry, config: Config) -> None:
    """
    Applies all weight overrides from .env to the score registry.
    Called once at startup — no code changes needed, just edit .env and restart.

    To go further (custom metrics, custom scorers), add them below.
    """

    # ── Pillar weights (technical / fundamental / momentum) per sector ────────
    registry.set_weights("DEFAULT",    technical=config.w_default_technical,   fundamental=config.w_default_fundamental,   momentum=config.w_default_momentum)
    registry.set_weights("IT",         technical=config.w_it_technical,        fundamental=config.w_it_fundamental,        momentum=config.w_it_momentum)
    registry.set_weights("BANKING",    technical=config.w_banking_technical,   fundamental=config.w_banking_fundamental,   momentum=config.w_banking_momentum)
    registry.set_weights("PSU_BANK",   technical=config.w_psu_bank_technical,  fundamental=config.w_psu_bank_fundamental,  momentum=config.w_psu_bank_momentum)
    registry.set_weights("PHARMA",     technical=config.w_pharma_technical,    fundamental=config.w_pharma_fundamental,    momentum=config.w_pharma_momentum)
    registry.set_weights("AUTO",       technical=config.w_auto_technical,      fundamental=config.w_auto_fundamental,      momentum=config.w_auto_momentum)
    registry.set_weights("FMCG",       technical=config.w_fmcg_technical,      fundamental=config.w_fmcg_fundamental,      momentum=config.w_fmcg_momentum)
    registry.set_weights("METAL",      technical=config.w_metal_technical,     fundamental=config.w_metal_fundamental,     momentum=config.w_metal_momentum)
    registry.set_weights("ENERGY",     technical=config.w_energy_technical,    fundamental=config.w_energy_fundamental,    momentum=config.w_energy_momentum)
    registry.set_weights("REALTY",     technical=config.w_realty_technical,    fundamental=config.w_realty_fundamental,    momentum=config.w_realty_momentum)
    registry.set_weights("INFRA",      technical=config.w_infra_technical,     fundamental=config.w_infra_fundamental,     momentum=config.w_infra_momentum)
    registry.set_weights("FINANCIAL",  technical=config.w_financial_technical, fundamental=config.w_financial_fundamental, momentum=config.w_financial_momentum)
    registry.set_weights("MEDIA",      technical=config.w_media_technical,     fundamental=config.w_media_fundamental,     momentum=config.w_media_momentum)
    registry.set_weights("CONSUMER",   technical=config.w_consumer_technical,  fundamental=config.w_consumer_fundamental,  momentum=config.w_consumer_momentum)

    # ── Technical sub-weights (same across all sectors) ───────────────────────
    for sector in registry.all_sectors():
        registry.set_technical_weights(sector,
            rsi       = config.w_tech_rsi,
            macd      = config.w_tech_macd,
            bollinger = config.w_tech_bollinger,
            sma_cross = config.w_tech_sma_cross,
            volume    = config.w_tech_volume,
            momentum  = config.w_tech_momentum,
        )

    # ── Fundamental sub-weights — DEFAULT ─────────────────────────────────────
    for sector in registry.all_sectors():
        registry.set_fundamental_weights(sector,
            pe         = config.w_fund_pe,
            pb         = config.w_fund_pb,
            roe        = config.w_fund_roe,
            de         = config.w_fund_de,
            curr_ratio = config.w_fund_curr_ratio,
            rev_growth = config.w_fund_rev_growth,
            eps_growth = config.w_fund_eps_growth,
            margin     = config.w_fund_margin,
            dividend   = config.w_fund_dividend,
        )

    # ── Fundamental sub-weights — sector overrides ────────────────────────────
    registry.set_fundamental_weights("BANKING",
        pe=config.w_fund_banking_pe,         pb=config.w_fund_banking_pb,
        roe=config.w_fund_banking_roe,       de=config.w_fund_banking_de,
        curr_ratio=config.w_fund_banking_curr_ratio,
        rev_growth=config.w_fund_banking_rev_growth,
        eps_growth=config.w_fund_banking_eps_growth,
        margin=config.w_fund_banking_margin, dividend=config.w_fund_banking_dividend,
    )
    registry.set_fundamental_weights("PSU_BANK",  # same logic as banking
        pe=config.w_fund_banking_pe,         pb=config.w_fund_banking_pb,
        roe=config.w_fund_banking_roe,       de=config.w_fund_banking_de,
        curr_ratio=config.w_fund_banking_curr_ratio,
        rev_growth=config.w_fund_banking_rev_growth,
        eps_growth=config.w_fund_banking_eps_growth,
        margin=config.w_fund_banking_margin, dividend=config.w_fund_banking_dividend,
    )
    registry.set_fundamental_weights("IT",
        pe=config.w_fund_it_pe,              pb=config.w_fund_it_pb,
        roe=config.w_fund_it_roe,            de=config.w_fund_it_de,
        curr_ratio=config.w_fund_it_curr_ratio,
        rev_growth=config.w_fund_it_rev_growth,
        eps_growth=config.w_fund_it_eps_growth,
        margin=config.w_fund_it_margin,      dividend=config.w_fund_it_dividend,
    )
    registry.set_fundamental_weights("PHARMA",
        pe=config.w_fund_pharma_pe,          pb=config.w_fund_pharma_pb,
        roe=config.w_fund_pharma_roe,        de=config.w_fund_pharma_de,
        curr_ratio=config.w_fund_pharma_curr_ratio,
        rev_growth=config.w_fund_pharma_rev_growth,
        eps_growth=config.w_fund_pharma_eps_growth,
        margin=config.w_fund_pharma_margin,  dividend=config.w_fund_pharma_dividend,
    )

    # ── Custom metrics — add yours here ───────────────────────────────────────
    #
    #   def npa_quality(df, fund):
    #       npa = fund.get("npa_ratio", 0.03)
    #       return max(0.0, 100.0 - npa * 2000)
    #   registry.add_metric("BANKING", "npa_quality", npa_quality, weight=0.12)


# ======================================================================
#  Bot wiring — normally no need to edit below this line
# ======================================================================

class TradingBot:
    def __init__(self, config: Config, strategy: ScoreBasedStrategy) -> None:
        self.config   = config
        self.strategy = strategy
        self._running = False

    _INTERVAL_EOD = 60   # 1 min — poll during EOD window until data lands

    # Track whether we've already done the EOD refresh this session
    _eod_refreshed_today: str = ""   # stores "YYYY-MM-DD" of last EOD refresh

    # Broker re-sync counter — re-syncs holdings from Groww every N ticks
    # so manual trades or transfers done outside the bot stay in sync
    _BROKER_SYNC_EVERY = 10   # re-sync every 10 ticks (~10 min at default interval)
    _tick_count: int = 0

    def start(self) -> None:
        mode = "DRY RUN" if self.config.dry_run else "⚠️  LIVE TRADING"
        log.info("=" * 55)
        log.info("  TradingBot  [%s]", mode)
        log.info("  Strategy  : %s", self.strategy.__class__.__name__)
        log.info("  Interval  : %ds (open) / %ds (closed)",
                 self.config.poll_interval_open, self.config.poll_interval_closed)
        log.info("  Buy ≥     : %.0f  |  Sell < %.0f",
                 self.config.score_buy_threshold,
                 self.config.score_sell_threshold)
        log.info("  Market    : %s", market_status_line())
        log.info("=" * 55)

        self._running = True
        self.strategy.on_start()

        try:
            while self._running:
                state = market_state()

                if state == "closed":
                    # ── Night / weekend ───────────────────────────────────
                    # One tick per hour so news sentiment can update,
                    # then sleep until the next pre-open.
                    self._tick(force_refresh=False)
                    secs = seconds_until_open()
                    h, m = divmod(int(secs) // 60, 60)
                    log.info(
                        "Market CLOSED — sleeping %dh %dm until pre-open.  "
                        "Next tick at 09:00 IST.", h, m,
                    )
                    self._sleep(min(secs, self.config.poll_interval_closed))

                elif state == "eod_window":
                    # ── 15:30–15:55: EOD data lands ───────────────────────
                    from datetime import date
                    today = str(date.today())
                    if self._eod_refreshed_today != today:
                        log.info("EOD WINDOW — forcing OHLCV refresh to pick up today's close.")
                        self._tick(force_refresh=True)
                        self._eod_refreshed_today = today
                    else:
                        time.sleep(self._INTERVAL_EOD)

                else:
                    # ── Market OPEN: 09:15–15:30 ─────────────────────────
                    # Live candles are injected automatically in get_ohlcv()
                    # so RSI/MACD/Bollinger compute on the CURRENT price.
                    self._tick(force_refresh=False)
                    self._sleep(self.config.poll_interval_open)

        except KeyboardInterrupt:
            log.info("Shutdown requested (Ctrl-C).")
        except Exception as exc:
            log.exception("Unhandled exception: %s", exc)
        finally:
            self._shutdown()

    def _sleep(self, seconds: float) -> None:
        """Interruptible sleep — checks _running every 30s."""
        deadline = time.monotonic() + seconds
        while self._running and time.monotonic() < deadline:
            time.sleep(min(30.0, deadline - time.monotonic()))

    def stop(self) -> None:
        self._running = False

    def _tick(self, force_refresh: bool = False) -> None:
        import time as _time
        self._tick_count += 1
        state = market_state()
        label = {"open": "OPEN", "eod_window": "EOD", "closed": "CLOSED"}.get(state, state)
        log.info("─" * 36 + f"  tick [{label}] #{self._tick_count}  " + "─" * 36)

        # Re-sync holdings from Groww every N ticks so manual trades
        # done outside the bot (e.g. in the Groww app) stay reflected here
        pos = self.strategy.positions
        if self._tick_count % self._BROKER_SYNC_EVERY == 0:
            log.info("Re-syncing holdings from Groww (tick #%d)...", self._tick_count)
            pos.refresh_from_broker()

        # Poll fill status for all pending orders BEFORE generating new signals.
        # This ensures positions are up-to-date and idempotency checks are current.
        pos.sync_pending_orders(self.strategy.orders)

        t0 = _time.monotonic()
        try:
            signals = self.strategy.generate_signals(force_refresh=force_refresh)
        except Exception as exc:
            log.error("Strategy error: %s", exc, exc_info=True)
            return

        elapsed = _time.monotonic() - t0
        log.info("Tick completed in %.1fs — %d signal(s) generated.", elapsed, len(signals))

        # Print portfolio snapshot after every tick
        pos.print_portfolio(self.strategy.fetcher)

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
            if order_id:
                if self.config.dry_run:
                    # Dry-run: no real settlement, record immediately
                    pos.record_buy(sig.symbol, sig.quantity, sig.price)
                else:
                    # Live: order is placed but not yet filled/settled
                    # add_pending() prevents duplicate orders on next tick
                    pos.add_pending(sig.symbol, order_id, "BUY",
                                    sig.quantity, sig.price)

        elif sig.signal is Signal.SELL:
            log.info("SELL %-12s  qty=%-3d  reason: %s", sig.symbol, sig.quantity, sig.reason)
            order_id = orders.sell(sig.symbol, sig.quantity, sig.order_type, sig.price)
            if order_id:
                if self.config.dry_run:
                    pos.record_sell(sig.symbol, sig.quantity, sig.price)
                else:
                    pos.add_pending(sig.symbol, order_id, "SELL",
                                    sig.quantity, sig.price)

    def _passes_risk(self, sig) -> bool:
        pos = self.strategy.positions

        # Hard stop: daily loss limit
        if pos.total_realized_pnl() <= -abs(self.config.max_daily_loss):
            log.warning("Daily loss limit hit. Signal for %s rejected.", sig.symbol)
            return False

        if sig.signal is Signal.BUY:
            # Idempotency: already have a pending or confirmed position for this symbol
            if pos.has_pending(sig.symbol):
                log.debug(
                    "BUY for %s skipped — pending order already exists (order not yet filled).",
                    sig.symbol,
                )
                return False

            # Capacity: at max holdings (count confirmed + pending buys)
            effective = len(pos.effective_holdings())
            if effective >= self.config.max_holdings:
                log.warning(
                    "Max holdings (%d) reached (%d confirmed + pending). "
                    "BUY for %s rejected.",
                    self.config.max_holdings, effective, sig.symbol,
                )
                return False

        if sig.signal is Signal.SELL:
            # Idempotency: don't sell something already being sold
            if pos.has_pending(sig.symbol):
                log.debug(
                    "SELL for %s skipped — pending order already exists.",
                    sig.symbol,
                )
                return False

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
    positions.refresh_from_broker()   # seed local state from your real Groww account
    cache     = DataCache()
    fetcher   = DataFetcher(cache)
    universe  = StockUniverse()
    universe.refresh()

    registry  = ScoreRegistry()
    configure_registry(registry, config)   # ← reads all weights from .env
    registry.summary()                     # print table to console on startup

    # ── News sentiment (4th pillar) — all settings read from .env ─────────
    news_fetcher   = NewsFetcher(
        cache_minutes = config.sentiment_cache_minutes,
        max_age_hours = config.sentiment_max_age_hours,
    )
    news_analyzer  = SentimentAnalyzer(
        backend        = "finbert",  # ProsusAI/finbert — trained on financial text
        keyword_weight = config.sentiment_keyword_weight,
        recency_decay  = config.sentiment_recency_decay,
    )
    news_scorer    = NewsSentimentScorer(
        news_fetcher,
        news_analyzer,
        min_articles  = config.sentiment_min_articles,
    )
    engine   = ScoringEngine(
        universe, fetcher, registry,
        sentiment_scorer = news_scorer,
        sentiment_weight = config.sentiment_composite_weight,  # 0.0 = disable
    )
    strategy = ScoreBasedStrategy(
        config=config, orders=orders, positions=positions,
        universe=universe, fetcher=fetcher, engine=engine,
    )

    return TradingBot(config=config, strategy=strategy)


if __name__ == "__main__":
    bot = build_bot()
    bot.start()
