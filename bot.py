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

import signal
import time

from pathlib import Path

from config import Config
from logger import get_logger
from market_hours import market_state, seconds_until_open, market_status_line
from orders import OrderManager
from paper_ledger import PaperLedger
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
        self._token_refreshed_date: str = ""   # instance-level (not class-level)

    _INTERVAL_EOD = 60   # 1 min — poll during EOD window until data lands

    # Track whether we've already done the EOD refresh this session
    _eod_refreshed_today: str = ""   # stores "YYYY-MM-DD" of last EOD refresh

    # Broker re-sync counter — re-syncs holdings from Groww every N ticks
    # so manual trades or transfers done outside the bot stay in sync
    _BROKER_SYNC_EVERY = 10   # re-sync every 10 ticks (~10 min at default interval)
    _tick_count: int = 0

    def _refresh_groww_token(self) -> bool:
        """Re-authenticate with Groww and update the client in orders + positions.

        Called once per trading day at pre-open (before 09:15) so the token
        is always fresh during market hours.  Returns True on success.
        """
        from growwapi import GrowwAPI
        from datetime import datetime
        today = datetime.now(IST).date().isoformat()
        if self._token_refreshed_date == today:
            return True   # already refreshed today

        try:
            token  = _groww_get_token(self.config)
            client = GrowwAPI(token)
            # Push new client into OrderManager, PositionTracker, and DataFetcher
            # All three live on self.strategy, not directly on TradingBot
            self.strategy.orders._client    = client
            self.strategy.positions._client = client
            self.strategy.fetcher.attach_groww_client(client)  # intraday OHLCV via Groww
            self._token_refreshed_date = today
            log.info("Groww token refreshed successfully for %s.", today)
            return True
        except Exception as exc:
            log.error("Groww token refresh FAILED: %s — LTP will use OHLCV cache.", exc)
            return False

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

        # Graceful shutdown on SIGTERM (sent by EC2 stop / systemd stop)
        # Without this, Python terminates immediately and _shutdown() never runs.
        signal.signal(signal.SIGTERM, lambda *_: self.stop())

        self.strategy.on_start()

        try:
            while self._running:
                state = market_state()

                if state == "pre_open":
                    # ── 09:00–09:15: pre-open warmup ──────────────────────
                    # Refresh Groww token once per day at pre-open so it is
                    # always valid during market hours (tokens expire daily).
                    self._refresh_groww_token()
                    # Run a full scoring tick so signals are ready the moment
                    # the regular session opens at 09:15. Sleep 60s between
                    # warmup ticks — there are only ~15 minutes here.
                    log.info("PRE-OPEN warmup tick — scoring universe before 09:15.")
                    self._tick(force_refresh=False)
                    self._sleep(60)

                elif state == "closed":
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
        label = {"pre_open": "PRE-OPEN", "open": "OPEN", "eod_window": "EOD", "closed": "CLOSED"}.get(state, state)
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

    def _top_scored(self, top_n: int = 5) -> tuple[list, list]:
        """Return (top_n highest scored, top_n lowest scored) from last scoring pass."""
        scores = getattr(self.strategy, "last_scores", [])
        if not scores:
            return [], []
        return scores[:top_n], scores[-top_n:]

    def _shutdown(self) -> None:
        log.info("Running shutdown hook...")
        try:
            self.strategy.on_stop()
        except Exception as exc:
            log.error("on_stop() raised: %s", exc)

        orders      = self.strategy.orders.get_order_history()
        pnl         = self.strategy.positions.total_realized_pnl()
        open_pos    = self.strategy.positions.all_open()
        all_pos     = self.strategy.positions.all_positions()
        paper       = self.strategy.positions._paper   # None in live mode

        log.info("Orders placed  : %d", len(orders))
        log.info("Realized P&L   : %.2f INR", pnl)
        log.info("Bot stopped cleanly.")

        self._send_daily_summary(orders, pnl, open_pos, all_pos, paper)

    def _send_daily_summary(
        self,
        orders:        list,
        pnl:           float,
        open_positions: list,
        all_positions:  list,
        paper:          "PaperLedger | None",
    ) -> None:
        """Send end-of-day trade summary via AWS SNS email."""
        topic_arn = self.config.sns_topic_arn
        if not topic_arn:
            return

        from datetime import date
        try:
            import boto3
            sns = boto3.client("sns", region_name="ap-south-1")

            buys  = [o for o in orders if o.get("type") == "BUY"]
            sells = [o for o in orders if o.get("type") == "SELL"]

            fetcher = getattr(self.strategy, "_fetcher", None)

            # ── Unrealized P&L for open positions ─────────────────────────────
            unrealized_total = 0.0
            open_pos_rows = []
            for pos in open_positions:
                ltp = fetcher.get_ltp(pos.symbol) if fetcher else 0.0
                if ltp > 0:
                    unreal     = (ltp - pos.avg_buy_price) * pos.quantity
                    unreal_pct = (ltp - pos.avg_buy_price) / pos.avg_buy_price * 100
                    unrealized_total += unreal
                    open_pos_rows.append(
                        f"  {pos.symbol:12s}  qty={pos.quantity}  "
                        f"avg=₹{pos.avg_buy_price:.2f}  ltp=₹{ltp:.2f}  "
                        f"unreal={unreal:+.0f} ({unreal_pct:+.1f}%)"
                    )
                else:
                    open_pos_rows.append(
                        f"  {pos.symbol:12s}  qty={pos.quantity}  avg=₹{pos.avg_buy_price:.2f}"
                    )

            # ── Top/bottom gainers (by realized P&L this session) ──────────
            traded = [p for p in all_positions if p.realized_pnl != 0]
            gainers = sorted(traded, key=lambda p: p.realized_pnl, reverse=True)[:5]
            losers  = sorted(traded, key=lambda p: p.realized_pnl)[:5]

            mode = "PAPER TRADING" if self.config.dry_run else "LIVE"

            lines = [
                f"TradingBot Daily Summary — {date.today().isoformat()}",
                f"Mode: {mode}",
                "=" * 55,
                f"Orders today   : {len(orders)}  ({len(buys)} BUY, {len(sells)} SELL)",
                f"Realized P&L   : ₹{pnl:+.2f}",
                f"Unrealized P&L : ₹{unrealized_total:+.2f}  (open positions)",
                f"Open positions : {len(open_positions)}",
                "",
            ]

            # ── Today's BUYs ───────────────────────────────────────────────
            if buys:
                lines.append("── BUYs ──")
                for o in buys:
                    lines.append(
                        f"  {o['symbol']:12s}  qty={o['qty']}  "
                        f"price=₹{o.get('price', 0):.2f}  status={o.get('status','?')}"
                    )
                lines.append("")

            # ── Today's SELLs ──────────────────────────────────────────────
            if sells:
                lines.append("── SELLs ──")
                for o in sells:
                    lines.append(
                        f"  {o['symbol']:12s}  qty={o['qty']}  "
                        f"price=₹{o.get('price', 0):.2f}  status={o.get('status','?')}"
                    )
                lines.append("")

            # ── Open Positions ─────────────────────────────────────────────
            if open_pos_rows:
                lines.append("── Open Positions ──")
                lines.extend(open_pos_rows)
                lines.append("")

            # ── Session Gainers / Losers ───────────────────────────────────
            if gainers:
                lines.append("── Top Gainers (Session) ──")
                for p in gainers:
                    lines.append(f"  {p.symbol:12s}  realized=₹{p.realized_pnl:+.2f}")
                lines.append("")
            if losers:
                lines.append("── Top Losers (Session) ──")
                for p in losers:
                    lines.append(f"  {p.symbol:12s}  realized=₹{p.realized_pnl:+.2f}")
                lines.append("")

            # ── Paper trading account snapshot ─────────────────────────────
            if paper is not None:
                snap = paper.snapshot(open_positions, fetcher)
                ret_sign = "+" if snap["return_pct"] >= 0 else ""
                gain_sign = "+" if snap["total_gain"] >= 0 else ""

                lines += [
                    "=" * 55,
                    "PAPER TRADING ACCOUNT",
                    "=" * 55,
                    f"  Starting capital : ₹{snap['starting_balance']:>12,.2f}",
                    f"  Cash available   : ₹{snap['cash']:>12,.2f}",
                    f"  Open pos. value  : ₹{snap['open_value']:>12,.2f}",
                    f"  Total portfolio  : ₹{snap['total_value']:>12,.2f}",
                    f"  Overall return   : {ret_sign}{snap['return_pct']:.2f}%  "
                    f"({gain_sign}₹{snap['total_gain']:,.2f})",
                    "",
                ]

                # Per-position breakdown
                if snap["open_rows"]:
                    lines.append("  Holdings:")
                    for r in snap["open_rows"]:
                        pct_s = f"{r['pct']:+.1f}%"
                        unreal_s = f"₹{r['unrealized']:+.0f}"
                        lines.append(
                            f"    {r['symbol']:12s}  qty={r['qty']}  "
                            f"avg=₹{r['avg_buy']:,.2f}  ltp=₹{r['ltp']:,.2f}  "
                            f"{unreal_s} ({pct_s})"
                        )
                    lines.append("")

                # Today's completed trades with individual P&L
                todays = paper.todays_trades()
                todays_sells = [t for t in todays if t.action == "SELL"]
                if todays_sells:
                    lines.append("  Today's Closed Trades:")
                    day_pnl = 0.0
                    for t in todays_sells:
                        sign = "+" if t.pnl >= 0 else ""
                        pct  = ((t.price - t.avg_buy_price) / t.avg_buy_price * 100
                                if t.avg_buy_price else 0)
                        day_pnl += t.pnl
                        lines.append(
                            f"    {t.symbol:12s}  qty={t.quantity}  "
                            f"buy=₹{t.avg_buy_price:.2f}  sell=₹{t.price:.2f}  "
                            f"P&L={sign}₹{t.pnl:.2f} ({sign}{pct:.1f}%)"
                        )
                    day_sign = "+" if day_pnl >= 0 else ""
                    lines.append(f"  Today's realized : {day_sign}₹{day_pnl:.2f}")
                    lines.append("")

                # All-time per-symbol P&L
                sym_pnl = paper.realized_pnl_by_symbol()
                if sym_pnl:
                    lines.append("  All-Time Realized P&L by Symbol:")
                    for sym, sym_gain in sorted(sym_pnl.items(),
                                                key=lambda x: x[1], reverse=True):
                        sign = "+" if sym_gain >= 0 else ""
                        lines.append(f"    {sym:12s}  {sign}₹{sym_gain:.2f}")
                    total_realized = paper.total_realized_pnl()
                    r_sign = "+" if total_realized >= 0 else ""
                    lines.append(f"  All-time total   : {r_sign}₹{total_realized:.2f}")
                    lines.append("")

            # ── Top Scored / Bottom Scored from last scoring pass ──────────
            top_scored, bottom_scored = self._top_scored(top_n=5)
            if top_scored:
                lines.append("── Top Picks (Highest Composite Score) ──")
                for s in top_scored:
                    sector = getattr(s, "sector", "")
                    lines.append(
                        f"  {s.symbol:12s}  [{sector:12s}]  "
                        f"composite={s.composite:.1f}  "
                        f"tech={s.technical:.1f}  fund={s.fundamental:.1f}  mom={s.momentum:.1f}"
                    )
                lines.append("")
            if bottom_scored:
                lines.append("── Bottom Picks (Lowest Composite Score) ──")
                for s in bottom_scored:
                    sector = getattr(s, "sector", "")
                    lines.append(
                        f"  {s.symbol:12s}  [{sector:12s}]  "
                        f"composite={s.composite:.1f}  "
                        f"tech={s.technical:.1f}  fund={s.fundamental:.1f}  mom={s.momentum:.1f}"
                    )
                lines.append("")

            # Subject line
            if paper is not None:
                snap = paper.snapshot(open_positions, fetcher)
                ret_sign = "+" if snap["return_pct"] >= 0 else ""
                subject = (
                    f"[TradingBot] EOD Summary {date.today().isoformat()} "
                    f"| Paper ₹{snap['total_value']:,.0f} ({ret_sign}{snap['return_pct']:.1f}%) "
                    f"| Today ₹{pnl:+.2f}"
                )
            else:
                subject = (
                    f"[TradingBot] EOD Summary {date.today().isoformat()} "
                    f"| Realized ₹{pnl:+.2f} | Unrealized ₹{unrealized_total:+.2f}"
                )
            message = "\n".join(lines)

            sns.publish(TopicArn=topic_arn, Subject=subject, Message=message)
            log.info("Daily summary sent via SNS.")

        except Exception as exc:
            log.error("Failed to send SNS summary: %s", exc)


# ======================================================================
#  Entry point
# ======================================================================

def _groww_get_token(config) -> str:
    """Get a Groww access token — TOTP mode preferred, approval mode fallback."""
    from growwapi import GrowwAPI
    if config.totp_secret.strip():
        import pyotp
        return GrowwAPI.get_access_token(
            api_key=config.api_key,
            totp=pyotp.TOTP(config.totp_secret.strip()).now(),
        )
    return GrowwAPI.get_access_token(
        api_key=config.api_key,
        secret=config.secret,
    )


def build_bot() -> TradingBot:
    config = Config()

    from growwapi import GrowwAPI
    groww_client = None
    try:
        token = _groww_get_token(config)
        groww_client = GrowwAPI(token)
        log.info("Authenticated with Groww%s.", " (dry-run — no real orders)" if config.dry_run else "")
    except Exception as exc:
        if not config.dry_run:
            raise   # live mode: auth failure is fatal
        # dry-run: auth failure means NO live LTP — OHLCV cache will be used instead.
        # This is a significant degradation — log it as ERROR so it is never missed.
        log.error(
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "  Groww auth FAILED — LTP will use yesterday's OHLCV close.\n"
            "  Paper P&L will be based on stale prices, not live market.\n"
            "  Fix: check GROWW_API_KEY / GROWW_SECRET in your .env\n"
            "  and ensure your Groww API key has Market Data permissions.\n"
            "  Error: %s\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            exc,
        )

    orders    = OrderManager(groww_client, config)
    positions = PositionTracker(groww_client, config)
    positions.refresh_from_broker()   # seed local state from your real Groww account

    # ── Paper trading ledger (dry-run only) ───────────────────────────────────
    # In live mode (dry_run=False): PaperLedger is never created.
    # record_buy/record_sell check `_paper is not None` before touching it,
    # so live trades go straight to Groww with zero paper ledger involvement.
    if config.dry_run:
        ledger = PaperLedger(
            starting_balance = config.dry_run_balance,
            ledger_path      = Path(config.paper_ledger_path),
        )
        positions.attach_paper_ledger(ledger)

        # Restore open positions from previous sessions so the bot doesn't
        # re-buy stocks it already "owns" in paper mode after a restart.
        restored = ledger.open_positions()
        if restored:
            from positions import Position
            for sym, (qty, avg) in restored.items():
                pos               = positions._positions.setdefault(sym, Position(symbol=sym))
                pos.quantity      = qty
                pos.avg_buy_price = avg
                pos.peak_price    = avg
            log.info(
                "Paper trading: restored %d open position(s) from ledger: %s",
                len(restored),
                ", ".join(f"{s} x{q}" for s, (q, _) in restored.items()),
            )

        log.info(
            "Paper trading enabled — balance ₹%.2f | cash ₹%.2f | "
            "ledger: %s",
            config.dry_run_balance, ledger.cash, config.paper_ledger_path,
        )
    cache     = DataCache()
    fetcher   = DataFetcher(cache, cache_only=config.fetcher_cache_only)
    if config.fetcher_cache_only:
        log.info("DataFetcher: FETCHER_CACHE_ONLY=true — bulk refresh skipped, data from rsync.")
    if groww_client is not None:
        fetcher.attach_groww_client(groww_client)   # intraday OHLCV via Groww (works on EC2)
    orders.attach_fetcher(fetcher)   # dry-run: use cached close as LTP for sizing
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
        sentiment_scorer          = news_scorer,
        sentiment_weight          = config.sentiment_composite_weight,  # 0.0 = disable
        # IntraDayPulse — live price sensitivity during 09:15–15:30 IST
        intraday_pulse_weight     = config.intraday_pulse_weight,
        intraday_w_day_return     = config.intraday_w_day_return,
        intraday_w_range_position = config.intraday_w_range_position,
        intraday_w_volume_pace    = config.intraday_w_volume_pace,
        intraday_w_open_distance  = config.intraday_w_open_distance,
    )
    strategy = ScoreBasedStrategy(
        config=config, orders=orders, positions=positions,
        universe=universe, fetcher=fetcher, engine=engine,
    )

    return TradingBot(config=config, strategy=strategy)


if __name__ == "__main__":
    bot = build_bot()
    bot.start()
