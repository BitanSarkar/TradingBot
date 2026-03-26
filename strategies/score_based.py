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
from strategies.entry_signals import ScoreHistory, compute_entry_quality
from strategies.exit_signals import check_intraday_exit, clear_high_water

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
        # Rolling score history per symbol — used for velocity/acceleration at entry
        self._score_history = ScoreHistory(window=10)
        # Pending limit orders: symbol → ticks_waiting (cancel after timeout)
        self._pending_limits: dict[str, int] = {}

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
            self.orders.sell(pos.symbol, pos.quantity)
        self._score_history.save()
        self.log.info("Score history saved.")

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

        # Update rolling score history for velocity/acceleration analysis.
        # One entry per calendar day — scores from daily OHLCV don't change
        # intraday, so only day-over-day comparison produces meaningful velocity.
        self._score_history.update_batch(scores)
        if force_refresh:
            # EOD tick uses today's final close — best time to snapshot the day's score
            self._score_history.save()

        # Log top 5 / bottom 5
        self._log_scores(scores)

        signals: list[TradeSignal] = []
        cfg = self.config

        # All current composite scores (for market regime calculation)
        universe_scores = [s.composite for s in scores]

        # ---- BUY candidates (top-N scoring stocks) ----
        buy_candidates = [
            s for s in scores[: cfg.score_top_n]
            if s.composite >= cfg.score_buy_threshold
        ]
        self.log.info(
            "BUY pipeline: %d candidates above threshold %.1f  (top_n=%d)",
            len(buy_candidates), cfg.score_buy_threshold, cfg.score_top_n,
        )
        # effective_holdings = confirmed positions + pending BUY orders
        effective_holdings = self.positions.effective_holdings()

        # Age out stale pending limits
        for sym in list(self._pending_limits):
            self._pending_limits[sym] += 1
            if self._pending_limits[sym] > cfg.entry_limit_timeout_ticks:
                del self._pending_limits[sym]
                self.log.info(
                    "LIMIT TIMEOUT for %s — switching to market order next tick", sym
                )

        for candidate in buy_candidates:
            if candidate.symbol in effective_holdings:
                self.log.info("BUY skipped %-12s score=%.1f  reason: already holding", candidate.symbol, candidate.composite)
                continue
            if len(effective_holdings) + len(signals) >= cfg.max_holdings:
                self.log.info("BUY stopped — max_holdings=%d reached", cfg.max_holdings)
                break

            open_slots = cfg.max_holdings - len(effective_holdings) - len(signals)
            qty = self.orders.compute_quantity(candidate.symbol, open_slots)
            if qty < 1:
                ltp = self._fetcher.get_ltp(candidate.symbol)
                bal = self.orders.available_balance()
                self.log.info(
                    "BUY skipped %-12s score=%.1f  reason: qty<1  ltp=%.2f  balance=%.2f  slots=%d",
                    candidate.symbol, candidate.composite, ltp, bal, open_slots,
                )
                continue

            # ── Entry quality gate ──────────────────────────────────────────
            ltp     = self._fetcher.get_ltp(candidate.symbol)
            df      = self._fetcher._cache.load_ohlcv(candidate.symbol)
            history = self._score_history.get(candidate.symbol)

            v_session, v_recent = self._fetcher.get_intraday_velocities(candidate.symbol)

            entry = compute_entry_quality(
                df                          = df,
                current_score               = candidate.composite,
                score_history               = history,
                universe_scores             = universe_scores,
                current_ltp                 = ltp if ltp > 0 else 0.0,
                min_score_velocity          = cfg.entry_min_score_velocity,
                velocity_window             = cfg.entry_velocity_window,
                rsi_ideal_max               = cfg.entry_rsi_ideal_max,
                bollinger_b_ideal_max       = cfg.entry_bollinger_b_max,
                vol_min_ratio               = cfg.entry_vol_min_ratio,
                bull_ratio_min              = cfg.entry_bull_ratio_min,
                regime_bypass_min_score     = cfg.entry_regime_bypass_min_score,
                regime_bypass_min_velocity  = cfg.entry_regime_bypass_min_velocity,
                regime_bypass_max_rsi       = cfg.entry_regime_bypass_max_rsi,
                atr_period                  = cfg.exit_atr_period,
                entry_pullback_mult         = cfg.entry_pullback_mult,
                min_quality_score           = cfg.entry_min_quality,
                w_velocity                  = cfg.entry_w_velocity,
                w_price                     = cfg.entry_w_price,
                w_volume                    = cfg.entry_w_volume,
                w_regime                    = cfg.entry_w_regime,
                v_session                   = v_session,
                v_recent                    = v_recent,
                w_price_velocity            = cfg.entry_w_price_velocity,
                momentum_chase_premium      = cfg.intraday_momentum_chase_premium,
                min_score_overall           = cfg.score_buy_threshold,
            )

            if not entry.qualified:
                self.log.info(
                    "BUY skipped %-12s score=%.1f  reason: %s",
                    candidate.symbol, candidate.composite, entry.reason,
                )
                continue

            order_type = "LIMIT" if entry.use_limit else "MARKET"
            if entry.use_limit:
                self._pending_limits[candidate.symbol] = 0

            # For MOMENTUM_CHASE: use entry_premium above LTP
            if entry.intraday_mode == "MOMENTUM_CHASE" and ltp > 0:
                limit_price = ltp * (1.0 + entry.entry_premium)
            else:
                limit_price = entry.entry_price

            signals.append(TradeSignal(
                symbol     = candidate.symbol,
                signal     = Signal.BUY,
                quantity   = qty,
                order_type = order_type,
                price      = limit_price if entry.use_limit else 0.0,
                reason     = (
                    f"score={candidate.composite:.1f} "
                    f"(tech={candidate.technical:.0f} "
                    f"fund={candidate.fundamental:.0f} "
                    f"mom={candidate.momentum:.0f}) | {entry.reason}"
                ),
            ))

        # ---- SELL candidates — statistically-derived, P&L-aware exit logic ----
        #
        # Exit levels (stop-loss, take-profit, trailing stop) are computed fresh
        # each tick from the stock's own ATR and return distribution (VaR).
        # No hardcoded percentages — a volatile stock gets a wider stop
        # automatically; a stable stock gets a tighter one.
        #
        # Priority order:
        #   1. Stop-loss  (ATR + VaR blended — tightest wins)
        #   2. Take-profit (entry + R:R × stop_distance)
        #   3. Chandelier trailing stop (peak − ATR×mult) — arms after N% gain
        #   4. Score exit — only fires when already in profit
        #   5. Emergency exit — score collapsed below threshold (sell even at loss)
        #   6. Intraday score peak exit (Signal A) — score dropped from high-water
        #   7. Intraday collapse exit (Signal B) — simultaneous score+vel+price collapse
        from strategies.exit_signals import compute_exit_levels

        score_map = {s.symbol: s for s in scores}
        cfg = self.config

        for pos in self.positions.all_open():
            ltp = self._fetcher.get_ltp(pos.symbol)
            if ltp <= 0:
                continue

            pos.update_peak(ltp)
            pnl_pct = pos.pct_change(ltp)

            # Compute statistical exit levels from this stock's OHLCV history
            df = self._fetcher._cache.load_ohlcv(pos.symbol)
            exits = compute_exit_levels(
                df                       = df,
                avg_buy_price            = pos.avg_buy_price,
                peak_price               = pos.peak_price,
                atr_period               = cfg.exit_atr_period,
                atr_stop_mult            = cfg.exit_atr_stop_mult,
                atr_chandelier_mult      = cfg.exit_atr_chandelier_mult,
                risk_reward_ratio        = cfg.exit_risk_reward_ratio,
                var_period               = cfg.exit_var_period,
                var_confidence           = cfg.exit_var_confidence,
                trailing_activation_pct  = cfg.exit_trailing_activation_pct,
            )

            sell_reason: str | None = None

            # Ensure score high-water is tracked for this position on every tick
            # (initialises on first tick if not already set)
            from strategies.exit_signals import _score_high_water as _hw_dict
            stock_score_now = score_map.get(pos.symbol)
            if stock_score_now is not None and pos.symbol not in _hw_dict:
                _hw_dict[pos.symbol] = stock_score_now.composite

            # 1. Stop-loss (ATR/VaR blended — computed per stock)
            if ltp <= exits.stop_loss:
                sell_reason = (
                    f"STOP LOSS [{exits.method}]: "
                    f"ltp=₹{ltp:.2f} ≤ stop=₹{exits.stop_loss:.2f} "
                    f"({exits.stop_pct:.1f}% below entry, ATR={exits.atr:.2f})"
                )

            # 2. Take-profit
            elif exits.take_profit > 0 and ltp >= exits.take_profit:
                tp_pct = (exits.take_profit - pos.avg_buy_price) / pos.avg_buy_price * 100
                sell_reason = (
                    f"TAKE PROFIT: ltp=₹{ltp:.2f} ≥ tp=₹{exits.take_profit:.2f} "
                    f"(+{tp_pct:.1f}%, R:R={cfg.exit_risk_reward_ratio:.1f})"
                )

            # 3. Chandelier trailing stop
            elif exits.trail_armed and ltp <= exits.trail_level:
                sell_reason = (
                    f"CHANDELIER STOP: ltp=₹{ltp:.2f} ≤ trail=₹{exits.trail_level:.2f} "
                    f"(peak=₹{pos.peak_price:.2f}, ATR×{cfg.exit_atr_chandelier_mult}={exits.atr * cfg.exit_atr_chandelier_mult:.2f})"
                )

            else:
                stock_score = score_map.get(pos.symbol)
                if stock_score is not None:
                    composite = stock_score.composite

                    # Compute score velocity for this position (for intraday exits)
                    import numpy as np
                    pos_history = self._score_history.get(pos.symbol)
                    all_s = list(pos_history) + [composite]
                    if len(all_s) >= 3:
                        n = min(cfg.entry_velocity_window, len(all_s))
                        vals = np.array(all_s[-n:], dtype=float)
                        score_vel = float(np.polyfit(np.arange(n, dtype=float), vals, 1)[0])
                    else:
                        score_vel = 0.0

                    # Get intraday price velocity for this position
                    _, pos_v_recent = self._fetcher.get_intraday_velocities(pos.symbol)

                    # 4. Score exit — only when in profit
                    if composite < cfg.score_sell_threshold and pnl_pct > 0:
                        sell_reason = (
                            f"SCORE EXIT (in profit +{pnl_pct:.1f}%): "
                            f"score={composite:.1f} < {cfg.score_sell_threshold:.0f}"
                        )

                    # 5. Emergency exit — fundamental breakdown
                    elif composite < cfg.score_emergency_sell_threshold:
                        sell_reason = (
                            f"EMERGENCY EXIT: score={composite:.1f} "
                            f"< emergency={cfg.score_emergency_sell_threshold:.0f}, "
                            f"P&L={pnl_pct:+.1f}%"
                        )

                    # 6 & 7. Intraday smart exits (Signal A: peak exit, Signal B: collapse)
                    else:
                        should_exit, intraday_reason = check_intraday_exit(
                            symbol                   = pos.symbol,
                            composite_score          = composite,
                            score_velocity           = score_vel,
                            v_recent                 = pos_v_recent,
                            min_score                = cfg.score_buy_threshold,
                            peak_exit_pct            = cfg.score_peak_exit_pct,
                            collapse_score_ratio     = cfg.collapse_score_ratio,
                            collapse_velocity_floor  = cfg.collapse_velocity_threshold,
                            collapse_price_vel_floor = cfg.collapse_price_vel_threshold,
                        )
                        if should_exit:
                            sell_reason = f"INTRADAY EXIT: {intraday_reason}"

            if sell_reason:
                clear_high_water(pos.symbol)
                signals.append(TradeSignal(
                    symbol=pos.symbol,
                    signal=Signal.SELL,
                    quantity=pos.quantity,
                    reason=sell_reason,
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
