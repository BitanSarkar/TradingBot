"""
strategies/example.py — A minimal example strategy.

This strategy does nothing real. It exists solely to show you the pattern
for building your own strategies:

  1. Inherit from BaseStrategy.
  2. Implement generate_signals() — return a list of TradeSignal objects.
  3. Optionally override on_start() and on_stop().

Replace this file (or add new files alongside it) with your own logic.
"""

from __future__ import annotations

from strategies.base import BaseStrategy, Signal, TradeSignal


class ExampleStrategy(BaseStrategy):

    SYMBOL = "IDEA"   # Vodafone Idea — cheap, good for testing tiny lots

    def on_start(self) -> None:
        self.log.info("ExampleStrategy started. Watching %s.", self.SYMBOL)
        self._tick_count = 0

    def on_stop(self) -> None:
        # Square off any open position before shutdown
        pos = self.positions.get(self.SYMBOL)
        if not pos.is_flat:
            self.log.info("Squaring off %d shares of %s on shutdown.", pos.quantity, self.SYMBOL)
            self.orders.sell(self.SYMBOL, pos.quantity)

    def generate_signals(self) -> list[TradeSignal]:
        self._tick_count += 1

        # ------------------------------------------------------------------
        # >>> REPLACE everything below with your real strategy logic <<<
        #
        # Typical patterns:
        #
        #   • Fetch OHLCV data from an external source (Yahoo Finance, broker
        #     websocket, etc.) and compute indicators (SMA, RSI, MACD …)
        #
        #   • Compare current LTP against your computed signal thresholds
        #
        #   • Return TradeSignal(symbol, Signal.BUY/SELL, quantity, reason=…)
        #
        # Example skeleton:
        #   ltp = fetch_ltp(self.SYMBOL)
        #   sma = compute_sma(prices, window=20)
        #   if ltp > sma and self.positions.get(self.SYMBOL).is_flat:
        #       return [TradeSignal(self.SYMBOL, Signal.BUY, quantity=1, reason="LTP > SMA20")]
        #   elif ltp < sma and not self.positions.get(self.SYMBOL).is_flat:
        #       qty = self.positions.get(self.SYMBOL).quantity
        #       return [TradeSignal(self.SYMBOL, Signal.SELL, quantity=qty, reason="LTP < SMA20")]
        # ------------------------------------------------------------------

        # Demo: buy on tick 1, sell on tick 3, idle afterwards
        if self._tick_count == 1:
            self.log.info("Tick 1 — emitting BUY signal (demo).")
            return [TradeSignal(
                symbol=self.SYMBOL,
                signal=Signal.BUY,
                quantity=1,
                reason="Demo: first tick buy",
            )]

        if self._tick_count == 3:
            held = self.positions.get(self.SYMBOL).quantity
            if held > 0:
                self.log.info("Tick 3 — emitting SELL signal (demo).")
                return [TradeSignal(
                    symbol=self.SYMBOL,
                    signal=Signal.SELL,
                    quantity=held,
                    reason="Demo: third tick sell",
                )]

        return []  # HOLD — nothing to do this tick
