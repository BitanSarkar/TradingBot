from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Position:
    symbol: str
    quantity: int = 0
    avg_buy_price: float = 0.0
    realized_pnl: float = 0.0   # booked profit/loss this session

    @property
    def is_flat(self) -> bool:
        return self.quantity == 0

    def unrealized_pnl(self, ltp: float) -> float:
        """Unrealized P&L given a last-traded-price."""
        return (ltp - self.avg_buy_price) * self.quantity


class PositionTracker:
    """
    In-memory position tracker.

    Updated every time OrderManager places a (non-dry-run) order, OR
    you can call `refresh_from_broker()` to sync with Groww's live data.
    """

    def __init__(self, groww_client, config) -> None:
        self._client = groww_client
        self._config = config
        self.log = logging.getLogger("PositionTracker")
        self._positions: dict[str, Position] = {}

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get(self, symbol: str) -> Position:
        """Always returns a Position object (quantity=0 if not held)."""
        return self._positions.get(symbol, Position(symbol=symbol))

    def all_open(self) -> list[Position]:
        return [p for p in self._positions.values() if not p.is_flat]

    def total_realized_pnl(self) -> float:
        return sum(p.realized_pnl for p in self._positions.values())

    def count_open(self) -> int:
        return len(self.all_open())

    # ------------------------------------------------------------------
    # Writes (called by the bot after each order)
    # ------------------------------------------------------------------

    def record_buy(self, symbol: str, quantity: int, price: float) -> None:
        pos = self._positions.setdefault(symbol, Position(symbol=symbol))
        total_cost = pos.avg_buy_price * pos.quantity + price * quantity
        pos.quantity += quantity
        pos.avg_buy_price = total_cost / pos.quantity if pos.quantity else 0.0
        self.log.debug("Position after BUY  %s: qty=%d avg=%.2f", symbol, pos.quantity, pos.avg_buy_price)

    def record_sell(self, symbol: str, quantity: int, price: float) -> None:
        pos = self._positions.get(symbol)
        if pos is None or pos.quantity < quantity:
            self.log.warning("Tried to record sell for %s but position is insufficient.", symbol)
            return
        pnl = (price - pos.avg_buy_price) * quantity
        pos.realized_pnl += pnl
        pos.quantity -= quantity
        if pos.is_flat:
            pos.avg_buy_price = 0.0
        self.log.debug(
            "Position after SELL %s: qty=%d pnl_this_trade=%.2f total_realized=%.2f",
            symbol, pos.quantity, pnl, pos.realized_pnl,
        )

    # ------------------------------------------------------------------
    # Broker sync (optional, call when you want live data from Groww)
    # ------------------------------------------------------------------

    def refresh_from_broker(self) -> None:
        """Overwrite local state with live positions from Groww."""
        if self._config.dry_run:
            self.log.debug("Dry-run mode — skipping broker position refresh.")
            return
        try:
            live = self._client.get_positions()  # adjust to actual API method name
            self._positions.clear()
            for item in live:
                symbol = item.get("trading_symbol") or item.get("symbol")
                qty = int(item.get("quantity", 0))
                avg = float(item.get("average_price", 0))
                if symbol and qty:
                    self._positions[symbol] = Position(
                        symbol=symbol, quantity=qty, avg_buy_price=avg
                    )
            self.log.info("Positions refreshed from broker: %d open.", self.count_open())
        except Exception as exc:
            self.log.error("Failed to refresh positions: %s", exc)
