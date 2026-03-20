"""
positions.py — tracks what the bot owns and what it's worth right now.

Two sources of truth:
  1. Groww API  (get_holdings())  — your actual demat holdings
  2. Local memory — updated every time the bot places an order this session

On startup the bot calls refresh_from_broker() to seed local state from
Groww.  It re-syncs every N ticks so manual trades or transfers don't get
out of sync.

Terminology
-----------
  avg_buy_price   — weighted average cost per share (cost basis)
  unrealized_pnl  — current market value minus cost basis  (open position)
  realized_pnl    — profit/loss locked in when you sell  (closed trades)
  total_pnl       — unrealized + realized for all positions
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional


log = logging.getLogger("PositionTracker")


@dataclass
class Position:
    symbol:        str
    quantity:      int   = 0
    avg_buy_price: float = 0.0
    realized_pnl:  float = 0.0

    @property
    def is_flat(self) -> bool:
        return self.quantity == 0

    def unrealized_pnl(self, ltp: float) -> float:
        """Open position value vs cost basis at current market price."""
        if self.is_flat:
            return 0.0
        return (ltp - self.avg_buy_price) * self.quantity

    def current_value(self, ltp: float) -> float:
        """Current market value of this position (qty x LTP)."""
        return self.quantity * ltp

    def pct_change(self, ltp: float) -> float:
        """% gain/loss from average buy price."""
        if self.avg_buy_price == 0:
            return 0.0
        return (ltp - self.avg_buy_price) / self.avg_buy_price * 100


class PositionTracker:
    """
    Tracks all open positions and session P&L.

    Usage
    -----
        tracker = PositionTracker(groww_client, config)
        tracker.refresh_from_broker()          # call once at startup

        # After each order:
        tracker.record_buy("TCS", qty=5, price=3456.00)
        tracker.record_sell("TCS", qty=5, price=3520.00)

        # Per-tick portfolio snapshot:
        tracker.print_portfolio(fetcher)       # logs current holdings + P&L
    """

    def __init__(self, groww_client, config) -> None:
        self._client  = groww_client
        self._config  = config
        self._positions: dict[str, Position] = {}

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get(self, symbol: str) -> Position:
        """Always returns a Position (quantity=0 if not held)."""
        return self._positions.get(symbol, Position(symbol=symbol))

    def is_holding(self, symbol: str) -> bool:
        return self.get(symbol).quantity > 0

    def all_open(self) -> list[Position]:
        return [p for p in self._positions.values() if not p.is_flat]

    def count_open(self) -> int:
        return len(self.all_open())

    def total_realized_pnl(self) -> float:
        return sum(p.realized_pnl for p in self._positions.values())

    def total_unrealized_pnl(self, fetcher) -> float:
        """Sum of unrealized P&L across all open positions using live LTP."""
        total = 0.0
        for pos in self.all_open():
            ltp = fetcher.get_ltp(pos.symbol)
            if ltp > 0:
                total += pos.unrealized_pnl(ltp)
        return total

    def total_pnl(self, fetcher) -> float:
        return self.total_realized_pnl() + self.total_unrealized_pnl(fetcher)

    def portfolio_value(self, fetcher) -> float:
        """Current market value of all open positions."""
        total = 0.0
        for pos in self.all_open():
            ltp = fetcher.get_ltp(pos.symbol)
            if ltp > 0:
                total += pos.current_value(ltp)
        return total

    # ------------------------------------------------------------------
    # Writes — called by the bot after each order
    # ------------------------------------------------------------------

    def record_buy(self, symbol: str, quantity: int, price: float) -> None:
        pos = self._positions.setdefault(symbol, Position(symbol=symbol))
        # Weighted average: (old_cost + new_cost) / total_qty
        total_cost        = pos.avg_buy_price * pos.quantity + price * quantity
        pos.quantity      += quantity
        pos.avg_buy_price = total_cost / pos.quantity if pos.quantity else 0.0
        log.debug(
            "BUY  recorded: %s  qty=%d  avg_cost=Rs%.2f",
            symbol, pos.quantity, pos.avg_buy_price,
        )

    def record_sell(self, symbol: str, quantity: int, price: float) -> None:
        pos = self._positions.get(symbol)
        if pos is None or pos.quantity < quantity:
            log.warning(
                "SELL recorded for %s but position is insufficient "
                "(have %d, selling %d).",
                symbol, getattr(pos, "quantity", 0), quantity,
            )
            return
        pnl               = (price - pos.avg_buy_price) * quantity
        pos.realized_pnl  += pnl
        pos.quantity      -= quantity
        if pos.is_flat:
            pos.avg_buy_price = 0.0
        log.debug(
            "SELL recorded: %s  qty_remaining=%d  trade_pnl=Rs%.2f  "
            "session_realized=Rs%.2f",
            symbol, pos.quantity, pnl, pos.realized_pnl,
        )

    # ------------------------------------------------------------------
    # Broker sync — pull live holdings from Groww
    # ------------------------------------------------------------------

    def refresh_from_broker(self) -> None:
        """
        Overwrite local state with live holdings from Groww.

        Groww separates:
          get_holdings()  -> long-term demat holdings (CNC / delivery)
          get_positions() -> intraday positions (MIS trades for today)

        We load both and merge them so the bot is aware of everything you own
        regardless of how it was acquired.

        Call this:
          - Once at startup (seeds local state from real demat account)
          - Every N ticks (catches manual trades done outside the bot)
        """
        if self._config.dry_run:
            log.info(
                "Dry-run mode — skipping Groww sync. "
                "Local positions are bot-session-only."
            )
            return

        # Preserve realized P&L from this session before overwriting
        session_realized = {
            sym: p.realized_pnl
            for sym, p in self._positions.items()
        }
        self._positions.clear()

        loaded = 0
        errors = []

        # ── Long-term demat holdings (CNC / delivery) ──────────────────
        try:
            holdings_resp = self._client.get_holdings()
            # growwapi wraps responses in {"data": {"holdings": [...]}}
            holdings = (
                holdings_resp.get("data", {}).get("holdings", [])
                if isinstance(holdings_resp, dict)
                else []
            )
            for item in holdings:
                sym = (
                    item.get("trading_symbol")
                    or item.get("tradingSymbol")
                    or item.get("symbol", "")
                ).upper().strip()
                qty = int(
                    item.get("quantity")
                    or item.get("holdingQuantity")
                    or 0
                )
                avg = float(
                    item.get("average_price")
                    or item.get("averagePrice")
                    or item.get("ltp")
                    or 0
                )
                if sym and qty > 0:
                    self._positions[sym] = Position(
                        symbol        = sym,
                        quantity      = qty,
                        avg_buy_price = avg,
                        realized_pnl  = session_realized.get(sym, 0.0),
                    )
                    loaded += 1
        except Exception as exc:
            errors.append(f"get_holdings: {exc}")

        # ── Intraday positions (MIS) ────────────────────────────────────
        try:
            positions_resp = self._client.get_positions()
            positions = (
                positions_resp.get("data", {}).get("positions", [])
                if isinstance(positions_resp, dict)
                else []
            )
            for item in positions:
                sym = (
                    item.get("trading_symbol")
                    or item.get("tradingSymbol")
                    or item.get("symbol", "")
                ).upper().strip()
                qty = int(
                    item.get("quantity")
                    or item.get("netQuantity")
                    or 0
                )
                avg = float(
                    item.get("average_price")
                    or item.get("averagePrice")
                    or 0
                )
                if sym and qty > 0:
                    if sym in self._positions:
                        # Merge with existing holding — re-weight average cost
                        existing = self._positions[sym]
                        total_cost = (
                            existing.avg_buy_price * existing.quantity
                            + avg * qty
                        )
                        existing.quantity += qty
                        existing.avg_buy_price = total_cost / existing.quantity
                    else:
                        self._positions[sym] = Position(
                            symbol        = sym,
                            quantity      = qty,
                            avg_buy_price = avg,
                            realized_pnl  = session_realized.get(sym, 0.0),
                        )
                    loaded += 1
        except Exception as exc:
            errors.append(f"get_positions: {exc}")

        if errors:
            log.warning("Broker sync partial failure: %s", " | ".join(errors))

        log.info(
            "Broker sync: %d position(s) loaded from Groww.%s",
            loaded,
            " (partial, check DEBUG log for details)" if errors else "",
        )

    # ------------------------------------------------------------------
    # Portfolio display — called every tick
    # ------------------------------------------------------------------

    def print_portfolio(self, fetcher) -> None:
        """
        Log a compact portfolio snapshot every tick.

        Example:
            Portfolio (3 positions)
            -----------------------------------------------------------------------
            Symbol          Qty   Avg Cost      LTP        Unrealized     Change
            -----------------------------------------------------------------------
            TCS               5   Rs 3,456.00   Rs 3,520.00   +Rs   320.00   +1.8%
            INFY             10   Rs 1,820.00   Rs 1,795.00   -Rs   250.00   -1.4%
            HDFCBANK          3   Rs 1,640.00   Rs 1,680.00   +Rs   120.00   +2.4%
            -----------------------------------------------------------------------
            Unrealized P&L : +Rs 190.00     Portfolio value : Rs 54,635.00
            Realized P&L   : +Rs 850.00     (this session)
        """
        open_positions = self.all_open()
        if not open_positions:
            log.info("Portfolio: no open positions.")
            return

        total_unrealized = 0.0
        total_value      = 0.0
        divider          = "-" * 75

        log.info("Portfolio (%d position%s)",
                 len(open_positions), "s" if len(open_positions) != 1 else "")
        log.info(divider)
        log.info(
            "  %-14s  %4s  %12s  %12s  %12s  %7s",
            "Symbol", "Qty", "Avg Cost", "LTP", "Unrealized", "Change",
        )
        log.info(divider)

        for pos in sorted(open_positions, key=lambda p: p.symbol):
            ltp   = fetcher.get_ltp(pos.symbol)
            upnl  = pos.unrealized_pnl(ltp)
            val   = pos.current_value(ltp)
            pct   = pos.pct_change(ltp)
            total_unrealized += upnl
            total_value      += val
            sign  = "+" if upnl >= 0 else "-"
            log.info(
                "  %-14s  %4d  Rs%10,.2f  Rs%10,.2f  %s Rs%9,.2f  %+.1f%%",
                pos.symbol, pos.quantity,
                pos.avg_buy_price, ltp,
                sign, abs(upnl), pct,
            )

        log.info(divider)
        u_sign = "+" if total_unrealized >= 0 else "-"
        r_sign = "+" if self.total_realized_pnl() >= 0 else "-"
        log.info(
            "  Unrealized P&L : %s Rs%-12.2f  Portfolio value : Rs %.2f",
            u_sign, abs(total_unrealized), total_value,
        )
        log.info(
            "  Realized P&L   : %s Rs%-12.2f  (this session)",
            r_sign, abs(self.total_realized_pnl()),
        )
