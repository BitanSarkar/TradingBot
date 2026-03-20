"""
positions.py — tracks what the bot owns and what it's worth right now.

Sources of truth (in priority order):
  1. Pending orders  — placed this session, awaiting fill confirmation
  2. Local memory    — confirmed fills recorded by this session
  3. Groww API       — broker's view (synced at startup + every N ticks)

Why three layers?
  Indian equity markets have T+1 settlement for CNC/delivery orders.
  A stock you buy today won't appear in get_holdings() until tomorrow.
  If we relied on broker sync alone, confirmed buys would vanish every
  10 ticks and the bot would try to re-buy the same stock repeatedly.

  The pending registry solves this:
    - add_pending()          called immediately after placing an order
    - sync_pending_orders()  polls Groww for fill status each tick
    - refresh_from_broker()  re-applies pending buys missing from holdings
    - has_pending()          used by risk gate to block duplicate orders

Idempotency guarantee:
  A symbol can have at most ONE pending order at a time.
  The risk gate rejects any new signal for a symbol that has a pending order.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Optional


log = logging.getLogger("PositionTracker")


# ── Order and position data classes ──────────────────────────────────────────

@dataclass
class PendingOrder:
    symbol:     str
    order_id:   str
    action:     str    # "BUY" or "SELL"
    quantity:   int
    price:      float  # estimated price at order time (actual fill may differ)
    placed_at:  float  # time.time() when placed
    status:     str = "PLACED"

    @property
    def age_seconds(self) -> float:
        return time.time() - self.placed_at


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
        if self.is_flat:
            return 0.0
        return (ltp - self.avg_buy_price) * self.quantity

    def current_value(self, ltp: float) -> float:
        return self.quantity * ltp

    def pct_change(self, ltp: float) -> float:
        if self.avg_buy_price == 0:
            return 0.0
        return (ltp - self.avg_buy_price) / self.avg_buy_price * 100


# ── PositionTracker ───────────────────────────────────────────────────────────

class PositionTracker:
    """
    Thread-safe position + pending order tracker.

    Lifecycle per order (live mode):
      1. Signal generated  →  risk gate checks has_pending(symbol)
      2. Order placed      →  add_pending(symbol, order_id, ...)
      3. Next tick         →  sync_pending_orders() polls Groww
      4a. Status FILLED    →  record_buy/sell(), remove from pending
      4b. Status REJECTED  →  log warning, remove from pending
      4c. Status still OPEN/PLACED → keep waiting (max 30 min TTL)

    Lifecycle per order (dry-run mode):
      1. Signal generated
      2. Order "placed"    →  record_buy/sell() immediately (no real settlement)
    """

    # How long to wait for a fill before giving up and removing the pending order
    _PENDING_TTL_SECONDS = 1800   # 30 minutes

    def __init__(self, groww_client, config) -> None:
        self._client    = groww_client
        self._config    = config
        self._positions: dict[str, Position] = {}
        self._pending:   dict[str, PendingOrder] = {}   # symbol → pending order

    # ------------------------------------------------------------------
    # Pending order management
    # ------------------------------------------------------------------

    def add_pending(
        self,
        symbol: str,
        order_id: str,
        action: str,
        quantity: int,
        price: float,
    ) -> None:
        """
        Register a placed order as pending.
        Replaces any previous pending order for the same symbol.
        """
        self._pending[symbol] = PendingOrder(
            symbol    = symbol,
            order_id  = order_id,
            action    = action,
            quantity  = quantity,
            price     = price,
            placed_at = time.time(),
        )
        log.info(
            "Pending order registered: %s %s x%d @ est. Rs%.2f  (order_id=%s)",
            action, symbol, quantity, price, order_id,
        )

    def has_pending(self, symbol: str) -> bool:
        """True if there is an unconfirmed order for this symbol."""
        return symbol in self._pending

    def pending_count(self) -> int:
        return len(self._pending)

    def sync_pending_orders(self, order_manager) -> None:
        """
        Poll Groww for the fill status of every pending order.

        Called at the start of each tick so the bot always works with
        up-to-date position information before deciding new signals.

        Terminal states (remove from pending):
          COMPLETE / FILLED / EXECUTED  →  record_buy or record_sell
          REJECTED / CANCELLED / FAILED →  log warning, discard
          Age > 30 min                  →  assume stale, discard

        Non-terminal states (keep waiting):
          PLACED / OPEN / PENDING / TRIGGER_PENDING → poll again next tick
        """
        if not self._pending:
            return

        filled_symbols = []

        for symbol, order in list(self._pending.items()):
            # TTL guard — orders older than 30 min are considered stale
            if order.age_seconds > self._PENDING_TTL_SECONDS:
                log.warning(
                    "Pending order for %s (id=%s) expired after %.0f min — "
                    "removing from pending. Check Groww manually.",
                    symbol, order.order_id, order.age_seconds / 60,
                )
                filled_symbols.append(symbol)
                continue

            status_resp = order_manager.get_order_status(order.order_id)
            if status_resp is None:
                continue   # API call failed, try again next tick

            raw_status = (
                status_resp.get("status")
                or status_resp.get("orderStatus")
                or status_resp.get("state")
                or ""
            ).upper().strip()

            if raw_status in ("COMPLETE", "FILLED", "EXECUTED", "TRADED"):
                # Use actual fill price if available; fall back to estimate
                fill_price = float(
                    status_resp.get("average_price")
                    or status_resp.get("averagePrice")
                    or status_resp.get("price")
                    or order.price
                )
                fill_qty = int(
                    status_resp.get("filled_quantity")
                    or status_resp.get("filledQuantity")
                    or order.quantity
                )
                if order.action == "BUY":
                    self.record_buy(symbol, fill_qty, fill_price)
                    log.info(
                        "Order FILLED: BUY %s x%d @ Rs%.2f",
                        symbol, fill_qty, fill_price,
                    )
                else:
                    self.record_sell(symbol, fill_qty, fill_price)
                    log.info(
                        "Order FILLED: SELL %s x%d @ Rs%.2f  "
                        "P&L this trade: Rs%.2f",
                        symbol, fill_qty, fill_price,
                        (fill_price - self._positions.get(symbol, Position(symbol)).avg_buy_price) * fill_qty,
                    )
                filled_symbols.append(symbol)

            elif raw_status in ("REJECTED", "CANCELLED", "FAILED", "EXPIRED"):
                log.warning(
                    "Order %s for %s was %s. Position NOT updated.",
                    order.order_id, symbol, raw_status,
                )
                filled_symbols.append(symbol)

            # else: PLACED / OPEN / PENDING / TRIGGER_PENDING → still waiting

        for sym in filled_symbols:
            self._pending.pop(sym, None)

        if filled_symbols:
            log.debug("Cleared %d pending order(s): %s", len(filled_symbols), filled_symbols)

    # ------------------------------------------------------------------
    # Position reads
    # ------------------------------------------------------------------

    def get(self, symbol: str) -> Position:
        return self._positions.get(symbol, Position(symbol=symbol))

    def is_holding(self, symbol: str) -> bool:
        """True if we have a confirmed position (qty > 0)."""
        return self.get(symbol).quantity > 0

    def all_open(self) -> list[Position]:
        return [p for p in self._positions.values() if not p.is_flat]

    def count_open(self) -> int:
        return len(self.all_open())

    def effective_holdings(self) -> set[str]:
        """
        All symbols we effectively own or have an active order for.
        Used by the strategy to avoid duplicate orders.

          confirmed positions  (qty > 0)
        + pending BUY orders   (placed but not filled yet)
        - pending SELL orders  (we're selling these, don't count as held)
        """
        held = {p.symbol for p in self.all_open()}
        for sym, order in self._pending.items():
            if order.action == "BUY":
                held.add(sym)
            elif order.action == "SELL":
                held.discard(sym)
        return held

    def total_realized_pnl(self) -> float:
        return sum(p.realized_pnl for p in self._positions.values())

    def total_unrealized_pnl(self, fetcher) -> float:
        total = 0.0
        for pos in self.all_open():
            ltp = fetcher.get_ltp(pos.symbol)
            if ltp > 0:
                total += pos.unrealized_pnl(ltp)
        return total

    def total_pnl(self, fetcher) -> float:
        return self.total_realized_pnl() + self.total_unrealized_pnl(fetcher)

    def portfolio_value(self, fetcher) -> float:
        total = 0.0
        for pos in self.all_open():
            ltp = fetcher.get_ltp(pos.symbol)
            if ltp > 0:
                total += pos.current_value(ltp)
        return total

    # ------------------------------------------------------------------
    # Position writes — called after confirmed fills
    # ------------------------------------------------------------------

    def record_buy(self, symbol: str, quantity: int, price: float) -> None:
        pos = self._positions.setdefault(symbol, Position(symbol=symbol))
        total_cost        = pos.avg_buy_price * pos.quantity + price * quantity
        pos.quantity      += quantity
        pos.avg_buy_price = total_cost / pos.quantity if pos.quantity else 0.0
        log.debug(
            "BUY confirmed: %s  qty=%d  new_avg=Rs%.2f",
            symbol, pos.quantity, pos.avg_buy_price,
        )

    def record_sell(self, symbol: str, quantity: int, price: float) -> None:
        pos = self._positions.get(symbol)
        if pos is None or pos.quantity < quantity:
            log.warning(
                "SELL for %s: have %d shares, tried to sell %d.",
                symbol, getattr(pos, "quantity", 0), quantity,
            )
            return
        pnl              = (price - pos.avg_buy_price) * quantity
        pos.realized_pnl += pnl
        pos.quantity     -= quantity
        if pos.is_flat:
            pos.avg_buy_price = 0.0
        log.debug(
            "SELL confirmed: %s  qty_left=%d  trade_pnl=Rs%.2f  session_pnl=Rs%.2f",
            symbol, pos.quantity, pnl, pos.realized_pnl,
        )

    # ------------------------------------------------------------------
    # Broker sync — seed/refresh from Groww
    # ------------------------------------------------------------------

    def refresh_from_broker(self) -> None:
        """
        Load live holdings + intraday positions from Groww.

        T+1 settlement awareness:
          CNC/delivery orders settle the next business day.
          A stock bought today won't appear in get_holdings() until tomorrow.
          After loading broker data, we re-apply any pending BUY orders
          that aren't in broker holdings yet, so the bot correctly treats
          those stocks as "owned" and doesn't try to buy them again.

        Call at:
          • Startup — seeds local state from real demat account
          • Every N ticks — picks up manual trades done in the Groww app
        """
        if self._config.dry_run:
            log.info("Dry-run: skipping Groww sync (positions are session-only).")
            return

        # Preserve this-session realized P&L before clearing
        session_realized = {s: p.realized_pnl for s, p in self._positions.items()}
        self._positions.clear()

        loaded = 0
        errors = []

        # ── CNC / delivery holdings (T+1 settled) ─────────────────────
        try:
            resp = self._client.get_holdings()
            items = (
                resp.get("data", {}).get("holdings", [])
                if isinstance(resp, dict) else []
            )
            for item in items:
                sym = (
                    item.get("trading_symbol")
                    or item.get("tradingSymbol")
                    or item.get("symbol", "")
                ).upper().strip()
                qty = int(item.get("quantity") or item.get("holdingQuantity") or 0)
                avg = float(
                    item.get("average_price") or item.get("averagePrice")
                    or item.get("ltp") or 0
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

        # ── MIS / intraday positions ───────────────────────────────────
        try:
            resp = self._client.get_positions()
            items = (
                resp.get("data", {}).get("positions", [])
                if isinstance(resp, dict) else []
            )
            for item in items:
                sym = (
                    item.get("trading_symbol")
                    or item.get("tradingSymbol")
                    or item.get("symbol", "")
                ).upper().strip()
                qty = int(item.get("quantity") or item.get("netQuantity") or 0)
                avg = float(item.get("average_price") or item.get("averagePrice") or 0)
                if sym and qty > 0:
                    if sym in self._positions:
                        # Merge with existing holding
                        existing = self._positions[sym]
                        total_cost = existing.avg_buy_price * existing.quantity + avg * qty
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

        # ── T+1 gap fill: re-apply pending BUYs not yet in broker ─────
        # These are stocks bought today but not yet settled into holdings.
        # Without this, broker sync would wipe them and the bot would re-buy.
        reapplied = 0
        for sym, order in self._pending.items():
            if order.action == "BUY" and sym not in self._positions:
                self._positions[sym] = Position(
                    symbol        = sym,
                    quantity      = order.quantity,
                    avg_buy_price = order.price,
                    realized_pnl  = session_realized.get(sym, 0.0),
                )
                reapplied += 1
                log.debug(
                    "T+1 gap fill: kept %s x%d in tracker (pending settlement)",
                    sym, order.quantity,
                )

        if errors:
            log.warning("Broker sync partial failure: %s", " | ".join(errors))

        log.info(
            "Broker sync: %d position(s) from Groww%s%s.",
            loaded,
            f" + {reapplied} pending (T+1 gap)" if reapplied else "",
            " (partial, see DEBUG log)" if errors else "",
        )

    # ------------------------------------------------------------------
    # Portfolio display
    # ------------------------------------------------------------------

    def print_portfolio(self, fetcher) -> None:
        open_positions = self.all_open()
        pending        = list(self._pending.values())

        if not open_positions and not pending:
            log.info("Portfolio: no open positions.")
            return

        total_unrealized = 0.0
        total_value      = 0.0
        div = "-" * 75

        log.info(
            "Portfolio — %d confirmed, %d pending",
            len(open_positions), len(pending),
        )
        log.info(div)
        log.info(
            "  %-14s  %4s  %12s  %12s  %12s  %7s",
            "Symbol", "Qty", "Avg Cost", "LTP", "Unrealized", "Change",
        )
        log.info(div)

        for pos in sorted(open_positions, key=lambda p: p.symbol):
            ltp  = fetcher.get_ltp(pos.symbol)
            upnl = pos.unrealized_pnl(ltp)
            val  = pos.current_value(ltp)
            pct  = pos.pct_change(ltp)
            total_unrealized += upnl
            total_value      += val
            sign = "+" if upnl >= 0 else "-"
            log.info(
                "  %-14s  %4d  Rs%10,.2f  Rs%10,.2f  %s Rs%9,.2f  %+.1f%%",
                pos.symbol, pos.quantity, pos.avg_buy_price, ltp,
                sign, abs(upnl), pct,
            )

        if pending:
            log.info("  -- Pending (unconfirmed) --")
            for order in sorted(pending, key=lambda o: o.symbol):
                log.info(
                    "  %-14s  %4d  %-6s  est.Rs%8,.2f  waiting %.0fs",
                    order.symbol, order.quantity, order.action,
                    order.price, order.age_seconds,
                )

        log.info(div)
        u_sign = "+" if total_unrealized >= 0 else "-"
        r_sign = "+" if self.total_realized_pnl() >= 0 else "-"
        log.info(
            "  Unrealized : %s Rs%-12.2f   Portfolio value : Rs %.2f",
            u_sign, abs(total_unrealized), total_value,
        )
        log.info(
            "  Realized   : %s Rs%-12.2f   (this session)",
            r_sign, abs(self.total_realized_pnl()),
        )
