"""
paper_ledger.py — Persistent paper trading ledger for simulation mode.

Every BUY deducts from the virtual cash balance.
Every SELL credits back the proceeds and records the realized P&L.

The ledger is persisted to cache/paper_ledger.json after every trade,
so it survives bot restarts and accumulates over weeks/months of monitoring.

Use BOT_DRY_RUN=true + RISK_DRY_RUN_BALANCE=100000 in .env to enable.
Reset the ledger by deleting cache/paper_ledger.json.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger("PaperLedger")


# ── Trade record ──────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    symbol:        str
    action:        str    # "BUY" or "SELL"
    quantity:      int
    price:         float
    timestamp:     str    # ISO format
    pnl:           float = 0.0   # realised P&L — only meaningful for SELL
    avg_buy_price: float = 0.0   # entry price — only for SELL


# ── Ledger ────────────────────────────────────────────────────────────────────

class PaperLedger:
    """
    Tracks virtual cash and trade history for paper trading.

    Lifecycle:
      on_buy(symbol, qty, price)                  → deduct cash, log trade
      on_sell(symbol, qty, price, avg_buy_price)  → credit cash, compute P&L, log trade
      snapshot(open_positions, fetcher)            → full portfolio picture for EOD email
    """

    def __init__(self, starting_balance: float, ledger_path: Path) -> None:
        self._path             = ledger_path
        self._starting_balance = starting_balance
        self._cash             = starting_balance
        self._trades: list[TradeRecord] = []
        self._load()

    # ── Write operations ──────────────────────────────────────────────────────

    def on_buy(self, symbol: str, qty: int, price: float) -> None:
        cost = qty * price
        self._cash -= cost
        self._trades.append(TradeRecord(
            symbol        = symbol,
            action        = "BUY",
            quantity      = qty,
            price         = price,
            timestamp     = datetime.now().isoformat(timespec="seconds"),
            pnl           = 0.0,
            avg_buy_price = 0.0,
        ))
        log.info(
            "📄 Paper BUY : %s  x%d @ ₹%.2f  cost=₹%.2f  cash_left=₹%.2f",
            symbol, qty, price, cost, self._cash,
        )
        self._save()

    def on_sell(
        self,
        symbol:        str,
        qty:           int,
        price:         float,
        avg_buy_price: float,
    ) -> None:
        proceeds = qty * price
        pnl      = (price - avg_buy_price) * qty
        self._cash += proceeds
        self._trades.append(TradeRecord(
            symbol        = symbol,
            action        = "SELL",
            quantity      = qty,
            price         = price,
            timestamp     = datetime.now().isoformat(timespec="seconds"),
            pnl           = pnl,
            avg_buy_price = avg_buy_price,
        ))
        sign = "✅" if pnl >= 0 else "❌"
        log.info(
            "📄 Paper SELL: %s  x%d @ ₹%.2f  pnl=%s₹%.2f  cash=₹%.2f",
            symbol, qty, price, "+" if pnl >= 0 else "-", abs(pnl), self._cash,
        )
        self._save()

    # ── Read operations ───────────────────────────────────────────────────────

    @property
    def cash(self) -> float:
        return self._cash

    @property
    def starting_balance(self) -> float:
        return self._starting_balance

    def all_trades(self) -> list[TradeRecord]:
        return list(self._trades)

    def todays_trades(self) -> list[TradeRecord]:
        today = date.today().isoformat()
        return [t for t in self._trades if t.timestamp.startswith(today)]

    def total_realized_pnl(self) -> float:
        return sum(t.pnl for t in self._trades if t.action == "SELL")

    def realized_pnl_by_symbol(self) -> dict[str, float]:
        result: dict[str, float] = {}
        for t in self._trades:
            if t.action == "SELL":
                result[t.symbol] = result.get(t.symbol, 0.0) + t.pnl
        return result

    def snapshot(self, open_positions: list, fetcher) -> dict:
        """
        Full portfolio snapshot for the EOD email.

        Returns a dict with:
          cash, open_value, total_value, total_gain, return_pct,
          open_rows (list of dicts, one per open position)
        """
        open_rows  = []
        open_value = 0.0

        for pos in open_positions:
            ltp = fetcher.get_ltp(pos.symbol) if fetcher else 0.0
            if ltp > 0:
                val   = pos.quantity * ltp
                unreal = (ltp - pos.avg_buy_price) * pos.quantity
                pct   = (ltp - pos.avg_buy_price) / pos.avg_buy_price * 100 if pos.avg_buy_price else 0
                open_value += val
                open_rows.append({
                    "symbol":    pos.symbol,
                    "qty":       pos.quantity,
                    "avg_buy":   pos.avg_buy_price,
                    "ltp":       ltp,
                    "value":     val,
                    "unrealized": unreal,
                    "pct":       pct,
                })
            else:
                cost = pos.quantity * pos.avg_buy_price
                open_value += cost
                open_rows.append({
                    "symbol":    pos.symbol,
                    "qty":       pos.quantity,
                    "avg_buy":   pos.avg_buy_price,
                    "ltp":       0.0,
                    "value":     cost,
                    "unrealized": 0.0,
                    "pct":       0.0,
                })

        total_value = self._cash + open_value
        total_gain  = total_value - self._starting_balance
        return_pct  = (total_gain / self._starting_balance * 100) if self._starting_balance else 0.0

        return {
            "starting_balance": self._starting_balance,
            "cash":             self._cash,
            "open_value":       open_value,
            "total_value":      total_value,
            "total_gain":       total_gain,
            "return_pct":       return_pct,
            "open_rows":        open_rows,
        }

    def open_positions(self) -> dict[str, tuple[int, float]]:
        """
        Reconstruct currently open positions from trade history.

        Called at bot startup (dry_run mode) to restore positions that were
        open when the bot last shut down, so it doesn't re-buy them.

        Returns: { symbol: (quantity, avg_buy_price) }
        """
        qty_map: dict[str, int]   = {}
        cost_map: dict[str, float] = {}

        for t in self._trades:
            sym = t.symbol
            if t.action == "BUY":
                prev_qty  = qty_map.get(sym, 0)
                prev_cost = cost_map.get(sym, 0.0)
                new_qty   = prev_qty + t.quantity
                cost_map[sym] = (prev_cost + t.price * t.quantity)
                qty_map[sym]  = new_qty
            elif t.action == "SELL":
                qty_map[sym]  = qty_map.get(sym, 0) - t.quantity
                # proportionally reduce cost basis
                if qty_map[sym] <= 0:
                    qty_map.pop(sym, None)
                    cost_map.pop(sym, None)

        result = {}
        for sym, qty in qty_map.items():
            if qty > 0:
                avg = cost_map.get(sym, 0.0) / qty
                result[sym] = (qty, avg)
        return result

    # ── Persistence ───────────────────────────────────────────────────────────

    def _save(self) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "starting_balance": self._starting_balance,
                "cash":             self._cash,
                "trades":           [asdict(t) for t in self._trades],
            }
            self._path.write_text(json.dumps(data, indent=2))
        except Exception as exc:
            log.error("PaperLedger save failed: %s", exc)

    def _load(self) -> None:
        if not self._path.exists():
            log.info(
                "PaperLedger: no existing ledger at %s — starting fresh with ₹%.2f",
                self._path, self._starting_balance,
            )
            return
        try:
            data = json.loads(self._path.read_text())
            self._starting_balance = data.get("starting_balance", self._starting_balance)
            self._cash             = data.get("cash",             self._starting_balance)
            self._trades           = [TradeRecord(**t) for t in data.get("trades", [])]
            log.info(
                "PaperLedger loaded: %d trades | cash=₹%.2f | realized P&L=₹%+.2f",
                len(self._trades), self._cash, self.total_realized_pnl(),
            )
        except Exception as exc:
            log.error("PaperLedger load failed (%s) — starting fresh.", exc)
