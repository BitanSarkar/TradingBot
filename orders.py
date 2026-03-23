from __future__ import annotations

import logging
import math
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from config import Config
    from data.fetcher import DataFetcher


class OrderManager:
    """
    Thin wrapper around GrowwAPI for placing and querying orders.

    When config.dry_run is True no real orders are sent — useful for
    back-testing your strategy logic before going live.
    """

    def __init__(self, groww_client, config: "Config") -> None:
        self._client  = groww_client
        self._config  = config
        self._fetcher: Optional["DataFetcher"] = None   # set by attach_fetcher()
        self.log = logging.getLogger("OrderManager")
        self._order_history: list[dict] = []  # local record of every order this session

    def attach_fetcher(self, fetcher: "DataFetcher") -> None:
        """Wire in the DataFetcher so dry-run mode can use cached close as LTP."""
        self._fetcher = fetcher

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def buy(
        self,
        symbol: str,
        quantity: int,
        order_type: str = "MARKET",
        price: float = 0.0,
    ) -> Optional[str]:
        return self._place(symbol, quantity, "BUY", order_type, price)

    def sell(
        self,
        symbol: str,
        quantity: int,
        order_type: str = "MARKET",
        price: float = 0.0,
    ) -> Optional[str]:
        return self._place(symbol, quantity, "SELL", order_type, price)

    def available_balance(self) -> float:
        """Return available CNC (delivery) cash balance from Groww wallet.
        Returns 0.0 on error or in dry-run mode (caller must handle gracefully)."""
        if self._config.dry_run:
            # In dry-run, return a large fake balance so sizing math still works
            return float(self._config.dry_run_balance)
        try:
            margin = self._client.get_available_margin_details()
            equity = margin.get("equity", {}) or {}
            return float(equity.get("cnc_balance_available", 0.0))
        except Exception as exc:
            self.log.warning("Could not fetch wallet balance: %s — defaulting to 0", exc)
            return 0.0

    def compute_quantity(self, symbol: str, open_slots: int) -> int:
        """
        Compute how many shares to buy for `symbol` given current wallet balance.

        Logic:
          capital_per_trade = (balance × deploy_fraction) / open_slots
          qty = floor(capital_per_trade / ltp)

        Falls back to config.quantity_per_trade if:
          - dynamic_sizing is disabled
          - balance is 0 or LTP fetch fails
          - computed qty < 1 (can't afford even one share)
        """
        if not self._config.dynamic_sizing:
            return self._config.quantity_per_trade

        balance = self.available_balance()
        if balance <= 0:
            self.log.warning("%s: wallet balance is ₹0 — skipping dynamic sizing", symbol)
            return self._config.quantity_per_trade

        slots = max(1, open_slots)
        capital_per_trade = (balance * self._config.deploy_fraction) / slots

        ltp = self._fetch_ltp(symbol)
        if ltp is None or ltp <= 0:
            self.log.warning("%s: could not fetch LTP — falling back to static qty", symbol)
            return self._config.quantity_per_trade

        qty = math.floor(capital_per_trade / ltp)
        if qty < 1:
            self.log.warning(
                "%s: LTP ₹%.2f exceeds per-trade capital ₹%.2f — skipping",
                symbol, ltp, capital_per_trade,
            )
            return 0   # caller should skip this signal

        qty = min(qty, self._config.max_quantity_per_order)
        self.log.info(
            "%s: balance=₹%.0f  slots=%d  capital/trade=₹%.0f  LTP=₹%.2f  → qty=%d",
            symbol, balance, slots, capital_per_trade, ltp, qty,
        )
        return qty

    def _fetch_ltp(self, symbol: str) -> Optional[float]:
        """Fetch last traded price for a single NSE equity symbol via Groww API."""
        try:
            key = f"NSE_{symbol}"
            result = self._client.get_ltp(
                exchange_trading_symbols=(key,),
                segment=self._client.SEGMENT_CASH,
                timeout=5,
            )
            return float(result.get(key, 0)) or None
        except Exception as exc:
            self.log.warning("LTP fetch failed for %s: %s", symbol, exc)
            # Fall back to cached OHLCV close if Groww API fails
            if self._fetcher is not None:
                ltp = self._fetcher.get_ltp(symbol)
                return ltp if ltp > 0 else None
            return None

    def get_order_status(self, order_id: str) -> Optional[dict]:
        if self._config.dry_run:
            return {"status": "COMPLETE", "order_id": order_id}
        try:
            return self._client.get_order_details(order_id)
        except Exception as exc:
            self.log.error("get_order_status failed for %s: %s", order_id, exc)
            return None

    def get_order_history(self) -> list[dict]:
        return list(self._order_history)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _place(
        self,
        symbol: str,
        quantity: int,
        transaction_type: str,
        order_type: str,
        price: float,
    ) -> Optional[str]:
        # Safety: clip quantity to configured max
        if quantity > self._config.max_quantity_per_order:
            self.log.warning(
                "Requested qty %d exceeds max %d — clipping.",
                quantity,
                self._config.max_quantity_per_order,
            )
            quantity = self._config.max_quantity_per_order

        record = {
            "symbol": symbol,
            "qty": quantity,
            "type": transaction_type,
            "order_type": order_type,
            "price": price,
        }

        if self._config.dry_run:
            fake_id = f"DRY-{transaction_type}-{symbol}-{quantity}"
            self.log.info("[DRY RUN] Would place %s: %s", transaction_type, record)
            self._order_history.append({**record, "order_id": fake_id, "status": "SIMULATED"})
            return fake_id

        try:
            kwargs = dict(
                trading_symbol=symbol,
                quantity=quantity,
                validity=getattr(self._client, f"VALIDITY_{self._config.default_validity}"),
                exchange=getattr(self._client, f"EXCHANGE_{self._config.default_exchange}"),
                segment=getattr(self._client, f"SEGMENT_{self._config.default_segment}"),
                product=getattr(self._client, f"PRODUCT_{self._config.default_product}"),
                order_type=getattr(self._client, f"ORDER_TYPE_{order_type}"),
                transaction_type=getattr(self._client, f"TRANSACTION_TYPE_{transaction_type}"),
            )
            if order_type == "LIMIT" and price > 0:
                kwargs["price"] = price

            response = self._client.place_order(**kwargs)
            order_id: str = response["groww_order_id"]
            self.log.info("%s order placed — %s x%d | order_id=%s", transaction_type, symbol, quantity, order_id)
            self._order_history.append({**record, "order_id": order_id, "status": "PLACED"})
            return order_id

        except Exception as exc:
            self.log.error("Failed to place %s order for %s: %s", transaction_type, symbol, exc)
            return None
