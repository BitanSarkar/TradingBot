from __future__ import annotations

import logging
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from config import Config


class OrderManager:
    """
    Thin wrapper around GrowwAPI for placing and querying orders.

    When config.dry_run is True no real orders are sent — useful for
    back-testing your strategy logic before going live.
    """

    def __init__(self, groww_client, config: "Config") -> None:
        self._client = groww_client
        self._config = config
        self.log = logging.getLogger("OrderManager")
        self._order_history: list[dict] = []  # local record of every order this session

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
