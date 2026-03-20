from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from orders import OrderManager
    from positions import PositionTracker
    from config import Config


class Signal(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class TradeSignal:
    symbol: str
    signal: Signal
    quantity: int
    order_type: str = "MARKET"   # MARKET | LIMIT
    price: float = 0.0           # only used for LIMIT orders
    reason: str = ""             # human-readable explanation, useful for logging


class BaseStrategy(ABC):
    """
    All strategies must inherit from this class and implement `generate_signals`.

    Lifecycle
    ---------
    1. Bot calls `on_start()` once before the trading loop begins.
    2. Bot calls `generate_signals()` on every tick (every poll_interval seconds).
    3. Bot calls `on_stop()` once when shutting down (KeyboardInterrupt or error).

    Available helpers
    -----------------
    self.orders    -> OrderManager  (place / cancel / query orders)
    self.positions -> PositionTracker (query what you currently hold)
    self.config    -> Config
    self.log       -> Logger
    """

    def __init__(
        self,
        config: "Config",
        orders: "OrderManager",
        positions: "PositionTracker",
    ) -> None:
        self.config = config
        self.orders = orders
        self.positions = positions

        import logging
        self.log = logging.getLogger(f"Strategy.{self.__class__.__name__}")

    # ------------------------------------------------------------------
    # Override these in your strategy
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """Called once before the loop. Use for initialization / warm-up."""

    def on_stop(self) -> None:
        """Called once on shutdown. Use for cleanup / square-off logic."""

    @abstractmethod
    def generate_signals(self) -> list[TradeSignal]:
        """
        Core strategy logic — called every tick.

        Returns a list of TradeSignal objects.  Return an empty list or
        signals with Signal.HOLD to do nothing this tick.
        """
