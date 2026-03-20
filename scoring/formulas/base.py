"""
scoring/formulas/base.py — Base scorer and StockScore dataclass.

Every scorer (technical, fundamental, sector-specific) inherits BaseScorer
and implements `score(df, fundamentals) -> StockScore`.

StockScore
----------
A fully transparent score object:
  .composite   — final weighted score (0–100)
  .technical   — sub-score from price/volume indicators
  .fundamental — sub-score from financial ratios
  .momentum    — sub-score from price momentum
  .components  — dict of every individual metric value, for auditing

Weights
-------
Default weights are defined on the class but can be updated at runtime:
    scorer.set_weights(technical=0.5, fundamental=0.3, momentum=0.2)

Custom Metrics
--------------
Add extra metrics without subclassing:
    scorer.add_metric("npa_ratio", my_npa_fn, weight=0.15)
    scorer.remove_metric("pe_ratio")
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Optional

import pandas as pd


@dataclass
class StockScore:
    symbol:      str
    sector:      str
    composite:   float              # 0–100 final score
    technical:   float = 0.0       # 0–100
    fundamental: float = 0.0       # 0–100
    momentum:    float = 0.0       # 0–100
    components:  dict  = field(default_factory=dict)   # metric name -> raw value

    def __repr__(self) -> str:
        return (
            f"StockScore({self.symbol} | sector={self.sector} | "
            f"composite={self.composite:.1f} | "
            f"tech={self.technical:.1f} fund={self.fundamental:.1f} mom={self.momentum:.1f})"
        )

    def to_dict(self) -> dict:
        return {
            "symbol":      self.symbol,
            "sector":      self.sector,
            "composite":   round(self.composite, 2),
            "technical":   round(self.technical, 2),
            "fundamental": round(self.fundamental, 2),
            "momentum":    round(self.momentum, 2),
            **{f"comp_{k}": round(v, 4) if isinstance(v, float) else v
               for k, v in self.components.items()},
        }


# Metric type alias:  (ohlcv_df, fundamentals_dict) -> float (0–100)
MetricFn = Callable[[pd.DataFrame, dict], float]


class BaseScorer(ABC):
    """
    Base class for all scorers.

    Subclass and override `score()` — OR use the default implementation
    which assembles a composite from registered sub-scorers with weights.

    Default weight breakdown (override freely):
        technical   : 40%
        fundamental : 35%
        momentum    : 25%
    """

    # ---- Default top-level weights -----------------------------------
    DEFAULT_WEIGHTS: dict[str, float] = {
        "technical":   0.40,
        "fundamental": 0.35,
        "momentum":    0.25,
    }

    def __init__(self) -> None:
        self._weights: dict[str, float] = dict(self.DEFAULT_WEIGHTS)
        # Extra per-metric callables injected at runtime
        self._extra_metrics: dict[str, tuple[MetricFn, float]] = {}

    # ------------------------------------------------------------------
    # Public configuration API
    # ------------------------------------------------------------------

    def set_weights(self, **kwargs: float) -> "BaseScorer":
        """
        Update top-level weights.  Automatically re-normalises to sum=1.

        scorer.set_weights(technical=0.6, fundamental=0.2, momentum=0.2)
        """
        for k, v in kwargs.items():
            if k in self._weights:
                self._weights[k] = float(v)
            else:
                raise ValueError(f"Unknown weight key '{k}'. Valid keys: {list(self._weights)}")
        # Normalise
        total = sum(self._weights.values())
        if total > 0:
            for k in self._weights:
                self._weights[k] /= total
        return self

    def add_metric(self, name: str, fn: MetricFn, weight: float = 0.10) -> "BaseScorer":
        """
        Inject a custom metric into this scorer.

        The metric fn receives (ohlcv_df, fundamentals_dict) and should
        return a float in [0, 100].

        Example:
            def my_metric(df, fund):
                npa = fund.get("npa_ratio", 0.5)
                return max(0, 100 - npa * 200)   # lower NPA → higher score

            banking_scorer.add_metric("npa_quality", my_metric, weight=0.15)
        """
        self._extra_metrics[name] = (fn, weight)
        return self

    def remove_metric(self, name: str) -> "BaseScorer":
        self._extra_metrics.pop(name, None)
        return self

    def get_weights(self) -> dict[str, float]:
        return dict(self._weights)

    # ------------------------------------------------------------------
    # Core scoring (subclasses override this)
    # ------------------------------------------------------------------

    @abstractmethod
    def score(
        self,
        symbol: str,
        sector: str,
        df: pd.DataFrame,
        fundamentals: dict,
    ) -> StockScore:
        """
        Compute and return a StockScore for the given symbol.

        Parameters
        ----------
        symbol       : NSE trading symbol (e.g. "RELIANCE")
        sector       : sector label (e.g. "IT", "BANKING")
        df           : OHLCV DataFrame with columns Open, High, Low, Close, Volume
        fundamentals : dict from yfinance .info (P/E, P/B, ROE, etc.)
        """

    # ------------------------------------------------------------------
    # Helper utilities available to subclasses
    # ------------------------------------------------------------------

    @staticmethod
    def _safe(value: Optional[float], default: float = 50.0) -> float:
        """Return default when value is None/NaN/inf."""
        if value is None:
            return default
        try:
            v = float(value)
            import math
            return default if math.isnan(v) or math.isinf(v) else v
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
        return max(lo, min(hi, value))

    @staticmethod
    def _normalise(value: float, lo: float, hi: float, invert: bool = False) -> float:
        """Map value from [lo, hi] to [0, 100]. Set invert=True for metrics where lower = better."""
        if hi == lo:
            return 50.0
        score = (value - lo) / (hi - lo) * 100.0
        score = max(0.0, min(100.0, score))
        return 100.0 - score if invert else score
