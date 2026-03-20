"""
scoring/formulas/sectors/default.py

DefaultSectorScorer — used for any sector without a dedicated scorer.
Combines TechnicalScorer + FundamentalScorer with balanced weights.
You can override this scorer's weights at runtime via the registry.
"""

from __future__ import annotations

import pandas as pd

from scoring.formulas.base import BaseScorer, StockScore
from scoring.formulas.technical import TechnicalScorer
from scoring.formulas.fundamental import FundamentalScorer


class DefaultSectorScorer(BaseScorer):
    """
    Balanced scorer:
        Technical   40%
        Fundamental 35%
        Momentum    25%   (momentum is already inside TechnicalScorer but we
                           expose it as a top-level knob via weights)
    """

    # Top-level weights between the three sub-buckets
    DEFAULT_WEIGHTS = {
        "technical":   0.40,
        "fundamental": 0.35,
        "momentum":    0.25,
    }

    def __init__(self) -> None:
        super().__init__()
        self._tech  = TechnicalScorer()
        self._fund  = FundamentalScorer()

    # Allow caller to drill into sub-scorer weights
    @property
    def technical_scorer(self) -> TechnicalScorer:
        return self._tech

    @property
    def fundamental_scorer(self) -> FundamentalScorer:
        return self._fund

    def score(
        self,
        symbol: str,
        sector: str,
        df: pd.DataFrame,
        fundamentals: dict,
    ) -> StockScore:
        tech_score,  tech_comp  = self._tech.compute(df)
        fund_score,  fund_comp  = self._fund.compute(fundamentals, sector)

        # Momentum is already part of technical but surfaced separately
        mom_score = tech_comp.get("momentum", tech_score)

        # Apply extra injected metrics (from add_metric())
        extra_components: dict[str, float] = {}
        extra_total_weight = 0.0
        extra_weighted_sum = 0.0
        for name, (fn, weight) in self._extra_metrics.items():
            try:
                val = fn(df, fundamentals)
                val = self._clamp(float(val))
                extra_components[name] = val
                extra_total_weight += weight
                extra_weighted_sum  += val * weight
            except Exception:
                extra_components[name] = 50.0

        # Re-weight to accommodate extras
        base_weight = 1.0 - extra_total_weight
        w = self._weights  # shorthand

        composite = (
            tech_score   * w["technical"]   * base_weight +
            fund_score   * w["fundamental"] * base_weight +
            mom_score    * w["momentum"]    * base_weight +
            extra_weighted_sum
        )
        composite = self._clamp(composite)

        return StockScore(
            symbol=symbol,
            sector=sector,
            composite=round(composite, 2),
            technical=round(tech_score, 2),
            fundamental=round(fund_score, 2),
            momentum=round(mom_score, 2),
            components={**tech_comp, **fund_comp, **extra_components},
        )
