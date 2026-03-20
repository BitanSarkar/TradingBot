"""
scoring/formulas/sectors/it.py — IT / Technology sector scorer.

IT sector priorities
---------------------
• High growth trumps low P/E  → revenue & earnings growth weighted heavily
• Profit margins matter more  (SaaS-like companies should have fat margins)
• Low debt is standard in IT  → debt_equity weight moderate
• P/E can be higher           → P/E range relaxed to 15–60
• Technical momentum matters  → we boost technical weight slightly

Overrides vs Default
--------------------
Technical   45% (↑ from 40%)
Fundamental 30% (↓ from 35%)
Momentum    25% (same)

Within Fundamental:
    revenue_growth  20% (↑)
    earn_growth     18% (↑)
    margin          15% (↑)
    pe              10% (↓ — IT stocks command premium P/E, don't penalise too hard)
    roe             15%
    debt_equity      8%
    pb               6%
    curr_ratio       5%
    dividend         3% (↓ — IT rarely pays fat dividends)
"""

from __future__ import annotations

import pandas as pd

from scoring.formulas.base import BaseScorer, StockScore
from scoring.formulas.technical import TechnicalScorer
from scoring.formulas.fundamental import FundamentalScorer


class ITSectorScorer(BaseScorer):
    DEFAULT_WEIGHTS = {
        "technical":   0.45,
        "fundamental": 0.30,
        "momentum":    0.25,
    }

    def __init__(self) -> None:
        super().__init__()
        self._tech = TechnicalScorer()
        self._fund = FundamentalScorer()

        # Tune fundamental weights for IT
        self._fund.set_weights(
            pe=0.10,
            pb=0.06,
            roe=0.15,
            debt_equity=0.08,
            curr_ratio=0.05,
            rev_growth=0.20,
            earn_growth=0.18,
            margin=0.15,
            dividend=0.03,
        )

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
        tech_score, tech_comp = self._tech.compute(df)
        fund_score, fund_comp = self._fund.compute(fundamentals, "IT")
        mom_score = tech_comp.get("momentum", tech_score)

        # Extra injected metrics
        extra_components: dict[str, float] = {}
        extra_weight = 0.0
        extra_sum    = 0.0
        for name, (fn, weight) in self._extra_metrics.items():
            try:
                val = self._clamp(float(fn(df, fundamentals)))
                extra_components[name] = val
                extra_weight += weight
                extra_sum    += val * weight
            except Exception:
                extra_components[name] = 50.0

        base = 1.0 - extra_weight
        w = self._weights

        composite = (
            tech_score  * w["technical"]   * base +
            fund_score  * w["fundamental"] * base +
            mom_score   * w["momentum"]    * base +
            extra_sum
        )

        return StockScore(
            symbol=symbol,
            sector="IT",
            composite=round(self._clamp(composite), 2),
            technical=round(tech_score, 2),
            fundamental=round(fund_score, 2),
            momentum=round(mom_score, 2),
            components={**tech_comp, **fund_comp, **extra_components},
        )
