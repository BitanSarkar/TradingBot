"""
scoring/formulas/sectors/pharma.py — Pharma / Healthcare sector scorer.

Pharma sector priorities
-------------------------
• R&D intensity matters → earnings growth weighted heavily
• Regulatory risk means margins can be volatile → moderate weight
• P/E acceptable at 20–40x due to pipeline premium
• Debt-to-equity is relevant (capex-heavy) but moderate
• Dividend yield is low in growth pharma → low weight
• Technical momentum very important — FDA/DCGI events cause spikes

Overrides vs Default
--------------------
Technical    40%
Fundamental  35%
Momentum     25%

Within Fundamental:
    earn_growth  20% (pipeline commercialisation)
    roe          18%
    margin       16% (EBITDA/net margin proxy)
    pe           12%
    rev_growth   14%
    debt_equity  10%
    pb            6%
    curr_ratio    3%
    dividend      1%
"""

from __future__ import annotations

import pandas as pd

from scoring.formulas.base import BaseScorer, StockScore
from scoring.formulas.technical import TechnicalScorer
from scoring.formulas.fundamental import FundamentalScorer


class PharmaSectorScorer(BaseScorer):
    DEFAULT_WEIGHTS = {
        "technical":   0.40,
        "fundamental": 0.35,
        "momentum":    0.25,
    }

    def __init__(self) -> None:
        super().__init__()
        self._tech = TechnicalScorer()
        self._fund = FundamentalScorer()

        self._fund.set_weights(
            pe=0.12,
            pb=0.06,
            roe=0.18,
            debt_equity=0.10,
            curr_ratio=0.03,
            rev_growth=0.14,
            earn_growth=0.20,
            margin=0.16,
            dividend=0.01,
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
        fund_score, fund_comp = self._fund.compute(fundamentals, "PHARMA")
        mom_score = tech_comp.get("momentum", tech_score)

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
            sector="PHARMA",
            composite=round(self._clamp(composite), 2),
            technical=round(tech_score, 2),
            fundamental=round(fund_score, 2),
            momentum=round(mom_score, 2),
            components={**tech_comp, **fund_comp, **extra_components},
        )
