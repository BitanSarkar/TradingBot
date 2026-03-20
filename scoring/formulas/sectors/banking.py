"""
scoring/formulas/sectors/banking.py — Banking / NBFC sector scorer.

Banking sector priorities
--------------------------
• D/E metric is MEANINGLESS for banks (banks run on deposits = "debt")
  → debt_equity weight set to 0
• ROE (Return on Equity) is THE key profitability metric for banks
• P/B is the primary valuation metric (P/E less reliable for banks)
• NIM (Net Interest Margin), NPA (Non-Performing Assets) are critical
  → We expose add_metric() hooks for these (user must inject them)
• Revenue growth = loan book growth (important)
• Current ratio irrelevant for banks → weight 0

Overrides vs Default
--------------------
Technical    35% (↓ — fundamentals matter more for banking)
Fundamental  45% (↑)
Momentum     20% (↓)

Within Fundamental:
    pb          20% (↑ — primary valuation)
    roe         22% (↑ — profitability king)
    rev_growth  15% (loan book growth)
    earn_growth 15%
    margin      12% (NIM proxy)
    pe           8%
    dividend     8%
    debt_equity  0% (irrelevant)
    curr_ratio   0% (irrelevant)

NPA / NIM hooks (inject via registry):
    registry.add_metric("BANKING", "npa_quality",  npa_fn,  weight=0.10)
    registry.add_metric("BANKING", "nim_quality",  nim_fn,  weight=0.08)
"""

from __future__ import annotations

import pandas as pd

from scoring.formulas.base import BaseScorer, StockScore
from scoring.formulas.technical import TechnicalScorer
from scoring.formulas.fundamental import FundamentalScorer


class BankingSectorScorer(BaseScorer):
    DEFAULT_WEIGHTS = {
        "technical":   0.35,
        "fundamental": 0.45,
        "momentum":    0.20,
    }

    def __init__(self) -> None:
        super().__init__()
        self._tech = TechnicalScorer()
        self._fund = FundamentalScorer()

        # Bank-specific fundamental weights
        self._fund.set_weights(
            pe=0.08,
            pb=0.20,
            roe=0.22,
            debt_equity=0.00,   # not meaningful for banks
            curr_ratio=0.00,    # not meaningful for banks
            rev_growth=0.15,
            earn_growth=0.15,
            margin=0.12,
            dividend=0.08,
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
        fund_score, fund_comp = self._fund.compute(fundamentals, "BANKING")
        mom_score = tech_comp.get("momentum", tech_score)

        # Extra injected metrics (e.g. NPA ratio, NIM)
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
            sector="BANKING",
            composite=round(self._clamp(composite), 2),
            technical=round(tech_score, 2),
            fundamental=round(fund_score, 2),
            momentum=round(mom_score, 2),
            components={**tech_comp, **fund_comp, **extra_components},
        )
