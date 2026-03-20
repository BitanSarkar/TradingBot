"""
scoring/registry.py — Formula Registry (the "inject & update" hub).

This is the single place where you wire sector names to scorer instances
and push live updates to scoring formulas without restarting the bot.

Quick reference
---------------

  from scoring.registry import ScoreRegistry
  from scoring.formulas.sectors import ITSectorScorer

  registry = ScoreRegistry()          # pre-loads all built-in sectors

  # --- Read ---
  scorer = registry.get("IT")         # ITSectorScorer instance

  # --- Replace an entire sector scorer ---
  registry.register("FINTECH", MyFintechScorer())

  # --- Tweak top-level weights (tech / fundamental / momentum) ---
  registry.set_weights("IT", technical=0.55, fundamental=0.25, momentum=0.20)

  # --- Tweak sub-scorer weights ---
  registry.set_technical_weights("IT",  macd=0.30, rsi=0.15)
  registry.set_fundamental_weights("IT", rev_growth=0.25, earn_growth=0.20)

  # --- Inject a fully custom metric into a sector ---
  def npa_quality(df, fund):
      npa = fund.get("npa_ratio", 0.05)
      return max(0, 100 - npa * 1000)   # e.g. 1% NPA → 90 score

  registry.add_metric("BANKING", "npa_quality", npa_quality, weight=0.10)

  # --- Remove a metric ---
  registry.remove_metric("BANKING", "dividend")

  # --- List what's registered ---
  registry.summary()
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from scoring.formulas.base import BaseScorer, MetricFn

log = logging.getLogger("ScoreRegistry")


class ScoreRegistry:
    """
    Maps sector names (uppercase strings) → BaseScorer instances.
    Acts as the single injection point for all formula updates.
    """

    def __init__(self) -> None:
        self._scorers: dict[str, BaseScorer] = {}
        self._load_defaults()

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _load_defaults(self) -> None:
        from scoring.formulas.sectors.default  import DefaultSectorScorer
        from scoring.formulas.sectors.it       import ITSectorScorer
        from scoring.formulas.sectors.banking  import BankingSectorScorer
        from scoring.formulas.sectors.pharma   import PharmaSectorScorer

        self._scorers = {
            "DEFAULT":      DefaultSectorScorer(),
            "IT":           ITSectorScorer(),
            "BANKING":      BankingSectorScorer(),
            "PSU_BANK":     BankingSectorScorer(),    # shares banking logic
            "PRIVATE_BANK": BankingSectorScorer(),
            "FINANCIAL":    BankingSectorScorer(),
            "PHARMA":       PharmaSectorScorer(),
            "AUTO":         DefaultSectorScorer(),
            "FMCG":         DefaultSectorScorer(),
            "METAL":        DefaultSectorScorer(),
            "REALTY":       DefaultSectorScorer(),
            "ENERGY":       DefaultSectorScorer(),
            "INFRA":        DefaultSectorScorer(),
            "MEDIA":        DefaultSectorScorer(),
            "CONSUMER":     DefaultSectorScorer(),
            "UNKNOWN":      DefaultSectorScorer(),
        }
        log.debug("ScoreRegistry loaded with %d sectors.", len(self._scorers))

    # ------------------------------------------------------------------
    # Core CRUD
    # ------------------------------------------------------------------

    def register(self, sector: str, scorer: BaseScorer) -> None:
        """Replace or add a scorer for a sector."""
        self._scorers[sector.upper()] = scorer
        log.info("Scorer registered for sector '%s': %s", sector.upper(), type(scorer).__name__)

    def get(self, sector: str) -> BaseScorer:
        """Return sector scorer, falling back to DEFAULT."""
        return self._scorers.get(sector.upper(), self._scorers["DEFAULT"])

    def all_sectors(self) -> list[str]:
        return sorted(self._scorers.keys())

    # ------------------------------------------------------------------
    # Live weight updates (no restart needed)
    # ------------------------------------------------------------------

    def set_weights(self, sector: str, **kwargs: float) -> None:
        """
        Update top-level weights (technical / fundamental / momentum) for a sector.

        registry.set_weights("IT", technical=0.55, fundamental=0.25, momentum=0.20)
        """
        self.get(sector).set_weights(**kwargs)
        log.info("Weights updated for '%s': %s", sector.upper(), kwargs)

    def set_technical_weights(self, sector: str, **kwargs: float) -> None:
        """
        Update weights inside the TechnicalScorer for a sector.
        Valid keys: rsi, macd, bollinger, sma_cross, volume, momentum

        registry.set_technical_weights("PHARMA", macd=0.30, momentum=0.20)
        """
        scorer = self.get(sector)
        if hasattr(scorer, "technical_scorer"):
            scorer.technical_scorer.set_weights(**kwargs)
            log.info("Technical weights updated for '%s': %s", sector.upper(), kwargs)
        else:
            log.warning("Scorer for '%s' does not expose technical_scorer.", sector)

    def set_fundamental_weights(self, sector: str, **kwargs: float) -> None:
        """
        Update weights inside the FundamentalScorer for a sector.
        Valid keys: pe, pb, roe, debt_equity, curr_ratio, rev_growth,
                    earn_growth, margin, dividend

        registry.set_fundamental_weights("BANKING", roe=0.25, pb=0.25)
        """
        scorer = self.get(sector)
        if hasattr(scorer, "fundamental_scorer"):
            scorer.fundamental_scorer.set_weights(**kwargs)
            log.info("Fundamental weights updated for '%s': %s", sector.upper(), kwargs)
        else:
            log.warning("Scorer for '%s' does not expose fundamental_scorer.", sector)

    # ------------------------------------------------------------------
    # Custom metric injection
    # ------------------------------------------------------------------

    def add_metric(
        self,
        sector: str,
        name: str,
        fn: MetricFn,
        weight: float = 0.10,
    ) -> None:
        """
        Inject a custom metric into a sector scorer.

        fn signature: (ohlcv_df: pd.DataFrame, fundamentals: dict) -> float [0, 100]

        Example:
            def npa_quality(df, fund):
                npa = fund.get("npa_ratio", 0.05)  # you'd need to inject this
                return max(0, 100 - npa * 1000)

            registry.add_metric("BANKING", "npa_quality", npa_quality, weight=0.12)
        """
        self.get(sector).add_metric(name, fn, weight)
        log.info("Custom metric '%s' added to sector '%s' (weight=%.2f).", name, sector.upper(), weight)

    def remove_metric(self, sector: str, name: str) -> None:
        self.get(sector).remove_metric(name)
        log.info("Metric '%s' removed from sector '%s'.", name, sector.upper())

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def summary(self) -> None:
        """Print a human-readable summary of all registered scorers."""
        print(f"\n{'─'*60}")
        print(f"  ScoreRegistry — {len(self._scorers)} sectors")
        print(f"{'─'*60}")
        for sector, scorer in sorted(self._scorers.items()):
            weights = scorer.get_weights()
            extras  = list(scorer._extra_metrics.keys())
            print(
                f"  {sector:<18} → {type(scorer).__name__:<28} "
                f"  weights={weights}"
                + (f"  extras={extras}" if extras else "")
            )
        print(f"{'─'*60}\n")
