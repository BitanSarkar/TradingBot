"""
universe.py — Stock Universe Manager

Responsibilities
----------------
1. Fetch the full NSE equity list via nselib (capital_market.equity_list).
2. Map each symbol to its sector using nselib index constituent data.
3. Cache everything locally (once a day) so NSE is not hammered every run.
4. Expose a clean API:  universe.all_symbols(), universe.by_sector("IT"), etc.

Data sources (in priority order)
---------------------------------
  1. nselib  — capital_market.equity_list()  +  capital_market.index_data()
  2. NSE public JSON API  (session-based fallback for index constituents)
  3. Local cache/universe.json  (fast re-runs, never stale within a day)

Usage
-----
    from universe import StockUniverse
    u = StockUniverse()
    u.refresh()                        # download & cache (once a day)
    symbols = u.all_symbols()          # ['RELIANCE', 'TCS', ...]
    it_stocks = u.by_sector("IT")      # ['TCS', 'INFY', 'WIPRO', ...]
    sector = u.sector_of("INFY")       # 'IT'
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

log = logging.getLogger("Universe")

# ---------------------------------------------------------------------------
# Sector → NSE index name  (used by nselib's index_data + NSE API fallback)
# ---------------------------------------------------------------------------
_SECTOR_INDICES: dict[str, str] = {
    "IT":           "NIFTY IT",
    "BANKING":      "NIFTY BANK",
    "PHARMA":       "NIFTY PHARMA",
    "AUTO":         "NIFTY AUTO",
    "FMCG":         "NIFTY FMCG",
    "METAL":        "NIFTY METAL",
    "REALTY":       "NIFTY REALTY",
    "ENERGY":       "NIFTY ENERGY",
    "INFRA":        "NIFTY INFRA",
    "MEDIA":        "NIFTY MEDIA",
    "PSU_BANK":     "NIFTY PSU BANK",
    "PRIVATE_BANK": "NIFTY PRIVATE BANK",
    "FINANCIAL":    "NIFTY FINANCIAL SERVICES",
    "HEALTHCARE":   "NIFTY HEALTHCARE INDEX",
    "CONSUMER":     "NIFTY INDIA CONSUMPTION",
    "MIDCAP":       "NIFTY MIDCAP 100",
    "SMALLCAP":     "NIFTY SMALLCAP 100",
}

_CACHE_DIR      = Path("cache")
_UNIVERSE_CACHE = _CACHE_DIR / "universe.json"
_CACHE_TTL_DAYS = 1


class StockUniverse:
    """
    Manages the full NSE stock universe with sector mapping.

    Internal store
    --------------
    _stocks : dict[SYMBOL -> {"name": str, "sector": str, "isin": str}]
    """

    def __init__(self, cache_dir: Path = _CACHE_DIR, ttl_days: int = _CACHE_TTL_DAYS) -> None:
        self._cache_file = Path(cache_dir) / "universe.json"
        self._ttl_days   = ttl_days
        self._stocks: dict[str, dict] = {}
        _CACHE_DIR.mkdir(exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def refresh(self, force: bool = False) -> None:
        """Load from cache if fresh, else re-download from NSE via nselib."""
        if not force and self._is_cache_fresh():
            self._load_cache()
            log.info("Universe loaded from cache: %d stocks.", len(self._stocks))
            return

        log.info("Refreshing stock universe via nselib…")
        self._fetch_equity_list()
        self._fetch_sector_mappings()
        self._save_cache()
        log.info(
            "Universe ready: %d stocks across %d sectors.",
            len(self._stocks), len(self.all_sectors()),
        )

    def all_symbols(self) -> list[str]:
        return sorted(self._stocks.keys())

    def all_sectors(self) -> list[str]:
        return sorted({v["sector"] for v in self._stocks.values()})

    def by_sector(self, sector: str) -> list[str]:
        sector = sector.upper()
        return [s for s, v in self._stocks.items() if v["sector"] == sector]

    def sector_of(self, symbol: str) -> str:
        return self._stocks.get(symbol.upper(), {}).get("sector", "DEFAULT")

    def meta(self, symbol: str) -> dict:
        return self._stocks.get(symbol.upper(), {})

    def size(self) -> int:
        return len(self._stocks)

    def sector_breakdown(self) -> dict[str, int]:
        counts = Counter(v["sector"] for v in self._stocks.values())
        return dict(counts.most_common())

    def set_sector(self, symbol: str, sector: str) -> None:
        """Manually override a symbol's sector and persist the change."""
        sym = symbol.upper()
        if sym in self._stocks:
            self._stocks[sym]["sector"] = sector.upper()
            self._save_cache()

    # ------------------------------------------------------------------
    # Equity list — primary source: nselib
    # ------------------------------------------------------------------

    def _fetch_equity_list(self) -> None:
        try:
            from nselib import capital_market
            df = self._clean_equity_df(capital_market.equity_list())

            sym_col    = "SYMBOL"
            name_col   = "NAME OF COMPANY"
            isin_col   = "ISIN NUMBER"
            # nselib sometimes returns " SERIES" (leading space) — stripped above
            series_col = "SERIES" if "SERIES" in df.columns else None

            for _, row in df.iterrows():
                symbol = str(row.get(sym_col, "")).strip().upper()
                series = str(row.get(series_col, "EQ")).strip().upper() if series_col else "EQ"
                if not symbol or series != "EQ":
                    continue
                self._stocks[symbol] = {
                    "name":   str(row.get(name_col, "")).strip(),
                    "isin":   str(row.get(isin_col, "")).strip(),
                    "sector": "DEFAULT",
                }
            log.info("nselib: fetched %d EQ symbols.", len(self._stocks))

        except Exception as exc:
            log.error("nselib equity_list failed: %s. Falling back to cache.", exc)
            if self._cache_file.exists():
                self._load_cache()

    def _clean_equity_df(self, df):
        """nselib equity_list has leading spaces in some column names — strip them."""
        df.columns = [c.strip() for c in df.columns]
        return df

    # ------------------------------------------------------------------
    # Sector mapping — nselib index_data → NSE API fallback
    # ------------------------------------------------------------------

    def _fetch_sector_mappings(self) -> None:
        tagged = 0
        for sector, index_name in _SECTOR_INDICES.items():
            constituents = self._get_index_constituents(index_name)
            for sym in constituents:
                if sym in self._stocks:
                    self._stocks[sym]["sector"] = sector
                    tagged += 1
            log.info("  %-14s → %d stocks  (%s)", sector, len(constituents), index_name)
        log.info("Total sector-tagged: %d symbols.", tagged)

    def _get_index_constituents(self, index_name: str) -> list[str]:
        """Try nselib first, then NSE public API, then return []."""

        # Method 1: nselib
        try:
            result = self._via_nselib(index_name)
            if result:
                return result
        except Exception as exc:
            log.debug("nselib index fetch failed (%s): %s", index_name, exc)

        # Method 2: NSE public JSON API
        try:
            result = self._via_nse_api(index_name)
            if result:
                return result
        except Exception as exc:
            log.debug("NSE API fetch failed (%s): %s", index_name, exc)

        return []

    def _via_nselib(self, index_name: str) -> list[str]:
        from nselib import capital_market
        today  = date.today()
        from_d = (today - timedelta(days=7)).strftime("%d-%m-%Y")
        to_d   = today.strftime("%d-%m-%Y")

        df = capital_market.index_data(index_name, from_date=from_d, to_date=to_d)
        if df is None or df.empty:
            return []

        for col in ("symbol", "Symbol", "SYMBOL"):
            if col in df.columns:
                return df[col].dropna().str.strip().str.upper().unique().tolist()
        return []

    def _via_nse_api(self, index_name: str) -> list[str]:
        import urllib.parse
        import requests
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://www.nseindia.com/",
        }
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=10)
        encoded = urllib.parse.quote(index_name)
        url  = f"https://www.nseindia.com/api/equity-stockIndices?index={encoded}"
        resp = session.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return [
            item["symbol"].upper()
            for item in data.get("data", [])
            if item.get("symbol") and item["symbol"].upper() != index_name.upper()
        ]

    # ------------------------------------------------------------------
    # Cache helpers
    # ------------------------------------------------------------------

    def _is_cache_fresh(self) -> bool:
        if not self._cache_file.exists():
            return False
        mtime = date.fromtimestamp(self._cache_file.stat().st_mtime)
        return (date.today() - mtime) < timedelta(days=self._ttl_days)

    def _save_cache(self) -> None:
        payload = {"date": date.today().isoformat(), "stocks": self._stocks}
        self._cache_file.write_text(json.dumps(payload, indent=2))
        log.debug("Universe cached → %s", self._cache_file)

    def _load_cache(self) -> None:
        payload      = json.loads(self._cache_file.read_text())
        self._stocks = payload["stocks"]
