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
import time
import urllib.parse
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

import requests as _requests

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
_CACHE_TTL_DAYS = 7   # universe changes rarely; 7-day TTL avoids hammering NSE


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

        if not self._stocks:
            # nselib failed (likely EC2/cloud IP blocked by NSE) and no cache exists.
            if self._cache_file.exists():
                log.warning(
                    "nselib fetch failed and cache is stale — loading stale cache as fallback."
                )
                self._load_cache()
            else:
                raise RuntimeError(
                    "\n"
                    "  ✗  Could not fetch NSE equity list — 0 symbols returned.\n"
                    "\n"
                    "  Most likely cause: NSE blocks requests from AWS / cloud IPs.\n"
                    "\n"
                    "  Fix — copy the cache from your local Mac to EC2 (one-time):\n"
                    "\n"
                    "    rsync -avz --progress cache/universe.json \\\n"
                    "      ec2-user@<YOUR_EC2_IP>:~/TradingBot/cache/universe.json\n"
                    "\n"
                    "  After that, EC2 will reuse the cache automatically (7-day TTL)."
                )

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
        """
        Tag each stock with its sector using a two-pass priority scheme:

        Pass 1 — Specific sector indices (IT, BANKING, PHARMA, …)
                  Only tags a symbol if it is currently DEFAULT.
                  First-write-wins so overlapping index membership doesn't
                  cause a later specific sector to clobber an earlier one.

        Pass 2 — Cap-based indices (MIDCAP, SMALLCAP)
                  Only tags symbols that are still DEFAULT after Pass 1.
                  This stops MIDCAP/SMALLCAP from wiping out sector tags.

        A single warmed-up NSE session is shared across all requests to avoid
        NSE rate-limiting (which triggers when a new session is created per call).
        """
        # Split indices into specific-sector vs cap-based
        _CAP_SECTORS = {"MIDCAP", "SMALLCAP"}
        specific = {s: n for s, n in _SECTOR_INDICES.items() if s not in _CAP_SECTORS}
        cap      = {s: n for s, n in _SECTOR_INDICES.items() if s in _CAP_SECTORS}

        nse_session = self._warm_nse_session()
        tagged = 0

        # ── Pass 1: specific sectors ──────────────────────────────────────
        for sector, index_name in specific.items():
            constituents = self._get_index_constituents(index_name, nse_session)
            n = 0
            for sym in constituents:
                if sym in self._stocks and self._stocks[sym]["sector"] == "DEFAULT":
                    self._stocks[sym]["sector"] = sector
                    tagged += 1
                    n += 1
            log.info("  [pass1] %-14s → %d new tags  (%s)", sector, n, index_name)
            time.sleep(0.4)

        # ── Pass 2: cap-based (only for stocks still DEFAULT) ─────────────
        for sector, index_name in cap.items():
            constituents = self._get_index_constituents(index_name, nse_session)
            n = 0
            for sym in constituents:
                if sym in self._stocks and self._stocks[sym]["sector"] == "DEFAULT":
                    self._stocks[sym]["sector"] = sector
                    tagged += 1
                    n += 1
            log.info("  [pass2] %-14s → %d new tags  (%s)", sector, n, index_name)
            time.sleep(0.4)

        log.info("Total sector-tagged: %d symbols.", tagged)

    def _warm_nse_session(self) -> "_requests.Session":
        """Open a single requests.Session and warm up the NSE cookie."""
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://www.nseindia.com/",
        }
        session = _requests.Session()
        session.headers.update(headers)
        try:
            session.get("https://www.nseindia.com", timeout=10)
            log.debug("NSE session warmed up (cookies: %s).", list(session.cookies.keys()))
        except Exception as exc:
            log.warning("NSE session warm-up failed: %s", exc)
        return session

    def _get_index_constituents(
        self, index_name: str, nse_session: "_requests.Session | None" = None
    ) -> list[str]:
        """Try nselib first, then NSE public API, then return []."""

        # Method 1: nselib
        try:
            result = self._via_nselib(index_name)
            if result:
                return result
        except Exception as exc:
            log.debug("nselib index fetch failed (%s): %s", index_name, exc)

        # Method 2: NSE public JSON API (reuse caller's session if provided)
        try:
            result = self._via_nse_api(index_name, session=nse_session)
            if result:
                return result
        except Exception as exc:
            log.debug("NSE API fetch failed (%s): %s", index_name, exc)

        return []

    def _via_nselib(self, index_name: str) -> list[str]:
        """
        nselib has no direct 'get index constituents' function.
        capital_market.index_data() returns PRICE HISTORY of the index,
        not the constituent stocks — so it never has a SYMBOL column.

        Instead use nselib's dedicated constituent lists for known indices:
          nifty50_equity_list(), niftynext50_equity_list(),
          niftymidcap150_equity_list(), niftysmallcap250_equity_list()

        For sector indices (IT, FMCG, AUTO, …) nselib has no equivalent,
        so this returns [] and _via_nse_api() is the real workhorse.
        """
        from nselib import capital_market
        _NSELIB_LISTS = {
            "NIFTY 50":              capital_market.nifty50_equity_list,
            "NIFTY NEXT 50":         capital_market.niftynext50_equity_list,
            "NIFTY MIDCAP 150":      capital_market.niftymidcap150_equity_list,
            "NIFTY SMALLCAP 250":    capital_market.niftysmallcap250_equity_list,
            # aliases
            "NIFTY MIDCAP 100":      capital_market.niftymidcap150_equity_list,
            "NIFTY SMALLCAP 100":    capital_market.niftysmallcap250_equity_list,
        }
        fn = _NSELIB_LISTS.get(index_name)
        if fn is None:
            return []   # sector indices → fall through to _via_nse_api
        try:
            df = fn()
            if df is None or df.empty:
                return []
            for col in ("SYMBOL", "Symbol", "symbol", "TckrSymb"):
                if col in df.columns:
                    return df[col].dropna().str.strip().str.upper().unique().tolist()
        except Exception as exc:
            log.debug("nselib list fetch failed (%s): %s", index_name, exc)
        return []

    def _via_nse_api(
        self,
        index_name: str,
        session: "_requests.Session | None" = None,
    ) -> list[str]:
        """
        Fetch index constituents from NSE's public JSON API.

        If `session` is provided (pre-warmed by _fetch_sector_mappings) it is
        reused so that NSE's rate-limiter sees one continuous browser session
        rather than a brand-new connection for every index.
        """
        own_session = session is None
        if own_session:
            # Called stand-alone (e.g. tests) — warm up a fresh session
            session = self._warm_nse_session()

        encoded = urllib.parse.quote(index_name)
        url = f"https://www.nseindia.com/api/equity-stockIndices?index={encoded}"
        resp = session.get(url, timeout=15)
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
