"""
data/fetcher.py — OHLCV + fundamentals fetcher

Primary source  : nselib  (capital_market.price_volume_and_deliverable_position_data)
Fallback source : nsepy   (get_history)

Why two sources?
  nselib is the authoritative NSE data feed.
  nsepy is used when nselib times out / returns empty.

Threading
---------
Both libraries are single-symbol APIs, so we parallelise across a
ThreadPoolExecutor.  Concurrency is capped to avoid hammering NSE.

Public API
----------
    fetcher = DataFetcher(cache)
    fetcher.refresh(symbols)              # download stale OHLCV for all symbols
    df   = fetcher.get_ohlcv("TCS")       # pd.DataFrame with Open/High/Low/Close/Volume
    fund = fetcher.get_fundamentals("TCS")  # dict {pe, pb, roe, ...}
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Optional

import pandas as pd

from data.cache import DataCache

log = logging.getLogger("DataFetcher")

_LOOKBACK_DAYS = 365   # 1 year of daily OHLCV for indicator calculation
_MAX_WORKERS   = 20    # parallel threads — keep <= 30 to respect NSE rate limits
_NSELIB_FMT    = "%d-%m-%Y"   # nselib date format: DD-MM-YYYY


# ---------------------------------------------------------------------------
# Column normalisation helpers
# ---------------------------------------------------------------------------

def _normalise_nselib(df: pd.DataFrame) -> pd.DataFrame:
    """
    nselib actual columns (tested live):
      'ï»¿"Symbol"', 'Series', 'Date', 'PrevClose',
      'OpenPrice', 'HighPrice', 'LowPrice', 'LastPrice', 'ClosePrice',
      'AveragePrice', 'TotalTradedQuantity', 'TurnoverInRs', ...
    Normalise to: Date(index), Open, High, Low, Close, Volume
    """
    # Strip UTF-8 BOM and stray quotes that nselib sometimes emits
    df.columns = [c.encode("ascii", "ignore").decode().strip().strip('"') for c in df.columns]

    # Prefer ClosePrice; only use LastPrice when ClosePrice is absent
    if "ClosePrice" not in df.columns and "LastPrice" in df.columns:
        df = df.rename(columns={"LastPrice": "ClosePrice"})
    if "Closing Price" not in df.columns and "Last Price" in df.columns:
        df = df.rename(columns={"Last Price": "Closing Price"})

    rename = {
        # Current nselib column names
        "OpenPrice":             "Open",
        "HighPrice":             "High",
        "LowPrice":              "Low",
        "ClosePrice":            "Close",
        "TotalTradedQuantity":   "Volume",
        # Legacy spellings (older nselib versions)
        "Open Price":            "Open",
        "High Price":            "High",
        "Low Price":             "Low",
        "Closing Price":         "Close",
        "Total Traded Quantity": "Volume",
    }
    df = df.rename(columns=rename)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce")
        df = df.set_index("Date")

    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]
    df   = df[keep]
    # nselib returns numbers as strings with commas: "2,417.00" → 2417.0
    df   = df.apply(lambda col: pd.to_numeric(col.astype(str).str.replace(",", ""), errors="coerce"))
    df.dropna(how="all", inplace=True)
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    return df


def _normalise_nsepy(df: pd.DataFrame) -> pd.DataFrame:
    """
    nsepy returns: Symbol, Series, Prev Close, Open, High, Low,
                   Last, Close, VWAP, Volume, Turnover, ...
    Index is already a DatetimeIndex.
    """
    rename = {"Last": "Close"} if "Close" not in df.columns else {}
    df = df.rename(columns=rename)
    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]
    df   = df[keep].apply(pd.to_numeric, errors="coerce").dropna(how="all")
    df.sort_index(inplace=True)
    return df


# ---------------------------------------------------------------------------
# DataFetcher
# ---------------------------------------------------------------------------

class DataFetcher:
    def __init__(self, cache: DataCache) -> None:
        self._cache = cache

    # ------------------------------------------------------------------
    # Main refresh — call once per day before strategy runs
    # ------------------------------------------------------------------

    def refresh(self, symbols: list[str], force: bool = False) -> None:
        """
        Download OHLCV for every stale symbol (parallelised).
        Also refreshes fundamentals on a weekly schedule.
        """
        stale_ohlcv = symbols if force else self._cache.stale_ohlcv_symbols(symbols)
        stale_fund  = symbols if force else self._cache.stale_fund_symbols(symbols)

        if stale_ohlcv:
            log.info(
                "Refreshing OHLCV for %d symbols (nselib → nsepy, %d threads)…",
                len(stale_ohlcv), _MAX_WORKERS,
            )
            self._parallel_fetch_ohlcv(stale_ohlcv)

        if stale_fund:
            log.info("Refreshing fundamentals for %d symbols…", len(stale_fund))
            self._parallel_fetch_fundamentals(stale_fund)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_ohlcv(self, symbol: str) -> Optional[pd.DataFrame]:
        return self._cache.load_ohlcv(symbol)

    def get_fundamentals(self, symbol: str) -> dict:
        return self._cache.load_fund(symbol) or {}

    def get_ltp(self, symbol: str) -> float:
        """Last traded price = most recent Close in cached OHLCV."""
        df = self.get_ohlcv(symbol)
        if df is None or df.empty:
            return 0.0
        return float(df["Close"].iloc[-1])

    # ------------------------------------------------------------------
    # OHLCV — parallel workers
    # ------------------------------------------------------------------

    def _parallel_fetch_ohlcv(self, symbols: list[str]) -> None:
        success = fail = 0
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {pool.submit(self._fetch_ohlcv_one, s): s for s in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    df = fut.result()
                    if df is not None and not df.empty:
                        self._cache.save_ohlcv(sym, df)
                        success += 1
                    else:
                        fail += 1
                except Exception as exc:
                    log.debug("OHLCV failed for %s: %s", sym, exc)
                    fail += 1
        log.info("OHLCV refresh done: %d ok / %d failed.", success, fail)

    def _fetch_ohlcv_one(self, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch OHLCV for a single symbol: nselib first, nsepy as fallback."""
        today   = date.today()
        from_dt = today - timedelta(days=_LOOKBACK_DAYS)

        # Method 1: nselib
        try:
            df = self._nselib_ohlcv(symbol, from_dt, today)
            if df is not None and not df.empty:
                return df
        except Exception as exc:
            log.debug("[nselib] %s: %s", symbol, exc)

        # Method 2: nsepy fallback
        try:
            df = self._nsepy_ohlcv(symbol, from_dt, today)
            if df is not None and not df.empty:
                return df
        except Exception as exc:
            log.debug("[nsepy] %s: %s", symbol, exc)

        return None

    def _nselib_ohlcv(self, symbol: str, from_dt: date, to_dt: date) -> Optional[pd.DataFrame]:
        from nselib import capital_market
        df = capital_market.price_volume_and_deliverable_position_data(
            symbol    = symbol,
            from_date = from_dt.strftime(_NSELIB_FMT),
            to_date   = to_dt.strftime(_NSELIB_FMT),
        )
        if df is None or df.empty:
            return None
        return _normalise_nselib(df)

    def _nsepy_ohlcv(self, symbol: str, from_dt: date, to_dt: date) -> Optional[pd.DataFrame]:
        from nsepy import get_history
        df = get_history(symbol=symbol, start=from_dt, end=to_dt)
        if df is None or df.empty:
            return None
        return _normalise_nsepy(df)

    # ------------------------------------------------------------------
    # Fundamentals — NSE equity info API (weekly refresh)
    # ------------------------------------------------------------------

    def _parallel_fetch_fundamentals(self, symbols: list[str]) -> None:
        success = 0
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {pool.submit(self._fetch_fund_one, s): s for s in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    fund = fut.result()
                    if fund:
                        self._cache.save_fund(sym, fund)
                        success += 1
                except Exception as exc:
                    log.debug("Fund failed for %s: %s", sym, exc)
        log.info("Fundamentals refresh: %d/%d updated.", success, len(symbols))

    def _fetch_fund_one(self, symbol: str) -> dict:
        """
        Pull P/E, P/B, EPS, market cap, ROE from NSE's equity quote API.
        Returns {} if unavailable — scoring degrades gracefully (technical only).
        """
        try:
            import requests
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer":    "https://www.nseindia.com/",
            }
            session = requests.Session()
            session.get("https://www.nseindia.com", headers=headers, timeout=8)
            url  = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
            resp = session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            metadata = data.get("metadata", {})
            price    = data.get("priceInfo", {})
            fin      = data.get("financialData", {})

            return {
                "pe":         metadata.get("pdSymbolPe"),
                "eps":        metadata.get("eps"),
                "market_cap": metadata.get("marketCap"),
                "52w_high":   price.get("weekHighLow", {}).get("max"),
                "52w_low":    price.get("weekHighLow", {}).get("min"),
                "roe":        fin.get("returnOnEquity"),
                "div_yield":  fin.get("dividendYield"),
            }
        except Exception:
            return {}
