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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Optional

import pandas as pd
from requests.adapters import HTTPAdapter

from data.cache import DataCache

log = logging.getLogger("DataFetcher")

_LOOKBACK_DAYS    = 365   # 1 year of daily OHLCV for indicator calculation
_MAX_WORKERS      = 20    # parallel threads — keep <= 30 to respect NSE rate limits
_NSELIB_FMT       = "%d-%m-%Y"   # nselib date format: DD-MM-YYYY
_LIVE_QUOTE_TTL   = 90    # seconds — refresh live intraday candle at most every 90s


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
    # In-memory live quote cache: symbol → (fetched_at_epoch, ohlcv_dict)
    # NOT saved to disk — live candles are ephemeral (not final EOD prices)
    _live_quotes: dict[str, tuple[float, dict]] = {}

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
        """
        Returns OHLCV dataframe.

        During market hours (09:15–15:30 IST, Mon–Fri):
          • Appends / updates today's row with a live intraday candle from NSE.
          • The live candle uses today's real Open, High, Low, LTP, Volume.
          • This makes RSI / MACD / Bollinger react to the CURRENT price,
            not last night's close.
          • Live quotes are cached for _LIVE_QUOTE_TTL seconds (default 90s)
            to avoid hammering NSE for every symbol on every tick.

        Outside market hours:
          • Returns the cached EOD data unchanged.
        """
        df = self._cache.load_ohlcv(symbol)
        if df is None or df.empty:
            return df

        from market_hours import is_market_open
        if is_market_open():
            df = self._inject_live_candle(df, symbol)

        return df

    def get_fundamentals(self, symbol: str) -> dict:
        return self._cache.load_fund(symbol) or {}

    def get_ltp(self, symbol: str) -> float:
        """Last traded price — live during market hours, last close otherwise."""
        df = self.get_ohlcv(symbol)
        if df is None or df.empty:
            return 0.0
        return float(df["Close"].iloc[-1])

    # ------------------------------------------------------------------
    # Live intraday candle injection
    # ------------------------------------------------------------------

    def _inject_live_candle(
        self, df: pd.DataFrame, symbol: str
    ) -> pd.DataFrame:
        """
        Fetch today's intraday OHLCV from NSE and upsert it as the last row.

        NSE's quote-equity API returns:
          priceInfo.open                → today's open
          priceInfo.intraDayHighLow.max → today's high
          priceInfo.intraDayHighLow.min → today's low
          priceInfo.lastPrice           → current traded price (LTP)
          priceInfo.totalTradedVolume   → cumulative volume today

        The row is stamped with today's date (IST).
        It is NOT written to the Parquet cache because the candle isn't
        final until market close.
        """
        import time as _time
        from zoneinfo import ZoneInfo
        from datetime import datetime

        cached = DataFetcher._live_quotes.get(symbol)
        now_ts = _time.time()

        if cached and (now_ts - cached[0]) < _LIVE_QUOTE_TTL:
            candle = cached[1]
        else:
            candle = self._fetch_live_quote_raw(symbol)
            if candle:
                DataFetcher._live_quotes[symbol] = (now_ts, candle)

        if not candle:
            return df   # no live data, return unchanged

        IST       = ZoneInfo("Asia/Kolkata")
        today_ist = datetime.now(IST).date()
        today_ts  = pd.Timestamp(today_ist)

        row = pd.DataFrame([{
            "Open":   candle["open"],
            "High":   candle["high"],
            "Low":    candle["low"],
            "Close":  candle["close"],   # LTP
            "Volume": candle["volume"],
        }], index=[today_ts])

        # Drop today's row if it was already in EOD cache (rare but possible)
        df = df[df.index.normalize() != today_ts]
        df = pd.concat([df, row])
        df.sort_index(inplace=True)
        return df

    def _fetch_live_quote_raw(self, symbol: str) -> dict | None:
        """
        Hit NSE quote-equity API and return a normalised candle dict.
        Returns None on any error (scoring falls back to last EOD data).
        """
        try:
            session = self._get_nse_session()
            url  = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
            resp = session.get(url, timeout=5)
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, dict):
                return None

            pi = data.get("priceInfo", {}) or {}
            intra = pi.get("intraDayHighLow", {}) or {}

            o = pi.get("open")
            h = intra.get("max")
            l = intra.get("min")
            c = pi.get("lastPrice")
            v = pi.get("totalTradedVolume")

            if not all(x is not None for x in (o, h, l, c)):
                return None

            return {
                "open":   float(o),
                "high":   float(h),
                "low":    float(l),
                "close":  float(c),
                "volume": float(v) if v is not None else 0.0,
            }
        except Exception:
            return None

    # ------------------------------------------------------------------
    # OHLCV — parallel workers
    # ------------------------------------------------------------------

    def _parallel_fetch_ohlcv(self, symbols: list[str]) -> None:
        success = fail = done = 0
        total = len(symbols)
        _PROGRESS_EVERY = max(1, min(200, total // 10))   # log every ~10% or 200 symbols
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
                done += 1
                if done % _PROGRESS_EVERY == 0 or done == total:
                    pct = done / total * 100
                    log.info(
                        "  OHLCV  [%d/%d]  %.0f%%  ✓ %d  ✗ %d",
                        done, total, pct, success, fail,
                    )
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
        success = done = 0
        total = len(symbols)
        _PROGRESS_EVERY = max(1, min(200, total // 10))
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
                done += 1
                if done % _PROGRESS_EVERY == 0 or done == total:
                    log.info(
                        "  Fund   [%d/%d]  %.0f%%  ✓ %d",
                        done, total, done / total * 100, success,
                    )
        log.info("Fundamentals refresh: %d/%d updated.", success, len(symbols))

    # ------------------------------------------------------------------ #
    # Shared NSE session — one session for the whole process             #
    #   • Pool sized to 2× workers so no connection is ever discarded    #
    #   • Lock prevents two threads from racing to warm-up simultaneously #
    # ------------------------------------------------------------------ #
    _nse_session: object    = None
    _nse_session_ts: float  = 0.0
    _NSE_SESSION_TTL: float = 300.0          # re-establish every 5 minutes
    _NSE_SESSION_LOCK       = threading.Lock()
    _NSE_POOL_SIZE: int     = _MAX_WORKERS * 2   # enough for all concurrent threads

    def _get_nse_session(self):
        """
        Return a requests.Session with:
          • Valid NSE cookies (two-page warm-up: homepage → equities)
          • HTTPAdapter with pool_maxsize = _NSE_POOL_SIZE (no "pool full" warnings)
          • Automatic retry on transient errors (3 retries, backoff)

        Thread-safe: uses a class-level lock so only one thread establishes
        the session; all others wait and then share the same session.
        """
        import time as _time
        import requests as _requests
        from urllib3.util.retry import Retry

        now = _time.time()
        # Fast path — session exists and is fresh (no lock needed for read)
        if (DataFetcher._nse_session is not None and
                now - DataFetcher._nse_session_ts < DataFetcher._NSE_SESSION_TTL):
            return DataFetcher._nse_session

        with DataFetcher._NSE_SESSION_LOCK:
            # Re-check inside the lock (another thread may have just created it)
            now = _time.time()
            if (DataFetcher._nse_session is not None and
                    now - DataFetcher._nse_session_ts < DataFetcher._NSE_SESSION_TTL):
                return DataFetcher._nse_session

            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IN,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection":      "keep-alive",
            }

            # Retry strategy: 3 attempts, exponential backoff (0.5s, 1s, 2s)
            retry = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            # Pool sized to accommodate all concurrent worker threads
            adapter = HTTPAdapter(
                pool_connections = DataFetcher._NSE_POOL_SIZE,
                pool_maxsize     = DataFetcher._NSE_POOL_SIZE,
                max_retries      = retry,
            )

            session = _requests.Session()
            session.headers.update(headers)
            session.mount("https://", adapter)
            session.mount("http://",  adapter)

            try:
                # Step 1 — homepage: sets nsit + nseappid cookies
                session.get("https://www.nseindia.com", timeout=8)
                # Step 2 — equities page: sets ak_bmsc / bm_sv anti-bot cookies
                session.get(
                    "https://www.nseindia.com/market-data/live-equity-market",
                    timeout=8,
                )
                log.info(
                    "NSE session established  (pool=%d, TTL=%ds).",
                    DataFetcher._NSE_POOL_SIZE, int(DataFetcher._NSE_SESSION_TTL),
                )
            except Exception as exc:
                log.warning("NSE session warm-up partial failure: %s", exc)

            DataFetcher._nse_session    = session
            DataFetcher._nse_session_ts = _time.time()
            return session

    def _fetch_fund_one(self, symbol: str) -> dict:
        """
        Pull P/E, P/B, EPS, market cap, ROE from NSE's equity quote API.
        Returns {} if unavailable — scoring degrades gracefully (technical only).
        """
        try:
            session = self._get_nse_session()
            url  = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
            resp = session.get(url, timeout=8)
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, dict):
                return {}

            metadata = data.get("metadata", {}) or {}
            price    = data.get("priceInfo",  {}) or {}
            fin      = data.get("financialData", {}) or {}

            result = {
                "pe":         metadata.get("pdSymbolPe"),
                "eps":        metadata.get("eps"),
                "market_cap": metadata.get("marketCap"),
                "52w_high":   price.get("weekHighLow", {}).get("max"),
                "52w_low":    price.get("weekHighLow", {}).get("min"),
                "roe":        fin.get("returnOnEquity"),
                "div_yield":  fin.get("dividendYield"),
            }
            # Only count as success if at least P/E was populated
            return result if result.get("pe") is not None else {}
        except Exception:
            return {}
