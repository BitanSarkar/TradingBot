"""
data/fetcher.py — OHLCV + fundamentals fetcher

Source priority for historical OHLCV (tried in order, first success wins):
  1. nselib  — capital_market.price_volume_and_deliverable_position_data()
               (authoritative NSE feed; broken when NSE changes column names)
  2. yfinance — yf.download("SYMBOL.NS", ...)
               (Yahoo Finance NSE mirror; reliable, works from any IP including EC2)
  3. nsepy   — get_history()
               (broken on Python 3.14 due to FrameLocalsProxy; kept as last resort)

Threading
---------
All three are single-symbol APIs — parallelised across a ThreadPoolExecutor.
Concurrency capped to avoid hammering NSE / Yahoo rate limits.

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
_MAX_WORKERS      = 20    # parallel threads for nselib (keep <= 30)
_YF_CHUNK         = 200   # symbols per yfinance batch call (avoids rate limiting)
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


def _normalise_yfinance(df: pd.DataFrame) -> pd.DataFrame:
    """
    yfinance.download() for a single symbol returns MultiIndex columns:
        ('Close', 'SYMBOL.NS'), ('High', 'SYMBOL.NS'), ...
    Flatten to: Open, High, Low, Close, Volume with DatetimeIndex.
    """
    if isinstance(df.columns, pd.MultiIndex):
        # ('Close', 'RELIANCE.NS') → 'Close'
        df.columns = [c[0] for c in df.columns]
    keep = [c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]
    df   = df[keep].apply(pd.to_numeric, errors="coerce").dropna(how="all")
    df.index = pd.to_datetime(df.index)
    df.index.name = "Date"
    df.sort_index(inplace=True)
    return df


# ---------------------------------------------------------------------------
# DataFetcher
# ---------------------------------------------------------------------------

class DataFetcher:
    # In-memory live quote cache: symbol → (fetched_at_epoch, ohlcv_dict)
    # NOT saved to disk — live candles are ephemeral (not final EOD prices)
    _live_quotes: dict[str, tuple[float, dict]] = {}

    def __init__(self, cache: DataCache, cache_only: bool = False) -> None:
        self._cache        = cache
        self._cache_only   = cache_only   # True on EC2 — bulk refresh skipped, data from rsync
        self._groww_client = None         # injected via attach_groww_client() after auth

    # ------------------------------------------------------------------
    # Main refresh — call once per day before strategy runs
    # ------------------------------------------------------------------

    def refresh(self, symbols: list[str], force: bool = False) -> None:
        """
        Download OHLCV for every stale symbol (parallelised).
        Also refreshes fundamentals on a weekly schedule.

        When cache_only=True (EC2 mode), all data comes via rsync from the Mac
        bootstrap run — skip every network call to avoid 401/block errors.
        """
        if self._cache_only:
            log.debug("DataFetcher: cache_only mode — skipping network refresh (data from rsync).")
            return

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

    def batch_refresh(self, symbols: list[str], force: bool = False) -> tuple[int, int]:
        """
        OHLCV-only refresh used by bootstrap.py for Step 2.
        Returns (ok_count, failed_count).
        """
        stale = symbols if force else self._cache.stale_ohlcv_symbols(symbols)
        if not stale:
            log.info("All OHLCV caches are fresh — nothing to download.")
            return len(symbols), 0
        log.info(
            "batch_refresh: %d symbols to fetch (nselib → nsepy, %d threads)…",
            len(stale), _MAX_WORKERS,
        )
        return self._parallel_fetch_ohlcv(stale)

    def refresh_fundamentals(self, symbols: list[str], force: bool = False) -> int:
        """
        Fundamentals-only refresh used by bootstrap.py for Step 3.
        Returns the number of symbols successfully updated.
        """
        stale = symbols if force else self._cache.stale_fund_symbols(symbols)
        if not stale:
            log.info("All fundamental caches are fresh — nothing to download.")
            return 0
        log.info("refresh_fundamentals: %d symbols to fetch…", len(stale))
        return self._parallel_fetch_fundamentals(stale)

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

    def attach_groww_client(self, client) -> None:
        """Inject Groww API client so live intraday candles use Groww instead of NSE API."""
        self._groww_client = client

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
        Fetch today's intraday OHLCV candle for a symbol.

        Priority:
          1. Groww API  get_ohlc()  — works from EC2, always valid if token is fresh
          2. NSE quote-equity API   — fallback for non-EC2 / Groww auth failure

        Returns None on all failures (scoring falls back to last EOD data).
        """
        # ── Method 1: Groww API (preferred on EC2) ───────────────────────────
        if self._groww_client is not None:
            try:
                key    = f"NSE_{symbol}"
                result = self._groww_client.get_ohlc(
                    exchange_trading_symbols=(key,),
                    segment=self._groww_client.SEGMENT_CASH,
                    timeout=5,
                )
                data = result.get(key, {}) if isinstance(result, dict) else {}
                o = data.get("open")
                h = data.get("high")
                l = data.get("low")
                c = data.get("close") or data.get("ltp")
                v = data.get("volume", 0.0)
                if all(x is not None for x in (o, h, l, c)):
                    return {
                        "open":   float(o),
                        "high":   float(h),
                        "low":    float(l),
                        "close":  float(c),
                        "volume": float(v) if v is not None else 0.0,
                    }
            except Exception:
                pass   # fall through to NSE API

        # ── Method 2: NSE quote-equity API (fallback, may be blocked on EC2) ─
        try:
            session = self._get_nse_session()
            url  = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
            resp = session.get(url, timeout=5)
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, dict):
                return None

            pi    = data.get("priceInfo", {}) or {}
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

    def _parallel_fetch_ohlcv(self, symbols: list[str]) -> tuple[int, int]:
        """
        Three-phase OHLCV download.  Returns (ok_count, failed_count).

        Phase 1 — nselib (20 parallel workers, fast):
          Authoritative NSE feed.  May fail if NSE changes API column names.

        Phase 2 — yfinance BATCH (one grouped API call per 200-symbol chunk):
          Yahoo Finance NSE mirror ("SYMBOL.NS").  Reliable, no rate-limit
          issues because we batch many symbols into a single request instead
          of making one request per symbol in parallel.

        Phase 3 — nsepy (one-by-one, slow):
          Last resort.  Broken on Python 3.14+ but kept for older envs.
        """
        today   = date.today()
        from_dt = today - timedelta(days=_LOOKBACK_DAYS)

        # ── Phase 1: nselib ───────────────────────────────────────────────
        nselib_fail: list[str] = []
        success = fail = done = 0
        total = len(symbols)
        _PROGRESS_EVERY = max(1, min(200, total // 10))

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {pool.submit(self._nselib_ohlcv, s, from_dt, today): s for s in symbols}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    df = fut.result()
                    if df is not None and not df.empty:
                        self._cache.save_ohlcv(sym, df)
                        success += 1
                    else:
                        nselib_fail.append(sym)
                except Exception as exc:
                    log.debug("[nselib] %s: %s", sym, exc)
                    nselib_fail.append(sym)
                done += 1
                if done % _PROGRESS_EVERY == 0 or done == total:
                    log.info(
                        "  nselib [%d/%d]  %.0f%%  ✓ %d  ✗ %d",
                        done, total, done / total * 100, success, len(nselib_fail),
                    )

        # ── Phase 2: yfinance batch ───────────────────────────────────────
        yf_fail: list[str] = nselib_fail
        if nselib_fail:
            log.info(
                "yfinance batch fallback for %d symbols (chunks of %d)…",
                len(nselib_fail), _YF_CHUNK,
            )
            yf_ok, yf_fail = self._yfinance_batch_fetch(nselib_fail, from_dt, today)
            success += yf_ok
            log.info("yfinance batch done: ✓ %d  ✗ %d", yf_ok, len(yf_fail))

        # ── Phase 3: nsepy (one-by-one, last resort) ──────────────────────
        for sym in yf_fail:
            try:
                df = self._nsepy_ohlcv(sym, from_dt, today)
                if df is not None and not df.empty:
                    self._cache.save_ohlcv(sym, df)
                    success += 1
                else:
                    fail += 1
            except Exception as exc:
                log.debug("[nsepy] %s: %s", sym, exc)
                fail += 1

        log.info("OHLCV refresh done: %d ok / %d failed.", success, fail)
        return success, fail

    def _yfinance_batch_fetch(
        self, symbols: list[str], from_dt: date, to_dt: date
    ) -> tuple[int, list[str]]:
        """
        Download OHLCV for a list of symbols using yfinance batch mode.
        Downloads _YF_CHUNK symbols per API call to stay within Yahoo's limits.
        Returns (ok_count, still_failed_symbols).
        """
        import yfinance as yf
        ok      = 0
        failed  = []

        for i in range(0, len(symbols), _YF_CHUNK):
            chunk   = symbols[i : i + _YF_CHUNK]
            tickers = [f"{s}.NS" for s in chunk]
            try:
                raw = yf.download(
                    tickers,
                    start    = from_dt.isoformat(),
                    end      = to_dt.isoformat(),
                    progress = False,
                    auto_adjust = True,
                    threads  = True,   # yfinance manages its own internal threading
                )
            except Exception as exc:
                log.debug("[yfinance] batch chunk %d-%d failed: %s", i, i + _YF_CHUNK, exc)
                failed.extend(chunk)
                continue

            if raw is None or raw.empty:
                failed.extend(chunk)
                continue

            for sym, ticker in zip(chunk, tickers):
                try:
                    if isinstance(raw.columns, pd.MultiIndex):
                        # Multi-ticker download: columns are (metric, ticker)
                        sym_df = raw.xs(ticker, axis=1, level=1).copy()
                    else:
                        # Single-ticker fallback: flat columns
                        sym_df = raw.copy()

                    sym_df = sym_df[[c for c in ("Open","High","Low","Close","Volume")
                                     if c in sym_df.columns]]
                    sym_df = sym_df.apply(pd.to_numeric, errors="coerce").dropna(how="all")
                    sym_df.index = pd.to_datetime(sym_df.index)
                    sym_df.sort_index(inplace=True)

                    if not sym_df.empty:
                        self._cache.save_ohlcv(sym, sym_df)
                        ok += 1
                    else:
                        failed.append(sym)
                except Exception as exc:
                    log.debug("[yfinance] %s extract failed: %s", sym, exc)
                    failed.append(sym)

            log.info(
                "  yfinance [%d/%d]  ✓ %d so far",
                min(i + _YF_CHUNK, len(symbols)), len(symbols), ok,
            )

        return ok, failed

    def _fetch_ohlcv_one(self, symbol: str) -> Optional[pd.DataFrame]:
        """
        Single-symbol fetch (used by get_ohlcv() for on-demand loads).
        Tries nselib → yfinance → nsepy in order.
        NOT used for bulk bootstrap (use _parallel_fetch_ohlcv instead).
        """
        today   = date.today()
        from_dt = today - timedelta(days=_LOOKBACK_DAYS)

        try:
            df = self._nselib_ohlcv(symbol, from_dt, today)
            if df is not None and not df.empty:
                return df
        except Exception as exc:
            log.debug("[nselib] %s: %s", symbol, exc)

        try:
            df = self._yfinance_ohlcv(symbol, from_dt, today)
            if df is not None and not df.empty:
                return df
        except Exception as exc:
            log.debug("[yfinance] %s: %s", symbol, exc)

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

    def _yfinance_ohlcv(self, symbol: str, from_dt: date, to_dt: date) -> Optional[pd.DataFrame]:
        """Yahoo Finance via yfinance — appends '.NS' for NSE symbols."""
        import yfinance as yf
        ticker = f"{symbol}.NS"
        df = yf.download(
            ticker,
            start=from_dt.isoformat(),
            end=to_dt.isoformat(),
            progress=False,
            auto_adjust=True,
        )
        if df is None or df.empty:
            return None
        return _normalise_yfinance(df)

    def _nsepy_ohlcv(self, symbol: str, from_dt: date, to_dt: date) -> Optional[pd.DataFrame]:
        from nsepy import get_history
        df = get_history(symbol=symbol, start=from_dt, end=to_dt)
        if df is None or df.empty:
            return None
        return _normalise_nsepy(df)

    # ------------------------------------------------------------------
    # Fundamentals — NSE equity info API (weekly refresh)
    # ------------------------------------------------------------------

    def _parallel_fetch_fundamentals(self, symbols: list[str]) -> int:
        """Returns ok_count (number of symbols with data saved)."""
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
        return success

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
        Pull P/E, P/B, EPS, market cap, ROE.
        Tries NSE quote API first; falls back to yfinance Ticker.info.
        Returns {} if both sources fail — scoring degrades to technical only.
        """
        # ── Method 1: NSE quote API ───────────────────────────────────────
        try:
            session = self._get_nse_session()
            url  = f"https://www.nseindia.com/api/quote-equity?symbol={symbol}"
            resp = session.get(url, timeout=8)
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, dict):
                metadata = data.get("metadata", {}) or {}
                price    = data.get("priceInfo",  {}) or {}
                fin      = data.get("financialData", {}) or {}

                result = {
                    "pe":         metadata.get("pdSymbolPe"),
                    "pb":         metadata.get("pdPb"),
                    "eps":        metadata.get("eps"),
                    "market_cap": metadata.get("marketCap"),
                    "52w_high":   price.get("weekHighLow", {}).get("max"),
                    "52w_low":    price.get("weekHighLow", {}).get("min"),
                    "roe":        fin.get("returnOnEquity"),
                    "div_yield":  fin.get("dividendYield"),
                }
                if result.get("pe") is not None:
                    return result
        except Exception:
            pass

        # ── Method 2: yfinance Ticker.info ───────────────────────────────
        try:
            import yfinance as yf
            info = yf.Ticker(f"{symbol}.NS").info
            if not info:
                return {}
            pe = info.get("trailingPE") or info.get("forwardPE")
            result = {
                "pe":         pe,
                "pb":         info.get("priceToBook"),
                "eps":        info.get("trailingEps"),
                "market_cap": info.get("marketCap"),
                "52w_high":   info.get("fiftyTwoWeekHigh"),
                "52w_low":    info.get("fiftyTwoWeekLow"),
                "roe":        info.get("returnOnEquity"),
                "div_yield":  info.get("dividendYield"),
            }
            return result if pe is not None else {}
        except Exception:
            return {}
