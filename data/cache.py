"""
data/cache.py — Parquet-based daily OHLCV cache.

Each symbol gets its own file: cache/ohlcv/<SYMBOL>.parquet
Fundamentals are stored in: cache/fundamentals/<SYMBOL>.json  (weekly TTL)

Why parquet?  Fast columnar reads, tiny disk footprint, works natively with pandas.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

log = logging.getLogger("DataCache")

_CACHE_DIR      = Path("cache")
_OHLCV_DIR      = _CACHE_DIR / "ohlcv"
_FUND_DIR       = _CACHE_DIR / "fundamentals"
_OHLCV_TTL_DAYS = 1     # refresh OHLCV daily
_FUND_TTL_DAYS  = 7     # refresh fundamentals weekly


class DataCache:
    def __init__(
        self,
        ohlcv_ttl: int = _OHLCV_TTL_DAYS,
        fund_ttl:  int = _FUND_TTL_DAYS,
    ) -> None:
        self.ohlcv_ttl = ohlcv_ttl
        self.fund_ttl  = fund_ttl
        _OHLCV_DIR.mkdir(parents=True, exist_ok=True)
        _FUND_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # OHLCV
    # ------------------------------------------------------------------

    def ohlcv_path(self, symbol: str) -> Path:
        return _OHLCV_DIR / f"{symbol.upper()}.parquet"

    def ohlcv_fresh(self, symbol: str) -> bool:
        p = self.ohlcv_path(symbol)
        if not p.exists():
            return False
        mtime = date.fromtimestamp(p.stat().st_mtime)
        return (date.today() - mtime) < timedelta(days=self.ohlcv_ttl)

    def load_ohlcv(self, symbol: str) -> pd.DataFrame | None:
        p = self.ohlcv_path(symbol)
        if not p.exists():
            return None
        return pd.read_parquet(p)

    def save_ohlcv(self, symbol: str, df: pd.DataFrame) -> None:
        if df is None or df.empty:
            return
        df.to_parquet(self.ohlcv_path(symbol))

    # ------------------------------------------------------------------
    # Fundamentals
    # ------------------------------------------------------------------

    def fund_path(self, symbol: str) -> Path:
        return _FUND_DIR / f"{symbol.upper()}.json"

    def fund_fresh(self, symbol: str) -> bool:
        p = self.fund_path(symbol)
        if not p.exists():
            return False
        mtime = date.fromtimestamp(p.stat().st_mtime)
        return (date.today() - mtime) < timedelta(days=self.fund_ttl)

    def load_fund(self, symbol: str) -> dict | None:
        p = self.fund_path(symbol)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text())
        except Exception:
            return None

    def save_fund(self, symbol: str, data: dict) -> None:
        self.fund_path(symbol).write_text(json.dumps(data, default=str))

    # ------------------------------------------------------------------
    # Bulk helpers
    # ------------------------------------------------------------------

    def stale_ohlcv_symbols(self, symbols: list[str]) -> list[str]:
        return [s for s in symbols if not self.ohlcv_fresh(s)]

    def stale_fund_symbols(self, symbols: list[str]) -> list[str]:
        return [s for s in symbols if not self.fund_fresh(s)]
