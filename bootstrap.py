"""
bootstrap.py — First-time setup script.

Run this ONCE after cloning the repo (or whenever you want to force a full
data refresh).  After this, `python bot.py` starts in seconds.

Usage
-----
    # Full setup (universe + OHLCV + fundamentals)
    python bootstrap.py

    # Only refresh the stock universe / sector map
    python bootstrap.py --universe

    # Only refresh OHLCV price history
    python bootstrap.py --ohlcv

    # Only refresh fundamental data
    python bootstrap.py --fundamentals

    # Force re-download even if cache exists
    python bootstrap.py --force
"""

import argparse
import datetime as _dt
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

_IST = _dt.timezone(_dt.timedelta(hours=5, minutes=30))


class _ISTFormatter(logging.Formatter):
    """logging.Formatter that always renders %(asctime)s in IST (UTC+5:30)."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        ct = _dt.datetime.fromtimestamp(record.created, tz=_IST)
        if datefmt:
            return ct.strftime(datefmt)
        return ct.strftime("%Y-%m-%d %H:%M:%S")


# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    """
    Set up logging to both console (stdout) and a rotating log file.
    Every bootstrap run appends to logs/bootstrap.log with timestamps.
    """
    Path("logs").mkdir(parents=True, exist_ok=True)

    log_format  = "%(asctime)s  %(levelname)-8s  %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S IST"

    # Root logger
    logger = logging.getLogger("bootstrap")
    logger.setLevel(logging.DEBUG)

    # Console handler — same format so the terminal also shows timestamps
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)
    console.setFormatter(_ISTFormatter(log_format, datefmt=date_format))

    # File handler — appends across runs; easy to grep by date
    file_handler = logging.FileHandler("logs/bootstrap.log", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(_ISTFormatter(log_format, datefmt=date_format))

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger


# ── Helpers ───────────────────────────────────────────────────────────────────

def step(log: logging.Logger, msg: str) -> None:
    log.info("─" * 56)
    log.info("  %s", msg)
    log.info("─" * 56)


def parse_args():
    p = argparse.ArgumentParser(description="TradingBot bootstrap / cache refresh")
    p.add_argument("--universe",     action="store_true", help="Refresh universe + sector map only")
    p.add_argument("--ohlcv",        action="store_true", help="Refresh OHLCV price history only")
    p.add_argument("--fundamentals", action="store_true", help="Refresh fundamental data only")
    p.add_argument("--force",        action="store_true", help="Re-download even if cache is fresh")
    return p.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log  = setup_logging()
    args = parse_args()

    run_start = time.time()
    log.info("═" * 56)
    log.info("  TradingBot Bootstrap started at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("  Flags: universe=%s  ohlcv=%s  fundamentals=%s  force=%s",
             args.universe, args.ohlcv, args.fundamentals, args.force)
    log.info("═" * 56)

    # If no specific flag given, run everything
    run_all = not (args.universe or args.ohlcv or args.fundamentals)

    # ── 0. Pre-flight check ────────────────────────────────────────────────
    step(log, "Pre-flight: checking imports")

    required = ["nselib", "pandas", "numpy", "requests"]
    if run_all or args.ohlcv or args.fundamentals:
        required += ["pyarrow"]
    if run_all or args.ohlcv:
        required += ["nsepy"]

    missing = []
    for pkg in required:
        try:
            __import__(pkg)
            log.info("  ✅  %s", pkg)
        except ImportError:
            log.error("  ❌  %s  ← NOT installed", pkg)
            missing.append(pkg)

    if missing:
        log.error("Missing packages: %s", ", ".join(missing))
        log.error("Run:  pip install %s", " ".join(missing))
        sys.exit(1)

    for d in ["cache/ohlcv", "cache/fundamentals", "logs"]:
        Path(d).mkdir(parents=True, exist_ok=True)
    log.info("Cache directories ready.")

    # ── 1. Universe ────────────────────────────────────────────────────────
    if run_all or args.universe:
        step(log, "Step 1 / 3 — Building stock universe & sector map")
        log.info("Source : nselib  →  NSE EQ-series equity list")
        log.info("Output : cache/universe.json")

        from universe import StockUniverse
        u = StockUniverse()
        t0 = time.time()
        try:
            u.refresh(force=args.force)
        except RuntimeError as exc:
            log.error("Universe refresh failed: %s", exc)
            sys.exit(1)

        symbols = u.all_symbols()
        sectors = u.all_sectors()
        elapsed = time.time() - t0
        log.info("✅  Universe done: %d symbols across %d sectors  (%.0fs)",
                 len(symbols), len(sectors), elapsed)
        for s in sorted(sectors):
            count = len(u.by_sector(s))
            log.info("     %-22s  %d stocks", s, count)

    # ── 2. OHLCV ──────────────────────────────────────────────────────────
    if run_all or args.ohlcv:
        step(log, "Step 2 / 3 — Downloading OHLCV history (1 year)")
        log.info("Source   : nselib  →  price_volume_and_deliverable_position_data()")
        log.info("Fallback : nsepy   →  get_history()")
        log.info("Output   : cache/ohlcv/<SYMBOL>.parquet")
        log.info("⚠️  First run takes ~2–5 min (2,117 symbols). Subsequent runs are fast.")

        from universe import StockUniverse
        from data.cache import DataCache
        from data.fetcher import DataFetcher

        u       = StockUniverse()
        u.refresh()
        cache   = DataCache()
        fetcher = DataFetcher(cache)

        symbols = u.all_symbols()
        t0      = time.time()
        log.info("Starting OHLCV batch refresh for %d symbols...", len(symbols))

        ok, failed = fetcher.batch_refresh(symbols, force=args.force)

        elapsed = time.time() - t0
        log.info("✅  OHLCV done: %d ok / %d failed  (%.0fs)", ok, failed, elapsed)
        if failed > 0:
            log.warning("%d symbols failed to fetch — they will use score=50 (neutral) this session.", failed)

    # ── 3. Fundamentals ───────────────────────────────────────────────────
    if run_all or args.fundamentals:
        step(log, "Step 3 / 3 — Downloading fundamental data")
        log.info("Source : NSE quote API  (P/E, EPS, P/B, 52W, Market Cap)")
        log.info("Output : cache/fundamentals/<SYMBOL>.json")
        log.info("Note   : ~254 / 2117 symbols have full data — small-caps often have no filings.")

        from universe import StockUniverse
        from data.cache import DataCache
        from data.fetcher import DataFetcher

        u       = StockUniverse()
        u.refresh()
        cache   = DataCache()
        fetcher = DataFetcher(cache)

        symbols = u.all_symbols()
        t0      = time.time()
        log.info("Starting fundamentals refresh for %d symbols...", len(symbols))

        updated = fetcher.refresh_fundamentals(symbols, force=args.force)

        elapsed = time.time() - t0
        log.info("✅  Fundamentals done: %d/%d updated  (%.0fs)", updated, len(symbols), elapsed)

    # ── Done ───────────────────────────────────────────────────────────────
    total = time.time() - run_start
    log.info("═" * 56)
    log.info("  Bootstrap complete in %.0fs  (%s)",
             total, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("  Run:  python bot.py")
    log.info("═" * 56)


if __name__ == "__main__":
    main()
