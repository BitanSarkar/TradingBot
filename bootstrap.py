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
import sys
import time
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="TradingBot bootstrap / cache refresh")
    p.add_argument("--universe",     action="store_true", help="Refresh universe + sector map only")
    p.add_argument("--ohlcv",        action="store_true", help="Refresh OHLCV price history only")
    p.add_argument("--fundamentals", action="store_true", help="Refresh fundamental data only")
    p.add_argument("--force",        action="store_true", help="Re-download even if cache is fresh")
    return p.parse_args()


def step(msg: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {msg}")
    print(f"{'─' * 60}")


def main():
    args = parse_args()

    # If no specific flag given, run everything
    run_all = not (args.universe or args.ohlcv or args.fundamentals)

    # ── 0. Pre-flight check ────────────────────────────────────────────────
    step("Pre-flight: checking imports")
    missing = []
    for pkg in ["nselib", "nsepy", "pandas", "numpy", "pyarrow", "requests"]:
        try:
            __import__(pkg)
            print(f"  ✅  {pkg}")
        except ImportError:
            print(f"  ❌  {pkg}  ← NOT installed")
            missing.append(pkg)

    if missing:
        print(f"\n  Run:  pip install {' '.join(missing)}")
        sys.exit(1)

    # Create cache dirs
    for d in ["cache/ohlcv", "cache/fundamentals", "logs"]:
        Path(d).mkdir(parents=True, exist_ok=True)
    print("\n  Cache directories ready.")

    # ── 1. Universe ────────────────────────────────────────────────────────
    if run_all or args.universe:
        step("Step 1 / 3 — Building stock universe & sector map")
        print("  Source : nselib  →  NSE EQ-series equity list")
        print("  Output : cache/universe.json\n")

        from universe import StockUniverse
        u = StockUniverse()
        u.refresh(force=args.force)

        symbols = u.all_symbols()
        sectors = u.all_sectors()
        print(f"\n  ✅  {len(symbols)} symbols loaded across {len(sectors)} sectors")
        for s in sorted(sectors):
            count = len(u.symbols_by_sector(s))
            print(f"       {s:<20}  {count} stocks")

    # ── 2. OHLCV ──────────────────────────────────────────────────────────
    if run_all or args.ohlcv:
        step("Step 2 / 3 — Downloading OHLCV history (1 year)")
        print("  Source : nselib  →  price_volume_and_deliverable_position_data()")
        print("  Fallback: nsepy  →  get_history()")
        print("  Output : cache/ohlcv/<SYMBOL>.parquet\n")
        print("  ⚠️   This takes ~2–5 minutes on first run (2,117 symbols).")
        print("       Subsequent runs only fetch the latest candle per symbol.\n")

        from universe import StockUniverse
        from data.cache import DataCache
        from data.fetcher import DataFetcher

        u       = StockUniverse()
        u.refresh()
        cache   = DataCache()
        fetcher = DataFetcher(cache)

        symbols = u.all_symbols()
        t0      = time.time()

        ok, failed = fetcher.batch_refresh(symbols, force=args.force)

        elapsed = time.time() - t0
        print(f"\n  ✅  OHLCV done: {ok} ok / {failed} failed  ({elapsed:.0f}s)")

    # ── 3. Fundamentals ───────────────────────────────────────────────────
    if run_all or args.fundamentals:
        step("Step 3 / 3 — Downloading fundamental data")
        print("  Source : NSE quote API  (P/E, EPS, P/B, 52W, Market Cap)")
        print("  Output : cache/fundamentals/<SYMBOL>.json")
        print("  Note   : ~254 / 2117 symbols have full fundamental data on NSE.\n")
        print("           Missing fundamentals default to score=50 (neutral).")
        print("           This is expected — small-caps often have no filings.\n")

        from universe import StockUniverse
        from data.cache import DataCache
        from data.fetcher import DataFetcher

        u       = StockUniverse()
        u.refresh()
        cache   = DataCache()
        fetcher = DataFetcher(cache)

        symbols = u.all_symbols()
        t0      = time.time()

        updated = fetcher.refresh_fundamentals(symbols, force=args.force)

        elapsed = time.time() - t0
        print(f"\n  ✅  Fundamentals done: {updated}/{len(symbols)} updated  ({elapsed:.0f}s)")

    # ── Done ───────────────────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    print("  Bootstrap complete.  You can now run:")
    print()
    print("      python bot.py")
    print()
    print("  To force a full refresh at any time:")
    print()
    print("      python bootstrap.py --force")
    print(f"{'═' * 60}\n")


if __name__ == "__main__":
    main()
