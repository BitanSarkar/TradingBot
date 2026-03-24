#!/usr/bin/env python3
"""
select_strategy.py — Market-regime-aware strategy selector.

Reads cached OHLCV data (no network calls), detects the current market
regime, and writes the matching strategy profile's parameters into .env
before bot.py starts.  Credentials in .env are NEVER touched.

Regimes and their default profiles:
  bull    → aggressive    (Nifty proxies trending up, RSI > 55)
  neutral → balanced      (mixed signals)
  bear    → bear-fighter  (majority of proxies below 50-SMA)
  crash   → contrarian    (all proxies deeply broken, RSI < 35)

.env controls:
  STRATEGY_AUTO_SELECT=true            # set false to disable entirely
  STRATEGY_PROFILE_OVERRIDE=balanced   # force a specific profile, skip detection

Usage:
  python select_strategy.py            # auto-detect and apply
  python select_strategy.py --dry-run  # print what would be applied, don't write
  python select_strategy.py --profile bear-fighter  # force profile
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# ── Constants ─────────────────────────────────────────────────────────────────

BOT_DIR = Path(__file__).parent

# Keys that are NEVER overwritten — credentials and runtime flags
PROTECTED_KEYS = {
    "GROWW_API_KEY",
    "GROWW_SECRET",
    "SNS_TOPIC_ARN",
    "BOT_DRY_RUN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "RISK_DRY_RUN_BALANCE",
    "PAPER_LEDGER_PATH",
    "FETCHER_CACHE_ONLY",
    "STRATEGY_AUTO_SELECT",
    "STRATEGY_PROFILE_OVERRIDE",
    "LOG_LEVEL",
    "TZ",
}

# Large-cap NSE proxies used for regime detection
# These are always in the bootstrap universe and have clean OHLCV data
PROXY_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "ICICIBANK", "INFY",
    "HINDUNILVR", "BHARTIARTL", "LT", "AXISBANK", "KOTAKBANK",
    "SBIN", "WIPRO", "BAJFINANCE", "MARUTI", "TATAMOTORS",
]

VALID_PROFILES = ["max-profit", "bear-fighter", "aggressive", "contrarian", "balanced"]

REGIME_TO_PROFILE = {
    "bull":    "aggressive",
    "neutral": "balanced",
    "bear":    "bear-fighter",
    "crash":   "contrarian",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rsi(series: pd.Series, period: int = 14) -> float:
    """Compute RSI for the last value in the series."""
    delta = series.diff().dropna()
    if len(delta) < period:
        return 50.0   # neutral if not enough data
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    last_loss = float(loss.iloc[-1])
    if last_loss == 0:
        return 100.0
    rs = float(gain.iloc[-1]) / last_loss
    return round(100.0 - 100.0 / (1.0 + rs), 1)


def _load_env_file(path: Path) -> dict[str, str]:
    """Parse a .env file into a dict.  Ignores comments and blank lines."""
    result: dict[str, str] = {}
    if not path.exists():
        return result
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        result[key.strip()] = val.strip()
    return result


def _write_env_file(path: Path, data: dict[str, str], profile: str) -> None:
    """Write merged env vars back to file, preserving original key order."""
    # Read original lines so we can do in-place replacement (keeps comments)
    original_lines = path.read_text().splitlines() if path.exists() else []
    original_keys  = [
        line.partition("=")[0].strip()
        for line in original_lines
        if line.strip() and not line.strip().startswith("#") and "=" in line
    ]

    out_lines: list[str] = []
    written: set[str] = set()

    # Pass 1: rewrite existing lines (keeps formatting + comments)
    for line in original_lines:
        stripped = line.strip()
        if stripped.startswith("#") or not stripped or "=" not in stripped:
            out_lines.append(line)
            continue
        key = stripped.partition("=")[0].strip()
        if key in data:
            out_lines.append(f"{key}={data[key]}")
            written.add(key)
        else:
            out_lines.append(line)
            written.add(key)

    # Pass 2: append any new keys from the profile that weren't in original
    new_keys = [k for k in data if k not in written]
    if new_keys:
        out_lines.append("")
        out_lines.append(f"# ── Strategy profile: {profile} ──────────────────────")
        for k in new_keys:
            out_lines.append(f"{k}={data[k]}")

    path.write_text("\n".join(out_lines) + "\n")


# ── Regime detection ──────────────────────────────────────────────────────────

def detect_regime(verbose: bool = True) -> str:
    """
    Detect market regime from cached OHLCV of large-cap proxy symbols.

    Returns one of: 'bull', 'neutral', 'bear', 'crash'
    """
    ohlcv_dir = BOT_DIR / "cache" / "ohlcv"
    if not ohlcv_dir.exists():
        print("  ⚠  cache/ohlcv not found — defaulting to 'neutral'")
        return "neutral"

    results: list[dict] = []

    for sym in PROXY_SYMBOLS:
        f = ohlcv_dir / f"{sym}.parquet"
        if not f.exists():
            continue
        try:
            df = pd.read_parquet(f)
            if len(df) < 50:
                continue

            close    = df["Close"].dropna()
            price    = float(close.iloc[-1])
            sma50    = float(close.rolling(50).mean().iloc[-1])
            sma200   = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
            rsi_val  = _rsi(close)
            # Price vs 50-SMA as % distance
            pct_from_sma50 = (price - sma50) / sma50 * 100

            above_50sma  = price > sma50
            above_200sma = (price > sma200) if sma200 else None

            results.append({
                "symbol":       sym,
                "price":        price,
                "sma50":        sma50,
                "rsi":          rsi_val,
                "above_50sma":  above_50sma,
                "above_200sma": above_200sma,
                "pct_50sma":    pct_from_sma50,
            })

            if verbose:
                trend = "▲" if above_50sma else "▼"
                print(f"    {sym:<15}  price={price:>8.2f}  sma50={sma50:>8.2f} "
                      f"({pct_from_sma50:+.1f}%)  RSI={rsi_val:.0f}  {trend}")
        except Exception as exc:
            if verbose:
                print(f"    {sym:<15}  ✗ {exc}")
            continue

    if not results:
        print("  ⚠  No proxy data found — defaulting to 'neutral'")
        return "neutral"

    n              = len(results)
    above_50       = sum(1 for r in results if r["above_50sma"])
    bull_ratio     = above_50 / n
    avg_rsi        = np.mean([r["rsi"] for r in results])
    avg_pct_sma50  = np.mean([r["pct_50sma"] for r in results])

    if verbose:
        print(f"\n  Summary: {above_50}/{n} proxies above 50-SMA  "
              f"| avg RSI={avg_rsi:.0f}  | avg dist from 50-SMA={avg_pct_sma50:+.1f}%")

    # Regime thresholds
    if bull_ratio >= 0.65 and avg_rsi >= 52:
        regime = "bull"
    elif bull_ratio >= 0.35 and avg_rsi >= 42:
        regime = "neutral"
    elif bull_ratio >= 0.15 or avg_rsi >= 30:
        regime = "bear"
    else:
        regime = "crash"

    return regime


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run",  action="store_true",
                        help="Print what would be applied without writing .env")
    parser.add_argument("--profile",  choices=VALID_PROFILES, default=None,
                        help="Force a specific profile (overrides auto-detection)")
    parser.add_argument("--verbose",  action="store_true", default=True,
                        help="Show per-symbol regime data (default: on)")
    parser.add_argument("--quiet",    action="store_true",
                        help="Suppress per-symbol output")
    args = parser.parse_args()

    verbose = args.verbose and not args.quiet

    env_path = BOT_DIR / ".env"
    if not env_path.exists():
        print(f"ERROR: {env_path} not found.")
        sys.exit(1)

    current_env = _load_env_file(env_path)

    # ── Check if disabled ────────────────────────────────────────────────────
    if current_env.get("STRATEGY_AUTO_SELECT", "true").lower() == "false" and not args.profile:
        print("STRATEGY_AUTO_SELECT=false — strategy selection disabled.  Exiting.")
        return

    # ── Determine profile ────────────────────────────────────────────────────
    chosen_profile: Optional[str] = None

    # 1. CLI flag wins first
    if args.profile:
        chosen_profile = args.profile
        print(f"Profile forced via --profile flag: {chosen_profile}")

    # 2. .env override
    elif current_env.get("STRATEGY_PROFILE_OVERRIDE", "").strip():
        chosen_profile = current_env["STRATEGY_PROFILE_OVERRIDE"].strip()
        if chosen_profile not in VALID_PROFILES:
            print(f"ERROR: STRATEGY_PROFILE_OVERRIDE={chosen_profile!r} is not a valid profile.")
            print(f"  Valid profiles: {', '.join(VALID_PROFILES)}")
            sys.exit(1)
        print(f"Profile forced via STRATEGY_PROFILE_OVERRIDE: {chosen_profile}")

    # 3. Auto-detect from market data
    else:
        print("Detecting market regime from cached OHLCV…")
        print()
        regime = detect_regime(verbose=verbose)
        chosen_profile = REGIME_TO_PROFILE[regime]
        print(f"\n  Regime: {regime.upper()}  →  profile: {chosen_profile}")

    # ── Load profile .env ────────────────────────────────────────────────────
    profile_path = BOT_DIR / "configs" / f".env.{chosen_profile}"
    if not profile_path.exists():
        print(f"ERROR: Profile file not found: {profile_path}")
        sys.exit(1)

    profile_env = _load_env_file(profile_path)

    # ── Merge: profile params into current .env (protecting credentials) ────
    merged = dict(current_env)
    applied: list[str] = []
    skipped: list[str] = []

    for key, val in profile_env.items():
        if key in PROTECTED_KEYS:
            skipped.append(key)
            continue
        merged[key] = val
        applied.append(f"  {key}={val}")

    print(f"\n  Applying {len(applied)} parameter(s) from '{chosen_profile}':")
    for line in applied:
        print(line)

    if skipped and verbose:
        print(f"\n  Protected keys preserved ({len(skipped)}): {', '.join(skipped)}")

    # ── Write (unless dry-run) ───────────────────────────────────────────────
    if args.dry_run:
        print("\n  [dry-run] — .env was NOT modified.")
    else:
        _write_env_file(env_path, merged, chosen_profile)
        print(f"\n✓ .env updated with '{chosen_profile}' profile.  Starting bot…")


if __name__ == "__main__":
    main()
