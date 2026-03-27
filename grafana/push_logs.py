#!/usr/bin/env python3
"""
push_logs.py — Push server log files to local Loki

Usage:
    python3 grafana/push_logs.py

Reads all *.log* files from logs/server/, parses each line,
and pushes to Loki at http://localhost:3100.

Run this after dropping new log files into logs/server/.
Already-pushed files are tracked in grafana/.pushed so they
are never pushed twice.
"""

import glob
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

LOKI_URL   = "http://localhost:3100/loki/api/v1/push"
LOGS_DIR   = Path(__file__).parent.parent / "logs" / "server"
PUSHED_LOG = Path(__file__).parent / ".pushed"

# Regex matches: "2026-03-27 09:15:23 IST  INFO  [Bot]  message"
LINE_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) IST\s+(\w+)\s+\[([^\]]+)\]\s+(.*)"
)


def load_pushed() -> set:
    if PUSHED_LOG.exists():
        return set(PUSHED_LOG.read_text().splitlines())
    return set()


def mark_pushed(filename: str):
    with open(PUSHED_LOG, "a") as f:
        f.write(filename + "\n")


def parse_line(line: str):
    m = LINE_RE.match(line.strip())
    if not m:
        return None
    ts_str, level, component, message = m.groups()
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    # Convert IST → UTC for Loki (subtract 5h30m)
    ts_utc_s = dt.timestamp() - 19800
    ts_ns = str(int(ts_utc_s * 1_000_000_000))
    return ts_ns, level, component, line.strip()


def push_file(filepath: Path, filename: str):
    print(f"  Pushing {filename} ...", end="", flush=True)
    entries = []
    skipped = 0

    with open(filepath, encoding="utf-8", errors="replace") as f:
        for line in f:
            parsed = parse_line(line)
            if parsed:
                ts_ns, level, component, raw = parsed
                entries.append([ts_ns, raw])
            else:
                skipped += 1

    if not entries:
        print(f" skipped (no parseable lines)")
        return

    # Loki max batch = 5MB / 1000 entries — chunk to be safe
    CHUNK = 500
    for i in range(0, len(entries), CHUNK):
        chunk = entries[i:i + CHUNK]
        payload = {
            "streams": [{
                "stream": {
                    "job":      "tradingbot",
                    "source":   "server",
                    "filename": filename,
                },
                "values": chunk,
            }]
        }
        resp = requests.post(LOKI_URL, json=payload, timeout=10)
        if resp.status_code not in (200, 204):
            print(f"\n  ERROR: Loki returned {resp.status_code}: {resp.text}")
            return

    print(f" done  ({len(entries)} lines, {skipped} unparseable)")
    mark_pushed(filename)


def main():
    pushed = load_pushed()
    log_files = sorted(LOGS_DIR.glob("*.log*"))

    if not log_files:
        print(f"No log files found in {LOGS_DIR}")
        print("Drop your server log files there and re-run.")
        sys.exit(0)

    new_files = [f for f in log_files if f.name not in pushed]

    if not new_files:
        print("All files already pushed. Nothing to do.")
        print(f"Delete {PUSHED_LOG} to re-push everything.")
        sys.exit(0)

    print(f"Found {len(new_files)} new file(s) to push:\n")
    for lf in new_files:
        push_file(lf, lf.name)

    print(f"\nDone. Open http://localhost:3000 → Explore → Loki")
    print('Query: {job="tradingbot"}')


if __name__ == "__main__":
    # Quick check Loki is up
    try:
        requests.get("http://localhost:3100/ready", timeout=3)
    except Exception:
        print("Loki is not running. Start it first:")
        print("  cd grafana && docker-compose up -d")
        sys.exit(1)

    main()
