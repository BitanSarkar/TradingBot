#!/bin/bash
cd /Users/bitansarkar/MySpace/TradingBot
.venv/bin/python bootstrap.py --ohlcv --fundamentals --universe >> /tmp/bootstrap_mac.log 2>&1
