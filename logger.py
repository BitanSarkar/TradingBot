import logging
import sys
from pathlib import Path


def get_logger(name: str = "TradingBot") -> logging.Logger:
    """
    Configure the ROOT logger once so that every module's logger
    (DataFetcher, ScoringEngine, ScoreBasedStrategy, etc.) automatically
    inherits the same console + file handlers via propagation.

    All subsequent calls return the named child logger without
    re-adding handlers.
    """
    root = logging.getLogger()          # the root logger

    if not root.handlers:               # only wire up handlers once
        root.setLevel(logging.DEBUG)    # let handlers decide the cutoff

        fmt = logging.Formatter(
            fmt="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # ── Console — INFO and above ─────────────────────────────────────
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(fmt)
        root.addHandler(console)

        # ── File — DEBUG and above, size-capped at 20 GB total ──────────────
        #   Each file  : 1 GB   (LOG_MAX_BYTES)
        #   Max backups: 19     (LOG_BACKUP_COUNT)   → 20 files × 1 GB = 20 GB cap
        #
        # When bot.log hits 1 GB it is renamed to bot.log.1, bot.log.2 … etc.
        # Once bot.log.19 exists the oldest file is deleted automatically, so
        # the logs/ folder never exceeds LOG_MAX_BYTES × (LOG_BACKUP_COUNT + 1).
        import os
        from logging.handlers import RotatingFileHandler

        _MB = 1024 * 1024
        _GB = 1024 * _MB
        max_bytes    = int(os.getenv("LOG_MAX_BYTES",    1 * _GB))   # 1 GB per file
        backup_count = int(os.getenv("LOG_BACKUP_COUNT", 19))        # 19 backups → 20 GB cap

        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        file_handler = RotatingFileHandler(
            log_dir / "bot.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)

        # Silence noisy third-party libraries (they propagate to root too)
        for lib in (
            "urllib3", "urllib3.connectionpool",   # "Connection pool is full" warnings
            "requests", "httpx", "httpcore",
            "transformers", "torch", "filelock",
            "huggingface_hub", "accelerate",
            "nsepy", "nselib",
            "charset_normalizer",
            "yfinance", "peewee",              # yfinance internal noise
        ):
            logging.getLogger(lib).setLevel(logging.ERROR)

    return logging.getLogger(name)
