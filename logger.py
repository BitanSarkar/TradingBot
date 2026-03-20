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

        # ── File — DEBUG and above, rotates daily, keeps 7 days ──────────
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        from logging.handlers import TimedRotatingFileHandler
        file_handler = TimedRotatingFileHandler(
            log_dir / "bot.log", when="midnight", backupCount=7, encoding="utf-8"
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
        ):
            logging.getLogger(lib).setLevel(logging.ERROR)

    return logging.getLogger(name)
