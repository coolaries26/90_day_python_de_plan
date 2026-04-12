#!/usr/bin/env python3
"""
logger.py — Centralised Logging Factory
=========================================
Provides two loggers for the 90-day program:

  get_logger(name)         → stdlib logging.Logger  (for library modules)
  get_pipeline_logger()    → loguru logger           (for pipeline scripts)

Usage:
    # In library/utility modules (db_utils.py etc.):
    from logger import get_logger
    log = get_logger(__name__)
    log.info("Pool created")

    # In pipeline scripts:
    from logger import get_pipeline_logger
    logger = get_pipeline_logger()
    logger.info("Pipeline started | table={table} rows={rows}", table="film", rows=1000)
"""

import logging
import logging.handlers
import sys
import os
from pathlib import Path
from datetime import datetime

# ── Resolve project root and logs directory ───────────────────────────────────
_here = Path(__file__).resolve().parent
for _candidate in [_here, _here.parent, _here.parent.parent]:
    if (_candidate / ".env").is_file() or (_candidate / "logs").exists():
        PROJECT_ROOT = _candidate
        break
else:
    PROJECT_ROOT = _here.parent.parent

LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)

# ── Log format constants ──────────────────────────────────────────────────────
LOG_FORMAT     = "[{asctime}] {levelname:<8} {name} | {message}"
LOG_DATE_FMT   = "%Y-%m-%d %H:%M:%S"
LOG_STYLE      = "{"          # use {}-style formatting (not %-style)

# File naming: one log file per day per pipeline
def _log_file(prefix: str = "pipeline") -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    return LOGS_DIR / f"{prefix}_{date_str}.log"


# ── stdlib logging factory ────────────────────────────────────────────────────
_configured_loggers: set[str] = set()

def get_logger(
    name: str,
    level: str = None,
    log_file_prefix: str = "pipeline",
) -> logging.Logger:
    """
    Return a configured stdlib Logger for library/utility code.

    Args:
        name:            Logger name — use __name__ to get module path
        level:           Override log level (default: LOG_LEVEL env var or INFO)
        log_file_prefix: Prefix for log filename (default: 'pipeline')

    Returns:
        logging.Logger with StreamHandler + RotatingFileHandler attached
    """
    logger = logging.getLogger(name)

    # Only configure once per name — avoid duplicate handlers on re-import
    if name in _configured_loggers:
        return logger

    # Resolve level: argument → env var → INFO
    resolved_level = getattr(
        logging,
        (level or os.getenv("LOG_LEVEL", "INFO")).upper(),
        logging.INFO,
    )
    logger.setLevel(resolved_level)

    formatter = logging.Formatter(
        fmt=LOG_FORMAT, datefmt=LOG_DATE_FMT, style=LOG_STYLE
    )

    # Console handler — INFO and above
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # Rotating file handler — DEBUG and above
    # Rotates at 5MB, keeps 7 files: pipeline_20250115.log, .1, .2, ...
    file_handler = logging.handlers.RotatingFileHandler(
        filename=_log_file(log_file_prefix),
        maxBytes=5 * 1024 * 1024,   # 5 MB
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Prevent propagation to root logger (avoid double logging)
    logger.propagate = False

    _configured_loggers.add(name)
    return logger


# ── loguru pipeline logger factory ───────────────────────────────────────────
def get_pipeline_logger(
    pipeline_name: str = "pipeline",
    level: str = None,
):
    """
    Return a configured loguru logger for pipeline/application scripts.
    loguru's logger is a global singleton — this function configures it
    on first call and returns it for subsequent calls.

    Features enabled:
      - Console: coloured, human-readable
      - File: rotating at 5MB, JSON-structured, 7-day retention
      - Automatic exception traceback capture

    Args:
        pipeline_name: Used as log file prefix (e.g. 'etl', 'dashboard')
        level:         Log level string (default: LOG_LEVEL env var or INFO)

    Returns:
        loguru.logger (global singleton, reconfigured each call)
    """
    try:
        from loguru import logger
    except ImportError:
        # Fallback: return stdlib logger with same interface
        return get_logger(pipeline_name, level)

    resolved_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()

    # Remove default loguru handler
    logger.remove()

    # Console sink — coloured, human-readable
    logger.add(
        sys.stdout,
        level=resolved_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level:<8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        ),
        colorize=True,
    )

    # File sink — JSON structured, rotating
    log_path = LOGS_DIR / f"{pipeline_name}_{{time:YYYYMMDD}}.log"
    logger.add(
        str(log_path),
        level="DEBUG",
        rotation="5 MB",
        retention=7,            # keep 7 rotated files
        compression="zip",      # compress old files
        serialize=True,         # write as JSON — parseable by log aggregators
        backtrace=True,         # full stack on exceptions
        diagnose=True,          # variable values in tracebacks
        encoding="utf-8",
    )

    return logger


# ── Convenience: log pipeline run boundaries ─────────────────────────────────
def log_pipeline_start(logger, pipeline_name: str, **context) -> None:
    """Log a standardised pipeline start message with context."""
    logger.info(
        f"{'='*50}\n"
        f"  PIPELINE START: {pipeline_name}\n"
        f"  Context: {context}\n"
        f"{'='*50}"
    )

def log_pipeline_end(logger, pipeline_name: str, rows_processed: int,
                     elapsed_sec: float, status: str = "SUCCESS") -> None:
    """Log a standardised pipeline end message."""
    logger.info(
        f"{'='*50}\n"
        f"  PIPELINE END: {pipeline_name} | {status}\n"
        f"  Rows: {rows_processed:,} | Elapsed: {elapsed_sec:.2f}s\n"
        f"{'='*50}"
    )

