from __future__ import annotations

import logging
import os
from pathlib import Path

def configure_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE")

    handlers = []

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level, logging.INFO))
        handlers.append(file_handler)

    handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        handlers=handlers,
        format="%(asctime)s %(levelname)s %(threadName)s %(name)s - %(message)s",
    )

def get_logger(name: str = "pipeline") -> logging.Logger:
    """Get a named logger."""
    return logging.getLogger(name)
