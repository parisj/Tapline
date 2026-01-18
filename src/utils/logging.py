"""Centralized logging configuration with structured JSON support.

Provides:
- Structured JSON logging for production
- Text logging for development
- Trace context injection (OpenTelemetry)
- Correlation ID injection
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.observability.config import ObservabilityConfig

# Default format for text logging
TEXT_FORMAT = "%(asctime)s %(levelname)s %(threadName)s %(name)s - %(message)s"


class StructuredLogFormatter(logging.Formatter):
    """JSON formatter with trace context and correlation ID injection.

    Produces structured JSON logs suitable for log aggregation systems
    like ELK, Loki, or CloudWatch.
    """

    def __init__(
        self,
        include_trace_context: bool = True,
        include_correlation_id: bool = True,
        include_caller_info: bool = False,
    ) -> None:
        super().__init__()
        self.include_trace_context = include_trace_context
        self.include_correlation_id = include_correlation_id
        self.include_caller_info = include_caller_info

    def format(self, record: logging.LogRecord) -> str:
        log_dict: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "thread": record.threadName,
        }

        # Add correlation ID if available
        if self.include_correlation_id:
            try:
                from src.observability.correlation import get_correlation_id

                cid = get_correlation_id()
                if cid:
                    log_dict["correlation_id"] = cid
            except ImportError:
                pass

        # Add trace context if available
        if self.include_trace_context:
            try:
                from src.observability.tracing import get_current_trace_context

                trace_ctx = get_current_trace_context()
                if trace_ctx:
                    log_dict["trace_id"] = trace_ctx.get("trace_id")
                    log_dict["span_id"] = trace_ctx.get("span_id")
            except ImportError:
                pass

        # Add caller info
        if self.include_caller_info:
            log_dict["file"] = record.pathname
            log_dict["line"] = record.lineno
            log_dict["function"] = record.funcName

        # Add exception info if present
        if record.exc_info:
            log_dict["exception"] = self.formatException(record.exc_info)

        # Add any extra fields from the record
        for key, value in record.__dict__.items():
            if key not in {
                "name",
                "msg",
                "args",
                "created",
                "filename",
                "funcName",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "exc_info",
                "exc_text",
                "thread",
                "threadName",
                "taskName",
                "message",
            }:
                log_dict[key] = value

        return json.dumps(log_dict, default=str)


class TextLogFormatter(logging.Formatter):
    """Text formatter with optional trace context.

    Produces human-readable logs for development.
    """

    def __init__(
        self,
        include_trace_context: bool = True,
        include_correlation_id: bool = True,
    ) -> None:
        super().__init__(TEXT_FORMAT)
        self.include_trace_context = include_trace_context
        self.include_correlation_id = include_correlation_id

    def format(self, record: logging.LogRecord) -> str:
        # Add correlation ID to message prefix
        prefix_parts = []

        if self.include_correlation_id:
            try:
                from src.observability.correlation import get_correlation_id

                cid = get_correlation_id()
                if cid:
                    prefix_parts.append(f"[cid:{cid[:8]}]")
            except ImportError:
                pass

        if self.include_trace_context:
            try:
                from src.observability.tracing import get_current_trace_context

                trace_ctx = get_current_trace_context()
                if trace_ctx:
                    trace_id = trace_ctx.get("trace_id", "")[:8]
                    prefix_parts.append(f"[tid:{trace_id}]")
            except ImportError:
                pass

        if prefix_parts:
            record.msg = " ".join(prefix_parts) + " " + str(record.msg)

        return super().format(record)


def configure_logging(config: ObservabilityConfig | None = None) -> None:
    """Configure logging based on environment and config.

    Args:
        config: Observability config. If None, uses environment variables.
    """
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE")
    log_format = os.getenv("LOG_FORMAT", "text").lower()

    # Override with config if provided
    if config is not None:
        log_format = config.log_format.lower()
        include_trace = config.include_trace_context
        include_correlation = config.include_correlation_id
        include_caller = config.include_caller_info
    else:
        include_trace = True
        include_correlation = True
        include_caller = False

    # Create formatter based on format type
    if log_format == "json":
        formatter = StructuredLogFormatter(
            include_trace_context=include_trace,
            include_correlation_id=include_correlation,
            include_caller_info=include_caller,
        )
    else:
        formatter = TextLogFormatter(
            include_trace_context=include_trace,
            include_correlation_id=include_correlation,
        )

    # Configure handlers
    handlers: list[logging.Handler] = []

    # File handler
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level, logging.INFO))
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # Console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(getattr(logging, level, logging.INFO))
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level, logging.INFO))

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add new handlers
    for handler in handlers:
        root_logger.addHandler(handler)

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)


def get_logger(name: str = "pipeline") -> logging.Logger:
    """Get a named logger.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def log_with_context(
    logger: logging.Logger,
    level: int,
    msg: str,
    **context: Any,
) -> None:
    """Log a message with additional context fields.

    When using JSON logging, context fields appear as separate keys.

    Args:
        logger: Logger to use
        level: Log level (e.g., logging.INFO)
        msg: Log message
        **context: Additional context fields
    """
    logger.log(level, msg, extra=context)
