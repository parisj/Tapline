"""Unit tests for logging utilities."""

import json
import logging
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.utils.logging import (
    StructuredLogFormatter,
    TextLogFormatter,
    configure_logging,
    get_logger,
    log_with_context,
)


class TestStructuredLogFormatter:
    def test_format_returns_valid_json(self) -> None:
        formatter = StructuredLogFormatter(
            include_trace_context=False,
            include_correlation_id=False,
        )
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "test.logger"
        assert parsed["message"] == "Test message"
        assert "timestamp" in parsed
        assert "thread" in parsed

    def test_format_with_args(self) -> None:
        formatter = StructuredLogFormatter(
            include_trace_context=False,
            include_correlation_id=False,
        )
        record = logging.LogRecord(
            name="test.logger",
            level=logging.WARNING,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Value is %d",
            args=(42,),
            exc_info=None,
        )

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed["message"] == "Value is 42"

    def test_format_with_exception(self) -> None:
        formatter = StructuredLogFormatter(
            include_trace_context=False,
            include_correlation_id=False,
        )

        try:
            msg = "Test error"
            raise ValueError(msg)
        except ValueError:
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test.logger",
            level=logging.ERROR,
            pathname="/path/to/file.py",
            lineno=42,
            msg="An error occurred",
            args=(),
            exc_info=exc_info,
        )

        result = formatter.format(record)
        parsed = json.loads(result)

        assert "exception" in parsed
        assert "ValueError: Test error" in parsed["exception"]

    def test_format_with_caller_info(self) -> None:
        formatter = StructuredLogFormatter(
            include_trace_context=False,
            include_correlation_id=False,
            include_caller_info=True,
        )
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.funcName = "test_function"

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed["file"] == "/path/to/file.py"
        assert parsed["line"] == 42
        assert parsed["function"] == "test_function"

    def test_format_with_extra_fields(self) -> None:
        formatter = StructuredLogFormatter(
            include_trace_context=False,
            include_correlation_id=False,
        )
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.custom_field = "custom_value"
        record.job_id = "job-123"

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed["custom_field"] == "custom_value"
        assert parsed["job_id"] == "job-123"

    @patch("src.observability.correlation.get_correlation_id")
    def test_format_with_correlation_id(self, mock_get_cid: MagicMock) -> None:
        mock_get_cid.return_value = "test-correlation-id"

        formatter = StructuredLogFormatter(
            include_trace_context=False,
            include_correlation_id=True,
        )
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed.get("correlation_id") == "test-correlation-id"

    @patch("src.observability.tracing.get_current_trace_context")
    def test_format_with_trace_context(self, mock_get_trace: MagicMock) -> None:
        mock_get_trace.return_value = {
            "trace_id": "abc123",
            "span_id": "def456",
        }

        formatter = StructuredLogFormatter(
            include_trace_context=True,
            include_correlation_id=False,
        )
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)
        parsed = json.loads(result)

        assert parsed.get("trace_id") == "abc123"
        assert parsed.get("span_id") == "def456"


class TestTextLogFormatter:
    def test_format_basic_message(self) -> None:
        formatter = TextLogFormatter(
            include_trace_context=False,
            include_correlation_id=False,
        )
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)

        assert "INFO" in result
        assert "test.logger" in result
        assert "Test message" in result

    @patch("src.observability.correlation.get_correlation_id")
    def test_format_with_correlation_prefix(self, mock_get_cid: MagicMock) -> None:
        mock_get_cid.return_value = "abcd1234efgh5678"

        formatter = TextLogFormatter(
            include_trace_context=False,
            include_correlation_id=True,
        )
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)

        # Correlation ID is truncated to 8 chars in text format
        assert "[cid:abcd1234]" in result

    @patch("src.observability.tracing.get_current_trace_context")
    def test_format_with_trace_prefix(self, mock_get_trace: MagicMock) -> None:
        mock_get_trace.return_value = {
            "trace_id": "abc12345def67890",
            "span_id": "span123",
        }

        formatter = TextLogFormatter(
            include_trace_context=True,
            include_correlation_id=False,
        )
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="/path/to/file.py",
            lineno=42,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)

        # Trace ID is truncated to 8 chars in text format
        assert "[tid:abc12345]" in result


class TestConfigureLogging:
    def test_configure_with_default_env(self) -> None:
        # Clear environment variables that might affect test
        env_backup = {}
        for key in ["LOG_LEVEL", "LOG_FILE", "LOG_FORMAT"]:
            env_backup[key] = os.environ.pop(key, None)

        try:
            configure_logging(None)
            root_logger = logging.getLogger()

            # Should configure with defaults
            assert root_logger.level == logging.INFO
        finally:
            # Restore environment
            for key, value in env_backup.items():
                if value is not None:
                    os.environ[key] = value

    def test_configure_with_debug_level(self) -> None:
        with patch.dict(os.environ, {"LOG_LEVEL": "DEBUG", "LOG_FORMAT": "text"}):
            configure_logging(None)
            root_logger = logging.getLogger()
            assert root_logger.level == logging.DEBUG

    def test_configure_with_json_format(self) -> None:
        with patch.dict(os.environ, {"LOG_FORMAT": "json", "LOG_LEVEL": "INFO"}):
            configure_logging(None)
            root_logger = logging.getLogger()

            # Check that JSON formatter is used
            for handler in root_logger.handlers:
                if isinstance(handler, logging.StreamHandler):
                    assert isinstance(handler.formatter, StructuredLogFormatter)

    def test_configure_with_file_handler(self, tmp_path: Path) -> None:
        log_file = tmp_path / "test.log"

        with patch.dict(os.environ, {"LOG_FILE": str(log_file), "LOG_LEVEL": "INFO"}):
            configure_logging(None)
            root_logger = logging.getLogger()

            # Should have file handler
            file_handlers = [h for h in root_logger.handlers if isinstance(h, logging.FileHandler)]
            assert len(file_handlers) >= 1

    def test_configure_creates_log_directory(self, tmp_path: Path) -> None:
        log_dir = tmp_path / "logs" / "subdir"
        log_file = log_dir / "test.log"

        with patch.dict(os.environ, {"LOG_FILE": str(log_file), "LOG_LEVEL": "INFO"}):
            configure_logging(None)

            # Directory should be created
            assert log_dir.exists()

    def test_configure_reduces_third_party_noise(self) -> None:
        configure_logging(None)

        urllib3_logger = logging.getLogger("urllib3")
        kafka_logger = logging.getLogger("kafka")
        confluent_logger = logging.getLogger("confluent_kafka")

        assert urllib3_logger.level >= logging.WARNING
        assert kafka_logger.level >= logging.WARNING
        assert confluent_logger.level >= logging.WARNING


class TestGetLogger:
    def test_returns_logger_instance(self) -> None:
        logger = get_logger("test.module")

        assert isinstance(logger, logging.Logger)
        assert logger.name == "test.module"

    def test_default_name(self) -> None:
        logger = get_logger()

        assert logger.name == "pipeline"

    def test_same_name_returns_same_logger(self) -> None:
        logger1 = get_logger("test.same")
        logger2 = get_logger("test.same")

        assert logger1 is logger2


class TestLogWithContext:
    def test_log_with_extra_context(self) -> None:
        # Use a list to capture log records
        captured_records: list[logging.LogRecord] = []

        class CaptureHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured_records.append(record)

        # Use a unique logger name to avoid test pollution
        logger = logging.getLogger("test.context.extra.isolated")
        logger.propagate = False  # Don't propagate to root logger
        # Clear any existing handlers
        logger.handlers.clear()

        handler = CaptureHandler()
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            log_with_context(
                logger,
                logging.INFO,
                "Test message",
                job_id="job-123",
                metric_name="accuracy",
            )

            # Verify log was captured
            assert len(captured_records) == 1
            record = captured_records[0]
            assert record.msg == "Test message"
            assert record.job_id == "job-123"  # type: ignore[attr-defined]
            assert record.metric_name == "accuracy"  # type: ignore[attr-defined]
        finally:
            logger.removeHandler(handler)

    def test_log_different_levels(self) -> None:
        captured_records: list[logging.LogRecord] = []

        class CaptureHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                captured_records.append(record)

        # Use a unique logger name to avoid test pollution
        logger = logging.getLogger("test.levels.context.isolated")
        logger.propagate = False  # Don't propagate to root logger
        # Clear any existing handlers
        logger.handlers.clear()

        handler = CaptureHandler()
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        try:
            log_with_context(logger, logging.WARNING, "Warning message", code=500)

            assert len(captured_records) == 1
            record = captured_records[0]
            assert record.levelno == logging.WARNING
            assert record.code == 500  # type: ignore[attr-defined]
        finally:
            logger.removeHandler(handler)
