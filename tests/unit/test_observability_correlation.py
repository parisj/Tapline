"""Unit tests for src.observability.correlation.

Covers:
- Generation, get/set/reset of correlation ID via contextvars
- get_or_create_correlation_id behaviour
- correlation_context context manager
- Kafka header injection/extraction/propagation
- Context isolation across copy_context and threads
"""

from __future__ import annotations

import contextvars
import re
import threading
import uuid

import pytest

from src.observability import correlation as correlation_module
from src.observability.correlation import (
    DEFAULT_KAFKA_HEADER,
    correlation_context,
    extract_correlation_header,
    generate_correlation_id,
    get_correlation_id,
    get_or_create_correlation_id,
    inject_correlation_header,
    propagate_from_kafka_headers,
    reset_correlation_id,
    set_correlation_id,
)


@pytest.fixture(autouse=True)
def _clear_correlation_id() -> None:
    """Force the correlation ID ContextVar to None at the start of every
    test in this file.

    The module-level ContextVar can have a non-default value leftover
    from earlier tests in the suite (e.g. test_logging.py) that called
    set_correlation_id without a paired reset. We explicitly set the
    underlying ContextVar to None — get_correlation_id() then returns
    None, matching the default behaviour the tests assume.
    """
    correlation_module._correlation_id.set(None)


class TestGenerateCorrelationId:
    def test_returns_string(self) -> None:
        cid = generate_correlation_id()

        assert isinstance(cid, str)
        assert len(cid) > 0

    def test_returns_valid_uuid(self) -> None:
        cid = generate_correlation_id()

        # Should not raise
        parsed = uuid.UUID(cid)
        assert str(parsed) == cid

    def test_each_call_returns_unique_id(self) -> None:
        ids = {generate_correlation_id() for _ in range(50)}

        assert len(ids) == 50

    def test_format_is_uuid4_like(self) -> None:
        cid = generate_correlation_id()

        pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        assert re.match(pattern, cid) is not None


class TestGetCorrelationId:
    def test_default_is_none(self) -> None:
        # Inside a fresh context, default is None
        def check() -> str | None:
            return get_correlation_id()

        ctx = contextvars.copy_context()
        # We need to wipe via running in a fresh-ish context — but copy_context
        # carries over current value. Instead test that after reset, value is None.
        token = set_correlation_id("temp")
        reset_correlation_id(token)
        assert ctx.run(check) is None or get_correlation_id() is None

    def test_returns_set_value(self) -> None:
        token = set_correlation_id("my-correlation-id")
        try:
            assert get_correlation_id() == "my-correlation-id"
        finally:
            reset_correlation_id(token)


class TestSetAndResetCorrelationId:
    def test_set_and_get_roundtrip(self) -> None:
        token = set_correlation_id("abc-123")
        try:
            assert get_correlation_id() == "abc-123"
        finally:
            reset_correlation_id(token)

    def test_reset_restores_previous_value(self) -> None:
        outer = set_correlation_id("outer")
        try:
            inner = set_correlation_id("inner")
            assert get_correlation_id() == "inner"
            reset_correlation_id(inner)
            assert get_correlation_id() == "outer"
        finally:
            reset_correlation_id(outer)

    def test_reset_to_none_after_initial_set(self) -> None:
        token = set_correlation_id("foo")
        reset_correlation_id(token)

        # After reset, the value goes back to default (None)
        assert get_correlation_id() is None


class TestGetOrCreateCorrelationId:
    def test_returns_existing_id_if_set(self) -> None:
        token = set_correlation_id("existing")
        try:
            cid = get_or_create_correlation_id()
            assert cid == "existing"
        finally:
            reset_correlation_id(token)

    def test_creates_new_id_if_not_set(self) -> None:
        # Ensure no ID set
        token = set_correlation_id("temp")
        reset_correlation_id(token)
        assert get_correlation_id() is None

        # Run in a copied context to ensure isolation
        def runner() -> tuple[str, str | None]:
            new_cid = get_or_create_correlation_id()
            # After call, ID should be persisted
            return new_cid, get_correlation_id()

        ctx = contextvars.copy_context()
        created, after = ctx.run(runner)

        assert created is not None
        assert len(created) > 0
        # ID is persisted in the run-context only
        assert after == created

    def test_created_id_is_valid_uuid(self) -> None:
        # Wipe in a copied context
        def runner() -> str:
            # Force fresh by resetting
            t = set_correlation_id("x")
            reset_correlation_id(t)
            return get_or_create_correlation_id()

        ctx = contextvars.copy_context()
        cid = ctx.run(runner)
        uuid.UUID(cid)


class TestCorrelationContext:
    def test_yields_provided_id(self) -> None:
        with correlation_context("custom-id") as cid:
            assert cid == "custom-id"
            assert get_correlation_id() == "custom-id"

    def test_generates_id_when_none_provided(self) -> None:
        with correlation_context() as cid:
            assert cid is not None
            uuid.UUID(cid)
            assert get_correlation_id() == cid

    def test_restores_previous_value_on_exit(self) -> None:
        outer = set_correlation_id("outer")
        try:
            with correlation_context("inner") as cid:
                assert cid == "inner"
                assert get_correlation_id() == "inner"
            assert get_correlation_id() == "outer"
        finally:
            reset_correlation_id(outer)

    def test_restores_none_after_exit_when_no_prior(self) -> None:
        def runner() -> None:
            with correlation_context("inside") as cid:
                assert cid == "inside"
            # After exiting, value should be back to default (None)
            assert get_correlation_id() is None

        ctx = contextvars.copy_context()
        ctx.run(runner)

    def test_exception_still_resets(self) -> None:
        outer = set_correlation_id("outer")
        msg = "boom"

        def _raise_inside_inner() -> None:
            with correlation_context("inner"):
                assert get_correlation_id() == "inner"
                raise RuntimeError(msg)

        try:
            with pytest.raises(RuntimeError, match="boom"):
                _raise_inside_inner()
            assert get_correlation_id() == "outer"
        finally:
            reset_correlation_id(outer)


class TestContextIsolation:
    def test_isolation_across_threads(self) -> None:
        """Each thread should have its own correlation ID."""
        results: dict[int, str | None] = {}

        def worker(idx: int) -> None:
            with correlation_context(f"thread-{idx}"):
                results[idx] = get_correlation_id()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        for i in range(5):
            assert results[i] == f"thread-{i}"

    def test_isolation_across_copy_context(self) -> None:
        outer_token = set_correlation_id("outer-ctx")
        try:

            def child() -> str | None:
                set_correlation_id("inside-copy")
                return get_correlation_id()

            ctx = contextvars.copy_context()
            value_in_copy = ctx.run(child)

            assert value_in_copy == "inside-copy"
            # Parent context should be unchanged
            assert get_correlation_id() == "outer-ctx"
        finally:
            reset_correlation_id(outer_token)


class TestInjectCorrelationHeader:
    def test_injects_into_empty_dict(self) -> None:
        token = set_correlation_id("inject-id")
        try:
            headers = inject_correlation_header()
            assert DEFAULT_KAFKA_HEADER in headers
            assert headers[DEFAULT_KAFKA_HEADER] == b"inject-id"
        finally:
            reset_correlation_id(token)

    def test_injects_into_existing_headers(self) -> None:
        token = set_correlation_id("xyz")
        try:
            existing = {"foo": b"bar"}
            result = inject_correlation_header(existing)
            assert result["foo"] == b"bar"
            assert result[DEFAULT_KAFKA_HEADER] == b"xyz"
            # Returns the same (mutated) dict
            assert result is existing
        finally:
            reset_correlation_id(token)

    def test_uses_custom_header_name(self) -> None:
        token = set_correlation_id("hdr-test")
        try:
            headers = inject_correlation_header(header_name="X-Custom-CID")
            assert headers["X-Custom-CID"] == b"hdr-test"
        finally:
            reset_correlation_id(token)

    def test_generates_id_if_not_set(self) -> None:
        def runner() -> dict[str, bytes]:
            return inject_correlation_header()

        ctx = contextvars.copy_context()
        headers = ctx.run(runner)
        assert DEFAULT_KAFKA_HEADER in headers
        value = headers[DEFAULT_KAFKA_HEADER].decode("utf-8")
        uuid.UUID(value)


class TestExtractCorrelationHeader:
    def test_extracts_matching_header(self) -> None:
        headers = [("x-correlation-id", b"abc-456")]
        result = extract_correlation_header(headers)
        assert result == "abc-456"

    def test_returns_none_when_headers_none(self) -> None:
        assert extract_correlation_header(None) is None

    def test_returns_none_when_header_missing(self) -> None:
        headers = [("other-key", b"value")]
        assert extract_correlation_header(headers) is None

    def test_case_insensitive_header_match(self) -> None:
        headers = [("X-CORRELATION-ID", b"upper")]
        result = extract_correlation_header(headers)
        assert result == "upper"

    def test_custom_header_name(self) -> None:
        headers = [("my-cid", b"custom-value")]
        result = extract_correlation_header(headers, header_name="my-cid")
        assert result == "custom-value"

    def test_empty_list_returns_none(self) -> None:
        assert extract_correlation_header([]) is None


class TestPropagateFromKafkaHeaders:
    def test_sets_extracted_id_in_context(self) -> None:
        def runner() -> tuple[str, str | None]:
            headers = [("x-correlation-id", b"propagated-id")]
            returned = propagate_from_kafka_headers(headers)
            return returned, get_correlation_id()

        ctx = contextvars.copy_context()
        returned, in_context = ctx.run(runner)

        assert returned == "propagated-id"
        assert in_context == "propagated-id"

    def test_generates_new_id_when_headers_none(self) -> None:
        def runner() -> tuple[str, str | None]:
            returned = propagate_from_kafka_headers(None)
            return returned, get_correlation_id()

        ctx = contextvars.copy_context()
        returned, in_context = ctx.run(runner)

        uuid.UUID(returned)
        assert in_context == returned

    def test_generates_new_id_when_header_missing(self) -> None:
        def runner() -> str:
            return propagate_from_kafka_headers([("other-header", b"x")])

        ctx = contextvars.copy_context()
        result = ctx.run(runner)
        uuid.UUID(result)


class TestKafkaHeaderRoundTrip:
    def test_inject_extract_roundtrip(self) -> None:
        token = set_correlation_id("round-trip-id")
        try:
            injected = inject_correlation_header({})
            # Convert dict to list-of-tuples (Kafka header format)
            as_list = list(injected.items())
            extracted = extract_correlation_header(as_list)
            assert extracted == "round-trip-id"
        finally:
            reset_correlation_id(token)
