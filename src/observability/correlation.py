"""Correlation ID management for request tracing.

Provides correlation ID propagation across service boundaries,
including Kafka message headers.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from contextvars import ContextVar, Token
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

# Context variable for correlation ID
_correlation_id: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def generate_correlation_id() -> str:
    """Generate a new unique correlation ID.

    Returns:
        UUID string

    """
    return str(uuid.uuid4())


def get_correlation_id() -> str | None:
    """Get the current correlation ID.

    Returns:
        Current correlation ID or None if not set

    """
    return _correlation_id.get()


def get_or_create_correlation_id() -> str:
    """Get current correlation ID or create a new one.

    Returns:
        Existing or new correlation ID

    """
    cid = _correlation_id.get()
    if cid is None:
        cid = generate_correlation_id()
        _correlation_id.set(cid)
    return cid


def set_correlation_id(cid: str) -> Token[str | None]:
    """Set the correlation ID for current context.

    Args:
        cid: Correlation ID to set

    Returns:
        Token that can be used to reset the value

    """
    return _correlation_id.set(cid)


def reset_correlation_id(token: Token[str | None]) -> None:
    """Reset correlation ID to previous value.

    Args:
        token: Token from set_correlation_id

    """
    _correlation_id.reset(token)


@contextmanager
def correlation_context(cid: str | None = None) -> Iterator[str]:
    """Context manager for correlation ID scope.

    Args:
        cid: Correlation ID to use, or None to generate new one

    Yields:
        The correlation ID in use

    Example:
        with correlation_context("req-123") as cid:
            process_request()
            # All code here has access to cid via get_correlation_id()

    """
    if cid is None:
        cid = generate_correlation_id()

    token = set_correlation_id(cid)
    try:
        yield cid
    finally:
        reset_correlation_id(token)


# =============================================================================
# Kafka Header Helpers
# =============================================================================

DEFAULT_KAFKA_HEADER = "x-correlation-id"


def inject_correlation_header(
    headers: dict[str, bytes] | None = None,
    header_name: str = DEFAULT_KAFKA_HEADER,
) -> dict[str, bytes]:
    """Inject correlation ID into Kafka headers.

    Args:
        headers: Existing headers dict (or None)
        header_name: Header name to use

    Returns:
        Headers dict with correlation ID added

    """
    if headers is None:
        headers = {}

    cid = get_or_create_correlation_id()
    headers[header_name] = cid.encode("utf-8")
    return headers


def extract_correlation_header(
    headers: list[tuple[str, bytes]] | None,
    header_name: str = DEFAULT_KAFKA_HEADER,
) -> str | None:
    """Extract correlation ID from Kafka headers.

    Args:
        headers: Kafka message headers
        header_name: Header name to look for

    Returns:
        Correlation ID or None if not found

    """
    if headers is None:
        return None

    for key, value in headers:
        if key.lower() == header_name.lower():
            return value.decode("utf-8")
    return None


def propagate_from_kafka_headers(
    headers: list[tuple[str, bytes]] | None,
    header_name: str = DEFAULT_KAFKA_HEADER,
) -> str:
    """Extract correlation ID from headers and set in context.

    If no correlation ID is found, generates a new one.

    Args:
        headers: Kafka message headers
        header_name: Header name to look for

    Returns:
        The correlation ID (extracted or generated)

    """
    cid = extract_correlation_header(headers, header_name)
    if cid is None:
        cid = generate_correlation_id()
    set_correlation_id(cid)
    return cid
