"""Middleware for rate limiting and security headers."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING

from flask import Response, jsonify, request

from src.utils.logging import get_logger
from src.visualization.constants import (
    CLEANUP_CUTOFF_SECONDS,
    RATE_LIMIT_BUCKET_CAPACITY,
    RATE_LIMIT_TOKENS_PER_SEC,
)

if TYPE_CHECKING:
    from flask import Flask

logger = get_logger(__name__)

# Type alias for Flask route responses (single Response or Response with status code)
FlaskResponse = Response | tuple[Response, int]


class RateLimiter:
    """Simple token bucket rate limiter for API protection.

    Each client (by IP) gets a bucket of tokens that refills at a constant rate.
    Each request consumes one token. If no tokens are available, the request
    is rate limited.
    """

    def __init__(
        self,
        rate: float = RATE_LIMIT_TOKENS_PER_SEC,
        capacity: int = RATE_LIMIT_BUCKET_CAPACITY,
        cleanup_interval: float = 60.0,
    ) -> None:
        """Initialize rate limiter.

        Args:
            rate: Tokens added per second (refill rate)
            capacity: Maximum tokens in bucket
            cleanup_interval: Seconds between old entry cleanup

        """
        self._rate = rate
        self._capacity = capacity
        self._cleanup_interval = cleanup_interval
        self._tokens: dict[str, tuple[float, float]] = {}  # key -> (tokens, last_update)
        self._lock = threading.Lock()
        self._last_cleanup = time.time()

    def allow(self, key: str) -> bool:
        """Check if request should be allowed.

        Args:
            key: Client identifier (typically IP address)

        Returns:
            True if request allowed, False if rate limited

        """
        with self._lock:
            now = time.time()

            # Periodic cleanup of old entries
            if now - self._last_cleanup > self._cleanup_interval:
                self._cleanup_old_entries(now)
                self._last_cleanup = now

            # Get or create bucket for this key
            tokens, last_update = self._tokens.get(key, (float(self._capacity), now))

            # Refill tokens based on time elapsed
            elapsed = now - last_update
            tokens = min(self._capacity, tokens + elapsed * self._rate)

            if tokens >= 1.0:
                self._tokens[key] = (tokens - 1.0, now)
                return True

            # Not enough tokens - rate limited
            self._tokens[key] = (tokens, now)
            return False

    def _cleanup_old_entries(self, now: float) -> None:
        """Remove entries that have been idle for a long time."""
        cutoff = now - CLEANUP_CUTOFF_SECONDS
        to_remove = [k for k, (_, last) in self._tokens.items() if last < cutoff]
        for k in to_remove:
            del self._tokens[k]


# Global rate limiter instance
rate_limiter = RateLimiter()


def register_middleware(app: Flask) -> None:
    """Register middleware on a Flask app.

    Args:
        app: Flask application instance

    """

    @app.before_request
    def check_rate_limit() -> FlaskResponse | None:
        """Apply rate limiting to API endpoints."""
        if request.path.startswith("/api/"):
            client_ip = request.remote_addr or "unknown"
            if not rate_limiter.allow(client_ip):
                logger.warning("Rate limit exceeded for client: %s", client_ip)
                response = jsonify({"error": "Rate limit exceeded. Please slow down."})
                response.headers["Retry-After"] = "5"
                return response, 429
        return None

    @app.after_request
    def add_security_headers(response: Response) -> Response:
        """Add security and cache headers to all responses."""
        # Security headers (OWASP recommendations)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"

        # Content Security Policy - restrict script sources
        csp_parts = [
            "default-src 'self'",
            "script-src 'self' 'unsafe-inline' https://cdn.plot.ly "
            "https://ajax.googleapis.com https://maxcdn.bootstrapcdn.com",
            "style-src 'self' 'unsafe-inline' https://maxcdn.bootstrapcdn.com",
            "img-src 'self' data:",
            "font-src 'self' https://maxcdn.bootstrapcdn.com",
            "connect-src 'self' http://localhost:* ws://localhost:*",
        ]
        response.headers["Content-Security-Policy"] = "; ".join(csp_parts)

        # No-cache headers for API responses to prevent stale data
        if request.path.startswith("/api/"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"

        return response
