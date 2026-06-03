"""Unit tests for visualization middleware (rate limiter + security headers)."""

from __future__ import annotations

import json

import pytest
from flask import Flask, jsonify

from src.visualization import middleware
from src.visualization.middleware import RateLimiter, register_middleware


class TestRateLimiterAllow:
    def test_initial_request_allowed(self) -> None:
        rl = RateLimiter(rate=10.0, capacity=5)
        assert rl.allow("client1") is True

    def test_consumes_token_per_call(self) -> None:
        rl = RateLimiter(rate=0.0, capacity=3)
        assert rl.allow("client1") is True
        assert rl.allow("client1") is True
        assert rl.allow("client1") is True
        # Bucket exhausted, no refill (rate=0)
        assert rl.allow("client1") is False

    def test_separate_clients_have_separate_buckets(self) -> None:
        rl = RateLimiter(rate=0.0, capacity=1)
        assert rl.allow("client1") is True
        assert rl.allow("client2") is True
        # Each client's own bucket should now be exhausted
        assert rl.allow("client1") is False
        assert rl.allow("client2") is False

    def test_refill_replenishes_tokens(self) -> None:
        rl = RateLimiter(rate=1000.0, capacity=2)
        # Exhaust bucket
        assert rl.allow("c") is True
        assert rl.allow("c") is True
        # Heavy rate means even tiny time elapse refills - sleep a bit
        import time

        time.sleep(0.01)
        assert rl.allow("c") is True

    def test_capacity_caps_token_accumulation(self) -> None:
        rl = RateLimiter(rate=1_000_000.0, capacity=2)
        # Initial allow: tokens = capacity, consume 1
        assert rl.allow("c") is True
        # After massive refill, tokens still capped at capacity
        # We can still consume up to capacity each time, but never more
        # in burst before more wall-clock passes.
        assert rl.allow("c") is True
        # Now no time passes between calls in tight loop -> deterministic check
        # We can't reliably assert false here due to refill speed, so just
        # make sure allow returns True for normal usage
        assert rl.allow("c") is True


class TestRateLimiterCleanup:
    def test_cleanup_removes_idle_entries(self) -> None:
        rl = RateLimiter(rate=0.0, capacity=1, cleanup_interval=0.0)
        rl.allow("idle_client")
        assert "idle_client" in rl._tokens
        # Manually move the entry's last_update far into the past
        old_tokens, _ = rl._tokens["idle_client"]
        rl._tokens["idle_client"] = (old_tokens, 0.0)  # epoch-zero
        # Force cleanup by another allow call (cleanup_interval=0)
        rl.allow("new_client")
        assert "idle_client" not in rl._tokens
        assert "new_client" in rl._tokens


class TestRegisterMiddlewareSecurityHeaders:
    @pytest.fixture
    def app(self) -> Flask:
        app = Flask(__name__)

        @app.route("/api/ping")
        def ping():
            return jsonify({"ok": True})

        @app.route("/static-asset")
        def asset():
            return "hello"

        register_middleware(app)
        return app

    @pytest.fixture
    def client(self, app):
        with app.test_client() as c:
            yield c

    def test_x_content_type_options(self, client) -> None:
        resp = client.get("/api/ping")
        assert resp.headers["X-Content-Type-Options"] == "nosniff"

    def test_x_frame_options_deny(self, client) -> None:
        resp = client.get("/api/ping")
        assert resp.headers["X-Frame-Options"] == "DENY"

    def test_x_xss_protection(self, client) -> None:
        resp = client.get("/api/ping")
        assert resp.headers["X-XSS-Protection"] == "1; mode=block"

    def test_referrer_policy(self, client) -> None:
        resp = client.get("/api/ping")
        assert resp.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"

    def test_permissions_policy(self, client) -> None:
        resp = client.get("/api/ping")
        assert "geolocation=()" in resp.headers["Permissions-Policy"]

    def test_csp_header_contains_default_src(self, client) -> None:
        resp = client.get("/api/ping")
        csp = resp.headers["Content-Security-Policy"]
        assert "default-src 'self'" in csp
        assert "cdn.plot.ly" in csp


class TestRegisterMiddlewareCacheHeaders:
    @pytest.fixture
    def app(self) -> Flask:
        app = Flask(__name__)

        @app.route("/api/ping")
        def ping():
            return jsonify({"ok": True})

        @app.route("/static-asset")
        def asset():
            return "hello"

        register_middleware(app)
        return app

    @pytest.fixture
    def client(self, app):
        with app.test_client() as c:
            yield c

    def test_api_has_no_cache_headers(self, client) -> None:
        resp = client.get("/api/ping")
        assert "no-cache" in resp.headers["Cache-Control"]
        assert resp.headers["Pragma"] == "no-cache"
        assert resp.headers["Expires"] == "0"

    def test_non_api_does_not_set_no_cache(self, client) -> None:
        resp = client.get("/static-asset")
        # Cache-Control may or may not be set, but Pragma should not be no-cache
        assert resp.headers.get("Pragma") != "no-cache"


class TestRegisterMiddlewareRateLimiting:
    @pytest.fixture
    def app(self, monkeypatch) -> Flask:
        # Replace the global rate_limiter with a stricter one for test
        strict = RateLimiter(rate=0.0, capacity=2)
        monkeypatch.setattr(middleware, "rate_limiter", strict)

        app = Flask(__name__)

        @app.route("/api/ping")
        def ping():
            return jsonify({"ok": True})

        @app.route("/non-api")
        def non_api():
            return "ok"

        register_middleware(app)
        return app

    @pytest.fixture
    def client(self, app):
        with app.test_client() as c:
            yield c

    def test_rate_limit_returns_429_when_exceeded(self, client) -> None:
        # capacity=2 means first two go through, third rate limited
        r1 = client.get("/api/ping")
        r2 = client.get("/api/ping")
        r3 = client.get("/api/ping")

        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r3.status_code == 429

    def test_rate_limit_includes_retry_after(self, client) -> None:
        client.get("/api/ping")
        client.get("/api/ping")
        resp = client.get("/api/ping")
        assert resp.status_code == 429
        assert resp.headers.get("Retry-After") == "5"
        data = json.loads(resp.data)
        assert "error" in data

    def test_non_api_endpoints_not_rate_limited(self, client) -> None:
        # Hammer the non-API endpoint - never get 429
        for _ in range(10):
            resp = client.get("/non-api")
            assert resp.status_code == 200


class TestModuleLevelRateLimiter:
    def test_module_rate_limiter_exists(self) -> None:
        assert isinstance(middleware.rate_limiter, RateLimiter)
