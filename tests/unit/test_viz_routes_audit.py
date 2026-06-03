"""Unit tests for the audit route blueprint."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

from src.visualization.routes.audit import (
    _get_chain_status_message,
    _verify_chain_status,
    audit_bp,
    init_blueprint,
)


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    if "audit" not in app.blueprints:
        app.register_blueprint(audit_bp)
    yield app


@pytest.fixture
def client(app):
    with app.test_client() as c:
        yield c


@pytest.fixture(autouse=True)
def _reset_blueprint_state():
    init_blueprint(None)
    yield
    init_blueprint(None)


class TestVerifyChainNotInitialized:
    def test_returns_503_when_storage_none(self, client) -> None:
        resp = client.get("/api/audit/verify")
        assert resp.status_code == 503


class TestVerifyChainNoEvents:
    @patch("src.visualization.routes.audit.requests")
    def test_no_events_status(self, mock_requests, client) -> None:
        # Prometheus returns no audit count
        mock_requests.get.return_value.ok = False

        storage = MagicMock()
        storage.list_objects.return_value = []
        init_blueprint(storage)

        resp = client.get("/api/audit/verify")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "no_events"
        assert body["event_count"] == 0
        assert body["verified"] is False


class TestVerifyChainKafkaOnly:
    @patch("src.visualization.routes.audit.requests")
    def test_returns_kafka_only_status(self, mock_requests, client) -> None:
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {
            "status": "success",
            "data": {"result": [{"value": [0, "42"]}]},
        }
        mock_requests.get.return_value = mock_resp

        storage = MagicMock()
        storage.list_objects.return_value = []
        init_blueprint(storage)

        resp = client.get("/api/audit/verify")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "kafka_only"
        assert body["kafka_events"] == 42


class TestVerifyChainWithStoredEvents:
    @patch("src.visualization.routes.audit.HashChainVerifier")
    @patch("src.visualization.routes.audit.requests")
    def test_invokes_chain_verifier(self, mock_requests, mock_verifier_cls, client) -> None:
        mock_requests.get.return_value.ok = False  # No prometheus

        # Audit doc with valid events list
        audit_doc = {
            "events": [
                {
                    "event_id": "e1",
                    "event_type": "TASK_CREATED",
                    "source_id": "src",
                    "timestamp": "2024-01-01T00:00:00",
                    "payload": {},
                    "content_hash": "h1",
                },
            ],
        }
        storage = MagicMock()
        storage.list_objects.return_value = [{"key": "audit/k1"}]
        storage.retrieve_by_key.return_value = json.dumps(audit_doc).encode("utf-8")

        # Verifier returns valid
        mock_verifier = MagicMock()
        result = MagicMock()
        result.valid = True
        result.errors = []
        mock_verifier.verify_chain.return_value = result
        mock_verifier_cls.return_value = mock_verifier

        init_blueprint(storage)

        resp = client.get("/api/audit/verify")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["status"] == "valid"
        assert body["verified"] is True
        assert body["stored_events"] == 1

    @patch("src.visualization.routes.audit.requests")
    def test_handles_invalid_event_type(self, mock_requests, client) -> None:
        mock_requests.get.return_value.ok = False

        audit_doc = {
            "events": [
                {
                    "event_id": "e1",
                    "event_type": "NOT_A_REAL_TYPE",
                    "source_id": "s",
                    "timestamp": "t",
                    "payload": {},
                    "content_hash": "h",
                },
            ],
        }
        storage = MagicMock()
        storage.list_objects.return_value = [{"key": "audit/k1"}]
        storage.retrieve_by_key.return_value = json.dumps(audit_doc).encode("utf-8")
        init_blueprint(storage)

        resp = client.get("/api/audit/verify")
        assert resp.status_code == 200
        body = resp.get_json()
        # No valid events ingested -> falls through to no_events or kafka_only
        assert body["status"] in {"no_events", "kafka_only"}
        # Error should be reported
        assert any("Failed to parse" in e for e in body["errors"])


class TestAuditStats:
    def test_returns_503_when_storage_none(self, client) -> None:
        resp = client.get("/api/audit/stats")
        assert resp.status_code == 503

    def test_returns_stats_with_no_objects(self, client) -> None:
        storage = MagicMock()
        storage.list_objects.return_value = []
        init_blueprint(storage)

        resp = client.get("/api/audit/stats")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["audit_objects"] == 0
        assert body["total_events"] == 0
        assert body["last_event_time"] is None

    def test_returns_stats_with_objects(self, client) -> None:
        storage = MagicMock()
        storage.list_objects.return_value = [
            {"key": "audit/x", "last_modified": "2024-01-01"},
            {"key": "audit/y", "last_modified": "2024-02-02"},
        ]
        init_blueprint(storage)

        resp = client.get("/api/audit/stats")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["audit_objects"] == 2
        assert body["last_event_time"] == "2024-02-02"

    def test_handles_list_failure(self, client) -> None:
        storage = MagicMock()
        # First call inside the inner try raises
        storage.list_objects.side_effect = RuntimeError("boom")
        init_blueprint(storage)

        resp = client.get("/api/audit/stats")
        # Inner try/except catches it; outer returns 200 with default stats
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["audit_objects"] == 0


class TestVerifyChainStatusHelper:
    def test_no_events(self) -> None:
        status, count = _verify_chain_status([], [], 0)
        assert status == "no_events"
        assert count == 0

    def test_kafka_only(self) -> None:
        status, count = _verify_chain_status([], [], 5)
        assert status == "kafka_only"
        assert count == 5


class TestChainStatusMessage:
    def test_valid(self) -> None:
        msg = _get_chain_status_message("valid", [], 0)
        assert "verified" in msg

    def test_invalid_includes_error_count(self) -> None:
        msg = _get_chain_status_message("invalid", ["e1", "e2"], 0)
        assert "2" in msg
        assert "FAILED" in msg

    def test_kafka_only_includes_count(self) -> None:
        msg = _get_chain_status_message("kafka_only", [], 7)
        assert "7" in msg

    def test_no_events(self) -> None:
        msg = _get_chain_status_message("no_events", [], 0)
        assert "No audit events" in msg

    def test_unknown_status(self) -> None:
        msg = _get_chain_status_message("not-a-status", [], 0)
        assert msg == "Unknown status"
