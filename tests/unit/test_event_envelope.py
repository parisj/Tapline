"""Unit tests for event envelope and event types."""

from datetime import datetime, timezone

import pytest

from src.domain.events import (
    EventEnvelope,
    EventType,
    compute_chain_hash,
    compute_content_hash,
    job_completed_payload,
    job_created_payload,
    job_failed_payload,
    job_started_payload,
    metric_emitted_payload,
    result_produced_payload,
)


class TestComputeContentHash:
    def test_deterministic(self):
        payload = {"key": "value", "nested": {"a": 1}}
        hash1 = compute_content_hash(payload)
        hash2 = compute_content_hash(payload)
        assert hash1 == hash2

    def test_order_independent(self):
        payload1 = {"a": 1, "b": 2}
        payload2 = {"b": 2, "a": 1}
        assert compute_content_hash(payload1) == compute_content_hash(payload2)

    def test_different_payloads_different_hashes(self):
        payload1 = {"key": "value1"}
        payload2 = {"key": "value2"}
        assert compute_content_hash(payload1) != compute_content_hash(payload2)

    def test_returns_hex_string(self):
        payload = {"test": True}
        hash_val = compute_content_hash(payload)
        assert isinstance(hash_val, str)
        assert len(hash_val) == 64  # SHA-256 hex


class TestComputeChainHash:
    def test_with_prev_hash(self):
        content_hash = "abc123"
        prev_hash = "def456"
        chain_hash = compute_chain_hash(content_hash, prev_hash)
        assert isinstance(chain_hash, str)
        assert len(chain_hash) == 64

    def test_without_prev_hash(self):
        content_hash = "abc123"
        chain_hash = compute_chain_hash(content_hash, None)
        assert isinstance(chain_hash, str)
        assert len(chain_hash) == 64

    def test_different_prev_hash_different_result(self):
        content_hash = "abc123"
        hash1 = compute_chain_hash(content_hash, "prev1")
        hash2 = compute_chain_hash(content_hash, "prev2")
        assert hash1 != hash2


class TestEventEnvelope:
    def test_create_event(self):
        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="source-1",
            payload={"job_id": "job-123"},
        )

        assert event.event_type == EventType.JOB_CREATED
        assert event.source_id == "source-1"
        assert event.payload == {"job_id": "job-123"}
        assert event.content_hash is not None
        assert event.prev_hash is None
        assert event.signature is None

    def test_create_event_with_prev_hash(self):
        event = EventEnvelope.create(
            event_type=EventType.JOB_STARTED,
            source_id="source-1",
            payload={"job_id": "job-123"},
            prev_hash="previous-hash-value",
        )

        assert event.prev_hash == "previous-hash-value"

    def test_create_event_with_timestamp(self):
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        event = EventEnvelope.create(
            event_type=EventType.JOB_COMPLETED,
            source_id="source-1",
            payload={"job_id": "job-123"},
            timestamp=ts,
        )

        assert event.timestamp == ts

    def test_to_dict(self):
        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="source-1",
            payload={"job_id": "job-123"},
        )

        data = event.to_dict()
        assert data["event_type"] == "JOB_CREATED"
        assert data["source_id"] == "source-1"
        assert data["payload"] == {"job_id": "job-123"}
        assert "event_id" in data
        assert "timestamp" in data
        assert "content_hash" in data

    def test_from_dict(self):
        original = EventEnvelope.create(
            event_type=EventType.JOB_FAILED,
            source_id="source-1",
            payload={"job_id": "job-123", "error": "test error"},
        )

        data = original.to_dict()
        restored = EventEnvelope.from_dict(data)

        assert restored.event_id == original.event_id
        assert restored.event_type == original.event_type
        assert restored.source_id == original.source_id
        assert restored.payload == original.payload
        assert restored.content_hash == original.content_hash

    def test_to_json_and_back(self):
        original = EventEnvelope.create(
            event_type=EventType.RESULT_PRODUCED,
            source_id="source-1",
            payload={"job_id": "job-123", "metric_count": 5},
        )

        json_str = original.to_json()
        restored = EventEnvelope.from_json(json_str)

        assert restored.event_id == original.event_id
        assert restored.payload == original.payload

    def test_immutability(self):
        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="source-1",
            payload={"job_id": "job-123"},
        )

        with pytest.raises(AttributeError):
            event.source_id = "new-source"


class TestPayloadFactories:
    def test_job_created_payload(self):
        payload = job_created_payload(
            job_id="job-123",
            directory_key="path0",
            path="/data/image.png",
            fingerprint="fp123",
            file_hash="hash456",
        )

        assert payload["job_id"] == "job-123"
        assert payload["directory_key"] == "path0"
        assert payload["path"] == "/data/image.png"
        assert payload["fingerprint"] == "fp123"
        assert payload["file_hash"] == "hash456"

    def test_job_started_payload(self):
        payload = job_started_payload(
            job_id="job-123",
            algo_name="analysis_probe",
            algo_version="1.0.0",
        )

        assert payload["job_id"] == "job-123"
        assert payload["algo_name"] == "analysis_probe"
        assert payload["algo_version"] == "1.0.0"

    def test_job_completed_payload(self):
        payload = job_completed_payload(
            job_id="job-123",
            algo_name="analysis_probe",
            algo_version="1.0.0",
            duration_ms=150.5,
        )

        assert payload["job_id"] == "job-123"
        assert payload["duration_ms"] == 150.5

    def test_job_failed_payload(self):
        payload = job_failed_payload(
            job_id="job-123",
            algo_name="analysis_probe",
            algo_version="1.0.0",
            error="Something went wrong",
            error_type="RuntimeError",
        )

        assert payload["job_id"] == "job-123"
        assert payload["error"] == "Something went wrong"
        assert payload["error_type"] == "RuntimeError"

    def test_result_produced_payload(self):
        payload = result_produced_payload(
            job_id="job-123",
            algo_name="analysis_probe",
            algo_version="1.0.0",
            metric_count=5,
            artifact_refs=["hash1", "hash2"],
        )

        assert payload["job_id"] == "job-123"
        assert payload["metric_count"] == 5
        assert payload["artifact_refs"] == ["hash1", "hash2"]

    def test_metric_emitted_payload(self):
        payload = metric_emitted_payload(
            job_id="job-123",
            algo_name="analysis_probe",
            algo_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            analysis_mask=1,
            meta={"unit": "percent"},
        )

        assert payload["job_id"] == "job-123"
        assert payload["metric_name"] == "accuracy"
        assert payload["value"] == 0.95
        assert payload["analysis_mask"] == 1
        assert payload["meta"] == {"unit": "percent"}
