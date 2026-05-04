"""Unit tests for event envelope and event types."""

from datetime import UTC, datetime

import pytest

from src.domain.events import (
    EventEnvelope,
    EventType,
    compute_chain_hash,
    compute_content_hash,
    metric_emitted_payload,
    result_produced_payload,
    task_completed_payload,
    task_created_payload,
    task_failed_payload,
    task_started_payload,
)


class TestComputeContentHash:
    def test_deterministic(self) -> None:
        payload = {"key": "value", "nested": {"a": 1}}
        hash1 = compute_content_hash(payload)
        hash2 = compute_content_hash(payload)
        assert hash1 == hash2

    def test_order_independent(self) -> None:
        payload1 = {"a": 1, "b": 2}
        payload2 = {"b": 2, "a": 1}
        assert compute_content_hash(payload1) == compute_content_hash(payload2)

    def test_different_payloads_different_hashes(self) -> None:
        payload1 = {"key": "value1"}
        payload2 = {"key": "value2"}
        assert compute_content_hash(payload1) != compute_content_hash(payload2)

    def test_returns_hex_string(self) -> None:
        payload = {"test": True}
        hash_val = compute_content_hash(payload)
        assert isinstance(hash_val, str)
        assert len(hash_val) == 64  # SHA-256 hex


class TestComputeChainHash:
    def test_with_prev_hash(self) -> None:
        content_hash = "abc123"
        prev_hash = "def456"
        chain_hash = compute_chain_hash(content_hash, prev_hash)
        assert isinstance(chain_hash, str)
        assert len(chain_hash) == 64

    def test_without_prev_hash(self) -> None:
        content_hash = "abc123"
        chain_hash = compute_chain_hash(content_hash, None)
        assert isinstance(chain_hash, str)
        assert len(chain_hash) == 64

    def test_different_prev_hash_different_result(self) -> None:
        content_hash = "abc123"
        hash1 = compute_chain_hash(content_hash, "prev1")
        hash2 = compute_chain_hash(content_hash, "prev2")
        assert hash1 != hash2


class TestEventEnvelope:
    def test_create_event(self) -> None:
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="source-1",
            payload={"task_id": "task-123"},
        )

        assert event.event_type == EventType.TASK_CREATED
        assert event.source_id == "source-1"
        assert event.payload == {"task_id": "task-123"}
        assert event.content_hash is not None
        assert event.prev_hash is None
        assert event.signature is None

    def test_create_event_with_prev_hash(self) -> None:
        event = EventEnvelope.create(
            event_type=EventType.TASK_STARTED,
            source_id="source-1",
            payload={"task_id": "task-123"},
            prev_hash="previous-hash-value",
        )

        assert event.prev_hash == "previous-hash-value"

    def test_create_event_with_timestamp(self) -> None:
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        event = EventEnvelope.create(
            event_type=EventType.TASK_COMPLETED,
            source_id="source-1",
            payload={"task_id": "task-123"},
            timestamp=ts,
        )

        assert event.timestamp == ts

    def test_to_dict(self) -> None:
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="source-1",
            payload={"task_id": "task-123"},
        )

        data = event.to_dict()
        assert data["event_type"] == "TASK_CREATED"
        assert data["source_id"] == "source-1"
        assert data["payload"] == {"task_id": "task-123"}
        assert "event_id" in data
        assert "timestamp" in data
        assert "content_hash" in data

    def test_from_dict(self) -> None:
        original = EventEnvelope.create(
            event_type=EventType.TASK_FAILED,
            source_id="source-1",
            payload={"task_id": "task-123", "error": "test error"},
        )

        data = original.to_dict()
        restored = EventEnvelope.from_dict(data)

        assert restored.event_id == original.event_id
        assert restored.event_type == original.event_type
        assert restored.source_id == original.source_id
        assert restored.payload == original.payload
        assert restored.content_hash == original.content_hash

    def test_to_json_and_back(self) -> None:
        original = EventEnvelope.create(
            event_type=EventType.RESULT_PRODUCED,
            source_id="source-1",
            payload={"task_id": "task-123", "metric_count": 5},
        )

        json_str = original.to_json()
        restored = EventEnvelope.from_json(json_str)

        assert restored.event_id == original.event_id
        assert restored.payload == original.payload

    def test_immutability(self) -> None:
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="source-1",
            payload={"task_id": "task-123"},
        )

        with pytest.raises(AttributeError):
            event.source_id = "new-source"


class TestPayloadFactories:
    def test_task_created_payload(self) -> None:
        payload = task_created_payload(
            task_id="task-123",
            directory_key="path0",
            path="/data/image.png",
            fingerprint="fp123",
            file_hash="hash456",
        )

        assert payload["task_id"] == "task-123"
        assert payload["directory_key"] == "path0"
        assert payload["path"] == "/data/image.png"
        assert payload["fingerprint"] == "fp123"
        assert payload["file_hash"] == "hash456"

    def test_task_started_payload(self) -> None:
        payload = task_started_payload(
            task_id="task-123",
            processor_name="analysis_probe",
            processor_version="1.0.0",
        )

        assert payload["task_id"] == "task-123"
        assert payload["processor_name"] == "analysis_probe"
        assert payload["processor_version"] == "1.0.0"

    def test_task_completed_payload(self) -> None:
        payload = task_completed_payload(
            task_id="task-123",
            processor_name="analysis_probe",
            processor_version="1.0.0",
            duration_ms=150.5,
        )

        assert payload["task_id"] == "task-123"
        assert payload["duration_ms"] == 150.5

    def test_task_failed_payload(self) -> None:
        payload = task_failed_payload(
            task_id="task-123",
            processor_name="analysis_probe",
            processor_version="1.0.0",
            error="Something went wrong",
            error_type="RuntimeError",
        )

        assert payload["task_id"] == "task-123"
        assert payload["error"] == "Something went wrong"
        assert payload["error_type"] == "RuntimeError"

    def test_result_produced_payload(self) -> None:
        payload = result_produced_payload(
            task_id="task-123",
            processor_name="analysis_probe",
            processor_version="1.0.0",
            metric_count=5,
            artifact_refs=["hash1", "hash2"],
        )

        assert payload["task_id"] == "task-123"
        assert payload["metric_count"] == 5
        assert payload["artifact_refs"] == ["hash1", "hash2"]

    def test_metric_emitted_payload(self) -> None:
        payload = metric_emitted_payload(
            task_id="task-123",
            processor_name="analysis_probe",
            processor_version="1.0.0",
            metric_name="accuracy",
            value=0.95,
            aggregation_mask=1,
            meta={"unit": "percent"},
        )

        assert payload["task_id"] == "task-123"
        assert payload["metric_name"] == "accuracy"
        assert payload["value"] == 0.95
        assert payload["aggregation_mask"] == 1
        assert payload["meta"] == {"unit": "percent"}
