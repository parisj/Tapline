"""Event models for Kafka-based event streaming.

Events are the core unit of communication in the streaming architecture.
Each event is wrapped in an EventEnvelope that provides:
- Unique identification (event_id)
- Ordering guarantees (source_id for partition key)
- Audit trail (content_hash, prev_hash for hash chain)
- Future extensibility (signature field for PKI)
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Any


class EventType(str, Enum):
    """Event types for the Tapline streaming pipeline."""

    # Task lifecycle events
    TASK_CREATED = "TASK_CREATED"
    TASK_STARTED = "TASK_STARTED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"

    # Result events
    RESULT_PRODUCED = "RESULT_PRODUCED"
    METRIC_EMITTED = "METRIC_EMITTED"

    # Aggregation events
    AGGREGATE_COMPUTED = "AGGREGATE_COMPUTED"
    ARTIFACT_STORED = "ARTIFACT_STORED"


# Backwards compatibility aliases (deprecated)
JOB_CREATED = EventType.TASK_CREATED
JOB_STARTED = EventType.TASK_STARTED
JOB_COMPLETED = EventType.TASK_COMPLETED
JOB_FAILED = EventType.TASK_FAILED


@dataclass(frozen=True)
class EventEnvelope:
    """Immutable event envelope with hash chain support.

    The envelope wraps event payloads with metadata for:
    - Identification: event_id (UUID), event_type
    - Ordering: source_id (partition key), timestamp
    - Audit: content_hash, prev_hash (chain), signature (future PKI)

    Hash Chain:
        Each event's content_hash is computed from its payload.
        The prev_hash links to the previous event in the same partition,
        creating a tamper-evident chain for GxP compliance.
    """

    event_id: str
    event_type: EventType
    source_id: str
    timestamp: datetime
    payload: dict[str, Any]
    content_hash: str
    prev_hash: str | None = None
    signature: str | None = None

    @classmethod
    def create(
        cls,
        event_type: EventType,
        source_id: str,
        payload: dict[str, Any],
        prev_hash: str | None = None,
        timestamp: datetime | None = None,
    ) -> EventEnvelope:
        """Factory method to create a new event with computed hash.

        Args:
            event_type: Type of event (from EventType enum)
            source_id: Partition key for ordering guarantees
            payload: Event data (must be JSON-serializable)
            prev_hash: Hash of previous event in partition (for chain)
            timestamp: Event timestamp (defaults to now UTC)

        Returns:
            New EventEnvelope with computed content_hash

        """
        if timestamp is None:
            timestamp = datetime.now(UTC)

        content_hash = compute_content_hash(payload)

        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            source_id=source_id,
            timestamp=timestamp,
            payload=payload,
            content_hash=content_hash,
            prev_hash=prev_hash,
            signature=None,
        )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary for Kafka/JSON."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "source_id": self.source_id,
            "timestamp": self.timestamp.isoformat(),
            "payload": self.payload,
            "content_hash": self.content_hash,
            "prev_hash": self.prev_hash,
            "signature": self.signature,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> EventEnvelope:
        """Deserialize from dictionary."""
        event_type_value = data["event_type"]
        # Handle backwards compatibility for old JOB_* event types
        event_type_map = {
            "JOB_CREATED": EventType.TASK_CREATED,
            "JOB_STARTED": EventType.TASK_STARTED,
            "JOB_COMPLETED": EventType.TASK_COMPLETED,
            "JOB_FAILED": EventType.TASK_FAILED,
        }
        event_type = event_type_map.get(event_type_value, EventType(event_type_value))

        return cls(
            event_id=data["event_id"],
            event_type=event_type,
            source_id=data["source_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            payload=data["payload"],
            content_hash=data["content_hash"],
            prev_hash=data.get("prev_hash"),
            signature=data.get("signature"),
        )

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_json(cls, json_str: str) -> EventEnvelope:
        """Deserialize from JSON string."""
        return cls.from_dict(json.loads(json_str))


def compute_content_hash(payload: dict[str, Any]) -> str:
    """Compute SHA-256 hash of payload for content addressing.

    Uses canonical JSON serialization (sorted keys) for deterministic hashing.

    Args:
        payload: Event payload dictionary

    Returns:
        Hex-encoded SHA-256 hash

    """
    canonical = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_chain_hash(content_hash: str, prev_hash: str | None) -> str:
    """Compute hash linking current event to previous in chain.

    This creates the cryptographic chain:
        chain_hash = SHA256(content_hash || prev_hash)

    Args:
        content_hash: Hash of current event's payload
        prev_hash: Hash of previous event (None for genesis)

    Returns:
        Hex-encoded chain hash

    """
    data = content_hash + (prev_hash or "")
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


# Payload factory functions for common event types


def task_created_payload(
    task_id: str,
    directory_key: str,
    path: str,
    fingerprint: str,
    file_hash: str | None = None,
) -> dict[str, Any]:
    """Create payload for TASK_CREATED event."""
    return {
        "task_id": task_id,
        "directory_key": directory_key,
        "path": path,
        "fingerprint": fingerprint,
        "file_hash": file_hash,
    }


def task_started_payload(task_id: str, processor_name: str, processor_version: str) -> dict[str, Any]:
    """Create payload for TASK_STARTED event."""
    return {
        "task_id": task_id,
        "processor_name": processor_name,
        "processor_version": processor_version,
    }


def task_completed_payload(
    task_id: str,
    processor_name: str,
    processor_version: str,
    duration_ms: float,
) -> dict[str, Any]:
    """Create payload for TASK_COMPLETED event."""
    return {
        "task_id": task_id,
        "processor_name": processor_name,
        "processor_version": processor_version,
        "duration_ms": duration_ms,
    }


def task_failed_payload(
    task_id: str,
    processor_name: str | None,
    processor_version: str | None,
    error: str,
    error_type: str | None = None,
) -> dict[str, Any]:
    """Create payload for TASK_FAILED event."""
    return {
        "task_id": task_id,
        "processor_name": processor_name,
        "processor_version": processor_version,
        "error": error,
        "error_type": error_type,
    }


def result_produced_payload(
    task_id: str,
    processor_name: str,
    processor_version: str,
    metric_count: int,
    artifact_refs: list[str],
) -> dict[str, Any]:
    """Create payload for RESULT_PRODUCED event."""
    return {
        "task_id": task_id,
        "processor_name": processor_name,
        "processor_version": processor_version,
        "metric_count": metric_count,
        "artifact_refs": artifact_refs,
    }


def metric_emitted_payload(
    task_id: str,
    processor_name: str,
    processor_version: str,
    metric_name: str,
    value: float | str | dict[str, Any] | bool | None,
    aggregation_mask: int,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create payload for METRIC_EMITTED event."""
    return {
        "task_id": task_id,
        "processor_name": processor_name,
        "processor_version": processor_version,
        "metric_name": metric_name,
        "value": value,
        "aggregation_mask": aggregation_mask,
        "meta": meta or {},
    }


def aggregate_computed_payload(
    processor_name: str,
    processor_version: str,
    metric_name: str,
    aggregation_type: str,
    window_start_unix: float,
    window_end_unix: float,
    summary: dict[str, Any],
    artifact_ref: str | None = None,
) -> dict[str, Any]:
    """Create payload for AGGREGATE_COMPUTED event."""
    return {
        "processor_name": processor_name,
        "processor_version": processor_version,
        "metric_name": metric_name,
        "aggregation_type": aggregation_type,
        "window_start_unix": window_start_unix,
        "window_end_unix": window_end_unix,
        "summary": summary,
        "artifact_ref": artifact_ref,
    }


def artifact_stored_payload(
    content_hash: str,
    bucket: str,
    key: str,
    size: int,
    mime: str,
    source_task_id: str | None = None,
) -> dict[str, Any]:
    """Create payload for ARTIFACT_STORED event."""
    return {
        "content_hash": content_hash,
        "bucket": bucket,
        "key": key,
        "size": size,
        "mime": mime,
        "source_task_id": source_task_id,
    }


# Backwards compatibility aliases (deprecated)
job_created_payload = task_created_payload
job_started_payload = task_started_payload
job_completed_payload = task_completed_payload
job_failed_payload = task_failed_payload
