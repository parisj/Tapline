"""Audit chain verification routes."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import requests
from flask import Blueprint, Response, jsonify

from src.audit.chain import HashChainVerifier
from src.domain.events import EventEnvelope, EventType
from src.utils.logging import get_logger
from src.visualization.constants import PROMETHEUS_URL

if TYPE_CHECKING:
    from src.storage.minio_service import MinioStorageService

logger = get_logger(__name__)

# Type alias for Flask route responses
FlaskResponse = Response | tuple[Response, int]

audit_bp = Blueprint("audit", __name__, url_prefix="/api/audit")

# Service injected via init_blueprint
_storage: MinioStorageService | None = None


def init_blueprint(storage: MinioStorageService | None) -> None:
    """Initialize the blueprint with required services.

    Args:
        storage: MinIO storage service instance

    """
    global _storage
    _storage = storage


@audit_bp.route("/verify", methods=["GET"])
def verify_audit_chain() -> FlaskResponse:
    """Verify the audit chain integrity.

    Checks that the hash chain in the audit log is intact and untampered.
    Returns verification status, event count, and any integrity errors.

    Note: Audit events are currently stored in Kafka (tapline.audit topic)
    but not persisted to MinIO. Full chain verification requires reading
    from Kafka or implementing an audit log consumer.
    """
    if _storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    try:
        events: list[EventEnvelope] = []
        errors: list[str] = []
        kafka_audit_count = _get_kafka_audit_count()

        # Check for audit log objects in MinIO aggregates bucket
        _load_audit_events_from_storage(events, errors)

        # Determine chain status
        chain_status, event_count = _verify_chain_status(events, errors, kafka_audit_count)

        return jsonify(
            {
                "status": chain_status,
                "event_count": event_count,
                "kafka_events": kafka_audit_count,
                "stored_events": len(events),
                "errors": errors,
                "verified": chain_status == "valid",
                "message": _get_chain_status_message(chain_status, errors, kafka_audit_count),
            },
        )
    except Exception as e:
        logger.exception("Failed to verify audit chain")
        return jsonify({"error": str(e), "status": "error"}), 500


@audit_bp.route("/stats", methods=["GET"])
def audit_stats() -> FlaskResponse:
    """Get audit chain statistics without full verification."""
    if _storage is None:
        return jsonify({"error": "Storage service not initialized"}), 503

    try:
        stats = {
            "partitions": 0,
            "total_events": 0,
            "last_event_time": None,
            "audit_objects": 0,
        }

        # Count audit objects in storage
        try:
            audit_objects = list(_storage.list_objects("aggregates", prefix="audit/", limit=1000))
            stats["audit_objects"] = len(audit_objects)

            # Get latest event info if available
            if audit_objects:
                latest = audit_objects[-1] if audit_objects else None
                if latest and isinstance(latest, dict):
                    stats["last_event_time"] = latest.get("last_modified")
        except Exception as e:
            logger.warning("Failed to get audit stats: %s", e)

        return jsonify(stats)
    except Exception as e:
        logger.exception("Failed to get audit stats")
        return jsonify({"error": str(e)}), 500


def _get_kafka_audit_count() -> int:
    """Get the count of audit events from Kafka via Prometheus metrics.

    Returns:
        Number of audit events in Kafka, or 0 if unavailable

    """
    try:
        query = 'sum(tapline_kafka_messages_produced_total{topic="tapline.audit"}) or vector(0)'
        resp = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=5)
        if resp.ok:
            data = resp.json()
            if data.get("status") == "success" and data.get("data", {}).get("result"):
                result = data["data"]["result"]
                if result and len(result) > 0:
                    return int(float(result[0].get("value", [0, 0])[1]))
    except Exception as e:
        logger.debug("Could not query Kafka audit metrics: %s", e)

    return 0


def _load_audit_events_from_storage(events: list[EventEnvelope], errors: list[str]) -> None:
    """Load audit events from MinIO storage.

    Args:
        events: List to populate with loaded events
        errors: List to populate with any errors encountered

    """
    if _storage is None:
        return

    try:
        audit_objects = list(_storage.list_objects("aggregates", prefix="audit/", limit=100))
        for obj in audit_objects:
            if isinstance(obj, dict) and obj.get("key"):
                try:
                    data = _storage.retrieve_by_key("aggregates", obj["key"])
                    if data:
                        doc = json.loads(data.decode("utf-8"))
                        if "events" in doc:
                            for evt_data in doc["events"]:
                                try:
                                    evt = EventEnvelope(
                                        event_id=evt_data.get("event_id", ""),
                                        event_type=EventType(evt_data.get("event_type", "JOB_CREATED")),
                                        source_id=evt_data.get("source_id", ""),
                                        timestamp=evt_data.get("timestamp", ""),
                                        payload=evt_data.get("payload", {}),
                                        content_hash=evt_data.get("content_hash", ""),
                                        prev_hash=evt_data.get("prev_hash"),
                                        signature=evt_data.get("signature"),
                                    )
                                    events.append(evt)
                                except Exception as e:
                                    errors.append(f"Failed to parse event: {e}")
                except Exception as e:
                    errors.append(f"Failed to read audit object {obj.get('key')}: {e}")
    except Exception as e:
        logger.debug("No audit log objects in MinIO: %s", e)


def _verify_chain_status(
    events: list[EventEnvelope],
    errors: list[str],
    kafka_audit_count: int,
) -> tuple[str, int]:
    """Verify the chain and determine status.

    Args:
        events: List of events to verify
        errors: List to populate with verification errors
        kafka_audit_count: Count of events in Kafka

    Returns:
        Tuple of (status string, event count)

    """
    if events:
        verifier = HashChainVerifier()
        result = verifier.verify_chain(events)
        errors.extend(result.errors)
        return ("valid" if result.valid else "invalid", len(events))
    if kafka_audit_count > 0:
        return ("kafka_only", kafka_audit_count)
    return ("no_events", 0)


def _get_chain_status_message(chain_status: str, errors: list[str], kafka_audit_count: int) -> str:
    """Get a human-readable message for the chain status.

    Args:
        chain_status: Chain verification status
        errors: List of errors encountered
        kafka_audit_count: Count of events in Kafka

    Returns:
        Human-readable status message

    """
    messages = {
        "valid": "Audit chain integrity verified - no tampering detected",
        "invalid": f"Audit chain integrity check FAILED - {len(errors)} error(s) found",
        "kafka_only": f"Audit events in Kafka ({kafka_audit_count}) - requires audit consumer",
        "no_events": "No audit events found - run the pipeline to generate audit trail",
        "unknown": "Unable to verify audit chain",
    }
    return messages.get(chain_status, "Unknown status")
