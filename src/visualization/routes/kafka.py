"""Kafka health check routes."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient
from flask import Blueprint, Response, jsonify

from src.utils.logging import get_logger
from src.visualization.constants import MAX_TOPICS_DISPLAY

if TYPE_CHECKING:
    from src.visualization.services import PrometheusService

logger = get_logger(__name__)

# Type alias for Flask route responses
FlaskResponse = Response | tuple[Response, int]

kafka_bp = Blueprint("kafka", __name__, url_prefix="/api/kafka")

# Service injected via init_blueprint
_prometheus_service: PrometheusService | None = None


def init_blueprint(prometheus_service: PrometheusService | None) -> None:
    """Initialize the blueprint with required services.

    Args:
        prometheus_service: Prometheus service instance for metrics queries

    """
    global _prometheus_service
    _prometheus_service = prometheus_service


@kafka_bp.route("/health", methods=["GET"])
def kafka_health() -> FlaskResponse:
    """Check Kafka broker connectivity and get cluster metadata.

    Returns broker status, topic count, and message statistics.
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        # Create admin client with short timeout for health check
        admin_conf: dict[str, str | int | float | bool] = {
            "bootstrap.servers": bootstrap_servers,
            "socket.timeout.ms": 5000,
            "request.timeout.ms": 5000,
        }
        admin = AdminClient(admin_conf)

        # Get cluster metadata (this verifies connectivity)
        metadata = admin.list_topics(timeout=5)

        # Count topics and partitions
        topics = [t for t in metadata.topics if not t.startswith("_")]
        total_partitions = sum(len(t.partitions) for t in metadata.topics.values())

        # Get broker info
        brokers = list(metadata.brokers.values())
        broker_count = len(brokers)

        # Try to get message count from Prometheus
        total_messages = 0
        if _prometheus_service:
            result = _prometheus_service.query("sum(tapline_kafka_messages_produced_total)")
            if result.data.get("status") == "success":
                data_result = result.data.get("data", {}).get("result", [])
                if data_result:
                    total_messages = int(float(data_result[0].get("value", [0, 0])[1]))

        return jsonify(
            {
                "healthy": True,
                "bootstrap_servers": bootstrap_servers,
                "broker_count": broker_count,
                "topic_count": len(topics),
                "partition_count": total_partitions,
                "total_messages": total_messages,
                "topics": topics[:MAX_TOPICS_DISPLAY],
            },
        )
    except KafkaException as e:
        logger.warning("Kafka health check failed: %s", e)
        return jsonify(
            {
                "healthy": False,
                "bootstrap_servers": bootstrap_servers,
                "error": str(e),
            },
        )
    except Exception as e:
        logger.warning("Kafka health check error: %s", e)
        return jsonify(
            {
                "healthy": False,
                "bootstrap_servers": bootstrap_servers,
                "error": str(e),
            },
        )


@kafka_bp.route("/audit-log/count", methods=["GET"])
def kafka_audit_log_count() -> FlaskResponse:
    """Get the number of events in the audit-log Kafka topic.

    Returns the total message count across all partitions.
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    audit_topic = "tapline.audit"

    try:
        # Create consumer to query offsets
        consumer_conf: dict[str, str | int | float | bool | None] = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "dashboard-audit-check",
            "socket.timeout.ms": 5000,
            "session.timeout.ms": 6000,
        }
        consumer = Consumer(consumer_conf)

        try:
            # Get topic metadata
            metadata = consumer.list_topics(audit_topic, timeout=5)

            if audit_topic not in metadata.topics:
                return jsonify(
                    {
                        "topic": audit_topic,
                        "exists": False,
                        "event_count": 0,
                        "partitions": 0,
                    },
                )

            topic_meta = metadata.topics[audit_topic]
            partition_count = len(topic_meta.partitions)

            # Get high watermarks for each partition
            total_events = 0
            for partition_id in topic_meta.partitions:
                tp = TopicPartition(audit_topic, partition_id)
                low, high = consumer.get_watermark_offsets(tp, timeout=5)
                total_events += high - low

            return jsonify(
                {
                    "topic": audit_topic,
                    "exists": True,
                    "event_count": total_events,
                    "partitions": partition_count,
                },
            )
        finally:
            consumer.close()

    except KafkaException as e:
        logger.warning("Failed to get audit log count: %s", e)
        return jsonify(
            {
                "topic": audit_topic,
                "exists": False,
                "event_count": 0,
                "error": str(e),
            },
        )
    except Exception as e:
        logger.warning("Audit log count error: %s", e)
        return jsonify(
            {
                "topic": audit_topic,
                "exists": False,
                "event_count": 0,
                "error": str(e),
            },
        )
