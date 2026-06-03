"""Unit tests for the Kafka health route blueprint."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException
from flask import Flask

from src.visualization.routes.kafka import init_blueprint, kafka_bp


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    if "kafka" not in app.blueprints:
        app.register_blueprint(kafka_bp)
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


class TestKafkaHealth:
    @patch("src.visualization.routes.kafka.AdminClient")
    def test_healthy_response(self, mock_admin_cls, client) -> None:
        admin = MagicMock()
        metadata = MagicMock()
        topic_partitions = MagicMock(partitions={0: MagicMock(), 1: MagicMock()})
        metadata.topics = {"tapline.tasks": topic_partitions, "_internal": MagicMock(partitions={})}
        metadata.brokers = {1: MagicMock(), 2: MagicMock()}
        admin.list_topics.return_value = metadata
        mock_admin_cls.return_value = admin

        # Wire up prometheus service to provide message count
        prom = MagicMock()
        result = MagicMock()
        result.data = {"status": "success", "data": {"result": [{"value": [0, "123"]}]}}
        prom.query.return_value = result
        init_blueprint(prom)

        resp = client.get("/api/kafka/health")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["healthy"] is True
        assert body["broker_count"] == 2
        # Topic count excludes "_"-prefixed internal topics
        assert body["topic_count"] == 1
        assert body["total_messages"] == 123
        assert "tapline.tasks" in body["topics"]

    @patch("src.visualization.routes.kafka.AdminClient")
    def test_kafka_exception_returns_unhealthy(self, mock_admin_cls, client) -> None:
        admin = MagicMock()
        admin.list_topics.side_effect = KafkaException("boom")
        mock_admin_cls.return_value = admin

        resp = client.get("/api/kafka/health")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["healthy"] is False
        assert "error" in body

    @patch("src.visualization.routes.kafka.AdminClient")
    def test_generic_exception_returns_unhealthy(self, mock_admin_cls, client) -> None:
        mock_admin_cls.side_effect = RuntimeError("generic")

        resp = client.get("/api/kafka/health")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["healthy"] is False

    @patch("src.visualization.routes.kafka.AdminClient")
    def test_no_prometheus_service_returns_zero_messages(self, mock_admin_cls, client) -> None:
        admin = MagicMock()
        metadata = MagicMock()
        metadata.topics = {}
        metadata.brokers = {}
        admin.list_topics.return_value = metadata
        mock_admin_cls.return_value = admin

        # Don't init blueprint with prometheus
        resp = client.get("/api/kafka/health")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["total_messages"] == 0

    @patch("src.visualization.routes.kafka.AdminClient")
    def test_prometheus_query_failure_returns_zero_messages(self, mock_admin_cls, client) -> None:
        admin = MagicMock()
        metadata = MagicMock()
        metadata.topics = {}
        metadata.brokers = {}
        admin.list_topics.return_value = metadata
        mock_admin_cls.return_value = admin

        prom = MagicMock()
        result = MagicMock()
        result.data = {"status": "error"}
        prom.query.return_value = result
        init_blueprint(prom)

        resp = client.get("/api/kafka/health")
        body = resp.get_json()
        assert body["total_messages"] == 0


class TestKafkaAuditLogCount:
    @patch("src.visualization.routes.kafka.TopicPartition")
    @patch("src.visualization.routes.kafka.Consumer")
    def test_returns_exists_false_when_topic_missing(self, mock_consumer_cls, mock_tp_cls, client) -> None:
        consumer = MagicMock()
        metadata = MagicMock()
        metadata.topics = {}  # tapline.audit not present
        consumer.list_topics.return_value = metadata
        mock_consumer_cls.return_value = consumer

        resp = client.get("/api/kafka/audit-log/count")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["exists"] is False
        assert body["event_count"] == 0
        consumer.close.assert_called_once()

    @patch("src.visualization.routes.kafka.TopicPartition")
    @patch("src.visualization.routes.kafka.Consumer")
    def test_returns_event_count_when_topic_exists(self, mock_consumer_cls, mock_tp_cls, client) -> None:
        consumer = MagicMock()
        # Build topic with 2 partitions
        topic_meta = MagicMock()
        topic_meta.partitions = {0: MagicMock(), 1: MagicMock()}
        metadata = MagicMock()
        metadata.topics = {"tapline.audit": topic_meta}
        consumer.list_topics.return_value = metadata
        # First partition: low=0, high=10, second: low=2, high=8
        consumer.get_watermark_offsets.side_effect = [(0, 10), (2, 8)]
        mock_consumer_cls.return_value = consumer

        resp = client.get("/api/kafka/audit-log/count")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["exists"] is True
        assert body["event_count"] == 16  # (10-0)+(8-2)
        assert body["partitions"] == 2
        consumer.close.assert_called_once()

    @patch("src.visualization.routes.kafka.Consumer")
    def test_kafka_exception_returns_default(self, mock_consumer_cls, client) -> None:
        consumer = MagicMock()
        consumer.list_topics.side_effect = KafkaException("boom")
        mock_consumer_cls.return_value = consumer

        resp = client.get("/api/kafka/audit-log/count")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["exists"] is False
        assert body["event_count"] == 0
        assert "error" in body

    @patch("src.visualization.routes.kafka.Consumer")
    def test_generic_exception_returns_default(self, mock_consumer_cls, client) -> None:
        mock_consumer_cls.side_effect = RuntimeError("nope")

        resp = client.get("/api/kafka/audit-log/count")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["exists"] is False
        assert "error" in body
