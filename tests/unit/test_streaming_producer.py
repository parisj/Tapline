"""Unit tests for EventProducer and DeliveryReport.

The confluent_kafka.Producer is mocked at the
src.streaming.producer.Producer boundary so no broker is needed.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from src.domain.events import EventEnvelope, EventType
from src.streaming.producer import DeliveryReport, EventProducer


@dataclass(frozen=True)
class MockKafkaConfig:
    """Mirror of KafkaConfig with defaults appropriate for tests."""

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "tapline"
    producer_acks: str = "all"
    producer_retries: int = 3
    producer_retry_backoff_ms: int = 100
    producer_linger_ms: int = 5
    producer_batch_size: int = 16384
    producer_compression_type: str = "lz4"
    producer_enable_idempotence: bool = True
    consumer_group_id: str = "tapline-workers"
    consumer_auto_offset_reset: str = "earliest"
    consumer_enable_auto_commit: bool = False
    consumer_session_timeout_ms: int = 30000
    consumer_heartbeat_interval_ms: int = 10000
    consumer_max_poll_interval_ms: int = 300000
    consumer_partition_assignment_strategy: str = "cooperative-sticky"
    topic_jobs: str = "tapline.tasks"
    topic_results: str = "tapline.results"
    topic_metrics: str = "tapline.metrics"
    topic_audit_log: str = "tapline.audit"
    topic_aggregates: str = "tapline.aggregates"
    jobs_partitions: int = 32
    results_partitions: int = 32
    metrics_partitions: int = 32
    audit_log_partitions: int = 1
    aggregates_partitions: int = 16
    schema_registry_url: str = "http://localhost:8085"
    security_protocol: str = "PLAINTEXT"
    ssl_ca_location: str | None = None
    ssl_certificate_location: str | None = None
    ssl_key_location: str | None = None
    ssl_key_password: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    socket_timeout_ms: int = 30000
    socket_connection_setup_timeout_ms: int = 10000


class FakeKafkaMessage:
    """Mimic confluent_kafka.Message API for delivery callbacks."""

    def __init__(
        self,
        topic: str = "tapline.tasks",
        partition: int = 0,
        offset: int = 0,
        key: bytes | None = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key


def _build_producer(
    config: MockKafkaConfig | None = None,
) -> tuple[EventProducer, MagicMock]:
    config = config or MockKafkaConfig()
    with patch("src.streaming.producer.Producer") as producer_cls:
        client = MagicMock()
        producer_cls.return_value = client
        producer = EventProducer(config)
    return producer, client


class TestDeliveryReport:
    def test_increments_delivered_on_success(self) -> None:
        report = DeliveryReport()

        report.on_delivery(None, FakeKafkaMessage())

        assert report.delivered == 1
        assert report.failed == 0

    def test_increments_failed_on_error(self) -> None:
        report = DeliveryReport()
        err = MagicMock()
        err.code = MagicMock(return_value="LEADER_NOT_AVAILABLE")

        report.on_delivery(err, FakeKafkaMessage())

        assert report.failed == 1
        assert report.delivered == 0

    def test_track_send_records_start_time(self) -> None:
        report = DeliveryReport()
        report.track_send("source-1")

        assert "source-1" in report._pending_start_times

    def test_handles_msg_without_topic_gracefully(self) -> None:
        report = DeliveryReport()
        err = MagicMock()
        # Should not raise even when msg is None.
        report.on_delivery(err, None)
        assert report.failed == 1


class TestEventProducerInit:
    def test_init_constructs_kafka_producer_with_config(self) -> None:
        config = MockKafkaConfig()
        with patch("src.streaming.producer.Producer") as producer_cls:
            EventProducer(config)

            producer_cls.assert_called_once()
            producer_config = producer_cls.call_args.args[0]
            assert producer_config["bootstrap.servers"] == config.bootstrap_servers
            assert producer_config["client.id"] == f"{config.client_id}-producer"
            assert producer_config["acks"] == config.producer_acks
            assert producer_config["enable.idempotence"] is True
            assert producer_config["security.protocol"] == "PLAINTEXT"

    def test_init_default_chain_tracker(self) -> None:
        producer, _ = _build_producer()
        assert producer._chain_tracker is not None


class TestPublish:
    def test_publish_serializes_event_to_json_bytes(self) -> None:
        producer, client = _build_producer()
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="src-1",
            payload={"task_id": "t-1"},
        )

        producer.publish("tapline.tasks", event)

        assert client.produce.call_count == 1
        kwargs = client.produce.call_args.kwargs
        assert kwargs["topic"] == "tapline.tasks"
        assert kwargs["key"] == b"src-1"
        # value is JSON-encoded EventEnvelope bytes.
        assert isinstance(kwargs["value"], bytes)
        assert b"TASK_CREATED" in kwargs["value"]
        assert b"src-1" in kwargs["value"]

    def test_publish_uses_explicit_key_when_provided(self) -> None:
        producer, client = _build_producer()
        event = EventEnvelope.create(
            event_type=EventType.METRIC_EMITTED,
            source_id="src-1",
            payload={"metric": "x"},
        )

        producer.publish("tapline.metrics", event, key="processor-name")

        assert client.produce.call_args.kwargs["key"] == b"processor-name"

    def test_publish_attaches_delivery_callback(self) -> None:
        producer, client = _build_producer()
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="src-1",
            payload={"task_id": "t-1"},
        )

        producer.publish("tapline.tasks", event)

        callback = client.produce.call_args.kwargs["callback"]
        assert callback == producer._delivery_report.on_delivery

    def test_publish_encodes_headers(self) -> None:
        producer, client = _build_producer()
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="src-1",
            payload={"task_id": "t-1"},
        )

        producer.publish("tapline.tasks", event, headers={"trace": "abc"})

        headers = client.produce.call_args.kwargs["headers"]
        assert headers is not None
        # Headers are list of (str, bytes) tuples.
        keys = [k for k, _ in headers]
        values = [v for _, v in headers]
        assert "trace" in keys
        assert b"abc" in values

    def test_publish_polls_after_produce(self) -> None:
        producer, client = _build_producer()
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="src-1",
            payload={"task_id": "t-1"},
        )

        producer.publish("tapline.tasks", event)

        client.poll.assert_called()

    def test_publish_updates_chain_tracker(self) -> None:
        producer, _ = _build_producer()
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="src-1",
            payload={"task_id": "t-1"},
        )

        producer.publish("tapline.tasks", event)

        assert producer._chain_tracker.get_prev_hash("src-1") == event.content_hash

    def test_publish_after_close_raises(self) -> None:
        producer, client = _build_producer()
        client.flush.return_value = 0  # close calls flush -> int comparison
        producer.close()

        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="src-1",
            payload={"task_id": "t-1"},
        )
        with pytest.raises(RuntimeError, match="Producer is closed"):
            producer.publish("tapline.tasks", event)


class TestPublishWithAudit:
    def test_dual_writes_to_topic_and_audit_log(self) -> None:
        producer, client = _build_producer()
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="src-1",
            payload={"task_id": "t-1"},
        )

        producer.publish_with_audit("tapline.tasks", event)

        topics_called = [call.kwargs["topic"] for call in client.produce.call_args_list]
        assert "tapline.tasks" in topics_called
        assert "tapline.audit" in topics_called

    def test_audit_has_source_topic_header(self) -> None:
        producer, client = _build_producer()
        event = EventEnvelope.create(
            event_type=EventType.TASK_CREATED,
            source_id="src-1",
            payload={"task_id": "t-1"},
        )

        producer.publish_with_audit("tapline.tasks", event, key="partition-k")

        audit_call = next(call for call in client.produce.call_args_list if call.kwargs["topic"] == "tapline.audit")
        header_dict = dict(audit_call.kwargs["headers"])
        assert header_dict["source-topic"] == b"tapline.tasks"
        assert header_dict["source-partition-key"] == b"partition-k"


class TestConvenienceEvents:
    def test_publish_job_created_writes_to_jobs_topic(self) -> None:
        producer, client = _build_producer()

        event = producer.publish_job_created(
            source_id="src-1",
            task_id="task-1",
            directory_key="dir-1",
            path="/foo/bar.png",
            fingerprint="fp-1",
        )

        assert event.event_type == EventType.TASK_CREATED
        assert event.payload["task_id"] == "task-1"
        # Two writes: tapline.tasks + audit log.
        assert client.produce.call_count == 2
        topics = {c.kwargs["topic"] for c in client.produce.call_args_list}
        assert topics == {"tapline.tasks", "tapline.audit"}

    def test_publish_metric_uses_processor_name_as_key(self) -> None:
        producer, client = _build_producer()

        event = producer.publish_metric_emitted(
            source_id="src-1",
            task_id="task-1",
            processor_name="proc-A",
            processor_version="1.0",
            metric_name="m",
            value=1.0,
            aggregation_mask=0,
        )

        assert event.event_type == EventType.METRIC_EMITTED
        # publish() (no audit) -> single produce call to tapline.metrics.
        assert client.produce.call_count == 1
        assert client.produce.call_args.kwargs["topic"] == "tapline.metrics"
        assert client.produce.call_args.kwargs["key"] == b"proc-A"

    def test_publish_aggregate_parses_iso_timestamps(self) -> None:
        producer, client = _build_producer()

        event = producer.publish_aggregate_produced(
            source_id="src-1",
            processor_name="proc-A",
            processor_version="1.0",
            metric_name="m",
            window_start="2024-01-01T00:00:00",
            window_end="2024-01-01T01:00:00",
            count=5,
            summary={"mean": 1.0},
        )

        assert event.event_type == EventType.AGGREGATE_COMPUTED
        assert event.payload["window_start_unix"] > 0
        assert event.payload["window_end_unix"] > event.payload["window_start_unix"]
        assert client.produce.call_args.kwargs["topic"] == "tapline.aggregates"

    def test_publish_aggregate_handles_invalid_timestamps(self) -> None:
        producer, _ = _build_producer()

        event = producer.publish_aggregate_produced(
            source_id="src-1",
            processor_name="proc",
            processor_version="1",
            metric_name="m",
            window_start="not-a-date",
            window_end="also-bad",
            count=0,
            summary={},
        )

        assert event.payload["window_start_unix"] == 0.0
        assert event.payload["window_end_unix"] == 0.0


class TestFlushPollClose:
    def test_flush_returns_remaining(self) -> None:
        producer, client = _build_producer()
        client.flush.return_value = 0

        remaining = producer.flush(timeout=1.0)

        client.flush.assert_called_once_with(timeout=1.0)
        assert remaining == 0

    def test_poll_returns_events_processed(self) -> None:
        producer, client = _build_producer()
        client.poll.return_value = 3

        result = producer.poll(timeout=0.5)

        assert result == 3

    def test_close_is_idempotent(self) -> None:
        producer, client = _build_producer()
        client.flush.return_value = 0

        producer.close()
        producer.close()

        # Flush should be invoked at most once during close (idempotent).
        assert client.flush.call_count == 1

    def test_close_logs_remaining_warning(self) -> None:
        producer, client = _build_producer()
        client.flush.return_value = 5  # simulate unsent messages

        # Should not raise.
        producer.close()

    def test_delivery_stats_returns_tuple(self) -> None:
        producer, _ = _build_producer()
        assert producer.delivery_stats == (0, 0)

    def test_context_manager_closes(self) -> None:
        with patch("src.streaming.producer.Producer") as producer_cls:
            client = MagicMock()
            client.flush.return_value = 0
            producer_cls.return_value = client

            with EventProducer(MockKafkaConfig()) as producer:
                assert producer._closed is False

            assert client.flush.call_count == 1
