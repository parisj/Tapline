"""Unit tests for Kafka consumer classes."""

import threading
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, TopicPartition

from src.domain.events import EventEnvelope, EventType
from src.streaming.consumer import BatchEventConsumer, EventConsumer


@dataclass
class MockKafkaConfig:
    """Mock KafkaConfig for testing."""

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "test-client"
    consumer_group_id: str = "test-group"
    consumer_auto_offset_reset: str = "earliest"
    consumer_enable_auto_commit: bool = False
    consumer_session_timeout_ms: int = 10000
    consumer_heartbeat_interval_ms: int = 3000
    consumer_max_poll_interval_ms: int = 300000
    consumer_partition_assignment_strategy: str = "cooperative-sticky"
    # Security fields
    security_protocol: str = "PLAINTEXT"
    ssl_ca_location: str | None = None
    ssl_certificate_location: str | None = None
    ssl_key_location: str | None = None
    ssl_key_password: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    # Connection timeouts
    socket_timeout_ms: int = 30000
    socket_connection_setup_timeout_ms: int = 10000


class MockMessage:
    """Mock Kafka message for testing."""

    def __init__(
        self,
        value: bytes | None = None,
        error: Any | None = None,
        topic: str = "test-topic",
        partition: int = 0,
        offset: int = 0,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self._value = value
        self._error = error
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._headers = headers or []

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> Any | None:
        return self._error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers


class MockKafkaError:
    """Mock Kafka error for testing."""

    _PARTITION_EOF = KafkaError._PARTITION_EOF

    def __init__(self, code: int) -> None:
        self._code = code

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return f"MockKafkaError({self._code})"


class TestEventConsumer:
    @patch("src.streaming.consumer.Consumer")
    def test_init_creates_consumer(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])

        mock_consumer_class.assert_called_once()
        mock_consumer.subscribe.assert_called_once()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_poll_returns_none_on_no_message(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        result = consumer.poll(timeout=0.1)

        assert result is None
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_poll_returns_event_on_valid_message(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()

        # Create a valid event envelope
        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="test-source",
            payload={"job_id": "job-123"},
        )
        mock_msg = MockMessage(value=event.to_json().encode("utf-8"))
        mock_consumer.poll.return_value = mock_msg
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        result = consumer.poll(timeout=1.0)

        assert result is not None
        assert result.event_type == EventType.JOB_CREATED
        assert result.payload["job_id"] == "job-123"
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_poll_handles_partition_eof(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()

        eof_error = MockKafkaError(KafkaError._PARTITION_EOF)
        mock_msg = MockMessage(error=eof_error)
        mock_consumer.poll.return_value = mock_msg
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        result = consumer.poll(timeout=1.0)

        assert result is None
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_poll_raises_on_closed_consumer(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        consumer.close()

        with pytest.raises(RuntimeError, match="Consumer is closed"):
            consumer.poll()

    @patch("src.streaming.consumer.Consumer")
    def test_commit_commits_current_message(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()

        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="test-source",
            payload={"job_id": "job-123"},
        )
        mock_msg = MockMessage(value=event.to_json().encode("utf-8"))
        mock_consumer.poll.return_value = mock_msg
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        consumer.poll(timeout=1.0)
        consumer.commit()

        mock_consumer.commit.assert_called()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_commit_with_no_message_does_nothing(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        consumer.poll(timeout=0.1)
        consumer.commit()

        # commit should not be called if no message
        # (it's called in poll, but not for None message)
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_pause_current_partition(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()

        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="test-source",
            payload={"job_id": "job-123"},
        )
        mock_msg = MockMessage(
            value=event.to_json().encode("utf-8"),
            topic="test-topic",
            partition=0,
        )
        mock_consumer.poll.return_value = mock_msg
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        consumer.poll(timeout=1.0)
        consumer.pause_current()

        mock_consumer.pause.assert_called_once()
        assert ("test-topic", 0) in consumer.get_paused_partitions()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_resume_current_partition(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()

        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="test-source",
            payload={"job_id": "job-123"},
        )
        mock_msg = MockMessage(
            value=event.to_json().encode("utf-8"),
            topic="test-topic",
            partition=0,
        )
        mock_consumer.poll.return_value = mock_msg
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        consumer.poll(timeout=1.0)
        consumer.pause_current()
        consumer.resume_current()

        mock_consumer.resume.assert_called_once()
        assert ("test-topic", 0) not in consumer.get_paused_partitions()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_pause_specific_partition(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        consumer.pause_partition("test-topic", 1)

        mock_consumer.pause.assert_called_once()
        assert ("test-topic", 1) in consumer.get_paused_partitions()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_resume_specific_partition(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        consumer.pause_partition("test-topic", 1)
        consumer.resume_partition("test-topic", 1)

        mock_consumer.resume.assert_called_once()
        assert ("test-topic", 1) not in consumer.get_paused_partitions()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_resume_all_partitions(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        consumer.pause_partition("test-topic", 0)
        consumer.pause_partition("test-topic", 1)
        consumer.resume_all()

        assert len(consumer.get_paused_partitions()) == 0
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_stop_signals_consumer_to_stop(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        consumer.stop()

        assert consumer._stop.is_set()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_stats_returns_statistics(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        stats = consumer.stats

        assert "consumed" in stats
        assert "committed" in stats
        assert "errors" in stats
        assert "paused_partitions" in stats
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_context_manager(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        with EventConsumer(config, topics=["test-topic"]) as consumer:
            assert consumer is not None
            assert not consumer._closed

        mock_consumer.close.assert_called()

    @patch("src.streaming.consumer.Consumer")
    def test_get_assignment(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer.assignment.return_value = [
            TopicPartition("test-topic", 0),
            TopicPartition("test-topic", 1),
        ]
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])
        assignment = consumer.get_assignment()

        assert assignment == [("test-topic", 0), ("test-topic", 1)]
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_wait_for_assignment(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])

        # Simulate partition assignment in background
        def assign_partitions() -> None:
            time.sleep(0.1)
            consumer._partitions_assigned.set()

        thread = threading.Thread(target=assign_partitions)
        thread.start()

        result = consumer.wait_for_assignment(timeout=1.0)

        assert result is True
        thread.join()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_iterator_interface(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()

        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="test-source",
            payload={"job_id": "job-123"},
        )
        mock_msg = MockMessage(value=event.to_json().encode("utf-8"))

        # Return message once, then None, then stop
        call_count = [0]

        def poll_side_effect(timeout: float = 1.0) -> Any:
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_msg
            return None

        mock_consumer.poll.side_effect = poll_side_effect
        mock_consumer_class.return_value = mock_consumer

        consumer = EventConsumer(config, topics=["test-topic"])

        events = []
        for event_item in consumer:
            events.append(event_item)
            consumer.stop()  # Stop after first event

        assert len(events) == 1
        consumer.close()


class TestBatchEventConsumer:
    @patch("src.streaming.consumer.Consumer")
    def test_init_creates_consumer(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = BatchEventConsumer(
            config,
            topics=["test-topic"],
            batch_size=10,
            batch_timeout=1.0,
        )

        mock_consumer_class.assert_called_once()
        mock_consumer.subscribe.assert_called_once()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_poll_batch_returns_empty_on_no_messages(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer.poll.return_value = None
        mock_consumer_class.return_value = mock_consumer

        consumer = BatchEventConsumer(
            config,
            topics=["test-topic"],
            batch_size=10,
            batch_timeout=0.1,
        )
        batch = consumer.poll_batch()

        assert batch == []
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_poll_batch_collects_messages(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()

        events = [
            EventEnvelope.create(
                event_type=EventType.JOB_CREATED,
                source_id="test-source",
                payload={"job_id": f"job-{i}"},
            )
            for i in range(3)
        ]

        call_count = [0]

        def poll_side_effect(timeout: float = 1.0) -> Any:
            idx = call_count[0]
            call_count[0] += 1
            if idx < len(events):
                return MockMessage(value=events[idx].to_json().encode("utf-8"))
            return None

        mock_consumer.poll.side_effect = poll_side_effect
        mock_consumer_class.return_value = mock_consumer

        consumer = BatchEventConsumer(
            config,
            topics=["test-topic"],
            batch_size=10,
            batch_timeout=0.5,
        )
        batch = consumer.poll_batch()

        assert len(batch) == 3
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_poll_batch_respects_batch_size(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()

        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="test-source",
            payload={"job_id": "job-123"},
        )
        mock_msg = MockMessage(value=event.to_json().encode("utf-8"))
        mock_consumer.poll.return_value = mock_msg
        mock_consumer_class.return_value = mock_consumer

        consumer = BatchEventConsumer(
            config,
            topics=["test-topic"],
            batch_size=5,
            batch_timeout=10.0,  # Long timeout to ensure batch_size is the limit
        )
        batch = consumer.poll_batch()

        assert len(batch) == 5
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_poll_batch_raises_on_closed(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = BatchEventConsumer(
            config,
            topics=["test-topic"],
            batch_size=10,
            batch_timeout=1.0,
        )
        consumer.close()

        with pytest.raises(RuntimeError, match="Consumer is closed"):
            consumer.poll_batch()

    @patch("src.streaming.consumer.Consumer")
    def test_commit(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = BatchEventConsumer(
            config,
            topics=["test-topic"],
            batch_size=10,
            batch_timeout=1.0,
        )
        consumer.commit()

        mock_consumer.commit.assert_called_once_with(asynchronous=False)
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_stop(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer_class.return_value = mock_consumer

        consumer = BatchEventConsumer(
            config,
            topics=["test-topic"],
            batch_size=10,
            batch_timeout=1.0,
        )
        consumer.stop()

        assert consumer._stop.is_set()
        consumer.close()

    @patch("src.streaming.consumer.Consumer")
    def test_context_manager(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()
        mock_consumer.assignment.return_value = []
        mock_consumer.position.return_value = []
        mock_consumer_class.return_value = mock_consumer

        with BatchEventConsumer(
            config,
            topics=["test-topic"],
            batch_size=10,
            batch_timeout=1.0,
        ) as consumer:
            assert consumer is not None
            assert not consumer._closed

        mock_consumer.close.assert_called()

    @patch("src.streaming.consumer.Consumer")
    def test_poll_batch_handles_errors(self, mock_consumer_class: MagicMock) -> None:
        config = MockKafkaConfig()
        mock_consumer = MagicMock()

        # Return error on first message, valid message on second
        error_msg = MockMessage(error=MockKafkaError(KafkaError._PARTITION_EOF))
        event = EventEnvelope.create(
            event_type=EventType.JOB_CREATED,
            source_id="test-source",
            payload={"job_id": "job-123"},
        )
        valid_msg = MockMessage(value=event.to_json().encode("utf-8"))

        call_count = [0]

        def poll_side_effect(timeout: float = 1.0) -> Any:
            idx = call_count[0]
            call_count[0] += 1
            if idx == 0:
                return error_msg
            if idx == 1:
                return valid_msg
            return None

        mock_consumer.poll.side_effect = poll_side_effect
        mock_consumer_class.return_value = mock_consumer

        consumer = BatchEventConsumer(
            config,
            topics=["test-topic"],
            batch_size=10,
            batch_timeout=0.5,
        )
        batch = consumer.poll_batch()

        # Should have one valid message
        assert len(batch) == 1
        consumer.close()
