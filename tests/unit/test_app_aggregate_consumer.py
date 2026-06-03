"""Unit tests for src/app/aggregate_consumer.py."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock, patch

from confluent_kafka import KafkaError, KafkaException

import src.app.aggregate_consumer as ac


def _make_kafka_config(**overrides: object) -> MagicMock:
    cfg = MagicMock()
    cfg.bootstrap_servers = "localhost:9092"
    cfg.client_id = "tapline-client"
    cfg.consumer_auto_offset_reset = "earliest"
    cfg.consumer_session_timeout_ms = 10000
    cfg.consumer_heartbeat_interval_ms = 3000
    cfg.consumer_max_poll_interval_ms = 300000
    cfg.topic_aggregates = "tapline.aggregates"
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _make_flink_config(group: str = "tapline-flink") -> MagicMock:
    cfg = MagicMock()
    cfg.kafka_consumer_group = group
    return cfg


def _make_msg(
    value: bytes | None = b'{"foo": "bar"}',
    error: object | None = None,
    partition: int = 0,
    offset: int = 0,
) -> MagicMock:
    msg = MagicMock()
    msg.value.return_value = value
    msg.error.return_value = error
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


class TestCreateConsumer:
    def test_create_consumer_passes_expected_config(self) -> None:
        kafka_config = _make_kafka_config()

        with patch.object(ac, "Consumer") as mock_consumer_cls:
            consumer = ac._create_consumer(kafka_config, "my-group", "my-topic")

        passed = mock_consumer_cls.call_args.args[0]
        assert passed["bootstrap.servers"] == "localhost:9092"
        assert passed["client.id"] == "tapline-client-aggregate-consumer"
        assert passed["group.id"] == "my-group"
        assert passed["auto.offset.reset"] == "earliest"
        assert passed["enable.auto.commit"] is False
        assert passed["session.timeout.ms"] == 10000
        assert passed["heartbeat.interval.ms"] == 3000
        assert passed["max.poll.interval.ms"] == 300000
        consumer.subscribe.assert_called_once_with(["my-topic"])


class TestHandleKafkaError:
    def test_partition_eof_continues(self) -> None:
        consumer = MagicMock()
        state = ac._ConsumerState(consumer)
        err = MagicMock()
        err.code.return_value = KafkaError._PARTITION_EOF  # type: ignore[attr-defined]

        assert ac._handle_kafka_error(err, "topic", state) is True
        assert state.needs_reconnect is False

    def test_unknown_topic_triggers_reconnect(self) -> None:
        consumer = MagicMock()
        state = ac._ConsumerState(consumer)
        err = MagicMock()
        err.code.return_value = KafkaError.UNKNOWN_TOPIC_OR_PART  # type: ignore[attr-defined]

        assert ac._handle_kafka_error(err, "topic", state) is False
        assert state.needs_reconnect is True

    def test_generic_error_continues(self) -> None:
        consumer = MagicMock()
        state = ac._ConsumerState(consumer)
        err = MagicMock()
        err.code.return_value = 999  # arbitrary code

        assert ac._handle_kafka_error(err, "topic", state) is True
        assert state.needs_reconnect is False


class TestConsumerState:
    def test_initial_state(self) -> None:
        consumer = MagicMock()
        state = ac._ConsumerState(consumer)
        assert state.consumer is consumer
        assert state.needs_reconnect is False


class TestParseFlinkTimestamp:
    def test_parses_valid_timestamp(self) -> None:
        result = ac._parse_flink_timestamp("2024-01-15 12:30:00")
        assert result > 0
        assert isinstance(result, int)

    def test_empty_string_returns_zero(self) -> None:
        assert ac._parse_flink_timestamp("") == 0

    def test_invalid_format_returns_zero(self) -> None:
        assert ac._parse_flink_timestamp("not-a-date") == 0

    def test_deterministic(self) -> None:
        ts1 = ac._parse_flink_timestamp("2024-01-15 12:30:00")
        ts2 = ac._parse_flink_timestamp("2024-01-15 12:30:00")
        assert ts1 == ts2


class TestStoreAggregate:
    def test_store_aggregate_writes_to_minio(self) -> None:
        storage = MagicMock()
        storage.store.return_value = MagicMock(full_path="aggregates/ab/cd/hash")

        aggregate = {
            "processor_name": "proc",
            "processor_version": "1.0.0",
            "metric_name": "accuracy",
            "aggregation_mask": 1,
            "window_start": "2024-01-15 12:30:00",
            "window_end": "2024-01-15 12:31:00",
            "metric_count": 5,
            "metric_sum": 4.5,
            "metric_avg": 0.9,
            "metric_min": 0.8,
            "metric_max": 1.0,
        }

        assert ac._store_aggregate(aggregate, storage) is True

        storage.store.assert_called_once()
        kwargs = storage.store.call_args.kwargs
        assert kwargs["bucket"] == "aggregates"
        assert kwargs["mime"] == "application/json"

    def test_store_aggregate_uses_defaults_on_missing_fields(self) -> None:
        storage = MagicMock()
        storage.store.return_value = MagicMock(full_path="aggregates/x/y/z")

        # Empty aggregate; should use defaults
        assert ac._store_aggregate({}, storage) is True
        storage.store.assert_called_once()

    def test_store_aggregate_includes_window_unix(self) -> None:
        storage = MagicMock()
        storage.store.return_value = MagicMock(full_path="aggregates/x/y/z")

        with patch.object(ac, "json_dumps") as mock_dumps:
            mock_dumps.return_value = b"{}"
            ac._store_aggregate(
                {
                    "window_start": "2024-01-15 12:30:00",
                    "window_end": "2024-01-15 12:31:00",
                },
                storage,
            )

        doc = mock_dumps.call_args.args[0]
        assert doc["window_start_unix"] > 0
        assert doc["window_end_unix"] > 0
        assert doc["window_end_unix"] > doc["window_start_unix"]


class TestProcessBatch:
    def test_process_batch_returns_count_of_successful_stores(self) -> None:
        storage = MagicMock()
        storage.store.return_value = MagicMock(full_path="aggregates/x/y/z")

        # Use a real ThreadPoolExecutor with one worker
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=2) as executor:
            count = ac._process_batch(
                [{"metric_name": "a"}, {"metric_name": "b"}, {"metric_name": "c"}],
                storage,
                executor,
            )

        assert count == 3
        assert storage.store.call_count == 3

    def test_process_batch_handles_store_failures(self) -> None:
        storage = MagicMock()
        # One success, one failure, one success
        storage.store.side_effect = [
            MagicMock(full_path="ok1"),
            Exception("storage down"),
            MagicMock(full_path="ok2"),
        ]

        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=1) as executor:
            count = ac._process_batch([{}, {}, {}], storage, executor)

        # Two of three succeeded
        assert count == 2

    def test_process_batch_empty(self) -> None:
        storage = MagicMock()
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=1) as executor:
            count = ac._process_batch([], storage, executor)

        assert count == 0
        storage.store.assert_not_called()


class _TimeStub:
    """Deterministic clock: first call returns 0.0, subsequent calls jump past timeout."""

    def __init__(self, jump_after_call: int = 1, jump_to: float = 100.0) -> None:
        self.calls = 0
        self.jump_after_call = jump_after_call
        self.jump_to = jump_to

    def __call__(self) -> float:
        self.calls += 1
        # batch_start = time.time() captures 0.0; from second call onward, return jump_to
        if self.calls <= self.jump_after_call:
            return 0.0
        return self.jump_to


class TestPollBatch:
    def test_poll_batch_returns_empty_when_no_messages(self) -> None:
        consumer = MagicMock()
        consumer.poll.return_value = None
        state = ac._ConsumerState(consumer)
        kafka_config = _make_kafka_config()

        # batch_start = 0.0; first elapsed = 0.0 (poll once, gets None, continues);
        # second iteration: elapsed = 100.0 -> poll_timeout <= 0 -> break with empty batch
        with patch.object(ac.time, "time", side_effect=_TimeStub(jump_after_call=2)):
            batch = ac._poll_batch(state, "topic", kafka_config, "group")

        assert batch == []

    def test_poll_batch_collects_parsed_messages(self) -> None:
        consumer = MagicMock()
        msg = _make_msg(value=b'{"x": 1}', error=None)
        # First poll returns msg; subsequent polls return None
        consumer.poll.side_effect = [msg] + [None] * 50
        state = ac._ConsumerState(consumer)
        kafka_config = _make_kafka_config()

        with patch.object(ac.time, "time", side_effect=_TimeStub(jump_after_call=3)):
            batch = ac._poll_batch(state, "topic", kafka_config, "group")

        assert batch == [{"x": 1}]

    def test_poll_batch_handles_message_error(self) -> None:
        consumer = MagicMock()
        err = MagicMock()
        err.code.return_value = KafkaError._PARTITION_EOF  # type: ignore[attr-defined]
        bad_msg = _make_msg(error=err)
        consumer.poll.side_effect = [bad_msg] + [None] * 50
        state = ac._ConsumerState(consumer)
        kafka_config = _make_kafka_config()

        with patch.object(ac.time, "time", side_effect=_TimeStub(jump_after_call=3)):
            batch = ac._poll_batch(state, "topic", kafka_config, "group")

        assert batch == []

    def test_poll_batch_unknown_topic_exception_triggers_reconnect(self) -> None:
        consumer = MagicMock()
        consumer.poll.side_effect = KafkaException("UNKNOWN_TOPIC_OR_PART: x")
        state = ac._ConsumerState(consumer)
        kafka_config = _make_kafka_config()

        new_consumer = MagicMock()
        new_consumer.poll.return_value = None

        # Allow enough time-stays-low calls for the reconnect to fire:
        # call 1: batch_start
        # call 2: elapsed -> 0 (loop iter 1; poll raises, needs_reconnect=True, continue)
        # call 3: elapsed -> 0 (loop iter 2; needs_reconnect path -> close + sleep + create;
        #                       batch_start re-assigned to time.time() -> call 4)
        # call 4: batch_start = 0
        # call 5+: jump to 100 to break out
        with (
            patch.object(ac.time, "time", side_effect=_TimeStub(jump_after_call=4)),
            patch.object(ac.time, "sleep") as mock_sleep,
            patch.object(ac, "_create_consumer", return_value=new_consumer) as mock_create,
        ):
            batch = ac._poll_batch(state, "topic", kafka_config, "group")

        assert batch == []
        # After reconnect cycle ran:
        mock_sleep.assert_called()
        mock_create.assert_called_with(kafka_config, "group", "topic")
        consumer.close.assert_called_once()
        # state.consumer was swapped to new_consumer
        assert state.consumer is new_consumer
        assert state.needs_reconnect is False

    def test_poll_batch_skips_unparseable_payload(self) -> None:
        consumer = MagicMock()
        bad_msg = _make_msg(value=b"not-json", error=None)
        consumer.poll.side_effect = [bad_msg] + [None] * 50
        state = ac._ConsumerState(consumer)
        kafka_config = _make_kafka_config()

        with patch.object(ac.time, "time", side_effect=_TimeStub(jump_after_call=3)):
            batch = ac._poll_batch(state, "topic", kafka_config, "group")

        # Bad payload was skipped
        assert batch == []

    def test_poll_batch_skips_none_value(self) -> None:
        consumer = MagicMock()
        none_msg = _make_msg(value=None, error=None)
        consumer.poll.side_effect = [none_msg] + [None] * 50
        state = ac._ConsumerState(consumer)
        kafka_config = _make_kafka_config()

        with patch.object(ac.time, "time", side_effect=_TimeStub(jump_after_call=3)):
            batch = ac._poll_batch(state, "topic", kafka_config, "group")

        assert batch == []


class TestRunAggregateConsumer:
    def test_run_aggregate_consumer_exits_when_stop_event_set(self) -> None:
        kafka_config = _make_kafka_config()
        flink_config = _make_flink_config()
        storage = MagicMock()
        stop_event = threading.Event()
        stop_event.set()  # Pre-set so loop never executes

        with patch.object(ac, "_create_consumer") as mock_create:
            consumer = MagicMock()
            mock_create.return_value = consumer

            ac.run_aggregate_consumer(flink_config, kafka_config, storage, stop_event)

        consumer.close.assert_called_once()
        # Group id derived from flink config + "-minio-sink"
        mock_create.assert_called_once_with(
            kafka_config,
            f"{flink_config.kafka_consumer_group}-minio-sink",
            kafka_config.topic_aggregates,
        )

    def test_run_aggregate_consumer_processes_batch_and_commits(self) -> None:
        kafka_config = _make_kafka_config()
        flink_config = _make_flink_config()
        storage = MagicMock()
        storage.store.return_value = MagicMock(full_path="aggregates/x/y/z")
        stop_event = threading.Event()

        consumer = MagicMock()

        # Have _poll_batch return one batch, then trigger stop
        call_count = {"n": 0}

        def fake_poll_batch(state, topic, kc, gid):  # type: ignore[no-untyped-def]
            call_count["n"] += 1
            if call_count["n"] == 1:
                return [{"metric_name": "a"}]
            stop_event.set()
            return []

        with (
            patch.object(ac, "_create_consumer", return_value=consumer),
            patch.object(ac, "_poll_batch", side_effect=fake_poll_batch),
        ):
            ac.run_aggregate_consumer(flink_config, kafka_config, storage, stop_event)

        # Committed at least once
        consumer.commit.assert_called_with(asynchronous=False)
        consumer.close.assert_called_once()

    def test_run_aggregate_consumer_logs_commit_failure(self) -> None:
        kafka_config = _make_kafka_config()
        flink_config = _make_flink_config()
        storage = MagicMock()
        storage.store.return_value = MagicMock(full_path="aggregates/x/y/z")
        stop_event = threading.Event()

        consumer = MagicMock()
        consumer.commit.side_effect = KafkaException("commit failed")

        call_count = {"n": 0}

        def fake_poll_batch(state, topic, kc, gid):  # type: ignore[no-untyped-def]
            call_count["n"] += 1
            if call_count["n"] == 1:
                return [{"metric_name": "a"}]
            stop_event.set()
            return []

        with (
            patch.object(ac, "_create_consumer", return_value=consumer),
            patch.object(ac, "_poll_batch", side_effect=fake_poll_batch),
        ):
            # Should not raise despite commit failure
            ac.run_aggregate_consumer(flink_config, kafka_config, storage, stop_event)

        consumer.commit.assert_called_with(asynchronous=False)

    def test_run_aggregate_consumer_breaks_when_stop_set_after_poll(self) -> None:
        """If stop_event is set after _poll_batch returns a non-empty batch, break early."""
        kafka_config = _make_kafka_config()
        flink_config = _make_flink_config()
        storage = MagicMock()
        stop_event = threading.Event()

        consumer = MagicMock()

        def fake_poll_batch(state, topic, kc, gid):  # type: ignore[no-untyped-def]
            stop_event.set()
            return [{"x": 1}]

        with (
            patch.object(ac, "_create_consumer", return_value=consumer),
            patch.object(ac, "_poll_batch", side_effect=fake_poll_batch),
        ):
            ac.run_aggregate_consumer(flink_config, kafka_config, storage, stop_event)

        # Batch was discarded; no store and no commit
        storage.store.assert_not_called()
        consumer.commit.assert_not_called()
        consumer.close.assert_called_once()
