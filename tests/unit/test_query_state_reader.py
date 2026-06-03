"""Unit tests for StateReader.

StateReader composes a MinioStorageService and a Kafka EventConsumer.
Both collaborators are mocked at the boundary: the storage is replaced
with a MagicMock and the EventConsumer is patched at
src.query.state_reader.EventConsumer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.domain.events import EventEnvelope, EventType
from src.query.state_reader import ArtifactInfo, JobStatus, StateReader


@dataclass(frozen=True)
class MockKafkaConfig:
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "tapline"
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


def _make_storage() -> MagicMock:
    storage = MagicMock()
    storage.buckets = {
        "artifacts": "artifacts",
        "inputs": "inputs",
        "aggregates": "aggregates",
        "metric-values": "metric-values",
    }
    return storage


def _create_event(event_type: EventType, payload: dict[str, Any], source_id: str = "src") -> EventEnvelope:
    return EventEnvelope.create(
        event_type=event_type,
        source_id=source_id,
        payload=payload,
    )


class TestJobStatusDataclass:
    def test_minimal_construction(self) -> None:
        status = JobStatus(
            task_id="t",
            status="created",
            directory_key="d",
            path="/p",
            fingerprint="fp",
        )

        assert status.processor_name is None
        assert status.artifact_refs is None


class TestArtifactInfoDataclass:
    def test_fields_preserved(self) -> None:
        ts = datetime.now()
        info = ArtifactInfo(
            content_hash="h",
            bucket="artifacts",
            key="ab/cd/h",
            size=100,
            mime="application/octet-stream",
            stored_at=ts,
        )

        assert info.content_hash == "h"
        assert info.stored_at == ts


class TestGetJobStatus:
    def test_returns_none_for_unknown_task(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())

        assert reader.get_job_status("missing") is None

    def test_returns_processed_state(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        event = _create_event(
            EventType.TASK_CREATED,
            {
                "task_id": "task-1",
                "directory_key": "dir-1",
                "path": "/p",
                "fingerprint": "fp",
            },
        )
        reader._process_event(event)

        status = reader.get_job_status("task-1")

        assert status is not None
        assert status.task_id == "task-1"
        assert status.status == "created"


class TestListJobs:
    def _seed(self, reader: StateReader, n: int) -> None:
        base = datetime(2024, 1, 1, 0, 0, 0)
        for i in range(n):
            reader._job_states[f"task-{i}"] = JobStatus(
                task_id=f"task-{i}",
                status="created" if i % 2 == 0 else "completed",
                directory_key="d",
                path="/p",
                fingerprint="fp",
                created_at=base + timedelta(minutes=i),
            )

    def test_empty(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        assert reader.list_jobs() == []

    def test_returns_sorted_descending_by_created(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        self._seed(reader, 3)

        result = reader.list_jobs()

        assert [j.task_id for j in result] == ["task-2", "task-1", "task-0"]

    def test_filter_by_status(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        self._seed(reader, 4)

        result = reader.list_jobs(status="completed")

        assert {j.status for j in result} == {"completed"}

    def test_limit(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        self._seed(reader, 10)

        result = reader.list_jobs(limit=3)

        assert len(result) == 3


class TestGetArtifact:
    def test_returns_bytes_from_storage(self) -> None:
        storage = _make_storage()
        storage.retrieve_by_hash.return_value = b"payload"
        reader = StateReader(MockKafkaConfig(), storage)

        result = reader.get_artifact("hash-1")

        assert result == b"payload"
        storage.retrieve_by_hash.assert_called_once_with(
            bucket="artifacts",
            content_hash="hash-1",
        )

    def test_returns_none_on_filenotfound(self) -> None:
        storage = _make_storage()
        storage.retrieve_by_hash.side_effect = FileNotFoundError()
        reader = StateReader(MockKafkaConfig(), storage)

        assert reader.get_artifact("missing") is None

    def test_other_errors_propagate(self) -> None:
        storage = _make_storage()
        storage.retrieve_by_hash.side_effect = RuntimeError("boom")
        reader = StateReader(MockKafkaConfig(), storage)

        with pytest.raises(RuntimeError, match="boom"):
            reader.get_artifact("any")


class TestArtifactIndex:
    def test_get_artifact_info_missing(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        assert reader.get_artifact_info("missing") is None

    def test_get_artifact_info_returns_indexed(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        ts = datetime.now()
        reader._artifact_index["h"] = ArtifactInfo(
            content_hash="h",
            bucket="artifacts",
            key="k",
            size=1,
            mime="x",
            stored_at=ts,
        )

        info = reader.get_artifact_info("h")

        assert info is not None
        assert info.content_hash == "h"

    def test_list_artifacts_sorted_descending_by_stored_at(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        base = datetime(2024, 1, 1)
        for i in range(3):
            reader._artifact_index[f"h{i}"] = ArtifactInfo(
                content_hash=f"h{i}",
                bucket="b",
                key="k",
                size=i,
                mime="x",
                stored_at=base + timedelta(seconds=i),
            )

        result = reader.list_artifacts()

        assert [a.content_hash for a in result] == ["h2", "h1", "h0"]

    def test_list_artifacts_respects_limit(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        for i in range(5):
            reader._artifact_index[f"h{i}"] = ArtifactInfo(
                content_hash=f"h{i}",
                bucket="b",
                key="k",
                size=i,
                mime="x",
                stored_at=datetime(2024, 1, 1) + timedelta(seconds=i),
            )

        result = reader.list_artifacts(limit=2)

        assert len(result) == 2


class TestProcessEvent:
    def test_task_created(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        event = _create_event(
            EventType.TASK_CREATED,
            {
                "task_id": "t-1",
                "directory_key": "dir",
                "path": "/file.png",
                "fingerprint": "fp",
            },
        )
        reader._process_event(event)

        status = reader.get_job_status("t-1")
        assert status is not None
        assert status.status == "created"
        assert status.directory_key == "dir"
        assert status.path == "/file.png"
        assert status.fingerprint == "fp"
        assert status.created_at == event.timestamp

    def test_task_started_updates_existing(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        created = _create_event(
            EventType.TASK_CREATED,
            {"task_id": "t-1", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
        )
        reader._process_event(created)

        started = _create_event(
            EventType.TASK_STARTED,
            {"task_id": "t-1", "processor_name": "proc", "processor_version": "1.0"},
        )
        reader._process_event(started)

        status = reader.get_job_status("t-1")
        assert status is not None
        assert status.status == "started"
        assert status.processor_name == "proc"
        assert status.processor_version == "1.0"
        assert status.started_at == started.timestamp
        assert status.created_at == created.timestamp

    def test_task_started_ignored_when_no_prior_create(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        started = _create_event(
            EventType.TASK_STARTED,
            {"task_id": "missing", "processor_name": "p", "processor_version": "1"},
        )
        reader._process_event(started)

        assert reader.get_job_status("missing") is None

    def test_task_completed_sets_duration(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        reader._process_event(
            _create_event(
                EventType.TASK_CREATED,
                {"task_id": "t-1", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
            ),
        )
        completed = _create_event(
            EventType.TASK_COMPLETED,
            {
                "task_id": "t-1",
                "processor_name": "proc",
                "processor_version": "1",
                "duration_ms": 123.4,
            },
        )
        reader._process_event(completed)

        status = reader.get_job_status("t-1")
        assert status is not None
        assert status.status == "completed"
        assert status.duration_ms == 123.4
        assert status.completed_at == completed.timestamp

    def test_task_failed_sets_error(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        reader._process_event(
            _create_event(
                EventType.TASK_CREATED,
                {"task_id": "t-1", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
            ),
        )
        failed = _create_event(
            EventType.TASK_FAILED,
            {
                "task_id": "t-1",
                "processor_name": "proc",
                "processor_version": "1",
                "error": "boom",
                "error_type": "RuntimeError",
            },
        )
        reader._process_event(failed)

        status = reader.get_job_status("t-1")
        assert status is not None
        assert status.status == "failed"
        assert status.error == "boom"

    def test_result_produced_attaches_artifact_refs(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        reader._process_event(
            _create_event(
                EventType.TASK_CREATED,
                {"task_id": "t-1", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
            ),
        )
        result = _create_event(
            EventType.RESULT_PRODUCED,
            {
                "task_id": "t-1",
                "processor_name": "proc",
                "processor_version": "1",
                "metric_count": 2,
                "artifact_refs": ["ref-a", "ref-b"],
            },
        )
        reader._process_event(result)

        status = reader.get_job_status("t-1")
        assert status is not None
        assert status.artifact_refs == ["ref-a", "ref-b"]

    def test_result_produced_ignored_when_no_prior_state(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        reader._process_event(
            _create_event(
                EventType.RESULT_PRODUCED,
                {
                    "task_id": "missing",
                    "processor_name": "p",
                    "processor_version": "1",
                    "metric_count": 0,
                    "artifact_refs": [],
                },
            ),
        )

        assert reader.get_job_status("missing") is None

    def test_artifact_stored_indexes_metadata(self) -> None:
        reader = StateReader(MockKafkaConfig(), _make_storage())
        event = _create_event(
            EventType.ARTIFACT_STORED,
            {
                "content_hash": "h-1",
                "bucket": "artifacts",
                "key": "ab/cd/h-1",
                "size": 1024,
                "mime": "image/png",
            },
        )
        reader._process_event(event)

        info = reader.get_artifact_info("h-1")
        assert info is not None
        assert info.bucket == "artifacts"
        assert info.size == 1024
        assert info.mime == "image/png"


class TestConsumeEvents:
    @patch("src.query.state_reader.EventConsumer")
    def test_uses_default_topics_when_none(self, consumer_cls: MagicMock) -> None:
        # Iterating the consumer yields no events.
        consumer = MagicMock()
        consumer.__iter__.return_value = iter([])
        consumer_cls.return_value = consumer

        reader = StateReader(MockKafkaConfig(), _make_storage())
        list(reader.consume_events())

        # Consumer constructor receives both jobs and results topics by default.
        topics = consumer_cls.call_args.kwargs["topics"]
        assert "tapline.tasks" in topics
        assert "tapline.results" in topics
        consumer.close.assert_called_once()

    @patch("src.query.state_reader.EventConsumer")
    def test_yields_events_and_processes_them(self, consumer_cls: MagicMock) -> None:
        events = [
            _create_event(
                EventType.TASK_CREATED,
                {"task_id": "t-1", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
            ),
        ]
        consumer = MagicMock()
        consumer.__iter__.return_value = iter(events)
        consumer_cls.return_value = consumer

        reader = StateReader(MockKafkaConfig(), _make_storage())
        yielded = list(reader.consume_events(topics=["x"]))

        assert len(yielded) == 1
        # Side effect: state was updated.
        assert reader.get_job_status("t-1") is not None
        consumer.close.assert_called_once()

    @patch("src.query.state_reader.EventConsumer")
    def test_respects_max_events(self, consumer_cls: MagicMock) -> None:
        events = [
            _create_event(
                EventType.TASK_CREATED,
                {"task_id": f"t-{i}", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
            )
            for i in range(5)
        ]
        consumer = MagicMock()
        consumer.__iter__.return_value = iter(events)
        consumer_cls.return_value = consumer

        reader = StateReader(MockKafkaConfig(), _make_storage())
        yielded = list(reader.consume_events(max_events=2))

        assert len(yielded) == 2
        consumer.close.assert_called_once()

    @patch("src.query.state_reader.EventConsumer")
    def test_closes_consumer_on_exception(self, consumer_cls: MagicMock) -> None:
        def bad_iter() -> Any:
            yield _create_event(
                EventType.TASK_CREATED,
                {
                    "task_id": "t",
                    "directory_key": "d",
                    "path": "/p",
                    "fingerprint": "fp",
                },
            )
            msg = "downstream failure"
            raise RuntimeError(msg)

        consumer = MagicMock()
        consumer.__iter__.return_value = bad_iter()
        consumer_cls.return_value = consumer

        reader = StateReader(MockKafkaConfig(), _make_storage())

        with pytest.raises(RuntimeError, match="downstream failure"):
            list(reader.consume_events())

        consumer.close.assert_called_once()


class TestRebuildState:
    @patch("src.query.state_reader.EventConsumer")
    def test_returns_count(self, consumer_cls: MagicMock) -> None:
        events = [
            _create_event(
                EventType.TASK_CREATED,
                {"task_id": f"t-{i}", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
            )
            for i in range(3)
        ]
        consumer = MagicMock()
        consumer.__iter__.return_value = iter(events)
        consumer_cls.return_value = consumer

        reader = StateReader(MockKafkaConfig(), _make_storage())

        count = reader.rebuild_state()

        assert count == 3
        assert len(reader._job_states) == 3
