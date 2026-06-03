"""Unit tests for src.flink.jobs.job_state_tracker.

PyFlink is an optional dependency. To test the pure-Python logic in this
module without requiring apache-flink at runtime, we install minimal
stand-in modules into sys.modules BEFORE importing the module under test.

The stubs supply just enough surface area for the module to import: Row,
Types, KeyedProcessFunction, RuntimeContext, ValueStateDescriptor and
StreamExecutionEnvironment. We then exercise:

- JobStatus enum
- JobState dataclass round-trip (to_dict / from_dict)
- JobStateProcessor.process_element across all event types
- The _MAX_EVENT_HISTORY truncation
- parse_job_event happy path and error path
- JobStateTrackerJob.setup_environment + build_job
"""

from __future__ import annotations

import json
import sys
import types
from datetime import UTC, datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest


def _make_pyflink_stubs() -> dict[str, Any]:
    """Build a dict of stub pyflink modules and return what was installed."""

    class _Row:
        def __init__(self, **kwargs: Any) -> None:
            self.__dict__.update(kwargs)

        def __eq__(self, other: object) -> bool:
            return isinstance(other, _Row) and self.__dict__ == other.__dict__

        __hash__ = None  # type: ignore[assignment]

    class _TypeFactory:
        @staticmethod
        def STRING() -> str:  # noqa: N802
            return "STRING"

        @staticmethod
        def ROW_NAMED(names: list[str], types_: list[Any]) -> dict[str, Any]:  # noqa: N802
            return {"names": names, "types": types_}

    class _KeyedProcessFunction:
        class Context:
            pass

    class _RuntimeContext:
        pass

    class _ValueStateDescriptor:
        def __init__(self, name: str, type_info: Any) -> None:
            self.name = name
            self.type_info = type_info

    class _StreamExecutionEnvironment:
        @staticmethod
        def get_execution_environment() -> Any:
            return MagicMock(name="MockExecutionEnv")

    pyflink = types.ModuleType("pyflink")
    pyflink_common = types.ModuleType("pyflink.common")
    pyflink_common.Row = _Row
    pyflink_common.Types = _TypeFactory

    pyflink_datastream = types.ModuleType("pyflink.datastream")
    pyflink_datastream.StreamExecutionEnvironment = _StreamExecutionEnvironment

    pyflink_datastream_functions = types.ModuleType("pyflink.datastream.functions")
    pyflink_datastream_functions.KeyedProcessFunction = _KeyedProcessFunction
    pyflink_datastream_functions.RuntimeContext = _RuntimeContext

    pyflink_datastream_state = types.ModuleType("pyflink.datastream.state")
    pyflink_datastream_state.ValueStateDescriptor = _ValueStateDescriptor

    installed = {
        "pyflink": pyflink,
        "pyflink.common": pyflink_common,
        "pyflink.datastream": pyflink_datastream,
        "pyflink.datastream.functions": pyflink_datastream_functions,
        "pyflink.datastream.state": pyflink_datastream_state,
    }
    for name, mod in installed.items():
        sys.modules[name] = mod
    return installed


# Install stubs and import once for the whole test module.
_STUBS = _make_pyflink_stubs()
from src.flink.jobs import job_state_tracker as tracker_mod  # noqa: E402
from src.flink.jobs.job_state_tracker import (  # noqa: E402
    JobState,
    JobStateProcessor,
    JobStateTrackerJob,
    JobStatus,
    parse_job_event,
)


class TestJobStatus:
    def test_values(self) -> None:
        assert JobStatus.CREATED.value == "created"
        assert JobStatus.STARTED.value == "started"
        assert JobStatus.COMPLETED.value == "completed"
        assert JobStatus.FAILED.value == "failed"

    def test_string_subclass(self) -> None:
        assert JobStatus.CREATED == "created"


class TestJobState:
    def _make_state(self) -> JobState:
        return JobState(
            task_id="t1",
            directory_key="dir/key",
            path="/path",
            fingerprint="fp",
            status=JobStatus.STARTED,
            processor_name="proc",
            processor_version="1.2.3",
            created_at=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
            started_at=datetime(2024, 1, 1, 0, 0, 1, tzinfo=UTC),
            completed_at=None,
            error=None,
            duration_ms=None,
            events=[{"type": "JOB_CREATED", "timestamp": "2024-01-01T00:00:00+00:00"}],
        )

    def test_to_dict_contains_event_count(self) -> None:
        s = self._make_state()
        d = s.to_dict()
        assert d["task_id"] == "t1"
        assert d["status"] == "started"
        assert d["event_count"] == 1
        assert d["created_at"] == "2024-01-01T00:00:00+00:00"
        assert d["completed_at"] is None

    def test_round_trip_via_dict(self) -> None:
        s = self._make_state()
        # from_dict expects an "events" key (it's optional via .get)
        data = s.to_dict()
        data["events"] = s.events
        restored = JobState.from_dict(data)
        assert restored.task_id == s.task_id
        assert restored.status == JobStatus.STARTED
        assert restored.processor_name == "proc"
        assert restored.created_at == s.created_at
        assert restored.started_at == s.started_at
        assert restored.completed_at is None
        assert restored.events == s.events

    def test_from_dict_optional_fields_default(self) -> None:
        minimal: dict[str, Any] = {
            "task_id": "t",
            "directory_key": "d",
            "path": "/p",
            "fingerprint": "f",
            "status": "created",
        }
        s = JobState.from_dict(minimal)
        assert s.processor_name is None
        assert s.created_at is None
        assert s.events == []


class _FakeValueState:
    """Test double for Flink ValueState."""

    def __init__(self) -> None:
        self._v: str | None = None

    def value(self) -> str | None:
        return self._v

    def update(self, v: str) -> None:
        self._v = v


class _FakeRuntimeContext:
    def __init__(self) -> None:
        self.state = _FakeValueState()

    def get_state(self, _descriptor: Any) -> _FakeValueState:
        return self.state


class _Row:
    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)


class TestJobStateProcessor:
    def _processor(self) -> tuple[JobStateProcessor, _FakeValueState]:
        proc = JobStateProcessor()
        ctx = _FakeRuntimeContext()
        proc.open(ctx)
        return proc, ctx.state

    def _event(self, event_type: str, payload: dict[str, Any], ts: str | None = None) -> Any:
        return tracker_mod.Row(
            task_id=payload.get("task_id", ""),
            event_type=event_type,
            payload=json.dumps(payload),
            timestamp=ts or "2024-01-01T00:00:00+00:00",
        )

    def test_created_event_initializes_state(self) -> None:
        proc, state = self._processor()
        payload = {
            "task_id": "t1",
            "directory_key": "dir",
            "path": "/x",
            "fingerprint": "fp",
        }
        out = list(proc.process_element(self._event("JOB_CREATED", payload), None))
        assert len(out) == 1
        stored = json.loads(state.value())
        assert stored["task_id"] == "t1"
        assert stored["status"] == "created"
        assert stored["event_count"] == 1

    def test_started_event_updates_existing_state(self) -> None:
        proc, state = self._processor()
        list(
            proc.process_element(
                self._event(
                    "JOB_CREATED",
                    {"task_id": "t", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
                ),
                None,
            )
        )
        out = list(
            proc.process_element(
                self._event(
                    "JOB_STARTED",
                    {"processor_name": "proc-x", "processor_version": "2.0.0"},
                    ts="2024-01-01T00:00:05+00:00",
                ),
                None,
            )
        )
        assert len(out) == 1
        stored = json.loads(state.value())
        assert stored["status"] == "started"
        assert stored["processor_name"] == "proc-x"
        assert stored["processor_version"] == "2.0.0"
        assert stored["started_at"] == "2024-01-01T00:00:05+00:00"
        # NOTE: JobState.to_dict() emits "event_count" but not "events", and
        # JobState.from_dict() defaults events=[] when absent. As a result the
        # event history does not persist across process_element invocations and
        # event_count is always 1 after each call. This is captured here as
        # characterization (potential source bug worth flagging).
        assert stored["event_count"] == 1

    def test_completed_event_sets_duration(self) -> None:
        proc, state = self._processor()
        list(
            proc.process_element(
                self._event(
                    "JOB_CREATED",
                    {"task_id": "t", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
                ),
                None,
            )
        )
        list(
            proc.process_element(
                self._event("JOB_COMPLETED", {"duration_ms": 123.4}, ts="2024-01-01T00:00:10+00:00"),
                None,
            )
        )
        stored = json.loads(state.value())
        assert stored["status"] == "completed"
        assert stored["duration_ms"] == 123.4
        assert stored["completed_at"] == "2024-01-01T00:00:10+00:00"

    def test_failed_event_records_error(self) -> None:
        proc, state = self._processor()
        list(
            proc.process_element(
                self._event(
                    "JOB_CREATED",
                    {"task_id": "t", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
                ),
                None,
            )
        )
        list(
            proc.process_element(
                self._event("JOB_FAILED", {"error": "boom"}, ts="2024-01-01T00:00:11+00:00"),
                None,
            )
        )
        stored = json.loads(state.value())
        assert stored["status"] == "failed"
        assert stored["error"] == "boom"

    def test_started_without_prior_state_emits_nothing(self) -> None:
        proc, state = self._processor()
        out = list(
            proc.process_element(
                self._event("JOB_STARTED", {"processor_name": "p", "processor_version": "v"}),
                None,
            )
        )
        assert out == []
        assert state.value() is None

    def test_unknown_event_with_existing_state_still_records_event(self) -> None:
        # Per source: any branch falls through, then if job_state exists, the event is appended
        # and a row is yielded. Verify that contract.
        proc, _state = self._processor()
        list(
            proc.process_element(
                self._event(
                    "JOB_CREATED",
                    {"task_id": "t", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
                ),
                None,
            )
        )
        out = list(
            proc.process_element(
                self._event("JOB_UNKNOWN", {"foo": "bar"}),
                None,
            )
        )
        # status unchanged from CREATED, but a new row is emitted and event appended
        assert len(out) == 1

    def test_payload_accepted_as_dict_directly(self) -> None:
        """process_element supports payload already deserialized to dict."""
        proc, state = self._processor()
        row = tracker_mod.Row(
            task_id="t",
            event_type="JOB_CREATED",
            payload={"task_id": "t", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        out = list(proc.process_element(row, None))
        assert len(out) == 1
        stored = json.loads(state.value())
        assert stored["task_id"] == "t"

    def test_event_history_max_constant_present(self) -> None:
        # The module defines a constant for capping event history.
        # Note: history does not actually persist across process_element calls
        # (to_dict omits the events list) so per-call event_count is always 1;
        # however we still verify the truncation constant exists and the
        # in-memory append + slice logic does not crash under load.
        assert tracker_mod._MAX_EVENT_HISTORY == 100

        proc, state = self._processor()
        list(
            proc.process_element(
                self._event(
                    "JOB_CREATED",
                    {"task_id": "t", "directory_key": "d", "path": "/p", "fingerprint": "fp"},
                ),
                None,
            )
        )
        for i in range(tracker_mod._MAX_EVENT_HISTORY + 5):
            list(
                proc.process_element(
                    self._event("JOB_OTHER", {"i": i}),
                    None,
                )
            )
        stored = json.loads(state.value())
        # As noted above, persisted event_count remains 1 between calls.
        assert stored["event_count"] == 1


class TestParseJobEvent:
    def test_happy_path(self) -> None:
        msg = json.dumps(
            {
                "event_type": "JOB_CREATED",
                "timestamp": "2024-01-01T00:00:00+00:00",
                "payload": {"task_id": "abc"},
            }
        )
        row = parse_job_event(msg)
        assert row is not None
        assert row.task_id == "abc"
        assert row.event_type == "JOB_CREATED"
        assert json.loads(row.payload) == {"task_id": "abc"}
        assert row.timestamp == "2024-01-01T00:00:00+00:00"

    def test_missing_payload_yields_empty_strings(self) -> None:
        msg = json.dumps({"event_type": "JOB_X", "timestamp": "t"})
        row = parse_job_event(msg)
        assert row is not None
        assert row.task_id == ""
        assert row.event_type == "JOB_X"

    def test_invalid_json_returns_none(self) -> None:
        assert parse_job_event("not json") is None


class TestJobStateTrackerJob:
    def _configs(self, *, checkpoints: bool = True) -> tuple[Any, Any]:
        flink_cfg = MagicMock(
            parallelism=4,
            max_parallelism=128,
            checkpoint_enabled=checkpoints,
            checkpoint_interval_ms=60000,
            checkpoint_min_pause_ms=5000,
            checkpoint_timeout_ms=600000,
        )
        kafka_cfg = MagicMock()
        return flink_cfg, kafka_cfg

    def test_setup_environment_with_checkpointing(self) -> None:
        flink_cfg, kafka_cfg = self._configs(checkpoints=True)
        job = JobStateTrackerJob(flink_cfg, kafka_cfg)
        env = job.setup_environment()

        env.set_parallelism.assert_called_once_with(4)
        env.set_max_parallelism.assert_called_once_with(128)
        env.enable_checkpointing.assert_called_once_with(60000)
        ckpt = env.get_checkpoint_config.return_value
        ckpt.set_min_pause_between_checkpoints.assert_called_with(5000)
        ckpt.set_checkpoint_timeout.assert_called_with(600000)

    def test_setup_environment_without_checkpointing(self) -> None:
        flink_cfg, kafka_cfg = self._configs(checkpoints=False)
        job = JobStateTrackerJob(flink_cfg, kafka_cfg)
        env = job.setup_environment()

        env.enable_checkpointing.assert_not_called()
        env.get_checkpoint_config.assert_not_called()

    def test_build_job_initializes_env_if_missing(self) -> None:
        flink_cfg, kafka_cfg = self._configs()
        job = JobStateTrackerJob(flink_cfg, kafka_cfg)
        assert job._env is None
        job.build_job()
        assert job._env is not None

    def test_execute_builds_then_runs(self) -> None:
        flink_cfg, kafka_cfg = self._configs()
        job = JobStateTrackerJob(flink_cfg, kafka_cfg)
        job.execute("my-tracker")
        job._env.execute.assert_called_once_with("my-tracker")


# A guard so pytest treats imports happening at module level deterministically.
@pytest.fixture(autouse=True)
def _ensure_pyflink_stubs_present() -> None:
    # If a prior test removed stubs, reinstall.
    for name, mod in _STUBS.items():
        sys.modules.setdefault(name, mod)
