"""Job state tracker Flink job.

Maintains stateful tracking of job lifecycle events for
querying job status and history.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from pyflink.common import Row, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    KeyedProcessFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator

    from src.flink.config import FlinkConfig
    from src.streaming.config import KafkaConfig

# Constants
_MAX_EVENT_HISTORY = 100

logger = get_logger(__name__)


class JobStatus(str, Enum):
    """Job status enumeration."""

    CREATED = "created"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class JobState:
    """Stateful representation of a job."""

    job_id: str
    directory_key: str
    path: str
    fingerprint: str
    status: JobStatus
    algo_name: str | None = None
    algo_version: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = None
    duration_ms: float | None = None
    events: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "directory_key": self.directory_key,
            "path": self.path,
            "fingerprint": self.fingerprint,
            "status": self.status.value,
            "algo_name": self.algo_name,
            "algo_version": self.algo_version,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "event_count": len(self.events),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> JobState:
        return cls(
            job_id=data["job_id"],
            directory_key=data["directory_key"],
            path=data["path"],
            fingerprint=data["fingerprint"],
            status=JobStatus(data["status"]),
            algo_name=data.get("algo_name"),
            algo_version=data.get("algo_version"),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else None,
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            error=data.get("error"),
            duration_ms=data.get("duration_ms"),
            events=data.get("events", []),
        )


class JobStateProcessor(KeyedProcessFunction):
    """Keyed process function for job state tracking.

    Maintains per-job state and emits state changes.
    Uses Flink's ValueState for fault-tolerant state management.
    """

    def __init__(self) -> None:
        self._state = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """Initialize state descriptors."""
        state_descriptor = ValueStateDescriptor(
            "job_state",
            Types.STRING(),
        )
        self._state = runtime_context.get_state(state_descriptor)

    def process_element(
        self,
        value: Row,
        _ctx: KeyedProcessFunction.Context,
    ) -> Iterator[Row]:
        """Process job lifecycle event and update state."""
        event_type = value.event_type
        payload = json.loads(value.payload) if isinstance(value.payload, str) else value.payload
        timestamp = datetime.fromisoformat(value.timestamp) if isinstance(value.timestamp, str) else value.timestamp

        # Get current state
        state_json = self._state.value()
        job_state = JobState.from_dict(json.loads(state_json)) if state_json else None

        # Process based on event type
        if event_type == "JOB_CREATED":
            job_state = JobState(
                job_id=payload["job_id"],
                directory_key=payload["directory_key"],
                path=payload["path"],
                fingerprint=payload["fingerprint"],
                status=JobStatus.CREATED,
                created_at=timestamp,
                events=[],
            )

        elif event_type == "JOB_STARTED" and job_state:
            job_state.status = JobStatus.STARTED
            job_state.algo_name = payload.get("algo_name")
            job_state.algo_version = payload.get("algo_version")
            job_state.started_at = timestamp

        elif event_type == "JOB_COMPLETED" and job_state:
            job_state.status = JobStatus.COMPLETED
            job_state.completed_at = timestamp
            job_state.duration_ms = payload.get("duration_ms")

        elif event_type == "JOB_FAILED" and job_state:
            job_state.status = JobStatus.FAILED
            job_state.completed_at = timestamp
            job_state.error = payload.get("error")

        if job_state:
            # Record event in history (limit to last N events)
            job_state.events.append(
                {
                    "type": event_type,
                    "timestamp": timestamp.isoformat() if timestamp else None,
                },
            )
            if len(job_state.events) > _MAX_EVENT_HISTORY:
                job_state.events = job_state.events[-_MAX_EVENT_HISTORY:]

            # Update state
            self._state.update(json.dumps(job_state.to_dict()))

            # Emit state change
            yield Row(
                job_id=job_state.job_id,
                status=job_state.status.value,
                state_json=json.dumps(job_state.to_dict()),
                updated_at=timestamp.isoformat() if timestamp else None,
            )


class JobStateTrackerJob:
    """Flink job for tracking job lifecycle state.

    Consumes job lifecycle events (JOB_CREATED, JOB_STARTED,
    JOB_COMPLETED, JOB_FAILED) and maintains stateful tracking
    for each job.

    State can be queried via Flink's queryable state or
    emitted to an output topic for external consumption.
    """

    def __init__(
        self,
        flink_config: FlinkConfig,
        kafka_config: KafkaConfig,
    ) -> None:
        self._flink_config = flink_config
        self._kafka_config = kafka_config
        self._env: StreamExecutionEnvironment | None = None

    def setup_environment(self) -> StreamExecutionEnvironment:
        """Create and configure Flink execution environment."""
        env = StreamExecutionEnvironment.get_execution_environment()

        env.set_parallelism(self._flink_config.parallelism)
        env.set_max_parallelism(self._flink_config.max_parallelism)

        if self._flink_config.checkpoint_enabled:
            env.enable_checkpointing(self._flink_config.checkpoint_interval_ms)
            checkpoint_config = env.get_checkpoint_config()
            checkpoint_config.set_min_pause_between_checkpoints(
                self._flink_config.checkpoint_min_pause_ms,
            )
            checkpoint_config.set_checkpoint_timeout(
                self._flink_config.checkpoint_timeout_ms,
            )

        self._env = env
        return env

    def build_job(self) -> None:
        """Build the Flink job graph."""
        if self._env is None:
            self.setup_environment()

        # Define row type for job events
        Types.ROW_NAMED(
            ["job_id", "event_type", "payload", "timestamp"],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()],
        )

        # Define output row type
        Types.ROW_NAMED(
            ["job_id", "status", "state_json", "updated_at"],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()],
        )

        logger.info(
            "JobStateTrackerJob configured: parallelism=%d",
            self._flink_config.parallelism,
        )

    def execute(self, job_name: str = "JobStateTracker") -> None:
        """Execute the Flink job."""
        if self._env is None:
            self.build_job()
        self._env.execute(job_name)


def parse_job_event(json_str: str) -> Row | None:
    """Parse job lifecycle event JSON to Row."""
    try:
        data = json.loads(json_str)

        return Row(
            job_id=data.get("payload", {}).get("job_id", ""),
            event_type=data.get("event_type", ""),
            payload=json.dumps(data.get("payload", {})),
            timestamp=data.get("timestamp", ""),
        )
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning("Failed to parse job event: %s", e)
        return None
