"""Unit tests for dispatcher and routing logic.

Tests for:
- Dispatcher class
- DispatchPlan dataclass
- Route dispatch functionality
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from src.dispatch.dispatcher import Dispatcher, DispatchPlan
from src.domain.jobs import Job


class TestDispatchPlan:
    """Tests for DispatchPlan dataclass."""

    def test_dispatch_plan_creation(self) -> None:
        mock_algo = MagicMock()
        mock_algo.name = "test_algo"
        mock_algo.version = "1.0.0"

        plan = DispatchPlan(algo=mock_algo, settings={"threshold": 0.5})

        assert plan.algo == mock_algo
        assert plan.settings == {"threshold": 0.5}

    def test_dispatch_plan_is_frozen(self) -> None:
        mock_algo = MagicMock()
        plan = DispatchPlan(algo=mock_algo, settings={})

        with pytest.raises(FrozenInstanceError):
            plan.settings = {"new": "value"}  # type: ignore[misc]

    def test_dispatch_plan_with_empty_settings(self) -> None:
        mock_algo = MagicMock()
        plan = DispatchPlan(algo=mock_algo, settings={})

        assert plan.settings == {}


class TestDispatcher:
    """Tests for Dispatcher class."""

    def test_dispatcher_creation(self) -> None:
        mock_algo = MagicMock()
        routes = {
            "inbox": DispatchPlan(algo=mock_algo, settings={"mode": "fast"}),
        }

        dispatcher = Dispatcher(routes)

        assert dispatcher._routes == routes

    def test_dispatcher_dispatch_success(self) -> None:
        mock_algo = MagicMock()
        mock_algo.name = "analysis_probe"
        plan = DispatchPlan(algo=mock_algo, settings={"threshold": 0.5})
        routes = {"inbox": plan}

        dispatcher = Dispatcher(routes)

        job = Job(
            job_id="job-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        result = dispatcher.dispatch(job)

        assert result == plan
        assert result.algo == mock_algo
        assert result.settings == {"threshold": 0.5}

    def test_dispatcher_dispatch_unknown_directory_raises_keyerror(self) -> None:
        mock_algo = MagicMock()
        routes = {
            "inbox": DispatchPlan(algo=mock_algo, settings={}),
        }

        dispatcher = Dispatcher(routes)

        job = Job(
            job_id="job-123",
            directory_key="unknown",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        with pytest.raises(KeyError, match="No dispatch route for directory_key=unknown"):
            dispatcher.dispatch(job)

    def test_dispatcher_with_multiple_routes(self) -> None:
        mock_algo_1 = MagicMock()
        mock_algo_1.name = "algo_1"
        mock_algo_2 = MagicMock()
        mock_algo_2.name = "algo_2"
        mock_algo_3 = MagicMock()
        mock_algo_3.name = "algo_3"

        routes = {
            "inbox": DispatchPlan(algo=mock_algo_1, settings={"priority": 1}),
            "archive": DispatchPlan(algo=mock_algo_2, settings={"priority": 2}),
            "processed": DispatchPlan(algo=mock_algo_3, settings={"priority": 3}),
        }

        dispatcher = Dispatcher(routes)

        job_inbox = Job(
            job_id="job-1",
            directory_key="inbox",
            path="/tmp/inbox/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )
        job_archive = Job(
            job_id="job-2",
            directory_key="archive",
            path="/tmp/archive/test.png",
            created_at_unix=1704067200.0,
            fingerprint="def456",
        )

        result_inbox = dispatcher.dispatch(job_inbox)
        result_archive = dispatcher.dispatch(job_archive)

        assert result_inbox.algo == mock_algo_1
        assert result_inbox.settings["priority"] == 1
        assert result_archive.algo == mock_algo_2
        assert result_archive.settings["priority"] == 2

    def test_dispatcher_with_empty_routes(self) -> None:
        dispatcher = Dispatcher({})

        job = Job(
            job_id="job-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        with pytest.raises(KeyError):
            dispatcher.dispatch(job)

    def test_dispatcher_returns_same_plan_for_same_directory(self) -> None:
        mock_algo = MagicMock()
        plan = DispatchPlan(algo=mock_algo, settings={})
        dispatcher = Dispatcher({"inbox": plan})

        job1 = Job(
            job_id="job-1",
            directory_key="inbox",
            path="/tmp/file1.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )
        job2 = Job(
            job_id="job-2",
            directory_key="inbox",
            path="/tmp/file2.png",
            created_at_unix=1704067201.0,
            fingerprint="def456",
        )

        result1 = dispatcher.dispatch(job1)
        result2 = dispatcher.dispatch(job2)

        assert result1 is result2
        assert result1 is plan
