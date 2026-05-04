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
from src.domain.tasks import Task


class TestDispatchPlan:
    """Tests for DispatchPlan dataclass."""

    def test_dispatch_plan_creation(self) -> None:
        mock_processor = MagicMock()
        mock_processor.name = "test_processor"
        mock_processor.version = "1.0.0"

        plan = DispatchPlan(processor=mock_processor, settings={"threshold": 0.5})

        assert plan.processor == mock_processor
        assert plan.settings == {"threshold": 0.5}

    def test_dispatch_plan_is_frozen(self) -> None:
        mock_processor = MagicMock()
        plan = DispatchPlan(processor=mock_processor, settings={})

        with pytest.raises(FrozenInstanceError):
            plan.settings = {"new": "value"}  # type: ignore[misc]

    def test_dispatch_plan_with_empty_settings(self) -> None:
        mock_processor = MagicMock()
        plan = DispatchPlan(processor=mock_processor, settings={})

        assert plan.settings == {}


class TestDispatcher:
    """Tests for Dispatcher class."""

    def test_dispatcher_creation(self) -> None:
        mock_processor = MagicMock()
        routes = {
            "inbox": DispatchPlan(processor=mock_processor, settings={"mode": "fast"}),
        }

        dispatcher = Dispatcher(routes)

        assert dispatcher._routes == routes

    def test_dispatcher_dispatch_success(self) -> None:
        mock_processor = MagicMock()
        mock_processor.name = "analysis_probe"
        plan = DispatchPlan(processor=mock_processor, settings={"threshold": 0.5})
        routes = {"inbox": plan}

        dispatcher = Dispatcher(routes)

        task = Task(
            task_id="task-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        result = dispatcher.dispatch(task)

        assert result == plan
        assert result.processor == mock_processor
        assert result.settings == {"threshold": 0.5}

    def test_dispatcher_dispatch_unknown_directory_raises_keyerror(self) -> None:
        mock_processor = MagicMock()
        routes = {
            "inbox": DispatchPlan(processor=mock_processor, settings={}),
        }

        dispatcher = Dispatcher(routes)

        task = Task(
            task_id="task-123",
            directory_key="unknown",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        with pytest.raises(KeyError, match="No dispatch route for directory_key=unknown"):
            dispatcher.dispatch(task)

    def test_dispatcher_with_multiple_routes(self) -> None:
        mock_processor_1 = MagicMock()
        mock_processor_1.name = "processor_1"
        mock_processor_2 = MagicMock()
        mock_processor_2.name = "processor_2"
        mock_processor_3 = MagicMock()
        mock_processor_3.name = "processor_3"

        routes = {
            "inbox": DispatchPlan(processor=mock_processor_1, settings={"priority": 1}),
            "archive": DispatchPlan(processor=mock_processor_2, settings={"priority": 2}),
            "processed": DispatchPlan(processor=mock_processor_3, settings={"priority": 3}),
        }

        dispatcher = Dispatcher(routes)

        task_inbox = Task(
            task_id="task-1",
            directory_key="inbox",
            path="/tmp/inbox/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )
        task_archive = Task(
            task_id="task-2",
            directory_key="archive",
            path="/tmp/archive/test.png",
            created_at_unix=1704067200.0,
            fingerprint="def456",
        )

        result_inbox = dispatcher.dispatch(task_inbox)
        result_archive = dispatcher.dispatch(task_archive)

        assert result_inbox.processor == mock_processor_1
        assert result_inbox.settings["priority"] == 1
        assert result_archive.processor == mock_processor_2
        assert result_archive.settings["priority"] == 2

    def test_dispatcher_with_empty_routes(self) -> None:
        dispatcher = Dispatcher({})

        task = Task(
            task_id="task-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        with pytest.raises(KeyError):
            dispatcher.dispatch(task)

    def test_dispatcher_returns_same_plan_for_same_directory(self) -> None:
        mock_processor = MagicMock()
        plan = DispatchPlan(processor=mock_processor, settings={})
        dispatcher = Dispatcher({"inbox": plan})

        task1 = Task(
            task_id="task-1",
            directory_key="inbox",
            path="/tmp/file1.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )
        task2 = Task(
            task_id="task-2",
            directory_key="inbox",
            path="/tmp/file2.png",
            created_at_unix=1704067201.0,
            fingerprint="def456",
        )

        result1 = dispatcher.dispatch(task1)
        result2 = dispatcher.dispatch(task2)

        assert result1 is result2
        assert result1 is plan
