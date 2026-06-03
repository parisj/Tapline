"""Unit tests for src/app/pipeline_runner.py.

Characterization tests: mocks subprocess.Popen, signal.signal, atexit,
time.sleep, and select.select so the orchestration logic can be exercised
without actually spawning processes.
"""

from __future__ import annotations

import signal
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

import src.app.pipeline_runner as runner


@pytest.fixture(autouse=True)
def _reset_child_processes() -> None:
    """Ensure the module-level child process list is empty before each test."""
    runner._child_processes.clear()
    yield
    runner._child_processes.clear()


def _make_proc(pid: int = 1234, poll_result: object | None = None) -> MagicMock:
    """Construct a fake Popen-like object."""
    proc = MagicMock(spec=subprocess.Popen)
    proc.pid = pid
    proc.poll.return_value = poll_result
    proc.returncode = 0
    proc.stdout = MagicMock()
    return proc


class TestCleanupChildren:
    def test_terminates_running_processes(self) -> None:
        proc = _make_proc(pid=111, poll_result=None)  # still running
        runner._child_processes.append(proc)

        runner.cleanup_children()

        proc.terminate.assert_called_once()
        proc.wait.assert_called_once_with(timeout=5)
        proc.kill.assert_not_called()

    def test_skips_already_exited_processes(self) -> None:
        proc = _make_proc(pid=111, poll_result=0)  # already exited
        runner._child_processes.append(proc)

        runner.cleanup_children()

        proc.terminate.assert_not_called()

    def test_force_kills_on_wait_timeout(self) -> None:
        proc = _make_proc(pid=222, poll_result=None)
        proc.wait.side_effect = subprocess.TimeoutExpired(cmd="x", timeout=5)
        runner._child_processes.append(proc)

        runner.cleanup_children()

        proc.terminate.assert_called_once()
        proc.kill.assert_called_once()

    def test_handles_multiple_children(self) -> None:
        proc_a = _make_proc(pid=1, poll_result=None)
        proc_b = _make_proc(pid=2, poll_result=0)  # exited
        proc_c = _make_proc(pid=3, poll_result=None)
        runner._child_processes.extend([proc_a, proc_b, proc_c])

        runner.cleanup_children()

        proc_a.terminate.assert_called_once()
        proc_b.terminate.assert_not_called()
        proc_c.terminate.assert_called_once()


class TestStartProcess:
    def test_start_process_invokes_popen_and_tracks(self) -> None:
        fake_proc = _make_proc(pid=4242)

        with patch.object(runner.subprocess, "Popen", return_value=fake_proc) as mock_popen:
            result = runner.start_process("widget", ["python", "-m", "x"])

        assert result is fake_proc
        assert fake_proc in runner._child_processes
        mock_popen.assert_called_once_with(
            ["python", "-m", "x"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    def test_start_process_appends_to_child_processes(self) -> None:
        fake_proc_1 = _make_proc(pid=1)
        fake_proc_2 = _make_proc(pid=2)

        with patch.object(runner.subprocess, "Popen", side_effect=[fake_proc_1, fake_proc_2]):
            runner.start_process("a", ["cmd-a"])
            runner.start_process("b", ["cmd-b"])

        assert runner._child_processes == [fake_proc_1, fake_proc_2]


class TestForwardOutput:
    def test_forward_output_returns_when_stdout_none(self) -> None:
        proc = _make_proc()
        proc.stdout = None
        # Should not raise
        runner.forward_output(proc, "x")

    def test_forward_output_drains_available_lines(self) -> None:
        proc = _make_proc()
        proc.stdout.readline.side_effect = ["hello\n", "world\n"]

        # select.select returns ready twice, then empty
        select_results = [
            ([proc.stdout], [], []),
            ([proc.stdout], [], []),
            ([], [], []),
        ]
        with (
            patch.object(runner.select, "select", side_effect=select_results),
            patch.object(runner.sys, "stderr") as mock_stderr,
        ):
            runner.forward_output(proc, "PREFIX")

        # Two writes, prefixed correctly
        assert mock_stderr.write.call_count == 2
        assert "[PREFIX] hello" in mock_stderr.write.call_args_list[0].args[0]

    def test_forward_output_breaks_on_empty_readline(self) -> None:
        proc = _make_proc()
        proc.stdout.readline.return_value = ""  # EOF

        with (
            patch.object(runner.select, "select", return_value=([proc.stdout], [], [])),
            patch.object(runner.sys, "stderr") as mock_stderr,
        ):
            runner.forward_output(proc, "p")

        mock_stderr.write.assert_not_called()

    def test_forward_output_no_data_ready(self) -> None:
        proc = _make_proc()

        with (
            patch.object(runner.select, "select", return_value=([], [], [])),
            patch.object(runner.sys, "stderr") as mock_stderr,
        ):
            runner.forward_output(proc, "p")

        mock_stderr.write.assert_not_called()
        proc.stdout.readline.assert_not_called()


class _LoopStopper:
    """Helper that flips main_proc's poll result after N iterations."""

    def __init__(self, proc: MagicMock, after_n: int) -> None:
        self.proc = proc
        self.after_n = after_n
        self.count = 0

    def __call__(self, _duration: float) -> None:
        self.count += 1
        if self.count >= self.after_n:
            self.proc.poll.return_value = 1  # Main pipeline "died" -> break loop


class TestMain:
    def test_main_python_aggregation_default(self) -> None:
        agg_proc = _make_proc(pid=100)
        sink_proc = _make_proc(pid=200)
        main_proc = _make_proc(pid=300)
        # main_proc.poll() will be flipped to 1 by _LoopStopper after one iteration

        with (
            patch.object(runner, "load_dotenv"),
            patch.object(runner, "load_observability_config"),
            patch.object(runner, "configure_logging"),
            patch.object(runner, "atexit") as mock_atexit,
            patch.object(runner.signal, "signal") as mock_signal,
            patch.dict(runner.os.environ, {}, clear=False),
            patch.object(
                runner,
                "start_process",
                side_effect=[agg_proc, sink_proc, main_proc],
            ) as mock_start,
            patch.object(runner, "forward_output"),
            patch.object(runner.time, "sleep", side_effect=_LoopStopper(main_proc, after_n=2)),
            patch.object(runner, "cleanup_children") as mock_cleanup,
        ):
            # Ensure TAPLINE_USE_FLINK_SQL is not set to true
            runner.os.environ.pop("TAPLINE_USE_FLINK_SQL", None)
            runner.main()

        # 3 subprocesses launched (Python aggregation default path)
        assert mock_start.call_count == 3
        names = [c.args[0] for c in mock_start.call_args_list]
        assert names == ["python-aggregation", "aggregate-sink", "main-pipeline"]

        # Signal handlers registered
        registered = {c.args[0] for c in mock_signal.call_args_list}
        assert signal.SIGINT in registered
        assert signal.SIGTERM in registered

        # atexit cleanup registered with the (now mocked) cleanup_children
        mock_atexit.register.assert_called_once_with(mock_cleanup)
        # cleanup invoked in finally
        mock_cleanup.assert_called()

    def test_main_flink_sql_mode_success(self) -> None:
        sink_proc = _make_proc(pid=200)
        main_proc = _make_proc(pid=300)

        ensure_flink_mod = MagicMock()
        ensure_flink_mod.ensure_aggregation_job_running.return_value = True

        with (
            patch.object(runner, "load_dotenv"),
            patch.object(runner, "load_observability_config"),
            patch.object(runner, "configure_logging"),
            patch.object(runner, "atexit"),
            patch.object(runner.signal, "signal"),
            patch.dict(runner.os.environ, {"TAPLINE_USE_FLINK_SQL": "true"}),
            patch.dict(
                "sys.modules",
                {"src.flink.submit": ensure_flink_mod},
            ),
            patch.object(
                runner,
                "start_process",
                side_effect=[sink_proc, main_proc],
            ) as mock_start,
            patch.object(runner, "forward_output"),
            patch.object(runner.time, "sleep", side_effect=_LoopStopper(main_proc, after_n=2)),
            patch.object(runner, "cleanup_children"),
        ):
            runner.main()

        # Only 2 subprocesses (no python-aggregation; Flink handles it)
        names = [c.args[0] for c in mock_start.call_args_list]
        assert names == ["aggregate-sink", "main-pipeline"]
        ensure_flink_mod.ensure_aggregation_job_running.assert_called_once()

    def test_main_flink_sql_mode_falls_back_when_not_running(self) -> None:
        """If ensure_aggregation_job_running returns False, fall back to Python."""
        agg_proc = _make_proc(pid=100)
        sink_proc = _make_proc(pid=200)
        main_proc = _make_proc(pid=300)

        ensure_flink_mod = MagicMock()
        ensure_flink_mod.ensure_aggregation_job_running.return_value = False

        with (
            patch.object(runner, "load_dotenv"),
            patch.object(runner, "load_observability_config"),
            patch.object(runner, "configure_logging"),
            patch.object(runner, "atexit"),
            patch.object(runner.signal, "signal"),
            patch.dict(runner.os.environ, {"TAPLINE_USE_FLINK_SQL": "true"}),
            patch.dict("sys.modules", {"src.flink.submit": ensure_flink_mod}),
            patch.object(
                runner,
                "start_process",
                side_effect=[agg_proc, sink_proc, main_proc],
            ) as mock_start,
            patch.object(runner, "forward_output"),
            patch.object(runner.time, "sleep", side_effect=_LoopStopper(main_proc, after_n=2)),
            patch.object(runner, "cleanup_children"),
        ):
            runner.main()

        # Fell back to python-aggregation -> 3 start_process calls
        names = [c.args[0] for c in mock_start.call_args_list]
        assert names == ["python-aggregation", "aggregate-sink", "main-pipeline"]

    def test_signal_handler_calls_cleanup_and_exits(self) -> None:
        """The locally defined signal_handler calls cleanup_children and sys.exit(0)."""
        main_proc = _make_proc(pid=300)
        captured_handler: dict[str, object] = {}

        def fake_signal(signum: int, handler: object) -> None:
            captured_handler.setdefault("handler", handler)

        with (
            patch.object(runner, "load_dotenv"),
            patch.object(runner, "load_observability_config"),
            patch.object(runner, "configure_logging"),
            patch.object(runner, "atexit"),
            patch.object(runner.signal, "signal", side_effect=fake_signal),
            patch.dict(runner.os.environ, {}, clear=False),
            patch.object(
                runner,
                "start_process",
                side_effect=[_make_proc(pid=1), _make_proc(pid=2), main_proc],
            ),
            patch.object(runner, "forward_output"),
            patch.object(runner.time, "sleep", side_effect=_LoopStopper(main_proc, after_n=2)),
            patch.object(runner, "cleanup_children") as mock_cleanup,
        ):
            runner.os.environ.pop("TAPLINE_USE_FLINK_SQL", None)
            runner.main()

        # Now invoke the captured handler with sys.exit patched
        handler = captured_handler["handler"]
        with patch.object(runner.sys, "exit") as mock_exit:
            handler(signal.SIGINT, None)  # type: ignore[operator]

        mock_exit.assert_called_once_with(0)
        # cleanup_children invoked at least once from the handler
        assert mock_cleanup.call_count >= 1

    def test_main_restarts_dead_python_aggregation(self) -> None:
        """If python-aggregation exits unexpectedly, it should be restarted."""
        agg_proc_initial = _make_proc(pid=100, poll_result=None)
        sink_proc = _make_proc(pid=200)
        main_proc = _make_proc(pid=300, poll_result=None)
        agg_proc_restart = _make_proc(pid=101, poll_result=None)

        # Sequence of start_process returns
        start_returns = iter([agg_proc_initial, sink_proc, main_proc, agg_proc_restart])

        def fake_start(_name: str, _cmd: list[str]) -> MagicMock:
            proc = next(start_returns)
            runner._child_processes.append(proc)
            return proc

        # Iteration sequence:
        # iter 1: main alive, agg dies (poll=1), trigger restart
        # iter 2: main dies -> break
        call_count = {"n": 0}

        def fake_sleep(_duration: float) -> None:
            call_count["n"] += 1
            if call_count["n"] == 1:
                agg_proc_initial.poll.return_value = 1
                agg_proc_initial.returncode = 1
            elif call_count["n"] >= 2:
                main_proc.poll.return_value = 1

        with (
            patch.object(runner, "load_dotenv"),
            patch.object(runner, "load_observability_config"),
            patch.object(runner, "configure_logging"),
            patch.object(runner, "atexit"),
            patch.object(runner.signal, "signal"),
            patch.dict(runner.os.environ, {}, clear=False),
            patch.object(runner, "start_process", side_effect=fake_start) as mock_start,
            patch.object(runner, "forward_output"),
            patch.object(runner.time, "sleep", side_effect=fake_sleep),
            patch.object(runner, "cleanup_children"),
        ):
            runner.os.environ.pop("TAPLINE_USE_FLINK_SQL", None)
            runner.main()

        # 3 initial + 1 restart = 4
        assert mock_start.call_count == 4
        # Final start_process call was for python-aggregation restart
        assert mock_start.call_args_list[-1].args[0] == "python-aggregation"

    def test_main_handles_keyboard_interrupt(self) -> None:
        """KeyboardInterrupt mid-loop should still trigger cleanup in finally."""
        main_proc = _make_proc(pid=300)

        # Raise KeyboardInterrupt on forward_output (called inside the while loop)
        def raise_kbi(*_args: object, **_kwargs: object) -> None:
            raise KeyboardInterrupt

        with (
            patch.object(runner, "load_dotenv"),
            patch.object(runner, "load_observability_config"),
            patch.object(runner, "configure_logging"),
            patch.object(runner, "atexit"),
            patch.object(runner.signal, "signal"),
            patch.dict(runner.os.environ, {}, clear=False),
            patch.object(
                runner,
                "start_process",
                side_effect=[_make_proc(pid=1), _make_proc(pid=2), main_proc],
            ),
            patch.object(runner, "forward_output", side_effect=raise_kbi),
            patch.object(runner.time, "sleep"),
            patch.object(runner, "cleanup_children") as mock_cleanup,
        ):
            runner.os.environ.pop("TAPLINE_USE_FLINK_SQL", None)
            runner.main()

        # cleanup_children invoked from the `finally` block
        mock_cleanup.assert_called()
