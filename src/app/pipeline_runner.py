"""Unified pipeline runner that spawns all components.

Starts:
1. Main pipeline (ingest + workers)
2. Python-based metric aggregation (writes to MinIO directly)
3. Aggregate sink (consumes Flink output if available, also runs metric values collector)

Usage:
    pixi run pipeline
"""

from __future__ import annotations

import atexit
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from src.observability.config import load_observability_config
from src.utils.logging import configure_logging, get_logger

logger = get_logger("pipeline.runner")

# Track child processes for cleanup
_child_processes: list[subprocess.Popen] = []


def cleanup_children() -> None:
    """Terminate all child processes."""
    for proc in _child_processes:
        if proc.poll() is None:  # Still running
            logger.info("Terminating child process %d", proc.pid)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("Force killing process %d", proc.pid)
                proc.kill()


def start_process(name: str, cmd: list[str]) -> subprocess.Popen:
    """Start a subprocess and track it.

    Args:
        name: Human-readable name for logging
        cmd: Command to execute

    Returns:
        Started process

    """
    logger.info("Starting %s: %s", name, " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    _child_processes.append(proc)
    logger.info("%s started with PID %d", name, proc.pid)
    return proc


def forward_output(proc: subprocess.Popen, prefix: str) -> None:
    """Forward process output to logger (non-blocking check)."""
    if proc.stdout is None:
        return

    # Non-blocking read
    import select

    while select.select([proc.stdout], [], [], 0)[0]:
        line = proc.stdout.readline()
        if not line:
            break
        # Log without the prefix cluttering JSON logs
        print(f"[{prefix}] {line.rstrip()}", file=sys.stderr)


def main() -> None:
    """Main entry point."""
    load_dotenv()

    # Configure logging
    obs_config = load_observability_config(Path("src/config/observability.toml"))
    configure_logging(obs_config)

    logger.info("=" * 60)
    logger.info("VisioEval Pipeline Runner")
    logger.info("=" * 60)

    # Register cleanup
    atexit.register(cleanup_children)

    # Handle signals
    def signal_handler(signum: int, _frame: object) -> None:
        logger.info("Received signal %d, shutting down...", signum)
        cleanup_children()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Get pixi executable path (use same Python environment)
    python_exe = sys.executable

    # Check if user wants to use Flink SQL (default: use Python aggregation)
    use_flink_sql = os.environ.get("VISIOEVAL_USE_FLINK_SQL", "").lower() == "true"

    # Step 1: Start aggregation
    if use_flink_sql:
        logger.info("Step 1: Starting Flink SQL aggregation...")
        from src.flink.submit import ensure_aggregation_job_running
        flink_ok = ensure_aggregation_job_running()
        if flink_ok:
            logger.info("Flink aggregation job is running")
        else:
            logger.warning("Flink job not running - falling back to Python aggregation")
            use_flink_sql = False

    if not use_flink_sql:
        logger.info("Step 1: Starting Python-based metric aggregation...")
        agg_proc = start_process(
            "python-aggregation",
            [python_exe, "-m", "src.app.flink_aggregation"],
        )

    # Step 2: Start aggregate sink (also runs metric values collector)
    logger.info("Step 2: Starting aggregate sink + metric values collector...")
    sink_proc = start_process(
        "aggregate-sink",
        [python_exe, "-m", "src.app.aggregate_sink"],
    )

    # Give processes a moment to start
    time.sleep(1)

    # Step 3: Start main pipeline
    logger.info("Step 3: Starting main pipeline...")
    main_proc = start_process(
        "main-pipeline",
        [python_exe, "-m", "src.app.main"],
    )

    logger.info("=" * 60)
    logger.info("All components started!")
    logger.info("  - Main pipeline: PID %d", main_proc.pid)
    if not use_flink_sql:
        logger.info("  - Python aggregation: PID %d", agg_proc.pid)
    else:
        logger.info("  - Flink SQL job: running in cluster")
    logger.info("  - Aggregate sink: PID %d", sink_proc.pid)
    logger.info("")
    logger.info("Dashboard: pixi run dashboard")
    logger.info("Press Ctrl+C to stop all components")
    logger.info("=" * 60)

    # Monitor processes
    try:
        while True:
            # Check if main pipeline is still running
            if main_proc.poll() is not None:
                logger.error("Main pipeline exited with code %d", main_proc.returncode)
                break

            # Check if aggregation is still running (Python mode)
            if not use_flink_sql and agg_proc.poll() is not None:
                logger.warning("Python aggregation exited with code %d, restarting...", agg_proc.returncode)
                _child_processes.remove(agg_proc)
                agg_proc = start_process(
                    "python-aggregation",
                    [python_exe, "-m", "src.app.flink_aggregation"],
                )

            # Check if sink is still running
            if sink_proc.poll() is not None:
                logger.warning("Aggregate sink exited with code %d, restarting...", sink_proc.returncode)
                _child_processes.remove(sink_proc)
                sink_proc = start_process(
                    "aggregate-sink",
                    [python_exe, "-m", "src.app.aggregate_sink"],
                )

            # Forward output from processes
            forward_output(main_proc, "main")
            if not use_flink_sql:
                forward_output(agg_proc, "agg")
            forward_output(sink_proc, "sink")

            time.sleep(0.5)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")

    finally:
        cleanup_children()
        logger.info("Pipeline runner stopped.")


if __name__ == "__main__":
    main()
