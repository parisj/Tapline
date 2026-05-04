"""Load generator for benchmarking the Tapline streaming pipeline.

Creates synthetic workload by:
1. Generating test image files at configurable rates
2. Placing them in watched directories
3. Measuring end-to-end latency and throughput

Usage:
    python -m benchmarks.load_generator --rps 10 --duration 60 --directory /path/to/input
"""

from __future__ import annotations

import argparse
import random
import statistics
import struct
import sys
import tempfile
import threading
import time
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

# Constants for latency percentile calculations
_MIN_SAMPLES_FOR_PERCENTILE = 2
_CRC_MASK = 0xFFFFFFFF


@dataclass
class BenchmarkConfig:
    """Configuration for load generation benchmark."""

    target_rps: float  # Target requests per second
    duration_sec: int  # Total test duration
    warmup_sec: int  # Warmup period (results discarded)
    cooldown_sec: int  # Cooldown period before collecting results
    directory: Path  # Directory to place test files
    image_width: int = 640
    image_height: int = 480
    file_prefix: str = "bench"


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""

    total_files_created: int = 0
    actual_rps: float = 0.0
    duration_sec: float = 0.0
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def avg_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        if not self.latencies_ms:
            return 0.0
        return statistics.mean(self.latencies_ms)

    @property
    def p50_latency_ms(self) -> float:
        """Calculate P50 (median) latency in milliseconds."""
        if not self.latencies_ms:
            return 0.0
        return statistics.median(self.latencies_ms)

    @property
    def p95_latency_ms(self) -> float:
        """Calculate P95 latency in milliseconds."""
        if len(self.latencies_ms) < _MIN_SAMPLES_FOR_PERCENTILE:
            return self.avg_latency_ms
        return statistics.quantiles(self.latencies_ms, n=20)[18]

    @property
    def p99_latency_ms(self) -> float:
        """Calculate P99 latency in milliseconds."""
        if len(self.latencies_ms) < _MIN_SAMPLES_FOR_PERCENTILE:
            return self.avg_latency_ms
        return statistics.quantiles(self.latencies_ms, n=100)[98]

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "total_files_created": self.total_files_created,
            "actual_rps": self.actual_rps,
            "duration_sec": self.duration_sec,
            "avg_latency_ms": self.avg_latency_ms,
            "p50_latency_ms": self.p50_latency_ms,
            "p95_latency_ms": self.p95_latency_ms,
            "p99_latency_ms": self.p99_latency_ms,
        }


def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    """Create a PNG chunk with CRC."""
    chunk = chunk_type + data
    return struct.pack(">I", len(data)) + chunk + struct.pack(">I", zlib.crc32(chunk) & _CRC_MASK)


def generate_png_bytes(width: int = 640, height: int = 480) -> bytes:
    """Generate a minimal valid PNG file.

    Creates a simple solid-color PNG without requiring PIL.

    Args:
        width: Image width in pixels.
        height: Image height in pixels.

    Returns:
        PNG file as bytes.

    """
    # Random color for each image
    r, g, b = random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)

    # IHDR chunk
    ihdr_data = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)

    # IDAT chunk (compressed image data)
    raw_data = b""
    for _ in range(height):
        raw_data += b"\x00"  # Filter byte (none)
        raw_data += bytes([r, g, b] * width)

    compressed = zlib.compress(raw_data)

    # Build PNG
    png = b"\x89PNG\r\n\x1a\n"
    png += _png_chunk(b"IHDR", ihdr_data)
    png += _png_chunk(b"IDAT", compressed)
    png += _png_chunk(b"IEND", b"")

    return png


class LoadGenerator:
    """Generates load by creating files at target RPS."""

    def __init__(self, config: BenchmarkConfig) -> None:
        """Initialize load generator.

        Args:
            config: Benchmark configuration.

        """
        self.config = config
        self._stop = threading.Event()
        self._files_created = 0
        self._creation_times: dict[str, float] = {}
        self._lock = threading.Lock()

    def _log(self, message: str) -> None:
        """Log a message to stdout."""
        sys.stdout.write(message + "\n")
        sys.stdout.flush()

    def _create_file(self, index: int) -> str:
        """Create a single test file."""
        filename = f"{self.config.file_prefix}_{index}_{int(time.time() * 1000)}.png"
        filepath = self.config.directory / filename

        png_data = generate_png_bytes(self.config.image_width, self.config.image_height)

        with self._lock:
            self._creation_times[filename] = time.perf_counter()

        filepath.write_bytes(png_data)

        with self._lock:
            self._files_created += 1

        return filename

    def run(self) -> BenchmarkResult:
        """Run the load generation benchmark.

        Returns:
            BenchmarkResult with metrics.

        """
        result = BenchmarkResult()

        # Ensure directory exists
        self.config.directory.mkdir(parents=True, exist_ok=True)

        self._log(
            f"Starting load generator: {self.config.target_rps} RPS for {self.config.duration_sec}s",
        )
        self._log(f"Directory: {self.config.directory}")
        self._log(
            f"Warmup: {self.config.warmup_sec}s, Cooldown: {self.config.cooldown_sec}s",
        )
        self._log("")

        # Calculate interval between files
        interval = 1.0 / self.config.target_rps if self.config.target_rps > 0 else 1.0

        start_time = time.perf_counter()
        warmup_end = start_time + self.config.warmup_sec
        test_end = warmup_end + self.config.duration_sec
        index = 0

        # Warmup phase
        self._log("Warmup phase...")
        while time.perf_counter() < warmup_end and not self._stop.is_set():
            self._create_file(index)
            index += 1
            time.sleep(interval)

        # Measurement phase
        self._log("Measurement phase...")
        measurement_start = time.perf_counter()
        measurement_files = 0

        while time.perf_counter() < test_end and not self._stop.is_set():
            self._create_file(index)
            index += 1
            measurement_files += 1

            # Try to maintain target RPS
            elapsed = time.perf_counter() - measurement_start
            expected = measurement_files * interval
            if expected > elapsed:
                time.sleep(expected - elapsed)

        measurement_end = time.perf_counter()

        # Cooldown
        self._log(f"Cooldown phase ({self.config.cooldown_sec}s)...")
        time.sleep(self.config.cooldown_sec)

        # Calculate results
        result.total_files_created = self._files_created
        result.duration_sec = measurement_end - measurement_start
        result.actual_rps = measurement_files / result.duration_sec if result.duration_sec > 0 else 0

        self._log("")
        self._log("=" * 60)
        self._log("BENCHMARK RESULTS")
        self._log("=" * 60)
        self._log(f"Files created:     {result.total_files_created}")
        self._log(f"Duration:          {result.duration_sec:.2f}s")
        self._log(f"Target RPS:        {self.config.target_rps}")
        self._log(f"Actual RPS:        {result.actual_rps:.2f}")
        self._log("")
        self._log("Note: Latency measurement requires Kafka event tracking")
        self._log("      Use Grafana dashboard for end-to-end latency metrics")
        self._log("=" * 60)

        return result

    def stop(self) -> None:
        """Signal the generator to stop."""
        self._stop.set()


def main() -> None:
    """Run the load generator benchmark."""
    parser = argparse.ArgumentParser(
        description="Tapline Load Generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--rps",
        type=float,
        default=10.0,
        help="Target requests (files) per second",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=5,
        help="Warmup period in seconds (results discarded)",
    )
    parser.add_argument(
        "--cooldown",
        type=int,
        default=5,
        help="Cooldown period in seconds",
    )
    parser.add_argument(
        "--directory",
        type=Path,
        default=None,
        help="Directory to place test files (default: temp directory)",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=640,
        help="Test image width",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=480,
        help="Test image height",
    )

    args = parser.parse_args()

    # Use temp directory if not specified
    if args.directory is None:
        temp_dir = tempfile.mkdtemp(prefix="tapline_bench_")
        args.directory = Path(temp_dir)
        sys.stdout.write(f"Using temp directory: {temp_dir}\n")

    config = BenchmarkConfig(
        target_rps=args.rps,
        duration_sec=args.duration,
        warmup_sec=args.warmup,
        cooldown_sec=args.cooldown,
        directory=args.directory,
        image_width=args.width,
        image_height=args.height,
    )

    generator = LoadGenerator(config)

    try:
        generator.run()
    except KeyboardInterrupt:
        sys.stdout.write("\nBenchmark interrupted by user\n")
        generator.stop()


if __name__ == "__main__":
    main()
