"""Kafka-specific benchmarks for producer/consumer performance.

Measures:
- Producer throughput (messages/second)
- Consumer throughput (messages/second)
- End-to-end latency (produce -> consume)

Usage:
    python -m benchmarks.kafka_benchmark --messages 10000
"""

from __future__ import annotations

import argparse
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from src.domain.events import EventEnvelope, EventType
from src.streaming.config import load_kafka_config
from src.streaming.consumer import EventConsumer
from src.streaming.producer import EventProducer

# Constants for benchmark thresholds
_MIN_SAMPLES_FOR_PERCENTILE = 2
_PROGRESS_INTERVAL = 1000


@dataclass
class KafkaBenchmarkResult:
    """Results from Kafka benchmark."""

    messages_sent: int = 0
    messages_received: int = 0
    produce_duration_sec: float = 0.0
    consume_duration_sec: float = 0.0
    roundtrip_latencies_ms: list[float] = field(default_factory=list)

    @property
    def produce_throughput_msg_sec(self) -> float:
        """Calculate producer throughput in messages per second."""
        if self.produce_duration_sec <= 0:
            return 0.0
        return self.messages_sent / self.produce_duration_sec

    @property
    def consume_throughput_msg_sec(self) -> float:
        """Calculate consumer throughput in messages per second."""
        if self.consume_duration_sec <= 0:
            return 0.0
        return self.messages_received / self.consume_duration_sec

    @property
    def avg_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        if not self.roundtrip_latencies_ms:
            return 0.0
        return statistics.mean(self.roundtrip_latencies_ms)

    @property
    def p99_latency_ms(self) -> float:
        """Calculate P99 latency in milliseconds."""
        if len(self.roundtrip_latencies_ms) < _MIN_SAMPLES_FOR_PERCENTILE:
            return self.avg_latency_ms
        return statistics.quantiles(self.roundtrip_latencies_ms, n=100)[98]

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "produce_duration_sec": self.produce_duration_sec,
            "consume_duration_sec": self.consume_duration_sec,
            "produce_throughput_msg_sec": self.produce_throughput_msg_sec,
            "consume_throughput_msg_sec": self.consume_throughput_msg_sec,
            "avg_latency_ms": self.avg_latency_ms,
            "p99_latency_ms": self.p99_latency_ms,
        }


class KafkaBenchmark:
    """Benchmark Kafka produce/consume performance."""

    def __init__(self, config_path: Path | None = None) -> None:
        """Initialize benchmark with config path.

        Args:
            config_path: Path to Kafka config file.

        """
        self.config_path = config_path or Path("src/config/kafka.toml")

    def _create_producer(self) -> EventProducer:
        """Create an EventProducer instance."""
        config = load_kafka_config(self.config_path)
        return EventProducer(config)

    def _create_consumer(self, topics: list[str], group_id: str) -> EventConsumer:
        """Create an EventConsumer instance."""
        config = load_kafka_config(self.config_path)
        return EventConsumer(config, topics=topics, group_id=group_id)

    def _log(self, message: str) -> None:
        """Log a message to stdout."""
        sys.stdout.write(message + "\n")
        sys.stdout.flush()

    def benchmark_producer(self, message_count: int) -> dict[str, Any]:
        """Benchmark raw producer throughput.

        Args:
            message_count: Number of messages to produce.

        Returns:
            Dict with throughput metrics.

        """
        topic = f"bench-producer-{int(time.time())}"

        self._log(f"Benchmarking producer: {message_count} messages to {topic}")

        producer = self._create_producer()

        start = time.perf_counter()

        for i in range(message_count):
            event = EventEnvelope.create(
                event_type=EventType.JOB_CREATED,
                source_id=f"bench-{i % 32}",  # Spread across partitions
                payload={"benchmark_id": i, "timestamp": time.time()},
            )
            producer.publish(topic, event)

            # Progress indicator
            if (i + 1) % _PROGRESS_INTERVAL == 0:
                self._log(f"  Produced {i + 1}/{message_count}")

        # Wait for all messages to be delivered
        producer.flush(timeout=30.0)
        end = time.perf_counter()

        producer.close()

        duration = end - start
        throughput = message_count / duration

        self._log("Producer benchmark complete:")
        self._log(f"  Duration: {duration:.2f}s")
        self._log(f"  Throughput: {throughput:.0f} msg/s")

        return {
            "messages": message_count,
            "duration_sec": duration,
            "throughput_msg_sec": throughput,
        }

    def benchmark_consumer(
        self,
        message_count: int,
        timeout_sec: float = 60.0,
    ) -> dict[str, Any]:
        """Benchmark consumer throughput.

        First produces messages, then measures consume rate.

        Args:
            message_count: Number of messages to consume.
            timeout_sec: Maximum time to wait for messages.

        Returns:
            Dict with throughput metrics.

        """
        topic = f"bench-consumer-{int(time.time())}"
        group_id = f"bench-group-{int(time.time())}"

        self._log(f"Benchmarking consumer: {message_count} messages from {topic}")

        # First, produce messages
        self._log("  Producing test messages...")
        producer = self._create_producer()

        for i in range(message_count):
            event = EventEnvelope.create(
                event_type=EventType.JOB_CREATED,
                source_id=f"bench-{i % 32}",
                payload={"benchmark_id": i},
            )
            producer.publish(topic, event)

        producer.flush()
        producer.close()
        self._log(f"  Produced {message_count} messages")

        # Now benchmark consumption
        self._log("  Consuming messages...")
        consumer = self._create_consumer(topics=[topic], group_id=group_id)

        consumed = 0
        start = time.perf_counter()

        while consumed < message_count:
            if time.perf_counter() - start > timeout_sec:
                self._log(f"  Timeout after {consumed} messages")
                break

            event = consumer.poll(timeout=1.0)
            if event:
                consumed += 1
                consumer.commit()

                if consumed % _PROGRESS_INTERVAL == 0:
                    self._log(f"  Consumed {consumed}/{message_count}")

        end = time.perf_counter()
        consumer.close()

        duration = end - start
        throughput = consumed / duration if duration > 0 else 0

        self._log("Consumer benchmark complete:")
        self._log(f"  Consumed: {consumed}")
        self._log(f"  Duration: {duration:.2f}s")
        self._log(f"  Throughput: {throughput:.0f} msg/s")

        return {
            "messages_consumed": consumed,
            "duration_sec": duration,
            "throughput_msg_sec": throughput,
        }

    def benchmark_roundtrip(self, message_count: int) -> KafkaBenchmarkResult:
        """Benchmark full produce-consume cycle with latency measurement.

        Args:
            message_count: Number of messages to benchmark.

        Returns:
            KafkaBenchmarkResult with latencies.

        """
        topic = f"bench-roundtrip-{int(time.time())}"
        group_id = f"bench-group-{int(time.time())}"

        self._log(f"Benchmarking roundtrip: {message_count} messages on {topic}")

        result = KafkaBenchmarkResult()
        send_times: dict[str, float] = {}
        receive_times: dict[str, float] = {}
        consumer_done = threading.Event()

        def consume_messages() -> None:
            consumer = self._create_consumer(topics=[topic], group_id=group_id)
            received = 0

            while received < message_count:
                event = consumer.poll(timeout=1.0)
                if event:
                    event_id = event.payload.get("benchmark_id")
                    receive_times[str(event_id)] = time.perf_counter()
                    received += 1
                    consumer.commit()

            consumer.close()
            consumer_done.set()

        # Start consumer thread
        consumer_thread = threading.Thread(target=consume_messages, daemon=True)
        consumer_thread.start()

        # Give consumer time to subscribe
        time.sleep(2.0)

        # Produce messages
        producer = self._create_producer()
        produce_start = time.perf_counter()

        for i in range(message_count):
            send_times[str(i)] = time.perf_counter()

            event = EventEnvelope.create(
                event_type=EventType.JOB_CREATED,
                source_id=f"bench-{i % 32}",
                payload={"benchmark_id": i},
            )
            producer.publish(topic, event)

        producer.flush()
        produce_end = time.perf_counter()
        producer.close()

        result.messages_sent = message_count
        result.produce_duration_sec = produce_end - produce_start

        self._log(
            f"  Produced {message_count} messages in {result.produce_duration_sec:.2f}s",
        )

        # Wait for consumer
        consumer_done.wait(timeout=60.0)
        consumer_thread.join(timeout=5.0)

        result.messages_received = len(receive_times)

        # Calculate latencies
        for msg_id, send_time in send_times.items():
            if msg_id in receive_times:
                latency_ms = (receive_times[msg_id] - send_time) * 1000
                result.roundtrip_latencies_ms.append(latency_ms)

        if result.roundtrip_latencies_ms:
            result.consume_duration_sec = (
                max(receive_times.values()) - min(receive_times.values())
            )

        self._log("")
        self._log("=" * 60)
        self._log("ROUNDTRIP BENCHMARK RESULTS")
        self._log("=" * 60)
        self._log(f"Messages sent:       {result.messages_sent}")
        self._log(f"Messages received:   {result.messages_received}")
        self._log(f"Produce duration:    {result.produce_duration_sec:.2f}s")
        self._log(f"Produce throughput:  {result.produce_throughput_msg_sec:.0f} msg/s")
        self._log(f"Consume throughput:  {result.consume_throughput_msg_sec:.0f} msg/s")
        self._log(f"Avg latency:         {result.avg_latency_ms:.2f}ms")
        self._log(f"P99 latency:         {result.p99_latency_ms:.2f}ms")
        self._log("=" * 60)

        return result


def main() -> None:
    """Run the Kafka benchmark."""
    # Add project root to path for imports
    sys.path.insert(0, str(Path(__file__).parent.parent))
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Kafka Benchmark Tool",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=1000,
        help="Number of messages to benchmark",
    )
    parser.add_argument(
        "--mode",
        choices=["producer", "consumer", "roundtrip", "all"],
        default="roundtrip",
        help="Benchmark mode",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("src/config/kafka.toml"),
        help="Path to Kafka config",
    )

    args = parser.parse_args()

    benchmark = KafkaBenchmark(args.config)

    if args.mode in ("producer", "all"):
        sys.stdout.write("\n" + "=" * 60 + "\n")
        sys.stdout.write("PRODUCER BENCHMARK\n")
        sys.stdout.write("=" * 60 + "\n")
        benchmark.benchmark_producer(args.messages)

    if args.mode in ("consumer", "all"):
        sys.stdout.write("\n" + "=" * 60 + "\n")
        sys.stdout.write("CONSUMER BENCHMARK\n")
        sys.stdout.write("=" * 60 + "\n")
        benchmark.benchmark_consumer(args.messages)

    if args.mode in ("roundtrip", "all"):
        sys.stdout.write("\n" + "=" * 60 + "\n")
        sys.stdout.write("ROUNDTRIP BENCHMARK\n")
        sys.stdout.write("=" * 60 + "\n")
        benchmark.benchmark_roundtrip(args.messages)


if __name__ == "__main__":
    main()
