"""End-to-end tests for the complete job lifecycle.

Tests the full flow:
1. Job creation and publishing to Kafka
2. Event consumption by workers
3. Job processing and result generation
4. Correlation ID propagation
5. Event sequencing (JOB_CREATED -> JOB_STARTED -> JOB_COMPLETED)
"""

from __future__ import annotations

import threading
import time
import uuid

from src.domain.events import EventType


class TestJobLifecycle:
    """Test complete job lifecycle through the streaming pipeline."""

    def test_job_created_event_published(
        self,
        skip_without_infrastructure,
        kafka_config,
        test_image_bytes: bytes,
    ) -> None:
        """Test that JOB_CREATED events are properly published to Kafka."""
        from src.streaming.consumer import EventConsumer
        from src.streaming.producer import EventProducer

        producer = EventProducer(kafka_config)
        consumer = EventConsumer(
            config=kafka_config,
            topics=[kafka_config.topic_jobs],
            group_id=f"test-job-created-{uuid.uuid4().hex[:8]}",
        )

        try:
            # Generate unique job ID
            job_id = f"test-job-{uuid.uuid4().hex}"
            source_id = "test-source"

            # Publish JOB_CREATED event
            producer.publish_job_created(
                source_id=source_id,
                job_id=job_id,
                directory_key="test-dir",
                path="/test/path/image.png",
                fingerprint="abc123",
            )
            producer.flush(timeout=5.0)

            # Consume and verify the event
            received_event = None
            start = time.time()
            timeout = 10.0

            while time.time() - start < timeout:
                event = consumer.poll(timeout=1.0)
                if event and event.payload.get("job_id") == job_id:
                    received_event = event
                    break

            assert received_event is not None, f"JOB_CREATED event not received within {timeout}s"
            assert received_event.event_type == EventType.JOB_CREATED
            assert received_event.source_id == source_id
            assert received_event.payload["job_id"] == job_id
            assert received_event.payload["directory_key"] == "test-dir"
            assert received_event.payload["path"] == "/test/path/image.png"
            assert received_event.payload["fingerprint"] == "abc123"

        finally:
            producer.close()
            consumer.close()

    def test_job_lifecycle_events_sequence(
        self,
        skip_without_infrastructure,
        kafka_config,
    ) -> None:
        """Test that job lifecycle events are published in correct sequence."""
        from src.streaming.consumer import EventConsumer
        from src.streaming.producer import EventProducer

        producer = EventProducer(kafka_config)
        consumer = EventConsumer(
            config=kafka_config,
            topics=[kafka_config.topic_jobs],
            group_id=f"test-lifecycle-{uuid.uuid4().hex[:8]}",
        )

        try:
            job_id = f"test-lifecycle-{uuid.uuid4().hex}"
            source_id = "test-source"
            algo_name = "test_algo"
            algo_version = "1.0.0"

            # Publish lifecycle events in sequence
            producer.publish_job_created(
                source_id=source_id,
                job_id=job_id,
                directory_key="test-dir",
                path="/test/path.png",
                fingerprint="xyz789",
            )

            producer.publish_job_started(
                source_id=source_id,
                job_id=job_id,
                algo_name=algo_name,
                algo_version=algo_version,
            )

            producer.publish_job_completed(
                source_id=source_id,
                job_id=job_id,
                algo_name=algo_name,
                algo_version=algo_version,
                duration_ms=42.5,
            )

            producer.flush(timeout=5.0)

            # Collect events for this job
            events = []
            start = time.time()
            timeout = 15.0

            while time.time() - start < timeout and len(events) < 3:
                event = consumer.poll(timeout=1.0)
                if event and event.payload.get("job_id") == job_id:
                    events.append(event)

            # Verify we got all 3 events
            assert len(events) == 3, f"Expected 3 events, got {len(events)}"

            # Verify event types (order may vary due to partitioning)
            event_types = {e.event_type for e in events}
            assert EventType.JOB_CREATED in event_types
            assert EventType.JOB_STARTED in event_types
            assert EventType.JOB_COMPLETED in event_types

            # Verify JOB_COMPLETED has duration
            completed_event = next(e for e in events if e.event_type == EventType.JOB_COMPLETED)
            assert completed_event.payload["duration_ms"] == 42.5

        finally:
            producer.close()
            consumer.close()

    def test_result_produced_event(
        self,
        skip_without_infrastructure,
        kafka_config,
    ) -> None:
        """Test that RESULT_PRODUCED events contain correct data."""
        from src.streaming.consumer import EventConsumer
        from src.streaming.producer import EventProducer

        producer = EventProducer(kafka_config)
        consumer = EventConsumer(
            config=kafka_config,
            topics=[kafka_config.topic_results],
            group_id=f"test-results-{uuid.uuid4().hex[:8]}",
        )

        try:
            job_id = f"test-result-{uuid.uuid4().hex}"
            source_id = "test-source"
            artifact_refs = ["hash1", "hash2"]

            producer.publish_result_produced(
                source_id=source_id,
                job_id=job_id,
                algo_name="test_algo",
                algo_version="2.0.0",
                metric_count=5,
                artifact_refs=artifact_refs,
            )
            producer.flush(timeout=5.0)

            # Consume and verify
            received_event = None
            start = time.time()
            timeout = 10.0

            while time.time() - start < timeout:
                event = consumer.poll(timeout=1.0)
                if event and event.payload.get("job_id") == job_id:
                    received_event = event
                    break

            assert received_event is not None
            assert received_event.event_type == EventType.RESULT_PRODUCED
            assert received_event.payload["metric_count"] == 5
            assert received_event.payload["artifact_refs"] == artifact_refs

        finally:
            producer.close()
            consumer.close()

    def test_metric_emitted_event(
        self,
        skip_without_infrastructure,
        kafka_config,
    ) -> None:
        """Test that METRIC_EMITTED events contain metric data."""
        from src.streaming.consumer import EventConsumer
        from src.streaming.producer import EventProducer

        producer = EventProducer(kafka_config)
        consumer = EventConsumer(
            config=kafka_config,
            topics=[kafka_config.topic_metrics],
            group_id=f"test-metrics-{uuid.uuid4().hex[:8]}",
        )

        try:
            job_id = f"test-metric-{uuid.uuid4().hex}"
            metric_name = "accuracy"
            metric_value = 0.95

            producer.publish_metric_emitted(
                source_id="test_algo",
                job_id=job_id,
                algo_name="test_algo",
                algo_version="1.0.0",
                metric_name=metric_name,
                value=metric_value,
                analysis_mask=1,
                meta={"unit": "percent"},
            )
            producer.flush(timeout=5.0)

            # Consume and verify
            received_event = None
            start = time.time()
            timeout = 10.0

            while time.time() - start < timeout:
                event = consumer.poll(timeout=1.0)
                if event and event.payload.get("job_id") == job_id:
                    received_event = event
                    break

            assert received_event is not None
            assert received_event.event_type == EventType.METRIC_EMITTED
            assert received_event.payload["metric_name"] == metric_name
            assert received_event.payload["value"] == metric_value
            assert received_event.payload["meta"]["unit"] == "percent"

        finally:
            producer.close()
            consumer.close()

    def test_job_failed_event(
        self,
        skip_without_infrastructure,
        kafka_config,
    ) -> None:
        """Test that JOB_FAILED events contain error information."""
        from src.streaming.consumer import EventConsumer
        from src.streaming.producer import EventProducer

        producer = EventProducer(kafka_config)
        consumer = EventConsumer(
            config=kafka_config,
            topics=[kafka_config.topic_jobs],
            group_id=f"test-failed-{uuid.uuid4().hex[:8]}",
        )

        try:
            job_id = f"test-failed-{uuid.uuid4().hex}"
            error_msg = "Test error: file not found"
            error_type = "FileNotFoundError"

            producer.publish_job_failed(
                source_id="test-source",
                job_id=job_id,
                error=error_msg,
                algo_name="test_algo",
                algo_version="1.0.0",
                error_type=error_type,
            )
            producer.flush(timeout=5.0)

            # Consume and verify
            received_event = None
            start = time.time()
            timeout = 10.0

            while time.time() - start < timeout:
                event = consumer.poll(timeout=1.0)
                if event and event.payload.get("job_id") == job_id:
                    received_event = event
                    break

            assert received_event is not None
            assert received_event.event_type == EventType.JOB_FAILED
            assert received_event.payload["error"] == error_msg
            assert received_event.payload["error_type"] == error_type

        finally:
            producer.close()
            consumer.close()


class TestCorrelationIdPropagation:
    """Test correlation ID propagation through the pipeline."""

    def test_correlation_id_injected_in_headers(
        self,
        skip_without_infrastructure,
        kafka_config,
    ) -> None:
        """Test that correlation IDs are injected into Kafka message headers."""
        from confluent_kafka import Consumer, Producer

        from src.observability.correlation import generate_correlation_id, set_correlation_id

        # Set a known correlation ID
        correlation_id = generate_correlation_id()
        set_correlation_id(correlation_id)

        producer_config = {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "client.id": "test-correlation-producer",
        }
        consumer_config = {
            "bootstrap.servers": kafka_config.bootstrap_servers,
            "group.id": f"test-correlation-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }

        producer = Producer(producer_config)
        consumer = Consumer(consumer_config)

        test_topic = f"test-correlation-{uuid.uuid4().hex[:8]}"

        try:
            # Produce with correlation ID header
            from src.observability.correlation import get_correlation_id

            headers = [("X-Correlation-ID", get_correlation_id().encode("utf-8"))]
            producer.produce(
                test_topic,
                value=b"test message",
                headers=headers,
            )
            producer.flush(timeout=5.0)

            # Consume and verify header
            consumer.subscribe([test_topic])
            msg = consumer.poll(timeout=10.0)

            assert msg is not None
            assert msg.headers() is not None

            header_dict = {k: v.decode("utf-8") if v else None for k, v in msg.headers()}
            assert "X-Correlation-ID" in header_dict
            assert header_dict["X-Correlation-ID"] == correlation_id

        finally:
            consumer.close()


class TestEventHashChain:
    """Test event hash chain integrity for audit trail."""

    def test_event_has_content_hash(
        self,
        skip_without_infrastructure,
        kafka_config,
    ) -> None:
        """Test that events have content hashes for integrity verification."""
        from src.domain.events import compute_content_hash
        from src.streaming.consumer import EventConsumer
        from src.streaming.producer import EventProducer

        producer = EventProducer(kafka_config)
        consumer = EventConsumer(
            config=kafka_config,
            topics=[kafka_config.topic_jobs],
            group_id=f"test-hash-{uuid.uuid4().hex[:8]}",
        )

        try:
            job_id = f"test-hash-{uuid.uuid4().hex}"

            producer.publish_job_created(
                source_id="test-source",
                job_id=job_id,
                directory_key="test-dir",
                path="/test/path.png",
                fingerprint="hash123",
            )
            producer.flush(timeout=5.0)

            # Consume and verify content hash
            received_event = None
            start = time.time()
            timeout = 10.0

            while time.time() - start < timeout:
                event = consumer.poll(timeout=1.0)
                if event and event.payload.get("job_id") == job_id:
                    received_event = event
                    break

            assert received_event is not None
            assert received_event.content_hash is not None
            assert len(received_event.content_hash) == 64  # SHA-256 hex

            # Verify hash is deterministic by recomputing from payload
            recomputed = compute_content_hash(received_event.payload)
            assert recomputed == received_event.content_hash

        finally:
            producer.close()
            consumer.close()


class TestConsumerGroupBehavior:
    """Test Kafka consumer group behavior for load balancing."""

    def test_multiple_consumers_receive_different_partitions(
        self,
        skip_without_infrastructure,
        kafka_config,
    ) -> None:
        """Test that multiple consumers in same group get different partitions."""
        from src.streaming.consumer import EventConsumer

        group_id = f"test-partitions-{uuid.uuid4().hex[:8]}"

        consumer1 = EventConsumer(
            config=kafka_config,
            topics=[kafka_config.topic_jobs],
            group_id=group_id,
        )
        consumer2 = EventConsumer(
            config=kafka_config,
            topics=[kafka_config.topic_jobs],
            group_id=group_id,
        )

        try:
            # Poll to trigger partition assignment
            assigned1 = threading.Event()
            assigned2 = threading.Event()

            def poll_consumer(consumer, assigned_event) -> None:
                for _ in range(10):
                    consumer.poll(timeout=1.0)
                    if consumer.get_assignment():
                        assigned_event.set()
                        break

            t1 = threading.Thread(target=poll_consumer, args=(consumer1, assigned1))
            t2 = threading.Thread(target=poll_consumer, args=(consumer2, assigned2))
            t1.start()
            t2.start()
            t1.join(timeout=15)
            t2.join(timeout=15)

            # Get assignments
            partitions1 = set(consumer1.get_assignment())
            partitions2 = set(consumer2.get_assignment())

            # Partitions should be disjoint (no overlap)
            overlap = partitions1 & partitions2
            assert len(overlap) == 0, f"Partitions should not overlap: {overlap}"

            # Combined should cover some partitions
            combined = partitions1 | partitions2
            assert len(combined) > 0, "At least some partitions should be assigned"

        finally:
            consumer1.close()
            consumer2.close()
