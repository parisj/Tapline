"""Integration tests for Kafka produce/consume cycle."""

import time

from src.domain.events import EventType


class TestKafkaRoundtripIntegration:
    """Integration tests for Kafka event streaming."""

    def test_publish_job_created(self, kafka_producer) -> None:
        """Test publishing a JOB_CREATED event."""
        event = kafka_producer.publish_job_created(
            source_id="test-source",
            job_id="test-job-123",
            directory_key="path0",
            path="/data/test.png",
            fingerprint="fp123",
            file_hash="hash456",
        )

        assert event.event_type == EventType.JOB_CREATED
        assert event.source_id == "test-source"
        assert event.payload["job_id"] == "test-job-123"

        # Flush to ensure delivery
        remaining = kafka_producer.flush(timeout=5.0)
        assert remaining == 0

    def test_publish_job_lifecycle(self, kafka_producer) -> None:
        """Test publishing full job lifecycle events."""
        source_id = "test-source"
        job_id = "lifecycle-job-123"

        # Create
        created = kafka_producer.publish_job_created(
            source_id=source_id,
            job_id=job_id,
            directory_key="path0",
            path="/data/test.png",
            fingerprint="fp123",
        )

        # Start
        started = kafka_producer.publish_job_started(
            source_id=source_id,
            job_id=job_id,
            algo_name="analysis_probe",
            algo_version="1.0.0",
        )

        # Complete
        completed = kafka_producer.publish_job_completed(
            source_id=source_id,
            job_id=job_id,
            algo_name="analysis_probe",
            algo_version="1.0.0",
            duration_ms=150.5,
        )

        # Verify chain linkage
        assert started.prev_hash == created.content_hash
        assert completed.prev_hash == started.content_hash

        kafka_producer.flush(timeout=5.0)

    def test_publish_metrics(self, kafka_producer) -> None:
        """Test publishing metric events."""
        events = []
        for i in range(5):
            event = kafka_producer.publish_metric_emitted(
                source_id="analysis_probe",
                job_id=f"metric-job-{i}",
                algo_name="analysis_probe",
                algo_version="1.0.0",
                metric_name="accuracy",
                value=0.9 + i * 0.01,
                analysis_mask=1,
            )
            events.append(event)

        kafka_producer.flush(timeout=5.0)

        # All events should have same source_id (algo_name for metrics)
        for event in events:
            assert event.source_id == "analysis_probe"

    def test_delivery_stats(self, kafka_producer) -> None:
        """Test that delivery statistics are tracked."""
        initial_delivered, _initial_failed = kafka_producer.delivery_stats

        kafka_producer.publish_job_created(
            source_id="stats-test",
            job_id="stats-job-123",
            directory_key="path0",
            path="/data/test.png",
            fingerprint="fp123",
        )

        kafka_producer.flush(timeout=5.0)

        # Give time for delivery callback
        time.sleep(0.5)

        final_delivered, _final_failed = kafka_producer.delivery_stats

        # At least one more message delivered (possibly more from audit log)
        assert final_delivered > initial_delivered
