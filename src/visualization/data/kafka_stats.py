"""Kafka topic statistics reader for dashboard metrics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from confluent_kafka.admin import AdminClient

logger = get_logger(__name__)


@dataclass
class TopicStats:
    """Statistics for a Kafka topic."""

    name: str
    partition_count: int
    message_count: int
    consumer_lag: int


class KafkaStatsReader:
    """Reader for Kafka topic statistics."""

    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        """Initialize Kafka stats reader.

        Args:
            bootstrap_servers: Kafka broker address

        """
        self.bootstrap_servers = bootstrap_servers
        self._admin: AdminClient | None = None
        self._available: bool | None = None

    def _get_admin(self) -> AdminClient | None:
        """Get or create admin client."""
        if self._admin is not None:
            return self._admin

        try:
            from confluent_kafka.admin import AdminClient

            self._admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
            return self._admin
        except ImportError:
            logger.warning("confluent_kafka not available")
            return None
        except Exception as e:
            logger.warning("Failed to create Kafka admin client: %s", e)
            return None

    def is_available(self) -> bool:
        """Check if Kafka is reachable."""
        if self._available is not None:
            return self._available

        admin = self._get_admin()
        if admin is None:
            self._available = False
            return False

        try:
            # Try to list topics with a short timeout
            cluster_metadata = admin.list_topics(timeout=2)
            self._available = cluster_metadata is not None
        except Exception as e:
            logger.warning("Kafka not available: %s", e)
            self._available = False

        return self._available

    def get_topic_stats(self, topic_name: str) -> TopicStats | None:
        """Get statistics for a specific topic.

        Args:
            topic_name: Name of the Kafka topic

        Returns:
            TopicStats or None if unavailable

        """
        if not self.is_available():
            return None

        admin = self._get_admin()
        if admin is None:
            return None

        try:
            cluster_metadata = admin.list_topics(topic=topic_name, timeout=5)
            topic_metadata = cluster_metadata.topics.get(topic_name)

            if topic_metadata is None:
                return None

            partition_count = len(topic_metadata.partitions)

            return TopicStats(
                name=topic_name,
                partition_count=partition_count,
                message_count=0,  # Would need consumer to get accurate count
                consumer_lag=0,  # Would need consumer group info
            )
        except Exception as e:
            logger.warning("Failed to get topic stats for %s: %s", topic_name, e)
            return None

    def get_all_topics(self) -> list[str]:
        """Get list of all topics.

        Returns:
            List of topic names

        """
        if not self.is_available():
            return []

        admin = self._get_admin()
        if admin is None:
            return []

        try:
            cluster_metadata = admin.list_topics(timeout=5)
            return list(cluster_metadata.topics.keys())
        except Exception as e:
            logger.warning("Failed to list topics: %s", e)
            return []

    def get_visio_topics(self) -> list[str]:
        """Get list of VisioEval-related topics.

        Returns:
            List of visio.* topic names

        """
        all_topics = self.get_all_topics()
        return [t for t in all_topics if t.startswith("visio.")]
