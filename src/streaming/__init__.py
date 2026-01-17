"""Streaming module for Kafka-based event processing.

Provides producer and consumer abstractions for the VisioEval
event streaming architecture.
"""

from src.streaming.config import KafkaConfig, load_kafka_config
from src.streaming.consumer import EventConsumer
from src.streaming.producer import EventProducer

__all__ = [
    "EventConsumer",
    "EventProducer",
    "KafkaConfig",
    "load_kafka_config",
]
