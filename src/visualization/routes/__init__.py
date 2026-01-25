"""Route blueprints for the Tapline dashboard API."""

from src.visualization.routes.artifacts import artifacts_bp
from src.visualization.routes.audit import audit_bp
from src.visualization.routes.kafka import kafka_bp
from src.visualization.routes.metrics import metrics_bp
from src.visualization.routes.prometheus import prometheus_bp

__all__ = [
    "artifacts_bp",
    "audit_bp",
    "kafka_bp",
    "metrics_bp",
    "prometheus_bp",
]
