"""Prometheus proxy service.

Handles Prometheus API requests with proper error handling and response formatting.
Extracted from api_server.py for better testability and separation of concerns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests

from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class PrometheusQueryResult:
    """Result of a Prometheus query."""

    success: bool
    data: dict[str, Any]
    error: str | None = None


@dataclass
class PipelineStatus:
    """Pipeline worker status."""

    pipeline_running: bool
    workers_active: int
    worker_pool_size: int
    status: str
    detail: str


class PrometheusService:
    """Service for interacting with Prometheus API."""

    def __init__(self, base_url: str = "http://localhost:9091", timeout: int = 10) -> None:
        """Initialize Prometheus service.

        Args:
            base_url: Prometheus server URL.
            timeout: Default request timeout in seconds.

        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def is_healthy(self) -> bool:
        """Check if Prometheus is reachable.

        Returns:
            True if Prometheus is healthy, False otherwise.

        """
        try:
            resp = requests.get(f"{self.base_url}/-/healthy", timeout=5)
            return bool(resp.ok)
        except requests.RequestException:
            return False

    def query(self, promql: str) -> PrometheusQueryResult:
        """Execute instant PromQL query.

        Args:
            promql: PromQL query string.

        Returns:
            PrometheusQueryResult with success status and data.

        """
        try:
            resp = requests.get(
                f"{self.base_url}/api/v1/query",
                params={"query": promql},
                timeout=self.timeout,
            )
            return PrometheusQueryResult(success=True, data=resp.json())
        except requests.RequestException as e:
            logger.warning("Prometheus query failed: %s", e)
            return PrometheusQueryResult(
                success=False,
                data={"status": "error", "error": str(e), "data": {"result": []}},
                error=str(e),
            )

    def query_range(
        self,
        promql: str,
        start: str,
        end: str,
        step: str = "60",
    ) -> PrometheusQueryResult:
        """Execute range PromQL query.

        Args:
            promql: PromQL query string.
            start: Start time (Unix timestamp or RFC3339).
            end: End time (Unix timestamp or RFC3339).
            step: Query resolution step.

        Returns:
            PrometheusQueryResult with success status and data.

        """
        try:
            resp = requests.get(
                f"{self.base_url}/api/v1/query_range",
                params={"query": promql, "start": start, "end": end, "step": step},
                timeout=30,
            )
            return PrometheusQueryResult(success=True, data=resp.json())
        except requests.RequestException as e:
            logger.warning("Prometheus range query failed: %s", e)
            return PrometheusQueryResult(
                success=False,
                data={"status": "error", "error": str(e), "data": {"result": []}},
                error=str(e),
            )

    def get_metric_value(self, promql: str, default: float = 0.0) -> float:
        """Get a single metric value.

        Args:
            promql: PromQL query string.
            default: Default value if query fails.

        Returns:
            Metric value or default.

        """
        result = self.query(promql)
        if not result.success:
            return default

        try:
            data = result.data
            if data.get("status") == "success":
                results = data.get("data", {}).get("result", [])
                if results:
                    return float(results[0].get("value", [0, default])[1])
        except (IndexError, KeyError, TypeError, ValueError):
            pass

        return default

    def get_pipeline_status(self) -> PipelineStatus:
        """Get pipeline worker status from Prometheus metrics.

        Returns:
            PipelineStatus with worker and pipeline information.

        """
        workers_active = 0
        worker_pool_size = 0
        pipeline_up = False

        try:
            # Query pipeline_up to check if pipeline is running
            pipeline_up = self.get_metric_value("visioeval_pipeline_up") == 1.0

            # Query workers_active (number currently processing jobs)
            workers_active = int(self.get_metric_value("visioeval_workers_active"))

            # Query worker_pool_size
            worker_pool_size = int(self.get_metric_value("visioeval_worker_pool_size"))
        except Exception as e:
            logger.warning("Failed to get pipeline status: %s", e)

        # Determine status and detail message
        if pipeline_up:
            if workers_active > 0:
                status = "healthy"
                detail = f"{workers_active}/{worker_pool_size} processing"
            else:
                status = "healthy"  # Pipeline is running, workers are idle
                detail = f"{worker_pool_size} workers idle"
        else:
            status = "stopped"
            detail = "Pipeline not running"

        return PipelineStatus(
            pipeline_running=pipeline_up,
            workers_active=workers_active,
            worker_pool_size=worker_pool_size,
            status=status,
            detail=detail,
        )


class JaegerService:
    """Service for interacting with Jaeger API."""

    def __init__(self, base_url: str = "http://localhost:16686", timeout: int = 5) -> None:
        """Initialize Jaeger service.

        Args:
            base_url: Jaeger server URL.
            timeout: Default request timeout in seconds.

        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def is_healthy(self) -> bool:
        """Check if Jaeger is reachable.

        Returns:
            True if Jaeger is healthy, False otherwise.

        """
        try:
            resp = requests.get(f"{self.base_url}/api/services", timeout=self.timeout)
            return bool(resp.ok)
        except requests.RequestException:
            return False

    def get_services(self) -> dict[str, Any]:
        """Get list of services from Jaeger.

        Returns:
            Dictionary with healthy status and data.

        """
        try:
            resp = requests.get(f"{self.base_url}/api/services", timeout=self.timeout)
            if resp.ok:
                return {"healthy": True, "data": resp.json()}
            return {"healthy": False, "error": "Jaeger returned error"}
        except requests.RequestException as e:
            return {"healthy": False, "error": str(e)}
