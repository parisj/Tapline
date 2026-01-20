"""Prometheus HTTP API client for querying metrics."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from typing import Any

logger = get_logger(__name__)


@dataclass
class MetricSample:
    """A single metric sample from Prometheus."""

    name: str
    value: float
    timestamp: datetime
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class RangeVector:
    """Time-series data from a range query."""

    metric_name: str
    labels: dict[str, str]
    values: list[tuple[datetime, float]]  # (timestamp, value) pairs


class PrometheusClient:
    """Client for querying Prometheus metrics via HTTP API."""

    # Allowed URL schemes for security (prevent file:// or other schemes)
    _ALLOWED_SCHEMES = frozenset({"http", "https"})

    def __init__(self, base_url: str = "http://localhost:9091") -> None:
        """Initialize Prometheus client.

        Args:
            base_url: Prometheus server URL (default: http://localhost:9091)

        Raises:
            ValueError: If URL scheme is not http or https.

        """
        # Security: validate URL scheme
        from urllib.parse import urlparse

        parsed = urlparse(base_url)
        if parsed.scheme not in self._ALLOWED_SCHEMES:
            msg = f"Invalid URL scheme '{parsed.scheme}'. Only http/https allowed."
            raise ValueError(msg)

        self.base_url = base_url.rstrip("/")
        self._available: bool | None = None

    def is_available(self) -> bool:
        """Check if Prometheus is reachable."""
        if self._available is not None:
            return self._available

        try:
            url = f"{self.base_url}/-/healthy"
            # URL scheme validated in __init__
            with urlopen(url, timeout=2) as resp:  # noqa: S310
                self._available = resp.status == 200
        except (URLError, TimeoutError):
            self._available = False
            logger.warning("Prometheus not available at %s", self.base_url)

        return self._available

    def query(self, promql: str) -> list[MetricSample]:
        """Execute instant PromQL query.

        Args:
            promql: PromQL query string

        Returns:
            List of metric samples

        """
        if not self.is_available():
            return []

        try:
            url = f"{self.base_url}/api/v1/query?{urlencode({'query': promql})}"
            # URL scheme validated in __init__
            with urlopen(url, timeout=5) as resp:  # noqa: S310
                data = json.loads(resp.read())
                return self._parse_instant_response(data)
        except (URLError, TimeoutError, json.JSONDecodeError) as e:
            logger.warning("Prometheus query failed: %s", e)
            return []

    def query_range(
        self,
        promql: str,
        start: datetime,
        end: datetime,
        step: str = "1m",
    ) -> list[RangeVector]:
        """Execute range query for time-series data.

        Args:
            promql: PromQL query string
            start: Start time
            end: End time
            step: Query resolution (default: 1m)

        Returns:
            List of range vectors with time-series data

        """
        if not self.is_available():
            return []

        try:
            params = {
                "query": promql,
                "start": start.timestamp(),
                "end": end.timestamp(),
                "step": step,
            }
            url = f"{self.base_url}/api/v1/query_range?{urlencode(params)}"
            # URL scheme validated in __init__
            with urlopen(url, timeout=10) as resp:  # noqa: S310
                data = json.loads(resp.read())
                return self._parse_range_response(data)
        except (URLError, TimeoutError, json.JSONDecodeError) as e:
            logger.warning("Prometheus range query failed: %s", e)
            return []

    def _parse_instant_response(self, data: dict[str, Any]) -> list[MetricSample]:
        """Parse instant query response."""
        samples = []

        if data.get("status") != "success":
            logger.warning("Prometheus query returned error: %s", data.get("error"))
            return samples

        result = data.get("data", {}).get("result", [])

        for item in result:
            metric = item.get("metric", {})
            value_pair = item.get("value", [])

            if len(value_pair) >= 2:
                timestamp = datetime.fromtimestamp(float(value_pair[0]))
                try:
                    value = float(value_pair[1])
                except (ValueError, TypeError):
                    continue

                name = metric.pop("__name__", "unknown")
                samples.append(
                    MetricSample(
                        name=name,
                        value=value,
                        timestamp=timestamp,
                        labels=metric,
                    ),
                )

        return samples

    def _parse_range_response(self, data: dict[str, Any]) -> list[RangeVector]:
        """Parse range query response."""
        vectors = []

        if data.get("status") != "success":
            logger.warning("Prometheus range query returned error: %s", data.get("error"))
            return vectors

        result = data.get("data", {}).get("result", [])

        for item in result:
            metric = item.get("metric", {})
            values = item.get("values", [])

            name = metric.pop("__name__", "unknown")
            parsed_values = []

            for value_pair in values:
                if len(value_pair) >= 2:
                    timestamp = datetime.fromtimestamp(float(value_pair[0]))
                    try:
                        value = float(value_pair[1])
                        parsed_values.append((timestamp, value))
                    except (ValueError, TypeError):
                        continue

            if parsed_values:
                vectors.append(
                    RangeVector(
                        metric_name=name,
                        labels=metric,
                        values=parsed_values,
                    ),
                )

        return vectors

    def get_jobs_total(self) -> float:
        """Get total jobs completed."""
        samples = self.query("sum(visioeval_jobs_completed_total)")
        return samples[0].value if samples else 0.0

    def get_jobs_failed(self) -> float:
        """Get total failed jobs."""
        samples = self.query("sum(visioeval_jobs_failed_total)")
        return samples[0].value if samples else 0.0

    def get_throughput(self) -> float:
        """Get current throughput (jobs/sec)."""
        samples = self.query("sum(rate(visioeval_jobs_completed_total[1m]))")
        return samples[0].value if samples else 0.0

    def get_active_workers(self) -> float:
        """Get number of active workers."""
        samples = self.query("visioeval_workers_active")
        return samples[0].value if samples else 0.0

    def get_metric_value(self, promql: str, default: float = 0.0) -> float:
        """Get a single metric value.

        Args:
            promql: PromQL query string
            default: Default value if query fails

        Returns:
            Metric value or default

        """
        samples = self.query(promql)
        return samples[0].value if samples else default

    def get_throughput_history(
        self,
        hours: float = 1.0,
        step: str = "1m",
    ) -> list[tuple[datetime, float]]:
        """Get throughput history for time-series chart.

        Args:
            hours: Number of hours of history
            step: Query resolution

        Returns:
            List of (timestamp, value) tuples

        """
        from datetime import timedelta

        end = datetime.now()
        start = end - timedelta(hours=hours)

        vectors = self.query_range(
            "sum(rate(visioeval_jobs_completed_total[1m]))",
            start=start,
            end=end,
            step=step,
        )

        if vectors:
            return vectors[0].values
        return []
