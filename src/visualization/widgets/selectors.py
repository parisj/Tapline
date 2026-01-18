"""Cascading Select widgets for the dashboard."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from bokeh.models import Select

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from src.visualization.discovery import DiscoveryService, MetricInfo

logger = get_logger(__name__)


class DashboardSelectors:
    """Cascading Select widgets for directory/algorithm/metric selection.

    Provides interconnected Select widgets that update based on user selection:
    1. Directory selection -> auto-populates algorithm/version
    2. Algorithm selection -> discovers available metrics from MinIO
    3. Metric selection -> shows available AnalysisKinds
    4. AnalysisKind selection -> triggers visualization update

    Usage:
        selectors = DashboardSelectors(discovery_service)
        selectors.populate_directories()
        selectors.on_visualization_change = my_callback

    """

    def __init__(self, discovery: DiscoveryService) -> None:
        self._discovery = discovery
        self._metrics: list[MetricInfo] = []
        self._current_metric: MetricInfo | None = None

        self.directory_select = Select(
            title="Directory",
            options=[],
            value="",
            width=200,
        )

        self.algorithm_select = Select(
            title="Algorithm",
            options=[],
            value="",
            disabled=True,
            width=200,
        )

        self.version_select = Select(
            title="Version",
            options=[],
            value="",
            disabled=True,
            width=200,
        )

        self.metric_select = Select(
            title="Metric",
            options=[],
            value="",
            disabled=True,
            width=200,
        )

        self.analysis_kind_select = Select(
            title="Analysis Kind",
            options=[],
            value="",
            disabled=True,
            width=200,
        )

        self._on_visualization_change: Callable[[str, MetricInfo | None], None] | None = None

        self.directory_select.on_change("value", self._on_directory_change)
        self.metric_select.on_change("value", self._on_metric_change)
        self.analysis_kind_select.on_change("value", self._on_analysis_kind_change)

    @property
    def on_visualization_change(self) -> Callable[[str, MetricInfo | None], None] | None:
        """Callback invoked when visualization should update.

        Callback signature: (analysis_kind: str, metric: MetricInfo | None) -> None
        """
        return self._on_visualization_change

    @on_visualization_change.setter
    def on_visualization_change(self, callback: Callable[[str, MetricInfo | None], None] | None) -> None:
        self._on_visualization_change = callback

    def populate_directories(self) -> None:
        """Populate the directory select with configured directories."""
        directories = self._discovery.list_directories()
        options = [("", "-- Select Directory --")]
        options.extend((d.key, d.key) for d in directories)
        self.directory_select.options = options
        self.directory_select.value = ""

    def _on_directory_change(self, attr: str, old: str, new: str) -> None:  # noqa: ARG002
        """Handle directory selection change."""
        self._reset_downstream_selects()

        if not new:
            return

        algo_info = self._discovery.get_algorithm_for_directory(new)
        if algo_info is None:
            logger.warning("No algorithm configured for directory: %s", new)
            return

        self.algorithm_select.options = [(algo_info.name, algo_info.name)]
        self.algorithm_select.value = algo_info.name
        self.algorithm_select.disabled = True

        self.version_select.options = [(algo_info.version, algo_info.version)]
        self.version_select.value = algo_info.version
        self.version_select.disabled = True

        self._refresh_metrics(algo_info.name, algo_info.version)

    def _refresh_metrics(self, algo_name: str, algo_version: str) -> None:
        """Refresh available metrics from MinIO."""
        self._metrics = self._discovery.discover_metrics_from_minio(
            algo_name=algo_name,
            algo_version=algo_version,
        )

        if not self._metrics:
            self.metric_select.options = [("", "-- No metrics found --")]
            self.metric_select.value = ""
            self.metric_select.disabled = True
            return

        options = [("", "-- Select Metric --")]
        options.extend((m.metric_name, m.metric_name) for m in self._metrics)
        self.metric_select.options = options
        self.metric_select.value = ""
        self.metric_select.disabled = False

    def _on_metric_change(self, attr: str, old: str, new: str) -> None:  # noqa: ARG002
        """Handle metric selection change."""
        self.analysis_kind_select.options = []
        self.analysis_kind_select.value = ""
        self.analysis_kind_select.disabled = True
        self._current_metric = None

        if not new:
            self._trigger_visualization_update("", None)
            return

        metric = next((m for m in self._metrics if m.metric_name == new), None)
        if metric is None:
            return

        self._current_metric = metric

        if not metric.analysis_kinds:
            self.analysis_kind_select.options = [("", "-- No analysis kinds --")]
            self.analysis_kind_select.disabled = True
            return

        options = [("", "-- Select Analysis Kind --")]
        options.extend((k, k) for k in metric.analysis_kinds)
        self.analysis_kind_select.options = options
        self.analysis_kind_select.value = ""
        self.analysis_kind_select.disabled = False

    def _on_analysis_kind_change(self, attr: str, old: str, new: str) -> None:  # noqa: ARG002
        """Handle analysis kind selection change."""
        self._trigger_visualization_update(new, self._current_metric)

    def _trigger_visualization_update(self, analysis_kind: str, metric: MetricInfo | None) -> None:
        """Trigger the visualization update callback."""
        if self._on_visualization_change is not None:
            try:
                self._on_visualization_change(analysis_kind, metric)
            except Exception:
                logger.exception("Visualization callback failed")

    def _reset_downstream_selects(self) -> None:
        """Reset all selects downstream of directory."""
        self.algorithm_select.options = []
        self.algorithm_select.value = ""
        self.algorithm_select.disabled = True

        self.version_select.options = []
        self.version_select.value = ""
        self.version_select.disabled = True

        self.metric_select.options = []
        self.metric_select.value = ""
        self.metric_select.disabled = True

        self.analysis_kind_select.options = []
        self.analysis_kind_select.value = ""
        self.analysis_kind_select.disabled = True

        self._metrics = []
        self._current_metric = None

    def get_current_selection(self) -> dict[str, Any]:
        """Get the current selection state."""
        return {
            "directory": self.directory_select.value,
            "algorithm": self.algorithm_select.value,
            "version": self.version_select.value,
            "metric": self.metric_select.value,
            "analysis_kind": self.analysis_kind_select.value,
            "metric_info": self._current_metric,
        }
