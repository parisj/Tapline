"""Main dashboard layout with minimal dark design."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from bokeh.layouts import column, row
from bokeh.models import Button, Div, Model, Spinner

from src.utils.logging import get_logger
from src.visualization.data import PrometheusClient
from src.visualization.plots import (
    THEME_COLORS,
    create_contour_plot,
    create_counter_chart,
    create_ellipse_plot,
    create_histogram_plot,
    create_no_data_placeholder,
    create_rate_chart,
    create_throughput_chart,
    format_number,
)
from src.visualization.widgets import (
    DashboardSelectors,
    create_kpi_card,
    create_status_indicator,
    create_summary_table,
)

if TYPE_CHECKING:
    from bokeh.document import Document

    from src.visualization.discovery import DiscoveryService, MetricInfo
    from src.visualization.readers.minio_reader import MinioArtifactReader

logger = get_logger(__name__)


class DashboardLayout:
    """Minimal dark dashboard layout."""

    def __init__(
        self,
        discovery: DiscoveryService,
        reader: MinioArtifactReader,
    ) -> None:
        self._discovery = discovery
        self._reader = reader
        self._doc: Document | None = None
        self._refresh_callback_id: object | None = None
        self._auto_refresh_enabled = True
        self._refresh_interval_ms = 30000
        self._last_refresh: datetime | None = None

        self._prometheus = PrometheusClient()

        self._selectors = DashboardSelectors(discovery)
        self._selectors.on_visualization_change = self._on_visualization_change

        self._kpi_cards_row = row(sizing_mode="stretch_width")
        self._chart_container = column(sizing_mode="stretch_width")
        self._stats_row = row(sizing_mode="stretch_width")
        self._plot_container = column(
            create_no_data_placeholder("Select a directory to view data"),
            sizing_mode="stretch_width",
        )

        self._refresh_button = Button(
            label="Refresh",
            button_type="primary",
            width=80,
            height=30,
        )
        self._refresh_button.on_click(self._on_manual_refresh)

        self._auto_refresh_toggle = Button(
            label="Auto",
            button_type="success",
            width=60,
            height=30,
        )
        self._auto_refresh_toggle.on_click(self._toggle_auto_refresh)

        self._refresh_spinner = Spinner(
            low=5, high=120, step=5, value=30,
            width=60, title="",
        )
        self._refresh_spinner.on_change("value", self._on_interval_change)

    def set_document(self, doc: Document) -> None:
        self._doc = doc
        if self._auto_refresh_enabled:
            self._start_auto_refresh()

    def _start_auto_refresh(self) -> None:
        if self._doc is None:
            return
        self._stop_auto_refresh()
        self._refresh_callback_id = self._doc.add_periodic_callback(
            self._on_auto_refresh, self._refresh_interval_ms,
        )

    def _stop_auto_refresh(self) -> None:
        if self._doc is not None and self._refresh_callback_id is not None:
            self._doc.remove_periodic_callback(self._refresh_callback_id)
            self._refresh_callback_id = None

    def _toggle_auto_refresh(self) -> None:
        self._auto_refresh_enabled = not self._auto_refresh_enabled
        if self._auto_refresh_enabled:
            self._auto_refresh_toggle.label = "Auto"
            self._auto_refresh_toggle.button_type = "success"
            self._start_auto_refresh()
        else:
            self._auto_refresh_toggle.label = "Off"
            self._auto_refresh_toggle.button_type = "default"
            self._stop_auto_refresh()

    def _on_interval_change(self, attr: str, old: int, new: int) -> None:  # noqa: ARG002
        self._refresh_interval_ms = new * 1000
        if self._auto_refresh_enabled and self._doc is not None:
            self._start_auto_refresh()

    def _on_manual_refresh(self) -> None:
        self._refresh_data()

    def _on_auto_refresh(self) -> None:
        self._refresh_data()

    def _refresh_data(self) -> None:
        self._last_refresh = datetime.now()
        self._discovery.clear_cache()
        self._update_kpi_cards()
        self._update_chart()

        selection = self._selectors.get_current_selection()
        if selection["metric_info"] and selection["analysis_kind"]:
            self._selectors._refresh_metrics(selection["algorithm"], selection["version"])  # noqa: SLF001
            self._on_visualization_change(selection["analysis_kind"], selection["metric_info"])

    def _update_kpi_cards(self) -> None:
        jobs_total = self._prometheus.get_jobs_total()
        jobs_failed = self._prometheus.get_jobs_failed()
        throughput = self._prometheus.get_throughput()
        dirs_count = len(self._discovery.list_directories())

        cards = [
            create_kpi_card("Jobs", jobs_total, color="primary"),
            create_kpi_card("Throughput", f"{throughput:.2f}/s", color="info"),
            create_kpi_card("Errors", int(jobs_failed), color="error" if jobs_failed > 0 else "neutral"),
            create_kpi_card("Sources", dirs_count, color="secondary"),
        ]
        self._kpi_cards_row.children = cards

    def _update_chart(self) -> None:
        history = self._prometheus.get_throughput_history(hours=1.0)

        if history:
            timestamps = [p[0] for p in history]
            values = [p[1] for p in history]
            chart = create_throughput_chart(
                timestamps, values,
                title="Throughput",
                y_label="Jobs/sec",
                width=850, height=220,
            )
        else:
            chart = self._create_chart_placeholder()

        self._chart_container.children = [chart]

    def _create_chart_placeholder(self) -> Div:
        return Div(
            text=f"""
            <div style="
                height: 220px;
                display: flex;
                align-items: center;
                justify-content: center;
                background: {THEME_COLORS['surface']};
                border: 1px solid {THEME_COLORS['border_light']};
                border-radius: 12px;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            ">
                <span style="color: {THEME_COLORS['text_muted']}; font-size: 13px;">No data available</span>
            </div>
            """,
            width=850, height=220,
        )

    def build_layout(self) -> Model:
        self._selectors.populate_directories()
        self._update_kpi_cards()
        self._update_chart()

        header = self._create_header()
        kpi_section = self._create_card_section("Overview", self._kpi_cards_row)
        chart_section = self._create_card_section("Metrics", self._chart_container)
        explorer_section = self._create_explorer_section()

        return column(
            header,
            kpi_section,
            chart_section,
            explorer_section,
            sizing_mode="stretch_width",
        )

    def _create_header(self) -> Div:
        prom_ok = self._prometheus.is_available()
        status_color = THEME_COLORS['success'] if prom_ok else THEME_COLORS['error']
        status_text = 'Connected' if prom_ok else 'Disconnected'

        return Div(
            text=f"""
            <div style="
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 16px 20px;
                margin-bottom: 20px;
                background: {THEME_COLORS['surface']};
                border: 1px solid {THEME_COLORS['border_light']};
                border-radius: 12px;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            ">
                <h1 style="margin: 0; color: {THEME_COLORS['text']}; font-size: 16px; font-weight: 600;">
                    VisioEval
                </h1>
                <div style="display: flex; align-items: center; gap: 20px;">
                    <div id="clock" style="
                        color: {THEME_COLORS['text_muted']};
                        font-size: 12px;
                        font-variant-numeric: tabular-nums;
                    ">--:--:--</div>
                    <div style="display: flex; align-items: center; gap: 6px;">
                        <div style="
                            width: 6px; height: 6px;
                            background: {status_color};
                            border-radius: 50%;
                        "></div>
                        <span style="color: {THEME_COLORS['text_muted']}; font-size: 11px;">
                            {status_text}
                        </span>
                    </div>
                </div>
            </div>
            <script>
                (function() {{
                    function tick() {{
                        var el = document.getElementById('clock');
                        if (el) el.textContent = new Date().toLocaleTimeString('en-US', {{hour12: false}});
                    }}
                    tick();
                    setInterval(tick, 1000);
                }})();
            </script>
            """,
            height=70,
        )

    def _create_card_section(self, title: str, content: Model) -> Model:
        header = Div(
            text=f"""
            <div style="
                padding: 14px 20px;
                background: {THEME_COLORS['surface']};
                border: 1px solid {THEME_COLORS['border_light']};
                border-radius: 12px 12px 0 0;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            ">
                <span style="color: {THEME_COLORS['text']}; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;">{title}</span>
            </div>
            """,
            height=48,
        )

        wrapper = Div(
            text=f"""
            <div style="
                padding: 20px;
                background: {THEME_COLORS['surface']};
                border: 1px solid {THEME_COLORS['border_light']};
                border-top: none;
                border-radius: 0 0 12px 12px;
                margin-bottom: 20px;
            "></div>
            """,
            height=0,
        )

        return column(header, content, wrapper, sizing_mode="stretch_width")

    def _create_explorer_section(self) -> Model:
        selectors_row = row(
            column(self._selectors.directory_select, width=140),
            column(self._selectors.algorithm_select, width=140),
            column(self._selectors.version_select, width=80),
            column(self._selectors.metric_select, width=160),
            column(self._selectors.analysis_kind_select, width=120),
            column(self._refresh_button, width=80),
            column(self._auto_refresh_toggle, width=65),
            column(self._refresh_spinner, width=65),
        )

        header = Div(
            text=f"""
            <div style="
                padding: 14px 20px;
                background: {THEME_COLORS['surface']};
                border: 1px solid {THEME_COLORS['border_light']};
                border-radius: 12px 12px 0 0;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            ">
                <span style="color: {THEME_COLORS['text']}; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;">Data Explorer</span>
            </div>
            """,
            height=48,
        )

        content_wrapper = column(
            selectors_row,
            self._stats_row,
            self._plot_container,
            sizing_mode="stretch_width",
        )

        footer = Div(
            text=f"""
            <div style="
                padding: 16px;
                background: {THEME_COLORS['surface']};
                border: 1px solid {THEME_COLORS['border_light']};
                border-top: none;
                border-radius: 0 0 12px 12px;
            "></div>
            """,
            height=0,
        )

        return column(
            header,
            content_wrapper,
            footer,
            sizing_mode="stretch_width",
        )

    def _on_visualization_change(self, analysis_kind: str, metric: MetricInfo | None) -> None:
        if not analysis_kind or metric is None:
            self._stats_row.children = []
            self._plot_container.children = [create_no_data_placeholder("Select options to view")]
            return

        try:
            self._update_stats_row(metric)
            widgets = self._create_visualization(analysis_kind, metric)
            self._plot_container.children = widgets
        except Exception as e:
            logger.exception("Visualization error")
            self._plot_container.children = [create_no_data_placeholder(f"Error: {e}")]

    def _update_stats_row(self, metric: MetricInfo) -> None:
        from src.visualization.plots import create_stat_card

        doc = self._discovery.get_metric_artifact(metric)
        summary = doc.get("summary", {}) if doc else metric.summary

        cards = []
        if "count" in summary:
            cards.append(create_stat_card("Count", int(summary["count"]), color="primary"))
        if "avg" in summary:
            cards.append(create_stat_card("Avg", format_number(summary["avg"]), color="info"))
        if "min" in summary:
            cards.append(create_stat_card("Min", format_number(summary["min"]), color="secondary"))
        if "max" in summary:
            cards.append(create_stat_card("Max", format_number(summary["max"]), color="accent"))

        self._stats_row.children = cards

    def _create_visualization(self, analysis_kind: str, metric: MetricInfo) -> list[Model]:
        doc = self._discovery.get_metric_artifact(metric)
        summary = doc.get("summary", {}) if doc else metric.summary

        widgets: list[Model] = []

        if analysis_kind == "SUMMARY":
            widgets.append(create_summary_table(summary))
            if summary:
                widgets.append(self._create_summary_chart(summary))
        elif analysis_kind == "DISTRIBUTION_1D":
            npz_data = self._fetch_npz_artifact(metric, "histogram.npz")
            widgets.append(create_histogram_plot(npz_data, summary=summary))
        elif analysis_kind == "RATE":
            widgets.append(create_rate_chart(summary))
        elif analysis_kind == "COUNTER":
            npz_data = self._fetch_npz_artifact(metric, "counter.npz")
            widgets.append(create_counter_chart(npz_data=npz_data, summary=summary))
        elif analysis_kind == "ELLIPSE_2D":
            npz_data = self._fetch_npz_artifact(metric, "ellipse.npz")
            widgets.append(create_ellipse_plot(npz_data, summary=summary))
        elif analysis_kind == "CONTOUR_2D":
            npz_data = self._fetch_npz_artifact(metric, "contour.npz")
            widgets.append(create_contour_plot(npz_data, summary=summary))
        elif analysis_kind == "INFO":
            widgets.append(create_summary_table(summary))
        else:
            widgets.append(create_no_data_placeholder(f"{analysis_kind} not implemented"))

        return widgets

    def _create_summary_chart(self, summary: dict) -> Model:
        from bokeh.models import ColumnDataSource, HoverTool
        from bokeh.plotting import figure

        keys, values, colors = [], [], []
        color_map = {
            "count": THEME_COLORS["primary"],
            "sum": THEME_COLORS["warning"],
            "avg": THEME_COLORS["info"],
            "min": THEME_COLORS["secondary"],
            "max": THEME_COLORS["accent"],
        }

        for key, value in summary.items():
            if isinstance(value, (int, float)):
                keys.append(key.upper())
                values.append(float(value))
                colors.append(color_map.get(key, THEME_COLORS["neutral"]))

        if not keys:
            return create_no_data_placeholder("No numeric data")

        source = ColumnDataSource(data={"keys": keys, "values": values, "colors": colors})

        p = figure(
            x_range=keys, height=250, width=700,
            title="Summary", toolbar_location=None, tools="",
        )

        p.vbar(x="keys", top="values", width=0.5, source=source, color="colors")
        p.add_tools(HoverTool(tooltips=[("Value", "@values{0.00}")]))

        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.title.text_font_size = "12pt"
        p.title.text_color = THEME_COLORS["text"]
        p.xaxis.major_label_text_color = THEME_COLORS["text_muted"]
        p.yaxis.major_label_text_color = THEME_COLORS["text_muted"]
        p.background_fill_color = THEME_COLORS["background"]
        p.border_fill_color = THEME_COLORS["background"]
        p.outline_line_color = THEME_COLORS["border"]

        return p

    def _fetch_npz_artifact(self, metric: MetricInfo, expected_name: str) -> dict | None:
        if self._reader is None:
            return None

        doc = self._discovery.get_metric_artifact(metric)
        if doc is None:
            return None

        artifact_ref = doc.get("artifact_ref")
        if artifact_ref and isinstance(artifact_ref, dict):
            content_hash = artifact_ref.get("content_hash")
            bucket = artifact_ref.get("bucket", "artifacts")
            if content_hash:
                return self._reader.fetch_npz(bucket, content_hash)

        for artifact in doc.get("artifacts", []):
            if isinstance(artifact, dict):
                name = artifact.get("name", "")
                if name == expected_name or name.endswith(expected_name):
                    content_hash = artifact.get("content_hash")
                    bucket = artifact.get("bucket", "artifacts")
                    if content_hash:
                        return self._reader.fetch_npz(bucket, content_hash)

        return None
