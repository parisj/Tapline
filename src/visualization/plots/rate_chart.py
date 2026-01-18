"""Rate bar chart with Wilson confidence interval for RATE analysis kind."""

from __future__ import annotations

from typing import Any

from bokeh.models import ColumnDataSource, HoverTool, LabelSet, Model

from src.visualization.plots.common import (
    THEME_COLORS,
    create_figure,
    create_no_data_placeholder,
)


def create_rate_chart(
    summary: dict[str, Any],
    *,
    title: str = "Yes/No Rate",
) -> Model:
    """Create a bar chart with Wilson confidence interval.

    Expected summary keys:
        - yes: Count of yes/true values
        - no: Count of no/false values
        - rate: Proportion (0-1)
        - wilson_low: Lower bound of Wilson CI
        - wilson_high: Upper bound of Wilson CI

    Args:
        summary: Dict containing rate statistics
        title: Plot title

    Returns:
        Bokeh figure or placeholder widget

    """
    if not summary:
        return create_no_data_placeholder("Rate data not available")

    yes = summary.get("yes", 0)
    no = summary.get("no", 0)
    rate = summary.get("rate")
    wilson_low = summary.get("wilson_low")
    wilson_high = summary.get("wilson_high")

    if rate is None:
        return create_no_data_placeholder("Rate data is empty")

    source = ColumnDataSource(
        data={
            "categories": ["Yes", "No"],
            "counts": [yes, no],
            "colors": [THEME_COLORS["secondary"], THEME_COLORS["accent"]],
            "labels": [str(yes), str(no)],
        },
    )

    rate_pct = rate * 100
    ci_low_pct = (wilson_low or 0) * 100
    ci_high_pct = (wilson_high or 0) * 100

    full_title = f"{title} - Rate: {rate_pct:.1f}%"
    if wilson_low is not None and wilson_high is not None:
        full_title += f" (95% CI: {ci_low_pct:.1f}%-{ci_high_pct:.1f}%)"

    p = create_figure(
        title=full_title,
        x_range=["Yes", "No"],
        y_axis_label="Count",
        height=400,
    )

    p.vbar(
        x="categories",
        top="counts",
        width=0.6,
        source=source,
        fill_color="colors",
        line_color="white",
        alpha=0.8,
    )

    labels = LabelSet(
        x="categories",
        y="counts",
        text="labels",
        source=source,
        text_align="center",
        text_baseline="bottom",
        y_offset=5,
        text_font_size="12pt",
        text_color=THEME_COLORS["text"],
    )
    p.add_layout(labels)

    hover = HoverTool(
        tooltips=[
            ("Category", "@categories"),
            ("Count", "@counts"),
        ],
    )
    p.add_tools(hover)

    p.xgrid.grid_line_color = None

    return p
