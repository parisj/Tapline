"""Summary statistics table widget."""

from __future__ import annotations

from typing import Any

from bokeh.models import ColumnDataSource, DataTable, TableColumn


def create_summary_table(summary: dict[str, Any], *, width: int = 400) -> DataTable:
    """Create a DataTable displaying summary statistics.

    Args:
        summary: Dict containing summary statistics with keys like:
            count, missing, mean, median, min, max, std, variance
        width: Table width in pixels

    Returns:
        Bokeh DataTable widget

    """
    stat_names = []
    stat_values = []

    stat_order = [
        ("count", "Count"),
        ("missing", "Missing"),
        ("mean", "Mean"),
        ("median", "Median"),
        ("min", "Min"),
        ("max", "Max"),
        ("std", "Std Dev"),
        ("variance", "Variance"),
        ("yes", "Yes"),
        ("no", "No"),
        ("rate", "Rate"),
        ("wilson_low", "Wilson CI Low"),
        ("wilson_high", "Wilson CI High"),
        ("unique", "Unique"),
        ("bins", "Bins"),
    ]

    for key, display_name in stat_order:
        if key in summary:
            value = summary[key]
            stat_names.append(display_name)
            if value is None:
                stat_values.append("N/A")
            elif isinstance(value, float):
                stat_values.append(f"{value:.6g}")
            else:
                stat_values.append(str(value))

    for key, value in summary.items():
        if key not in dict(stat_order):
            stat_names.append(key)
            if value is None:
                stat_values.append("N/A")
            elif isinstance(value, float):
                stat_values.append(f"{value:.6g}")
            else:
                stat_values.append(str(value))

    source = ColumnDataSource(
        data={
            "statistic": stat_names,
            "value": stat_values,
        },
    )

    columns = [
        TableColumn(field="statistic", title="Statistic", width=int(width * 0.5)),
        TableColumn(field="value", title="Value", width=int(width * 0.5)),
    ]

    table = DataTable(
        source=source,
        columns=columns,
        width=width,
        height=min(400, 30 + len(stat_names) * 25),
        index_position=None,
        editable=False,
    )

    return table


def create_empty_table(message: str = "No data available", *, width: int = 400) -> DataTable:
    """Create an empty table with a message.

    Args:
        message: Message to display
        width: Table width in pixels

    Returns:
        Bokeh DataTable widget

    """
    source = ColumnDataSource(
        data={
            "message": [message],
        },
    )

    columns = [
        TableColumn(field="message", title="Status", width=width),
    ]

    return DataTable(
        source=source,
        columns=columns,
        width=width,
        height=60,
        index_position=None,
        editable=False,
    )
