"""MCP tool function for chart generation."""

from __future__ import annotations

import json

import pandas as pd

from dante.chart import chart


def dante_chart(
    data: str,
    x: str | None = None,
    y: str | None = None,
    kind: str = "bar",
    title: str = "Chart",
    format: str = "html",
    theme: str = "dark",
) -> str:
    """Generate a Plotly chart from JSON data and save to outputs/.

    Args:
        data: JSON string — either an array of objects (rows) to be loaded
              as a DataFrame, or a raw Plotly spec dict.
        x: Column name for x-axis (when data is a row array).
        y: Column name(s) for y-axis. Pass a single column name or a
           comma-separated list for multi-series.
        kind: Chart type: bar, line, scatter, pie, heatmap, histogram, box.
        title: Chart title (also used for the output filename).
        format: Output format: "html" (interactive) or "png" (static image).
        theme: Color theme: "dark" or "light".

    Returns:
        Path to the generated chart file, or an error message.
    """
    try:
        parsed = json.loads(data)
    except (json.JSONDecodeError, TypeError) as e:
        return f"**Error parsing data JSON:** {e}"

    try:
        # Determine if parsed data is a Plotly spec or row data
        if isinstance(parsed, list):
            df = pd.DataFrame(parsed)
            # Handle comma-separated y columns
            y_val: str | list[str] | None = None
            if y is not None:
                parts = [p.strip() for p in y.split(",")]
                y_val = parts if len(parts) > 1 else parts[0]
            result_path = chart(
                data=df,
                x=x,
                y=y_val,
                kind=kind,
                title=title,
                format=format,
                theme=theme,
            )
        elif isinstance(parsed, dict):
            # Raw Plotly spec dict
            result_path = chart(
                data=parsed,
                kind=kind,
                title=title,
                format=format,
                theme=theme,
            )
        else:
            return "**Error:** data must be a JSON array of objects or a Plotly spec dict."

        return f"Chart saved to: `{result_path}`"
    except Exception as e:
        return f"**Error generating chart:** {type(e).__name__}: {e}"
