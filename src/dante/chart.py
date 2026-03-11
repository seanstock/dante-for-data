"""Chart generation via Plotly → self-contained HTML or PNG files."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from dante._utils import slugify
from dante.config import _find_project_root

_DARK_TEMPLATE = "plotly_dark"
_LIGHT_TEMPLATE = "plotly_white"

_KIND_MAP = {
    "bar": "bar",
    "line": "line",
    "scatter": "scatter",
    "pie": "pie",
    "heatmap": "density_heatmap",
    "histogram": "histogram",
    "box": "box",
}


def chart(
    data,
    x: str | None = None,
    y: str | list[str] | None = None,
    kind: str = "bar",
    title: str | None = None,
    filename: str | None = None,
    format: str = "html",
    theme: str = "dark",
    root: Path | None = None,
) -> str:
    """Generate a Plotly chart and save to outputs/.

    Args:
        data: pandas DataFrame or raw Plotly spec dict.
        x: Column for x-axis (DataFrame mode).
        y: Column(s) for y-axis (DataFrame mode).
        kind: Chart type: bar, line, scatter, pie, heatmap, histogram, box.
        title: Chart title. Also used for filename if filename is None.
        filename: Output filename without extension.
        format: "html" (interactive) or "png" (static image).
        theme: "dark" (default) or "light".
        root: Project root.

    Returns:
        Path to the generated file.
    """
    root = root or _find_project_root()
    outputs_dir = root / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    template = _DARK_TEMPLATE if theme == "dark" else _LIGHT_TEMPLATE

    if isinstance(data, dict):
        # Raw Plotly spec
        fig = go.Figure(data)
        if title:
            fig.update_layout(title=title)
        fig.update_layout(template=template)
    elif isinstance(data, pd.DataFrame):
        fig = _df_to_figure(data, x=x, y=y, kind=kind, title=title, template=template)
    else:
        raise TypeError(f"data must be a DataFrame or Plotly spec dict, got {type(data)}")

    # Determine output path
    if filename is None:
        filename = slugify(title or "chart", fallback="chart")
    ext = "png" if format == "png" else "html"
    out_path = outputs_dir / f"{filename}.{ext}"

    if format == "png":
        fig.write_image(str(out_path), width=1200, height=700, scale=2)
    else:
        fig.write_html(
            str(out_path),
            include_plotlyjs="cdn",
            full_html=True,
            config={"displayModeBar": True, "responsive": True},
        )

    return str(out_path)


def _df_to_figure(
    df: pd.DataFrame,
    x: str | None,
    y: str | list[str] | None,
    kind: str,
    title: str | None,
    template: str,
) -> go.Figure:
    """Create a Plotly figure from a DataFrame."""
    if kind == "pie":
        fig = px.pie(df, names=x, values=y if isinstance(y, str) else (y[0] if y else None), title=title)
    elif kind == "histogram":
        fig = px.histogram(df, x=x, title=title)
    elif kind == "box":
        fig = px.box(df, x=x, y=y if isinstance(y, str) else (y[0] if y else None), title=title)
    elif kind == "heatmap":
        fig = px.density_heatmap(df, x=x, y=y if isinstance(y, str) else (y[0] if y else None), title=title)
    elif kind == "scatter":
        fig = px.scatter(df, x=x, y=y if isinstance(y, str) else (y[0] if y else None), title=title)
    elif kind == "line":
        if isinstance(y, list):
            fig = go.Figure()
            for col in y:
                fig.add_trace(go.Scatter(x=df[x], y=df[col], mode="lines", name=col))
            if title:
                fig.update_layout(title=title)
        else:
            fig = px.line(df, x=x, y=y, title=title)
    else:  # bar
        if isinstance(y, list):
            fig = go.Figure()
            for col in y:
                fig.add_trace(go.Bar(x=df[x], y=df[col], name=col))
            if title:
                fig.update_layout(title=title, barmode="group")
        else:
            fig = px.bar(df, x=x, y=y, title=title)

    fig.update_layout(template=template)
    return fig
