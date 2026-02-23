"""Data App template engine — HTML dashboards with computed values."""

from __future__ import annotations

import re
import webbrowser
from pathlib import Path

from dante.config import _find_project_root
from dante.query import sql as run_sql


class App:
    """A Data App with computed value slots filled by SQL queries."""

    def __init__(self, title: str, template: str = "dashboard", root: Path | None = None):
        self.title = title
        self.template = template
        self.root = root or _find_project_root()
        self.id = _slugify(title)
        self._values: dict[str, dict] = {}  # name → {sql, format}
        self._html: str = ""
        self._css: str = ""
        self._js: str = ""

    @property
    def html(self) -> str:
        return self._html

    @html.setter
    def html(self, value: str):
        self._html = value

    @property
    def css(self) -> str:
        return self._css

    @css.setter
    def css(self, value: str):
        self._css = value

    @property
    def js(self) -> str:
        return self._js

    @js.setter
    def js(self, value: str):
        self._js = value

    def add_value(self, name: str, sql_query: str, format: str = "scalar"):
        """Bind a SQL query to a computed value slot.

        Args:
            name: Slot name (used as {NAME} in HTML).
            sql_query: SQL query to execute.
            format: "scalar" (single value), "table" (HTML table), or "chart".
        """
        self._values[name] = {"sql": sql_query, "format": format}

    def remove_value(self, name: str):
        """Remove a computed value."""
        self._values.pop(name, None)

    def render(self) -> str:
        """Execute all queries, substitute values, write HTML to outputs/.

        Returns:
            Path to the generated HTML file.
        """
        outputs_dir = self.root / "outputs"
        outputs_dir.mkdir(parents=True, exist_ok=True)

        # Execute all computed values
        computed: dict[str, str] = {}
        for name, config in self._values.items():
            try:
                df = run_sql(config["sql"], root=self.root)
                if config["format"] == "table":
                    computed[name] = _df_to_html_table(df)
                elif config["format"] == "chart":
                    # For chart format, create a plotly figure inline
                    computed[name] = _df_to_inline_chart(df, name)
                else:
                    # Scalar: first cell of first row
                    if not df.empty:
                        val = df.iloc[0, 0]
                        computed[name] = _format_scalar(val)
                    else:
                        computed[name] = "—"
            except Exception as e:
                computed[name] = f'<span class="error">Error: {e}</span>'

        # Substitute values into HTML
        body = self._html
        for name, value in computed.items():
            body = body.replace(f"{{{name}}}", value)

        # Build full HTML document
        template_css = _get_template_css(self.template)
        full_html = _build_document(
            title=self.title,
            template_css=template_css,
            custom_css=self._css,
            body=body,
            custom_js=self._js,
        )

        out_path = outputs_dir / f"{self.id}.html"
        out_path.write_text(full_html)
        return str(out_path)

    def open(self):
        """Open the rendered HTML in the default browser."""
        out_path = self.root / "outputs" / f"{self.id}.html"
        if out_path.exists():
            webbrowser.open(f"file://{out_path.resolve()}")

    def refresh(self) -> str:
        """Re-execute all queries and re-render."""
        return self.render()


def create(title: str, template: str = "dashboard", root: Path | None = None) -> App:
    """Create a new Data App.

    Args:
        title: App title.
        template: Template type: dashboard, report, map, profile, blank.

    Returns:
        App instance.
    """
    return App(title=title, template=template, root=root)


def _slugify(text: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"[\s_]+", "-", slug).strip("-")
    return slug or "app"


def _format_scalar(val) -> str:
    """Format a scalar value for display."""
    if isinstance(val, float):
        if abs(val) >= 1_000_000:
            return f"${val/1_000_000:,.1f}M" if val > 0 else f"{val/1_000_000:,.1f}M"
        elif abs(val) >= 1_000:
            return f"{val:,.0f}"
        else:
            return f"{val:,.2f}"
    return str(val)


def _df_to_html_table(df) -> str:
    """Convert a DataFrame to an HTML table with the .data-table class."""
    lines = ['<table class="data-table">']
    lines.append("<thead><tr>")
    for col in df.columns:
        lines.append(f"<th>{col}</th>")
    lines.append("</tr></thead>")
    lines.append("<tbody>")
    for _, row in df.iterrows():
        lines.append("<tr>")
        for val in row:
            lines.append(f"<td>{val}</td>")
        lines.append("</tr>")
    lines.append("</tbody></table>")
    return "\n".join(lines)


def _df_to_inline_chart(df, name: str) -> str:
    """Create an inline Plotly chart from a DataFrame."""
    import json
    cols = list(df.columns)
    if len(cols) >= 2:
        x_data = df[cols[0]].tolist()
        y_data = df[cols[1]].tolist()
        trace = {"x": x_data, "y": y_data, "type": "bar", "name": name}
    else:
        trace = {"y": df[cols[0]].tolist(), "type": "bar", "name": name}

    div_id = f"chart-{name.lower().replace(' ', '-')}"
    return (
        f'<div id="{div_id}"></div>'
        f'<script>Plotly.newPlot("{div_id}", [{json.dumps(trace)}], '
        f'{{"template": "plotly_dark", "paper_bgcolor": "transparent", "plot_bgcolor": "transparent"}}'
        f', {{responsive: true}});</script>'
    )


def _build_document(title: str, template_css: str, custom_css: str, body: str, custom_js: str) -> str:
    """Build a complete HTML document."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
{_BASE_CSS}
{template_css}
{custom_css}
</style>
</head>
<body>
<header><h1>{title}</h1></header>
<main>
{body}
</main>
{f'<script>{custom_js}</script>' if custom_js else ''}
</body>
</html>"""


_BASE_CSS = """
:root {
    --bg: #0d0d0d;
    --card-bg: #171717;
    --border: #333;
    --text: #ececec;
    --text-muted: #8e8e8e;
    --accent: #de2626;
    --success: #4ade80;
    --danger: #ef4444;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: var(--bg);
    color: var(--text);
    padding: 2rem;
}
header {
    margin-bottom: 2rem;
    border-bottom: 2px solid var(--border);
    padding-bottom: 1rem;
}
header h1 { color: var(--accent); font-size: 1.5rem; }
main { max-width: 1400px; margin: 0 auto; }
.error { color: var(--danger); font-style: italic; }
"""


def _get_template_css(template: str) -> str:
    """Return CSS for the specified template."""
    templates = {
        "dashboard": _DASHBOARD_CSS,
        "report": _REPORT_CSS,
        "map": _MAP_CSS,
        "profile": _PROFILE_CSS,
        "blank": "",
    }
    return templates.get(template, "")


_DASHBOARD_CSS = """
.kpis {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin-bottom: 2rem;
}
.kpi {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1.5rem;
    text-align: center;
}
.kpi-label { color: var(--text-muted); font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
.kpi-value { font-size: 2rem; font-weight: 700; color: var(--accent); }
.kpi-change { font-size: 0.85rem; margin-top: 0.25rem; }
.kpi-change.up { color: var(--success); }
.kpi-change.down { color: var(--danger); }
.chart-card {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1.5rem;
    margin-bottom: 1rem;
}
.chart-card.wide { grid-column: span 2; }
.data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}
.data-table th, .data-table td {
    padding: 0.75rem 1rem;
    text-align: left;
    border-bottom: 1px solid var(--border);
}
.data-table th {
    color: var(--text-muted);
    font-weight: 600;
    text-transform: uppercase;
    font-size: 0.75rem;
    letter-spacing: 0.05em;
}
.data-table tr:hover { background: rgba(0, 212, 255, 0.05); }
"""

_REPORT_CSS = """
.report { max-width: 800px; margin: 0 auto; line-height: 1.7; }
.key-findings {
    background: var(--card-bg);
    border-left: 4px solid var(--accent);
    padding: 1.5rem;
    margin: 1.5rem 0;
    border-radius: 0 8px 8px 0;
}
.callout {
    background: rgba(0, 212, 255, 0.1);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem 1.5rem;
    margin: 1rem 0;
}
.figure { margin: 1.5rem 0; }
.figure-caption { color: var(--text-muted); font-size: 0.85rem; margin-top: 0.5rem; font-style: italic; }
blockquote {
    border-left: 3px solid var(--accent);
    padding-left: 1rem;
    color: var(--text-muted);
    margin: 1rem 0;
}
.data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}
.data-table th, .data-table td {
    padding: 0.75rem 1rem;
    text-align: left;
    border-bottom: 1px solid var(--border);
}
.data-table th { color: var(--text-muted); font-weight: 600; }
"""

_MAP_CSS = """
.map-container { height: 500px; border-radius: 8px; overflow: hidden; border: 1px solid var(--border); }
.panel {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1.5rem;
    margin-bottom: 1rem;
}
.search-bar {
    width: 100%;
    padding: 0.75rem 1rem;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-size: 0.9rem;
    margin-bottom: 1rem;
}
.filter-chips { display: flex; gap: 0.5rem; flex-wrap: wrap; margin-bottom: 1rem; }
.stats-bar { display: flex; gap: 2rem; padding: 1rem 0; border-bottom: 1px solid var(--border); margin-bottom: 1rem; }
.location-list { list-style: none; }
.location-list li { padding: 0.75rem 0; border-bottom: 1px solid var(--border); }
.status-badge {
    display: inline-block;
    padding: 0.25rem 0.75rem;
    border-radius: 12px;
    font-size: 0.75rem;
    font-weight: 600;
}
.status-badge.operational { background: rgba(0, 230, 118, 0.2); color: var(--success); }
.status-badge.maintenance { background: rgba(255, 193, 7, 0.2); color: #ffc107; }
.status-badge.offline { background: rgba(255, 82, 82, 0.2); color: var(--danger); }
"""

_PROFILE_CSS = """
.sidebar {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 2rem;
    text-align: center;
}
.health-ring {
    width: 120px;
    height: 120px;
    border-radius: 50%;
    border: 4px solid var(--accent);
    display: flex;
    align-items: center;
    justify-content: center;
    margin: 1rem auto;
    font-size: 1.5rem;
    font-weight: 700;
}
.tab-nav {
    display: flex;
    gap: 0;
    border-bottom: 2px solid var(--border);
    margin-bottom: 1.5rem;
}
.tab-nav button {
    padding: 0.75rem 1.5rem;
    background: none;
    border: none;
    color: var(--text-muted);
    cursor: pointer;
    font-size: 0.9rem;
    border-bottom: 2px solid transparent;
    margin-bottom: -2px;
}
.tab-nav button.active { color: var(--accent); border-bottom-color: var(--accent); }
.tab-panel { display: none; }
.tab-panel.active { display: block; }
.timeline { border-left: 2px solid var(--border); padding-left: 1.5rem; margin: 1rem 0; }
.timeline .event { margin-bottom: 1.5rem; position: relative; }
.timeline .event::before {
    content: '';
    position: absolute;
    left: -1.75rem;
    top: 0.25rem;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: var(--accent);
}
.ticket {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 1rem;
    margin-bottom: 0.5rem;
}
.priority-high { border-left: 3px solid var(--danger); }
.priority-medium { border-left: 3px solid #ffc107; }
.priority-low { border-left: 3px solid var(--success); }
"""
