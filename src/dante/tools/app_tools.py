"""MCP tool implementations for app/dashboard generation."""

from __future__ import annotations

import threading

# In-memory registry of active apps (app_id → App instance)
_apps: dict[str, "dante.app.App"] = {}
_apps_lock = threading.Lock()


def dante_app_create(title: str, template: str = "dashboard") -> str:
    """Create a new data app scaffold.

    Args:
        title: Display title for the app.
        template: App template to use.
                  Options: "dashboard", "report", "map", "profile", "blank".

    Returns:
        App ID, available CSS classes, and instructions.
    """
    from dante.app import App

    app = App(title=title, template=template)
    with _apps_lock:
        _apps[app.id] = app

    css_help = {
        "dashboard": "CSS classes: .kpis, .kpi, .kpi-label, .kpi-value, .kpi-change.up/.down, .chart-card, .chart-card.wide, .data-table",
        "report": "CSS classes: .report, .key-findings, .callout, .figure, .figure-caption, blockquote, .data-table",
        "map": "CSS classes: .map-container, .panel, .search-bar, .filter-chips, .stats-bar, .location-list, .status-badge.operational/.maintenance/.offline",
        "profile": "CSS classes: .sidebar, .health-ring, .tab-nav, .tab-panel, .timeline, .event, .ticket, .priority-high/.medium/.low",
        "blank": "No predefined CSS classes.",
    }

    return (
        f"App **{title}** created (id: `{app.id}`, template: `{template}`).\n\n"
        f"{css_help.get(template, '')}\n\n"
        f"Next steps:\n"
        f"1. Use `dante_app_add_value` to bind SQL queries to named slots\n"
        f"2. Set `app.html` with the HTML body containing {{SLOT_NAME}} placeholders\n"
        f"3. Use `dante_app_render` to execute queries and produce the final HTML"
    )


def dante_app_add_value(
    app_id: str,
    name: str,
    sql: str,
    format: str = "scalar",
) -> str:
    """Add a data value (KPI, table, chart) to an existing app.

    Args:
        app_id: The app identifier returned by dante_app_create.
        name: Slot name — use {NAME} in the HTML body to place this value.
        sql: SQL query that produces the data for this value.
        format: How to render: "scalar" (first cell), "table" (HTML table), "chart" (Plotly bar).

    Returns:
        Confirmation message.
    """
    with _apps_lock:
        app = _apps.get(app_id)
    if app is None:
        return f"Error: No app found with id `{app_id}`. Create one first with `dante_app_create`."

    app.add_value(name, sql, format)
    return f"Value `{name}` (format: {format}) bound to app `{app_id}`. Use `{{{name}}}` in the HTML body."


def dante_app_set_html(app_id: str, html: str, css: str = "", js: str = "") -> str:
    """Set the HTML body for a data app. Use {SLOT_NAME} placeholders for computed values.

    Args:
        app_id: The app identifier.
        html: HTML body content with {SLOT_NAME} placeholders.
        css: Optional custom CSS to add.
        js: Optional custom JavaScript to add.

    Returns:
        Confirmation message.
    """
    with _apps_lock:
        app = _apps.get(app_id)
    if app is None:
        return f"Error: No app found with id `{app_id}`."

    app.html = html
    if css:
        app.css = css
    if js:
        app.js = js

    slots = app.value_names()
    return f"HTML set for app `{app_id}`. Slots bound: {', '.join(slots) if slots else 'none yet'}."


def dante_app_render(app_id: str) -> str:
    """Execute all queries, substitute values, write final HTML.

    Args:
        app_id: The app identifier returned by dante_app_create.

    Returns:
        Path to the rendered HTML file.
    """
    with _apps_lock:
        app = _apps.get(app_id)
    if app is None:
        return f"Error: No app found with id `{app_id}`. Create one first with `dante_app_create`."

    if not app.html:
        return f"Error: No HTML body set for app `{app_id}`. Use `dante_app_set_html` first."

    try:
        path = app.render()
        return f"Dashboard rendered to `{path}`."
    except Exception as e:
        return f"Error rendering app `{app_id}`: {e}"
