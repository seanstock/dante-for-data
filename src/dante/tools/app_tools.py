"""MCP tool stubs for app/dashboard generation.

These are placeholders for the future app builder that will allow
Claude to create lightweight data dashboards from query results.
"""

from __future__ import annotations


def dante_app_create(title: str, template: str = "dashboard") -> str:
    """Create a new data app scaffold.

    Args:
        title: Display title for the app.
        template: App template to use. Defaults to "dashboard".
                  Options: "dashboard", "report", "explorer".

    Returns:
        App ID and confirmation message.
    """
    # TODO: implement with app builder
    return (
        "_App creation is not yet implemented._ "
        "This feature will generate lightweight data dashboards."
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
        name: Display name for this value/widget.
        sql: SQL query that produces the data for this value.
        format: How to render the value: "scalar", "table", "bar", "line", "pie".

    Returns:
        Confirmation message.
    """
    # TODO: implement with app builder
    return (
        "_App value addition is not yet implemented._ "
        "This feature will add data widgets to app dashboards."
    )


def dante_app_render(app_id: str) -> str:
    """Render a data app to its output file(s).

    Args:
        app_id: The app identifier returned by dante_app_create.

    Returns:
        Path to the rendered app, or an error message.
    """
    # TODO: implement with app builder
    return (
        "_App rendering is not yet implemented._ "
        "This feature will generate standalone HTML dashboards."
    )
