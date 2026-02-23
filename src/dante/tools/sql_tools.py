"""MCP tool functions for SQL query execution and database introspection."""

from __future__ import annotations

from dante.query import sql_markdown, tables_markdown, describe_markdown, profile_markdown


def dante_sql(query: str, limit: int = 5000) -> str:
    """Execute a read-only SQL query and return results as a markdown table.

    Args:
        query: SQL query string. Must be read-only (SELECT, WITH, etc.).
        limit: Maximum number of rows to return. Defaults to 5000.

    Returns:
        Markdown-formatted table of results.
    """
    try:
        return sql_markdown(query, limit=limit)
    except ValueError as e:
        return f"**Error:** {e}"
    except ConnectionError as e:
        return f"**Connection error:** {e}"
    except Exception as e:
        return f"**Error executing query:** {type(e).__name__}: {e}"


def dante_tables(schema: str | None = None) -> str:
    """List all tables in the database, optionally filtered by schema.

    Args:
        schema: Optional schema name to filter tables.

    Returns:
        Markdown-formatted list of table names.
    """
    try:
        return tables_markdown(schema=schema)
    except ConnectionError as e:
        return f"**Connection error:** {e}"
    except Exception as e:
        return f"**Error listing tables:** {type(e).__name__}: {e}"


def dante_describe(table: str, schema: str | None = None) -> str:
    """Describe columns of a table: name, type, nullable, and sample values.

    Args:
        table: Table name to describe.
        schema: Optional schema name.

    Returns:
        Markdown table of column metadata.
    """
    try:
        return describe_markdown(table, schema=schema)
    except ConnectionError as e:
        return f"**Connection error:** {e}"
    except Exception as e:
        return f"**Error describing table:** {type(e).__name__}: {e}"


def dante_profile(table: str, schema: str | None = None) -> str:
    """Statistical profile of a table: row count, nulls, cardinality, min/max per column.

    Args:
        table: Table name to profile.
        schema: Optional schema name.

    Returns:
        Markdown table of column statistics.
    """
    try:
        return profile_markdown(table, schema=schema)
    except ConnectionError as e:
        return f"**Connection error:** {e}"
    except Exception as e:
        return f"**Error profiling table:** {type(e).__name__}: {e}"
