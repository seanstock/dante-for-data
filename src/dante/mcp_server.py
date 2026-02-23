"""Dante MCP server — exposes database tools to Claude via stdio transport.

Usage:
    python -m dante.mcp_server

Or via the console script entry point:
    dante-mcp
"""

from __future__ import annotations

import asyncio
import json
import traceback

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from dante.tools.sql_tools import dante_sql, dante_tables, dante_describe, dante_profile
from dante.tools.chart_tools import dante_chart
from dante.tools.knowledge_tools import dante_search, dante_save_pattern, dante_define_term
from dante.tools.app_tools import dante_app_create, dante_app_add_value, dante_app_render
from dante.tools.analyze_tools import dante_checkpoint, dante_rollback

# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS = [
    # --- SQL / Database ---
    Tool(
        name="dante_sql",
        description=(
            "Execute a read-only SQL query against the project's configured database "
            "and return results as a markdown table. Mutating statements (INSERT, "
            "UPDATE, DELETE, DROP, etc.) are rejected. A LIMIT clause is automatically "
            "injected if the query does not already contain one."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "SQL query to execute. Must be read-only.",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum rows to return. Defaults to 5000.",
                    "default": 5000,
                },
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="dante_tables",
        description=(
            "List all tables in the connected database. Optionally filter by schema. "
            "Returns a markdown bullet list of table names."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "schema": {
                    "type": "string",
                    "description": "Optional schema name to filter tables.",
                },
            },
            "required": [],
        },
    ),
    Tool(
        name="dante_describe",
        description=(
            "Describe the columns of a database table: column name, data type, "
            "whether it is nullable, and sample values from the first few rows. "
            "Returns a markdown table."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Table name to describe.",
                },
                "schema": {
                    "type": "string",
                    "description": "Optional schema name.",
                },
            },
            "required": ["table"],
        },
    ),
    Tool(
        name="dante_profile",
        description=(
            "Generate a statistical profile of a database table: row count, null "
            "count, null percentage, distinct value count, and min/max for numeric "
            "and date columns. Returns a markdown table."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "table": {
                    "type": "string",
                    "description": "Table name to profile.",
                },
                "schema": {
                    "type": "string",
                    "description": "Optional schema name.",
                },
            },
            "required": ["table"],
        },
    ),
    # --- Charts ---
    Tool(
        name="dante_chart",
        description=(
            "Generate a Plotly chart from JSON data and save it to the project's "
            "outputs/ directory. Data can be a JSON array of row objects (used as a "
            "DataFrame) or a raw Plotly spec dict. Returns the path to the saved file."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "data": {
                    "type": "string",
                    "description": (
                        "JSON string: either an array of row objects "
                        '(e.g. [{"x": 1, "y": 2}, ...]) or a Plotly spec dict.'
                    ),
                },
                "x": {
                    "type": "string",
                    "description": "Column name for the x-axis.",
                },
                "y": {
                    "type": "string",
                    "description": (
                        "Column name(s) for the y-axis. Use a comma-separated "
                        "string for multiple series (e.g. 'revenue,cost')."
                    ),
                },
                "kind": {
                    "type": "string",
                    "description": "Chart type: bar, line, scatter, pie, heatmap, histogram, box.",
                    "default": "bar",
                    "enum": ["bar", "line", "scatter", "pie", "heatmap", "histogram", "box"],
                },
                "title": {
                    "type": "string",
                    "description": "Chart title. Also used for the output filename.",
                    "default": "Chart",
                },
                "format": {
                    "type": "string",
                    "description": "Output format: 'html' (interactive) or 'png' (static image).",
                    "default": "html",
                    "enum": ["html", "png"],
                },
                "theme": {
                    "type": "string",
                    "description": "Color theme: 'dark' or 'light'.",
                    "default": "dark",
                    "enum": ["dark", "light"],
                },
            },
            "required": ["data"],
        },
    ),
    # --- Knowledge ---
    Tool(
        name="dante_search",
        description=(
            "Search the project knowledge base for relevant SQL patterns, business "
            "term definitions, and saved notes. Returns the top matching results."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language search query.",
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of results to return. Defaults to 5.",
                    "default": 5,
                },
            },
            "required": ["query"],
        },
    ),
    Tool(
        name="dante_save_pattern",
        description=(
            "Save a reusable SQL pattern to the project knowledge base. Patterns "
            "capture a business question, the SQL that answers it, the tables "
            "involved, and a human-readable description."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "question": {
                    "type": "string",
                    "description": "The business question this SQL answers.",
                },
                "sql": {
                    "type": "string",
                    "description": "The SQL query template.",
                },
                "tables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of table names referenced by the query.",
                },
                "description": {
                    "type": "string",
                    "description": "Human-readable description of the pattern.",
                },
            },
            "required": ["question", "sql", "tables", "description"],
        },
    ),
    Tool(
        name="dante_define_term",
        description=(
            "Add or update a business term definition in the project glossary. "
            "Useful for capturing domain-specific terminology so future queries "
            "use consistent language."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "term": {
                    "type": "string",
                    "description": "The business term to define.",
                },
                "definition": {
                    "type": "string",
                    "description": "Plain-language definition of the term.",
                },
            },
            "required": ["term", "definition"],
        },
    ),
    # --- App Builder ---
    Tool(
        name="dante_app_create",
        description=(
            "Create a new data app scaffold. Returns an app ID that can be "
            "used with dante_app_add_value and dante_app_render."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Display title for the app.",
                },
                "template": {
                    "type": "string",
                    "description": "App template: 'dashboard', 'report', or 'explorer'.",
                    "default": "dashboard",
                    "enum": ["dashboard", "report", "explorer"],
                },
            },
            "required": ["title"],
        },
    ),
    Tool(
        name="dante_app_add_value",
        description=(
            "Add a data value (KPI, table, or chart) to an existing app. "
            "The SQL query is executed when the app is rendered."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "app_id": {
                    "type": "string",
                    "description": "App identifier from dante_app_create.",
                },
                "name": {
                    "type": "string",
                    "description": "Display name for this value/widget.",
                },
                "sql": {
                    "type": "string",
                    "description": "SQL query that produces the data.",
                },
                "format": {
                    "type": "string",
                    "description": "Render format: 'scalar', 'table', 'bar', 'line', 'pie'.",
                    "default": "scalar",
                    "enum": ["scalar", "table", "bar", "line", "pie"],
                },
            },
            "required": ["app_id", "name", "sql"],
        },
    ),
    Tool(
        name="dante_app_render",
        description=(
            "Render a data app to its output file. Executes all SQL queries "
            "and generates a standalone HTML dashboard."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "app_id": {
                    "type": "string",
                    "description": "App identifier from dante_app_create.",
                },
            },
            "required": ["app_id"],
        },
    ),
    # --- Analysis Checkpoints ---
    Tool(
        name="dante_checkpoint",
        description=(
            "Save a named checkpoint of the current analysis state. "
            "Use dante_rollback to restore it later."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "A descriptive name for this checkpoint.",
                },
            },
            "required": ["name"],
        },
    ),
    Tool(
        name="dante_rollback",
        description=(
            "Roll back to a previously saved analysis checkpoint, "
            "restoring the state that was captured at that point."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "The checkpoint name to restore.",
                },
            },
            "required": ["name"],
        },
    ),
]

# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_DISPATCH = {
    "dante_sql": lambda args: dante_sql(
        query=args["query"],
        limit=args.get("limit", 5000),
    ),
    "dante_tables": lambda args: dante_tables(
        schema=args.get("schema"),
    ),
    "dante_describe": lambda args: dante_describe(
        table=args["table"],
        schema=args.get("schema"),
    ),
    "dante_profile": lambda args: dante_profile(
        table=args["table"],
        schema=args.get("schema"),
    ),
    "dante_chart": lambda args: dante_chart(
        data=args["data"],
        x=args.get("x"),
        y=args.get("y"),
        kind=args.get("kind", "bar"),
        title=args.get("title", "Chart"),
        format=args.get("format", "html"),
        theme=args.get("theme", "dark"),
    ),
    "dante_search": lambda args: dante_search(
        query=args["query"],
        top_k=args.get("top_k", 5),
    ),
    "dante_save_pattern": lambda args: dante_save_pattern(
        question=args["question"],
        sql=args["sql"],
        tables=args.get("tables", []),
        description=args["description"],
    ),
    "dante_define_term": lambda args: dante_define_term(
        term=args["term"],
        definition=args["definition"],
    ),
    "dante_app_create": lambda args: dante_app_create(
        title=args["title"],
        template=args.get("template", "dashboard"),
    ),
    "dante_app_add_value": lambda args: dante_app_add_value(
        app_id=args["app_id"],
        name=args["name"],
        sql=args["sql"],
        format=args.get("format", "scalar"),
    ),
    "dante_app_render": lambda args: dante_app_render(
        app_id=args["app_id"],
    ),
    "dante_checkpoint": lambda args: dante_checkpoint(
        name=args["name"],
    ),
    "dante_rollback": lambda args: dante_rollback(
        name=args["name"],
    ),
}

# ---------------------------------------------------------------------------
# Server setup
# ---------------------------------------------------------------------------

app = Server("dante")


@app.list_tools()
async def list_tools() -> list[Tool]:
    """Return all registered Dante tools."""
    return TOOLS


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Dispatch a tool call to the appropriate function."""
    handler = _DISPATCH.get(name)
    if handler is None:
        return [TextContent(type="text", text=f"**Error:** Unknown tool `{name}`.")]

    try:
        result = handler(arguments)
        return [TextContent(type="text", text=result)]
    except Exception:
        tb = traceback.format_exc()
        return [TextContent(type="text", text=f"**Internal error:**\n```\n{tb}\n```")]


async def main() -> None:
    """Run the Dante MCP server over stdio."""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options(),
        )


if __name__ == "__main__":
    asyncio.run(main())
