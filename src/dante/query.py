"""Safe SQL execution with automatic LIMIT injection and audit logging.

All queries are read-only. Mutating statements are rejected.
Results are returned as pandas DataFrames or formatted markdown tables.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from dante.connect import connect as get_engine
from dante.config import project_dir
from dante._utils import dataframe_to_markdown

logger = logging.getLogger(__name__)

_MUTATING_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "MERGE",
    "REPLACE",
}
_DEFAULT_LIMIT = 5000


def _strip_comments_and_strings(query: str) -> str:
    """Remove SQL comments and string literals so keyword scanning is reliable.

    String literals are replaced with empty quotes so a word like ``limit`` or
    ``delete`` appearing inside a literal cannot be mistaken for SQL syntax.
    """
    s = re.sub(r"--[^\n]*", "", query)  # line comments
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)  # block comments
    s = re.sub(r"'(?:''|[^'])*'", "''", s)  # single-quoted literals
    s = re.sub(r'"(?:""|[^"])*"', '""', s)  # double-quoted identifiers/strings
    return s


def _is_mutating(query: str) -> bool:
    """Check if a query contains any mutating SQL statement.

    Scans the whole (comment/literal-stripped) query for mutating keywords as
    whole words, not just the first token — so data-modifying CTEs
    (``WITH x AS (DELETE ...)``) and stacked statements (``SELECT 1; DROP ...``)
    are caught too. This is defense-in-depth; the real guarantee is a read-only
    DB role.
    """
    cleaned = _strip_comments_and_strings(query).upper()
    # A mutating keyword only counts when it *starts a statement*: at the
    # beginning, after a ';' (stacked statement), or after '(' (data-modifying
    # CTE). This avoids false positives on functions/columns like REPLACE() or
    # an "update" column appearing mid-SELECT.
    for kw in _MUTATING_KEYWORDS:
        if re.search(rf"(?:^|[;(])\s*{kw}\b", cleaned):
            return True
    return False


def _inject_limit(query: str, limit: int) -> str:
    """Inject a LIMIT clause if none is present.

    The presence check runs against a comment/literal-stripped copy so a
    ``limit`` substring inside a string or column name does not suppress
    injection.
    """
    stripped = query.strip().rstrip(";")
    cleaned = _strip_comments_and_strings(stripped)
    if re.search(r"\bLIMIT\b", cleaned, re.IGNORECASE):
        return query
    return f"{stripped}\nLIMIT {limit}"


def _log_query(
    query: str, rows: int, elapsed_ms: float, root: Path | None = None
) -> None:
    """Append query to .dante/query_log.jsonl."""
    try:
        log_path = project_dir(root) / "query_log.jsonl"
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "query": query.strip(),
            "rows": rows,
            "elapsed_ms": round(elapsed_ms, 1),
        }
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        logger.warning("Query logging failed: %s", e)


def sql(
    query: str,
    limit: int = _DEFAULT_LIMIT,
    engine: Engine | None = None,
    root: Path | None = None,
) -> pd.DataFrame:
    """Execute read-only SQL and return a pandas DataFrame.

    Args:
        query: SQL query string.
        limit: Max rows to return. Auto-injected if no LIMIT in query.
        engine: SQLAlchemy engine. If None, uses the project's default connection.
        root: Project root for logging.

    Returns:
        pandas DataFrame with query results.

    Raises:
        ValueError: If query contains mutating statements.
    """
    if _is_mutating(query):
        raise ValueError(
            "Mutating queries (INSERT, UPDATE, DELETE, DROP, etc.) are not allowed. "
            "dante.sql() is read-only."
        )

    if engine is None:
        engine = get_engine(root=root)

    limited_query = _inject_limit(query, limit)
    start = time.monotonic()

    with engine.connect() as conn:
        result = conn.execute(text(limited_query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    elapsed_ms = (time.monotonic() - start) * 1000
    _log_query(query, len(df), elapsed_ms, root)
    return df


def sql_markdown(
    query: str,
    limit: int = _DEFAULT_LIMIT,
    engine: Engine | None = None,
    root: Path | None = None,
) -> str:
    """Execute read-only SQL and return results as a markdown table.

    Used by MCP tools to return formatted results to Claude.
    """
    # Fetch one extra row so we can tell "exactly limit rows" from "capped".
    df = sql(query, limit=limit + 1, engine=engine, root=root)

    truncated = len(df) > limit
    if truncated:
        df = df.head(limit)

    result = dataframe_to_markdown(df)
    if truncated:
        result += f"\n\n_Results truncated to {limit} rows._"
    return result


def tables(schema: str | None = None, engine: Engine | None = None) -> list[str]:
    """List all table names, optionally filtered by schema."""
    if engine is None:
        engine = get_engine()
    insp = inspect(engine)

    # Databricks requires an explicit schema; list all schemas if none given
    if schema is None and engine.dialect.name == "databricks":
        result = []
        for s in insp.get_schema_names():
            if s == "information_schema":
                continue
            try:
                for t in insp.get_table_names(schema=s):
                    result.append(f"{s}.{t}")
            except Exception as e:
                logger.debug("Failed to list tables for schema %r: %s", s, e)
        return result

    return insp.get_table_names(schema=schema)


def tables_markdown(schema: str | None = None, engine: Engine | None = None) -> str:
    """List tables as a markdown list."""
    tbl_list = tables(schema=schema, engine=engine)
    if not tbl_list:
        return "_No tables found._"
    return "\n".join(f"- `{t}`" for t in sorted(tbl_list))


def describe(
    table: str, schema: str | None = None, engine: Engine | None = None
) -> pd.DataFrame:
    """Get column metadata for a table: name, type, nullable, and sample values."""
    if engine is None:
        engine = get_engine()
    insp = inspect(engine)
    columns = insp.get_columns(table, schema=schema)

    # Fallback for Databricks and other dialects where inspect() returns empty:
    # query information_schema.columns directly.
    if not columns and schema:
        try:
            with engine.connect() as conn:
                q = text(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table "
                    "ORDER BY ordinal_position"
                )
                result = conn.execute(q, {"schema": schema, "table": table})
                for row in result.fetchall():
                    columns.append({
                        "name": row[0],
                        "type": row[1],
                        "nullable": row[2] == "YES" if isinstance(row[2], str) else row[2],
                    })
        except Exception as e:
            logger.warning("information_schema fallback failed for %r.%r: %s", schema, table, e)

    rows = []
    # Try to get sample values
    samples = {}
    try:
        sample_query = (
            f"SELECT * FROM {_qualified_name(table, schema, engine)} LIMIT 3"
        )
        with engine.connect() as conn:
            result = conn.execute(text(sample_query))
            sample_rows = result.fetchall()
            col_names = list(result.keys())
            for idx, col in enumerate(col_names):
                vals = [str(row[idx]) for row in sample_rows if row[idx] is not None]
                samples[col] = ", ".join(vals[:3]) if vals else ""
    except Exception as e:
        logger.warning("Failed to fetch sample values for %r: %s", table, e)

    for col in columns:
        rows.append(
            {
                "column": col["name"],
                "type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "samples": samples.get(col["name"], ""),
            }
        )

    return pd.DataFrame(rows)


def describe_markdown(
    table: str, schema: str | None = None, engine: Engine | None = None
) -> str:
    """Get column metadata as a markdown table."""
    df = describe(table, schema=schema, engine=engine)
    if df.empty:
        return f"_Table `{table}` not found or has no columns._"

    lines = ["| Column | Type | Nullable | Samples |"]
    lines.append("| --- | --- | --- | --- |")
    for _, row in df.iterrows():
        lines.append(
            f"| `{row['column']}` | {row['type']} | {row['nullable']} | {row['samples']} |"
        )
    return "\n".join(lines)


def profile(
    table: str, schema: str | None = None, engine: Engine | None = None
) -> pd.DataFrame:
    """Statistical profile: row count, nulls, cardinality, min/max per column."""
    if engine is None:
        engine = get_engine()

    qualified = _qualified_name(table, schema, engine)
    insp = inspect(engine)
    columns = insp.get_columns(table, schema=schema)

    rows = []
    # One connection for the whole profile — count, fallback metadata, and every
    # per-column query share it rather than reconnecting per column.
    with engine.connect() as conn:
        total_rows = conn.execute(text(f"SELECT COUNT(*) FROM {qualified}")).scalar()

        # Fallback for Databricks and other dialects where inspect() returns empty
        if not columns and schema:
            try:
                q = text(
                    "SELECT column_name, data_type "
                    "FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table "
                    "ORDER BY ordinal_position"
                )
                result = conn.execute(q, {"schema": schema, "table": table})
                for row in result.fetchall():
                    columns.append({"name": row[0], "type": row[1]})
            except Exception as e:
                logger.warning(
                    "information_schema fallback failed for %r.%r: %s",
                    schema, table, e,
                )

        for col in columns:
            col_name = col["name"]
            col_type = str(col["type"]).upper()
            qcol = _qualified_name(col_name, engine=engine)
            stats: dict = {
                "column": col_name,
                "type": col_type,
                "total_rows": total_rows,
            }

            try:
                q = text(
                    f"SELECT COUNT(*) - COUNT({qcol}) as nulls, "
                    f"COUNT(DISTINCT {qcol}) as distinct_count "
                    f"FROM {qualified}"
                )
                r = conn.execute(q).fetchone()
                stats["nulls"] = r[0]
                stats["null_pct"] = (
                    round(r[0] / total_rows * 100, 1) if total_rows else 0
                )
                stats["distinct"] = r[1]

                # Min/max for numeric and date types
                if any(
                    t in col_type
                    for t in ("INT", "FLOAT", "NUMERIC", "DECIMAL", "DATE", "TIME")
                ):
                    q2 = text(f"SELECT MIN({qcol}), MAX({qcol}) FROM {qualified}")
                    r2 = conn.execute(q2).fetchone()
                    stats["min"] = str(r2[0]) if r2[0] is not None else ""
                    stats["max"] = str(r2[1]) if r2[1] is not None else ""
                else:
                    stats["min"] = ""
                    stats["max"] = ""
            except Exception as e:
                logger.warning(
                    "Failed to profile column %r in %r: %s", col_name, table, e
                )
                stats.update(
                    {"nulls": "", "null_pct": "", "distinct": "", "min": "", "max": ""}
                )

            rows.append(stats)

    return pd.DataFrame(rows)


def profile_markdown(
    table: str, schema: str | None = None, engine: Engine | None = None
) -> str:
    """Statistical profile as a markdown table."""
    df = profile(table, schema=schema, engine=engine)
    if df.empty:
        return f"_Table `{table}` not found._"

    lines = ["| Column | Type | Rows | Nulls | Null% | Distinct | Min | Max |"]
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for _, row in df.iterrows():
        lines.append(
            f"| `{row['column']}` | {row['type']} | {row['total_rows']} | "
            f"{row['nulls']} | {row['null_pct']}% | {row['distinct']} | "
            f"{row['min']} | {row['max']} |"
        )
    return "\n".join(lines)


def _ansi_quote(identifier: str) -> str:
    """Quote a single SQL identifier with ANSI double quotes, escaping any."""
    return '"' + identifier.replace('"', '""') + '"'


def _qualified_name(
    table: str, schema: str | None = None, engine: Engine | None = None
) -> str:
    """Build a safely-quoted ``schema.table`` (or just ``table``) identifier.

    Quoting both prevents injection through caller-supplied table/schema names
    and lets identifiers with special characters or reserved words work. Uses
    the engine's dialect preparer when available, falling back to ANSI quoting.
    """
    preparer = None
    if engine is not None:
        try:
            preparer = engine.dialect.identifier_preparer
        except Exception:
            preparer = None

    quote = preparer.quote if preparer is not None else _ansi_quote
    if schema:
        return f"{quote(schema)}.{quote(table)}"
    return quote(table)
