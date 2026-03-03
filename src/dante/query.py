"""Safe SQL execution with automatic LIMIT injection and audit logging.

All queries are read-only. Mutating statements are rejected.
Results are returned as pandas DataFrames or formatted markdown tables.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from dante.connect import connect as get_engine
from dante.config import project_dir

_MUTATING_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE", "MERGE", "REPLACE"}
_DEFAULT_LIMIT = 5000


def _is_mutating(query: str) -> bool:
    """Check if a query contains mutating SQL statements."""
    stripped = re.sub(r"--[^\n]*", "", query)  # strip line comments
    stripped = re.sub(r"/\*.*?\*/", "", stripped, flags=re.DOTALL)  # strip block comments
    first_word = stripped.strip().split()[0].upper() if stripped.strip() else ""
    return first_word in _MUTATING_KEYWORDS


def _inject_limit(query: str, limit: int) -> str:
    """Inject a LIMIT clause if none is present."""
    stripped = query.strip().rstrip(";")
    if re.search(r"\bLIMIT\b", stripped, re.IGNORECASE):
        return query
    return f"{stripped}\nLIMIT {limit}"


def _log_query(query: str, rows: int, elapsed_ms: float, root: Path | None = None) -> None:
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
    except Exception:
        pass  # logging should never break queries


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
    df = sql(query, limit=limit, engine=engine, root=root)

    if df.empty:
        return "_No results._"

    # Format as markdown table
    headers = list(df.columns)
    lines = ["| " + " | ".join(str(h) for h in headers) + " |"]
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for _, row in df.iterrows():
        lines.append("| " + " | ".join(str(v) for v in row) + " |")

    result = "\n".join(lines)
    if len(df) == limit:
        result += f"\n\n_Results truncated to {limit} rows._"
    return result


def tables(schema: str | None = None, engine: Engine | None = None) -> list[str]:
    """List all table names, optionally filtered by schema."""
    if engine is None:
        engine = get_engine()
    insp = inspect(engine)
    return insp.get_table_names(schema=schema)


def tables_markdown(schema: str | None = None, engine: Engine | None = None) -> str:
    """List tables as a markdown list."""
    tbl_list = tables(schema=schema, engine=engine)
    if not tbl_list:
        return "_No tables found._"
    return "\n".join(f"- `{t}`" for t in sorted(tbl_list))


def describe(table: str, schema: str | None = None, engine: Engine | None = None) -> pd.DataFrame:
    """Get column metadata for a table: name, type, nullable, and sample values."""
    if engine is None:
        engine = get_engine()
    insp = inspect(engine)
    columns = insp.get_columns(table, schema=schema)

    rows = []
    # Try to get sample values
    samples = {}
    try:
        sample_query = f"SELECT * FROM {_qualified_name(table, schema)} LIMIT 3"
        with engine.connect() as conn:
            result = conn.execute(text(sample_query))
            sample_rows = result.fetchall()
            col_names = list(result.keys())
            for col in col_names:
                idx = col_names.index(col)
                vals = [str(row[idx]) for row in sample_rows if row[idx] is not None]
                samples[col] = ", ".join(vals[:3]) if vals else ""
    except Exception:
        pass

    for col in columns:
        rows.append({
            "column": col["name"],
            "type": str(col["type"]),
            "nullable": col.get("nullable", True),
            "samples": samples.get(col["name"], ""),
        })

    return pd.DataFrame(rows)


def describe_markdown(table: str, schema: str | None = None, engine: Engine | None = None) -> str:
    """Get column metadata as a markdown table."""
    df = describe(table, schema=schema, engine=engine)
    if df.empty:
        return f"_Table `{table}` not found or has no columns._"

    lines = ["| Column | Type | Nullable | Samples |"]
    lines.append("| --- | --- | --- | --- |")
    for _, row in df.iterrows():
        lines.append(f"| `{row['column']}` | {row['type']} | {row['nullable']} | {row['samples']} |")
    return "\n".join(lines)


def profile(table: str, schema: str | None = None, engine: Engine | None = None) -> pd.DataFrame:
    """Statistical profile: row count, nulls, cardinality, min/max per column."""
    if engine is None:
        engine = get_engine()

    qualified = _qualified_name(table, schema)

    # Get row count
    with engine.connect() as conn:
        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {qualified}"))
        total_rows = count_result.scalar()

    insp = inspect(engine)
    columns = insp.get_columns(table, schema=schema)

    rows = []
    for col in columns:
        col_name = col["name"]
        col_type = str(col["type"]).upper()
        stats: dict = {"column": col_name, "type": col_type, "total_rows": total_rows}

        try:
            with engine.connect() as conn:
                # Null count and distinct count
                q = text(
                    f"SELECT COUNT(*) - COUNT({col_name}) as nulls, "
                    f"COUNT(DISTINCT {col_name}) as distinct_count "
                    f"FROM {qualified}"
                )
                r = conn.execute(q).fetchone()
                stats["nulls"] = r[0]
                stats["null_pct"] = round(r[0] / total_rows * 100, 1) if total_rows > 0 else 0
                stats["distinct"] = r[1]

                # Min/max for numeric and date types
                if any(t in col_type for t in ("INT", "FLOAT", "NUMERIC", "DECIMAL", "DATE", "TIME")):
                    q2 = text(f"SELECT MIN({col_name}), MAX({col_name}) FROM {qualified}")
                    r2 = conn.execute(q2).fetchone()
                    stats["min"] = str(r2[0]) if r2[0] is not None else ""
                    stats["max"] = str(r2[1]) if r2[1] is not None else ""
                else:
                    stats["min"] = ""
                    stats["max"] = ""
        except Exception:
            stats.update({"nulls": "", "null_pct": "", "distinct": "", "min": "", "max": ""})

        rows.append(stats)

    return pd.DataFrame(rows)


def profile_markdown(table: str, schema: str | None = None, engine: Engine | None = None) -> str:
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


def _qualified_name(table: str, schema: str | None = None) -> str:
    """Build schema.table or just table."""
    if schema:
        return f"{schema}.{table}"
    return table
