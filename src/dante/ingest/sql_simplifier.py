"""LLM-based SQL simplification for embedding quality.

Strips BI platform noise (pivot scaffolding, unnecessary nesting, etc.)
while preserving business logic.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

_MAX_SQL_LENGTH = 4000

_SIMPLIFICATION_PROMPT = """You simplify SQL queries from BI platforms. You ONLY operate on SQL queries \
(statements starting with SELECT, WITH, etc.).

If the input is NOT a SQL query — for example a table description, schema metadata, DDL output, \
or any other non-query text — return it EXACTLY as-is. Do not convert it into a query.

If there is SQL and Metadata, you always retain the metadata in your output.

For actual SQL queries, simplify by:

REMOVING:
- Pivot and ranking window functions added by BI tools, especially Looker syntax.
- Unnecessary subquery nesting
- Platform-specific column alias prefixes (e.g., Looker's dimension_name.field_name)
- LIMIT clauses
- Complex date formatting expressions (simplify to just the column reference)

PRESERVING:
- SQL comments (they contain valuable business context)
- Core SELECT columns (the actual metrics and dimensions)
- FROM and JOIN clauses with real table names
- WHERE conditions that contain business filters
- GROUP BY clauses
- Actual aggregation functions (SUM, COUNT, AVG, etc.)
- HAVING clauses
- ORDER BY clauses
- CASE WHEN expressions that represent business logic

Return ONLY the result. No explanation, no markdown code blocks.

Do not be surprised to return the input exactly as-is. That is the most common outcome.
"""


def _strip_markdown_wrappers(text: str) -> str:
    text = text.strip()
    if text.startswith("```sql"):
        text = text[6:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


async def simplify_sql(raw_sql: str, chart_title: str, enabled: bool = True) -> str:
    """Simplify raw BI platform SQL using a lightweight LLM.

    Args:
        raw_sql: The raw SQL from the BI platform.
        chart_title: The chart title for context.
        enabled: If False, returns raw_sql unchanged.

    Returns:
        Simplified SQL string, or raw_sql if simplification fails.
    """
    if not enabled:
        return raw_sql

    if not raw_sql or not raw_sql.strip():
        return raw_sql

    truncated = raw_sql[:_MAX_SQL_LENGTH]
    prompt = f"Chart title: {chart_title}\n\nInput:\n{truncated}"

    try:
        from openai import AsyncOpenAI
        import os

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.warning("OPENAI_API_KEY not set, skipping SQL simplification")
            return raw_sql

        client = AsyncOpenAI(api_key=api_key)
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _SIMPLIFICATION_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
            max_tokens=2000,
        )

        simplified = _strip_markdown_wrappers(response.choices[0].message.content or "")
        if simplified:
            return simplified
        logger.warning("SQL simplification returned empty result for '%s'", chart_title)
        return raw_sql
    except Exception:
        logger.exception("SQL simplification failed for '%s'", chart_title)
        return raw_sql
