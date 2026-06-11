"""MCP tool implementations for knowledge management (search, patterns)."""

from __future__ import annotations


async def dante_search(query: str, top_k: int = 10) -> str:
    """Search the project knowledge base for relevant SQL patterns and keywords."""
    from dante.knowledge.search import search_async

    results = await search_async(query, top_k=top_k)

    if not results:
        return "No matching patterns or keywords found."

    parts = []
    parts.append("### Knowledge Matches\n")
    for r in results:
        sim = r.get("similarity", 0)
        source = r.get("source", "")
        kw = r.get("keyword_match")

        if kw:
            parts.append(f"**Keyword: {kw}** (exact match)")
            parts.append(f"> {r.get('description', '')}")
        else:
            q = r.get("question", "(no question)")
            parts.append(f"**{q}** (similarity: {sim:.2f}, source: {source})")
            if r.get("description"):
                parts.append(f"> {r['description']}")
            if r.get("sql"):
                parts.append(f"```sql\n{r['sql']}\n```")
        parts.append("")

    return "\n".join(parts)


async def dante_save_pattern(
    question: str,
    sql: str,
    tables: list[str],
    description: str,
) -> str:
    """Save a reusable SQL pattern to the project knowledge base."""
    from dante.knowledge import save_pattern_async

    path = await save_pattern_async(
        question=question,
        sql=sql,
        tables=tables,
        description=description,
    )
    return f"Pattern saved to `{path}`. It will be matched in future `dante_search` calls."
