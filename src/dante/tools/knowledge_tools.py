"""MCP tool implementations for knowledge management (search, patterns, glossary)."""

from __future__ import annotations


def dante_search(query: str, top_k: int = 10) -> str:
    """Search the project knowledge base for relevant SQL patterns, terms, and notes."""
    from dante.knowledge.search import search
    from dante.knowledge import glossary

    results = search(query, top_k=top_k)

    # Also search glossary terms by substring
    terms = glossary.load()
    query_lower = query.lower()
    term_matches = [
        (t, d) for t, d in terms.items()
        if query_lower in t.lower() or query_lower in (d or "").lower()
    ]

    if not results and not term_matches:
        return "No matching patterns, keywords, or glossary terms found."

    parts = []

    if term_matches:
        parts.append("### Glossary Matches\n")
        for term, definition in sorted(term_matches):
            parts.append(f"- **{term}**: {definition}")
        parts.append("")

    if results:
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


def dante_save_pattern(
    question: str,
    sql: str,
    tables: list[str],
    description: str,
) -> str:
    """Save a reusable SQL pattern to the project knowledge base."""
    from dante.knowledge.patterns import save_pattern

    path = save_pattern(
        question=question,
        sql=sql,
        tables=tables,
        description=description,
    )
    return f"Pattern saved to `{path.name}`. It will be matched in future `dante_search` calls."


def dante_define_term(term: str, definition: str) -> str:
    """Add or update a business term definition in the project glossary."""
    from dante.knowledge.glossary import define

    define(term, definition)
    return f"Glossary term **{term}** saved."
