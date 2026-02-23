"""MCP tool stubs for knowledge management (search, patterns, glossary).

These are placeholders that will be wired to the knowledge subsystem
once embedding storage and retrieval are implemented.
"""

from __future__ import annotations


def dante_search(query: str, top_k: int = 5) -> str:
    """Search the project knowledge base for relevant SQL patterns, terms, and notes.

    Args:
        query: Natural language search query.
        top_k: Number of results to return. Defaults to 5.

    Returns:
        Markdown-formatted search results.
    """
    # TODO: implement with knowledge.search() once embedding store is ready
    return (
        "_Knowledge search is not yet implemented._ "
        "This feature will allow searching saved SQL patterns, "
        "business term definitions, and project notes."
    )


def dante_save_pattern(
    question: str,
    sql: str,
    tables: list[str],
    description: str,
) -> str:
    """Save a reusable SQL pattern to the project knowledge base.

    Args:
        question: The business question this pattern answers.
        sql: The SQL query template.
        tables: List of tables referenced by the query.
        description: Human-readable description of what the pattern does.

    Returns:
        Confirmation message.
    """
    # TODO: implement with knowledge.save_pattern()
    return (
        "_Pattern saving is not yet implemented._ "
        "This feature will persist SQL patterns for future retrieval."
    )


def dante_define_term(term: str, definition: str) -> str:
    """Add or update a business term definition in the project glossary.

    Args:
        term: The business term to define.
        definition: Plain-language definition of the term.

    Returns:
        Confirmation message.
    """
    # TODO: implement with knowledge.define_term()
    return (
        "_Term definition is not yet implemented._ "
        "This feature will maintain a project glossary of business terms."
    )
