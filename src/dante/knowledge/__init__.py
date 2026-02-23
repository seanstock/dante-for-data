"""Knowledge system for dante-lib.

Four layers:
    1. Notes    -- free-form markdown (.dante/knowledge/notes.md)
    2. Glossary -- term definitions (.dante/knowledge/terms.yaml)
    3. Keywords -- substring-matched triggers (.dante/knowledge/keywords.yaml)
    4. Embeddings -- SQLite + vector similarity (.dante/knowledge/embeddings.db)

This module exposes a unified public API that delegates to the individual
sub-modules (notes, glossary, keywords, patterns, embeddings, search).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from dante.config import knowledge_dir
from dante.knowledge import (
    embeddings as emb_module,
    glossary as glossary_module,
    keywords as kw_module,
    notes as notes_module,
    patterns as patterns_module,
    search as search_module,
    vectorize,
)

logger = logging.getLogger(__name__)

# Re-export sub-modules for direct access
glossary = glossary_module
keywords = kw_module
notes = notes_module
patterns = patterns_module
embeddings = emb_module

def _db_path(root: Path | None = None) -> Path:
    return knowledge_dir(root) / "embeddings.db"


# ---------------------------------------------------------------------------
# Unified search
# ---------------------------------------------------------------------------

def search(
    query: str,
    top_k: int = 5,
    threshold: float = 0.3,
    root: Path | None = None,
) -> list[dict]:
    """Unified search across keywords and embeddings.

    Returns a ranked list of dicts with keys:
        question, sql, source, dashboard, similarity, keyword_match, description
    """
    return search_module.search(query, top_k=top_k, threshold=threshold, root=root)


async def search_async(
    query: str,
    top_k: int = 5,
    threshold: float = 0.3,
    root: Path | None = None,
) -> list[dict]:
    """Async version of search."""
    return await search_module.search_async(
        query, top_k=top_k, threshold=threshold, root=root
    )


# ---------------------------------------------------------------------------
# Patterns (SQL examples)
# ---------------------------------------------------------------------------

def save_pattern(
    question: str,
    sql: str,
    tables: list[str] | None = None,
    description: str = "",
    root: Path | None = None,
) -> dict:
    """Save a SQL pattern and generate an embedding for it.

    Writes a .sql file with YAML frontmatter to .dante/knowledge/patterns/
    and upserts an embedding row into the SQLite database.

    Returns the pattern dict.
    """
    # Save the file
    path = patterns_module.save_pattern(
        question=question, sql=sql, tables=tables,
        description=description, root=root,
    )
    pattern = patterns_module.load_pattern(path)

    # Try to generate and store an embedding
    _embed_pattern(pattern, root=root)

    return pattern


def list_patterns(root: Path | None = None) -> list[dict]:
    """List all saved SQL patterns."""
    return patterns_module.list_patterns(root)


# ---------------------------------------------------------------------------
# Glossary
# ---------------------------------------------------------------------------

def define(term: str, definition: str, root: Path | None = None) -> None:
    """Add or update a glossary term in terms.yaml."""
    glossary_module.define(term, definition, root)


def undefine(term: str, root: Path | None = None) -> bool:
    """Remove a glossary term. Returns True if it existed."""
    return glossary_module.undefine(term, root)


def list_terms(root: Path | None = None) -> list[dict[str, str]]:
    """List all glossary terms as [{term, definition}, ...]."""
    return glossary_module.list_terms(root)


# ---------------------------------------------------------------------------
# Keywords
# ---------------------------------------------------------------------------

def add_keyword(keyword: str, content: str, root: Path | None = None) -> None:
    """Add or update a keyword trigger in keywords.yaml."""
    kw_module.add(keyword, content, root)


def remove_keyword(keyword: str, root: Path | None = None) -> bool:
    """Remove a keyword. Returns True if it existed."""
    return kw_module.remove(keyword, root)


# ---------------------------------------------------------------------------
# Notes
# ---------------------------------------------------------------------------

def add_note(text: str, root: Path | None = None) -> None:
    """Append text to notes.md."""
    notes_module.append(text, root)


# ---------------------------------------------------------------------------
# Stats & maintenance
# ---------------------------------------------------------------------------

def stats(root: Path | None = None) -> dict:
    """Return knowledge system statistics.

    Returns a dict with:
        embedding_count: total embeddings
        by_source: {source: count}
        last_updated: ISO timestamp of last embedding update
        pattern_count: number of saved SQL patterns
        keyword_count: number of keyword triggers
        term_count: number of glossary terms
        notes_size: size of notes.md in bytes
    """
    result = {
        "embedding_count": 0,
        "by_source": {},
        "last_updated": None,
        "pattern_count": len(patterns_module.list_patterns(root)),
        "keyword_count": len(kw_module.load(root)),
        "term_count": len(glossary_module.load(root)),
        "notes_size": 0,
    }

    # Notes size
    notes_path = knowledge_dir(root) / "notes.md"
    if notes_path.exists():
        result["notes_size"] = notes_path.stat().st_size

    # Embedding stats
    db = _db_path(root)
    if db.exists():
        conn = emb_module.init_db(db)
        try:
            emb_stats = emb_module.stats(conn)
            result["embedding_count"] = emb_stats["total"]
            result["by_source"] = emb_stats["by_source"]
            result["last_updated"] = emb_stats["last_updated"]
        finally:
            conn.close()

    return result


def rebuild(root: Path | None = None) -> dict:
    """Rebuild the embedding database from all patterns.

    Placeholder implementation: re-reads all pattern files and
    regenerates their embeddings.

    Returns a dict with {rebuilt: count, errors: count}.
    """
    pats = patterns_module.list_patterns(root)
    rebuilt = 0
    errors = 0

    for pat in pats:
        try:
            _embed_pattern(pat, root=root)
            rebuilt += 1
        except Exception as e:
            logger.warning("Failed to embed pattern %s: %s", pat["filename"], e)
            errors += 1

    return {"rebuilt": rebuilt, "errors": errors}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _embed_pattern(pattern: dict, root: Path | None = None) -> None:
    """Generate an embedding for a pattern and store it in the database.

    Silently skips if OPENAI_API_KEY is not set.
    """
    text = _pattern_to_text(pattern)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    try:
        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                vec = pool.submit(
                    asyncio.run, vectorize.generate_embedding(text)
                ).result()
        else:
            vec = asyncio.run(vectorize.generate_embedding(text))
    except RuntimeError as e:
        # OPENAI_API_KEY not set
        logger.debug("Skipping embedding generation: %s", e)
        return
    except Exception as e:
        logger.warning("Embedding generation failed: %s", e)
        return

    db = _db_path(root)
    conn = emb_module.init_db(db)
    try:
        emb_module.upsert(
            conn,
            id=pattern["filename"],
            question=pattern["question"],
            sql=pattern.get("sql", ""),
            source=pattern.get("source", "manual"),
            dashboard="",
            description=pattern.get("description", ""),
            embedding_vector=vec,
        )
    finally:
        conn.close()


def _pattern_to_text(pattern: dict) -> str:
    """Convert a pattern dict into a text string suitable for embedding.

    Combines the question, description, and SQL so the embedding captures
    both the intent and the implementation.
    """
    parts = []
    if pattern.get("question"):
        parts.append(pattern["question"])
    if pattern.get("description"):
        parts.append(pattern["description"])
    if pattern.get("sql"):
        parts.append(pattern["sql"])
    return "\n".join(parts)
