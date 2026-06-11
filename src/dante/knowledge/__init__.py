"""Knowledge system for dante-lib.

Three layers:
    1. Notes      -- free-form markdown (.dante/knowledge/notes.md)
    2. Keywords   -- substring-matched triggers (.dante/knowledge/keywords.yaml)
    3. Embeddings -- SQLite + vector similarity (.dante/knowledge/embeddings.db)

This module exposes a unified public API that delegates to the individual
sub-modules (notes, keywords, patterns, embeddings, search).
"""

from __future__ import annotations

import logging
from pathlib import Path

from dante._utils import run_async
from dante.config import knowledge_dir
from dante.knowledge import (
    embeddings as emb_module,
    keywords as kw_module,
    notes as notes_module,
    patterns as patterns_module,
    search as search_module,
    vectorize,
)

logger = logging.getLogger(__name__)

# Re-export sub-modules for direct access
keywords = kw_module
notes = notes_module
patterns = patterns_module
embeddings = emb_module


def _db_path(root: Path | None = None) -> Path:
    return knowledge_dir(root) / "embeddings.db"


def _remote(root: Path | None = None):
    """Return a configured remote client, or None for local mode."""
    from dante.remote import _get_remote_client

    return _get_remote_client(root)


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

    In remote mode (Studio configured) the pattern is saved to Studio only.
    In local mode it writes a .sql file with YAML frontmatter to
    .dante/knowledge/patterns/ and upserts an embedding row into SQLite.

    Returns the pattern dict.
    """
    remote = _remote(root)
    if remote is not None:
        return remote.save_pattern(
            question=question,
            sql=sql,
            tables=tables,
            description=description,
        )

    # Save the file
    path = patterns_module.save_pattern(
        question=question,
        sql=sql,
        tables=tables,
        description=description,
        root=root,
    )
    pattern = patterns_module.load_pattern(path)

    # Try to generate and store an embedding
    _embed_pattern(pattern, root=root)

    return pattern


async def save_pattern_async(
    question: str,
    sql: str,
    tables: list[str] | None = None,
    description: str = "",
    root: Path | None = None,
) -> str:
    """Async version of save_pattern. Returns the pattern filename.

    Used by MCP tools to avoid sync-in-async thread workarounds.

    In remote mode the pattern is saved to Studio only.
    """
    import asyncio

    remote = _remote(root)
    if remote is not None:
        result = await asyncio.to_thread(
            remote.save_pattern,
            question=question,
            sql=sql,
            tables=tables,
            description=description,
        )
        return result.get("id") or result.get("filename") or question

    path = patterns_module.save_pattern(
        question=question,
        sql=sql,
        tables=tables,
        description=description,
        root=root,
    )
    pattern = patterns_module.load_pattern(path)
    await _embed_pattern_async(pattern, root=root)
    return path.name


def list_patterns(root: Path | None = None) -> list[dict]:
    """List all saved SQL patterns.

    In remote mode, lists patterns stored in Studio.
    """
    remote = _remote(root)
    if remote is not None:
        return remote.list_patterns()
    return patterns_module.list_patterns(root)


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
        notes_size: size of notes.md in bytes

    In remote mode, returns Studio's stats.
    """
    remote = _remote(root)
    if remote is not None:
        return remote.stats()

    result = {
        "embedding_count": 0,
        "by_source": {},
        "last_updated": None,
        "pattern_count": len(patterns_module.list_patterns(root)),
        "keyword_count": len(kw_module.load(root)),
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

    Re-reads all pattern files and regenerates their embeddings.

    Returns a dict with {rebuilt: count, errors: count}. This is a local-only
    operation; in remote mode Studio owns embedding generation, so it is a no-op.
    """
    if _remote(root) is not None:
        return {"rebuilt": 0, "errors": 0, "skipped": "remote mode"}

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
        vec = run_async(vectorize.generate_embedding(text))
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


async def _embed_pattern_async(pattern: dict, root: Path | None = None) -> None:
    """Async version of _embed_pattern — avoids thread workaround.

    Silently skips if OPENAI_API_KEY is not set.
    """
    text = _pattern_to_text(pattern)

    try:
        vec = await vectorize.generate_embedding(text)
    except RuntimeError as e:
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
